package peer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRaceTimer struct {
	ch      chan time.Time
	stopped atomic.Bool
}

func (t *fakeRaceTimer) Chan() <-chan time.Time { return t.ch }
func (t *fakeRaceTimer) Stop() bool {
	return !t.stopped.Swap(true)
}

type fakeRaceClock struct {
	timer   *fakeRaceTimer
	created chan struct{}
}

func (c *fakeRaceClock) NewTimer(time.Duration) RaceTimer {
	c.timer = &fakeRaceTimer{ch: make(chan time.Time, 1)}
	if c.created != nil {
		close(c.created)
	}
	return c.timer
}

func (c *fakeRaceClock) Fire() { c.timer.ch <- time.Unix(1, 0) }

func testHandshake() Handshake {
	return Handshake{InfoHash: [20]byte{1, 2, 3}, PeerID: [20]byte{4, 5, 6}}
}

func pipeServer(t *testing.T, conn net.Conn, local Handshake, remote Handshake, delay time.Duration) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.Close()
		if _, err := ReadHandshake(conn, &local.InfoHash, nil); err != nil {
			return
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		_ = WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved)
	}()
	return done
}

func handshakePipeDial(t *testing.T, local, remote Handshake) DialFunc {
	t.Helper()
	return func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		pipeServer(t, server, local, remote, 0)
		return client, nil
	}
}

func TestRaceEndpointUTPWinsAndUsesExactEndpoint(t *testing.T) {
	local := testHandshake()
	remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{9}}
	ep := Endpoint{Addr: netip.MustParseAddr("2001:db8::1"), Port: 6881}
	var callsMu sync.Mutex
	var calls []string
	tcpCalled := make(chan struct{}, 1)
	config := RaceConfig{
		LocalHandshake: local,
		UTPHeadStart:   20 * time.Millisecond,
		UTPDial: func(ctx context.Context, network, address string) (net.Conn, error) {
			callsMu.Lock()
			calls = append(calls, network+" "+address)
			callsMu.Unlock()
			client, server := net.Pipe()
			pipeServer(t, server, local, remote, 0)
			return client, nil
		},
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			tcpCalled <- struct{}{}
			return nil, errors.New("TCP should not start before uTP wins")
		},
	}
	result, err := RaceEndpoint(context.Background(), ep, config)
	if err != nil {
		t.Fatalf("RaceEndpoint: %v", err)
	}
	if result.Transport != TransportUTP || result.Conn == nil || result.Endpoint != ep || result.Handshake.PeerID != remote.PeerID {
		t.Fatalf("race result = %+v", result)
	}
	_ = result.Conn.Close()
	select {
	case <-tcpCalled:
		t.Fatal("TCP started after uTP already won")
	default:
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if len(calls) != 1 || calls[0] != "utp6 [2001:db8::1]:6881" {
		t.Fatalf("dial calls = %v", calls)
	}
}

func TestRaceEndpointTCPWinsAndCancelsLoser(t *testing.T) {
	local := testHandshake()
	remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{8}}
	ep := Endpoint{Addr: netip.MustParseAddr("192.0.2.9"), Port: 51413}
	var utpServerDone <-chan struct{}
	var utpClient net.Conn
	config := RaceConfig{
		LocalHandshake: local,
		UTPHeadStart:   5 * time.Millisecond,
		UTPDial: func(context.Context, string, string) (net.Conn, error) {
			var server net.Conn
			utpClient, server = net.Pipe()
			utpServerDone = pipeServer(t, server, local, remote, 500*time.Millisecond)
			return utpClient, nil
		},
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			pipeServer(t, server, local, remote, 0)
			return client, nil
		},
	}
	result, err := RaceEndpoint(context.Background(), ep, config)
	if err != nil {
		t.Fatalf("RaceEndpoint: %v", err)
	}
	if result.Transport != TransportTCP {
		t.Fatalf("transport winner = %v, want TCP", result.Transport)
	}
	_ = result.Conn.Close()
	select {
	case <-utpServerDone:
	case <-time.After(time.Second):
		t.Fatal("uTP loser was not closed and joined")
	}
	if utpClient != nil {
		// Close is idempotent for the race owner and makes ownership explicit in
		// case the fake server returned before observing cancellation.
		_ = utpClient.Close()
	}
}

func TestRaceEndpointCancellationClosesStalledHandshakes(t *testing.T) {
	local := testHandshake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var serversMu sync.Mutex
	var servers []net.Conn
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		serversMu.Lock()
		servers = append(servers, server)
		serversMu.Unlock()
		return client, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := RaceEndpoint(ctx, Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 1}, RaceConfig{
			LocalHandshake: local,
			UTPDial:        dial,
			TCPDial:        dial,
			UTPHeadStart:   time.Millisecond,
		})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled race error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled race did not join stalled attempts")
	}
	serversMu.Lock()
	for _, server := range servers {
		_ = server.Close()
	}
	serversMu.Unlock()
}

func TestRaceEndpointReportsBothFailuresWithoutStrike(t *testing.T) {
	local := testHandshake()
	var calls atomic.Int32
	utpRelease := make(chan struct{})
	clock := &fakeRaceClock{created: make(chan struct{})}
	tcpStarted := make(chan struct{})
	done := make(chan struct {
		result HandshakeResult
		err    error
	}, 1)
	go func() {
		result, err := RaceEndpoint(context.Background(), Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 6881}, RaceConfig{
			LocalHandshake: local,
			UTPDial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				<-utpRelease
				return nil, errors.New("uTP down")
			},
			TCPDial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				close(tcpStarted)
				return nil, errors.New("TCP down")
			},
			Clock:        clock,
			UTPHeadStart: time.Second,
		})
		done <- struct {
			result HandshakeResult
			err    error
		}{result: result, err: err}
	}()
	<-clock.created
	clock.Fire()
	select {
	case <-tcpStarted:
	case <-time.After(time.Second):
		t.Fatal("TCP did not start after controlled head start")
	}
	close(utpRelease)
	out := <-done
	result, err := out.result, out.err
	if result.Conn != nil {
		t.Fatal("failed race returned connection")
	}
	var raceErr *RaceError
	if !errors.As(err, &raceErr) || len(raceErr.Attempts) != 2 || calls.Load() != 2 {
		t.Fatalf("race error=%v calls=%d", err, calls.Load())
	}
}

func TestRaceEndpointStartsTCPAfterHeadStartWhenUTPFailsEarly(t *testing.T) {
	local := testHandshake()
	remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{10}}
	clock := &fakeRaceClock{created: make(chan struct{})}
	tcpStarted := make(chan struct{})
	done := make(chan struct {
		result HandshakeResult
		err    error
	}, 1)
	go func() {
		result, err := RaceEndpoint(context.Background(), Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 6881}, RaceConfig{
			LocalHandshake: local,
			UTPDial: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("uTP unavailable")
			},
			TCPDial: func(context.Context, string, string) (net.Conn, error) {
				close(tcpStarted)
				client, server := net.Pipe()
				pipeServer(t, server, local, remote, 0)
				return client, nil
			},
			Clock:        clock,
			UTPHeadStart: time.Second,
		})
		done <- struct {
			result HandshakeResult
			err    error
		}{result: result, err: err}
	}()
	<-clock.created
	clock.Fire()
	select {
	case <-tcpStarted:
	case <-time.After(time.Second):
		t.Fatal("TCP did not start after the controlled head start")
	}
	out := <-done
	if out.err != nil {
		t.Fatalf("RaceEndpoint: %v", out.err)
	}
	if out.result.Transport != TransportTCP || out.result.Handshake.PeerID != remote.PeerID {
		t.Fatalf("race result = %+v, want valid TCP handshake", out.result)
	}
	_ = out.result.Conn.Close()
}

func TestDialManagerExpectedPeerIDMismatchDoesNotBlacklistAndCanBeRefreshed(t *testing.T) {
	local := testHandshake()
	actual := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{8}}
	falseID := [20]byte{9}
	endpoint := Endpoint{Addr: netip.MustParseAddr("192.0.2.9"), Port: 51413}
	now := time.Unix(100, 0)
	backoff := NewEndpointBackoff()
	clock := &fakeRaceClock{created: make(chan struct{})}
	manager, err := NewDialManager(DialManagerConfig{
		Race: RaceConfig{
			LocalHandshake: local,
			UTPDial:        handshakePipeDial(t, local, actual),
			TCPDial: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("TCP connection refused")
			},
			Clock:        clock,
			UTPHeadStart: time.Second,
		},
		Backoff: backoff,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewDialManager: %v", err)
	}

	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 2})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	if _, err := pool.Add(ResolvedCandidate{Endpoint: endpoint, ExpectedPeerID: falseID, HasExpectedID: true}); err != nil {
		t.Fatalf("add tracker candidate: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, raceErr := manager.Race(context.Background(), pool.Snapshot()[0])
		result <- raceErr
	}()
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		t.Fatal("race did not start its controlled uTP head-start timer")
	}
	clock.Fire()
	raceErr := <-result
	var raceFailure *RaceError
	if !errors.As(raceErr, &raceFailure) || len(raceFailure.Attempts) != 2 {
		t.Fatalf("mixed uTP/TCP failure = %v, want two attempts", raceErr)
	}
	if !errors.Is(raceErr, ErrExpectedPeerIDMismatch) || IsProtocolViolation(raceErr) {
		t.Fatalf("false tracker ID error = %v, want a non-violation mismatch", raceErr)
	}
	if backoff.IsBlacklisted(endpoint) || backoff.Ready(endpoint, now) {
		t.Fatalf("mismatch health: blacklisted=%t ready=%t, want bounded ordinary backoff", backoff.IsBlacklisted(endpoint), backoff.Ready(endpoint, now))
	}

	// A later tracker response without an ID clears the stale assertion. The
	// same resolved endpoint can then complete a normal TCP handshake.
	if _, err := pool.Admit(context.Background(), Candidate{Host: endpoint.Addr.String(), Port: endpoint.Port}); err != nil {
		t.Fatalf("refresh without expected ID: %v", err)
	}
	candidate := pool.Snapshot()[0]
	if candidate.HasExpectedID {
		t.Fatalf("refreshed candidate retained stale expected ID: %+v", candidate)
	}
	now = now.Add(time.Second)
	retryManager, err := NewDialManager(DialManagerConfig{
		Race:    RaceConfig{LocalHandshake: local, TCPDial: handshakePipeDial(t, local, actual)},
		Backoff: backoff,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewDialManager for retry: %v", err)
	}
	connected, err := retryManager.Race(context.Background(), candidate)
	if err != nil {
		t.Fatalf("retry after unpoisoned announcement: %v", err)
	}
	_ = connected.Conn.Close()
	if backoff.IsBlacklisted(endpoint) {
		t.Fatal("successful retry left endpoint blacklisted")
	}
}

func TestDialManagerCanAcceptWinnerAfterOtherTransportExpectedIDMismatch(t *testing.T) {
	local := testHandshake()
	expected := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{9}}
	actual := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{8}}
	endpoint := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 6881}
	clock := &fakeRaceClock{created: make(chan struct{})}
	manager, err := NewDialManager(DialManagerConfig{Race: RaceConfig{
		LocalHandshake: local,
		UTPDial:        handshakePipeDial(t, local, actual),
		TCPDial:        handshakePipeDial(t, local, expected),
		Clock:          clock,
		UTPHeadStart:   time.Second,
	}})
	if err != nil {
		t.Fatalf("NewDialManager: %v", err)
	}
	result := make(chan struct {
		peer HandshakeResult
		err  error
	}, 1)
	go func() {
		peer, raceErr := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint, ExpectedPeerID: expected.PeerID, HasExpectedID: true})
		result <- struct {
			peer HandshakeResult
			err  error
		}{peer: peer, err: raceErr}
	}()
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		t.Fatal("race did not start its controlled uTP head-start timer")
	}
	clock.Fire()
	out := <-result
	if out.err != nil || out.peer.Transport != TransportTCP || out.peer.Handshake.PeerID != expected.PeerID {
		t.Fatalf("TCP winner after uTP mismatch = %+v, %v", out.peer, out.err)
	}
	_ = out.peer.Conn.Close()
	if manager.backoff.IsBlacklisted(endpoint) {
		t.Fatal("non-winning expected-ID mismatch blacklisted endpoint")
	}
}

func TestDialManagerBlacklistsProtocolViolationsAndBacksOffOrdinaryFailures(t *testing.T) {
	now := time.Unix(100, 0)
	endpoint := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 6881}
	t.Run("invalid handshake is blacklisted", func(t *testing.T) {
		var calls atomic.Int32
		local := testHandshake()
		manager, err := NewDialManager(DialManagerConfig{
			Race: RaceConfig{
				LocalHandshake: local,
				TCPDial: func(context.Context, string, string) (net.Conn, error) {
					calls.Add(1)
					client, server := net.Pipe()
					go func() {
						defer server.Close()
						_, _ = ReadHandshake(server, nil, nil)
						wrong := Handshake{InfoHash: [20]byte{99}, PeerID: [20]byte{11}}
						_ = WriteHandshake(server, wrong.InfoHash, wrong.PeerID, wrong.Reserved)
					}()
					return client, nil
				},
			},
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewDialManager: %v", err)
		}
		if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint}); !IsProtocolViolation(err) {
			t.Fatalf("first Race error = %v, want protocol violation", err)
		}
		now = now.Add(24 * time.Hour)
		if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint}); err == nil || calls.Load() != 1 {
			t.Fatalf("Race after time advance = %v, calls=%d; endpoint should remain blacklisted", err, calls.Load())
		}
	})

	t.Run("transport failure uses ordinary backoff", func(t *testing.T) {
		var calls atomic.Int32
		manager, err := NewDialManager(DialManagerConfig{
			Race: RaceConfig{
				LocalHandshake: testHandshake(),
				TCPDial: func(context.Context, string, string) (net.Conn, error) {
					calls.Add(1)
					return nil, errors.New("connection refused")
				},
			},
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewDialManager: %v", err)
		}
		if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint}); err == nil {
			t.Fatal("Race unexpectedly succeeded")
		}
		if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint}); err == nil || calls.Load() != 1 {
			t.Fatalf("Race during backoff = %v, calls=%d", err, calls.Load())
		}
		now = now.Add(time.Second)
		if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: endpoint}); err == nil || calls.Load() != 2 {
			t.Fatalf("Race after backoff = %v, calls=%d; ordinary failure should be retryable", err, calls.Load())
		}
	})
}

func TestDialManagerBudgetAllowsRetriesAndCountsNormalizedEndpointsOnce(t *testing.T) {
	backoff, err := NewEndpointBackoffWithLimit(1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	var calls atomic.Int32
	manager, err := NewDialManager(DialManagerConfig{
		Race: RaceConfig{
			LocalHandshake: testHandshake(),
			TCPDial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, errors.New("fixture dial failure")
			},
		},
		Backoff: backoff,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	v4Mapped := Endpoint{Addr: netip.MustParseAddr("::ffff:127.0.0.1"), Port: 51413}
	ipv4 := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 51413}
	if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: v4Mapped}); err == nil {
		t.Fatal("first fixture race unexpectedly succeeded")
	}
	if len(backoff.attempted) != 1 {
		t.Fatalf("attempted endpoint count after first race = %d, want 1", len(backoff.attempted))
	}

	// Retry the same normalized endpoint after ordinary backoff expires. It
	// starts another transport race without consuming another unique slot.
	now = now.Add(time.Second)
	if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: ipv4}); err == nil {
		t.Fatal("retry fixture race unexpectedly succeeded")
	}
	if len(backoff.attempted) != 1 || calls.Load() != 2 {
		t.Fatalf("retry attempts/calls = %d/%d, want one identity and two races", len(backoff.attempted), calls.Load())
	}

	budgetEndpoint := Endpoint{Addr: netip.MustParseAddr("127.0.0.2"), Port: 51413}
	_, err = manager.Race(context.Background(), ResolvedCandidate{Endpoint: budgetEndpoint})
	var budgetErr *EndpointBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrEndpointBudget) || budgetErr.Limit != 1 {
		t.Fatalf("next distinct endpoint error = %v, want typed one-endpoint budget error", err)
	}
	if calls.Load() != 2 || len(backoff.attempted) != 1 {
		t.Fatalf("budget rejection started work: calls=%d attempted=%d", calls.Load(), len(backoff.attempted))
	}
}

func TestDialManagerEndpointBudgetIsSharedAcrossPhasesAndConcurrentRaces(t *testing.T) {
	const limit = 8
	backoff, err := NewEndpointBackoffWithLimit(limit)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	dial := func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("fixture dial failure")
	}
	newManager := func() *DialManager {
		manager, err := NewDialManager(DialManagerConfig{
			Race: RaceConfig{LocalHandshake: testHandshake(), TCPDial: dial}, Backoff: backoff,
		})
		if err != nil {
			t.Fatalf("NewDialManager: %v", err)
		}
		return manager
	}
	metadataManager := newManager()
	transferManager := newManager()

	type raceResult struct {
		err error
	}
	const total = 32
	results := make(chan raceResult, total)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			endpoint := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: uint16(10000 + index)}
			manager := metadataManager
			if index%2 != 0 {
				manager = transferManager
			}
			_, err := manager.Race(ctx, ResolvedCandidate{Endpoint: endpoint})
			results <- raceResult{err: err}
		}(i)
	}
	wg.Wait()
	close(results)
	budgetErrors := 0
	for result := range results {
		var budgetErr *EndpointBudgetError
		if errors.As(result.err, &budgetErr) {
			budgetErrors++
			if budgetErr != backoff.budgetErr {
				t.Fatalf("budget error pointer = %p, want shared error %p", budgetErr, backoff.budgetErr)
			}
		}
	}
	if got := len(backoff.attempted); got != limit {
		t.Fatalf("concurrent unique attempts = %d, want %d", got, limit)
	}
	if got := calls.Load(); got != limit {
		t.Fatalf("transport dials = %d, want %d; over-budget endpoints must not start", got, limit)
	}
	if budgetErrors != total-limit {
		t.Fatalf("budget errors = %d, want %d", budgetErrors, total-limit)
	}

	// A second manager standing in for the next session phase shares the same
	// backoff authority and cannot start a new endpoint after the global cap.
	_, err = transferManager.Race(ctx, ResolvedCandidate{Endpoint: Endpoint{Addr: netip.MustParseAddr("127.0.0.2"), Port: 51413}})
	var budgetErr *EndpointBudgetError
	if !errors.As(err, &budgetErr) || calls.Load() != limit {
		t.Fatalf("cross-phase race = %v, dials=%d; want shared fatal budget and no new dial", err, calls.Load())
	}
}

type closeSpy struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeSpy) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestPeerRegistryRetainsOlderPeerIDConnectionAndReleasesIt(t *testing.T) {
	registry, err := NewPeerRegistry(2)
	if err != nil {
		t.Fatalf("NewPeerRegistry: %v", err)
	}
	info := testHandshake().InfoHash
	id := [20]byte{7}
	firstClient, firstServer := net.Pipe()
	first := &closeSpy{Conn: firstClient}
	firstResult := HandshakeResult{Endpoint: Endpoint{Addr: netip.MustParseAddr("192.0.2.1"), Port: 1}, Conn: first, Handshake: Handshake{InfoHash: info, PeerID: id}}
	if _, err := registry.Admit(firstResult); err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	secondClient, secondServer := net.Pipe()
	second := &closeSpy{Conn: secondClient}
	secondResult := HandshakeResult{Endpoint: Endpoint{Addr: netip.MustParseAddr("192.0.2.2"), Port: 1}, Conn: second, Handshake: Handshake{InfoHash: info, PeerID: id}}
	if _, err := registry.Admit(secondResult); !errors.Is(err, ErrPeerIDCollision) || !second.closed.Load() || first.closed.Load() {
		t.Fatalf("collision err=%v secondClosed=%v firstClosed=%v", err, second.closed.Load(), first.closed.Load())
	}
	_ = firstServer.Close()
	_ = secondServer.Close()
	if !registry.Release(id, first) || registry.Len() != 0 {
		t.Fatalf("Release did not remove retained peer")
	}
	thirdClient, thirdServer := net.Pipe()
	third := &closeSpy{Conn: thirdClient}
	if _, err := registry.Admit(HandshakeResult{Endpoint: firstResult.Endpoint, Conn: third, Handshake: Handshake{InfoHash: info, PeerID: id}}); err != nil {
		t.Fatalf("reconnect Admit: %v", err)
	}
	_ = thirdServer.Close()
	_ = third.Close()
}
