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
