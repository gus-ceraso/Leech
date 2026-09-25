package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type metadataFixtureTracker struct {
	mu       sync.Mutex
	requests []tracker.AnnounceRequest
	urls     []string
	port     uint16
	ports    []uint16
}

type metadataUnansweredTracker struct {
	mu       sync.Mutex
	requests []tracker.AnnounceRequest
	started  chan struct{}
	once     sync.Once
}

type metadataFinalFailureTracker struct {
	fixture *metadataFixtureTracker
}

func (f metadataFinalFailureTracker) Announce(ctx context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	result, err := f.fixture.Announce(ctx, rawURL, request)
	if request.Event == tracker.EventStopped {
		return result, errors.New("stopped fixture failure")
	}
	return result, err
}

func (f *metadataUnansweredTracker) Announce(ctx context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	if request.Event == tracker.EventStarted {
		f.once.Do(func() { close(f.started) })
		<-ctx.Done()
		return tracker.HTTPAnnounceResult{Transmitted: true}, ctx.Err()
	}
	return tracker.HTTPAnnounceResult{Transmitted: true, Interval: time.Second}, nil
}

func (f *metadataUnansweredTracker) snapshot() []tracker.AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tracker.AnnounceRequest(nil), f.requests...)
}

type metadataJoinTracker struct {
	trackerLoopDone chan struct{}
	started         chan struct{}
	joined          sync.Once
	startedOnce     sync.Once
}

type metadataCloseConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func (c *metadataCloseConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return err
}

func (f *metadataJoinTracker) Announce(ctx context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventStarted {
		f.startedOnce.Do(func() { close(f.started) })
		<-ctx.Done()
		f.joined.Do(func() { close(f.trackerLoopDone) })
		return tracker.HTTPAnnounceResult{Transmitted: true}, ctx.Err()
	}
	if request.Event == tracker.EventStopped {
		select {
		case <-f.trackerLoopDone:
		default:
			return tracker.HTTPAnnounceResult{}, errors.New("stopped began before tracker loop joined")
		}
		return tracker.HTTPAnnounceResult{Transmitted: true, Interval: time.Second}, nil
	}
	return tracker.HTTPAnnounceResult{}, errors.New("unexpected regular metadata announce")
}

func TestNewMetadataDiscoveryAcceptsZeroInfoHash(t *testing.T) {
	if _, err := NewMetadataDiscovery(MetadataConfig{}); err != nil {
		t.Fatalf("zero info hash rejected: %v", err)
	}
}

func (f *metadataFixtureTracker) Announce(_ context.Context, trackerURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.urls = append(f.urls, trackerURL)
	f.mu.Unlock()
	f.mu.Lock()
	ports := append([]uint16(nil), f.ports...)
	if len(ports) == 0 {
		ports = []uint16{f.port}
	}
	f.mu.Unlock()
	peers := make([]tracker.HTTPPeer, 0, len(ports))
	for _, port := range ports {
		peers = append(peers, tracker.HTTPPeer{Host: "127.0.0.1", Port: port})
	}
	return tracker.HTTPAnnounceResult{Interval: time.Second, Transmitted: true, Peers: peers}, nil
}

func (f *metadataFixtureTracker) snapshot() []tracker.AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tracker.AnnounceRequest(nil), f.requests...)
}

func (f *metadataFixtureTracker) snapshotURLs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.urls...)
}

func TestMetadataDiscoveryObtainsAllBlocksAndFinalizesPhase(t *testing.T) {
	info := largeTestInfo(t)
	hash := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], hash[:])

	serverDone := make(chan struct{})
	var dialMu sync.Mutex
	var dialCount int
	fixture := &metadataFixtureTracker{}
	fixture.port = 51413
	dial := func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		client, server := net.Pipe()
		dialMu.Lock()
		dialCount++
		dialMu.Unlock()
		go serveMetadataPeer(t, server, expected, info, serverDone)
		return client, nil
	}

	result, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash:    expected,
		Trackers:    []string{"http://fixture.test/announce"},
		HTTP:        fixture,
		TCPDial:     dial,
		PeerTimeout: time.Second,
		Identity:    tracker.Identity{PeerID: [20]byte{1}, Port: 49152},
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	if result.Metainfo.InfoHash != expected || result.Metainfo.Name != "fixture" {
		t.Fatalf("metainfo = %+v", result.Metainfo)
	}
	if len(result.Metainfo.Trackers) != 2 || result.Metainfo.Trackers[0] != torrent.DefaultTracker {
		t.Fatalf("trackers = %v", result.Metainfo.Trackers)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer did not finish")
	}
	dialMu.Lock()
	if dialCount != 1 {
		t.Fatalf("dial count = %d, want one deduplicated endpoint", dialCount)
	}
	dialMu.Unlock()

	requests := fixture.snapshot()
	if len(requests) < 4 {
		t.Fatalf("tracker requests = %d, want started/stopped for both trackers", len(requests))
	}
	started, stopped := 0, 0
	for _, request := range requests {
		if request.Left != 1 || request.Uploaded != 0 || request.InfoHash != [20]byte(expected) {
			t.Fatalf("premetadata accounting = %+v", request)
		}
		switch request.Event {
		case tracker.EventStarted:
			started++
		case tracker.EventStopped:
			stopped++
		case tracker.EventCompleted:
			t.Fatal("metadata phase sent completed")
		}
	}
	if started != 2 || stopped != 2 {
		t.Fatalf("started/stopped = %d/%d, want 2/2", started, stopped)
	}
	urls := fixture.snapshotURLs()
	seenDefault, seenFixture := false, false
	for _, url := range urls {
		seenDefault = seenDefault || url == torrent.DefaultTracker
		seenFixture = seenFixture || url == "http://fixture.test/announce"
	}
	if !seenDefault || !seenFixture {
		t.Fatalf("tracker URLs = %v, want mandatory default and fixture", urls)
	}
}

func TestRunReportsMetadataFinalEventFailureAsSecondary(t *testing.T) {
	info := largeTestInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	serverDone := make(chan struct{})
	fixture := &metadataFixtureTracker{port: 51414}
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		go serveMetadataPeer(t, server, expected, info, serverDone)
		return client, nil
	}
	secondary := make(chan error, 1)
	result, err := Run(context.Background(), RunConfig{
		Source:    torrent.Source{Kind: torrent.SourceInfoHash, InfoHash: expected, Trackers: []string{"http://fixture.test/announce"}},
		ListFiles: true, HTTP: metadataFinalFailureTracker{fixture: fixture}, TCPDial: dial,
		Identity:    tracker.Identity{PeerID: [20]byte{18}, Port: 49160},
		OnSecondary: func(err error) { secondary <- err },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Metainfo.InfoHash != expected {
		t.Fatalf("metainfo hash = %x, want %x", result.Metainfo.InfoHash, expected)
	}
	select {
	case err := <-secondary:
		if err == nil || !strings.Contains(err.Error(), "stopped fixture failure") {
			t.Fatalf("secondary error = %v, want stopped announce failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("final stopped announce failure was not reported")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer did not finish")
	}
	started, stopped := 0, 0
	for _, request := range fixture.snapshot() {
		switch request.Event {
		case tracker.EventStarted:
			started++
		case tracker.EventStopped:
			stopped++
		case tracker.EventCompleted:
			t.Fatal("metadata phase sent completed")
		}
	}
	if started == 0 || stopped == 0 {
		t.Fatalf("tracker events = %d started, %d stopped; want both", started, stopped)
	}
}

func TestMetadataDiscoveryRejectsInvalidCompleteCandidateAndReportsStrike(t *testing.T) {
	info := testInfo(t)
	hash := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], hash[:])
	bad := append([]byte(nil), info...)
	bad[len(bad)-1] ^= 1

	fixture := &metadataFixtureTracker{port: 51414}
	var strikes []EndpointStrike
	var strikeMu sync.Mutex
	dial := func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveMetadataPeer(t, server, expected, bad, make(chan struct{}))
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err := DiscoverMetadata(ctx, MetadataConfig{
		InfoHash:    expected,
		HTTP:        fixture,
		TCPDial:     dial,
		PeerTimeout: 20 * time.Millisecond,
		Identity:    tracker.Identity{PeerID: [20]byte{2}, Port: 49153},
		OnStrike: func(endpoint peer.Endpoint, count uint8) {
			strikeMu.Lock()
			strikes = append(strikes, EndpointStrike{Endpoint: endpoint, Strikes: count})
			strikeMu.Unlock()
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
	strikeMu.Lock()
	defer strikeMu.Unlock()
	if len(strikes) != 1 || strikes[0].Strikes != 1 {
		t.Fatalf("strikes = %v, want one sole-supplier strike", strikes)
	}
}

func TestFetchMetadataRejectUsesLocalExtensionID(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	serverDone := make(chan error, 1)
	go func() {
		defer server.Close()
		if _, err := peer.ReadMessage(server); err != nil {
			serverDone <- err
			return
		}
		if err := writeTestFrame(server, extensionHandshakeFrame(7, 1)); err != nil {
			serverDone <- err
			return
		}
		if _, err := readMetadataPeerMessage(server, 7); err != nil {
			serverDone <- err
			return
		}
		serverDone <- writeTestFrame(server, metadataControlFrame(1, peer.MetadataReject, 0))
	}()
	_, err := fetchMetadata(context.Background(), client, time.Second)
	if !errors.Is(err, ErrMetadataRejected) {
		t.Fatalf("metadata reject error = %v, want ErrMetadataRejected", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestMetadataDiscoveryRotatesRefusalAndAcceptsRepeatedExtensionID(t *testing.T) {
	info := largeTestInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	fixture := &metadataFixtureTracker{ports: []uint16{51415, 51416}}
	dial := func(ctx context.Context, _ string, address string) (net.Conn, error) {
		client, server := net.Pipe()
		if strings.HasSuffix(address, ":51415") {
			go serveRefusingMetadataPeer(t, server, expected)
		} else {
			go serveMetadataPeerWithIDChange(t, server, expected, info)
		}
		return client, nil
	}
	result, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash:    expected,
		HTTP:        fixture,
		TCPDial:     dial,
		Identity:    tracker.Identity{PeerID: [20]byte{3}, Port: 49154},
		PeerTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	if result.Metainfo.InfoHash != expected {
		t.Fatalf("info hash = %x, want %x", result.Metainfo.InfoHash, expected)
	}
}

func TestMetadataDiscoveryCancellationClosesSilentPeer(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	serverDone := make(chan struct{})
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveSilentMetadataPeer(t, server, expected, serverDone)
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := DiscoverMetadata(ctx, MetadataConfig{
		InfoHash:    expected,
		Peers:       []torrent.PeerAddress{{Host: "127.0.0.1", Port: 51417}},
		HTTP:        &metadataFixtureTracker{},
		TCPDial:     dial,
		PeerTimeout: time.Minute,
		Identity:    tracker.Identity{PeerID: [20]byte{4}, Port: 49155},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("silent metadata peer was not joined")
	}
}

func TestMetadataDiscoveryCancellationUnblocksMetadataWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := fetchMetadata(ctx, client, time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}

func TestMetadataWrongAdvertisedSizeRotatesWithoutStrikeOrMixedBlocks(t *testing.T) {
	info := largeTestInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	fixture := &metadataFixtureTracker{ports: []uint16{51419, 51420}}
	wrongPeerDone := make(chan struct{})
	goodPeerDone := make(chan struct{})
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		client, server := net.Pipe()
		if strings.HasSuffix(address, ":51419") {
			go serveWrongSizeMetadataPeer(t, server, expected, info, wrongPeerDone)
		} else {
			go serveMetadataPeer(t, server, expected, info, goodPeerDone)
		}
		return client, nil
	}
	var strikes []EndpointStrike
	result, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash: expected, HTTP: fixture, TCPDial: dial,
		Identity:    tracker.Identity{PeerID: [20]byte{14}, Port: 49157},
		PeerTimeout: time.Second,
		OnStrike: func(endpoint peer.Endpoint, count uint8) {
			strikes = append(strikes, EndpointStrike{Endpoint: endpoint, Strikes: count})
		},
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	if result.Metainfo.InfoHash != expected {
		t.Fatalf("info hash = %x, want %x", result.Metainfo.InfoHash, expected)
	}
	if len(result.Strikes) != 0 || len(strikes) != 0 {
		t.Fatalf("wrong advertised size caused a corruption strike: result=%v callback=%v", result.Strikes, strikes)
	}
	for name, done := range map[string]<-chan struct{}{"wrong-size peer": wrongPeerDone, "complete supplier": goodPeerDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
}

func TestMetadataRefusalUsesOrdinaryBackoffThenRotates(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	fixture := &metadataFixtureTracker{ports: []uint16{51421, 51422}}
	backoff := peer.NewEndpointBackoff()
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		client, server := net.Pipe()
		if strings.HasSuffix(address, ":51421") {
			go serveRefusingMetadataPeer(t, server, expected)
		} else {
			go serveMetadataPeer(t, server, expected, info, make(chan struct{}))
		}
		return client, nil
	}
	result, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash: expected, HTTP: fixture, TCPDial: dial, Backoff: backoff,
		Identity:    tracker.Identity{PeerID: [20]byte{15}, Port: 49158},
		PeerTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	if result.Metainfo.InfoHash != expected {
		t.Fatalf("info hash = %x, want %x", result.Metainfo.InfoHash, expected)
	}
	refused := peer.Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 51421}
	if backoff.IsBlacklisted(refused) {
		t.Fatal("ordinary metadata refusal blacklisted the endpoint")
	}
	if backoff.Ready(refused, time.Now()) {
		t.Fatal("ordinary metadata refusal did not set a retry delay")
	}
	if len(result.Strikes) != 0 {
		t.Fatalf("ordinary metadata refusal caused a corruption strike: %v", result.Strikes)
	}
}

func TestMetadataUnansweredStartedTrackerGetsStoppedWithoutLaterRegularAnnounce(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	started := make(chan struct{})
	fixture := &metadataUnansweredTracker{started: started}
	peerDone := make(chan struct{})
	dial := func(_ context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer close(peerDone)
			<-started
			serveMetadataPeer(t, server, expected, info, make(chan struct{}))
		}()
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := DiscoverMetadata(ctx, MetadataConfig{
		InfoHash: expected,
		Peers:    []torrent.PeerAddress{{Host: "127.0.0.1", Port: 51423}},
		HTTP:     fixture, TCPDial: dial,
		Identity:    tracker.Identity{PeerID: [20]byte{16}, Port: 49159},
		PeerTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	requests := fixture.snapshot()
	if len(requests) != 2 || requests[0].Event != tracker.EventStarted || requests[1].Event != tracker.EventStopped {
		t.Fatalf("tracker events = %v, want transmitted started then stopped", requests)
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer was not closed before return")
	}
}

func TestMetadataFetcherIgnoresIncomingCorePayloadRequest(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	resultCh := make(chan string, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, server := net.Pipe()
	go serveMetadataPeerWithCoreRequest(t, server, expected, info, resultCh)
	local := peer.Handshake{InfoHash: [20]byte(expected), PeerID: [20]byte{17}, Reserved: [8]byte{5: 0x10}}
	if err := peer.WriteHandshake(client, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadHandshake(client, &local.InfoHash, nil); err != nil {
		t.Fatalf("read fixture handshake: %v", err)
	}
	got, err := fetchMetadata(ctx, client, time.Second)
	if err != nil {
		client.Close()
		t.Fatalf("fetchMetadata: %v", err)
	}
	if !bytes.Equal(got, info) {
		client.Close()
		t.Fatal("metadata bytes changed after the core payload request")
	}
	_ = client.Close()
	select {
	case got := <-resultCh:
		if got != "closed without piece" {
			t.Fatalf("incoming core request result = %q, want no piece response", got)
		}
	case <-time.After(time.Second):
		t.Fatal("payload request fixture did not finish")
	}
}

func TestMetadataDiscoveryReturnsAfterPeerAndTrackerLoopsJoin(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	peerConnClosed := make(chan struct{})
	trackerLoopDone := make(chan struct{})
	fixture := &metadataJoinTracker{trackerLoopDone: trackerLoopDone, started: make(chan struct{})}
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			<-fixture.started
			serveMetadataPeer(t, server, expected, info, make(chan struct{}))
		}()
		return &metadataCloseConn{Conn: client, done: peerConnClosed}, nil
	}
	_, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash: expected,
		Peers:    []torrent.PeerAddress{{Host: "127.0.0.1", Port: 51425}},
		HTTP:     fixture, TCPDial: dial,
		Identity:    tracker.Identity{PeerID: [20]byte{18}, Port: 49161},
		PeerTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	select {
	case <-peerConnClosed:
	default:
		t.Fatal("discovery returned before closing its metadata peer connection")
	}
	select {
	case <-trackerLoopDone:
	default:
		t.Fatal("discovery returned before the regular tracker loop exited")
	}
}

func TestMetadataDiscoveryRejectsIncomingMetadataRequest(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var expected torrent.InfoHash
	copy(expected[:], digest[:])
	seenReject := make(chan bool, 1)
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveNoUploadMetadataPeer(t, server, expected, info, seenReject)
		return client, nil
	}
	result, err := DiscoverMetadata(context.Background(), MetadataConfig{
		InfoHash: expected,
		Peers:    []torrent.PeerAddress{{Host: "127.0.0.1", Port: 51418}},
		HTTP:     &metadataFixtureTracker{}, TCPDial: dial,
		Identity: tracker.Identity{PeerID: [20]byte{5}, Port: 49156},
	})
	if err != nil {
		t.Fatalf("DiscoverMetadata: %v", err)
	}
	if result.Metainfo.InfoHash != expected {
		t.Fatalf("info hash = %x, want %x", result.Metainfo.InfoHash, expected)
	}
	select {
	case rejected := <-seenReject:
		if !rejected {
			t.Fatal("incoming metadata request was not rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("metadata peer did not report request response")
	}
}

func serveMetadataPeer(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, metadata []byte, done chan struct{}) {
	t.Helper()
	defer close(done)
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{9}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		return
	}
	total := int64(len(metadata))
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, total)); err != nil {
		return
	}
	for {
		message, err := readMetadataPeerMessage(conn, 7)
		if err != nil {
			return
		}
		if message.KeepAlive || message.ID != peer.ExtendedID || len(message.Payload) == 0 {
			continue
		}
		request, err := peer.ParseMetadataControl(message.Payload[1:])
		if err != nil || request.Type != peer.MetadataRequest {
			return
		}
		begin := int(request.Piece) * 16 << 10
		if begin >= len(metadata) {
			return
		}
		end := begin + 16<<10
		if end > len(metadata) {
			end = len(metadata)
		}
		if err := writeTestFrame(conn, metadataDataFrame(1, request.Piece, total, metadata[begin:end])); err != nil {
			return
		}
		if end == len(metadata) {
			return
		}
	}
}

func readMetadataPeerMessage(conn net.Conn, remoteID byte) (peer.Message, error) {
	return peer.ReadMessageWithOptions(conn, peer.ReadOptions{MetadataExtensionID: remoteID})
}

func serveRefusingMetadataPeer(t *testing.T, conn net.Conn, infoHash torrent.InfoHash) {
	t.Helper()
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{10}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		return
	}
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, 1)); err != nil {
		return
	}
	_, _ = readMetadataPeerMessage(conn, 7)
	_ = writeTestFrame(conn, metadataControlFrame(1, peer.MetadataReject, 0))
}

func serveWrongSizeMetadataPeer(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, metadata []byte, done chan struct{}) {
	t.Helper()
	defer close(done)
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{19}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		return
	}
	wrongSize := int64(len(metadata) - 1)
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, wrongSize)); err != nil {
		return
	}
	request, err := readMetadataPeerMessage(conn, 7)
	if err != nil || request.ID != peer.ExtendedID || len(request.Payload) == 0 {
		return
	}
	control, err := peer.ParseMetadataControl(request.Payload[1:])
	if err != nil || control.Type != peer.MetadataRequest || control.Piece != 0 {
		return
	}
	// The advertised size is wrong, but the first block is otherwise valid.
	// A second supplier must restart at piece zero instead of combining blocks.
	_ = writeTestFrame(conn, metadataDataFrame(1, 0, wrongSize, metadata[:16<<10]))
}

func serveMetadataPeerWithCoreRequest(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, metadata []byte, result chan string) {
	t.Helper()
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{20}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		t.Logf("payload fixture: read handshake: %v", err)
		result <- "read handshake failed"
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		t.Logf("payload fixture: write handshake: %v", err)
		result <- "write handshake failed"
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil { // local extension handshake
		t.Logf("payload fixture: read local extension handshake: %v", err)
		result <- "read extension handshake failed"
		return
	}
	// Ask for torrent payload while discovery is waiting on this connection.
	// The metadata phase has no output/cache handle and must not serve file bytes.
	coreRequest := make([]byte, 12)
	binary.BigEndian.PutUint32(coreRequest[8:], 16<<10)
	if err := writeFixtureFrame(conn, peer.RequestID, coreRequest); err != nil {
		t.Logf("payload fixture: write core request: %v", err)
		result <- "write core request failed"
		return
	}
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, int64(len(metadata)))); err != nil {
		t.Logf("payload fixture: write remote extension handshake: %v", err)
		result <- "write extension handshake failed"
		return
	}
	request, err := readMetadataPeerMessage(conn, 7)
	if err != nil || request.ID != peer.ExtendedID || len(request.Payload) == 0 {
		t.Logf("payload fixture: read metadata request: message=%+v err=%v", request, err)
		result <- "read metadata request failed"
		return
	}
	control, err := peer.ParseMetadataControl(request.Payload[1:])
	if err != nil || control.Type != peer.MetadataRequest || control.Piece != 0 {
		t.Logf("payload fixture: parse metadata request: control=%+v err=%v", control, err)
		result <- "parse metadata request failed"
		return
	}
	if err := writeTestFrame(conn, metadataDataFrame(1, 0, int64(len(metadata)), metadata)); err != nil {
		t.Logf("payload fixture: write metadata data: %v", err)
		result <- "write metadata response failed"
		return
	}
	// The caller closes the connection after accepting metadata. Any message
	// after the expected extension handshake and metadata request would be a
	// response to the core request, including a piece payload.
	message, err := peer.ReadMessage(conn)
	resultValue := "closed without piece"
	if err == nil && message.ID == peer.PieceID {
		resultValue = "piece response"
	} else if err == nil {
		resultValue = "other peer message"
	}
	select {
	case result <- resultValue:
	default:
	}
}

func serveMetadataPeerWithIDChange(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, metadata []byte) {
	t.Helper()
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{11}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		return
	}
	total := int64(len(metadata))
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, total)); err != nil {
		return
	}
	remoteMetadataID := byte(7)
	for piece := uint32(0); ; piece++ {
		message, err := readMetadataPeerMessage(conn, remoteMetadataID)
		if err != nil || message.ID != peer.ExtendedID || len(message.Payload) == 0 {
			return
		}
		request, err := peer.ParseMetadataControl(message.Payload[1:])
		if err != nil || request.Type != peer.MetadataRequest || request.Piece != piece {
			return
		}
		if piece == 0 {
			if err := writeTestFrame(conn, extensionHandshakeFrame(9, total)); err != nil {
				return
			}
			remoteMetadataID = 9
		}
		begin := int(piece) * 16 << 10
		end := begin + 16<<10
		if end > len(metadata) {
			end = len(metadata)
		}
		if err := writeTestFrame(conn, metadataDataFrame(1, piece, total, metadata[begin:end])); err != nil {
			return
		}
		if end == len(metadata) {
			return
		}
	}
}

func serveSilentMetadataPeer(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, done chan struct{}) {
	t.Helper()
	defer close(done)
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{12}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		return
	}
	_, _ = peer.ReadMessage(conn)
	_, _ = peer.ReadMessage(conn)
}

func serveNoUploadMetadataPeer(t *testing.T, conn net.Conn, infoHash torrent.InfoHash, metadata []byte, result chan bool) {
	t.Helper()
	defer conn.Close()
	remote := peer.Handshake{InfoHash: [20]byte(infoHash), PeerID: [20]byte{13}, Reserved: [8]byte{5: 0x10}}
	if _, err := peer.ReadHandshake(conn, &remote.InfoHash, nil); err != nil {
		result <- false
		return
	}
	if err := peer.WriteHandshake(conn, remote.InfoHash, remote.PeerID, remote.Reserved); err != nil {
		result <- false
		return
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		result <- false
		return
	}
	total := int64(len(metadata))
	if err := writeTestFrame(conn, extensionHandshakeFrame(7, total)); err != nil {
		result <- false
		return
	}
	message, err := readMetadataPeerMessage(conn, 7)
	if err != nil || message.ID != peer.ExtendedID || len(message.Payload) == 0 {
		result <- false
		return
	}
	request, err := peer.ParseMetadataControl(message.Payload[1:])
	if err != nil || request.Type != peer.MetadataRequest {
		result <- false
		return
	}
	if err := writeTestFrame(conn, metadataControlFrame(1, peer.MetadataRequest, 0)); err != nil {
		result <- false
		return
	}
	message, err = readMetadataPeerMessage(conn, 7)
	if err != nil || message.ID != peer.ExtendedID || len(message.Payload) == 0 {
		result <- false
		return
	}
	control, err := peer.ParseMetadataControl(message.Payload[1:])
	if err != nil || control.Type != peer.MetadataReject {
		result <- false
		return
	}
	result <- writeTestFrame(conn, metadataDataFrame(1, 0, total, metadata)) == nil
}

func largeTestInfo(t *testing.T) []byte {
	t.Helper()
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: 0}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("fixture")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: 16 << 10}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes}},
		{Key: []byte("x"), Value: bencode.Value{Type: bencode.Bytes, Bytes: bytes.Repeat([]byte{'x'}, 20<<10)}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testInfo(t *testing.T) []byte {
	t.Helper()
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: 0}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("fixture")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: 16 << 10}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func extensionHandshakeFrame(id byte, total int64) []byte {
	body, _ := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("m"), Value: bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("ut_metadata"), Value: bencode.Value{Type: bencode.Integer, Int: int64(id)}}}}},
		{Key: []byte("metadata_size"), Value: bencode.Value{Type: bencode.Integer, Int: total}},
	}})
	return extensionFrame(0, body)
}

func metadataDataFrame(id byte, piece uint32, total int64, block []byte) []byte {
	header, _ := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("msg_type"), Value: bencode.Value{Type: bencode.Integer, Int: 1}},
		{Key: []byte("piece"), Value: bencode.Value{Type: bencode.Integer, Int: int64(piece)}},
		{Key: []byte("total_size"), Value: bencode.Value{Type: bencode.Integer, Int: total}},
	}})
	return extensionFrame(id, append(header, block...))
}

func metadataControlFrame(id byte, typ peer.MetadataMessageType, piece uint32) []byte {
	body, _ := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("msg_type"), Value: bencode.Value{Type: bencode.Integer, Int: int64(typ)}},
		{Key: []byte("piece"), Value: bencode.Value{Type: bencode.Integer, Int: int64(piece)}},
	}})
	return extensionFrame(id, body)
}

func extensionFrame(id byte, body []byte) []byte {
	frame := make([]byte, 6+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(2+len(body)))
	frame[4] = peer.ExtendedID
	frame[5] = id
	copy(frame[6:], body)
	return frame
}

func writeTestFrame(conn net.Conn, frame []byte) error {
	for len(frame) != 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}
