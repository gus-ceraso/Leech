package session

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestTrackerPeerUpdateQueueBoundsRetainedPeersAndBytes(t *testing.T) {
	t.Run("entry cap and newest IP", func(t *testing.T) {
		queue := newTrackerPeerUpdateQueue()
		peers := make([]tracker.TrackerPeer, trackerPeerQueueLimit)
		for i := range peers {
			address := netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)})
			peers[i] = tracker.TrackerPeer{Host: address.String(), Port: uint16(i%65535 + 1)}
		}
		queue.enqueuePeers(tracker.TransferPhase, peers)
		queue.enqueuePeers(tracker.TransferPhase, []tracker.TrackerPeer{{Host: "127.0.0.1", Port: 51413}})
		if got := queue.pendingPeers(); got != trackerPeerQueueLimit {
			t.Fatalf("retained peers = %d, want %d", got, trackerPeerQueueLimit)
		}
		item := queue.takePeer(false)
		if item == nil || item.peer.Host != "127.0.0.1" {
			t.Fatalf("first admitted IP = %+v, want later loopback endpoint", item)
		}
		queue.releasePeer(item)
	})

	t.Run("hostname bytes", func(t *testing.T) {
		queue := newTrackerPeerUpdateQueue()
		host := strings.Repeat("h", 512)
		peers := make([]tracker.TrackerPeer, trackerPeerQueueLimit)
		for i := range peers {
			peers[i] = tracker.TrackerPeer{Host: host, Port: uint16(i%65535 + 1)}
		}
		queue.enqueuePeers(tracker.MetadataPhase, peers)
		if queue.total > trackerPeerQueueLimit || queue.hostBytes > trackerPeerHostBytesLimit {
			t.Fatalf("retained peers/hostname bytes = %d/%d, limits %d/%d", queue.total, queue.hostBytes, trackerPeerQueueLimit, trackerPeerHostBytesLimit)
		}
		if queue.total != trackerPeerHostBytesLimit/len(host) {
			t.Fatalf("retained host peers = %d, want byte-budget bound %d", queue.total, trackerPeerHostBytesLimit/len(host))
		}
	})
}

func TestTrackerPeerUpdateQueuePreservesIPOrderWithinNewestUpdate(t *testing.T) {
	queue := newTrackerPeerUpdateQueue()
	queue.enqueuePeers(tracker.TransferPhase, []tracker.TrackerPeer{
		{Host: "127.0.0.1", Port: 51411},
		{Host: "127.0.0.1", Port: 51412},
	})
	queue.enqueuePeers(tracker.TransferPhase, []tracker.TrackerPeer{
		{Host: "127.0.0.1", Port: 51413},
		{Host: "127.0.0.1", Port: 51414},
	})

	for _, want := range []uint16{51413, 51414, 51411, 51412} {
		item := queue.takePeer(false)
		if item == nil || item.peer.Port != want {
			t.Fatalf("next IP = %+v, want port %d", item, want)
		}
		queue.releasePeer(item)
	}
}

func TestTrackerPeerUpdateQueuePreservesStatusWithoutPeerBackingSlice(t *testing.T) {
	queue := newTrackerPeerUpdateQueue()
	update := tracker.Update{
		Tracker: "http://fixture.test/announce", Phase: tracker.MetadataPhase,
		Request:  tracker.AnnounceRequest{Event: tracker.EventStarted, Left: 1},
		Peers:    []tracker.TrackerPeer{{Host: "127.0.0.1", Port: 51413}},
		Interval: time.Minute, Transmitted: true, Activated: true, Err: errors.New("fixture status"),
	}
	if err := queue.enqueue(update); err != nil {
		t.Fatal(err)
	}
	got := <-queue.events
	if got.Tracker != update.Tracker || got.Phase != update.Phase || got.Request != update.Request || got.Interval != update.Interval || !got.Transmitted || !got.Activated || got.Err == nil || got.Err.Error() != update.Err.Error() {
		t.Fatalf("status event = %+v, want accounting and error fields preserved", got)
	}
	if got.Peers != nil {
		t.Fatalf("status event retained %d peers", len(got.Peers))
	}
	for i := 0; i < metadataEventQueueSize; i++ {
		if err := queue.enqueue(tracker.Update{Phase: tracker.MetadataPhase, Request: update.Request}); err != nil {
			t.Fatalf("enqueue status event %d: %v", i, err)
		}
	}
	if err := queue.enqueue(update); !errors.Is(err, ErrTrackerUpdateQueue) {
		t.Fatalf("enqueue beyond status queue capacity = %v, want %v", err, ErrTrackerUpdateQueue)
	}
}

func TestTrackerPeerResolverPreservesSourceAcrossPhases(t *testing.T) {
	for _, phase := range []tracker.Phase{tracker.MetadataPhase, tracker.TransferPhase} {
		name := map[tracker.Phase]string{tracker.MetadataPhase: "metadata", tracker.TransferPhase: "transfer"}[phase]
		t.Run(name, func(t *testing.T) {
			queue := newTrackerPeerUpdateQueue()
			pool, err := peer.NewCandidatePool(peer.CandidatePoolConfig{MaxCandidates: 3})
			if err != nil {
				t.Fatalf("NewCandidatePool: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			admission := newTrackerPeerResolver(ctx, sec3SourceResolver{}, queue)
			t.Cleanup(func() {
				cancel()
				admission.close()
				queue.clear()
			})

			enqueue := func(source string, peers []tracker.TrackerPeer) {
				t.Helper()
				if err := queue.enqueue(tracker.Update{Tracker: source, Phase: phase, Peers: peers}); err != nil {
					t.Fatalf("enqueue %s update: %v", source, err)
				}
			}
			admitPeers := func(source string, peers []tracker.TrackerPeer) {
				t.Helper()
				enqueue(source, peers)
				admission.pump(ctx, phase, pool)
				if got := queue.pendingPeers(); got != 0 {
					t.Fatalf("pending peers after %s admission = %d, want 0", source, got)
				}
			}
			admit := func(source string, ports ...uint16) {
				t.Helper()
				peers := make([]tracker.TrackerPeer, len(ports))
				for i, port := range ports {
					peers[i] = tracker.TrackerPeer{Host: "127.0.0.1", Port: port}
				}
				admitPeers(source, peers)
			}
			byPort := func() map[uint16]peer.ResolvedCandidate {
				result := make(map[uint16]peer.ResolvedCandidate)
				for _, candidate := range pool.Snapshot() {
					result[candidate.Endpoint.Port] = candidate
				}
				return result
			}

			admit("tracker-A", 51431, 51432, 51433)
			for port, candidate := range byPort() {
				if candidate.Source != "tracker-A" {
					t.Fatalf("initial endpoint %d source = %q, want tracker-A", port, candidate.Source)
				}
			}
			admit("tracker-B", 51434)
			admit("tracker-A", 51431, 51432, 51433)
			candidates := byPort()
			if _, ok := candidates[51434]; !ok {
				t.Fatalf("tracker-B endpoint was evicted after tracker-A reannouncement: %+v", candidates)
			}
			admit("tracker-B", 51435)
			candidates = byPort()
			for _, port := range []uint16{51434, 51435} {
				candidate, ok := candidates[port]
				if !ok || candidate.Source != "tracker-B" {
					t.Fatalf("tracker-B endpoint %d = %+v, present=%t; want source-aware admission", port, candidate, ok)
				}
			}

			// Exercise the resolver-worker result path as well as direct IP
			// admission. Drain the enqueue signal, then wait for the worker's
			// post-result notification before pumping the resolved candidate.
			enqueue("tracker-C", []tracker.TrackerPeer{{Host: "source.test", Port: 51436}})
			select {
			case <-queue.notify: // discard the enqueue notification
			default:
			}
			admission.pump(ctx, phase, pool)
			select {
			case <-queue.notify:
			case <-ctx.Done():
				t.Fatal("resolver result was not signaled")
			case <-time.After(time.Second):
				t.Fatal("resolver result was not signaled")
			}
			admission.pump(ctx, phase, pool)
			if got := queue.pendingPeers(); got != 0 {
				t.Fatalf("pending peers after resolved tracker-C admission = %d, want 0", got)
			}
			candidates = byPort()
			if candidate, ok := candidates[51436]; !ok || candidate.Source != "tracker-C" {
				t.Fatalf("resolved tracker-C candidate = %+v, present=%t", candidate, ok)
			}
		})
	}
}

type sec3SourceResolver struct{}

func (sec3SourceResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if host != "source.test" {
		return nil, fmt.Errorf("unexpected lookup host %q", host)
	}
	return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
}

type sec3ControlledResolver struct {
	started chan string
	mu      sync.Mutex
	active  int
	maximum int
	lookups int
}

func newSEC3ControlledResolver() *sec3ControlledResolver {
	return &sec3ControlledResolver{started: make(chan string, 1024)}
}

func (r *sec3ControlledResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > trackerResolverTimeout+50*time.Millisecond {
		return nil, fmt.Errorf("lookup deadline %v, ok=%v", deadline, ok)
	}
	r.mu.Lock()
	r.active++
	r.lookups++
	if r.active > r.maximum {
		r.maximum = r.active
	}
	r.mu.Unlock()
	select {
	case r.started <- host:
	default:
	}
	<-ctx.Done()
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return nil, ctx.Err()
}

func (r *sec3ControlledResolver) snapshot() (active, maximum, lookups int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active, r.maximum, r.lookups
}

func waitSEC3ResolverHost(t *testing.T, resolver *sec3ControlledResolver, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := awaitSEC3ResolverHost(ctx, resolver, prefix); err != nil {
		t.Fatalf("resolver did not start a %q lookup: %v", prefix, err)
	}
}

func awaitSEC3ResolverHost(ctx context.Context, resolver *sec3ControlledResolver, prefix string) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case host := <-resolver.started:
			if strings.HasPrefix(host, prefix) {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timed out waiting for lookup")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func TestTrackerPeerResolverKeepsLaterIPSchedulableAndJoins(t *testing.T) {
	for _, phase := range []tracker.Phase{tracker.MetadataPhase, tracker.TransferPhase} {
		t.Run(map[tracker.Phase]string{tracker.MetadataPhase: "metadata", tracker.TransferPhase: "transfer"}[phase], func(t *testing.T) {
			queue := newTrackerPeerUpdateQueue()
			peers := make([]tracker.TrackerPeer, 24)
			for i := range peers {
				peers[i] = tracker.TrackerPeer{Host: fmt.Sprintf("slow-%d.test", i), Port: 51413}
			}
			for i := 0; i < metadataEventQueueSize; i++ {
				queue.events <- tracker.Update{Phase: phase}
			}
			queue.enqueuePeers(phase, peers)
			ctx, cancel := context.WithCancel(context.Background())
			resolver := newSEC3ControlledResolver()
			admission := newTrackerPeerResolver(ctx, resolver, queue)
			pool, err := peer.NewCandidatePool(peer.CandidatePoolConfig{})
			if err != nil {
				t.Fatal(err)
			}
			admission.pump(ctx, phase, pool)
			if got := len(queue.events); got != metadataEventQueueSize-trackerEventBatch {
				t.Fatalf("status events after one pump = %d, want bounded drain to %d", got, metadataEventQueueSize-trackerEventBatch)
			}
			waitSEC3ResolverHost(t, resolver, "slow-")
			queue.enqueuePeers(phase, []tracker.TrackerPeer{{Host: "127.0.0.1", Port: 51414}})
			admission.pump(ctx, phase, pool)
			candidates := pool.Snapshot()
			if len(candidates) != 1 || candidates[0].Endpoint.String() != "127.0.0.1:51414" {
				t.Fatalf("admitted candidates = %v, want later loopback endpoint while DNS is blocked", candidates)
			}
			cancel()
			admission.close()
			queue.clear()
			active, maximum, lookups := resolver.snapshot()
			if active != 0 {
				t.Fatalf("resolver workers still active after close: %d", active)
			}
			if maximum > trackerResolverWorkers || lookups == 0 {
				t.Fatalf("resolver maximum/lookups = %d/%d, worker limit %d", maximum, lookups, trackerResolverWorkers)
			}
			if got := queue.pendingPeers(); got != 0 {
				t.Fatalf("retained peers after resolver close = %d, want 0", got)
			}
		})
	}
}

type sec3PhaseTracker struct {
	metadataPort uint16
	transferPort uint16
	mu           sync.Mutex
	requests     []tracker.AnnounceRequest
}

func (f *sec3PhaseTracker) Announce(ctx context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.HTTPAnnounceResult{}, err
	}
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	if request.Event != tracker.EventStarted {
		return tracker.HTTPAnnounceResult{Interval: time.Minute, Transmitted: true}, nil
	}
	phase := "transfer"
	port := f.transferPort
	if request.Left == 1 {
		phase, port = "metadata", f.metadataPort
	}
	peers := make([]tracker.HTTPPeer, 33)
	for i := 0; i < 32; i++ {
		peers[i] = tracker.HTTPPeer{Host: fmt.Sprintf("%s-%d.test", phase, i), Port: 51413}
	}
	peers[32] = tracker.HTTPPeer{Host: "127.0.0.1", Port: port}
	return tracker.HTTPAnnounceResult{Interval: time.Minute, Transmitted: true, Peers: peers}, nil
}

func TestRunTrackerAdmissionLetsLaterLoopbackPeersWinBothPhases(t *testing.T) {
	data := []byte("later peer wins")
	info := sec3TestInfo(t, data)
	infoHash := torrent.InfoHash(sha1.Sum(info))
	metadataPort, transferPort := uint16(51420), uint16(51421)
	fixture := &sec3PhaseTracker{metadataPort: metadataPort, transferPort: transferPort}
	resolver := newSEC3ControlledResolver()
	transferDial, joined := h2Dialer(t, infoHash, transferPort, make(chan struct{}), func(conn net.Conn, _ int) error {
		block, err := h2Request(conn)
		if err != nil {
			return err
		}
		return h2Reply(conn, data, block)
	})
	tcpDial := func(ctx context.Context, network, address string) (net.Conn, error) {
		_, rawPort, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if rawPort == fmt.Sprint(metadataPort) {
			if err := awaitSEC3ResolverHost(ctx, resolver, "metadata-"); err != nil {
				return nil, err
			}
			client, server := net.Pipe()
			go serveMetadataPeer(t, server, infoHash, info, make(chan struct{}))
			return client, nil
		}
		if rawPort == fmt.Sprint(transferPort) {
			if err := awaitSEC3ResolverHost(ctx, resolver, "transfer-"); err != nil {
				return nil, err
			}
			return transferDial(ctx, network, address)
		}
		return nil, fmt.Errorf("unexpected peer endpoint %q", address)
	}
	trackerURL := "http://fixture.test/announce"
	source, err := torrent.ParseSource("magnet:?xt=urn:btih:" + hex.EncodeToString(infoHash[:]) + "&tr=" + trackerURL)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	output := filepath.Join(root, "out")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	result, err := Run(ctx, RunConfig{
		Source: source, OutputDir: output, HTTP: fixture, Resolver: resolver, TCPDial: tcpDial,
		UTPDial:      func(ctx context.Context, _, _ string) (net.Conn, error) { return nil, ctx.Err() },
		UTPHeadStart: time.Microsecond, Identity: tracker.Identity{PeerID: [20]byte{0x53}, Port: 49158},
		CacheRoot: filepath.Join(root, "cache"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	joined(t)
	if !result.TorrentComplete {
		t.Fatalf("result = %+v, want completed transfer", result)
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload")); err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v, want %q", got, err, data)
	}
	active, maximum, lookups := resolver.snapshot()
	if active != 0 {
		t.Fatalf("resolver workers still active after Run: %d", active)
	}
	if maximum > trackerResolverWorkers || lookups == 0 {
		t.Fatalf("resolver maximum/lookups = %d/%d, worker limit %d", maximum, lookups, trackerResolverWorkers)
	}
}

func sec3TestInfo(t *testing.T, data []byte) []byte {
	t.Helper()
	pieceHash := sha1.Sum(data)
	encoded, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieceHash[:]}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
