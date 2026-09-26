package session

import (
	"context"
	"crypto/sha1"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type endpointBudgetTracker struct {
	mu            sync.Mutex
	requests      []tracker.AnnounceRequest
	metadataPeers []tracker.HTTPPeer
	transferPeers []tracker.HTTPPeer
}

func (f *endpointBudgetTracker) Announce(_ context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	peers := f.metadataPeers
	if request.Left != 1 {
		peers = f.transferPeers
	}
	peers = append([]tracker.HTTPPeer(nil), peers...)
	f.mu.Unlock()
	return tracker.HTTPAnnounceResult{Interval: time.Second, Transmitted: true, Peers: peers}, nil
}

func (f *endpointBudgetTracker) snapshot() []tracker.AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tracker.AnnounceRequest(nil), f.requests...)
}

func TestMetadataEndpointBudgetStopsAfterFirstDistinctAttempt(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	fixture := &endpointBudgetTracker{metadataPeers: []tracker.HTTPPeer{
		{Host: "127.0.0.1", Port: 51431},
		{Host: "127.0.0.1", Port: 51432},
	}}
	backoff, err := peer.NewEndpointBackoffWithLimit(1)
	if err != nil {
		t.Fatal(err)
	}
	var dialCalls atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = DiscoverMetadata(ctx, MetadataConfig{
		InfoHash: infoHash, Trackers: []string{"http://fixture.test/announce"},
		HTTP: fixture, Backoff: backoff,
		Identity: tracker.Identity{PeerID: [20]byte{31}, Port: 49160},
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			dialCalls.Add(1)
			return nil, errors.New("fixture endpoint is unreachable")
		},
	})
	budgetErr, ok := err.(*peer.EndpointBudgetError)
	if !ok || budgetErr.Limit != 1 {
		t.Fatalf("metadata discovery error = %v, want typed one-endpoint budget error", err)
	}
	if dialCalls.Load() != 1 {
		t.Fatalf("metadata transport dials = %d, want only the first endpoint attempted", dialCalls.Load())
	}
	requests := fixture.snapshot()
	started, stopped := 0, 0
	for _, request := range requests {
		if request.Event == tracker.EventStarted {
			started++
		}
		if request.Event == tracker.EventStopped {
			stopped++
		}
	}
	if started == 0 || stopped == 0 {
		t.Fatalf("metadata tracker events = %d started, %d stopped; want orderly finalization", started, stopped)
	}
}

func TestRunEndpointBudgetPropagatesAcrossPhasesAndCleansWorkspace(t *testing.T) {
	payload := []byte("abc")
	pieceHash := sha1.Sum(payload)
	infoValue := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(payload))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(payload))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieceHash[:]}},
	}}
	info, err := bencode.Encode(infoValue)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha1.Sum(info)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metadataEndpoint := listener.Addr().(*net.TCPAddr)
	metadataPort := uint16(metadataEndpoint.Port)
	transferPort := metadataPort
	fixture := &endpointBudgetTracker{
		metadataPeers: []tracker.HTTPPeer{{Host: "127.0.0.1", Port: metadataPort}},
		transferPeers: []tracker.HTTPPeer{{Host: "127.0.0.2", Port: transferPort}},
	}
	identity := tracker.Identity{PeerID: [20]byte{32}, Port: 49161}
	updates := make(chan tracker.Update, 128)
	trackerSet, err := tracker.NewTrackerSet(tracker.TrackerSetConfig{
		InfoHash: [20]byte(infoHash), Trackers: []string{"http://fixture.test/announce"},
		Identity: identity, HTTP: fixture,
		Snapshot: func(context.Context) (tracker.Snapshot, error) {
			return tracker.Snapshot{Total: int64(len(payload))}, nil
		},
		OnUpdate: func(update tracker.Update) {
			select {
			case updates <- update:
			default:
			}
		},
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = listener.Close()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		if err := trackerSet.Close(closeCtx); err != nil {
			t.Errorf("close tracker set: %v", err)
		}
	}()

	serverDone := make(chan struct{})
	acceptDone := make(chan struct{})
	accepted := make(chan net.Conn, 1)
	go func() {
		defer close(acceptDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(serverDone)
			return
		}
		accepted <- conn
		serveMetadataPeer(t, conn, infoHash, info, serverDone)
		_ = listener.Close()
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
		select {
		case <-acceptDone:
		case <-time.After(time.Second):
			t.Error("metadata accept fixture did not join during cleanup")
		}
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("metadata peer fixture did not join during cleanup")
		}
	})

	backoff, err := peer.NewEndpointBackoffWithLimit(1)
	if err != nil {
		t.Fatal(err)
	}
	var dialMu sync.Mutex
	var dialed []string
	var metadataDials atomic.Int32
	tcpDial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialMu.Lock()
		dialed = append(dialed, address)
		dialMu.Unlock()
		if address == net.JoinHostPort("127.0.0.1", strconv.Itoa(int(metadataPort))) && metadataDials.Add(1) > 1 {
			return nil, errors.New("metadata fixture accepts one connection")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	root := t.TempDir()
	cacheRoot := filepath.Join(root, "cache")
	outputRoot := filepath.Join(root, "out")
	if err := os.Mkdir(outputRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Run(ctx, RunConfig{
		Source:    torrent.Source{Kind: torrent.SourceInfoHash, InfoHash: infoHash},
		OutputDir: outputRoot, CacheRoot: cacheRoot,
		Identity: identity, TCPDial: tcpDial,
		UTPDial: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("fixture uTP unavailable")
		}, UTPHeadStart: time.Millisecond,
		TrackerSet: trackerSet, Updates: updates, backoff: backoff,
	})
	var budgetErr *peer.EndpointBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Limit != 1 || !errors.Is(err, peer.ErrEndpointBudget) {
		t.Fatalf("Run error = %v, want primary typed endpoint-budget error", err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer fixture did not join")
	}
	select {
	case <-acceptDone:
	case <-time.After(time.Second):
		t.Fatal("metadata accept fixture did not join")
	}
	dialMu.Lock()
	gotDialed := append([]string(nil), dialed...)
	dialMu.Unlock()
	if len(gotDialed) == 0 {
		t.Fatal("metadata endpoint was never raced")
	}
	for _, address := range gotDialed {
		if address == net.JoinHostPort("127.0.0.2", strconv.Itoa(int(transferPort))) {
			t.Fatalf("over-budget transfer endpoint started a transport dial: %v", gotDialed)
		}
	}
	entries, err := os.ReadDir(cacheRoot)
	if err != nil {
		t.Fatalf("read cache root after Run: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache workspaces remain after budget failure: %v", entries)
	}
	started, stopped := 0, 0
	for _, request := range fixture.snapshot() {
		if request.Event == tracker.EventStarted {
			started++
		}
		if request.Event == tracker.EventStopped {
			stopped++
		}
	}
	if started != 2 || stopped != 2 {
		t.Fatalf("tracker events = %d started, %d stopped; want orderly finalization of both phases", started, stopped)
	}
}
