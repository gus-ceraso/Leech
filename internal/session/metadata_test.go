package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"net"
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
}

func (f *metadataFixtureTracker) Announce(_ context.Context, trackerURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.urls = append(f.urls, trackerURL)
	f.mu.Unlock()
	return tracker.HTTPAnnounceResult{
		Interval:    time.Second,
		Transmitted: true,
		Peers:       []tracker.HTTPPeer{{Host: "127.0.0.1", Port: f.port}},
	}, nil
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
		message, err := peer.ReadMessage(conn)
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
