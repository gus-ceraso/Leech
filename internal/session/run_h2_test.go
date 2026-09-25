package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestRunReassignsDisconnectedBlockAndSendsFinalTrackerEvents(t *testing.T) {
	data := []byte("move")
	root := t.TempDir()
	infoHash, torrentPath := h2Torrent(t, root, data)
	output := filepath.Join(root, "out")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &h2Tracker{ports: []uint16{45101}}
	firstBlock, secondBlock := make(chan peer.Block, 1), make(chan peer.Block, 1)
	dial, joined := h2Dialer(t, infoHash, 45101, func(conn net.Conn, ordinal int) error {
		block, err := h2Request(conn)
		if err != nil {
			return err
		}
		if ordinal == 1 {
			select {
			case firstBlock <- block:
			default:
			}
			return nil
		}
		select {
		case secondBlock <- block:
		default:
		}
		return h2Reply(conn, data, block)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := Run(ctx, RunConfig{
		Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: output,
		HTTP: trackerFixture, TCPDial: dial,
		UTPDial:      func(ctx context.Context, _, _ string) (net.Conn, error) { return nil, ctx.Err() },
		UTPHeadStart: time.Microsecond, Identity: tracker.Identity{PeerID: [20]byte{0x48, 0x32}, Port: 49153},
		CacheRoot: filepath.Join(root, "cache"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	joined(t)
	if !result.TorrentComplete {
		t.Fatalf("result = %#v, want complete torrent", result)
	}
	if first, second := <-firstBlock, <-secondBlock; first != second {
		t.Fatalf("reassigned block %v differs from disconnected request %v", second, first)
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output = %q, %v; want %q", got, err, data)
	}
	assertH2Final(t, trackerFixture.snapshot())
}

type h2Tracker struct {
	mu       sync.Mutex
	ports    []uint16
	requests map[string][]tracker.AnnounceRequest
}

func (f *h2Tracker) Announce(ctx context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.HTTPAnnounceResult{}, err
	}
	f.mu.Lock()
	if f.requests == nil {
		f.requests = make(map[string][]tracker.AnnounceRequest)
	}
	f.requests[rawURL] = append(f.requests[rawURL], request)
	ports := append([]uint16(nil), f.ports...)
	f.mu.Unlock()
	peers := make([]tracker.HTTPPeer, len(ports))
	for i, port := range ports {
		peers[i] = tracker.HTTPPeer{Host: "127.0.0.1", Port: port}
	}
	return tracker.HTTPAnnounceResult{Interval: time.Minute, Transmitted: true, Peers: peers}, nil
}

func (f *h2Tracker) snapshot() map[string][]tracker.AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	copy := make(map[string][]tracker.AnnounceRequest, len(f.requests))
	for key, value := range f.requests {
		copy[key] = append([]tracker.AnnounceRequest(nil), value...)
	}
	return copy
}

type h2PeerScript func(net.Conn, int) error

func h2Dialer(t *testing.T, hash torrent.InfoHash, wantPort uint16, script h2PeerScript) (peer.DialFunc, func(*testing.T)) {
	t.Helper()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ordinal int
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		_, p, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return nil, err
		}
		port := uint16(n)
		mu.Lock()
		ordinal++
		attempt := ordinal
		wg.Add(1)
		mu.Unlock()
		if port != wantPort {
			wg.Done()
			return nil, fmt.Errorf("unexpected peer port %d, want %d", port, wantPort)
		}
		client, server := net.Pipe()
		go func() {
			defer wg.Done()
			defer server.Close()
			if _, err := peer.ReadHandshake(server, (*[20]byte)(&hash), nil); err != nil {
				return
			}
			if err := peer.WriteHandshake(server, [20]byte(hash), [20]byte{byte(port), byte(attempt)}, [8]byte{}); err != nil {
				return
			}
			if err := writeFixtureFrame(server, peer.BitfieldID, []byte{0x80}); err != nil {
				return
			}
			if err := writeFixtureFrame(server, peer.UnchokeID, nil); err != nil {
				return
			}
			if err := script(server, attempt); err != nil && !peer.IsDisconnect(err) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("peer script attempt %d: %v", attempt, err)
			}
		}()
		return client, nil
	}
	joined := func(t *testing.T) {
		t.Helper()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			return
		case <-time.After(2 * time.Second):
			t.Fatal("local peer workers were not joined")
		}
	}
	return dial, joined
}

func h2Request(conn net.Conn) (peer.Block, error) {
	for {
		message, err := peer.ReadMessage(conn)
		if err != nil {
			return peer.Block{}, err
		}
		if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID || message.ID == peer.CancelID {
			continue
		}
		if message.ID != peer.RequestID || len(message.Payload) != 12 {
			return peer.Block{}, fmt.Errorf("unexpected client message %#v", message)
		}
		return peer.Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:])}, nil
	}
}

func h2Reply(conn net.Conn, data []byte, block peer.Block) error {
	payload := make([]byte, 8+block.Length)
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	copy(payload[8:], data[block.Begin:block.Begin+block.Length])
	if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
		return err
	}
	for {
		message, err := peer.ReadMessage(conn)
		if err != nil {
			if peer.IsDisconnect(err) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			return err
		}
		if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID || message.ID == peer.CancelID {
			continue
		}
		return fmt.Errorf("unexpected client shutdown message %#v", message)
	}
}

func h2Torrent(t *testing.T, root string, data []byte) (torrent.InfoHash, string) {
	t.Helper()
	piece := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: piece[:]}},
	}}
	encoded, err := bencode.Encode(info)
	if err != nil {
		t.Fatal(err)
	}
	infoHash := torrent.InfoHash(sha1.Sum(encoded))
	metainfo, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("announce"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("http://h2.fixture/announce")}},
		{Key: []byte("info"), Value: info},
	}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(path, metainfo, 0o600); err != nil {
		t.Fatal(err)
	}
	return infoHash, path
}

func assertH2Final(t *testing.T, traces map[string][]tracker.AnnounceRequest) {
	t.Helper()
	for url, requests := range traces {
		var started, completed, stopped int
		for _, request := range requests {
			if request.Uploaded != 0 {
				t.Errorf("tracker %s uploaded=%d", url, request.Uploaded)
			}
			switch request.Event {
			case tracker.EventStarted:
				started++
			case tracker.EventCompleted:
				completed++
			case tracker.EventStopped:
				stopped++
			}
		}
		if started == 0 || completed != 1 || stopped != 1 {
			t.Errorf("tracker %s events=%v; want started/completed/stopped", url, requests)
		}
		if requests[len(requests)-1].Event != tracker.EventStopped {
			t.Errorf("tracker %s last event=%v, want stopped", url, requests[len(requests)-1].Event)
		}
	}
}
