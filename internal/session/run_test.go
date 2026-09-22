package session

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestRunKnownResumeCompletesBeforeTrackerActivity(t *testing.T) {
	data := []byte("resume me")
	pieces := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieces[:]}},
	}}
	torrentBytes, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, torrentBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := torrent.LoadMetainfo(torrentPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), RunConfig{Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root, Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NoTransferNeeded || !result.SelectionComplete || result.Metainfo.InfoHash != meta.InfoHash {
		t.Fatalf("unexpected run result: %#v", result)
	}
	got, err := os.ReadFile(filepath.Join(root, "payload"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("resume output = %q, %v", got, err)
	}
}

func TestRetainedAccountingExcludesUnselectedMixedPieceBytes(t *testing.T) {
	mapping := torrent.PiecePlan{
		Data: []torrent.FileRange{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}},
			{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 6}},
		},
		Selected: []torrent.FileRange{{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}}},
	}
	if got := realPieceBytes(mapping); got != 3 {
		t.Fatalf("retained mixed-piece bytes = %d, want 3", got)
	}
}

func TestFullSelectionRequiresZeroLengthRegularFiles(t *testing.T) {
	meta := torrent.Metainfo{
		TotalLength: 1,
		PieceLength: 1,
		Files: []torrent.File{
			{Index: 0, Path: "empty", Kind: torrent.RegularFile},
			{Index: 1, Path: "data", Range: torrent.ByteRange{Begin: 0, End: 1}, Kind: torrent.RegularFile},
		},
		Pieces: []torrent.Piece{{Index: 0, Range: torrent.ByteRange{End: 1}}},
	}
	selected, err := torrent.Select(meta, []string{"data"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fullSelection(meta, selected) {
		t.Fatal("selection omitting zero-length regular file reported full torrent")
	}
}

func TestRunTrackerTraceIncludesReceivedPayload(t *testing.T) {
	data := []byte("tracker payload")
	pieceHash := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieceHash[:]}},
	}}
	torrentBytes, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, torrentBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := torrent.LoadMetainfo(torrentPath)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fixture := &metadataFixtureTracker{port: uint16(listener.Addr().(*net.TCPAddr).Port)}
	peerDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			peerDone <- acceptErr
			return
		}
		defer conn.Close()
		var infoHash [20]byte
		copy(infoHash[:], meta.InfoHash[:])
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			peerDone <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{9, 8, 7}, [8]byte{}); err != nil {
			peerDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			peerDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			peerDone <- err
			return
		}
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				peerDone <- err
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID {
				continue
			}
			if message.ID != peer.RequestID || len(message.Payload) != 12 {
				peerDone <- fmt.Errorf("peer message = %#v", message)
				return
			}
			blockLength := binary.BigEndian.Uint32(message.Payload[8:])
			if blockLength != uint32(len(data)) || binary.BigEndian.Uint32(message.Payload[:4]) != 0 || binary.BigEndian.Uint32(message.Payload[4:8]) != 0 {
				peerDone <- fmt.Errorf("request payload = %x", message.Payload)
				return
			}
			piecePayload := make([]byte, 8+len(data))
			copy(piecePayload[8:], data)
			if err := writeFixtureFrame(conn, peer.PieceID, piecePayload); err != nil {
				peerDone <- err
				return
			}
			peerDone <- nil
			return
		}
	}()

	outputRoot := filepath.Join(root, "out")
	if err := os.Mkdir(outputRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := Run(ctx, RunConfig{
		Source:    torrent.Source{Kind: torrent.SourcePath, Path: torrentPath},
		OutputDir: outputRoot,
		HTTP:      fixture,
		UTPDial: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		TCPDial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
		CacheRoot: filepath.Join(root, "cache"),
	})
	if err != nil {
		t.Fatalf("Run: %v, requests=%#v", err, fixture.snapshot())
	}
	if !result.TorrentComplete {
		t.Fatalf("result = %#v, want complete torrent", result)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(outputRoot, "payload"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("output = %q, want %q", got, data)
	}
	requests := fixture.snapshot()
	if len(requests) < 3 {
		t.Fatalf("tracker requests = %d, want started/completed/stopped", len(requests))
	}
	if requests[0].Event != tracker.EventStarted {
		t.Fatalf("first tracker event = %v, want started", requests[0].Event)
	}
	for _, request := range requests {
		if request.Uploaded != 0 {
			t.Fatalf("uploaded = %d, want zero", request.Uploaded)
		}
	}
	var sawDownloaded bool
	for _, request := range requests[1:] {
		if request.Downloaded >= int64(len(data)) {
			sawDownloaded = true
			break
		}
	}
	if !sawDownloaded {
		t.Fatalf("tracker requests after payload = %#v", requests)
	}
}
