package session

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
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
		OnProgress: func(progress RunProgress) {
			if progress.VerifiedSelectedBytes != int64(len(data)) || progress.ActivePeers != 1 || progress.RecentRateBytesPerSec == 0 {
				t.Errorf("progress = %#v, want verified bytes, one peer, and recent payload rate", progress)
			}
		},
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

type noPeersTracker struct{}

func (noPeersTracker) Announce(_ context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventStopped {
		return tracker.HTTPAnnounceResult{Transmitted: true}, errors.New("stopped fixture failure")
	}
	return tracker.HTTPAnnounceResult{Interval: time.Second, Transmitted: true}, nil
}

func TestRunNoProgressTimeoutRemainsPrimaryShutdownError(t *testing.T) {
	data := []byte("timeout")
	hash := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: hash[:]}},
	}}
	encoded, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	secondary := make(chan error, 1)
	_, err = Run(context.Background(), RunConfig{
		Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root,
		HTTP: noPeersTracker{}, CacheRoot: filepath.Join(root, "cache"), Timeout: 60 * time.Millisecond,
		OnSecondary: func(err error) { secondary <- err },
	})
	if !errors.Is(err, ErrNoProgressTimeout) {
		t.Fatalf("Run error = %v, want no-progress timeout", err)
	}
	select {
	case secondaryErr := <-secondary:
		if secondaryErr == nil || !strings.Contains(secondaryErr.Error(), "stopped fixture failure") {
			t.Fatalf("secondary error = %v, want stopped-event failure", secondaryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("tracker final-event failure was not reported as secondary")
	}
}

func TestRunNoProgressTimeoutReportsStagedCloseFailure(t *testing.T) {
	data := []byte("withheld payload")
	hash := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: hash[:]}},
	}}
	encoded, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, encoded, 0o600); err != nil {
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
	trackerFixture := &metadataFixtureTracker{port: uint16(listener.Addr().(*net.TCPAddr).Port)}
	peerRequested := make(chan struct{})
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
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{7, 6, 5}, [8]byte{}); err != nil {
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
				peerDone <- fmt.Errorf("peer message = %#v, want request", message)
				return
			}
			break
		}
		close(peerRequested)
		_, readErr := peer.ReadMessage(conn) // Withhold the requested block until shutdown.
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, net.ErrClosed) && !strings.Contains(readErr.Error(), "use of closed network connection") {
			peerDone <- readErr
			return
		}
		peerDone <- nil
	}()

	outputRoot := filepath.Join(root, "out")
	if err := os.Mkdir(outputRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected staged close failure")
	opened := make(chan struct{})
	closed := 0
	var closeMu sync.Mutex
	secondary := make(chan error, 1)
	runDone := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), RunConfig{
			Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: outputRoot,
			HTTP: trackerFixture, TCPDial: (&net.Dialer{}).DialContext,
			UTPDial:   func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() },
			CacheRoot: filepath.Join(root, "cache"), Timeout: 500 * time.Millisecond,
			StageFileOpener: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
				file, err := os.OpenFile(path, flag, mode)
				if err != nil {
					return nil, err
				}
				close(opened)
				return runCloseFailureFile{File: file, err: closeErr, closes: &closed, mu: &closeMu}, nil
			},
			OnSecondary: func(err error) { secondary <- err },
		})
		runDone <- runErr
	}()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("stage was not opened")
	}
	select {
	case <-peerRequested:
	case <-time.After(time.Second):
		t.Fatal("peer did not receive requested block")
	}
	select {
	case err = <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish after no-progress timeout")
	}
	if !errors.Is(err, ErrNoProgressTimeout) {
		t.Fatalf("Run error = %v, want no-progress timeout", err)
	}
	select {
	case secondaryErr := <-secondary:
		if !errors.Is(secondaryErr, closeErr) {
			t.Fatalf("secondary error = %v, want staged close failure", secondaryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("staged close failure was not reported as secondary")
	}
	closeMu.Lock()
	gotCloses := closed
	closeMu.Unlock()
	if gotCloses != 1 {
		t.Errorf("stage close count = %d, want 1", gotCloses)
	}
	if err := <-peerDone; err != nil {
		t.Errorf("peer fixture: %v", err)
	}
}

type runCloseFailureFile struct {
	*os.File
	err    error
	closes *int
	mu     *sync.Mutex
}

func (f runCloseFailureFile) Close() error {
	f.mu.Lock()
	(*f.closes)++
	f.mu.Unlock()
	_ = f.File.Close()
	return f.err
}

func TestPayloadRateUsesOnlyRecentBoundedWindow(t *testing.T) {
	rate := newPayloadRate()
	start := time.Unix(100, 0)
	rate.add(start, 1000)
	rate.add(start.Add(time.Second), 1500)
	if got := rate.perSecond(start.Add(time.Second)); got != 500 {
		t.Fatalf("rate = %d, want 500 B/s across five-second window", got)
	}
	if got := rate.perSecond(start.Add(6 * time.Second)); got != 0 {
		t.Fatalf("stale rate = %d, want 0", got)
	}
}

func TestRunMetadataPhaseStopsBeforeSelectionAndOutputAccess(t *testing.T) {
	info := largeTestInfo(t)
	digest := sha1.Sum(info)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	fixture := &metadataFixtureTracker{port: 51419}
	serverDone := make(chan struct{})
	tcpDial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveMetadataPeer(t, server, infoHash, info, serverDone)
		return client, nil
	}
	utpDial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	source, err := torrent.ParseSource("magnet:?xt=urn:btih:" + hex.EncodeToString(infoHash[:]) + "&tr=http://fixture.test/announce")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	output := filepath.Join(root, "missing-output")
	cache := filepath.Join(root, "missing-cache")
	_, err = Run(context.Background(), RunConfig{
		Source: source, OutputDir: output, Patterns: []string{"no-such-file"}, HTTP: fixture,
		TCPDial: tcpDial, UTPDial: utpDial, UTPHeadStart: time.Millisecond,
		CacheRoot: cache, Identity: tracker.Identity{PeerID: [20]byte{8}, Port: 49157},
	})
	if err == nil || !strings.Contains(err.Error(), "matches no regular files") {
		t.Fatalf("Run error = %v, want selection failure after metadata", err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer remained active across selection")
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
		t.Fatalf("metadata tracker events = started %d, stopped %d; want both trackers finalized before selection", started, stopped)
	}
	for _, path := range []string{output, cache} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			t.Fatalf("pre-selection path %q was touched: %v", path, statErr)
		}
	}
}

func TestRunRemoteListingStopsMetadataWorkersWithoutStorageAccess(t *testing.T) {
	info := testInfo(t)
	digest := sha1.Sum(info)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	fixture := &metadataFixtureTracker{port: 51420}
	serverDone := make(chan struct{})
	tcpDial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveMetadataPeer(t, server, infoHash, info, serverDone)
		return client, nil
	}
	utpDial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	source, err := torrent.ParseSource("magnet:?xt=urn:btih:" + hex.EncodeToString(infoHash[:]))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	output := filepath.Join(root, "missing-output")
	cache := filepath.Join(root, "missing-cache")
	result, err := Run(context.Background(), RunConfig{
		Source: source, OutputDir: output, ListFiles: true, HTTP: fixture,
		TCPDial: tcpDial, UTPDial: utpDial, UTPHeadStart: time.Millisecond,
		CacheRoot: cache, Identity: tracker.Identity{PeerID: [20]byte{7}, Port: 49158},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Listed) != 1 || result.Listed[0] != "fixture" || result.Selection != nil {
		t.Fatalf("listing result = %#v, want metadata paths only", result)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("metadata peer remained active after listing")
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
	if started != 1 || stopped != 1 {
		t.Fatalf("remote listing tracker events = started %d, stopped %d", started, stopped)
	}
	for _, path := range []string{output, cache} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			t.Fatalf("listing touched %q: %v", path, statErr)
		}
	}
}

type countingTracker struct {
	mu       sync.Mutex
	requests []tracker.AnnounceRequest
}

func (f *countingTracker) Announce(_ context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	return tracker.HTTPAnnounceResult{Interval: time.Second, Transmitted: true}, nil
}

func (f *countingTracker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func TestRunResumeTruncatesValidatedOverlongFileWithoutTrackerActivity(t *testing.T) {
	data := []byte("valid prefix")
	hash := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: hash[:]}},
	}}
	encoded, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), append(append([]byte(nil), data...), []byte(" excess")...), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture := &countingTracker{}
	result, err := Run(context.Background(), RunConfig{Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root, Resume: true, HTTP: fixture})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NoTransferNeeded {
		t.Fatalf("result = %#v, want complete resume", result)
	}
	got, err := os.ReadFile(filepath.Join(root, "payload"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("resumed payload = %q, %v; want validated prefix", got, err)
	}
	if fixture.count() != 0 {
		t.Fatalf("tracker activity occurred during complete resume: %d calls", fixture.count())
	}
}

func TestRunCreatesSelectedZeroLengthOutputWithoutTransfer(t *testing.T) {
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: 0}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("empty")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: 16 << 10}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes}},
	}}
	encoded, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "empty.torrent")
	if err := os.WriteFile(torrentPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	fixture := &countingTracker{}
	result, err := Run(context.Background(), RunConfig{Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root, HTTP: fixture})
	if err != nil {
		t.Fatal(err)
	}
	infoOut, err := os.Stat(filepath.Join(root, "empty"))
	if err != nil || infoOut.Size() != 0 {
		t.Fatalf("zero-length output stat = %v, %v", infoOut, err)
	}
	if !result.SelectionComplete || !result.TorrentComplete || fixture.count() != 0 {
		t.Fatalf("zero-length result = %#v, tracker calls = %d", result, fixture.count())
	}
}
