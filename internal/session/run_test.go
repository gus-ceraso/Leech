package session

import (
	"bytes"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestTrackerWarningsCoalesceRecoverAndKeepFinalFailuresSecondary(t *testing.T) {
	var warnings []string
	var diagnostics []Diagnostic
	coordinator := &coordinator{config: RunConfig{
		OnWarning:    func(message string) { warnings = append(warnings, message) },
		OnDiagnostic: func(event Diagnostic) { diagnostics = append(diagnostics, event) },
	}}
	endpoint := "https://user:password@example.test/private?token=secret"
	failure := &tracker.HTTPError{Code: tracker.HTTPErrorTimeout, Err: errors.New("timeout")}
	coordinator.observeTracker(tracker.Update{Tracker: endpoint, Phase: tracker.TransferPhase, Request: tracker.AnnounceRequest{Event: tracker.EventStarted}, Attempted: true, Transmitted: true, Err: failure})
	coordinator.observeTracker(tracker.Update{Tracker: endpoint, Phase: tracker.TransferPhase, Request: tracker.AnnounceRequest{Event: tracker.EventNone}, Attempted: true, Err: failure})
	coordinator.observeTracker(tracker.Update{Tracker: endpoint, Phase: tracker.TransferPhase, Request: tracker.AnnounceRequest{Event: tracker.EventNone}, Attempted: true, Activated: true})
	coordinator.observeTracker(tracker.Update{Tracker: endpoint, Phase: tracker.TransferPhase, Request: tracker.AnnounceRequest{Event: tracker.EventStopped}, Attempted: true, Transmitted: true, Err: failure})
	if len(warnings) != 2 || !strings.Contains(warnings[0], "retrying") || !strings.Contains(warnings[1], "recovered") {
		t.Fatalf("coalesced failure/recovery warnings = %q", warnings)
	}
	if strings.Contains(strings.Join(warnings, " "), "password") || strings.Contains(strings.Join(warnings, " "), "token=secret") {
		t.Fatalf("tracker warning leaked URL credentials: %q", warnings)
	}
	if len(diagnostics) != 4 || !strings.Contains(diagnostics[3].Detail, "stopped attempted, transmitted, response failed, final event") || strings.Contains(diagnostics[3].Detail, "retrying") {
		t.Fatalf("tracker attempt diagnostics = %+v", diagnostics)
	}
}

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
	var statuses atomic.Int32
	_, err = Run(context.Background(), RunConfig{
		Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root,
		HTTP: noPeersTracker{}, CacheRoot: filepath.Join(root, "cache"), Timeout: 1400 * time.Millisecond,
		OnStatus:    func(RunProgress) { statuses.Add(1) },
		OnSecondary: func(err error) { secondary <- err },
	})
	if !errors.Is(err, ErrNoProgressTimeout) {
		t.Fatalf("Run error = %v, want no-progress timeout", err)
	}
	if statuses.Load() == 0 {
		t.Fatal("status refresh did not occur before the no-progress timeout")
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

func TestRunNoProgressDeadlineOnlyRenewsForVerifiedPiece(t *testing.T) {
	for _, tc := range []struct {
		name        string
		commitPiece bool
	}{
		{name: "status connection and payload do not renew"},
		{name: "verified piece renews", commitPiece: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pieceLength := 32 << 10
			data := bytes.Repeat([]byte{'x'}, 64<<10)
			if tc.commitPiece {
				pieceLength = 4
				data = []byte("goodnext")
			}
			path, trackerFixture, requested, payloadSent, deliver, peerDone := runTimeoutPeerFixture(t, data, pieceLength, tc.commitPiece)
			clock := &manualRunClock{changed: make(chan struct{}, 1)}
			timeout := 10 * time.Second
			progress := make(chan RunProgress, 2)
			statuses := make(chan RunProgress, 2)
			dialStarted := make(chan struct{}, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runDone := make(chan error, 1)
			runJoined, peerJoined := false, false
			defer func() {
				cancel()
				if !runJoined {
					select {
					case <-runDone:
					case <-time.After(3 * time.Second):
						t.Error("Run goroutine did not join")
					}
				}
				if !peerJoined {
					select {
					case <-peerDone:
					case <-time.After(3 * time.Second):
						t.Error("peer fixture did not join")
					}
				}
			}()
			go func() {
				_, err := Run(ctx, RunConfig{
					Source: torrent.Source{Kind: torrent.SourcePath, Path: path}, OutputDir: t.TempDir(),
					HTTP: trackerFixture, TCPDial: func(ctx context.Context, network, address string) (net.Conn, error) {
						select {
						case dialStarted <- struct{}{}:
						default:
						}
						return (&net.Dialer{}).DialContext(ctx, network, address)
					},
					UTPDial: func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() },
					Timeout: timeout, Now: clock.Now, newTimeoutTimer: clock.NewTimer,
					OnProgress: func(p RunProgress) { progress <- p },
					OnStatus:   func(p RunProgress) { statuses <- p },
				})
				runDone <- err
			}()
			timer := clock.timerReady(t)
			waitTestSignal(t, dialStarted, "peer dial")
			select {
			case <-requested:
			case err := <-runDone:
				runJoined = true
				t.Fatalf("Run ended before peer request: %v", err)
			case <-time.After(3 * time.Second):
				select {
				case peerErr := <-peerDone:
					t.Fatalf("peer fixture ended before request: %v; tracker=%+v", peerErr, trackerFixture.snapshot())
				default:
					t.Fatalf("timed out waiting for peer request; tracker=%+v", trackerFixture.snapshot())
				}
			}
			if !tc.commitPiece && timer.Resets() != 0 {
				t.Fatalf("peer admission reset timeout %d times", timer.Resets())
			}
			if tc.commitPiece {
				clock.Advance(timeout - time.Nanosecond)
				close(deliver)
				committed := waitTestValue(t, progress, "verified-piece callback")
				if committed.VerifiedSelectedBytes != 4 {
					t.Fatalf("verified progress = %+v, want first 4-byte piece", committed)
				}
				if timer.Resets() != 1 {
					t.Fatalf("timeout resets = %d, want one reset for committed piece", timer.Resets())
				}
				clock.Advance(2 * time.Nanosecond) // Past the original deadline, before the renewed deadline.
				select {
				case err := <-runDone:
					runJoined = true
					t.Fatalf("Run completed before renewed deadline: %v", err)
				default:
				}
				clock.Advance(timeout)
			} else {
				waitTestSignal(t, payloadSent, "partial payload")
				if timer.Resets() != 0 {
					t.Fatalf("payload receipt reset timeout %d times", timer.Resets())
				}
				status := waitTestValue(t, statuses, "live status tick")
				if status.ActivePeers != 1 || status.VerifiedSelectedBytes != 0 || status.RecentRateBytesPerSec == 0 {
					t.Fatalf("partial-transfer status = %+v", status)
				}
				if timer.Resets() != 0 {
					t.Fatalf("status, connection, or payload reset timeout %d times", timer.Resets())
				}
				clock.Advance(timeout)
			}
			select {
			case err := <-runDone:
				runJoined = true
				if !errors.Is(err, ErrNoProgressTimeout) {
					t.Fatalf("Run error = %v, want no-progress timeout", err)
				}
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("Run did not observe the controlled no-progress deadline")
			}
			cancel()
			select {
			case err := <-peerDone:
				peerJoined = true
				if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
					t.Errorf("peer fixture: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("peer fixture did not join")
			}
		})
	}
}

func runTimeoutPeerFixture(t *testing.T, data []byte, pieceLength int, commitFirst bool) (string, *metadataFixtureTracker, <-chan struct{}, <-chan struct{}, chan struct{}, <-chan error) {
	t.Helper()
	pieces := make([]byte, 0, len(data)/pieceLength*sha1.Size)
	for begin := 0; begin < len(data); begin += pieceLength {
		hash := sha1.Sum(data[begin : begin+pieceLength])
		pieces = append(pieces, hash[:]...)
	}
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(pieceLength)}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieces}},
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
	t.Cleanup(func() { _ = listener.Close() })
	fixture := &metadataFixtureTracker{port: uint16(listener.Addr().(*net.TCPAddr).Port)}
	requested, payloadSent, deliver := make(chan struct{}), make(chan struct{}), make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			peerDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		var infoHash [20]byte
		copy(infoHash[:], meta.InfoHash[:])
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			peerDone <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{0x71}, [8]byte{}); err != nil {
			peerDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0xc0}); err != nil {
			peerDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			peerDone <- err
			return
		}
		firstPiece := uint32(^uint32(0))
		responded := false
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
				peerDone <- fmt.Errorf("peer request message = %#v", message)
				return
			}
			index := binary.BigEndian.Uint32(message.Payload[:4])
			begin := binary.BigEndian.Uint32(message.Payload[4:8])
			length := binary.BigEndian.Uint32(message.Payload[8:])
			if firstPiece == ^uint32(0) {
				firstPiece = index
				close(requested)
			}
			if responded || (commitFirst && index != firstPiece) {
				continue
			}
			if !commitFirst {
				responded = true
			} else {
				<-deliver
				responded = true
			}
			start := int(index)*pieceLength + int(begin)
			end := start + int(length)
			if end > len(data) {
				peerDone <- fmt.Errorf("requested range [%d,%d) outside %d bytes", start, end, len(data))
				return
			}
			payload := make([]byte, 8+int(length))
			binary.BigEndian.PutUint32(payload[:4], index)
			binary.BigEndian.PutUint32(payload[4:8], begin)
			copy(payload[8:], data[start:end])
			if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
				peerDone <- err
				return
			}
			if !commitFirst {
				close(payloadSent)
			}
		}
	}()
	return torrentPath, fixture, requested, payloadSent, deliver, peerDone
}

type manualRunClock struct {
	mu      sync.Mutex
	now     time.Duration
	timer   *manualRunTimer
	changed chan struct{}
}

func (c *manualRunClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Unix(0, int64(c.now))
}

func (c *manualRunClock) NewTimer(duration time.Duration) timeoutTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualRunTimer{clock: c, deadline: c.now + duration, active: true, ticks: make(chan time.Time, 1)}
	c.timer = timer
	select {
	case c.changed <- struct{}{}:
	default:
	}
	return timer
}

func (c *manualRunClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now += duration
	if timer := c.timer; timer != nil && timer.active && c.now >= timer.deadline {
		timer.active = false
		timer.ticks <- time.Unix(0, int64(c.now))
	}
	c.mu.Unlock()
}

func (c *manualRunClock) timerReady(t *testing.T) *manualRunTimer {
	t.Helper()
	select {
	case <-c.changed:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout timer was not created")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timer
}

type manualRunTimer struct {
	clock    *manualRunClock
	deadline time.Duration
	active   bool
	resets   int
	ticks    chan time.Time
}

func (t *manualRunTimer) channel() <-chan time.Time { return t.ticks }

func (t *manualRunTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *manualRunTimer) Reset(duration time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = true
	t.deadline = t.clock.now + duration
	t.resets++
	return wasActive
}

func (t *manualRunTimer) Resets() int {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	return t.resets
}

func waitTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitTestValue(t *testing.T, values <-chan RunProgress, description string) RunProgress {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return RunProgress{}
	}
}

func TestRunNoProgressTimeoutReportsStagedCloseFailure(t *testing.T) {
	secondary := make(chan error, 1)
	err, closes, closeErr := runNoProgressTimeoutWithStagedCloseFailure(t, func(err error) { secondary <- err })
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
	if closes != 1 {
		t.Errorf("stage close count = %d, want 1", closes)
	}
}

func TestRunNoProgressTimeoutWithNilSecondaryCallback(t *testing.T) {
	err, closes, _ := runNoProgressTimeoutWithStagedCloseFailure(t, nil)
	if !errors.Is(err, ErrNoProgressTimeout) {
		t.Fatalf("Run error = %v, want no-progress timeout", err)
	}
	if closes != 1 {
		t.Errorf("stage close count = %d, want 1", closes)
	}
}

func runNoProgressTimeoutWithStagedCloseFailure(t *testing.T, onSecondary func(error)) (error, int, error) {
	t.Helper()
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
			OnSecondary: onSecondary,
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
	closeMu.Lock()
	gotCloses := closed
	closeMu.Unlock()
	if err := <-peerDone; err != nil {
		t.Errorf("peer fixture: %v", err)
	}
	return err, gotCloses, closeErr
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
