package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type pressureFailTracker struct {
	mu   sync.Mutex
	seen map[string]int
}

func (f *pressureFailTracker) Announce(ctx context.Context, rawURL string, _ tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.HTTPAnnounceResult{}, err
	}
	f.mu.Lock()
	f.seen[rawURL]++
	f.mu.Unlock()
	return tracker.HTTPAnnounceResult{Transmitted: true}, errors.New("controlled local tracker failure")
}

func (f *pressureFailTracker) snapshot() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.seen))
	for url, attempts := range f.seen {
		out[url] = attempts
	}
	return out
}

type blockedDebugWriter struct {
	mu          sync.Mutex
	output      bytes.Buffer
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	active      atomic.Int32
}

func newBlockedDebugWriter() *blockedDebugWriter {
	return &blockedDebugWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockedDebugWriter) Write(p []byte) (int, error) {
	w.active.Add(1)
	defer w.active.Add(-1)
	if bytes.HasPrefix(p, []byte("debug:")) {
		w.once.Do(func() {
			close(w.entered)
			<-w.release
		})
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.Write(p)
}

func (w *blockedDebugWriter) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func (w *blockedDebugWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

type blockedWarningWriter struct {
	mu          sync.Mutex
	output      bytes.Buffer
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	active      atomic.Int32
}

func newBlockedWarningWriter() *blockedWarningWriter {
	return &blockedWarningWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockedWarningWriter) Write(p []byte) (int, error) {
	w.active.Add(1)
	defer w.active.Add(-1)
	if bytes.HasPrefix(p, []byte("warning: tracker")) {
		w.once.Do(func() {
			close(w.entered)
			<-w.release
		})
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.Write(p)
}

func (w *blockedWarningWriter) unblock() {
	w.releaseOnce.Do(func() { close(w.release) })
}

func (w *blockedWarningWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

type warningAndPeerTracker struct {
	warningURL string
	peerPort   uint16
	warnings   chan struct{}
	stopped    chan struct{}
}

func (f *warningAndPeerTracker) Announce(_ context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventStopped {
		select {
		case f.stopped <- struct{}{}:
		default:
		}
		return tracker.HTTPAnnounceResult{Transmitted: true}, nil
	}
	if rawURL == f.warningURL {
		select {
		case f.warnings <- struct{}{}:
		default:
		}
		return tracker.HTTPAnnounceResult{Transmitted: true}, errors.New("controlled local tracker failure")
	}
	return tracker.HTTPAnnounceResult{Interval: time.Minute, Transmitted: true,
		Peers: []tracker.HTTPPeer{{Host: "127.0.0.1", Port: f.peerPort}}}, nil
}

func TestCLIBlockedWarningDoesNotStallTransferStatusOrShutdown(t *testing.T) {
	data := []byte("blocked reporter still joins")
	info, infoHash := v1Info(t, data)
	const warningURL = "http://warning.fixture/announce"
	torrentPath := writeV1Torrent(t, v1Metainfo(t, info, warningURL))
	peers := newV1Peers(t, infoHash, info, data, false)
	peers.stall = true
	peers.failAfterFirst = true
	defer peers.close()
	fixture := &warningAndPeerTracker{
		warningURL: warningURL, peerPort: v1FixturePeerPort,
		warnings: make(chan struct{}, 16), stopped: make(chan struct{}, 16),
	}
	config := v1SessionConfig(t, nil, peers)
	config.HTTP = fixture
	statuses := make(chan session.RunProgress, 8)
	config.OnStatus = func(progress session.RunProgress) {
		select {
		case statuses <- progress:
		default:
		}
	}
	warnings := make(chan struct{}, 1)
	config.OnWarning = func(string) {
		select {
		case warnings <- struct{}{}:
		default:
		}
	}
	writer := newBlockedWarningWriter()
	reporter := NewReporterWithOptions(ReporterOptions{
		Level: LogInfo, Stderr: writer, IsTerminal: func(io.Writer) bool { return true },
	})
	ctx, cancel := context.WithCancel(context.Background())
	opts := parseV1Options(t, "--output", t.TempDir(), "--loglevel", "info", torrentPath)
	done := make(chan error, 1)
	go func() { done <- runWithReporter(ctx, opts, &bytes.Buffer{}, config, reporter) }()
	returned := false
	defer func() {
		writer.unblock()
		cancel()
		if !returned {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Errorf("CLI did not join after releasing reporter")
			}
		}
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reporter did not block while writing the local tracker warning")
	}
	select {
	case <-warnings:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not deliver the recoverable tracker warning")
	}
	statusSeen := false
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !statusSeen {
		select {
		case progress := <-statuses:
			statusSeen = progress.ActivePeers == 1
		case <-deadline.C:
			t.Fatal("transfer status callback stalled behind the blocked warning writer")
		}
	}
	cancel()
	select {
	case <-fixture.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("session shutdown stalled before the final tracker event")
	}
	select {
	case err := <-done:
		returned = true
		t.Fatalf("CLI returned before the blocked reporter was released: %v", err)
	default:
	}
	writer.unblock()
	select {
	case err := <-done:
		returned = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CLI error = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("reporting worker did not join after writer release (active=%d output=%q)", writer.active.Load(), writer.String())
	}
	if writer.active.Load() != 0 {
		t.Fatalf("writer still active after CLI returned: %d", writer.active.Load())
	}
	output := writer.String()
	if !strings.Contains(output, "warning: tracker http://warning.fixture failure; retrying") {
		t.Fatalf("recoverable warning was lost: %q", output)
	}
	if strings.LastIndex(output, "status:") < strings.LastIndex(output, "warning: tracker") {
		t.Fatalf("status was not restored after warning: %q", output)
	}
}

func TestCLITransferContinuesAndJoinsUnderDiagnosticQueuePressure(t *testing.T) {
	data := []byte("bounded diagnostics still download")
	info, infoHash := v1Info(t, data)
	// Keep the peer wire quiet until pressure is established; duplicate Have
	// frames are then followed by availability transitions as a processing barrier.
	peers := newV1Peers(t, infoHash, info, data, true)
	peers.requestSeen = make(chan struct{})
	peers.releasePiece = make(chan struct{})
	peers.duplicateHaveGate = make(chan struct{})
	var source strings.Builder
	fmt.Fprintf(&source, "magnet:?xt=urn:btih:%x&x.pe=127.0.0.1%%3A%d", infoHash, v1FixturePeerPort)
	for i := 0; i < 63; i++ {
		fmt.Fprintf(&source, "&tr=http%%3A%%2F%%2Ftracker-%02d.test%%2Fannounce", i)
	}
	trackerFixture := &pressureFailTracker{seen: make(map[string]int)}
	// Hold stderr from the first debug line so tracker events deterministically
	// fill the bounded diagnostic queue.
	writer := newBlockedDebugWriter()
	output := t.TempDir()
	opts := parseV1Options(t, "--loglevel", "debug", "--output", output, source.String())
	progress := make(chan struct{}, 1)
	config := v1SessionConfig(t, nil, peers)
	config.HTTP = trackerFixture
	trackerDiagnostics := make(chan struct{}, diagnosticQueueCapacity+1)
	availabilityTransitions := make(chan string, 8)
	var emptyTransitions, nonemptyTransitions atomic.Int32
	config.OnDiagnostic = func(event session.Diagnostic) {
		switch event.Kind {
		case session.DiagnosticTrackerAttempt:
			select {
			case trackerDiagnostics <- struct{}{}:
			default:
			}
		case session.DiagnosticTransfer:
			switch event.Detail {
			case "availability became empty":
				emptyTransitions.Add(1)
				select {
				case availabilityTransitions <- event.Detail:
				default:
				}
			case "availability became nonempty":
				nonemptyTransitions.Add(1)
				select {
				case availabilityTransitions <- event.Detail:
				default:
				}
			}
		}
	}
	config.OnProgress = func(session.RunProgress) {
		select {
		case progress <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	done := make(chan error, 1)
	sessionReturned := false
	var releasePeer, releaseDuplicate sync.Once
	unblockPeer := func() { releasePeer.Do(func() { close(peers.releasePiece) }) }
	unblockDuplicates := func() { releaseDuplicate.Do(func() { close(peers.duplicateHaveGate) }) }
	defer func() {
		writer.unblock()
		unblockDuplicates()
		unblockPeer()
		if !sessionReturned {
			cancel()
		}
		peers.close()
		if !sessionReturned {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Errorf("CLI session did not join during failure cleanup")
			}
		}
		cancel()
		started := peers.calls.Load()
		if err := peers.waitCalls(t, started); err != nil {
			t.Errorf("%d started peer fixtures did not join during cleanup: %v", started, err)
		}
	}()
	go func() { done <- RunWithSession(ctx, opts, &bytes.Buffer{}, writer, config) }()
	select {
	case <-writer.entered:
	case <-ctx.Done():
		t.Fatal("diagnostic consumer did not reach the blocked writer")
	}
	for observed := 0; observed < diagnosticQueueCapacity+1; observed++ {
		select {
		case <-trackerDiagnostics:
		case <-ctx.Done():
			t.Fatalf("tracker diagnostics did not fill the blocked queue: observed %d", observed)
		}
	}
	select {
	case <-peers.requestSeen:
	case <-ctx.Done():
		t.Fatalf("local peer did not receive the transfer request (dials=%d, tracker URLs=%d)",
			peers.calls.Load(), len(trackerFixture.snapshot()))
	}
	unblockDuplicates()
	sawEmpty, sawRestored := false, false
	for !sawRestored {
		select {
		case detail := <-availabilityTransitions:
			if detail == "availability became empty" {
				sawEmpty = true
			} else if sawEmpty && detail == "availability became nonempty" {
				sawRestored = true
			}
		case <-ctx.Done():
			t.Fatal("duplicate Have processing barrier did not complete")
		}
	}
	// The two identical Have frames are silent; only Have None/Have drive the
	// single empty/nonempty transition pair used to confirm coordinator processing.
	if got := emptyTransitions.Load(); got != 1 {
		t.Fatalf("availability-empty transitions = %d, want 1; duplicate Have emitted excess diagnostics", got)
	}
	if got := nonemptyTransitions.Load(); got != 2 {
		t.Fatalf("availability-nonempty transitions = %d, want initial and restored state", got)
	}
	unblockPeer()
	select {
	case <-progress:
	case <-ctx.Done():
		t.Fatal("transfer stalled while diagnostic rendering was blocked")
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("piece was not verified before reporter release: %q, %v", got, err)
	}
	writer.unblock()
	select {
	case err := <-done:
		sessionReturned = true
		if err != nil {
			t.Fatalf("CLI transfer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("CLI reporter or transfer worker failed to join")
	}
	if writer.active.Load() != 0 {
		t.Fatalf("writer still active after RunWithSession returned: %d", writer.active.Load())
	}
	if got := peers.calls.Load(); got < 2 {
		t.Fatalf("successful metadata/transfer run used %d peer fixture calls, want at least 2", got)
	}
	outputText := writer.String()
	if strings.Count(outputText, "debug: diagnostics: dropped ") != 1 {
		t.Fatalf("debug queue did not report bounded pressure/drop: %q", outputText)
	}
	var dropped uint64
	for _, line := range strings.Split(outputText, "\n") {
		if strings.HasPrefix(line, "debug: diagnostics: dropped ") {
			if _, err := fmt.Sscanf(line, "debug: diagnostics: dropped %d", &dropped); err != nil || dropped == 0 {
				t.Fatalf("invalid dropped-record summary %q: %d, %v", line, dropped, err)
			}
		}
	}
	calls := trackerFixture.snapshot()
	if calls[torrent.DefaultTracker] == 0 {
		t.Fatalf("mandatory default tracker was not routed through the local failing fixture: %v", calls)
	}
	if len(calls) < 64 {
		t.Fatalf("local tracker failures did not exercise the tracker set: %d URLs", len(calls))
	}
	repeatedFailure := false
	for rawURL, count := range calls {
		if count == 0 {
			t.Errorf("tracker %q had no failed attempts", rawURL)
		}
		repeatedFailure = repeatedFailure || count > 1
	}
	if !repeatedFailure {
		t.Fatalf("no tracker produced repeated failures: %v", calls)
	}
	peers.assertNoUpload(t)
}
