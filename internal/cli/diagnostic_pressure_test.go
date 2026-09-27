package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	mu       sync.Mutex
	seen     map[string]int
	attempts chan string
}

func (f *pressureFailTracker) Announce(ctx context.Context, rawURL string, _ tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.HTTPAnnounceResult{}, err
	}
	f.mu.Lock()
	f.seen[rawURL]++
	f.mu.Unlock()
	select {
	case f.attempts <- rawURL:
	default:
	}
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
	if bytes.HasPrefix(p, []byte("debug: transfer")) {
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

func TestCLITransferContinuesAndJoinsUnderDiagnosticQueuePressure(t *testing.T) {
	data := []byte("bounded diagnostics still download")
	info, infoHash := v1Info(t, data)
	peers := newV1Peers(t, infoHash, info, data, true)
	peers.duplicateHave = true
	peers.diagnosticChurn = 80
	peers.requestSeen = make(chan struct{})
	peers.releasePiece = make(chan struct{})
	var source strings.Builder
	fmt.Fprintf(&source, "magnet:?xt=urn:btih:%x&x.pe=127.0.0.1%%3A%d", infoHash, v1FixturePeerPort)
	for i := 0; i < 63; i++ {
		fmt.Fprintf(&source, "&tr=http%%3A%%2F%%2Ftracker-%02d.test%%2Fannounce", i)
	}
	trackerFixture := &pressureFailTracker{seen: make(map[string]int), attempts: make(chan string, 512)}
	writer := newBlockedDebugWriter()
	output := t.TempDir()
	opts := parseV1Options(t, "--loglevel", "debug", "--output", output, source.String())
	progress := make(chan struct{}, 1)
	config := v1SessionConfig(t, nil, peers)
	config.HTTP = trackerFixture
	config.OnProgress = func(session.RunProgress) {
		select {
		case progress <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	done := make(chan error, 1)
	sessionReturned := false
	var releasePeer sync.Once
	unblockPeer := func() { releasePeer.Do(func() { close(peers.releasePiece) }) }
	defer func() {
		writer.unblock()
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
	select {
	case <-peers.requestSeen:
	case <-ctx.Done():
		t.Fatal("local peer did not receive the transfer request")
	}
	attemptCount := 0
	for attemptCount < 128 {
		select {
		case <-trackerFixture.attempts:
			attemptCount++
		case <-ctx.Done():
			t.Fatalf("tracker retries did not fill the diagnostic queue: observed %d attempts", attemptCount)
		}
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
