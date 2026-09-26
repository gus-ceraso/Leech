package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type statusOutput struct {
	mu      sync.Mutex
	data    bytes.Buffer
	changed chan struct{}
}

func (w *statusOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	n, err := w.data.Write(data)
	w.mu.Unlock()
	select {
	case w.changed <- struct{}{}:
	default:
	}
	return n, err
}

func (w *statusOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.String()
}

func waitStatusOutput(t *testing.T, output *statusOutput, want string) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for !strings.Contains(output.String(), want) {
		select {
		case <-output.changed:
		case <-deadline.C:
			t.Fatalf("status %q not emitted: %q", want, output.String())
		}
	}
}

func statusReporter(output io.Writer, level LogLevel, tty bool, clock *atomic.Int64) *Reporter {
	return NewReporterWithOptions(ReporterOptions{
		Stderr: output, Level: level, IsTerminal: func(io.Writer) bool { return tty },
		Now: func() time.Time { return time.Unix(0, clock.Load()) },
	})
}

func TestCLIResumeStatusBeforeAlreadyCompleteResult(t *testing.T) {
	info, _ := v1Info(t, []byte("x"))
	path := writeV1Torrent(t, v1Metainfo(t, info, ""))
	for _, tc := range []struct {
		name        string
		level       LogLevel
		tty, status bool
	}{
		{"interactive info", LogInfo, true, true},
		{"interactive debug", LogDebug, true, true},
		{"noninteractive info", LogInfo, false, false},
		{"interactive warning", LogWarning, true, false},
		{"interactive error", LogError, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := t.TempDir()
			if err := os.WriteFile(filepath.Join(output, "payload.bin"), []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			opts := parseV1Options(t, "--resume", "--loglevel", string(tc.level), "--output", output, path)
			var stderr, stdout bytes.Buffer
			var clock atomic.Int64
			var commits int
			err := runWithReporter(context.Background(), opts, &stdout, session.RunConfig{
				OnProgress: func(session.RunProgress) { commits++ },
			}, statusReporter(&stderr, tc.level, tc.tty, &clock))
			if err != nil {
				t.Fatal(err)
			}
			if stdout.Len() != 0 || commits != 0 {
				t.Fatalf("resume stdout/commits = %q/%d", stdout.String(), commits)
			}
			got := stderr.String()
			if strings.Contains(got, "status:") != tc.status {
				t.Fatalf("unexpected status output: %q", got)
			}
			if tc.status {
				status := strings.Index(got, `status: phase="resume" verified=0/1 bytes`)
				result := strings.Index(got, "output is already complete; no transfer was needed")
				phase := strings.Index(got, `info: phase: "resume"`)
				if phase < 0 || status < phase || result < status {
					t.Fatalf("resume phase/status/result order: %q", got)
				}
			}
		})
	}
}

// The initial announce stays in flight until the test cancels the phase, so
// metadata and transfer remain active without any peer or committed piece.
type statusHeldTracker struct{ started chan struct{} }

func (f *statusHeldTracker) Announce(ctx context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventStarted {
		close(f.started)
		<-ctx.Done()
		return tracker.HTTPAnnounceResult{Transmitted: true}, ctx.Err()
	}
	return tracker.HTTPAnnounceResult{Transmitted: true, Interval: time.Minute}, nil
}

func TestCLIStatusDuringHeldMetadataAndTransfer(t *testing.T) {
	for _, phase := range []string{"metadata", "transfer", "resumed transfer"} {
		t.Run(phase, func(t *testing.T) {
			var clock atomic.Int64
			stderr := &statusOutput{changed: make(chan struct{}, 1)}
			var stdout bytes.Buffer
			output := t.TempDir()
			opts := Options{Source: strings.Repeat("0", 40), Output: output, LogLevel: LogInfo}
			want := `status: phase="metadata"`
			if phase == "transfer" {
				info, _ := v1Info(t, []byte("x"))
				opts.Source = writeV1Torrent(t, v1Metainfo(t, info, ""))
				want = `status: phase="transfer" verified=0/1 bytes peers=0 rate=0 B/s`
			}
			if phase == "resumed transfer" {
				data := bytes.Repeat([]byte{'x'}, 32<<10)
				info, _ := v1TwoPieceInfo(t, data)
				opts.Source, opts.Resume = writeV1Torrent(t, v1Metainfo(t, info, "")), true
				if err := os.WriteFile(filepath.Join(output, "payload.bin"), data[:16<<10], 0600); err != nil {
					t.Fatal(err)
				}
				want = `status: phase="transfer" verified=16384/32768 bytes peers=0 rate=0 B/s`
			}
			fixture := &statusHeldTracker{started: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var commits atomic.Int64
			done := make(chan error, 1)
			cache := filepath.Join(t.TempDir(), "cache")
			go func() {
				done <- runWithReporter(ctx, opts, &stdout, session.RunConfig{
					HTTP: fixture, CacheRoot: cache,
					OnPhase: func(name string) {
						if phase == "resumed transfer" && name == "transfer" {
							clock.Add(int64(time.Second))
						}
					},
					OnProgress: func(session.RunProgress) { commits.Add(1) },
				}, statusReporter(stderr, LogInfo, true, &clock))
			}()
			select {
			case <-fixture.started:
			case <-time.After(3 * time.Second):
				t.Fatal("phase did not start its tracker")
			}
			waitStatusOutput(t, stderr, want)
			if phase == "metadata" && strings.Contains(stderr.String(), "verified=") {
				t.Fatalf("unknown metadata totals were rendered: %q", stderr.String())
			}
			if commits.Load() != 0 {
				t.Fatal("phase-entry status called commit progress")
			}
			select {
			case err := <-done:
				t.Fatalf("held phase returned early: %v", err)
			default:
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("held phase cancellation = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("held phase did not join")
			}
			if stdout.Len() != 0 {
				t.Fatalf("status wrote stdout: %q", stdout.String())
			}
		})
	}
}

func TestCLIStatusReturnsAfterRapidResumeToStalledTransfer(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, 32<<10)
	info, _ := v1TwoPieceInfo(t, data)
	path := writeV1Torrent(t, v1Metainfo(t, info, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), data[:16<<10], 0600); err != nil {
		t.Fatal(err)
	}
	opts := parseV1Options(t, "--resume", "--loglevel=info", "--output", output, path)
	fixture := &statusHeldTracker{started: make(chan struct{})}
	stderr := &statusOutput{changed: make(chan struct{}, 1)}
	var stdout bytes.Buffer
	var clock, commits atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	cache := filepath.Join(t.TempDir(), "cache")
	go func() {
		done <- runWithReporter(ctx, opts, &stdout, session.RunConfig{
			HTTP: fixture, CacheRoot: cache,
			OnProgress: func(session.RunProgress) { commits.Add(1) },
		}, statusReporter(stderr, LogInfo, true, &clock))
	}()
	select {
	case <-fixture.started:
	case <-time.After(3 * time.Second):
		t.Fatal("transfer tracker did not start")
	}
	initial := stderr.String()
	if !strings.Contains(initial, `status: phase="resume" verified=0/32768 bytes`) ||
		!strings.Contains(initial, `info: phase: "transfer"`) ||
		strings.Contains(initial, `status: phase="transfer"`) {
		t.Fatalf("rapid phase transition did not exercise the throttle: %q", initial)
	}
	clock.Store(int64(2 * time.Second))
	waitStatusOutput(t, stderr, `status: phase="transfer" verified=16384/32768 bytes peers=0 rate=0 B/s`)
	if commits.Load() != 0 {
		t.Fatal("status refresh reported a committed piece")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("held transfer cancellation = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("status refresh worker did not join")
	}
	if stdout.Len() != 0 {
		t.Fatalf("status wrote stdout: %q", stdout.String())
	}
	final := stderr.String()
	clock.Store(int64(4 * time.Second))
	time.Sleep(statusInterval + 50*time.Millisecond)
	if got := stderr.String(); got != final {
		t.Fatalf("status worker wrote after Run returned: %q", got)
	}
}

func TestCLIStatusRetainsCommittedPieceProgress(t *testing.T) {
	data := []byte("x")
	info, hash := v1Info(t, data)
	path := writeV1Torrent(t, v1Metainfo(t, info, ""))
	peers := newV1Peers(t, hash, info, data, false)
	defer peers.close()
	config := v1SessionConfig(t, &v1Tracker{}, peers)
	var clock atomic.Int64
	commits := 0
	config.OnProgress = func(progress session.RunProgress) {
		commits++
		clock.Add(int64(time.Second))
		if progress.VerifiedSelectedBytes != 1 {
			t.Errorf("committed bytes = %d", progress.VerifiedSelectedBytes)
		}
	}
	var stdout, stderr bytes.Buffer
	opts := parseV1Options(t, "--loglevel=info", "--output", t.TempDir(), path)
	if err := runWithReporter(context.Background(), opts, &stdout, config, statusReporter(&stderr, LogInfo, true, &clock)); err != nil {
		t.Fatal(err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || !strings.Contains(stderr.String(), `verified=0/1 bytes`) || !strings.Contains(stderr.String(), `verified=1/1 bytes`) {
		t.Fatalf("initial/commit progress = %d, %q", commits, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("status wrote stdout: %q", stdout.String())
	}
}

func TestCLIStatusTracksLivePeerAndPayloadRateWithoutCommit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		partial bool
	}{
		{name: "choked peer disconnect"},
		{name: "partial payload rate decay", partial: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte{'x'}, 32<<10)
			info, hash := v1Info(t, data)
			path := writeV1Torrent(t, v1Metainfo(t, info, ""))
			peers := newV1Peers(t, hash, info, data, false)
			peers.stall, peers.partial = !tc.partial, tc.partial
			peers.failAfterFirst = true
			fixture := &v1Tracker{}
			config := v1SessionConfig(t, fixture, peers)
			var clock atomic.Int64
			clock.Store(int64(100 * time.Second))
			statuses := make(chan session.RunProgress, 16)
			config.OnStatus = func(progress session.RunProgress) {
				statuses <- progress
				clock.Add(int64(time.Second))
			}
			config.Now = func() time.Time { return time.Unix(0, clock.Load()) }
			stderr := &statusOutput{changed: make(chan struct{}, 1)}
			var stdout bytes.Buffer
			opts := parseV1Options(t, "--loglevel=info", "--output", t.TempDir(), path)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- runWithReporter(ctx, opts, &stdout, config, statusReporter(stderr, LogInfo, true, &clock))
			}()
			waitProgress := func(check func(session.RunProgress) bool) session.RunProgress {
				t.Helper()
				timer := time.NewTimer(7 * time.Second)
				defer timer.Stop()
				for {
					select {
					case progress := <-statuses:
						if check(progress) {
							return progress
						}
					case <-timer.C:
						t.Fatalf("status condition not observed; output=%q", stderr.String())
					}
				}
			}
			if tc.partial {
				progress := waitProgress(func(progress session.RunProgress) bool { return progress.RecentRateBytesPerSec > 0 })
				if progress.VerifiedSelectedBytes != 0 || progress.ActivePeers != 1 {
					t.Fatalf("partial payload status = %+v", progress)
				}
				progress = waitProgress(func(progress session.RunProgress) bool { return progress.RecentRateBytesPerSec == 0 })
				if progress.VerifiedSelectedBytes != 0 {
					t.Fatalf("rate decay changed verified bytes: %+v", progress)
				}
			} else {
				progress := waitProgress(func(progress session.RunProgress) bool { return progress.ActivePeers == 1 })
				if progress.VerifiedSelectedBytes != 0 {
					t.Fatalf("choked peer status = %+v", progress)
				}
				peers.close()
				progress = waitProgress(func(progress session.RunProgress) bool { return progress.ActivePeers == 0 })
				if progress.VerifiedSelectedBytes != 0 {
					t.Fatalf("disconnect status = %+v", progress)
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(7 * time.Second):
				t.Fatal("status run did not join")
			}
			if stdout.Len() != 0 {
				t.Fatalf("status wrote stdout: %q", stdout.String())
			}
		})
	}
}

func TestStatusThrottleSurvivesPermanentLinesAndFastPhases(t *testing.T) {
	var output bytes.Buffer
	// A zero clock is intentional: even the first timestamp must survive
	// permanent lines without making the next update look like the first.
	now := time.Time{}
	reporter := NewReporterWithOptions(ReporterOptions{
		Level: LogInfo, Stderr: &output, IsTerminal: func(io.Writer) bool { return true }, Now: func() time.Time { return now },
	})
	for _, phase := range []string{"metadata", "resume", "transfer"} {
		if err := reporter.Phase(phase); err != nil {
			t.Fatal(err)
		}
		if err := reporter.Status(Status{Phase: phase, SelectedBytes: 1}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(100 * time.Millisecond)
	}
	if err := reporter.Warning("fixture warning"); err != nil {
		t.Fatal(err)
	}
	if err := reporter.Status(Status{Phase: "transfer", SelectedBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(output.String(), "status:"); got != 1 {
		t.Fatalf("rapid status count = %d: %q", got, output.String())
	}
	for _, phase := range []string{"metadata", "resume", "transfer"} {
		if !strings.Contains(output.String(), `info: phase: "`+phase+`"`) {
			t.Fatalf("missing permanent phase %s: %q", phase, output.String())
		}
	}
	now = time.Time{}.Add(time.Second)
	if err := reporter.Status(Status{Phase: "transfer", SelectedBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(output.String(), "status:"); got != 2 {
		t.Fatalf("status did not resume after a second: %q", output.String())
	}
}
