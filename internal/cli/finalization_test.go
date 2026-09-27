package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type heldFinalTracker struct {
	delegate *v1Tracker
	onFinal  func(tracker.Event)
	release  chan struct{}
	once     sync.Once
}

func (f *heldFinalTracker) Announce(ctx context.Context, raw string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventCompleted || request.Event == tracker.EventStopped {
		f.once.Do(func() {
			f.onFinal(request.Event)
			select {
			case <-f.release:
			case <-ctx.Done():
			}
		})
	}
	return f.delegate.Announce(ctx, raw, request)
}

func TestCLITransferStatusEndsBeforeFinalAnnounces(t *testing.T) {
	for _, tc := range []struct {
		name string
		tty  bool
	}{
		{"success redirected", false},
		{"success terminal", true},
		{"canceled", false},
		{"failed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte{'x'}, 32<<10)
			info, hash := v1Info(t, data)
			path := writeV1Torrent(t, v1Metainfo(t, info, ""))
			peers := newV1Peers(t, hash, info, data, false)
			peers.partial = tc.name == "canceled"
			delegate := &v1Tracker{}
			config := v1SessionConfig(t, delegate, peers)
			ctx, cancel := context.WithCancel(context.Background())
			var ended atomic.Bool
			var observations atomic.Int64
			var clock atomic.Int64
			clock.Store(int64(time.Second))
			observe := func(progress session.RunProgress) {
				if ended.Load() {
					t.Error("transfer callback after its owning phase ended")
				}
				if progress.ActivePeers > 0 && progress.RecentRateBytesPerSec > 0 {
					observations.Add(1)
					if tc.name == "canceled" {
						cancel() // Incomplete transfer, with live peer/rate in its last snapshot.
					}
				}
			}
			config.OnProgress, config.OnStatus = observe, observe
			config.OnPhase = func(phase string) {
				if phase == "shutdown" {
					ended.Store(true)
				}
			}
			readErr := errors.New("fixture staged read failure")
			if tc.name == "failed" {
				config.StageFileOpener = func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
					file, err := os.OpenFile(path, flag, mode)
					if err != nil {
						return nil, err
					}
					return v1ReadFailureStageFile{File: file, err: readErr}, nil
				}
			}
			entered, release := make(chan tracker.Event, 1), make(chan struct{})
			config.HTTP = &heldFinalTracker{delegate: delegate, release: release, onFinal: func(event tracker.Event) {
				clock.Add(int64(time.Minute)) // Beyond both progress throttles.
				entered <- event
			}}
			stderr := &statusOutput{changed: make(chan struct{}, 1)}
			var stdout bytes.Buffer
			opts := Options{Source: path, Output: t.TempDir(), LogLevel: LogDebug}
			done := make(chan error, 1)
			go func() {
				done <- runWithReporter(ctx, opts, &stdout, config, statusReporter(stderr, LogDebug, tc.tty, &clock))
			}()
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			returned := false
			defer func() {
				cancel()
				unblock()
				if !returned {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("CLI failed to join after releasing final tracker")
					}
				}
				peers.close()
			}()
			var firstFinal tracker.Event
			select {
			case firstFinal = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("final tracker event did not start")
			}
			if !ended.Load() {
				t.Fatal("final announces began before transfer reporting ended")
			}
			if tc.name != "failed" && observations.Load() == 0 {
				t.Fatal("fixture never supplied a nonzero peer/rate snapshot")
			}
			entries, err := os.ReadDir(config.CacheRoot)
			if err != nil || len(entries) != 0 {
				t.Fatalf("final announce preceded workspace cleanup: %v, %v", entries, err)
			}
			waitStatusOutput(t, stderr, `info: phase: "shutdown"`)
			// Exercise the real reporter ticker while Run is still waiting, with
			// the controlled observation clock advanced past the 30-second limit.
			timer := time.NewTimer(statusInterval + 100*time.Millisecond)
			select {
			case err := <-done:
				returned = true
				timer.Stop()
				t.Fatalf("Run returned through a held final announce: %v", err)
			case <-timer.C:
			}
			assertRetired := func() {
				t.Helper()
				text := stderr.String()
				at := strings.Index(text, `info: phase: "shutdown"`)
				if at < 0 || strings.Contains(text[at:], "info: progress:") || strings.Contains(text[at:], `status: phase="transfer"`) {
					t.Fatalf("retired transfer status reappeared: %q", text)
				}
			}
			assertRetired()
			unblock()
			select {
			case err = <-done:
				returned = true
			case <-time.After(3 * time.Second):
				t.Fatal("CLI did not join after final announce release")
			}
			want := error(nil)
			wantFinal := tracker.EventCompleted
			switch tc.name {
			case "canceled":
				want, wantFinal = context.Canceled, tracker.EventStopped
			case "failed":
				want, wantFinal = readErr, tracker.EventStopped
			}
			if !errors.Is(err, want) || firstFinal != wantFinal || stdout.Len() != 0 {
				t.Fatalf("result=%v first-final=%v stdout=%q; want %v/%v", err, firstFinal, stdout.String(), want, wantFinal)
			}
			assertRetired() // Includes warning/debug restoration and the final flush.
			if err := peers.wait(t); err != nil {
				t.Fatal(err)
			}
		})
	}
}
