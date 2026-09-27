package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestQueuedLogsUseObservationTimeAndSafeTransportFields(t *testing.T) {
	observed := time.Date(2026, 9, 26, 12, 1, 2, 345000000, time.FixedZone("fixture", 3600))
	var output bytes.Buffer
	reporter := NewReporterWithOptions(ReporterOptions{Level: LogDebug, Stderr: &output, Now: func() time.Time { return observed.Add(time.Hour) }})
	queue := newDiagnosticQueue()
	queue.enqueue(session.Diagnostic{
		At: observed, Kind: session.DiagnosticTransportRace, Phase: "transfer", Detail: "winner=tcp", RetryAfter: time.Second,
		Race: peer.RaceObservation{
			Winner: peer.TransportTCP, Duration: 200 * time.Millisecond,
			TCP: peer.AttemptObservation{Outcome: peer.AttemptSucceeded, Stage: peer.AttemptComplete, Duration: 50 * time.Millisecond},
			UTP: peer.AttemptObservation{Outcome: peer.AttemptCanceled, Stage: peer.AttemptReadHandshake, Failure: peer.DialFailureCanceled, Duration: 200 * time.Millisecond},
		},
	})
	renderDiagnostic(reporter, <-queue.records)
	for _, want := range []string{
		"2026-09-26T11:01:02.345Z debug: transport race phase=transfer",
		"winner=tcp", "utp=canceled(stage=handshake-read reason=canceled duration=200ms)",
		"tcp=succeeded(stage=complete reason=none duration=50ms)", "race-duration=200ms", "retry-after=1s",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("log lacks %q: %q", want, output.String())
		}
	}
	if strings.Contains(output.String(), "T12:01:") {
		t.Fatal("queued observation was timestamped at render time")
	}
}

func TestRedirectedProgressIsSparseAndLevelFiltered(t *testing.T) {
	for _, level := range []LogLevel{LogDebug, LogInfo, LogWarning, LogError} {
		var output bytes.Buffer
		reporter := NewReporterWithOptions(ReporterOptions{Level: level, Stderr: &output, IsTerminal: func(io.Writer) bool { return false }})
		snapshot := Status{Phase: "transfer", VerifiedSelectedBytes: 1, SelectedBytes: 3, ActivePeers: 2, RecentRateBytesPerSec: 7}
		base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
		for second := range 61 {
			_, err := reporter.renderProgressAt(snapshot, base.Add(time.Duration(second)*time.Second))
			if err != nil {
				t.Fatal(err)
			}
		}
		want := 0
		if level == LogInfo || level == LogDebug {
			want = 3 // Entry, 30 seconds, and 60 seconds; unchanged stalls stay visible.
		}
		if count := strings.Count(output.String(), "info: progress: verified=1/3 bytes peers=2 rate=7 B/s"); count != want || strings.Contains(output.String(), "\r") {
			t.Fatalf("%s progress = %q, want %d permanent lines", level, output.String(), want)
		}
	}
}

func TestSuccessfulDownloadKeepsFinalTrackerFailureNonfatal(t *testing.T) {
	data := []byte("logging fixture")
	info, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, info, ""))
	for _, level := range []LogLevel{LogDebug, LogInfo, LogError} {
		t.Run(string(level), func(t *testing.T) {
			peers := newV1Peers(t, infoHash, info, data, false)
			defer peers.close()
			delegate := &v1Tracker{}
			config := v1SessionConfig(t, delegate, peers)
			cause := errors.New("untrusted-secret\nhttps://user:password@example.test/private?token=secret")
			config.HTTP = v1FinalFailureTracker{delegate: delegate, err: &tracker.HTTPError{
				Code: tracker.HTTPErrorMalformed, Cause: tracker.CauseInvalidCompactEndpoint, StatusCode: 200, Err: cause,
			}}
			observed := false
			config.OnSecondary = func(err error) {
				var final *tracker.FinalAnnounceError
				observed = errors.As(err, &final) && final.Failures > 0 && errors.Is(err, cause)
			}
			var stdout, stderr bytes.Buffer
			err := RunWithSession(context.Background(), Options{Source: torrentPath, Output: t.TempDir(), LogLevel: level}, &stdout, &stderr, config)
			if err != nil || !observed || stdout.Len() != 0 {
				t.Fatalf("result err=%v secondary=%t stdout=%q", err, observed, stdout.String())
			}
			if err := peers.wait(t); err != nil {
				t.Fatal(err)
			}
			got := stderr.String()
			if strings.Contains(got, "error:") || strings.Contains(got, "secret") || strings.Contains(got, `\n`) {
				t.Fatalf("nonfatal diagnostics leaked error text or claimed failure: %q", got)
			}
			if detailed := strings.Contains(got, "code=malformed cause=invalid-compact-endpoint status=200"); detailed != (level == LogDebug) {
				t.Fatalf("safe final detail filtering at %s: %q", level, got)
			}
			if level == LogError {
				if got != "" {
					t.Fatalf("error-only successful run wrote %q", got)
				}
				return
			}
			for _, want := range []string{"warning: nonfatal:", "primary result unchanged", "info: summary: verified=15/15 bytes", "useful-connections=1", "info: result: torrent is complete"} {
				if !strings.Contains(got, want) {
					t.Errorf("successful result lacks %q: %q", want, got)
				}
			}
		})
	}
}
