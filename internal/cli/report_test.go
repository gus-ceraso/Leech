package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestReporterLevelFilteringUsesWarningDefault(t *testing.T) {
	var output bytes.Buffer
	r := NewReporterWithOptions(ReporterOptions{Stderr: &output})
	if r.Level() != LogWarning {
		t.Fatalf("default level = %q, want warning", r.Level())
	}
	if err := r.Debug("debug"); err != nil {
		t.Fatal(err)
	}
	if err := r.Info("info"); err != nil {
		t.Fatal(err)
	}
	if err := r.Warning("warning"); err != nil {
		t.Fatal(err)
	}
	if err := r.Error("error"); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "warning: warning\nerror: error\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestReporterStatusThrottleAndReplacement(t *testing.T) {
	var output bytes.Buffer
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	r := NewReporterWithOptions(ReporterOptions{
		Level:      LogInfo,
		Stderr:     &output,
		IsTerminal: func(_ io.Writer) bool { return true },
		Now:        func() time.Time { return now },
	})
	if err := r.Status(Status{Phase: "metadata", SelectedBytes: 3}); err != nil {
		t.Fatal(err)
	}
	first := output.String()
	if err := r.Status(Status{Phase: "transfer", VerifiedSelectedBytes: 1, SelectedBytes: 3, ActivePeers: 2}); err != nil {
		t.Fatal(err)
	}
	if output.String() != first {
		t.Fatalf("status updated before one second: %q", output.String())
	}
	now = now.Add(time.Second)
	if err := r.Status(Status{Phase: "transfer", VerifiedSelectedBytes: 1, SelectedBytes: 3, ActivePeers: 2}); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "status:") != 2 {
		t.Fatalf("status count = %d, output %q", strings.Count(output.String(), "status:"), output.String())
	}
	if err := r.Phase("done"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(output.String(), "info: phase: \"done\"\n") {
		t.Fatalf("permanent line did not clear status: %q", output.String())
	}
}

func TestReporterNonTTYOmitsStatus(t *testing.T) {
	var output bytes.Buffer
	r := NewReporterWithOptions(ReporterOptions{
		Level:      LogDebug,
		Stderr:     &output,
		IsTerminal: func(_ io.Writer) bool { return false },
	})
	if err := r.Status(Status{Phase: "transfer", SelectedBytes: 3}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("non-TTY status output = %q", output.String())
	}
	if err := r.Debug("worker joined"); err != nil {
		t.Fatal(err)
	}
	if output.String() != "debug: worker joined\n" {
		t.Fatalf("debug output = %q", output.String())
	}
}

func TestReporterNotableResults(t *testing.T) {
	var output bytes.Buffer
	r := NewReporter(LogInfo, &output)
	_ = r.ReportResult(Result{NoTransferNeeded: true})
	_ = r.ReportResult(Result{SelectionComplete: true})
	_ = r.ReportResult(Result{SelectionComplete: true, TorrentComplete: true})
	_ = r.PrivateIgnored()
	got := output.String()
	for _, want := range []string{
		"output is already complete; no transfer was needed",
		"selected content is complete",
		"torrent is complete",
		"private=1; treating the torrent as public",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output %q does not contain %q", got, want)
		}
	}
}

func TestReporterPrimaryAndSecondaryFailuresAreDistinctAndSafe(t *testing.T) {
	var output bytes.Buffer
	r := NewReporter(LogError, &output)
	primary := fmt.Errorf("tracker %s: %w", "https://user:secret@example.test:443/announce?token=abc", errors.New("wrapped magnet:?xt=urn:btih:0123456789012345678901234567890123456789&dn=private"))
	if err := r.PrimaryFailure(primary, true); err != nil {
		t.Fatal(err)
	}
	if err := r.SecondaryFailure(errors.New("stopped udp://secret@example.test/announce?x=y")); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, forbidden := range []string{"user:secret", "/announce", "token=abc", "magnet:?", "0123456789012345678901234567890123456789", "secret@example"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("output leaks %q: %q", forbidden, got)
		}
	}
	if !strings.Contains(got, "verified partial output remains resumable") || !strings.Contains(got, "shutdown:") {
		t.Fatalf("missing failure detail: %q", got)
	}
}

func TestQuoteAndSanitizeEscapeControlCharacters(t *testing.T) {
	if got := QuoteName("bad\x1b[31m\nname"); got != `"bad\x1b[31m\nname"` {
		t.Fatalf("quoted = %q", got)
	}
	got := SanitizeDiagnostic("bad\x1b[31m\nname")
	if got != `bad\x1b[31m\nname` {
		t.Fatalf("sanitized = %q", got)
	}
}

func TestRedactTrackerURL(t *testing.T) {
	for _, test := range []struct {
		input, want string
	}{
		{"https://user:pass@example.test:8443/announce?secret=yes", "https://example.test:8443"},
		{"udp://[2001:db8::1]:6969/announce", "udp://[2001:db8::1]:6969"},
		{"not a URL", "<redacted-tracker>"},
	} {
		if got := RedactTrackerURL(test.input); got != test.want {
			t.Errorf("RedactTrackerURL(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestSanitizeDiagnosticBoundsOutput(t *testing.T) {
	got := SanitizeDiagnostic(strings.Repeat("x", 100), 16)
	if len(got) > 16 {
		t.Fatalf("sanitized length = %d, want <= 16", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("sanitized = %q, want truncation marker", got)
	}
}
