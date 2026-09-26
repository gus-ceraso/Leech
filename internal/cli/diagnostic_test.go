package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/session"
)

func TestDiagnosticQueueBoundsRedactsAndDrops(t *testing.T) {
	queue := newDiagnosticQueue()
	input := []session.Diagnostic{
		{
			Kind: session.DiagnosticPhaseTransition, Phase: "tracker",
			Endpoint: session.DiagnosticEndpoint{Scheme: "https", Host: "[2001:db8::1]:443"},
			Detail:   "failed https://alice:pw@example.test/private?token=credential",
		},
		{
			Kind: session.DiagnosticPhaseTransition, Phase: "metadata",
			Detail: "source magnet:?xt=urn:btih:0123456789012345678901234567890123456789; control=\x1b[31m",
		},
		{
			Kind: session.DiagnosticPhaseTransition, Phase: "transfer",
			Detail: "long error https://bob:pw@example.test/hidden?secret=yes\x1b" + strings.Repeat("x", DefaultDiagnosticBytes*2),
		},
	}
	for _, record := range input {
		queue.enqueue(record)
	}
	for i := 0; i < diagnosticQueueCapacity+2; i++ {
		queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "transfer"})
	}
	if got := len(queue.records); got != diagnosticQueueCapacity {
		t.Fatalf("queued records = %d, want %d", got, diagnosticQueueCapacity)
	}
	if got := queue.dropped.Load(); got != 5 {
		t.Fatalf("dropped records = %d, want 5", got)
	}

	var rendered bytes.Buffer
	reporter := NewReporter(LogDebug, &rendered)
	for i := 0; i < len(input); i++ {
		record := <-queue.records
		retained := len(record.Phase) + len(record.Detail) + len(record.Endpoint.Scheme) + len(record.Endpoint.Host)
		if retained > DefaultDiagnosticBytes {
			t.Fatalf("record %d retained %d text bytes, limit %d", i, retained, DefaultDiagnosticBytes)
		}
		queued := record.Phase + record.Detail + record.Endpoint.Scheme + record.Endpoint.Host
		for _, secret := range []string{"user", "pass", "secret-path", "secret=query", "alice", "private", "token=", "credential", "magnet:?", "bob", "hidden", "secret=yes", "\x1b"} {
			if strings.Contains(queued, secret) {
				t.Errorf("queued record %d retained %q: %#v", i, secret, record)
			}
		}
		renderDiagnostic(reporter, record)
	}
	got := rendered.String()
	for _, secret := range []string{"user", "pass", "secret-path", "secret=query", "alice", "private", "token=", "credential", "magnet:?", "bob", "hidden", "secret=yes", "\x1b"} {
		if strings.Contains(got, secret) {
			t.Errorf("rendered diagnostic exposed %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "https://[2001:db8::1]:443") {
		t.Fatalf("IPv6 tracker host was not retained safely: %q", got)
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if len(strings.TrimPrefix(line, "debug: ")) > DefaultDiagnosticBytes {
			t.Fatalf("rendered diagnostic exceeded %d bytes: %d", DefaultDiagnosticBytes, len(line))
		}
	}
}

func TestDiagnosticQueueDoesNotBlockWhenFull(t *testing.T) {
	queue := newDiagnosticQueue()
	for i := 0; i < diagnosticQueueCapacity; i++ {
		queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition})
	}
	done := make(chan struct{})
	go func() {
		queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition})
		close(done)
	}()
	<-done
	if got := queue.dropped.Load(); got != 1 {
		t.Fatalf("dropped records = %d, want 1", got)
	}
}
