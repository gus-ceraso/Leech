package cli

import (
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/session"
)

func TestDiagnosticQueueBoundsRedactsAndDrops(t *testing.T) {
	queue := newDiagnosticQueue()
	queue.enqueue(session.Diagnostic{
		Kind: session.DiagnosticPhaseTransition, Phase: strings.Repeat("p", DefaultDiagnosticBytes),
		Endpoint: session.DiagnosticEndpoint{Scheme: "https", Host: "user:pass@[2001:db8::1]:443/path?secret=yes"},
		Detail:   "magnet:?xt=urn:btih:0123456789012345678901234567890123456789\x1b",
	})
	for i := 0; i < diagnosticQueueCapacity+2; i++ {
		queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "transfer"})
	}
	if got := len(queue.records); got != diagnosticQueueCapacity {
		t.Fatalf("queued records = %d, want %d", got, diagnosticQueueCapacity)
	}
	if got := queue.dropped.Load(); got != 3 {
		t.Fatalf("dropped records = %d, want 3", got)
	}
	first := <-queue.records
	retained := len(first.Phase) + len(first.Detail) + len(first.Endpoint.Scheme) + len(first.Endpoint.Host)
	if retained > DefaultDiagnosticBytes {
		t.Fatalf("retained %d text bytes, limit %d", retained, DefaultDiagnosticBytes)
	}
	if strings.Contains(first.Endpoint.Host, "user") || strings.Contains(first.Endpoint.Host, "path") || strings.Contains(first.Endpoint.Host, "secret") {
		t.Fatalf("tracker endpoint was not redacted: %#v", first.Endpoint)
	}
	if strings.Contains(first.Detail, "magnet:?") || strings.Contains(first.Detail, "\x1b") {
		t.Fatalf("detail was not redacted/escaped: %q", first.Detail)
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
