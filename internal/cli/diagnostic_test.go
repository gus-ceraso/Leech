package cli

import (
	"bytes"
	"errors"
	"net/netip"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/peer"
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

func TestReportQueueAndSecondaryRetentionAreBounded(t *testing.T) {
	queue := newReportQueue()
	queue.enqueuePrepared(reportWarning, SanitizeDiagnostic("warning https://user:password@example.test/private?token=secret"+strings.Repeat("x", DefaultDiagnosticBytes*2)))
	for i := 0; i < reportQueueCapacity+2; i++ {
		queue.enqueuePrepared(reportPhase, strings.Repeat("p", DefaultDiagnosticBytes))
	}
	if got := len(queue.records); got != reportQueueCapacity {
		t.Fatalf("queued report records = %d, want %d", got, reportQueueCapacity)
	}
	if got := queue.dropped.Load(); got != 3 {
		t.Fatalf("dropped report records = %d, want 3", got)
	}
	for len(queue.records) != 0 {
		event := <-queue.records
		if len(event.text) > DefaultDiagnosticBytes {
			t.Fatalf("retained report text = %d bytes, limit %d", len(event.text), DefaultDiagnosticBytes)
		}
		if strings.Contains(event.text, "password") || strings.Contains(event.text, "/private") || strings.Contains(event.text, "token=secret") {
			t.Fatalf("report queue retained sensitive data: %q", event.text)
		}
	}

	secondaries := &secondaryFailures{}
	for i := 0; i < secondaryFailureLimit; i++ {
		secondaries.add(errors.New("shutdown https://user:password@example.test/path?token=secret" + strings.Repeat("x", DefaultDiagnosticBytes*2)))
	}
	retained := secondaries.snapshot()
	if len(retained) != secondaryFailureLimit {
		t.Fatalf("retained secondary failures = %d, want %d", len(retained), secondaryFailureLimit)
	}
	for _, message := range retained {
		if len(message) > DefaultDiagnosticBytes || strings.Contains(message, "password") || strings.Contains(message, "/path") || strings.Contains(message, "token=secret") {
			t.Fatalf("secondary failure retention was not bounded/redacted: %q", message)
		}
	}
}

func TestDiagnosticQueueRetainsAndRendersTypedPeerEndpoint(t *testing.T) {
	queue := newDiagnosticQueue()
	endpoint := peer.Endpoint{Addr: netip.MustParseAddr("2001:db8::1"), Port: 51413}
	queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "transfer", Peer: endpoint})
	retained := <-queue.records
	if retained.Peer != endpoint {
		t.Fatalf("queued peer endpoint = %+v, want %+v", retained.Peer, endpoint)
	}

	var output bytes.Buffer
	reporter := NewReporter(LogDebug, &output)
	renderDiagnostic(reporter, retained)
	if got, want := output.String(), "debug: session phase=transfer peer=[2001:db8::1]:51413\n"; got != want {
		t.Fatalf("rendered peer endpoint = %q, want %q", got, want)
	}

	output.Reset()
	renderDiagnostic(reporter, session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "metadata"})
	if strings.Contains(output.String(), "peer=") {
		t.Fatalf("absent peer endpoint was rendered: %q", output.String())
	}
	output.Reset()
	zoned := session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "transfer", Peer: peer.Endpoint{Addr: netip.MustParseAddr("fe80::1%eth0"), Port: 51413}}
	queue.enqueue(zoned)
	retained = <-queue.records
	if retained.Peer != (peer.Endpoint{}) {
		t.Fatalf("queue retained zoned peer endpoint: %+v", retained.Peer)
	}
	renderDiagnostic(reporter, retained)
	if strings.Contains(output.String(), "peer=") || strings.Contains(output.String(), "eth0") {
		t.Fatalf("zoned peer endpoint was rendered: %q", output.String())
	}
	queue.enqueue(session.Diagnostic{Kind: session.DiagnosticPhaseTransition, Phase: "transfer", Peer: peer.Endpoint{Port: 51413}})
	retained = <-queue.records
	if retained.Peer != (peer.Endpoint{}) || formatDiagnosticPeer(retained.Peer) != "" {
		t.Fatalf("queue retained invalid peer endpoint: %+v", retained.Peer)
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
