package cli

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/session"
)

const (
	diagnosticQueueCapacity = 128
	reportQueueCapacity     = 256
	secondaryFailureLimit   = 4
)

type diagnosticQueue struct {
	records chan session.Diagnostic
	dropped atomic.Uint64
}

type reportEventKind uint8

const (
	reportPhase reportEventKind = iota + 1
	reportWarning
	reportStatus
	reportPhaseStatus
)

type reportEvent struct {
	kind       reportEventKind
	text       string
	status     Status
	at         time.Time
	generation uint64
	version    uint64
}

type reportQueue struct {
	records chan reportEvent
	dropped atomic.Uint64
}

type secondaryFailures struct {
	mu      sync.Mutex
	count   int
	records [secondaryFailureLimit]string
}

func newDiagnosticQueue() *diagnosticQueue {
	return &diagnosticQueue{records: make(chan session.Diagnostic, diagnosticQueueCapacity)}
}

func newReportQueue() *reportQueue {
	return &reportQueue{records: make(chan reportEvent, reportQueueCapacity)}
}

// enqueuePrepared retains already-sanitized phase or warning lines without
// waiting for the reporting worker. Queue pressure is summarized when it joins.
func (q *reportQueue) enqueuePrepared(kind reportEventKind, text string) {
	select {
	case q.records <- reportEvent{kind: kind, text: text}:
	default:
		incrementSaturating(&q.dropped)
	}
}

func (q *reportQueue) enqueueStatus(generation uint64, snapshot Status, at time.Time, version uint64) bool {
	select {
	case q.records <- reportEvent{kind: reportStatus, status: snapshot, at: at, generation: generation, version: version}:
		return true
	default:
		return false
	}
}

func (q *reportQueue) enqueuePhaseStatus(snapshot Status, at time.Time, version uint64) bool {
	select {
	case q.records <- reportEvent{kind: reportPhaseStatus, status: snapshot, at: at, version: version}:
		return true
	default:
		return false
	}
}

// add retains every session secondary error in a fixed array. Session has at
// most four OnSecondary boundaries: metadata finalization, session cleanup,
// transfer-stage cleanup, and final tracker events.
func (q *secondaryFailures) add(err error) {
	if err == nil {
		return
	}
	text := SanitizeDiagnostic(err.Error())
	q.mu.Lock()
	if q.count < len(q.records) {
		q.records[q.count] = text
		q.count++
	}
	q.mu.Unlock()
}

func (q *secondaryFailures) snapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.records[:q.count]...)
}

func incrementSaturating(counter *atomic.Uint64) {
	for {
		old := counter.Load()
		if old == math.MaxUint64 || counter.CompareAndSwap(old, old+1) {
			return
		}
	}
}

// enqueue sanitizes and bounds all retained text before the nonblocking send.
// The total retained string data is at most DefaultDiagnosticBytes per record.
func (q *diagnosticQueue) enqueue(record session.Diagnostic) {
	remaining := DefaultDiagnosticBytes
	bounded := func(value string) string {
		if remaining <= 0 {
			return ""
		}
		value = SanitizeDiagnostic(value, remaining)
		remaining -= len(value)
		return value
	}
	record.Phase = bounded(record.Phase)
	record.Detail = bounded(record.Detail)
	if !record.Peer.Addr.IsValid() || record.Peer.Port == 0 || record.Peer.Addr.Zone() != "" {
		record.Peer = peer.Endpoint{}
	}
	if record.Endpoint.Scheme != "" || record.Endpoint.Host != "" {
		raw := (&url.URL{Scheme: record.Endpoint.Scheme, Host: record.Endpoint.Host}).String()
		tracker := RedactTrackerURL(raw)
		parts := strings.SplitN(tracker, "://", 2)
		if len(parts) == 2 {
			record.Endpoint = session.DiagnosticEndpoint{Scheme: bounded(parts[0]), Host: bounded(parts[1])}
		} else {
			record.Endpoint = session.DiagnosticEndpoint{}
		}
	}
	select {
	case q.records <- record:
	default:
		incrementSaturating(&q.dropped)
	}
}

func renderDiagnostic(reporter *Reporter, diagnostic session.Diagnostic) {
	var fields []string
	if diagnostic.Endpoint.Host != "" {
		fields = append(fields, "tracker="+diagnostic.Endpoint.Scheme+"://"+diagnostic.Endpoint.Host)
	}
	if endpoint := formatDiagnosticPeer(diagnostic.Peer); endpoint != "" {
		fields = append(fields, "peer="+endpoint)
	}
	if diagnostic.Count != 0 {
		fields = append(fields, fmt.Sprintf("count=%d", diagnostic.Count))
	}
	if diagnostic.IPv4Count != 0 || diagnostic.IPv6Count != 0 {
		fields = append(fields, fmt.Sprintf("compact-v4=%d compact-v6=%d", diagnostic.IPv4Count, diagnostic.IPv6Count))
	}
	if diagnostic.Duration != 0 {
		fields = append(fields, "duration="+diagnostic.Duration.Round(time.Millisecond).String())
	}
	if diagnostic.Detail != "" {
		fields = append(fields, diagnostic.Detail)
	}
	var message string
	switch diagnostic.Kind {
	case session.DiagnosticPhaseTransition:
		message = "session phase=" + diagnostic.Phase
	case session.DiagnosticTrackerAttempt:
		message = "tracker attempt phase=" + diagnostic.Phase
	case session.DiagnosticMetadataRefusal:
		message = "metadata refusal phase=" + diagnostic.Phase
	case session.DiagnosticPeerSelection:
		message = "peer selection phase=" + diagnostic.Phase
	case session.DiagnosticLifecycle:
		message = "session lifecycle"
	case session.DiagnosticTransfer:
		message = "transfer"
	case session.DiagnosticTransportRace:
		message = "transport race"
	default:
		return
	}
	if len(fields) != 0 {
		message += " " + strings.Join(fields, " ")
	}
	_ = reporter.Debug("%s", message)
}

// formatDiagnosticPeer accepts only normalized numeric addresses without zones.
// Its output is at most 47 ASCII bytes: bracketed IPv6 plus a 16-bit port.
func formatDiagnosticPeer(endpoint peer.Endpoint) string {
	if !endpoint.Addr.IsValid() || endpoint.Addr.Zone() != "" || endpoint.Port == 0 {
		return ""
	}
	return net.JoinHostPort(endpoint.Addr.String(), strconv.Itoa(int(endpoint.Port)))
}
