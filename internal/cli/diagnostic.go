package cli

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
)

const diagnosticQueueCapacity = 128

type diagnosticQueue struct {
	records chan session.Diagnostic
	dropped atomic.Uint64
}

func newDiagnosticQueue() *diagnosticQueue {
	return &diagnosticQueue{records: make(chan session.Diagnostic, diagnosticQueueCapacity)}
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
		for {
			old := q.dropped.Load()
			if old == math.MaxUint64 || q.dropped.CompareAndSwap(old, old+1) {
				break
			}
		}
	}
}

func renderDiagnostic(reporter *Reporter, diagnostic session.Diagnostic) {
	if diagnostic.Kind != session.DiagnosticPhaseTransition {
		return
	}
	var fields []string
	if diagnostic.Endpoint.Host != "" {
		fields = append(fields, "tracker="+diagnostic.Endpoint.Scheme+"://"+diagnostic.Endpoint.Host)
	}
	if diagnostic.Count != 0 {
		fields = append(fields, fmt.Sprintf("count=%d", diagnostic.Count))
	}
	if diagnostic.Duration != 0 {
		fields = append(fields, "duration="+diagnostic.Duration.Round(time.Millisecond).String())
	}
	if diagnostic.Detail != "" {
		fields = append(fields, diagnostic.Detail)
	}
	message := "session phase=" + diagnostic.Phase
	if len(fields) != 0 {
		message += " " + strings.Join(fields, " ")
	}
	_ = reporter.Debug("%s", message)
}
