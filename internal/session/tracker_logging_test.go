package session

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/tracker"
)

func TestTrackerDiagnosticsKeepSafeFailureCauses(t *testing.T) {
	const secret = "secret\nhttps://user:password@example.test/private?token=secret"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"HTTP endpoint", &tracker.HTTPError{Code: tracker.HTTPErrorMalformed, Cause: tracker.CauseInvalidCompactEndpoint, StatusCode: 200, Err: errors.New(secret)}, "code=malformed cause=invalid-compact-endpoint status=200"},
		{"UDP endpoint", &tracker.Error{Code: tracker.ErrorMalformed, Cause: tracker.CauseInvalidCompactEndpoint, Operation: "announce", Err: errors.New(secret)}, "code=malformed cause=invalid-compact-endpoint operation=announce"},
		{"UDP framing", &tracker.Error{Code: tracker.ErrorMalformed, Cause: tracker.CauseIncompleteCompactPeer}, "code=malformed cause=incomplete-compact-peer"},
		{"UDP interval", &tracker.Error{Code: tracker.ErrorMalformed, Cause: tracker.CauseInvalidInterval}, "code=malformed cause=invalid-interval"},
		{"UDP deadline", &tracker.Error{Code: tracker.ErrorTimeout, Err: context.DeadlineExceeded}, "code=timeout cause=deadline-exceeded"},
		{"UDP cancellation", &tracker.Error{Code: tracker.ErrorCanceled, Err: context.Canceled}, "code=canceled cause=canceled"},
		{"HTTP truncated body", &tracker.HTTPError{Code: tracker.HTTPErrorResponse, Err: io.ErrUnexpectedEOF}, "code=response cause=incomplete-response"},
		{"untyped deadline", context.DeadlineExceeded, "transaction failed cause=deadline-exceeded"},
		{"unknown fields", &tracker.Error{Code: tracker.ErrorCode(secret), Cause: tracker.FailureCause(255), Operation: secret, Err: errors.New(secret)}, "code=unknown cause=unknown"},
		{"untyped text", errors.New(secret), "transaction failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []Diagnostic
			c := &coordinator{config: RunConfig{
				OnDiagnostic: func(event Diagnostic) { events = append(events, event) },
				OnWarning:    func(string) { t.Error("final failure became a retry warning") },
			}}
			c.observeTracker(tracker.Update{
				Tracker: "https://user:password@example.test/private?token=secret", Phase: tracker.TransferPhase,
				Request: tracker.AnnounceRequest{Event: tracker.EventStopped}, Attempted: true, Transmitted: true, Err: tc.err,
			})
			c.drainTrackerWarnings()
			if len(events) != 1 || !strings.Contains(events[0].Detail, "response failed, final event: ") || !strings.Contains(events[0].Detail, tc.want) {
				t.Fatalf("diagnostics = %+v, want %q", events, tc.want)
			}
			event := events[0]
			if event.Endpoint != (DiagnosticEndpoint{Scheme: "https", Host: "example.test"}) || strings.ContainsAny(event.Detail, "\n\r") || strings.Contains(event.Detail, "secret") || strings.Contains(event.Detail, "password") || len(event.Detail) > 256 {
				t.Fatalf("unsafe/unbounded diagnostic: %+v", event)
			}
		})
	}
}
