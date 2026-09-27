package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAttemptObservationsClassifyWithoutErrorText(t *testing.T) {
	for _, test := range []struct {
		err, contextErr error
		outcome         AttemptOutcome
		failure         DialFailure
	}{
		{nil, context.Canceled, AttemptSucceeded, DialFailureNone},
		{fmt.Errorf("wrapped: %w", syscall.ECONNREFUSED), nil, AttemptFailed, DialFailureRefused},
		{syscall.ECONNRESET, nil, AttemptFailed, DialFailureReset},
		{syscall.ENETUNREACH, nil, AttemptFailed, DialFailureUnreachable},
		{ErrExpectedPeerIDMismatch, nil, AttemptFailed, DialFailureExpectedID},
		{protocolError("read handshake", "untrusted-secret"), nil, AttemptFailed, DialFailureProtocol},
		{io.ErrUnexpectedEOF, nil, AttemptFailed, DialFailureEOF},
		{net.ErrClosed, nil, AttemptFailed, DialFailureClosed},
		{net.ErrClosed, context.Canceled, AttemptCanceled, DialFailureCanceled},
		{io.ErrClosedPipe, context.DeadlineExceeded, AttemptFailed, DialFailureTimeout},
		{io.EOF, context.Canceled, AttemptFailed, DialFailureEOF},
		{errors.New("untrusted-secret https://user:password@example.test/private?token=secret"), nil, AttemptFailed, DialFailureIO},
	} {
		got := observeAttempt(AttemptReadHandshake, test.err, test.contextErr, time.Second)
		if got.Outcome != test.outcome || got.Failure != test.failure || got.Stage != AttemptReadHandshake || got.Duration != time.Second {
			t.Errorf("classification = %+v, want %s/%s", got, test.outcome, test.failure)
		}
		if strings.Contains(fmt.Sprint(got), "secret") {
			t.Fatalf("observation retained raw error: %+v", got)
		}
	}
}
