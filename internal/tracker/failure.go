package tracker

import (
	"context"
	"errors"
	"io"
	"net"
)

// FailureCause is an observational refinement of a transaction's error code.
// Parsers set it at the failing check; it never controls retries or admission.
// String returns only fixed labels, including for unknown numeric values.
type FailureCause uint8

const (
	CauseUnknown FailureCause = iota
	CauseInvalidBencode
	CauseInvalidResponseField
	CauseInvalidDictionaryPeer
	CauseIncompleteResponse
	CauseIncompleteCompactPeer
	CauseInvalidCompactEndpoint
	CauseInvalidInterval
	CauseInvalidRetryDelay
	CauseDatagramLimit
	CauseDeadline
	CauseTimeout
	CauseCanceled
)

func (c FailureCause) String() string {
	switch c {
	case CauseInvalidBencode:
		return "invalid-bencode"
	case CauseInvalidResponseField:
		return "invalid-response-field"
	case CauseInvalidDictionaryPeer:
		return "invalid-dictionary-peer"
	case CauseIncompleteResponse:
		return "incomplete-response"
	case CauseIncompleteCompactPeer:
		return "incomplete-compact-peer"
	case CauseInvalidCompactEndpoint:
		return "invalid-compact-endpoint"
	case CauseInvalidInterval:
		return "invalid-interval"
	case CauseInvalidRetryDelay:
		return "invalid-retry-delay"
	case CauseDatagramLimit:
		return "datagram-limit"
	case CauseDeadline:
		return "deadline-exceeded"
	case CauseTimeout:
		return "timeout"
	case CauseCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// CauseOf preserves parser classifications through wrapping, or classifies
// standard I/O/context failures without inspecting potentially untrusted text.
func CauseOf(err error) FailureCause {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.Cause != CauseUnknown {
		return httpErr.Cause
	}
	var udpErr *Error
	if errors.As(err, &udpErr) && udpErr.Cause != CauseUnknown {
		return udpErr.Cause
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CauseDeadline
	case errors.Is(err, context.Canceled):
		return CauseCanceled
	case errors.Is(err, ErrTimeout), errors.Is(err, ErrHTTPTimeout):
		return CauseTimeout
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return CauseIncompleteResponse
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return CauseTimeout
	}
	return CauseUnknown
}

// ctxErr is the non-nil result of ctx.Err(). Deadline expiry is not ordinary
// cancellation; retain the original error identity and transmission accounting.
func udpContextError(operation string, transmitted bool, ctxErr error) *Error {
	code := ErrorCanceled
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		code = ErrorTimeout
	}
	return &Error{Code: code, Cause: CauseOf(ctxErr), Operation: operation, Transmitted: transmitted, Err: ctxErr}
}
