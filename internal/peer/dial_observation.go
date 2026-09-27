package peer

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

// AttemptOutcome and AttemptStage describe only local, bounded observations.
// They never retain an error string or peer-supplied handshake bytes.
type AttemptOutcome uint8

const (
	AttemptNotStarted AttemptOutcome = iota
	AttemptSucceeded
	AttemptFailed
	AttemptCanceled
)

func (o AttemptOutcome) String() string {
	switch o {
	case AttemptSucceeded:
		return "succeeded"
	case AttemptFailed:
		return "failed"
	case AttemptCanceled:
		return "canceled"
	default:
		return "not-started"
	}
}

type AttemptStage uint8

const (
	AttemptDial AttemptStage = iota
	AttemptWriteHandshake
	AttemptReadHandshake
	AttemptComplete
)

func (s AttemptStage) String() string {
	switch s {
	case AttemptWriteHandshake:
		return "handshake-write"
	case AttemptReadHandshake:
		return "handshake-read"
	case AttemptComplete:
		return "complete"
	default:
		return "dial"
	}
}

type DialFailure uint8

const (
	DialFailureNone DialFailure = iota
	DialFailureCanceled
	DialFailureTimeout
	DialFailureRefused
	DialFailureReset
	DialFailureUnreachable
	DialFailureEOF
	DialFailureClosed
	DialFailureProtocol
	DialFailureExpectedID
	DialFailureIO
)

func (f DialFailure) String() string {
	switch f {
	case DialFailureNone:
		return "none"
	case DialFailureCanceled:
		return "canceled"
	case DialFailureTimeout:
		return "timeout"
	case DialFailureRefused:
		return "connection-refused"
	case DialFailureReset:
		return "connection-reset"
	case DialFailureUnreachable:
		return "network-unreachable"
	case DialFailureEOF:
		return "eof-or-truncated"
	case DialFailureClosed:
		return "closed"
	case DialFailureProtocol:
		return "invalid-handshake"
	case DialFailureExpectedID:
		return "tracker-peer-id-mismatch"
	default:
		return "io-error"
	}
}

// AttemptObservation includes dial and BEP 3 handshake time, not queue wait.
// Succeeded means a valid handshake, even if another transport won the race.
type AttemptObservation struct {
	Outcome  AttemptOutcome
	Stage    AttemptStage
	Failure  DialFailure
	Duration time.Duration
}

// RaceObservation is filled after both attempts have joined. An unstarted
// transport is distinct from a canceled loser. Winner is independent of later
// live-peer admission, which can reject a valid handshake's peer ID.
type RaceObservation struct {
	TCP, UTP AttemptObservation
	Winner   Transport
	Duration time.Duration
}

func (r *RaceObservation) record(result raceAttemptResult) {
	if result.transport == TransportTCP {
		r.TCP = result.observation
	} else {
		r.UTP = result.observation
	}
}

func observeAttempt(stage AttemptStage, err, contextErr error, elapsed time.Duration) AttemptObservation {
	observation := AttemptObservation{Stage: stage, Duration: elapsed, Outcome: AttemptSucceeded}
	if err == nil {
		return observation
	}
	// Owned cancellation closes a stalled stream. Preserve independently
	// meaningful errors (refusal, EOF, protocol failure) rather than relabeling
	// every result drained after the winner as canceled.
	if contextErr != nil && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, context.Canceled)) {
		err = contextErr
	}
	observation.Outcome = AttemptFailed
	var timeout net.Error
	switch {
	case errors.Is(err, context.Canceled):
		observation.Outcome = AttemptCanceled
		observation.Failure = DialFailureCanceled
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout():
		observation.Failure = DialFailureTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		observation.Failure = DialFailureRefused
	case errors.Is(err, syscall.ECONNRESET):
		observation.Failure = DialFailureReset
	case errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH):
		observation.Failure = DialFailureUnreachable
	case errors.Is(err, ErrExpectedPeerIDMismatch):
		observation.Failure = DialFailureExpectedID
	case IsProtocolViolation(err):
		observation.Failure = DialFailureProtocol
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		observation.Failure = DialFailureEOF
	case errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe):
		observation.Failure = DialFailureClosed
	default:
		observation.Failure = DialFailureIO
	}
	return observation
}
