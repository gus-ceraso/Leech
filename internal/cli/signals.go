package cli

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// SignalEvent describes one termination signal received by a session. The
// first event requests graceful cancellation; a second event is marked
// Immediate so the process owner can stop waiting without more cleanup.
type SignalEvent struct {
	Signal    os.Signal
	ExitCode  int
	Immediate bool
}

// SignalAdapter owns process signal delivery for one reusable session. It
// never calls os.Exit: the command boundary maps ExitCode to process exit,
// while a library caller may choose another policy.
type SignalAdapter struct {
	events      chan SignalEvent
	signals     <-chan os.Signal
	cancel      func()
	stopSignals func()
	closed      chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
}

// NewSignalAdapter subscribes to SIGINT and SIGTERM and starts the adapter.
// The first received signal invokes cancel once and is reported as graceful.
func NewSignalAdapter(cancel func()) *SignalAdapter {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	return newSignalAdapter(signals, cancel, func() { signal.Stop(signals) })
}

// newSignalAdapter is the deterministic source seam used by presentation
// tests. stopSignals may be nil for a caller-owned channel.
func newSignalAdapter(signals <-chan os.Signal, cancel func(), stopSignals func()) *SignalAdapter {
	adapter := &SignalAdapter{
		events:      make(chan SignalEvent, 2),
		signals:     signals,
		cancel:      cancel,
		stopSignals: stopSignals,
		closed:      make(chan struct{}),
		done:        make(chan struct{}),
	}
	go adapter.run()
	return adapter
}

// Events returns the stream of received signals. It is closed after Close
// joins the adapter goroutine.
func (a *SignalAdapter) Events() <-chan SignalEvent {
	if a == nil {
		return nil
	}
	return a.events
}

// Wait waits for the next signal, returning false when the adapter was closed
// without receiving one.
func (a *SignalAdapter) Wait() (SignalEvent, bool) {
	if a == nil {
		return SignalEvent{}, false
	}
	event, ok := <-a.events
	return event, ok
}

// Close unsubscribes from process signals and joins the adapter goroutine.
func (a *SignalAdapter) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		close(a.closed)
		if a.stopSignals != nil {
			a.stopSignals()
		}
		<-a.done
		close(a.events)
	})
}

func (a *SignalAdapter) run() {
	defer close(a.done)
	first := true
	for {
		select {
		case <-a.closed:
			return
		case sig, ok := <-a.signals:
			if !ok {
				return
			}
			if sig == nil {
				continue
			}
			event := SignalEvent{Signal: sig, ExitCode: SignalExitCode(sig)}
			if first {
				first = false
				if a.cancel != nil {
					a.cancel()
				}
			} else {
				event.Immediate = true
			}
			select {
			case a.events <- event:
			case <-a.closed:
				return
			}
		}
	}
}

// SignalExitCode maps the supported signals to the shell status described in
// DESIGN §4.10. Unknown signals return the ordinary runtime failure status.
func SignalExitCode(sig os.Signal) int {
	switch sig {
	case os.Interrupt:
		return 130
	case syscall.SIGTERM:
		return 143
	default:
		return 1
	}
}
