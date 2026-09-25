package main

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/cli"
)

func TestRunWithSignalsExitsOnSecondSignalDuringCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan cli.SignalEvent, 2)
	runStarted := make(chan struct{})
	finishRun := make(chan struct{})
	exitCode := make(chan int, 1)
	var closed atomic.Bool

	done := make(chan int, 1)
	go func() {
		done <- runWithSignals(ctx, func() <-chan cli.SignalEvent { return events }, func() { closed.Store(true) }, func() error {
			close(runStarted)
			<-finishRun
			return errors.New("canceled")
		}, func(code int) { exitCode <- code })
	}()
	<-runStarted

	cancel()
	events <- cli.SignalEvent{Signal: os.Interrupt, ExitCode: 130}
	events <- cli.SignalEvent{Signal: syscall.SIGTERM, ExitCode: 143, Immediate: true}
	select {
	case code := <-exitCode:
		if code != 143 {
			t.Fatalf("immediate exit code = %d, want 143", code)
		}
	case <-time.After(time.Second):
		t.Fatal("second signal did not request immediate exit while cleanup was blocked")
	}
	select {
	case <-done:
		t.Fatal("runner returned before blocked cleanup finished")
	default:
	}

	close(finishRun)
	select {
	case code := <-done:
		if code != 130 {
			t.Fatalf("graceful signal exit code = %d, want 130", code)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not return after cleanup finished")
	}
	if !closed.Load() {
		t.Fatal("signal source was not closed after cleanup")
	}
}
