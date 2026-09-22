package peer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConnectionWorkerClosesWhenEventQueueIsBlocked(t *testing.T) {
	local, remote := net.Pipe()
	worker, err := NewConnectionWorkerWithCaps(local, ReadOptions{}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	go func() {
		_ = WriteChoke(remote)
		_ = WriteChoke(remote)
		_ = remote.Close()
	}()
	select {
	case <-worker.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after its event queue filled")
	}
	if !errors.Is(worker.Err(), ErrEventQueueFull) {
		t.Fatalf("worker error = %v", worker.Err())
	}
	for range worker.Events() {
	}
	if err := worker.Err(); !errors.Is(err, ErrEventQueueFull) {
		t.Fatalf("closed Events lost queue overflow error: %v", err)
	}
	_ = remote.Close()
}

func TestConnectionWorkerCloseBeforeStartSealsWorker(t *testing.T) {
	local, remote := net.Pipe()
	worker := NewConnectionWorker(local, ReadOptions{})
	if err := worker.Close(); err != nil {
		t.Fatalf("Close before Start = %v", err)
	}
	worker.Start(context.Background())
	select {
	case _, ok := <-worker.Events():
		if ok {
			t.Fatal("sealed worker emitted an event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sealed worker Events channel remained open")
	}
	if err := worker.Wait(); !errors.Is(err, ErrWorkerClosed) && err != nil {
		t.Fatalf("Wait after Close before Start = %v", err)
	}
	_ = remote.Close()
}

func TestConnectionWorkerConcurrentStartClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		local, remote := net.Pipe()
		worker := NewConnectionWorker(local, ReadOptions{})
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			worker.Start(context.Background())
		}()
		go func() {
			defer group.Done()
			<-start
			_ = worker.Close()
		}()
		close(start)
		group.Wait()
		worker.Start(context.Background())
		if err := worker.Close(); err != nil && !errors.Is(err, ErrWorkerClosed) {
			t.Fatalf("iteration %d Close = %v", iteration, err)
		}
		select {
		case _, ok := <-worker.Events():
			if ok {
				for range worker.Events() {
				}
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d Events channel remained open", iteration)
		}
		_ = remote.Close()
	}
}

func TestConnectionWorkerBlockedSendUnblocksOnClose(t *testing.T) {
	local, remote := net.Pipe()
	worker, err := NewConnectionWorkerWithCaps(local, ReadOptions{}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	if err := worker.Send(Message{ID: ChokeID}); err != nil {
		t.Fatal(err)
	}
	if err := worker.Send(Message{ID: ChokeID}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := worker.SendContext(ctx, Message{ID: ChokeID}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked queue send = %v", err)
	}
	if err := worker.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	_ = remote.Close()
}

func TestConnectionWorkerReportsFastFrameAndJoins(t *testing.T) {
	local, remote := net.Pipe()
	worker := NewConnectionWorker(local, ReadOptions{Fast: true})
	worker.Start(context.Background())
	if err := WriteFastHaveNone(remote, [8]byte{7: FastExtensionBit}, [8]byte{7: FastExtensionBit}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-worker.Events():
		if event.Err != nil || event.Message.ID != HaveNoneID {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not report frame")
	}
	if err := worker.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	_ = remote.Close()
}
