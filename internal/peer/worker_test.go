package peer

import (
	"context"
	"errors"
	"net"
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
	_ = remote.Close()
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
