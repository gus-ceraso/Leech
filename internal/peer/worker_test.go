package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestConnectionWorkerPreservesViolationWithFullEventQueue(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	worker := NewConnectionWorker(local, ReadOptions{})
	worker.Start(context.Background())
	for i := 0; i < maxPeerEventQueue; i++ {
		if err := WriteKeepAlive(remote); err != nil {
			t.Fatalf("keepalive %d: %v", i, err)
		}
	}
	var oversized [4]byte
	binary.BigEndian.PutUint32(oversized[:], MaxPeerFrameBytes+1)
	if _, err := remote.Write(oversized[:]); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after a protocol violation")
	}
	if !errors.Is(worker.Err(), ErrProtocolViolation) {
		t.Fatalf("worker error = %v", worker.Err())
	}
	var queued int
	for event := range worker.Events() {
		if event.Err != nil || !event.Message.KeepAlive {
			t.Fatalf("queued event %d = %#v", queued, event)
		}
		queued++
	}
	if queued != maxPeerEventQueue {
		t.Fatalf("queued events = %d, want %d", queued, maxPeerEventQueue)
	}
	if err := worker.Err(); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("closed Events lost protocol violation: %v", err)
	}
}

func TestConnectionWorkerBackpressuresValidBurst(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	worker := NewConnectionWorker(local, ReadOptions{PieceCount: 5, ValidateIndices: true})
	worker.Start(context.Background())
	defer worker.Close()

	for i := uint32(0); i < 5; i++ {
		var have [9]byte
		binary.BigEndian.PutUint32(have[:4], 5)
		have[4] = HaveID
		binary.BigEndian.PutUint32(have[5:], i)
		if n, err := remote.Write(have[:]); err != nil || n != len(have) {
			t.Fatalf("Have %d wrote %d/%d bytes: %v", i, n, len(have), err)
		}
	}
	select {
	case <-worker.done:
		t.Fatalf("valid burst closed worker: %v", worker.Err())
	case <-time.After(30 * time.Millisecond):
	}
	for i := uint32(0); i < 5; i++ {
		select {
		case event, ok := <-worker.Events():
			if !ok || event.Err != nil || event.Message.ID != HaveID || len(event.Message.Payload) != 4 || binary.BigEndian.Uint32(event.Message.Payload) != i {
				t.Fatalf("event %d = %#v, channel open = %t", i, event, ok)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for Have %d", i)
		}
	}
	if err := worker.Err(); err != nil {
		t.Fatalf("valid burst ended worker: %v", err)
	}
}

func TestConnectionWorkerCloseUnblocksBackpressuredReader(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	worker, err := NewConnectionWorkerWithCaps(local, ReadOptions{}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	for i := 0; i < 2; i++ {
		if err := WriteChoke(remote); err != nil {
			t.Fatalf("Choke %d: %v", i, err)
		}
	}
	select {
	case <-worker.done:
		t.Fatalf("full queue closed worker: %v", worker.Err())
	case <-time.After(30 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- worker.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join the backpressured reader")
	}
	var queued int
	for range worker.Events() {
		queued++
	}
	if queued != 1 {
		t.Fatalf("queued events = %d, want 1", queued)
	}
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
	if err := worker.Send(Message{ID: ChokeID}); !errors.Is(err, ErrWorkerClosed) {
		t.Fatalf("Send after Close before Start = %v", err)
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

func TestPeerEventRetentionBoundAcrossActivePeers(t *testing.T) {
	const maxBitfieldBytes = (limits.Pieces + 7) / 8
	bitfield := make([]byte, 4+1+maxBitfieldBytes)
	binary.BigEndian.PutUint32(bitfield[:4], uint32(1+maxBitfieldBytes))
	bitfield[4] = BitfieldID

	workers := make([]*ConnectionWorker, 0, limits.ActivePeers)
	for peerIndex := 0; peerIndex < limits.ActivePeers; peerIndex++ {
		local, remote := net.Pipe()
		worker, err := NewConnectionWorkerWithCaps(local, ReadOptions{
			PieceCount:      limits.Pieces,
			ValidateIndices: true,
		}, 1, maxPeerEventQueue)
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
		worker.Start(context.Background())
		for eventIndex := 0; eventIndex < maxPeerEventQueue; eventIndex++ {
			if n, err := remote.Write(bitfield); err != nil || n != len(bitfield) {
				t.Fatalf("peer %d frame %d wrote %d/%d bytes: %v", peerIndex, eventIndex, n, len(bitfield), err)
			}
		}
		_ = remote.Close()
	}

	var retained int64
	for peerIndex, worker := range workers {
		<-worker.done
		for event := range worker.Events() {
			if event.Err == nil {
				retained += int64(len(event.Message.Payload))
			}
		}
		_ = worker.Close()
		_ = worker.conn.Close()
		if cap(worker.events) != maxPeerEventQueue {
			t.Fatalf("peer %d event capacity = %d, want %d", peerIndex, cap(worker.events), maxPeerEventQueue)
		}
	}
	want := int64(limits.ActivePeers) * maxPeerEventQueue * maxBitfieldBytes
	if retained != want {
		t.Fatalf("queued payload bytes = %d, want maximum %d", retained, want)
	}
	if retained > 64<<20 {
		t.Fatalf("queued payload retention = %d bytes, exceeds 64 MiB", retained)
	}
}

func TestWorkerEventQueueRejectsExcessCapacity(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	if _, err := NewConnectionWorkerWithCaps(local, ReadOptions{}, 1, maxPeerEventQueue+1); !errors.Is(err, ErrWorkerConfig) {
		t.Fatalf("excess event capacity error = %v", err)
	}
	_ = local.Close()
}
