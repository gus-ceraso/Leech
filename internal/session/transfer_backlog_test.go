package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

// This local fixture forces a legal response burst to remain queued across a
// drain pass. The barrier exposes a valid I/O-worker schedule, not extra input:
// every Piece answers one of this peer's 32 outstanding requests.
func TestTransferValidPieceBacklogYieldsWithoutDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := bytes.Repeat([]byte{0x51}, 32*limits.BlockBytes)
		meta := singleFileMeta(data)
		selection, err := torrent.Select(meta, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		transfer, p, remote := reviewTransfer(t, selection, meta, Config{Shuffle: keepTieOrder})
		output, err := storage.Validate(t.TempDir(), meta, selection.SelectedIndices())
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := output.Prepare(storage.Overwrite)
		if err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		transfer.output = output
		p.state.SetReqQ(32)
		if err := transfer.scheduler.SetPeerLimit(p.input.ID, 32); err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		workers.Go(func() { _, _ = io.Copy(io.Discard, remote) })
		t.Cleanup(func() {
			_ = remote.Close()
			workers.Wait()
		})
		reviewAdvertise(t, transfer, p, 0)
		peers := []*transferPeer{p}
		if err := transfer.drive(context.Background(), &peers); err != nil {
			t.Fatal(err)
		}
		if len(p.active) != 32 {
			t.Fatalf("outstanding = %d, want 32", len(p.active))
		}
		done := make(chan error, 1)
		workers.Go(func() {
			for block := 0; block < 32; block++ {
				payload := make([]byte, 8+limits.BlockBytes)
				binary.BigEndian.PutUint32(payload[4:8], uint32(block*limits.BlockBytes))
				copy(payload[8:], data[block*limits.BlockBytes:(block+1)*limits.BlockBytes])
				if err := writeFixtureFrame(remote, peer.PieceID, payload); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		})
		transfer.onPayloadReceived = func(int64) error {
			// Let the bounded reader fill its four-slot queue and then block.
			synctest.Wait()
			return nil
		}
		synctest.Wait()
		if err := transfer.drainQueuedPeerEvents(context.Background(), peers, true); err != nil {
			t.Fatal(err)
		}
		if p.done {
			t.Fatalf("valid requested Piece burst disconnected after %d/%d accepted blocks", 32-transfer.scheduler.pieces[0].remaining, 32)
		}
		if got := transfer.scheduler.pieces[0].remaining; got != 16 {
			t.Fatalf("first pass remaining = %d, want 16 (bounded work)", got)
		}
		if err := transfer.drainQueuedPeerEvents(context.Background(), peers, true); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !transfer.scheduler.IsComplete() {
			t.Fatal("second bounded pass did not complete the piece")
		}
		got, err := os.ReadFile(filepath.Join(output.Root(), meta.Name))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("verified output mismatch: bytes=%d err=%v", len(got), err)
		}
	})
}
