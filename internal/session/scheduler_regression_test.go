package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func reviewPiecePlan(t testing.TB, count int, length int64) (*torrent.SelectionPlan, torrent.Metainfo) {
	t.Helper()
	pieces := make([]torrent.Piece, count)
	for index := range pieces {
		pieces[index] = piece(index, int64(index)*length, int64(index+1)*length)
	}
	total := int64(count) * length
	meta := torrent.Metainfo{
		Name: "fixture", TotalLength: total, PieceLength: length,
		Files: []torrent.File{regularFile(0, "payload", 0, total)}, Pieces: pieces,
	}
	plan, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return plan, meta
}

func reviewTransfer(t testing.TB, plan *torrent.SelectionPlan, meta torrent.Metainfo, cfg Config) (*Transfer, *transferPeer, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	transfer, err := NewTransfer(TransferConfig{
		Selection: plan, Output: &storage.Plan{},
		Stager: storage.NewStager(storage.StagerConfig{
			CacheRoot: filepath.Join(t.TempDir(), "cache"),
			MaxPieces: cfg.MaxStagedPieces, MaxBytes: cfg.MaxStagedBytes,
		}),
		SchedulerConfig: cfg,
		Peers:           []ConnectedPeer{{ID: "replacement", Endpoint: endpoint(62), Conn: local, ReqQ: 1, ReqQSet: true}},
		PieceCount:      uint32(len(meta.Pieces)), PieceLength: uint32(meta.PieceLength), LastPieceLength: uint32(meta.PieceLength),
	})
	if err != nil {
		_ = local.Close()
		t.Fatal(err)
	}
	if err := transfer.stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.worker.Close()
		_ = transfer.stager.Cleanup(nil)
	})
	return transfer, p, remote
}

func reviewAdvertise(t testing.TB, transfer *Transfer, p *transferPeer, indices ...int) {
	t.Helper()
	bits := make([]byte, (transfer.pieceCount+7)/8)
	for _, index := range indices {
		bits[index/8] |= 0x80 >> uint(index%8)
	}
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.BitfieldID, Payload: bits}}); err != nil {
		t.Fatal(err)
	}
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.UnchokeID}}); err != nil {
		t.Fatal(err)
	}
}

func reviewReadRequest(t testing.TB, remote net.Conn) peer.Block {
	t.Helper()
	if err := remote.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	defer remote.SetReadDeadline(time.Time{})
	for {
		message, err := peer.ReadMessage(remote)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != peer.RequestID {
			continue
		}
		if len(message.Payload) != 12 {
			t.Fatalf("request payload length = %d", len(message.Payload))
		}
		return peer.Block{
			Index:  binary.BigEndian.Uint32(message.Payload[0:4]),
			Begin:  binary.BigEndian.Uint32(message.Payload[4:8]),
			Length: binary.BigEndian.Uint32(message.Payload[8:12]),
		}
	}
}

func reviewAdmitStage(t *testing.T, transfer *Transfer, peerID string) (PieceOffer, *storage.PieceStage) {
	t.Helper()
	offer, ok, err := transfer.scheduler.ReservePiece(peerID)
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %t, %v", offer, ok, err)
	}
	mapping, ok := transfer.selection.Piece(offer.PieceIndex)
	if !ok {
		t.Fatalf("missing piece %d", offer.PieceIndex)
	}
	stage, err := transfer.stager.AdmitPiece(mapping.Piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	if !setStage(transfer, offer.PieceIndex, stage) {
		t.Fatalf("duplicate stage %d", offer.PieceIndex)
	}
	return offer, stage
}

func assertSchedulerWork(tb testing.TB, s *Scheduler) {
	tb.Helper()
	remaining, pending, active := 0, 0, 0
	staged, reserved := 0, 0
	var stagedBytes, reservedBytes int64
	peerAssignments := make(map[string]int)
	for index, piece := range s.pieces {
		seenPending := make([]bool, len(piece.blocks))
		for pos, blockIndex := range piece.pending {
			if blockIndex < 0 || blockIndex >= len(piece.blocks) || seenPending[blockIndex] || piece.blocks[blockIndex].pendingPos != pos {
				tb.Fatalf("piece %d has invalid pending inverse at %d", index, pos)
			}
			seenPending[blockIndex] = true
		}
		pieceRemaining, activeBlocks := 0, 0
		for blockIndex, block := range piece.blocks {
			if !block.done {
				pieceRemaining++
			}
			if block.active != (len(block.assignments) != 0) {
				tb.Fatalf("piece %d block %d active assignment mismatch", index, blockIndex)
			}
			if block.active {
				activeBlocks++
				if block.done || seenPending[blockIndex] || piece.activeBlocks[block.block.Begin] != blockIndex || block.pendingPos != -1 {
					tb.Fatalf("piece %d block %d has invalid active index", index, blockIndex)
				}
				for peerID := range block.assignments {
					peerAssignments[peerID]++
					active++
				}
			} else if block.done {
				if seenPending[blockIndex] || block.pendingPos != -1 {
					tb.Fatalf("piece %d block %d is both done and pending", index, blockIndex)
				}
			} else if !seenPending[blockIndex] {
				tb.Fatalf("piece %d block %d is missing from pending work", index, blockIndex)
			}
		}
		if piece.remaining != pieceRemaining || len(piece.activeBlocks) != activeBlocks {
			tb.Fatalf("piece %d work counts = remaining %d/%d, active %d/%d", index, piece.remaining, pieceRemaining, len(piece.activeBlocks), activeBlocks)
		}
		if !piece.complete {
			remaining += pieceRemaining
			pending += len(piece.pending)
		}
		length := piece.plan.Piece.Range.End - piece.plan.Piece.Range.Begin
		switch piece.stage {
		case stageAdmitted:
			staged++
			stagedBytes += length
		case stageReserved:
			reserved++
			reservedBytes += length
		}
	}
	for peerID, p := range s.peers {
		if p.active != peerAssignments[peerID] {
			tb.Fatalf("peer %q active count = %d, want %d", peerID, p.active, peerAssignments[peerID])
		}
	}
	if s.remainingBlocks != remaining || s.pendingTotal != pending || s.active != active ||
		s.staged != staged || s.reserved != reserved || s.stagedBytes != stagedBytes || s.reservedBytes != reservedBytes {
		tb.Fatalf("scheduler counters differ: remaining %d/%d, pending %d/%d, active %d/%d, staged %d/%d, reserved %d/%d, bytes %d/%d + %d/%d",
			s.remainingBlocks, remaining, s.pendingTotal, pending, s.active, active, s.staged, staged, s.reserved, reserved, s.stagedBytes, stagedBytes, s.reservedBytes, reservedBytes)
	}
}

func TestReviewOrphanedStagesBlockAvailableWork(t *testing.T) {
	const length = int64(limits.BlockBytes)
	for _, tc := range []struct {
		name     string
		count    int
		maxBytes int64
	}{
		{name: "count", count: 64, maxBytes: 65 * length},
		{name: "bytes", count: 2, maxBytes: 2 * length},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, meta := reviewPiecePlan(t, 65, length)
			cfg := Config{MaxStagedPieces: 64, MaxStagedBytes: tc.maxBytes, Shuffle: keepTieOrder}
			transfer, replacement, remote := reviewTransfer(t, plan, meta, cfg)
			if err := transfer.scheduler.AddPeer("departing", endpoint(63)); err != nil {
				t.Fatal(err)
			}
			available := make([]int, tc.count)
			for i := range available {
				available[i] = i
			}
			if err := transfer.scheduler.SetAvailability("departing", available); err != nil {
				t.Fatal(err)
			}
			paths := make([]string, tc.count)
			for i := range paths {
				offer, stage := reviewAdmitStage(t, transfer, "departing")
				if offer.PieceIndex != i {
					t.Fatalf("stage %d reserved piece %d", i, offer.PieceIndex)
				}
				paths[i] = stage.Path()
			}
			if err := transfer.scheduler.RemovePeer("departing"); err != nil {
				t.Fatal(err)
			}
			reviewAdvertise(t, transfer, replacement, 64)
			live := []*transferPeer{replacement}
			if err := transfer.drive(context.Background(), &live); err != nil {
				t.Fatal(err)
			}
			if got := reviewReadRequest(t, remote); got != (peer.Block{Index: 64, Length: limits.BlockBytes}) {
				t.Fatalf("replacement request = %#v", got)
			}
			if transfer.scheduler.StagedPieces() != tc.count || transfer.stager.StagedCount() != tc.count ||
				transfer.scheduler.StagedBytes() > cfg.MaxStagedBytes || transfer.stager.StagedBytes() > cfg.MaxStagedBytes {
				t.Fatalf("stage budgets diverged: scheduler=%d/%d storage=%d/%d", transfer.scheduler.StagedPieces(), transfer.scheduler.StagedBytes(), transfer.stager.StagedCount(), transfer.stager.StagedBytes())
			}
			removed := 0
			for _, path := range paths {
				if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
					removed++
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if removed != 1 || len(transfer.stages) != tc.count {
				t.Fatalf("reclaimed files = %d, tracked stages = %d", removed, len(transfer.stages))
			}
		})
	}
}

func TestReviewStageReclaimRemovalFailureIsFatal(t *testing.T) {
	plan, meta := reviewPiecePlan(t, 2, limits.BlockBytes)
	transfer, replacement, _ := reviewTransfer(t, plan, meta, Config{MaxStagedPieces: 1, MaxStagedBytes: 2 * limits.BlockBytes, Shuffle: keepTieOrder})
	if err := transfer.scheduler.AddPeer("departing", endpoint(63)); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability("departing", []int{0}); err != nil {
		t.Fatal(err)
	}
	_, stage := reviewAdmitStage(t, transfer, "departing")
	if err := transfer.scheduler.RemovePeer("departing"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(stage.Path()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(stage.Path(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage.Path(), "keep"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	reviewAdvertise(t, transfer, replacement, 1)
	live := []*transferPeer{replacement}
	if err := transfer.drive(context.Background(), &live); !errors.Is(err, storage.ErrStagingFatal) {
		t.Fatalf("reclaim error = %v, want fatal staging error", err)
	}
}

func TestReviewHealthyReplacementCanUseRetainedStage(t *testing.T) {
	plan, meta := reviewPiecePlan(t, 1, 2*limits.BlockBytes)
	transfer, replacement, remote := reviewTransfer(t, plan, meta, Config{MaxStagedPieces: 1, MaxStagedBytes: 2 * limits.BlockBytes, Shuffle: keepTieOrder})
	if err := transfer.scheduler.AddPeer("departing", endpoint(63)); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability("departing", []int{0}); err != nil {
		t.Fatal(err)
	}
	_, stage := reviewAdmitStage(t, transfer, "departing")
	requests, err := transfer.scheduler.NextRequests("departing", 1)
	if err != nil || len(requests) != 1 {
		t.Fatalf("initial requests = %#v, %v", requests, err)
	}
	first := requests[0].Block
	if err := stage.WriteBlock(int64(first.Begin), make([]byte, first.Length)); err != nil {
		t.Fatal(err)
	}
	if _, err := transfer.scheduler.AcceptBlock("departing", first); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.RemovePeer("departing"); err != nil {
		t.Fatal(err)
	}
	reviewAdvertise(t, transfer, replacement, 0)
	live := []*transferPeer{replacement}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	second := reviewReadRequest(t, remote)
	if second == first || second.Index != 0 {
		t.Fatalf("replacement request = %#v, first = %#v", second, first)
	}
	if transfer.stages[0] != stage || transfer.stager.StagedCount() != 1 {
		t.Fatal("useful partial stage was discarded")
	}
	if err := transfer.scheduler.RejectBlock(replacement.input.ID, second); err != nil {
		t.Fatal(err)
	}
	if len(transfer.scheduler.pieces[0].contributors) != 1 {
		t.Fatal("accepted block provenance was lost")
	}
}

func TestReviewPressureKeepsPartialStageWhenEmptyStageSuffices(t *testing.T) {
	const length = 2 * limits.BlockBytes
	plan, meta := reviewPiecePlan(t, 3, length)
	transfer, replacement, remote := reviewTransfer(t, plan, meta, Config{
		MaxStagedPieces: 2, MaxStagedBytes: 5 * limits.BlockBytes, Shuffle: keepTieOrder,
	})
	if err := transfer.scheduler.AddPeer("departing", endpoint(63)); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability("departing", []int{0, 1}); err != nil {
		t.Fatal(err)
	}
	_, partial := reviewAdmitStage(t, transfer, "departing")
	_, empty := reviewAdmitStage(t, transfer, "departing")
	requests, err := transfer.scheduler.NextRequests("departing", 1)
	if err != nil || len(requests) != 1 || requests[0].Block.Index != 0 {
		t.Fatalf("partial request = %#v, %v", requests, err)
	}
	block := requests[0].Block
	if err := partial.WriteBlock(int64(block.Begin), make([]byte, block.Length)); err != nil {
		t.Fatal(err)
	}
	if _, err := transfer.scheduler.AcceptBlock("departing", block); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.RemovePeer("departing"); err != nil {
		t.Fatal(err)
	}
	reviewAdvertise(t, transfer, replacement, 2)
	live := []*transferPeer{replacement}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if got := reviewReadRequest(t, remote); got.Index != 2 {
		t.Fatalf("replacement request = %#v", got)
	}
	if transfer.stages[0] != partial || transfer.stages[1] != nil || transfer.stages[2] == nil ||
		transfer.scheduler.StagedPieces() != 2 || len(transfer.scheduler.pieces[0].contributors) != 1 {
		t.Fatal("stage pressure discarded accepted partial data")
	}
	if _, err := os.Stat(partial.Path()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(empty.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty reclaimed stage still exists: %v", err)
	}
}

func TestReviewPressureKeepsCompletedStageAwaitingVerification(t *testing.T) {
	plan, meta := reviewPiecePlan(t, 2, limits.BlockBytes)
	transfer, replacement, _ := reviewTransfer(t, plan, meta, Config{
		MaxStagedPieces: 1, MaxStagedBytes: 2 * limits.BlockBytes, Shuffle: keepTieOrder,
	})
	if err := transfer.scheduler.AddPeer("departing", endpoint(63)); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability("departing", []int{0}); err != nil {
		t.Fatal(err)
	}
	_, stage := reviewAdmitStage(t, transfer, "departing")
	requests, err := transfer.scheduler.NextRequests("departing", 1)
	if err != nil || len(requests) != 1 {
		t.Fatalf("request = %#v, %v", requests, err)
	}
	if err := stage.WriteBlock(0, make([]byte, limits.BlockBytes)); err != nil {
		t.Fatal(err)
	}
	if result, err := transfer.scheduler.AcceptBlock("departing", requests[0].Block); err != nil || !result.Complete {
		t.Fatalf("accepted block = %#v, %v", result, err)
	}
	if err := transfer.scheduler.RemovePeer("departing"); err != nil {
		t.Fatal(err)
	}
	reviewAdvertise(t, transfer, replacement, 1)
	live := []*transferPeer{replacement}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if transfer.stages[0] != stage || transfer.scheduler.pieces[1].stage != stageNone {
		t.Fatal("verification-ready stage was reclaimed")
	}
	snapshot, err := transfer.scheduler.Snapshot(0)
	if err != nil || len(snapshot.Blocks) != 1 || snapshot.Blocks[0].Endpoint != endpoint(63) {
		t.Fatalf("verification snapshot = %#v, %v", snapshot, err)
	}
}

func TestReviewZeroReqQStagesWithoutRequests(t *testing.T) {
	plan, _ := reviewPiecePlan(t, 2, limits.BlockBytes)
	zeroLocal, zeroRemote := net.Pipe()
	goodLocal, goodRemote := net.Pipe()
	t.Cleanup(func() { _ = zeroRemote.Close(); _ = goodRemote.Close() })
	transfer, err := NewTransfer(TransferConfig{
		Selection: plan, Output: &storage.Plan{},
		Stager:          storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: limits.BlockBytes}),
		SchedulerConfig: Config{MaxStagedPieces: 1, MaxStagedBytes: limits.BlockBytes, Shuffle: keepTieOrder},
		Peers: []ConnectedPeer{
			{ID: "zero", Endpoint: endpoint(64), Conn: zeroLocal, ReqQSet: true},
			{ID: "good", Endpoint: endpoint(65), Conn: goodLocal, ReqQ: 1, ReqQSet: true},
		},
		PieceCount: 2, PieceLength: limits.BlockBytes, LastPieceLength: limits.BlockBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := transfer.stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	zero, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		t.Fatal(err)
	}
	good, err := transfer.startPeer(context.Background(), transfer.peers[1])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = zero.worker.Close(); _ = good.worker.Close(); _ = transfer.stager.Cleanup(nil) })
	reviewAdvertise(t, transfer, zero, 0)
	reviewAdvertise(t, transfer, good, 1)
	live := []*transferPeer{zero, good}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if got := reviewReadRequest(t, goodRemote); got.Index != 1 {
		t.Fatalf("useful peer request = %#v", got)
	}
	if transfer.scheduler.pieces[0].stage != stageNone || transfer.scheduler.pieces[1].stage != stageAdmitted || len(zero.active) != 0 {
		t.Fatal("zero-capacity peer consumed the only stage slot")
	}
}

func TestReviewRepeatedReqQZeroDoesNotAdmitStages(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, true)
	transfer, p := fixture.transfer, fixture.peer
	if err := transfer.stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transfer.stager.Cleanup(nil) })
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.BitfieldID, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability(p.input.ID, []int{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.UnchokeID}); err != nil {
		t.Fatal(err)
	}
	live := []*transferPeer{p}
	setReqQ := func(value int) {
		t.Helper()
		body := fmt.Sprintf("d4:reqqi%dee", value)
		if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte(body))); err != nil {
			t.Fatal(err)
		}
	}
	setReqQ(0)
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if transfer.scheduler.StagedPieces() != 0 || transfer.stager.StagedCount() != 0 {
		t.Fatal("reqq=0 reserved a stage")
	}
	setReqQ(1)
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	first := reviewReadRequest(t, fixture.remote)
	if transfer.scheduler.StagedPieces() != 1 || transfer.scheduler.ActiveRequests() != 1 {
		t.Fatal("positive reqq did not admit and request a block")
	}
	setReqQ(0)
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if transfer.scheduler.ActiveRequests() != 1 {
		t.Fatal("lowered reqq discarded an in-flight request")
	}
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.RejectRequestID, Payload: blockPayload(first)}}); err != nil {
		t.Fatal(err)
	}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if transfer.scheduler.ActiveRequests() != 0 || transfer.scheduler.StagedPieces() != 1 {
		t.Fatal("reqq=0 scheduled a new block or discarded the reusable stage")
	}
	setReqQ(1)
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if got := reviewReadRequest(t, fixture.remote); got.Index != 0 || transfer.scheduler.ActiveRequests() != 1 {
		t.Fatalf("raised reqq did not resume requests: %#v", got)
	}
}

func TestReviewUnchangedTransferEventsKeepSchedulingSparse(t *testing.T) {
	plan, meta := reviewPiecePlan(t, 10_000, 1)
	local, remote := net.Pipe()
	var reader sync.WaitGroup
	messages := make(chan peer.Message, 256)
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			message, err := peer.ReadMessage(remote)
			if err != nil {
				return
			}
			messages <- message
		}
	}()
	fast := [8]byte{7: peer.FastExtensionBit}
	transfer, err := NewTransfer(TransferConfig{
		Selection: plan, Output: &storage.Plan{},
		Stager:          storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 2, MaxBytes: 2}),
		SchedulerConfig: Config{MaxStagedPieces: 2, MaxStagedBytes: 2, Shuffle: keepTieOrder},
		LocalHandshake:  peer.Handshake{Reserved: fast},
		Peers: []ConnectedPeer{{ID: "sparse", Endpoint: endpoint(66), Conn: local,
			Handshake: peer.Handshake{Reserved: fast}, ReqQ: 1, ReqQSet: true}},
		PieceCount: uint32(len(meta.Pieces)), PieceLength: 1, LastPieceLength: 1,
	})
	if err != nil {
		_ = local.Close()
		_ = remote.Close()
		reader.Wait()
		t.Fatal(err)
	}
	if err := transfer.stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.worker.Close()
		_ = remote.Close()
		reader.Wait()
		_ = transfer.stager.Cleanup(nil)
	})
	deliver := func(message peer.Message) {
		t.Helper()
		if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: message}); err != nil {
			t.Fatal(err)
		}
	}
	have := func(index uint32) peer.Message {
		var payload [4]byte
		binary.BigEndian.PutUint32(payload[:], index)
		return peer.Message{ID: peer.HaveID, Payload: payload[:]}
	}
	request := func() peer.Block {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			select {
			case message := <-messages:
				if message.ID == peer.RequestID {
					return peer.Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:12])}
				}
			case <-deadline:
				t.Fatal("no request after sparse events")
			}
		}
	}
	deliver(peer.Message{ID: peer.HaveNoneID})
	before := transfer.scheduler.generation
	for i := 0; i < 20; i++ {
		deliver(peer.Message{KeepAlive: true})
		deliver(peer.Message{ID: peer.HaveNoneID})
	}
	if transfer.scheduler.generation != before {
		t.Fatal("unchanged empty availability changed scheduler eligibility")
	}
	deliver(have(0))
	deliver(have(1))
	deliver(peer.Message{ID: peer.UnchokeID})
	live := []*transferPeer{p}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	first := request()
	if first.Index != 0 || transfer.scheduler.StagedPieces() != 2 {
		t.Fatalf("initial sparse request = %#v, stages=%d", first, transfer.scheduler.StagedPieces())
	}
	before = transfer.scheduler.generation
	for i := 0; i < 20; i++ {
		deliver(have(0))
		deliver(peer.Message{KeepAlive: true})
		if transfer.scheduler.generation != before {
			t.Fatal("duplicate Have or keepalive changed scheduler eligibility")
		}
		deliver(peer.Message{ID: peer.ChokeID})
		if transfer.scheduler.pieces[0].rarity != 0 || transfer.scheduler.pieces[1].rarity != 0 {
			t.Fatal("choke retained requestable availability")
		}
		deliver(peer.Message{ID: peer.UnchokeID})
		if transfer.scheduler.pieces[0].rarity != 1 || transfer.scheduler.pieces[1].rarity != 1 {
			t.Fatal("unchoke did not restore sparse availability")
		}
		if err := transfer.drive(context.Background(), &live); err != nil {
			t.Fatal(err)
		}
	}
	if transfer.scheduler.ActiveRequests() != 1 {
		t.Fatalf("sparse events changed the active pipeline: active=%d, done=%t", transfer.scheduler.ActiveRequests(), p.done)
	}
	deliver(peer.Message{ID: peer.HaveNoneID})
	before = transfer.scheduler.generation
	deliver(peer.Message{ID: peer.HaveNoneID})
	if transfer.scheduler.generation != before {
		t.Fatal("duplicate Have None changed scheduler eligibility")
	}
	deliver(have(1))
	deliver(peer.Message{ID: peer.RejectRequestID, Payload: blockPayload(first)})
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if second := request(); second.Index != 1 {
		t.Fatalf("other requestable work was skipped: %#v", second)
	}
}

func TestReviewSparsePeerChangesDoNotRescanBroadOffer(t *testing.T) {
	plan, meta := reviewPiecePlan(t, 10_000, 1)
	shuffleCalls := 0
	cfg := Config{MaxStagedPieces: 1, MaxStagedBytes: 1, Shuffle: func(values []int) error {
		shuffleCalls++
		return nil
	}}
	transfer, broad, remote := reviewTransfer(t, plan, meta, cfg)
	broad.state.SetReqQ(2)
	if err := transfer.scheduler.SetPeerLimit(broad.input.ID, 2); err != nil {
		t.Fatal(err)
	}
	sparseLocal, sparseRemote := net.Pipe()
	t.Cleanup(func() { _ = sparseRemote.Close() })
	sparse, err := transfer.startPeer(context.Background(), ConnectedPeer{
		ID: "sparse", Endpoint: endpoint(67), Conn: sparseLocal, ReqQSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sparse.worker.Close() })
	all := make([]int, 10_000)
	for i := range all {
		all[i] = i
	}
	reviewAdvertise(t, transfer, broad, all...)
	reviewAdvertise(t, transfer, sparse, 9_999)
	live := []*transferPeer{broad, sparse}
	if err := transfer.drive(context.Background(), &live); err != nil {
		t.Fatal(err)
	}
	if got := reviewReadRequest(t, remote); got.Index != 0 || transfer.scheduler.ActiveRequests() != 1 {
		t.Fatalf("broad peer request = %#v, active=%d", got, transfer.scheduler.ActiveRequests())
	}
	before := shuffleCalls
	for i := 0; i < 20; i++ {
		for _, id := range []byte{peer.ChokeID, peer.UnchokeID} {
			if err := transfer.handleEvent(context.Background(), sparse, peer.PeerEvent{Message: peer.Message{ID: id}}); err != nil {
				t.Fatal(err)
			}
			if err := transfer.drive(context.Background(), &live); err != nil {
				t.Fatal(err)
			}
		}
	}
	if shuffleCalls != before || transfer.scheduler.ActiveRequests() != 1 || transfer.scheduler.StagedPieces() != 1 {
		t.Fatalf("broad offer was rescanned after sparse peer events: shuffles %d -> %d, active=%d, staged=%d", before, shuffleCalls, transfer.scheduler.ActiveRequests(), transfer.scheduler.StagedPieces())
	}
}

func TestReviewNoAssignableBlock(t *testing.T) {
	plan, _ := reviewPiecePlan(t, 10_000, 1)
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("owner", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("sparse", endpoint(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("owner", []int{0}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("sparse", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("owner")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %t, %v", offer, ok, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	if requests, err := s.NextRequests("owner", 1); err != nil || len(requests) != 1 {
		t.Fatalf("owner requests = %#v, %v", requests, err)
	}
	for i := 0; i < 100; i++ {
		if requests, err := s.NextRequests("owner", 1); err != nil || len(requests) != 0 {
			t.Fatalf("unchanged event %d assigned %#v: %v", i, requests, err)
		}
	}
}

func TestSchedulerUsesPieceThatFitsRemainingStageBytes(t *testing.T) {
	const full = 2 * limits.BlockBytes
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "payload", 0, 2*full+limits.BlockBytes)},
		[]torrent.Piece{
			piece(0, 0, full), piece(1, full, 2*full),
			piece(2, 2*full, 2*full+limits.BlockBytes),
		}, nil)
	s, err := NewScheduler(plan, Config{MaxStagedPieces: 2, MaxStagedBytes: 3 * limits.BlockBytes, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id string
		ep peer.Endpoint
	}{{"owner", endpoint(1)}, {"waiting", endpoint(2)}} {
		if err := s.AddPeer(item.id, item.ep); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetAvailability("owner", []int{0}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("waiting", []int{1, 2}); err != nil {
		t.Fatal(err)
	}
	first, ok, err := s.ReservePiece("owner")
	if err != nil || !ok || first.PieceIndex != 0 {
		t.Fatalf("first reserve = %#v, %t, %v", first, ok, err)
	}
	if err := s.AdmitPiece(first); err != nil {
		t.Fatal(err)
	}
	second, ok, err := s.ReservePiece("waiting")
	if err != nil || !ok || second.PieceIndex != 2 {
		t.Fatalf("fitting reserve = %#v, %t, %v; want short final piece", second, ok, err)
	}
}

func TestSchedulerTombstoneOnlyStageDoesNotReenterReservation(t *testing.T) {
	plan, _ := reviewPiecePlan(t, 2, limits.BlockBytes)
	s, err := NewScheduler(plan, Config{MaxStagedPieces: 1, MaxStagedBytes: limits.BlockBytes, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("peer", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("peer", []int{0, 1}); err != nil {
		t.Fatal(err)
	}
	first, ok, err := s.ReservePiece("peer")
	if err != nil || !ok || first.PieceIndex != 0 {
		t.Fatalf("first reserve = %#v, %t, %v", first, ok, err)
	}
	if err := s.AdmitPiece(first); err != nil {
		t.Fatal(err)
	}
	excluded := map[peer.Block]struct{}{{Index: 0, Begin: 0, Length: limits.BlockBytes}: {}}
	if err := s.discardPiece(0); err != nil {
		t.Fatal(err)
	}
	second, ok, err := s.reservePieceExcluding("peer", excluded)
	if err != nil || !ok || second.PieceIndex != 1 {
		t.Fatalf("replacement reserve = %#v, %t, %v", second, ok, err)
	}
	if err := s.AdmitPiece(second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_, _, pressure, found, err := s.nextReservation(s.peers["peer"], "peer", excluded)
		if err != nil || found {
			t.Fatalf("iteration %d pressure candidate = %d, %t, %v", i, pressure, found, err)
		}
		assertSchedulerWork(t, s)
	}
}

func BenchmarkReviewNoAssignableBlock(b *testing.B) {
	for _, count := range []int{10_000, 100_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			plan, _ := reviewPiecePlan(b, count, 1)
			s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
			if err != nil {
				b.Fatal(err)
			}
			if err := s.AddPeer("p", endpoint(1)); err != nil {
				b.Fatal(err)
			}
			if err := s.SetAvailability("p", []int{0}); err != nil {
				b.Fatal(err)
			}
			offer, ok, err := s.ReservePiece("p")
			if err != nil || !ok {
				b.Fatalf("reserve = %#v, %t, %v", offer, ok, err)
			}
			if err := s.AdmitPiece(offer); err != nil {
				b.Fatal(err)
			}
			if requests, err := s.NextRequests("p", 1); err != nil || len(requests) != 1 {
				b.Fatalf("initial = %#v, %v", requests, err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if requests, err := s.NextRequests("p", 1); err != nil || len(requests) != 0 {
					b.Fatalf("blocked = %#v, %v", requests, err)
				}
			}
		})
	}
}

func reviewTinyFilesPlan(tb testing.TB, count int, padding bool) (*torrent.SelectionPlan, torrent.Metainfo, []byte) {
	tb.Helper()
	files := make([]torrent.File, 0, count*2)
	data := make([]byte, 0, count*2)
	for i := 0; i < count; i++ {
		begin := int64(len(data))
		data = append(data, byte(i%251+1))
		files = append(files, regularFile(len(files), fmt.Sprintf("file-%05d", i), begin, begin+1))
		if padding && i+1 < count {
			begin = int64(len(data))
			data = append(data, 0)
			files = append(files, torrent.File{Index: len(files), Path: fmt.Sprintf(".pad/%05d", i), Range: torrent.ByteRange{Begin: begin, End: begin + 1}, Kind: torrent.PaddingFile})
		}
	}
	length := int64(len(data))
	meta := torrent.Metainfo{
		Name: "tiny", MultiFile: true, TotalLength: length, PieceLength: length,
		Files:  files,
		Pieces: []torrent.Piece{{Index: 0, Range: torrent.ByteRange{Begin: 0, End: length}, Hash: sha1.Sum(data)}},
	}
	plan, err := torrent.Select(meta, []string{"file-00000"}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return plan, meta, data
}

func reviewRequestWholePiece(tb testing.TB, s *Scheduler, write func(peer.Block)) int {
	tb.Helper()
	count := 0
	for !allDone(s.pieces[0]) {
		requests, err := s.NextRequests("peer", limits.PeerRequests)
		if err != nil || len(requests) == 0 {
			tb.Fatalf("requests after %d blocks = %#v, %v", count, requests, err)
		}
		for _, request := range requests {
			if write != nil {
				write(request.Block)
			}
			if _, err := s.AcceptBlock("peer", request.Block); err != nil {
				tb.Fatal(err)
			}
			count++
		}
	}
	return count
}

func reviewAdmittedTinyScheduler(tb testing.TB, plan *torrent.SelectionPlan) *Scheduler {
	tb.Helper()
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		tb.Fatal(err)
	}
	if err := s.AddPeer("peer", endpoint(1)); err != nil {
		tb.Fatal(err)
	}
	if err := s.SetAvailability("peer", []int{0}); err != nil {
		tb.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("peer")
	if err != nil || !ok {
		tb.Fatalf("reserve = %#v, %t, %v", offer, ok, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		tb.Fatal(err)
	}
	return s
}

func TestReviewTinyFilesOnePiece(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		padding bool
	}{
		{name: "contiguous-1000", count: 1_000},
		{name: "contiguous-10000", count: 10_000},
		{name: "padding-1000", count: 1_000, padding: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, meta, data := reviewTinyFilesPlan(t, tc.count, tc.padding)
			s := reviewAdmittedTinyScheduler(t, plan)
			root := t.TempDir()
			output, err := storage.Validate(root, meta, plan.SelectedIndices())
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
			stager := storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache")})
			if err := stager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = stager.Cleanup(nil) })
			stage, err := stager.AdmitPiece(meta.Pieces[0])
			if err != nil {
				t.Fatal(err)
			}
			count := reviewRequestWholePiece(t, s, func(block peer.Block) {
				begin, end := int(block.Begin), int(block.Begin+block.Length)
				if err := stage.WriteBlock(int64(begin), data[begin:end]); err != nil {
					t.Fatal(err)
				}
			})
			want := 1
			if tc.padding {
				want = tc.count
			}
			if count != want {
				t.Fatalf("request count = %d, want %d", count, want)
			}
			snapshot, err := s.Snapshot(0)
			if err != nil {
				t.Fatal(err)
			}
			coverage := make([]storage.BlockCoverage, len(snapshot.Blocks))
			for i, block := range snapshot.Blocks {
				coverage[i] = storage.BlockCoverage{Begin: int64(block.Block.Begin), Length: int64(block.Block.Length), Contributors: []storage.Endpoint{{Addr: block.Endpoint.Addr, Port: block.Endpoint.Port}}}
			}
			finalized, err := stager.Finalize(context.Background(), storage.NewPieceSnapshot(snapshot.Piece, coverage), snapshot.Plan, output)
			if err != nil || !finalized.OutputCommitted {
				t.Fatalf("finalize = %#v, %v", finalized, err)
			}
			if result, err := s.VerifyPiece(0, true); err != nil || !result.Completed {
				t.Fatalf("verify = %#v, %v", result, err)
			}
			got, err := os.ReadFile(filepath.Join(root, "tiny", "file-00000"))
			if err != nil || !bytes.Equal(got, data[:1]) {
				t.Fatalf("selected output = %v, %v", got, err)
			}
		})
	}
}

func BenchmarkReviewTinyFilesOnePiece(b *testing.B) {
	for _, padding := range []bool{false, true} {
		for _, count := range []int{1_000, 10_000} {
			name := fmt.Sprint(count)
			if padding {
				name = "padding-" + name
			}
			b.Run(name, func(b *testing.B) {
				plan, _, _ := reviewTinyFilesPlan(b, count, padding)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s := reviewAdmittedTinyScheduler(b, plan)
					blocks := reviewRequestWholePiece(b, s, nil)
					if !padding && blocks != 1 || padding && blocks != count {
						b.Fatalf("request count = %d", blocks)
					}
				}
			})
		}
	}
}

func BenchmarkReviewPipelineFill(b *testing.B) {
	for _, count := range []int{10_000, 100_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			plan, _ := reviewPiecePlan(b, count, 1)
			s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
			if err != nil {
				b.Fatal(err)
			}
			if err := s.AddPeer("peer", endpoint(1)); err != nil {
				b.Fatal(err)
			}
			indices := make([]int, 64)
			for i := range indices {
				indices[i] = i
			}
			if err := s.SetAvailability("peer", indices); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				offer, ok, err := s.ReservePiece("peer")
				if err != nil || !ok {
					b.Fatalf("reserve %d = %#v, %t, %v", i, offer, ok, err)
				}
				if err := s.AdmitPiece(offer); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				requests, err := s.NextRequests("peer", 64)
				if err != nil || len(requests) != 64 {
					b.Fatalf("pipeline = %d, %v", len(requests), err)
				}
				for _, request := range requests {
					if err := s.RejectBlock("peer", request.Block); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkReviewTransferSparsePeerChanges(b *testing.B) {
	for _, count := range []int{10_000, 100_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			plan, meta := reviewPiecePlan(b, count, 1)
			cfg := Config{MaxStagedPieces: 1, MaxStagedBytes: 1, Shuffle: keepTieOrder}
			transfer, broad, remote := reviewTransfer(b, plan, meta, cfg)
			broad.state.SetReqQ(2)
			if err := transfer.scheduler.SetPeerLimit(broad.input.ID, 2); err != nil {
				b.Fatal(err)
			}
			sparseLocal, sparseRemote := net.Pipe()
			b.Cleanup(func() { _ = sparseRemote.Close() })
			sparse, err := transfer.startPeer(context.Background(), ConnectedPeer{
				ID: "sparse", Endpoint: endpoint(67), Conn: sparseLocal, ReqQSet: true,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = sparse.worker.Close() })
			all := make([]int, count)
			for i := range all {
				all[i] = i
			}
			reviewAdvertise(b, transfer, broad, all...)
			reviewAdvertise(b, transfer, sparse, count-1)
			live := []*transferPeer{broad, sparse}
			if err := transfer.drive(context.Background(), &live); err != nil {
				b.Fatal(err)
			}
			if got := reviewReadRequest(b, remote); got.Index != 0 {
				b.Fatalf("first request = %#v", got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, id := range []byte{peer.ChokeID, peer.UnchokeID} {
					if err := transfer.handleEvent(context.Background(), sparse, peer.PeerEvent{Message: peer.Message{ID: id}}); err != nil {
						b.Fatal(err)
					}
					if err := transfer.drive(context.Background(), &live); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
