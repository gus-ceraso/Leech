package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestTransferDiagnosticsObserveAvailabilityAndFirstUsefulBlock(t *testing.T) {
	data := []byte("leech")
	var hash [20]byte
	hash = sha1.Sum(data)
	meta := torrent.Metainfo{
		Name:        "fixture",
		TotalLength: int64(len(data)),
		PieceLength: int64(len(data)),
		Files:       []torrent.File{{Index: 0, Path: "payload", Range: torrent.ByteRange{End: int64(len(data))}, Kind: torrent.RegularFile}},
		Pieces:      []torrent.Piece{{Index: 0, Range: torrent.ByteRange{End: int64(len(data))}, Hash: hash}},
	}
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	stager := storage.NewStager(storage.StagerConfig{CacheRoot: cache, MaxPieces: 1, MaxBytes: int64(len(data))})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	infoHash := [20]byte{1, 2, 3}
	remoteID := [20]byte{9, 8, 7}
	remoteDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			remoteDone <- acceptErr
			return
		}
		defer conn.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			remoteDone <- err
			return
		}
		reserved := [8]byte{7: peer.FastExtensionBit}
		if err := peer.WriteHandshake(conn, infoHash, remoteID, reserved); err != nil {
			remoteDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			remoteDone <- err
			return
		}
		index := make([]byte, 4)
		for _, message := range []peer.Message{{ID: peer.HaveID, Payload: index}, {ID: peer.HaveNoneID}, {ID: peer.HaveID, Payload: index}} {
			if err := writeFixtureFrame(conn, message.ID, message.Payload); err != nil {
				remoteDone <- err
				return
			}
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			remoteDone <- err
			return
		}
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				remoteDone <- err
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID || message.ID == peer.HaveNoneID {
				continue
			}
			if message.ID != peer.RequestID {
				remoteDone <- io.ErrUnexpectedEOF
				return
			}
			frame := make([]byte, 4+1+8+len(data))
			binary.BigEndian.PutUint32(frame[:4], uint32(1+8+len(data)))
			frame[4] = peer.PieceID
			copy(frame[13:], data)
			if _, err := conn.Write(frame); err != nil {
				remoteDone <- err
				return
			}
			remoteDone <- nil
			return
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}, Reserved: [8]byte{7: peer.FastExtensionBit}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	var callbackOrder []string
	var observations []Diagnostic
	var transfer *Transfer
	transfer, err = NewTransfer(TransferConfig{
		Selection:       selection,
		Output:          plan,
		Stager:          stager,
		LocalHandshake:  local,
		Peers:           []ConnectedPeer{{ID: "fixture-peer", Conn: conn, Handshake: remote}},
		PieceCount:      1,
		PieceLength:     uint32(len(data)),
		LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(n int64) error {
			if n != int64(len(data)) {
				return fmt.Errorf("payload bytes = %d, want %d", n, len(data))
			}
			if transfer.Progress().Verified != 0 {
				return fmt.Errorf("payload callback ran after verification")
			}
			callbackOrder = append(callbackOrder, "payload")
			return nil
		},
		OnPieceVerified: func(PieceVerified) { callbackOrder = append(callbackOrder, "verified") },
		OnDiagnostic:    func(event Diagnostic) { observations = append(observations, event) },
		BeforePeerShutdown: func() error {
			if stager.Workspace() == "" {
				return fmt.Errorf("shutdown callback ran after staging cleanup")
			}
			callbackOrder = append(callbackOrder, "shutdown")
			return nil
		},
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if err := <-remoteDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("output = %q, want %q", got, data)
	}
	if want := []string{"payload", "verified", "shutdown"}; !reflect.DeepEqual(callbackOrder, want) {
		t.Fatalf("callback order = %v, want %v", callbackOrder, want)
	}
	var sawEmpty, sawAvailable, sawRequest, sawUseful, sawComplete bool
	emptyTransitions, availableTransitions := 0, 0
	for _, event := range observations {
		switch event.Detail {
		case "availability empty":
			sawEmpty = true
			emptyTransitions++
		case "availability became empty":
			emptyTransitions++
		case "availability became nonempty":
			sawAvailable = true
			availableTransitions++
		default:
			if strings.HasPrefix(event.Detail, "request assignment summary ") {
				sawRequest = true
			}
		case "first useful block accepted and staged":
			sawUseful = true
		case "piece 0 completed":
			sawComplete = true
		}
	}
	if !sawEmpty || !sawAvailable || !sawRequest || !sawUseful || !sawComplete || emptyTransitions != 2 || availableTransitions != 2 {
		t.Fatalf("transfer observations empty=%t available=%t request=%t useful=%t complete=%t transitions=%d/%d: %+v", sawEmpty, sawAvailable, sawRequest, sawUseful, sawComplete, emptyTransitions, availableTransitions, observations)
	}
}

func TestTransferActivityDiagnosticsSummarizeRepeatedRequests(t *testing.T) {
	var observations []Diagnostic
	transfer := &Transfer{onDiagnostic: func(event Diagnostic) { observations = append(observations, event) }}
	p := &transferPeer{requestsSinceSummary: 257, allowedFastSinceSummary: 16}
	transfer.reportPeerActivity([]*transferPeer{p})
	transfer.reportPeerActivity([]*transferPeer{p})
	if len(observations) != 2 || observations[0].Detail != "request assignment summary outstanding=0" || observations[0].Count != 257 || observations[1].Detail != "Allowed Fast grant summary" || observations[1].Count != 16 {
		t.Fatalf("repeated activity diagnostics = %+v", observations)
	}
}

func TestTransferAppliesPeerAvailabilityDeltas(t *testing.T) {
	const pieceCount = 4096
	pieces := make([]torrent.Piece, pieceCount)
	for index := range pieces {
		pieces[index] = piece(index, int64(index), int64(index+1))
	}
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "payload", 0, int64(pieceCount))},
		pieces,
		nil,
	)
	scheduler, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddPeer("peer", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	transfer := &Transfer{scheduler: scheduler}
	state, err := peer.NewPeerState(uint32(pieceCount), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetWantedPieces(plan.WantedPieces()); err != nil {
		t.Fatal(err)
	}

	index := uint32(pieceCount - 1)
	indexPayload := make([]byte, 4)
	binary.BigEndian.PutUint32(indexPayload, index)
	for _, message := range []peer.Message{
		{ID: peer.HaveID, Payload: indexPayload},
		{ID: peer.AllowedFastID, Payload: indexPayload},
	} {
		effect, err := state.ApplyMessage(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := transfer.applyAvailabilityChanges("peer", effect); err != nil {
			t.Fatal(err)
		}
	}
	if rarity, err := scheduler.Rarity(int(index)); err != nil || rarity != 1 {
		t.Fatalf("last-piece rarity = %d, %v; want one changed availability", rarity, err)
	}
	if rarity, err := scheduler.Rarity(0); err != nil || rarity != 0 {
		t.Fatalf("unadvertised-piece rarity = %d, %v; want zero", rarity, err)
	}

	effect, err := state.ApplyMessage(peer.Message{ID: peer.HaveID, Payload: indexPayload})
	if err != nil || len(effect.AvailabilityAdded) != 0 || len(effect.AvailabilityRemoved) != 0 {
		t.Fatalf("duplicate Have delta = %#v, %v", effect, err)
	}
	if err := transfer.applyAvailabilityChanges("peer", effect); err != nil {
		t.Fatal(err)
	}
	effect, err = state.ApplyMessage(peer.Message{ID: peer.HaveNoneID})
	if err != nil || len(effect.AvailabilityRemoved) != 1 || effect.AvailabilityRemoved[0] != index || !state.AllowedFast(index) {
		t.Fatalf("Have None delta = %#v, allowed=%v, err=%v", effect, state.AllowedFast(index), err)
	}
	if err := transfer.applyAvailabilityChanges("peer", effect); err != nil {
		t.Fatal(err)
	}
	if rarity, err := scheduler.Rarity(int(index)); err != nil || rarity != 0 {
		t.Fatalf("rarity after Have None = %d, %v; want zero", rarity, err)
	}
}

func TestCommittedFinalizeErrorSettlesPieceAndCallsVerifiedOnce(t *testing.T) {
	data := []byte("payload")
	selection := schedulerPlan(t,
		[]torrent.File{regularFile(0, "payload", 0, int64(len(data)))},
		[]torrent.Piece{piece(0, 0, int64(len(data)))}, nil)
	scheduler, err := NewScheduler(selection, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.AddPeer("peer", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.SetAvailability("peer", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := scheduler.ReservePiece("peer")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := scheduler.NextRequests("peer", 1)
	if err != nil || len(requests) != 1 {
		t.Fatalf("requests = %#v, %v", requests, err)
	}
	if _, err := scheduler.AcceptBlock("peer", requests[0].Block); err != nil {
		t.Fatal(err)
	}
	var verified []PieceVerified
	transfer := &Transfer{
		scheduler:       scheduler,
		onPieceVerified: func(piece PieceVerified) { verified = append(verified, piece) },
	}
	removeErr := errors.New("staged file removal failed")
	finalized := storage.FinalizeResult{Piece: piece(0, 0, int64(len(data))), OutputCommitted: true}
	gotErr := transfer.settleFinalizedPiece(0, finalized.Piece, finalized, removeErr, 0)
	if !errors.Is(gotErr, removeErr) {
		t.Fatalf("settlement error = %v, want removal error", gotErr)
	}
	if progress := scheduler.Progress(); progress.Verified != int64(len(data)) || !scheduler.IsComplete() {
		t.Fatalf("scheduler progress = %#v, complete=%v", progress, scheduler.IsComplete())
	}
	if len(verified) != 1 || verified[0].SelectedBytes != int64(len(data)) {
		t.Fatalf("verified callbacks = %#v, want one committed selected piece", verified)
	}
	if err := transfer.settleFinalizedPiece(0, finalized.Piece, finalized, removeErr, 0); !errors.Is(err, removeErr) {
		t.Fatalf("duplicate settlement error = %v, want original removal error", err)
	}
	if len(verified) != 1 {
		t.Fatalf("verified callback count = %d, want one", len(verified))
	}
}

func TestTransferReqQPresencePreservesExplicitZero(t *testing.T) {
	data := []byte("q")
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer right.Close()
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "zero-reqq", Endpoint: endpoint(41), Conn: left, ReqQSet: true}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = p.worker.Close()
		_ = transfer.scheduler.RemovePeer(p.input.ID)
	}()
	if got := p.state.ReqQ(); got != 0 {
		t.Fatalf("explicit zero reqq = %d, want 0", got)
	}
	if err := transfer.scheduler.SetAvailability(p.input.ID, []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := transfer.scheduler.ReservePiece(p.input.ID)
	if err != nil || ok {
		t.Fatalf("zero reqq reserve = %#v, %t, %v; want no reservation", offer, ok, err)
	}
	requests, err := transfer.scheduler.NextRequests(p.input.ID, p.state.ReqQ())
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 0 {
		t.Fatalf("zero reqq requests = %#v, want none", requests)
	}

	missingLeft, missingRight := net.Pipe()
	defer missingRight.Close()
	missing, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "missing-reqq", Endpoint: endpoint(42), Conn: missingLeft}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		missingLeft.Close()
		t.Fatal(err)
	}
	missingPeer, err := missing.startPeer(context.Background(), missing.peers[0])
	if err != nil {
		missingLeft.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = missingPeer.worker.Close()
		_ = missing.scheduler.RemovePeer(missingPeer.input.ID)
	}()
	if got := missingPeer.state.ReqQ(); got != limits.PeerRequests {
		t.Fatalf("missing reqq = %d, want default %d", got, limits.PeerRequests)
	}
}

func TestTransferStartupFailureReleasesAllInitialPeers(t *testing.T) {
	data := []byte("startup")
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, singleFileMeta(data), selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	first, firstRemote := net.Pipe()
	defer firstRemote.Close()
	pending, pendingRemote := net.Pipe()
	defer pendingRemote.Close()
	released := make(map[string]int)
	var releaseMu sync.Mutex
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{Reserved: [8]byte{7: peer.FastExtensionBit}},
		Peers: []ConnectedPeer{
			{ID: "first", Endpoint: endpoint(51), Conn: first, Handshake: peer.Handshake{}},
			{ID: "failing", Endpoint: endpoint(52), Conn: &transferWriteErrorConn{}, Handshake: peer.Handshake{Reserved: [8]byte{7: peer.FastExtensionBit}}},
			{ID: "pending", Endpoint: endpoint(53), Conn: pending, Handshake: peer.Handshake{}},
		},
		ReleasePeer: func(input ConnectedPeer) {
			releaseMu.Lock()
			released[input.ID]++
			releaseMu.Unlock()
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		first.Close()
		pending.Close()
		t.Fatal(err)
	}
	err = transfer.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "transfer write failure") {
		t.Fatalf("Run = %v, want startup write failure", err)
	}
	releaseMu.Lock()
	defer releaseMu.Unlock()
	for _, id := range []string{"first", "failing", "pending"} {
		if released[id] != 1 {
			t.Errorf("release count for %q = %d, want 1", id, released[id])
		}
	}
}

func TestWaitPeerEventReportsClosedWorkerAsPeerLocal(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	worker := peer.NewConnectionWorker(local, peer.ReadOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	if err := worker.Close(); err != nil {
		t.Fatalf("worker close: %v", err)
	}
	result := waitPeerEventWithAdmission(ctx, []*transferPeer{{worker: worker}}, nil, nil)
	if !result.OK || !result.PeerClosed || result.Index != 0 {
		t.Fatalf("closed worker result = %#v, want peer-local closure", result)
	}
}

func TestTransferDriveClosedWorkerReleasesForLivePeer(t *testing.T) {
	data := []byte("x")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	left1, remote1 := net.Pipe()
	left2, remote2 := net.Pipe()
	defer remote2.Close()
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Stager: storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		Peers: []ConnectedPeer{
			{ID: "closed", Endpoint: endpoint(64), Conn: left1},
			{ID: "live", Endpoint: endpoint(65), Conn: left2},
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		left1.Close()
		left2.Close()
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := transfer.stager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer transfer.stager.Cleanup(nil)
	p1, err := transfer.startPeer(ctx, transfer.peers[0])
	if err != nil {
		t.Fatal(err)
	}
	p2, err := transfer.startPeer(ctx, transfer.peers[1])
	if err != nil {
		_ = p1.worker.Close()
		t.Fatal(err)
	}
	peers := []*transferPeer{p1, p2}
	for _, p := range peers {
		if _, err := p.state.ApplyMessage(peer.Message{ID: peer.BitfieldID, Payload: []byte{0x80}}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.state.ApplyMessage(peer.Message{ID: peer.UnchokeID}); err != nil {
			t.Fatal(err)
		}
		if err := transfer.scheduler.SetAvailability(p.input.ID, []int{0}); err != nil {
			t.Fatal(err)
		}
	}
	_ = remote1.Close()
	deadline := time.Now().Add(time.Second)
	for p1.worker.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p1.worker.Err() == nil {
		t.Fatal("closed worker did not report a terminal error")
	}
	if err := transfer.drive(ctx, &peers); !errors.Is(err, peer.ErrWorkerClosed) {
		t.Fatalf("drive closed worker error = %v, want ErrWorkerClosed", err)
	}
	if !p1.done {
		t.Fatal("closed worker was not disconnected")
	}
	if got := transfer.scheduler.ActiveRequests(); got != 0 {
		t.Fatalf("active requests after closed peer = %d, want 0", got)
	}
	if err := transfer.drive(ctx, &peers); err != nil {
		t.Fatalf("drive live peer: %v", err)
	}
	if len(p2.active) != 1 || transfer.scheduler.ActiveRequests() != 1 {
		t.Fatalf("live peer assignment = active %d scheduler %d, want 1/1", len(p2.active), transfer.scheduler.ActiveRequests())
	}
	_ = p2.worker.Close()
}

func TestTransferRunContinuesAfterDriveSeesClosedWorker(t *testing.T) {
	data := []byte("drive-recovery")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	first, firstRemote := net.Pipe()
	firstDone := make(chan error, 1)
	go func() {
		if err := writeFixtureFrame(firstRemote, peer.BitfieldID, []byte{0x80}); err != nil {
			firstDone <- err
			return
		}
		if err := writeFixtureFrame(firstRemote, peer.UnchokeID, nil); err != nil {
			firstDone <- err
			return
		}
		_ = firstRemote.Close()
		firstDone <- nil
	}()
	second, secondDone := startFixturePeer(t, [20]byte{19, 19, 19}, []fixturePiece{{index: 0, data: data}}, false, false)
	firstResult := make(chan error, 1)
	firstReleased := make(chan struct{}, 1)
	provided := false
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "closed-before-send", Endpoint: endpoint(66), Conn: first, Handshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{7, 7, 7}}}},
		AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
			if !provided {
				select {
				case firstErr := <-firstDone:
					firstResult <- firstErr
					if firstErr != nil {
						return ConnectedPeer{}, firstErr
					}
				case <-ctx.Done():
					return ConnectedPeer{}, ctx.Err()
				}
				select {
				case <-firstReleased:
				case <-ctx.Done():
					return ConnectedPeer{}, ctx.Err()
				}
				provided = true
				return ConnectedPeer{ID: "usable", Endpoint: endpoint(67), Conn: second, Handshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{3, 2, 1}}}, nil
			}
			<-ctx.Done()
			return ConnectedPeer{}, ctx.Err()
		},
		ReleasePeer: func(input ConnectedPeer) {
			if input.ID == "closed-before-send" {
				firstReleased <- struct{}{}
			}
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		_ = first.Close()
		_ = second.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runErr := transfer.Run(ctx)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first peer: %v", err)
		}
	default:
		t.Fatal("first peer did not finish its frames before replacement")
	}
	if runErr != nil {
		t.Fatalf("transfer: %v", runErr)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
}

func TestTransferTombstoneLimitClosesPeerWithoutStrike(t *testing.T) {
	data := make([]byte, 1024)
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	endpoint := endpoint(61)
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "tombstones", Endpoint: endpoint, Conn: local}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	for i := 0; i < limits.RequestTombstones; i++ {
		block := peer.Block{Index: 0, Begin: uint32(i), Length: 1}
		if err := p.state.Requests().Add(block); err != nil {
			t.Fatalf("add tombstone request %d: %v", i, err)
		}
		if err := p.state.Requests().Timeout(block); err != nil {
			t.Fatalf("timeout tombstone request %d: %v", i, err)
		}
	}
	outstanding := peer.Block{Index: 0, Begin: 512, Length: 1}
	if err := p.state.Requests().Add(outstanding); err != nil {
		t.Fatal(err)
	}
	err = transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.ChokeID}})
	if !errors.Is(err, peer.ErrTombstoneLimit) {
		t.Fatalf("choke error = %v, want tombstone limit", err)
	}
	if !p.done {
		t.Fatal("tombstone-limit peer remains live")
	}
	if transfer.scheduler.IsBlacklisted(endpoint) {
		t.Fatal("tombstone-limit peer was corruption-blacklisted")
	}
}

func TestTransferSeverePeerBlacklistsSharedDialState(t *testing.T) {
	data := []byte("payload")
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	endpoint := endpoint(65)
	backoff := peer.NewEndpointBackoff()
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "severe-peer", Endpoint: endpoint, Conn: local}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnEndpointBlacklisted: backoff.Blacklist,
	})
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	violation := fmt.Errorf("invalid bitfield: %w", peer.ErrProtocolViolation)
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Err: violation}); !errors.Is(err, peer.ErrProtocolViolation) {
		t.Fatalf("severe peer error = %v", err)
	}
	if !transfer.scheduler.IsBlacklisted(endpoint) || !backoff.IsBlacklisted(endpoint) {
		t.Fatal("severe endpoint was not blacklisted by both transfer and dialer")
	}
	if backoff.Ready(endpoint, time.Now().Add(24*time.Hour)) {
		t.Fatal("candidate acquisition may redial severe endpoint")
	}
}

func TestTransferPayloadCallbackCountsLatePiece(t *testing.T) {
	data := make([]byte, 32)
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	var got int64
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "late-piece", Endpoint: endpoint(62), Conn: local}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(n int64) error {
			got += n
			return nil
		},
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	block := peer.Block{Index: 0, Begin: 0, Length: uint32(len(data))}
	if err := p.state.Requests().Add(block); err != nil {
		t.Fatal(err)
	}
	if err := p.state.Requests().Timeout(block); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	copy(payload[8:], data)
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.PieceID, Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	if got != int64(len(data)) {
		t.Fatalf("late payload bytes = %d, want %d", got, len(data))
	}
	for _, event := range observations {
		if event.Detail == "first useful block accepted and staged" {
			t.Fatalf("late piece counted as useful: %+v", observations)
		}
	}
	_ = p.worker.Close()
}

func TestTransferPayloadCallbackErrorStopsBeforeAcceptance(t *testing.T) {
	data := make([]byte, 32)
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	wantErr := errors.New("payload accounting failed")
	var observations []Diagnostic
	var transfer *Transfer
	transfer, err = NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "payload-error", Endpoint: endpoint(63), Conn: local}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(n int64) error {
			if n != int64(len(data)) {
				t.Fatalf("payload bytes = %d, want %d", n, len(data))
			}
			if transfer.scheduler.ActiveRequests() != 1 {
				t.Fatalf("active requests in callback = %d, want 1", transfer.scheduler.ActiveRequests())
			}
			return wantErr
		},
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	block := peer.Block{Index: 0, Begin: 0, Length: uint32(len(data))}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.BitfieldID, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.UnchokeID}); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability(p.input.ID, []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := transfer.scheduler.ReservePiece(p.input.ID)
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := transfer.scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := transfer.scheduler.NextRequests(p.input.ID, 1)
	if err != nil || len(requests) != 1 {
		t.Fatalf("next requests = %#v, %v", requests, err)
	}
	block = requests[0].Block
	if err := p.state.AddRequest(block); err != nil {
		t.Fatal(err)
	}
	p.active[block] = transfer.clock()
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	copy(payload[8:], data)
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.PieceID, Payload: payload}}); !errors.Is(err, wantErr) {
		t.Fatalf("payload callback error = %v, want %v", err, wantErr)
	}
	if got := transfer.scheduler.ActiveRequests(); got != 1 {
		t.Fatalf("active requests after callback error = %d, want 1", got)
	}
	for _, event := range observations {
		if event.Detail == "first useful block accepted and staged" {
			t.Fatalf("rejected payload counted as useful: %+v", observations)
		}
	}
	_ = p.worker.Close()
}

func TestTransferResumeCompleteSkipsNetworkAndStaging(t *testing.T) {
	data := []byte("resume")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, ResumeComplete: []int{0},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := transfer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !transfer.scheduler.IsComplete() || transfer.Progress().Verified != int64(len(data)) {
		t.Fatalf("resume progress = %#v", transfer.Progress())
	}
}

func writeFixtureFrame(conn net.Conn, id byte, payload []byte) error {
	frame := make([]byte, 4+1+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(1+len(payload)))
	frame[4] = id
	copy(frame[5:], payload)
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if n > 0 {
			frame = frame[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func singleFileMeta(data []byte) torrent.Metainfo {
	return torrent.Metainfo{
		Name:        "fixture",
		TotalLength: int64(len(data)),
		PieceLength: int64(len(data)),
		Files:       []torrent.File{{Index: 0, Path: "payload", Range: torrent.ByteRange{End: int64(len(data))}, Kind: torrent.RegularFile}},
		Pieces:      []torrent.Piece{{Index: 0, Range: torrent.ByteRange{End: int64(len(data))}, Hash: sha1.Sum(data)}},
	}
}

func TestFinalizePieceCorruptStageCloseFailurePropagatesFatal(t *testing.T) {
	data := []byte("good")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected staged close failure")
	var closes atomic.Int32
	stager := storage.NewStager(storage.StagerConfig{
		CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data)),
		OpenFile: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
			file, err := os.OpenFile(path, flag, mode)
			if err != nil {
				return nil, err
			}
			return &closeErrorStageFile{StagingFile: file, closeErr: closeErr, closes: &closes}, nil
		},
	})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	piece := selection.PiecePlans()[0].Piece
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, []byte("evil")); err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(selection, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := endpoint(1)
	if err := scheduler.AddPeer("contributor", endpoint); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.SetAvailability("contributor", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := scheduler.ReservePiece("contributor")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := scheduler.NextRequests("contributor", 1)
	if err != nil || len(requests) != 1 {
		t.Fatalf("requests = %#v, %v", requests, err)
	}
	if _, err := scheduler.AcceptBlock("contributor", requests[0].Block); err != nil {
		t.Fatal(err)
	}
	var observations []Diagnostic
	transfer := &Transfer{scheduler: scheduler, stager: stager, output: plan, stages: map[int]*storage.PieceStage{0: stage}, onDiagnostic: func(event Diagnostic) { observations = append(observations, event) }}
	if err := transfer.finalizePiece(context.Background(), 0, 0); !errors.Is(err, closeErr) || !errors.Is(err, storage.ErrStagingFatal) {
		t.Fatalf("finalizePiece error = %v, want injected close error and ErrStagingFatal", err)
	}
	if got := scheduler.StrikeCount(endpoint); got != 1 {
		t.Fatalf("contributor strikes = %d, want exactly one", got)
	}
	if closes.Load() != 1 {
		t.Fatalf("staged file closes = %d, want one", closes.Load())
	}
	if _, err := os.Stat(filepath.Join(root, "fixture")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt output exists or stat failed: %v", err)
	}
	if len(observations) != 2 || observations[0].Detail != "piece 0 hash mismatch; fatal staging failure" || observations[1].Detail != "fatal staging failure during hash retry" {
		t.Fatalf("hash/fatal diagnostics = %+v", observations)
	}
	_ = stager.Cleanup(nil)
}

type closeErrorStageFile struct {
	storage.StagingFile
	closeErr error
	closes   *atomic.Int32
}

func (f *closeErrorStageFile) Close() error {
	f.closes.Add(1)
	_ = f.StagingFile.Close()
	return f.closeErr
}

func TestTransferFatalCorruptStageCloseFailureShutsDownImmediately(t *testing.T) {
	data := []byte("good")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected staged close failure")
	var closes atomic.Int32
	stager := storage.NewStager(storage.StagerConfig{
		CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data)),
		OpenFile: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
			file, err := os.OpenFile(path, flag, mode)
			if err != nil {
				return nil, err
			}
			return &closeErrorStageFile{StagingFile: file, closeErr: closeErr, closes: &closes}, nil
		},
	})
	infoHash := [20]byte{1, 3, 5}
	conn, remoteDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, true, false)
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "fatal-peer", Endpoint: endpoint(1), Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- transfer.Run(context.Background()) }()
	select {
	case err := <-result:
		if !errors.Is(err, closeErr) || !errors.Is(err, storage.ErrStagingFatal) {
			t.Fatalf("Transfer.Run error = %v, want close failure and ErrStagingFatal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fatal stage-close failure did not end transfer promptly")
	}
	select {
	case err := <-remoteDone:
		if err != nil {
			t.Fatalf("fixture peer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fixture peer was not joined during graceful shutdown")
	}
	if closes.Load() != 1 {
		t.Fatalf("staged file closes = %d, want exactly one without retry", closes.Load())
	}
	if got := transfer.scheduler.StrikeCount(endpoint(1)); got != 1 {
		t.Fatalf("contributor strikes = %d, want exactly one", got)
	}
	fatalMismatch, fatalStorage, retryScheduled := 0, 0, 0
	for _, event := range observations {
		switch event.Detail {
		case "piece 0 hash mismatch; fatal staging failure":
			fatalMismatch++
		case "fatal staging failure during hash retry":
			fatalStorage++
		case "piece 0 hash mismatch; retry scheduled":
			retryScheduled++
		}
	}
	if fatalMismatch != 1 || fatalStorage != 1 || retryScheduled != 0 {
		t.Fatalf("fatal hash diagnostics = %+v", observations)
	}
	if output, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || len(output) != 0 {
		t.Fatalf("corrupt output = %q, err=%v; want empty prepared output", output, err)
	}
}

func TestTransferCorruptPieceRetriesWithoutOutput(t *testing.T) {
	data := []byte("good")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	conn, remoteDone := startFixturePeer(t, [20]byte{1, 3, 5}, []fixturePiece{{index: 0, data: data}}, true, false)
	var payloadBytes []int64
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection:      selection,
		Output:         plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{1, 3, 5}, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "retry-peer", Conn: conn, Handshake: peer.Handshake{InfoHash: [20]byte{1, 3, 5}, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(n int64) error {
			payloadBytes = append(payloadBytes, n)
			return nil
		},
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if err := <-remoteDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("output = %q, want %q", got, data)
	}
	if got := transfer.scheduler.StrikeCount(transfer.peers[0].Endpoint); got != 1 {
		t.Fatalf("strike count = %d, want 1", got)
	}
	if want := []int64{int64(len(data)), int64(len(data))}; !reflect.DeepEqual(payloadBytes, want) {
		t.Fatalf("payload accounting = %v, want %v", payloadBytes, want)
	}
	mismatches := 0
	for _, event := range observations {
		if event.Detail == "piece 0 hash mismatch; retry scheduled" {
			mismatches++
		}
		if strings.Contains(event.Detail, "fatal staging failure") {
			t.Fatalf("clean corruption reported fatal staging: %+v", observations)
		}
	}
	if mismatches != 1 {
		t.Fatalf("retryable corruption diagnostics = %+v", observations)
	}
}

func TestTransferMixedSourceCorruptPieceStrikesContributorsThenRetriesCleanly(t *testing.T) {
	data := bytes.Repeat([]byte("h"), 32<<10)
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{1, 7, 3}
	firstEndpoint, secondEndpoint := endpoint(31), endpoint(32)
	firstRequest := make(chan peer.Block, 1)
	firstRequestForSecond := make(chan peer.Block, 1)
	firstConn, firstDone := startFixturePeerMode(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false, true, firstRequest, firstRequestForSecond, nil)
	secondConn, secondDone := startFixturePeerMode(t, infoHash, []fixturePiece{{index: 0, data: data}}, true, false, false, nil, nil, firstRequestForSecond)
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers: []ConnectedPeer{
			{ID: "first-contributor", Endpoint: firstEndpoint, Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}, ReqQ: 1, ReqQSet: true},
			{ID: "second-contributor", Endpoint: secondEndpoint, Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}, ReqQ: 1, ReqQSet: true},
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		firstConn.Close()
		secondConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	for name, done := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		if err := <-done; err != nil {
			t.Fatalf("%s peer: %v", name, err)
		}
	}
	firstBlock := <-firstRequest
	if firstBlock.Length != 16<<10 {
		t.Fatalf("first contributor range = %v, want one 16 KiB block", firstBlock)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output length=%d, err=%v; want clean %d-byte piece", len(got), err, len(data))
	}
	for _, endpoint := range []peer.Endpoint{firstEndpoint, secondEndpoint} {
		if got := transfer.scheduler.StrikeCount(endpoint); got != 1 {
			t.Errorf("endpoint %v strikes=%d, want one each for mixed-source hash failure", endpoint, got)
		}
	}
}

func TestTransferCancellationJoinsAndCleansWorkspace(t *testing.T) {
	data := []byte("pause")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(t.TempDir(), "cache")
	stager := storage.NewStager(storage.StagerConfig{CacheRoot: cache, MaxPieces: 1, MaxBytes: int64(len(data))})
	conn, remoteDone := startFixturePeer(t, [20]byte{2, 4, 6}, []fixturePiece{{index: 0, data: data}}, false, true)
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{2, 4, 6}, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "cancel-peer", Conn: conn, Handshake: peer.Handshake{InfoHash: [20]byte{2, 4, 6}, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transfer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	_ = conn.Close()
	select {
	case <-remoteDone:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture peer did not exit")
	}
	if workspace := stager.Workspace(); workspace != "" {
		if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("workspace still exists: %s (%v)", workspace, err)
		}
	}
}

func TestTransferSelectedMultiFileSkipsPaddingAndUnselectedFile(t *testing.T) {
	full := append([]byte("abc"), 0, 0, 'X', 'Y', 'Z', 'd', 'e', 'f')
	meta := torrent.Metainfo{
		Name:        "multi",
		MultiFile:   true,
		TotalLength: int64(len(full)),
		PieceLength: 6,
		Files: []torrent.File{
			{Index: 0, Path: "a", Range: torrent.ByteRange{Begin: 0, End: 3}, Kind: torrent.RegularFile},
			{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 5}, Kind: torrent.PaddingFile},
			{Index: 2, Path: "skip", Range: torrent.ByteRange{Begin: 5, End: 8}, Kind: torrent.RegularFile},
			{Index: 3, Path: "c", Range: torrent.ByteRange{Begin: 8, End: 11}, Kind: torrent.RegularFile},
		},
		Pieces: []torrent.Piece{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 6}, Hash: sha1.Sum(full[:6])},
			{Index: 1, Range: torrent.ByteRange{Begin: 6, End: 11}, Hash: sha1.Sum(full[6:])},
		},
	}
	selection, err := torrent.Select(meta, []string{"a", "c"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{7, 7, 7}
	conn, remoteDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: full[:6]}, {index: 1, data: full[6:]}}, false, false)
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 2, MaxBytes: int64(len(full))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "multi-peer", Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     2, PieceLength: 6, LastPieceLength: 5,
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-remoteDone; err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "multi", "a")); err != nil || string(got) != "abc" {
		t.Fatalf("a = %q, %v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "multi", "c")); err != nil || string(got) != "def" {
		t.Fatalf("c = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "multi", "skip")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected file exists: %v", err)
	}
}

func TestTransferDisconnectReassignsOutstandingBlock(t *testing.T) {
	data := []byte("move")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{8, 8, 8}
	firstEndpoint, secondEndpoint := endpoint(41), endpoint(42)
	firstRequests, secondRequests := make(chan peer.Block, 1), make(chan peer.Block, 1)
	firstConn, firstDone := startFixturePeerMode(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, true, false, firstRequests, nil, nil)
	secondConn, secondDone := startFixturePeerObserved(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false, secondRequests)
	firstFixtureResult := make(chan error, 1)
	provided := false
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "disconnecting", Endpoint: firstEndpoint, Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}}},
		AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
			if !provided {
				select {
				case firstErr := <-firstDone:
					firstFixtureResult <- firstErr
					if firstErr != nil {
						return ConnectedPeer{}, firstErr
					}
				case <-ctx.Done():
					return ConnectedPeer{}, ctx.Err()
				}
				provided = true
				return ConnectedPeer{ID: "replacement", Endpoint: secondEndpoint, Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}}, nil
			}
			<-ctx.Done()
			return ConnectedPeer{}, ctx.Err()
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		firstConn.Close()
		secondConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstFixtureResult:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("replacement peer was not gated on first peer disconnect")
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	var firstBlock, secondBlock peer.Block
	select {
	case firstBlock = <-firstRequests:
	default:
		t.Fatal("disconnecting peer did not receive an outstanding request")
	}
	select {
	case secondBlock = <-secondRequests:
	default:
		t.Fatal("replacement peer did not receive the reassigned request")
	}
	if firstBlock != secondBlock {
		t.Fatalf("disconnected request %v was not reassigned unchanged; replacement requested %v", firstBlock, secondBlock)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("output = %q, want %q", got, data)
	}
	if transfer.scheduler.StrikeCount(firstEndpoint) != 0 || transfer.scheduler.StrikeCount(secondEndpoint) != 0 {
		t.Fatalf("ordinary disconnect/reassignment caused corruption strikes: first=%d second=%d", transfer.scheduler.StrikeCount(firstEndpoint), transfer.scheduler.StrikeCount(secondEndpoint))
	}
}

func TestTransferAdmitsPeerAfterAllCurrentPeersDisconnect(t *testing.T) {
	data := []byte("late")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{11, 11, 11}
	firstConn, firstDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, true)
	secondConn, secondDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	provided := false
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "first", Endpoint: endpoint(1), Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}}},
		AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
			if !provided {
				select {
				case <-firstDone:
				case <-ctx.Done():
					return ConnectedPeer{}, ctx.Err()
				}
				provided = true
				return ConnectedPeer{ID: "late", Endpoint: endpoint(2), Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}}, nil
			}
			<-ctx.Done()
			return ConnectedPeer{}, ctx.Err()
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		firstConn.Close()
		secondConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
}

func TestTransferAcquireCancellationReleasesLocalCandidate(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	released := make(chan struct{}, 1)
	transfer := &Transfer{
		acquirePeer: func(context.Context) (ConnectedPeer, error) {
			return ConnectedPeer{Conn: local}, nil
		},
		releasePeer: func(ConnectedPeer) { released <- struct{}{} },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := make(chan acquireResult)
	var wg sync.WaitGroup
	wg.Add(1)
	go transfer.acquireLoop(ctx, results, &wg)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("canceled acquisition did not release candidate")
	}
	wg.Wait()
}

func TestTransferEndgameDuplicateWinnerDoesNotDoubleCommit(t *testing.T) {
	data := []byte("endgame")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{12, 12, 12}
	firstConn, firstDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	secondConn, secondDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers: []ConnectedPeer{
			{ID: "first", Endpoint: endpoint(1), Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}},
			{ID: "second", Endpoint: endpoint(2), Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}},
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		firstConn.Close()
		secondConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runErr := transfer.Run(ctx)
	firstErr, secondErr := <-firstDone, <-secondDone
	if runErr != nil {
		t.Fatalf("transfer = %v; fixture peers = %v, %v", runErr, firstErr, secondErr)
	}
	if got, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
	if progress := transfer.Progress(); progress.Verified != int64(len(data)) {
		t.Fatalf("progress = %#v", progress)
	}
	endgameEvents := 0
	for _, event := range observations {
		if event.Detail == "endgame entered" {
			endgameEvents++
		}
	}
	if endgameEvents != 1 {
		t.Fatalf("endgame transition diagnostics = %+v", observations)
	}
	// The winner can commit while the other fixture peer is writing its
	// redundant response. Closing that connection may interrupt the write.
	for _, err := range []error{firstErr, secondErr} {
		if err != nil && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("fixture peer = %v", err)
		}
	}
}

func TestTransferFatalStagingStartDiagnostic(t *testing.T) {
	data := []byte("good")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	cacheFile := filepath.Join(t.TempDir(), "cache")
	if err := os.WriteFile(cacheFile, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{40, 41, 42}
	conn, remoteDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: cacheFile}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "storage-failure", Endpoint: endpoint(40), Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if err := transfer.Run(context.Background()); !errors.Is(err, storage.ErrStagingFatal) {
		t.Fatalf("Transfer.Run = %v, want fatal cache-start error", err)
	}
	if err := <-remoteDone; err != nil {
		t.Fatalf("fixture peer: %v", err)
	}
	if len(observations) != 1 || observations[0].Kind != DiagnosticTransfer || observations[0].Phase != "transfer" || observations[0].Detail != "fatal staging failure: cache start" {
		t.Fatalf("cache-start diagnostics = %+v, want one bounded fatal-staging event", observations)
	}
	if output, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || len(output) != 0 {
		t.Fatalf("output after cache-start failure = %q, err=%v; want empty", output, err)
	}
}

func TestTransferFatalStagingFailureDiagnosticAtStorageBoundary(t *testing.T) {
	for _, test := range []struct {
		name       string
		failOpen   bool
		wantDetail string
		wantWrites int32
	}{
		{name: "open", failOpen: true, wantDetail: "fatal staging failure: piece admission"},
		{name: "write", wantDetail: "fatal staging failure: block write", wantWrites: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte("good")
			meta := singleFileMeta(data)
			selection, err := torrent.Select(meta, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			plan, err := storage.Validate(root, meta, selection.SelectedIndices())
			if err != nil {
				t.Fatal(err)
			}
			infoHash := [20]byte{41, 42, 43}
			conn, remoteDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
			injectedErr := errors.New("injected stage storage failure")
			var opens, reads, writes atomic.Int32
			stager := storage.NewStager(storage.StagerConfig{
				CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data)),
				OpenFile: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
					opens.Add(1)
					if test.failOpen {
						return nil, injectedErr
					}
					file, err := os.OpenFile(path, flag, mode)
					if err != nil {
						return nil, err
					}
					return &observedStageFile{StagingFile: file, reads: &reads, writes: &writes, writeErr: injectedErr}, nil
				},
			})
			var observations []Diagnostic
			transfer, err := NewTransfer(TransferConfig{
				Selection: selection, Output: plan, Stager: stager,
				LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
				Peers:          []ConnectedPeer{{ID: "storage-failure", Endpoint: endpoint(41), Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
				PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
				OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
			})
			if err != nil {
				conn.Close()
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := transfer.Run(ctx); !errors.Is(err, injectedErr) || !errors.Is(err, storage.ErrStagingFatal) {
				t.Fatalf("Transfer.Run = %v, want injected fatal staging error", err)
			}
			if err := <-remoteDone; err != nil {
				t.Fatalf("fixture peer: %v", err)
			}
			if opens.Load() != 1 || writes.Load() != test.wantWrites || reads.Load() != 0 {
				t.Fatalf("stage I/O = opens %d, writes %d, reads %d; want 1/%d/0", opens.Load(), writes.Load(), reads.Load(), test.wantWrites)
			}
			fatalCount := 0
			for _, event := range observations {
				if event.Kind == DiagnosticTransfer && event.Phase == "transfer" && event.Detail == test.wantDetail {
					fatalCount++
					continue
				}
				if strings.Contains(event.Detail, "block ") {
					t.Fatalf("unexpected per-block diagnostic: %+v", event)
				}
			}
			if fatalCount != 1 {
				t.Fatalf("fatal staging diagnostics = %d, want one (%q): %+v", fatalCount, test.wantDetail, observations)
			}
			output, err := os.ReadFile(filepath.Join(root, "fixture"))
			if err != nil || len(output) != 0 {
				t.Fatalf("output after staging failure = %q, err=%v; want empty", output, err)
			}
			workspace := stager.Workspace()
			if workspace != "" {
				if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staging workspace remains after failure: %s (%v)", workspace, err)
				}
			}
		})
	}
}

type countedOutputReadFile struct {
	*os.File
	reads *atomic.Int32
}

func (f *countedOutputReadFile) ReadAt(data []byte, offset int64) (int, error) {
	f.reads.Add(1)
	return f.File.ReadAt(data, offset)
}

type observedStageFile struct {
	storage.StagingFile
	reads           *atomic.Int32
	prePayloadReads *atomic.Int32
	payloadSeen     *atomic.Bool
	writes          *atomic.Int32
	writeErr        error
}

func (f *observedStageFile) ReadAt(data []byte, offset int64) (int, error) {
	f.reads.Add(1)
	if f.prePayloadReads != nil && f.payloadSeen != nil && !f.payloadSeen.Load() {
		f.prePayloadReads.Add(1)
	}
	return f.StagingFile.ReadAt(data, offset)
}

func (f *observedStageFile) WriteAt(data []byte, offset int64) (int, error) {
	f.writes.Add(1)
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.StagingFile.WriteAt(data, offset)
}

func TestTransferStagingAdmissionFailureIsFatalAndCleansWorkspace(t *testing.T) {
	data := []byte("full")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{9, 9, 9}
	conn, remoteDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	cache := filepath.Join(t.TempDir(), "cache")
	stager := storage.NewStager(storage.StagerConfig{CacheRoot: cache, MaxPieces: 1, MaxBytes: int64(len(data) - 1)})
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "storage-failure", Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnDiagnostic: func(event Diagnostic) { observations = append(observations, event) },
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); !errors.Is(err, storage.ErrStagingLimit) {
		t.Fatalf("Run = %v, want staging limit", err)
	}
	_ = conn.Close()
	select {
	case <-remoteDone:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture peer did not exit")
	}
	if workspace := stager.Workspace(); workspace != "" {
		if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("workspace still exists: %s (%v)", workspace, err)
		}
	}
	for _, event := range observations {
		if strings.Contains(event.Detail, "fatal staging failure") {
			t.Fatalf("staging budget exhaustion reported as fatal storage failure: %+v", event)
		}
	}
}

func TestTransferRejectsIncomingPayloadRequestWithoutUploading(t *testing.T) {
	data := []byte("fast")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "fixture")
	if err := os.WriteFile(outputPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var outputReads atomic.Int32
	plan = plan.WithReadAtOpener(func(path string) (storage.ReadAtCloser, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return &countedOutputReadFile{File: file, reads: &outputReads}, nil
	})
	probe := make([]byte, 1)
	if n, err := plan.ReadAt(0, 0, probe); err != nil || n != 1 || probe[0] != data[0] || outputReads.Load() != 1 {
		t.Fatalf("read spy probe = %d/%v/%q, calls=%d; want one actual output read", n, err, probe, outputReads.Load())
	}
	outputReads.Store(0)
	infoHash := [20]byte{6, 6, 6}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	payloadRequests := []peer.Block{{Index: 0, Begin: 0, Length: 2}, {Index: 0, Begin: 2, Length: 2}}
	metadataRequests := []uint32{0, 1}
	payloadRejects := make(map[peer.Block]int)
	metadataRejects := make(map[uint32]int)
	var reads, prePayloadReads atomic.Int32
	var payloadSeen atomic.Bool
	var readsAfterRequests, outputReadsAfterRequests int32
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			serverDone <- err
			return
		}
		reserved := [8]byte{5: 0x10, 7: peer.FastExtensionBit}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{2, 3, 4}, reserved); err != nil {
			serverDone <- err
			return
		}
		options := peer.ReadOptions{Fast: true, MetadataExtensionID: 7}
		message, err := peer.ReadMessageWithOptions(conn, options)
		if err != nil || message.ID != peer.HaveNoneID {
			serverDone <- fmt.Errorf("initial availability = %+v, %v; want Have None first", message, err)
			return
		}
		message, err = peer.ReadMessageWithOptions(conn, options)
		if err != nil || message.ID != peer.ExtendedID || len(message.Payload) == 0 || message.Payload[0] != peer.ExtensionHandshakeID {
			serverDone <- fmt.Errorf("local extension handshake = %+v, %v", message, err)
			return
		}
		if err := writeTestFrame(conn, extensionHandshakeFrame(7, (16<<10)+1)); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			serverDone <- err
			return
		}
		requested := false
		for !requested {
			message, err = peer.ReadMessageWithOptions(conn, options)
			if err != nil {
				serverDone <- err
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID {
				continue
			}
			if message.ID != peer.RequestID || len(message.Payload) != 12 || binary.BigEndian.Uint32(message.Payload[:4]) != 0 || binary.BigEndian.Uint32(message.Payload[4:8]) != 0 || binary.BigEndian.Uint32(message.Payload[8:12]) != uint32(len(data)) {
				serverDone <- fmt.Errorf("unexpected download request: %x", message.Payload)
				return
			}
			requested = true
		}
		for _, block := range payloadRequests {
			request := make([]byte, 12)
			binary.BigEndian.PutUint32(request[:4], block.Index)
			binary.BigEndian.PutUint32(request[4:8], block.Begin)
			binary.BigEndian.PutUint32(request[8:12], block.Length)
			if err := writeFixtureFrame(conn, peer.RequestID, request); err != nil {
				serverDone <- err
				return
			}
		}
		for _, piece := range metadataRequests {
			if err := writeTestFrame(conn, metadataControlFrame(1, peer.MetadataRequest, piece)); err != nil {
				serverDone <- err
				return
			}
		}
		// A valid Cancel has no upload response; it also must not access storage.
		cancelRequest := make([]byte, 12)
		binary.BigEndian.PutUint32(cancelRequest[8:], 1)
		if err := writeFixtureFrame(conn, peer.CancelID, cancelRequest); err != nil {
			serverDone <- err
			return
		}
		for received := 0; received < len(payloadRequests)+len(metadataRequests); received++ {
			message, err := peer.ReadMessageWithOptions(conn, options)
			if err != nil {
				serverDone <- err
				return
			}
			switch message.ID {
			case peer.RejectRequestID:
				block := peer.Block{
					Index:  binary.BigEndian.Uint32(message.Payload[:4]),
					Begin:  binary.BigEndian.Uint32(message.Payload[4:8]),
					Length: binary.BigEndian.Uint32(message.Payload[8:12]),
				}
				payloadRejects[block]++
			case peer.ExtendedID:
				if len(message.Payload) < 2 || message.Payload[0] != 7 {
					serverDone <- fmt.Errorf("unexpected extension response: %x", message.Payload)
					return
				}
				control, err := peer.ParseMetadataControl(message.Payload[1:])
				if err != nil || control.Type != peer.MetadataReject {
					serverDone <- fmt.Errorf("metadata response = %+v, %v; want reject", control, err)
					return
				}
				metadataRejects[control.Piece]++
			default:
				serverDone <- fmt.Errorf("unexpected response to incoming request: id %d", message.ID)
				return
			}
		}
		readsAfterRequests = reads.Load()
		outputReadsAfterRequests = outputReads.Load()
		payload := make([]byte, 8+len(data))
		copy(payload[8:], data)
		if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
			serverDone <- err
			return
		}
		for {
			message, err := peer.ReadMessageWithOptions(conn, options)
			if err != nil {
				if peer.IsDisconnect(err) {
					serverDone <- nil
				} else {
					serverDone <- err
				}
				return
			}
			serverDone <- fmt.Errorf("unexpected message after download piece: id %d", message.ID)
			return
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	reserved := [8]byte{5: 0x10, 7: peer.FastExtensionBit}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}, Reserved: reserved}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	stager := storage.NewStager(storage.StagerConfig{
		CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data)),
		OpenFile: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
			file, err := os.OpenFile(path, flag, mode)
			if err != nil {
				return nil, err
			}
			return &observedStageFile{StagingFile: file, reads: &reads, prePayloadReads: &prePayloadReads, payloadSeen: &payloadSeen, writes: new(atomic.Int32)}, nil
		},
	})
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "requesting-peer", Conn: conn, Handshake: remote}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(int64) error {
			payloadSeen.Store(true)
			return nil
		},
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	for _, block := range payloadRequests {
		if payloadRejects[block] != 1 {
			t.Errorf("Fast payload request %v received %d rejects, want exactly one", block, payloadRejects[block])
		}
	}
	for _, piece := range metadataRequests {
		if metadataRejects[piece] != 1 {
			t.Errorf("metadata request %d received %d rejects, want exactly one", piece, metadataRejects[piece])
		}
	}
	if len(payloadRejects) != len(payloadRequests) || len(metadataRejects) != len(metadataRequests) {
		t.Fatalf("unexpected rejection set: payload=%v metadata=%v", payloadRejects, metadataRejects)
	}
	if readsAfterRequests != 0 || prePayloadReads.Load() != 0 || outputReadsAfterRequests != 0 || outputReads.Load() != 0 {
		t.Fatalf("storage reads before payload acceptance: cache at rejection barrier=%d, cache total=%d, output at barrier=%d, output total=%d; want zero", readsAfterRequests, prePayloadReads.Load(), outputReadsAfterRequests, outputReads.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("cache read spy did not observe the normal finalizer read")
	}
	if outputReads.Load() != 0 {
		t.Fatalf("output read spy observed %d reads during transfer; want zero", outputReads.Load())
	}
	if output, err := os.ReadFile(outputPath); err != nil || string(output) != string(data) {
		t.Fatalf("downloaded output = %q, err=%v; want %q", output, err, data)
	}
}

func TestTransferIgnoresIncomingNonFastPayloadRequest(t *testing.T) {
	data := []byte("plain")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{14, 15, 16}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var reads, prePayloadReads atomic.Int32
	var payloadSeen atomic.Bool
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			serverDone <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{17, 18, 19}, [8]byte{}); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			serverDone <- err
			return
		}
		requested := false
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) && requested {
					serverDone <- nil
				} else {
					serverDone <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID {
				continue
			}
			if message.ID != peer.RequestID || requested {
				serverDone <- fmt.Errorf("unexpected response to non-Fast request: id %d", message.ID)
				return
			}
			if len(message.Payload) != 12 || binary.BigEndian.Uint32(message.Payload[8:12]) != uint32(len(data)) {
				serverDone <- fmt.Errorf("unexpected download request: %x", message.Payload)
				return
			}
			request := make([]byte, 12)
			binary.BigEndian.PutUint32(request[8:], uint32(len(data)))
			if err := writeFixtureFrame(conn, peer.RequestID, request); err != nil {
				serverDone <- err
				return
			}
			requested = true
			payload := make([]byte, 8+len(data))
			copy(payload[8:], data)
			if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
				serverDone <- err
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	stager := storage.NewStager(storage.StagerConfig{
		CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data)),
		OpenFile: func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
			file, err := os.OpenFile(path, flag, mode)
			if err != nil {
				return nil, err
			}
			return &observedStageFile{StagingFile: file, reads: &reads, prePayloadReads: &prePayloadReads, payloadSeen: &payloadSeen, writes: new(atomic.Int32)}, nil
		},
	})
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "non-fast-requester", Conn: conn, Handshake: remote}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(int64) error {
			payloadSeen.Store(true)
			return nil
		},
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if prePayloadReads.Load() != 0 {
		t.Fatalf("cache reads before payload acceptance = %d, want zero for ignored request", prePayloadReads.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("cache read spy did not observe the normal finalizer read")
	}
	if output, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || string(output) != string(data) {
		t.Fatalf("downloaded output = %q, err=%v; want %q", output, err, data)
	}
}

func TestTransferFastAllowedPieceCanProgressWhileChoked(t *testing.T) {
	data := []byte("fast")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{5, 5, 5}
	reserved := [8]byte{7: peer.FastExtensionBit}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			serverDone <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{7, 7, 7}, reserved); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			serverDone <- err
			return
		}
		allowed := make([]byte, 4)
		for range 32 {
			if err := writeFixtureFrame(conn, peer.AllowedFastID, allowed); err != nil {
				serverDone <- err
				return
			}
		}
		requested := false
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) && requested {
					serverDone <- nil
				} else {
					serverDone <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.HaveNoneID || message.ID == peer.InterestedID {
				continue
			}
			if message.ID != peer.RequestID {
				serverDone <- fmt.Errorf("unexpected message id %d", message.ID)
				return
			}
			requested = true
			payload := make([]byte, 8+len(data))
			copy(payload[8:], data)
			if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
				serverDone <- err
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}, Reserved: reserved}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(100, 0)
	var observations []Diagnostic
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "fast-peer", Conn: conn, Handshake: remote}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		Now: func() time.Time { return clock },
		OnDiagnostic: func(event Diagnostic) {
			observations = append(observations, event)
			if event.Detail == "Allowed Fast grant summary" {
				clock = clock.Add(3 * time.Second)
			}
		},
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
	var allowed, used, useful, chokeDuration bool
	grantSummaries, requestSummaries := 0, 0
	for _, event := range observations {
		if event.Detail == "Allowed Fast grant summary" {
			allowed = event.Count == 1
			grantSummaries++
		}
		if event.Detail == "request assignment summary outstanding=1" {
			requestSummaries++
		}
		used = used || event.Detail == "Allowed Fast enabled request piece=0"
		useful = useful || event.Detail == "first useful block accepted and staged"
		chokeDuration = chokeDuration || event.Detail == "peer disconnected while choked" && event.Duration >= 3*time.Second
	}
	if !allowed || !used || !useful || !chokeDuration || grantSummaries != 1 || requestSummaries > 1 {
		t.Fatalf("Fast/choke observations allowed=%t used=%t useful=%t duration=%t summaries=%d/%d: %+v", allowed, used, useful, chokeDuration, grantSummaries, requestSummaries, observations)
	}
}

func TestTransferOrdinaryChokeReassignsAfterLateTerminal(t *testing.T) {
	data := []byte("flip")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{4, 4, 4}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			serverDone <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{6, 6, 6}, [8]byte{}); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			serverDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			serverDone <- err
			return
		}
		requests := 0
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) && requests >= 2 {
					serverDone <- nil
				} else {
					serverDone <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID {
				continue
			}
			if message.ID != peer.RequestID {
				serverDone <- fmt.Errorf("unexpected message id %d", message.ID)
				return
			}
			requests++
			if requests == 1 {
				if err := writeFixtureFrame(conn, peer.ChokeID, nil); err != nil {
					serverDone <- err
					return
				}
				if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
					serverDone <- err
					return
				}
			}
			payload := make([]byte, 8+len(data))
			copy(payload[8:], data)
			if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
				serverDone <- err
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "choking-peer", Conn: conn, Handshake: remote}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
}

type transferWriteErrorConn struct{}

func (*transferWriteErrorConn) Read([]byte) (int, error) { return 0, io.EOF }
func (*transferWriteErrorConn) Write([]byte) (int, error) {
	return 0, errors.New("transfer write failure")
}
func (*transferWriteErrorConn) Close() error                     { return nil }
func (*transferWriteErrorConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*transferWriteErrorConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*transferWriteErrorConn) SetDeadline(time.Time) error      { return nil }
func (*transferWriteErrorConn) SetReadDeadline(time.Time) error  { return nil }
func (*transferWriteErrorConn) SetWriteDeadline(time.Time) error { return nil }

type fixturePiece struct {
	index int
	data  []byte
}

// startFixturePeer is intentionally a wire fixture rather than a peer
// encoder. It writes independent BEP 3 frames and only uses the production
// decoder to inspect the request tuple.
func startFixturePeer(t *testing.T, infoHash [20]byte, pieces []fixturePiece, badOnce bool, disconnectBeforePiece bool) (net.Conn, <-chan error) {
	return startFixturePeerMode(t, infoHash, pieces, badOnce, disconnectBeforePiece, false, nil, nil, nil)
}

func startFixturePeerObserved(t *testing.T, infoHash [20]byte, pieces []fixturePiece, badOnce bool, disconnectBeforePiece bool, observed chan<- peer.Block) (net.Conn, <-chan error) {
	return startFixturePeerMode(t, infoHash, pieces, badOnce, disconnectBeforePiece, false, observed, nil, nil)
}

func startFixturePeerMode(t *testing.T, infoHash [20]byte, pieces []fixturePiece, badOnce bool, disconnectBeforePiece bool, disconnectAfterFirst bool, observed chan<- peer.Block, observedAlso chan<- peer.Block, corruptDifferentFrom <-chan peer.Block) (net.Conn, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer conn.Close()
		defer listener.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			done <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{3, 2, 1}, [8]byte{}); err != nil {
			done <- err
			return
		}
		bitfield := []byte{0x80}
		for _, piece := range pieces {
			if piece.index >= 8 {
				bitfield = []byte{0xff, 0xff}
				break
			}
			bitfield[0] |= byte(1 << (7 - piece.index))
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, bitfield); err != nil {
			done <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			done <- err
			return
		}
		byIndex := make(map[int][]byte, len(pieces))
		for _, piece := range pieces {
			byIndex[piece.index] = piece.data
		}
		corrupted := false
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) {
					done <- nil
				} else {
					done <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID || message.ID == peer.CancelID {
				continue
			}
			if message.ID != peer.RequestID || len(message.Payload) != 12 {
				done <- io.ErrUnexpectedEOF
				return
			}
			index := int(binary.BigEndian.Uint32(message.Payload[:4]))
			begin := int(binary.BigEndian.Uint32(message.Payload[4:8]))
			length := int(binary.BigEndian.Uint32(message.Payload[8:12]))
			if observed != nil {
				block := peer.Block{Index: uint32(index), Begin: uint32(begin), Length: uint32(length)}
				observed <- block
				if observedAlso != nil {
					observedAlso <- block
				}
			}
			piece := byIndex[index]
			if begin < 0 || length <= 0 || begin+length > len(piece) {
				done <- io.ErrUnexpectedEOF
				return
			}
			if disconnectBeforePiece {
				_ = conn.Close()
				done <- nil
				return
			}
			payload := append([]byte(nil), piece[begin:begin+length]...)
			corruptThis := badOnce && !corrupted
			if corruptDifferentFrom != nil {
				first := <-corruptDifferentFrom
				corruptDifferentFrom = nil
				if uint32(index) == first.Index && uint32(begin) == first.Begin {
					corruptThis = false
				}
			}
			if corruptThis {
				payload[0] ^= 0xff
				corrupted = true
			}
			framePayload := make([]byte, 8+len(payload))
			binary.BigEndian.PutUint32(framePayload[:4], uint32(index))
			binary.BigEndian.PutUint32(framePayload[4:8], uint32(begin))
			copy(framePayload[8:], payload)
			if err := writeFixtureFrame(conn, peer.PieceID, framePayload); err != nil {
				done <- err
				return
			}
			if disconnectAfterFirst {
				done <- nil
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		conn.Close()
		listener.Close()
		t.Fatal(err)
	}
	if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
		conn.Close()
		listener.Close()
		t.Fatal(err)
	}
	return conn, done
}

func TestTransferExpiresStalledBlocksAndUsesLaterPeer(t *testing.T) {
	data := make([]byte, 2*limits.BlockBytes+5000)
	for i := range data {
		data[i] = byte(i * 31)
	}
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	plan, err := storage.Validate(root, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	infoHash := [20]byte{71, 72, 73}
	firstEndpoint, secondEndpoint := endpoint(71), endpoint(72)
	firstConn, firstDone, firstBlock, canceled := startFixturePeerOneBlockThenSilent(t, infoHash, data)
	secondRequests := make(chan peer.Block, 4)
	secondConn, secondDone := startFixturePeerObserved(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false, secondRequests)
	replacement := make(chan struct{})
	var provided bool
	base := time.Unix(1000, 0)
	var sawUsefulPayload atomic.Bool
	var usefulClockReturned atomic.Bool
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "stalling", Endpoint: firstEndpoint, Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 2, 3}}}},
		AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
			if provided {
				<-ctx.Done()
				return ConnectedPeer{}, ctx.Err()
			}
			select {
			case <-replacement:
				provided = true
				return ConnectedPeer{ID: "replacement", Endpoint: secondEndpoint, Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 3, 4}}}, nil
			case <-ctx.Done():
				return ConnectedPeer{}, ctx.Err()
			}
		},
		OnPayloadReceived: func(int64) error {
			sawUsefulPayload.CompareAndSwap(false, true)
			return nil
		},
		Now: func() time.Time {
			if sawUsefulPayload.Load() && usefulClockReturned.Swap(true) {
				return base.Add(peerRequestTimeout + time.Second)
			}
			return base
		},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		firstConn.Close()
		secondConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- transfer.Run(ctx) }()
	select {
	case block := <-firstBlock:
		if block.Begin != 0 {
			t.Fatalf("first block = %#v, want begin zero", block)
		}
	case <-ctx.Done():
		t.Fatal("stalling peer did not send its first valid block")
	}
	expired := make(map[peer.Block]struct{}, 2)
	for len(expired) < 2 {
		select {
		case block := <-canceled:
			expired[block] = struct{}{}
		case <-ctx.Done():
			t.Fatalf("timed-out requests were not canceled: got %d", len(expired))
		}
	}
	close(replacement)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("later peer did not complete transfer without a no-progress timeout")
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	requested := make(map[peer.Block]struct{}, len(secondRequests))
	for len(secondRequests) != 0 {
		requested[<-secondRequests] = struct{}{}
	}
	for block := range expired {
		if _, ok := requested[block]; !ok {
			t.Errorf("expired block %#v was not reassigned to the later peer", block)
		}
	}
	if transfer.scheduler.StrikeCount(firstEndpoint) != 0 || transfer.scheduler.StrikeCount(secondEndpoint) != 0 {
		t.Fatalf("request timeout caused corruption strikes: first=%d second=%d", transfer.scheduler.StrikeCount(firstEndpoint), transfer.scheduler.StrikeCount(secondEndpoint))
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output matches input = %v, %v", bytes.Equal(got, data), err)
	}
}

func TestTransferTimeoutPreservesFastLateTerminalWithoutStrike(t *testing.T) {
	transfer, p, remote, messages, block, setNow, base := newTransferWithOutstandingTestRequest(t, true)
	var observations []Diagnostic
	transfer.onDiagnostic = func(event Diagnostic) { observations = append(observations, event) }
	defer func() {
		_ = p.worker.Close()
		_ = transfer.scheduler.RemovePeer(p.input.ID)
		_ = remote.Close()
	}()
	setNow(base.Add(peerRequestTimeout + time.Second))
	if err := transfer.expireRequests(context.Background(), []*transferPeer{p}); err != nil {
		t.Fatal(err)
	}
	if !p.state.Requests().Tombstoned(block) || transfer.scheduler.ActiveRequests() != 0 {
		t.Fatalf("timeout did not preserve the terminal and release the assignment: tombstone=%v active=%d", p.state.Requests().Tombstoned(block), transfer.scheduler.ActiveRequests())
	}
	if _, ok := p.tombstoned[block]; !ok {
		t.Fatal("timed-out tuple was not excluded from this connection")
	}
	var cancelMessage peer.Message
	timeout := time.After(time.Second)
	for cancelMessage.ID != peer.CancelID {
		select {
		case cancelMessage = <-messages:
		case <-timeout:
			t.Fatal("timeout did not send a bounded Cancel")
		}
	}
	if cancelMessage.ID != peer.CancelID || !bytes.Equal(cancelMessage.Payload, blockPayload(block)) {
		t.Fatalf("timeout cancel = %#v, want exact tuple %#v", cancelMessage, block)
	}
	if requests, err := transfer.scheduler.nextRequestsExcluding(p.input.ID, p.state.ReqQ(), p.tombstoned); err != nil || len(requests) != 0 {
		t.Fatalf("tombstoned tuple was reassigned on same connection: %#v, %v", requests, err)
	}
	payload := make([]byte, 8+block.Length)
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.PieceID, Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	if p.state.Requests().TombstoneCount() != 0 {
		t.Fatalf("late terminal left %d tombstones", p.state.Requests().TombstoneCount())
	}
	if _, ok := p.tombstoned[block]; ok {
		t.Fatal("late terminal did not release the tuple exclusion")
	}
	requests, err := transfer.scheduler.NextRequests(p.input.ID, p.state.ReqQ())
	if err != nil || len(requests) != 1 || requests[0].Block != block {
		t.Fatalf("consumed late tuple was not reusable: %#v, %v", requests, err)
	}
	if transfer.scheduler.StrikeCount(p.input.Endpoint) != 0 {
		t.Fatalf("timeout or late terminal caused a strike: %d", transfer.scheduler.StrikeCount(p.input.Endpoint))
	}
	lateCount := 0
	for _, event := range observations {
		if event.Detail == "exact tombstone consumed" {
			lateCount++
		}
	}
	if lateCount != 1 || transfer.Progress().Verified != 0 {
		t.Fatalf("late terminal observation count=%d verified=%d observations=%+v", lateCount, transfer.Progress().Verified, observations)
	}
}

func TestTransferTimeoutPreservesFastLateRejectWithoutStrike(t *testing.T) {
	transfer, p, remote, messages, block, setNow, base := newTransferWithOutstandingTestRequest(t, true)
	var observations []Diagnostic
	transfer.onDiagnostic = func(event Diagnostic) { observations = append(observations, event) }
	defer func() {
		_ = p.worker.Close()
		_ = transfer.scheduler.RemovePeer(p.input.ID)
		_ = remote.Close()
	}()
	setNow(base.Add(peerRequestTimeout + time.Second))
	if err := transfer.expireRequests(context.Background(), []*transferPeer{p}); err != nil {
		t.Fatal(err)
	}
	timeout := time.After(time.Second)
	cancelSeen := false
	for !cancelSeen {
		select {
		case message := <-messages:
			if message.ID == peer.CancelID {
				cancelSeen = true
			}
		case <-timeout:
			t.Fatal("timeout did not send a bounded Cancel")
		}
	}
	if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.RejectRequestID, Payload: blockPayload(block)}}); err != nil {
		t.Fatal(err)
	}
	if p.state.Requests().TombstoneCount() != 0 {
		t.Fatalf("late Reject left %d tombstones", p.state.Requests().TombstoneCount())
	}
	if _, ok := p.tombstoned[block]; ok {
		t.Fatal("late Reject did not release the tuple exclusion")
	}
	if transfer.scheduler.StrikeCount(p.input.Endpoint) != 0 {
		t.Fatalf("timeout or late Reject caused a strike: %d", transfer.scheduler.StrikeCount(p.input.Endpoint))
	}
	lateCount := 0
	for _, event := range observations {
		if event.Detail == "exact tombstone consumed" {
			lateCount++
		}
	}
	if lateCount != 1 || transfer.Progress().Verified != 0 {
		t.Fatalf("late Reject observation count=%d verified=%d observations=%+v", lateCount, transfer.Progress().Verified, observations)
	}
}

func TestTransferClosesWithoutStrikeWhenTimeoutTombstonesAreFull(t *testing.T) {
	transfer, p, remote, _, block, setNow, base := newTransferWithOutstandingTestRequest(t, false)
	defer func() {
		_ = p.worker.Close()
		_ = remote.Close()
	}()
	for i := 0; i < limits.RequestTombstones; i++ {
		prior := peer.Block{Index: uint32(1000 + i), Begin: uint32(i), Length: 1}
		if err := p.state.Requests().Add(prior); err != nil {
			t.Fatalf("seed tombstone request %d: %v", i, err)
		}
		if err := p.state.TimeoutRequest(prior); err != nil {
			t.Fatalf("seed tombstone %d: %v", i, err)
		}
	}
	setNow(base.Add(peerRequestTimeout + time.Second))
	if err := transfer.expireRequests(context.Background(), []*transferPeer{p}); err != nil {
		t.Fatal(err)
	}
	if !p.done || transfer.scheduler.ActiveRequests() != 0 {
		t.Fatalf("full tombstone set did not close and release peer: done=%v active=%d", p.done, transfer.scheduler.ActiveRequests())
	}
	if transfer.scheduler.StrikeCount(p.input.Endpoint) != 0 {
		t.Fatalf("full tombstone set caused a strike: %d", transfer.scheduler.StrikeCount(p.input.Endpoint))
	}
	if !p.state.Requests().Outstanding(block) {
		t.Fatal("full tombstone set forgot the request before the connection closed")
	}
}

func TestTransferClosesWithoutStrikeWhenTimeoutCancelQueueIsBlocked(t *testing.T) {
	transfer, p, remote, _, block, setNow, base := newTransferWithOutstandingTestRequest(t, false)
	defer func() {
		_ = p.worker.Close()
		_ = remote.Close()
	}()
	fillCtx, cancelFill := context.WithTimeout(context.Background(), time.Second)
	defer cancelFill()
	full := false
	for i := 0; i < 2*limits.PeerCommands; i++ {
		if err := p.worker.SendContext(fillCtx, peer.Message{ID: peer.InterestedID}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				full = true
				break
			}
			t.Fatalf("fill peer command queue: %v", err)
		}
	}
	if !full {
		t.Fatal("fixture did not block the peer command queue")
	}
	setNow(base.Add(peerRequestTimeout + time.Second))
	started := time.Now()
	if err := transfer.expireRequests(context.Background(), []*transferPeer{p}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked Cancel enqueue took %v", elapsed)
	}
	if !p.done || transfer.scheduler.ActiveRequests() != 0 {
		t.Fatalf("blocked Cancel did not close and release peer: done=%v active=%d", p.done, transfer.scheduler.ActiveRequests())
	}
	if transfer.scheduler.StrikeCount(p.input.Endpoint) != 0 {
		t.Fatalf("blocked Cancel caused a strike: %d", transfer.scheduler.StrikeCount(p.input.Endpoint))
	}
	if !p.state.Requests().Tombstoned(block) {
		t.Fatal("blocked Cancel discarded the timeout tombstone before closing")
	}
}

func TestTransferRotatesNonrespondingAllowedFastPeer(t *testing.T) {
	transfer, p, remote, _, _, setNow, base := newTransferWithOutstandingTestRequest(t, true)
	defer func() {
		_ = p.worker.Close()
		_ = transfer.scheduler.RemovePeer(p.input.ID)
		_ = remote.Close()
	}()
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.ChokeID}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.AllowedFastID, Payload: []byte{0, 0, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if p.state.RequestableCount() != 1 {
		t.Fatalf("Allowed Fast requestable count = %d, want 1", p.state.RequestableCount())
	}
	setNow(base.Add(peerIdleLimit - time.Second))
	if got := transfer.findUnproductive([]*transferPeer{p}); got != nil {
		t.Fatal("useful Allowed Fast connection was rotated before its idle window")
	}
	setNow(base.Add(peerIdleLimit + time.Second))
	if got := transfer.findUnproductive([]*transferPeer{p}); got != p {
		t.Fatal("nonresponding Allowed Fast connection retained its slot indefinitely")
	}
}

func TestTransferReplacesIdlePeerAtActivePeerCap(t *testing.T) {
	data := []byte("cap")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var fakeNow = time.Unix(2000, 0)
	inputs := make([]ConnectedPeer, limits.ActivePeers)
	remotes := make([]net.Conn, 0, limits.ActivePeers+1)
	for i := range inputs {
		local, remote := net.Pipe()
		remotes = append(remotes, remote)
		inputs[i] = ConnectedPeer{ID: fmt.Sprintf("cap-%d", i), Endpoint: endpoint(byte(i + 1)), Conn: local}
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{}, Peers: inputs,
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		Now: func() time.Time { return fakeNow },
	})
	if err != nil {
		for _, input := range inputs {
			_ = input.Conn.Close()
		}
		t.Fatal(err)
	}
	peers := make([]*transferPeer, len(inputs))
	for i, input := range inputs {
		p, err := transfer.startPeer(context.Background(), input)
		if err != nil {
			for _, remote := range remotes {
				_ = remote.Close()
			}
			t.Fatal(err)
		}
		peers[i] = p
	}
	defer func() {
		for _, p := range peers {
			if p != nil {
				_ = p.worker.Close()
			}
		}
		for _, remote := range remotes {
			_ = remote.Close()
		}
	}()
	// This timestamp represents useful data delivered earlier; that connection
	// must still become replaceable after 60 idle seconds.
	peers[0].lastUseful = fakeNow
	fakeNow = fakeNow.Add(peerIdleLimit + time.Second)
	local, remote := net.Pipe()
	remotes = append(remotes, remote)
	candidate := ConnectedPeer{ID: "cap-replacement", Endpoint: endpoint(65), Conn: local}
	if err := transfer.admitCandidate(context.Background(), &peers, &candidate); err != nil {
		t.Fatal(err)
	}
	if countLive(peers) != limits.ActivePeers {
		t.Fatalf("live peers = %d, want cap %d", countLive(peers), limits.ActivePeers)
	}
	if !peers[0].done {
		t.Fatal("idle connection that had delivered useful data retained its slot")
	}
	if peers[len(peers)-1] == nil || peers[len(peers)-1].done || peers[len(peers)-1].input.ID != candidate.ID {
		t.Fatal("later candidate was not admitted after the idle peer released its slot")
	}
}

func newTransferWithOutstandingTestRequest(t *testing.T, fast bool) (*Transfer, *transferPeer, net.Conn, <-chan peer.Message, peer.Block, func(time.Time), time.Time) {
	t.Helper()
	data := []byte("late")
	meta := singleFileMeta(data)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	output, err := storage.Validate(t.TempDir(), meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	messages := make(chan peer.Message, 8)
	go func() {
		defer remote.Close()
		for {
			message, err := peer.ReadMessage(remote)
			if err != nil {
				return
			}
			messages <- message
		}
	}()
	base := time.Unix(3000, 0)
	fakeNow := base
	var reserved [8]byte
	if fast {
		reserved[7] = peer.FastExtensionBit
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: output,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{8, 8, 8}, PeerID: [20]byte{4, 5, 6}, Reserved: reserved},
		Peers:          []ConnectedPeer{{ID: "timeout-peer", Endpoint: endpoint(80), Conn: local, Handshake: peer.Handshake{Reserved: reserved}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		Now: func() time.Time { return fakeNow },
	})
	if err != nil {
		_ = local.Close()
		_ = remote.Close()
		t.Fatal(err)
	}
	p, err := transfer.startPeer(context.Background(), transfer.peers[0])
	if err != nil {
		_ = local.Close()
		_ = remote.Close()
		t.Fatal(err)
	}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.BitfieldID, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if err := transfer.scheduler.SetAvailability(p.input.ID, []int{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.state.ApplyMessage(peer.Message{ID: peer.UnchokeID}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := transfer.scheduler.ReservePiece(p.input.ID)
	if err != nil || !ok {
		t.Fatalf("reserve stage = %#v, %v (available=%v requestable=%d rarity=%d)", offer, err, p.state.Availability(0), p.state.RequestableCount(), transfer.scheduler.pieces[0].rarity)
	}
	if err := transfer.scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := transfer.scheduler.NextRequests(p.input.ID, p.state.ReqQ())
	if err != nil || len(requests) != 1 {
		t.Fatalf("scheduler request = %#v, %v", requests, err)
	}
	block := requests[0].Block
	if err := p.state.AddRequest(block); err != nil {
		t.Fatal(err)
	}
	p.active[block] = fakeNow
	return transfer, p, remote, messages, block, func(now time.Time) { fakeNow = now }, base
}

func startFixturePeerOneBlockThenSilent(t *testing.T, infoHash [20]byte, data []byte) (net.Conn, <-chan error, <-chan peer.Block, <-chan peer.Block) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	first := make(chan peer.Block, 1)
	canceled := make(chan peer.Block, 8)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		defer listener.Close()
		if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
			done <- err
			return
		}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{3, 2, 1}, [8]byte{}); err != nil {
			done <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			done <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.UnchokeID, nil); err != nil {
			done <- err
			return
		}
		answered := false
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) {
					done <- nil
				} else {
					done <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID || message.ID == peer.NotInterestedID {
				continue
			}
			if len(message.Payload) != 12 {
				done <- io.ErrUnexpectedEOF
				return
			}
			block := peer.Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:12])}
			if message.ID == peer.CancelID {
				canceled <- block
				continue
			}
			if message.ID != peer.RequestID || answered || int(block.Begin+block.Length) > len(data) {
				if message.ID == peer.RequestID {
					// The fixture intentionally leaves all later requests unanswered.
					continue
				}
				done <- io.ErrUnexpectedEOF
				return
			}
			answered = true
			first <- block
			payload := make([]byte, 8+block.Length)
			binary.BigEndian.PutUint32(payload[:4], block.Index)
			binary.BigEndian.PutUint32(payload[4:8], block.Begin)
			copy(payload[8:], data[block.Begin:block.Begin+block.Length])
			if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
				done <- err
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		_ = conn.Close()
		_ = listener.Close()
		t.Fatal(err)
	}
	if _, err := peer.ReadHandshake(conn, &infoHash, nil); err != nil {
		_ = conn.Close()
		_ = listener.Close()
		t.Fatal(err)
	}
	return conn, done, first, canceled
}
