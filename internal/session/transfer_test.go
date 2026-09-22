package session

import (
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
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestTransferLocalTCPSingleFile(t *testing.T) {
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
		if err := peer.WriteHandshake(conn, infoHash, remoteID, [8]byte{}); err != nil {
			remoteDone <- err
			return
		}
		if err := writeFixtureFrame(conn, peer.BitfieldID, []byte{0x80}); err != nil {
			remoteDone <- err
			return
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
			if message.KeepAlive || message.ID == peer.InterestedID {
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
	local := peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}}
	if err := peer.WriteHandshake(conn, local.InfoHash, local.PeerID, local.Reserved); err != nil {
		t.Fatal(err)
	}
	remote, err := peer.ReadHandshake(conn, &infoHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	var callbackOrder []string
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
	if err != nil || !ok {
		t.Fatalf("zero reqq reserve = %#v, %v", offer, err)
	}
	if err := transfer.scheduler.AdmitPiece(offer); err != nil {
		t.Fatal(err)
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
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{4, 5, 6}},
		Peers: []ConnectedPeer{
			{ID: "closed-before-send", Endpoint: endpoint(66), Conn: first, Handshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{7, 7, 7}}},
			{ID: "usable", Endpoint: endpoint(67), Conn: second, Handshake: peer.Handshake{InfoHash: [20]byte{19, 19, 19}, PeerID: [20]byte{3, 2, 1}}},
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
	if err := transfer.Run(ctx); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
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

func TestTransferPayloadCallbackCountsLatePiece(t *testing.T) {
	data := make([]byte, 32)
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	var got int64
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: &storage.Plan{},
		Peers:      []ConnectedPeer{{ID: "late-piece", Endpoint: endpoint(62), Conn: local}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
		OnPayloadReceived: func(n int64) error {
			got += n
			return nil
		},
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
	p.active[block] = struct{}{}
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
	firstConn, firstDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, true)
	secondConn, secondDone := startFixturePeer(t, infoHash, []fixturePiece{{index: 0, data: data}}, false, false)
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers: []ConnectedPeer{
			{ID: "disconnecting", Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}},
			{ID: "replacement", Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}},
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
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("output = %q, want %q", got, data)
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
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers: []ConnectedPeer{
			{ID: "first", Endpoint: endpoint(1), Conn: firstConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{1, 1, 1}}},
			{ID: "second", Endpoint: endpoint(2), Conn: secondConn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{2, 2, 2}}},
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
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "fixture")); err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v", got, err)
	}
	if progress := transfer.Progress(); progress.Verified != int64(len(data)) {
		t.Fatalf("progress = %#v", progress)
	}
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
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan, Stager: stager,
		LocalHandshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "storage-failure", Conn: conn, Handshake: peer.Handshake{InfoHash: infoHash, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
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
	infoHash := [20]byte{6, 6, 6}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	seenReject := make(chan bool, 1)
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
		reserved := [8]byte{7: peer.FastExtensionBit}
		if err := peer.WriteHandshake(conn, infoHash, [20]byte{2, 3, 4}, reserved); err != nil {
			serverDone <- err
			return
		}
		requestPayload := make([]byte, 12)
		binary.BigEndian.PutUint32(requestPayload[8:], uint32(len(data)))
		if err := writeFixtureFrame(conn, peer.RequestID, requestPayload); err != nil {
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
		gotReject := false
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				if peer.IsDisconnect(err) {
					seenReject <- gotReject
					serverDone <- nil
				} else {
					serverDone <- err
				}
				return
			}
			if message.KeepAlive || message.ID == peer.InterestedID {
				continue
			}
			switch message.ID {
			case peer.RejectRequestID:
				gotReject = true
			case peer.RequestID:
				payload := make([]byte, 8+len(data))
				copy(payload[8:], data)
				binary.BigEndian.PutUint32(payload[8-4:8], 0)
				if err := writeFixtureFrame(conn, peer.PieceID, payload); err != nil {
					serverDone <- err
					return
				}
			case peer.PieceID, peer.BitfieldID, peer.HaveID, peer.HaveAllID, peer.UnchokeID:
				serverDone <- fmt.Errorf("forbidden upload message id %d", message.ID)
				return
			}
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
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "requesting-peer", Conn: conn, Handshake: remote}},
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
	if !<-seenReject {
		t.Fatal("incoming Fast request did not receive Reject Request")
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
		if err := writeFixtureFrame(conn, peer.AllowedFastID, allowed); err != nil {
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
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: local,
		Peers:          []ConnectedPeer{{ID: "fast-peer", Conn: conn, Handshake: remote}},
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
			piece := byIndex[index]
			if begin < 0 || length <= 0 || begin+length > len(piece) {
				done <- io.ErrUnexpectedEOF
				return
			}
			if disconnectBeforePiece {
				done <- nil
				return
			}
			payload := append([]byte(nil), piece[begin:begin+length]...)
			corruptThis := badOnce && !corrupted
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
