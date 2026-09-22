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
	"sync"
	"testing"
	"time"

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
	transfer, err := NewTransfer(TransferConfig{
		Selection:       selection,
		Output:          plan,
		Stager:          stager,
		LocalHandshake:  local,
		Peers:           []ConnectedPeer{{ID: "fixture-peer", Conn: conn, Handshake: remote}},
		PieceCount:      1,
		PieceLength:     uint32(len(data)),
		LastPieceLength: uint32(len(data)),
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
	transfer, err := NewTransfer(TransferConfig{
		Selection:      selection,
		Output:         plan,
		Stager:         storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: int64(len(data))}),
		LocalHandshake: peer.Handshake{InfoHash: [20]byte{1, 3, 5}, PeerID: [20]byte{4, 5, 6}},
		Peers:          []ConnectedPeer{{ID: "retry-peer", Conn: conn, Handshake: peer.Handshake{InfoHash: [20]byte{1, 3, 5}, PeerID: [20]byte{3, 2, 1}}}},
		PieceCount:     1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
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
