package session

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
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
