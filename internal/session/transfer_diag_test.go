package session

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestTransferDriveReportsChokeAndZeroReqQBlocks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reqQ    uint32
		setReq  bool
		unchoke bool
		want    string
	}{
		{name: "choked", reqQ: 1, setReq: true, want: "assignment blocked: remote choking"},
		{name: "zero reqq", reqQ: 0, setReq: true, unchoke: true, want: "assignment blocked: zero reqq"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selection, err := torrent.Select(singleFileMeta([]byte("x")), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = remote.Close() })
			var events []Diagnostic
			transfer, err := NewTransfer(TransferConfig{
				Selection: selection, Output: &storage.Plan{},
				Stager:     storage.NewStager(storage.StagerConfig{CacheRoot: filepath.Join(t.TempDir(), "cache"), MaxPieces: 1, MaxBytes: 1}),
				Peers:      []ConnectedPeer{{ID: tc.name, Endpoint: endpoint(90), Conn: local, ReqQ: tc.reqQ, ReqQSet: tc.setReq}},
				PieceCount: 1, PieceLength: 1, LastPieceLength: 1,
				OnDiagnostic: func(event Diagnostic) { events = append(events, event) },
			})
			if err != nil {
				_ = local.Close()
				t.Fatal(err)
			}
			p, err := transfer.startPeer(context.Background(), transfer.peers[0])
			if err != nil {
				_ = local.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.worker.Close(); _ = transfer.scheduler.RemovePeer(p.input.ID) })
			if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.BitfieldID, Payload: []byte{0x80}}}); err != nil {
				t.Fatal(err)
			}
			if tc.unchoke {
				if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: peer.UnchokeID}}); err != nil {
					t.Fatal(err)
				}
			}
			live := []*transferPeer{p}
			if err := transfer.drive(context.Background(), &live); err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Detail == tc.want {
					return
				}
			}
			t.Fatalf("missing coordinator diagnostic %q in %+v", tc.want, events)
		})
	}
}
