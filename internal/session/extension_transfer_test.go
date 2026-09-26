package session

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

type extensionTransferFixture struct {
	transfer *Transfer
	peer     *transferPeer
	remote   net.Conn
}

func startExtensionTransferFixture(t *testing.T, localExtension, remoteExtension, fast bool) extensionTransferFixture {
	t.Helper()
	data := make([]byte, 3*limits.BlockBytes)
	selection, err := torrent.Select(singleFileMeta(data), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	localConn, remoteConn := net.Pipe()
	t.Cleanup(func() {
		_ = localConn.Close()
		_ = remoteConn.Close()
	})
	local := peer.Handshake{}
	remote := peer.Handshake{}
	if localExtension {
		local.Reserved[5] = metadataExtensionReservedBit
	}
	if remoteExtension {
		remote.Reserved[5] = metadataExtensionReservedBit
	}
	if fast {
		local.Reserved[7] = peer.FastExtensionBit
		remote.Reserved[7] = peer.FastExtensionBit
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection,
		Output:    &storage.Plan{},
		Stager: storage.NewStager(storage.StagerConfig{
			CacheRoot: t.TempDir(), MaxPieces: 1, MaxBytes: int64(len(data)),
		}),
		LocalHandshake: local,
		Peers: []ConnectedPeer{{
			ID: "extension-peer", Endpoint: endpoint(97), Conn: localConn, Handshake: remote,
		}},
		PieceCount: 1, PieceLength: uint32(len(data)), LastPieceLength: uint32(len(data)),
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct {
		peer *transferPeer
		err  error
	}, 1)
	go func() {
		p, err := transfer.startPeer(context.Background(), transfer.peers[0])
		started <- struct {
			peer *transferPeer
			err  error
		}{p, err}
	}()
	if err := remoteConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if fast {
		message, err := peer.ReadMessage(remoteConn)
		if err != nil || message.ID != peer.HaveNoneID {
			t.Fatalf("first startup message = %#v, %v; want Have None", message, err)
		}
	}
	if localExtension && remoteExtension {
		message, err := peer.ReadMessage(remoteConn)
		if err != nil || message.ID != peer.ExtendedID || len(message.Payload) == 0 || message.Payload[0] != peer.ExtensionHandshakeID {
			t.Fatalf("extension startup message = %#v, %v", message, err)
		}
		handshake, err := peer.ParseExtensionHandshake(message.Payload[1:])
		if err != nil || handshake.Extensions[peer.UtMetadataExtension] != 1 {
			t.Fatalf("local extension handshake = %#v, %v", handshake, err)
		}
	}
	select {
	case result := <-started:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if err := remoteConn.SetReadDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = result.peer.worker.Close()
			if !result.peer.removed {
				_ = transfer.scheduler.RemovePeer(result.peer.input.ID)
			}
		})
		return extensionTransferFixture{transfer: transfer, peer: result.peer, remote: remoteConn}
	case <-time.After(2 * time.Second):
		t.Fatal("transfer peer did not start")
		return extensionTransferFixture{}
	}
}

func deliverTransferExtension(t *testing.T, fixture extensionTransferFixture, frame []byte) error {
	t.Helper()
	if err := fixture.remote.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	defer fixture.remote.SetWriteDeadline(time.Time{})
	if err := writeTestFrame(fixture.remote, frame); err != nil {
		t.Fatal(err)
	}
	select {
	case event, ok := <-fixture.peer.worker.Events():
		if !ok {
			t.Fatal("extension worker closed before delivery")
		}
		return fixture.transfer.handleEvent(context.Background(), fixture.peer, event)
	case <-time.After(2 * time.Second):
		t.Fatal("extension worker did not deliver frame")
		return nil
	}
}

func expectNoTransferOutput(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("unexpected peer-wire output")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("peer-wire output read = %v, want timeout", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestTransferExtensionHandshakeStartupOrder(t *testing.T) {
	withFast := startExtensionTransferFixture(t, true, true, true)
	if withFast.peer.extensions == nil || !withFast.peer.state.Fast() {
		t.Fatal("negotiated Fast or BEP 10 state is missing")
	}
	expectNoTransferOutput(t, withFast.remote)

	withoutRemoteExtension := startExtensionTransferFixture(t, true, false, true)
	if withoutRemoteExtension.peer.extensions != nil {
		t.Fatal("BEP 10 was enabled without the remote reserved bit")
	}
	expectNoTransferOutput(t, withoutRemoteExtension.remote)

	withoutLocalExtension := startExtensionTransferFixture(t, false, true, false)
	if withoutLocalExtension.peer.extensions != nil {
		t.Fatal("BEP 10 was enabled without the local reserved bit")
	}
	expectNoTransferOutput(t, withoutLocalExtension.remote)
}

func TestTransferExtensionReqQUpdatesLiveLimit(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, false)
	p := fixture.peer
	s := fixture.transfer.scheduler
	if p.state.ReqQ() != limits.PeerRequests || s.peers[p.input.ID].limit != limits.PeerRequests {
		t.Fatal("missing reqq did not use the local request cap")
	}
	if err := s.SetAvailability(p.input.ID, []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece(p.input.ID)
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	outstanding, err := s.NextRequests(p.input.ID, 2)
	if err != nil || len(outstanding) != 2 {
		t.Fatalf("initial requests = %#v, %v", outstanding, err)
	}
	for _, test := range []struct {
		name string
		body string
		want int
	}{
		{"one", "d4:reqqi1ee", 1},
		{"zero", "d4:reqqi0ee", 0},
		{"large", "d4:reqqi9223372036854775807ee", limits.PeerRequests},
		{"omitted", "d1:md11:ut_metadatai11eee", limits.PeerRequests},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte(test.body))); err != nil {
				t.Fatal(err)
			}
			if got := p.state.ReqQ(); got != test.want {
				t.Fatalf("peer reqq = %d, want %d", got, test.want)
			}
			if got := s.peers[p.input.ID].limit; got != test.want {
				t.Fatalf("scheduler limit = %d, want %d", got, test.want)
			}
			if test.want <= 1 {
				requests, err := s.NextRequests(p.input.ID, p.state.ReqQ())
				if err != nil || len(requests) != 0 || s.peers[p.input.ID].active != 2 {
					t.Fatalf("lowered cap changed outstanding work: requests=%#v active=%d err=%v", requests, s.peers[p.input.ID].active, err)
				}
			}
		})
	}
	remaining, err := s.NextRequests(p.input.ID, p.state.ReqQ())
	if err != nil || len(remaining) != 1 {
		t.Fatalf("raised cap did not permit remaining block: %#v, %v", remaining, err)
	}
	if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte("d4:reqqi0ee"))); err != nil {
		t.Fatal(err)
	}
	for _, request := range append(outstanding, remaining...) {
		if err := s.RejectBlock(p.input.ID, request.Block); err != nil {
			t.Fatal(err)
		}
	}
	if requests, err := s.NextRequests(p.input.ID, 3); err != nil || len(requests) != 0 {
		t.Fatalf("zero reqq scheduled after outstanding work settled: %#v, %v", requests, err)
	}
	if id, ok := p.extensions.RemoteExtensionID(peer.UtMetadataExtension); !ok || id != 11 {
		t.Fatalf("repeated handshake remote ID = %d, %t", id, ok)
	}
}

func TestTransferExtensionRejectsMetadataRequestsWithoutUpload(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, false)
	if err := deliverTransferExtension(t, fixture, metadataControlFrame(1, peer.MetadataRequest, 1)); err != nil {
		t.Fatal(err)
	}
	expectNoTransferOutput(t, fixture.remote)
	if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte("d1:md11:ut_metadatai9eee"))); err != nil {
		t.Fatal(err)
	}
	if err := deliverTransferExtension(t, fixture, metadataControlFrame(9, peer.MetadataRequest, 2)); err != nil {
		t.Fatal(err)
	}
	expectNoTransferOutput(t, fixture.remote)
	for _, test := range []struct {
		remoteID byte
		piece    uint32
	}{
		{9, 3},
		{11, 4},
	} {
		if test.remoteID == 11 {
			if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte("d1:md11:ut_metadatai11eee"))); err != nil {
				t.Fatal(err)
			}
		}
		if err := deliverTransferExtension(t, fixture, metadataControlFrame(1, peer.MetadataRequest, test.piece)); err != nil {
			t.Fatal(err)
		}
		if err := fixture.remote.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		message, err := peer.ReadMessageWithOptions(fixture.remote, peer.ReadOptions{MetadataExtensionID: test.remoteID})
		if err != nil || message.ID != peer.ExtendedID || len(message.Payload) < 2 || message.Payload[0] != test.remoteID {
			t.Fatalf("metadata response = %#v, %v", message, err)
		}
		control, err := peer.ParseMetadataControl(message.Payload[1:])
		if err != nil || control.Type != peer.MetadataReject || control.Piece != test.piece {
			t.Fatalf("metadata control = %#v, %v", control, err)
		}
		expectNoTransferOutput(t, fixture.remote)
	}
}

func TestTransferMalformedExtensionHandshakeBlacklistsEndpoint(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, false)
	err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte("d4:reqqi-1ee")))
	if !errors.Is(err, peer.ErrProtocolViolation) || !fixture.peer.done || !fixture.transfer.scheduler.IsBlacklisted(fixture.peer.input.Endpoint) {
		t.Fatalf("malformed extension = %v, done=%t, blacklisted=%t", err, fixture.peer.done, fixture.transfer.scheduler.IsBlacklisted(fixture.peer.input.Endpoint))
	}
}

func TestTransferDrainsQueuedReqQBeforeScheduling(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, false)
	if err := fixture.transfer.stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = fixture.peer.worker.Close()
		_ = fixture.transfer.stager.Cleanup(nil)
	})
	if err := writeFixtureFrame(fixture.remote, peer.UnchokeID, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureFrame(fixture.remote, peer.HaveID, []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := writeTestFrame(fixture.remote, extensionFrame(0, []byte("d4:reqqi1ee"))); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(fixture.peer.worker.Events()) < 3 {
		select {
		case <-tick.C:
		case <-timer.C:
			t.Fatal("peer did not queue Unchoke, Have, and reqq before scheduling")
		}
	}
	peers := []*transferPeer{fixture.peer}
	if err := fixture.transfer.drainQueuedPeerEvents(context.Background(), peers, false); err != nil {
		t.Fatal(err)
	}
	if fixture.peer.state.ReqQ() != 1 {
		t.Fatalf("queued reqq = %d, want 1", fixture.peer.state.ReqQ())
	}
	if err := fixture.transfer.drive(context.Background(), &peers); err != nil {
		t.Fatal(err)
	}
	if err := fixture.remote.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for index, wantID := range []byte{peer.InterestedID, peer.RequestID} {
		message, err := peer.ReadMessage(fixture.remote)
		if err != nil || message.ID != wantID {
			t.Fatalf("outbound frame %d = %#v, %v; want ID %d", index, message, err, wantID)
		}
	}
	expectNoTransferOutput(t, fixture.remote)
}

func TestTransferBlockedMetadataRejectDisconnectsWithoutStrike(t *testing.T) {
	fixture := startExtensionTransferFixture(t, true, true, false)
	if err := deliverTransferExtension(t, fixture, extensionFrame(0, []byte("d1:md11:ut_metadatai9eee"))); err != nil {
		t.Fatal(err)
	}
	request := peer.PeerEvent{Message: peer.Message{
		ID: peer.ExtendedID, Payload: append([]byte{1}, []byte("d8:msg_typei0e5:piecei0ee")...),
	}}
	started := time.Now()
	var err error
	for i := 0; i < limits.PeerCommands+3; i++ {
		err = fixture.transfer.handleEvent(context.Background(), fixture.peer, request)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, context.DeadlineExceeded) || !fixture.peer.done {
		t.Fatalf("blocked reject = %v, peer done=%t", err, fixture.peer.done)
	}
	if fixture.transfer.scheduler.IsBlacklisted(fixture.peer.input.Endpoint) {
		t.Fatal("blocked reader received a corruption strike")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("blocked command took %s to disconnect", elapsed)
	}
}
