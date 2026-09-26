package session

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
)

func TestTransferDisconnectedPeersDoNotAccumulate(t *testing.T) {
	_, _, selection, output, _ := admissionFixture(t)
	registry, err := peer.NewPeerRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	releases := 0
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: output,
		PieceCount: 1, PieceLength: 5, LastPieceLength: 5,
		AcquirePeer:    func(context.Context) (ConnectedPeer, error) { return ConnectedPeer{}, ErrNoPeer },
		InitialStrikes: map[peer.Endpoint]int{endpoint(201): 2, endpoint(203): 3},
		ReleasePeer: func(input ConnectedPeer) {
			releases++
			if !registry.Release(input.Handshake.PeerID, input.Conn) {
				t.Error("connection or live peer ID released twice")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var peers []*transferPeer
	for i := 0; i < 200; i++ {
		local, remote := net.Pipe()
		// Reuse the same peer ID on both a repeated and an alternate endpoint.
		input := ConnectedPeer{ID: "reconnect", Endpoint: endpoint(byte(201 + i%2)), Conn: local, Handshake: peer.Handshake{PeerID: [20]byte{7}}}
		if _, err := registry.Admit(peer.HandshakeResult{Endpoint: input.Endpoint, Conn: local, Handshake: input.Handshake}); err != nil {
			local.Close()
			remote.Close()
			t.Fatalf("reconnect %d did not regain its live ID: %v", i, err)
		}
		if err := transfer.admitCandidate(ctx, &peers, &input); err != nil {
			t.Fatal(err)
		}
		if len(peers) != 1 || peers[0].done {
			t.Fatalf("reconnect %d retained %d peers, want one live peer", i, len(peers))
		}
		departing := peers[0]
		_ = remote.Close()
		result := waitPeerEventWithAdmission(ctx, peers, nil, nil)
		if !result.OK || result.Index != 0 {
			t.Fatalf("reconnect %d close event = %#v", i, result)
		}
		if result.PeerClosed {
			_ = transfer.disconnectPeer(departing, peer.ErrDisconnected)
		} else if err := transfer.handleRunPeerEvent(ctx, peers, result.Index, result.Event, true); err != nil {
			t.Fatal(err)
		}
		if !departing.done || !departing.removed || !departing.released {
			t.Fatalf("reconnect %d did not finish releasing ownership", i)
		}
		// Wait returns immediately only once both connection-worker loops join.
		_ = departing.worker.Wait()
		if err := transfer.drive(ctx, &peers); err != nil {
			t.Fatal(err)
		}
		if len(peers) != 0 || len(transfer.scheduler.peers) != 0 || registry.Len() != 0 {
			t.Fatalf("reconnect %d retained active peer state after disconnect", i)
		}
		for _, retained := range peers[:cap(peers)] {
			if retained != nil {
				t.Fatal("retired peer is retained in the slice backing array")
			}
		}
		_ = transfer.disconnectPeer(departing, peer.ErrDisconnected)
		if releases != i+1 {
			t.Fatalf("releases = %d after %d connections", releases, i+1)
		}
		if transfer.scheduler.StrikeCount(endpoint(201)) != 2 || !transfer.scheduler.IsBlacklisted(endpoint(203)) {
			t.Fatal("retirement reset run-long endpoint penalties")
		}
	}
}

func TestTransferRetirementReleasesRequestsBeforePeerIDReuse(t *testing.T) {
	transfer, departing, remote, _, _, _, _ := newTransferWithOutstandingTestRequest(t, false)
	defer remote.Close()
	releases := 0
	transfer.releasePeer = func(ConnectedPeer) { releases++ }
	peers := []*transferPeer{departing}
	if transfer.scheduler.ActiveRequests() != 1 {
		t.Fatal("fixture has no outstanding request")
	}
	_ = transfer.disconnectPeer(departing, io.EOF)
	if transfer.scheduler.ActiveRequests() != 0 {
		t.Fatal("disconnected peer still owns a request")
	}
	if err := transfer.drive(context.Background(), &peers); err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("retained peers = %d, want zero", len(peers))
	}
	local, replacementRemote := net.Pipe()
	defer replacementRemote.Close()
	input := ConnectedPeer{ID: departing.input.ID, Endpoint: departing.input.Endpoint, Conn: local}
	if err := transfer.admitCandidate(context.Background(), &peers, &input); err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].done {
		t.Fatal("released peer ID could not reconnect")
	}
	_ = transfer.disconnectPeer(departing, io.EOF)
	if releases != 1 || len(transfer.scheduler.peers) != 1 {
		t.Fatal("old connection cleanup released its replacement")
	}
	_ = transfer.disconnectPeer(peers[0], io.EOF)
	if releases != 2 {
		t.Fatalf("releases = %d, want one per connection", releases)
	}
}

func TestTransferRetirementPreservesSurvivorEventsAndRequests(t *testing.T) {
	c, _, selection, output, _ := admissionFixture(t)
	prepared, err := output.Prepare(storage.Overwrite)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: output,
		PieceCount: 1, PieceLength: 5, LastPieceLength: 5,
		Stager:      storage.NewStager(storage.StagerConfig{CacheRoot: c.config.CacheRoot}),
		AcquirePeer: func(context.Context) (ConnectedPeer, error) { return ConnectedPeer{}, ErrNoPeer },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transfer.stager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer transfer.stager.Cleanup(nil)
	var peers []*transferPeer
	defer func() {
		for _, p := range peers {
			_ = transfer.disconnectPeer(p, io.EOF)
		}
	}()
	departLocal, departRemote := net.Pipe()
	defer departRemote.Close()
	input := ConnectedPeer{ID: "departing", Endpoint: endpoint(210), Conn: departLocal}
	if err := transfer.admitCandidate(ctx, &peers, &input); err != nil {
		t.Fatal(err)
	}
	usefulLocal, usefulRemote := net.Pipe()
	defer usefulRemote.Close()
	input = ConnectedPeer{ID: "useful", Endpoint: endpoint(211), Conn: usefulLocal}
	if err := transfer.admitCandidate(ctx, &peers, &input); err != nil {
		t.Fatal(err)
	}
	survivor := peers[1]
	requestSeen := make(chan struct{})
	keepaliveWritten := make(chan struct{})
	sendPiece := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		defer usefulRemote.Close()
		if writeFixtureFrame(usefulRemote, peer.BitfieldID, []byte{0x80}) != nil || writeFixtureFrame(usefulRemote, peer.UnchokeID, nil) != nil {
			return
		}
		block, err := h2Request(usefulRemote)
		if err != nil {
			return
		}
		close(requestSeen)
		if _, err := usefulRemote.Write([]byte{0, 0, 0, 0}); err != nil {
			return
		}
		close(keepaliveWritten)
		select {
		case <-sendPiece:
		case <-ctx.Done():
			return
		}
		if h2Reply(usefulRemote, []byte("piece"), block) != nil {
			return
		}
		_, _ = io.Copy(io.Discard, usefulRemote)
	}()
	keepalives := 0
	step := func() {
		t.Helper()
		result := waitPeerEventWithAdmission(ctx, peers, nil, nil)
		if !result.OK {
			t.Fatal("peer event did not arrive before the safety deadline")
		}
		if result.Event.Message.KeepAlive {
			keepalives++
		}
		if result.PeerClosed {
			_ = transfer.disconnectPeer(peers[result.Index], io.EOF)
		} else if err := transfer.handleRunPeerEvent(ctx, peers, result.Index, result.Event, true); err != nil {
			t.Fatal(err)
		}
		if err := transfer.drive(ctx, &peers); err != nil && !errors.Is(err, peer.ErrWorkerClosed) {
			t.Fatal(err)
		}
	}
	for transfer.scheduler.ActiveRequests() == 0 {
		step()
	}
	select {
	case <-requestSeen:
	case <-ctx.Done():
		t.Fatal("survivor did not receive its request")
	}
	select {
	case <-keepaliveWritten:
	case <-ctx.Done():
		t.Fatal("keepalive did not cross the wire before removal")
	}
	_ = departRemote.Close()
	for len(peers) != 1 || keepalives == 0 {
		step()
	}
	if peers[0] != survivor || transfer.scheduler.ActiveRequests() != 1 || len(survivor.active) != 1 {
		t.Fatal("removing the first peer lost the survivor or its request")
	}
	close(sendPiece)
	for !transfer.scheduler.IsComplete() {
		step()
	}
	if keepalives != 1 {
		t.Fatalf("keepalives processed = %d, want one", keepalives)
	}
	if data, err := os.ReadFile(filepath.Join(output.Root(), "payload")); err != nil || string(data) != "piece" {
		t.Fatalf("output = %q, %v", data, err)
	}
	_ = transfer.disconnectPeer(survivor, io.EOF)
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("surviving peer fixture did not join")
	}
}
