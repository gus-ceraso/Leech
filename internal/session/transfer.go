package session

// This file owns the first, deliberately small transfer coordinator.  The
// coordinator receives connections after their BEP 3 handshakes have already
// completed.  Discovery, transport racing, and metadata acquisition belong to
// later session phases; this phase only drives peer-wire I/O, staging, and the
// serialized finalizer.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	ErrTransferConfig = errors.New("invalid transfer configuration")
	ErrTransferPeer   = errors.New("invalid transfer peer")
)

// ConnectedPeer is a transport that has already completed the BEP 3
// handshake.  Transfer never writes or reads a second handshake.  The
// handshake is retained so the coordinator can negotiate Fast and configure
// bounded peer-wire validation.
type ConnectedPeer struct {
	ID        string
	Endpoint  peer.Endpoint
	Conn      net.Conn
	Handshake peer.Handshake
	ReqQ      uint32
}

// TransferConfig wires the bounded pure-state scheduler to local output and a
// fixed set of already-handshaken TCP peers.  A zero PrepareMode means
// storage.Overwrite; Resume is accepted for later resume integration.
type TransferConfig struct {
	Selection       *torrent.SelectionPlan
	Output          *storage.Plan
	Stager          *storage.Stager
	SchedulerConfig Config
	LocalHandshake  peer.Handshake
	Peers           []ConnectedPeer
	PrepareMode     storage.PrepareMode
	// Full piece geometry is required to validate bitfields and block
	// boundaries when selection omits files or pieces. Zero values derive a
	// best-effort geometry from selected pieces for small fixtures.
	PieceCount      uint32
	PieceLength     uint32
	LastPieceLength uint32
}

// Transfer owns one transfer phase.  It starts no goroutines until Run.
type Transfer struct {
	selection       *torrent.SelectionPlan
	output          *storage.Plan
	stager          *storage.Stager
	scheduler       *Scheduler
	local           peer.Handshake
	peers           []ConnectedPeer
	mode            storage.PrepareMode
	stages          map[int]*storage.PieceStage
	pieceCount      uint32
	pieceLength     uint32
	lastPieceLength uint32
}

type transferPeer struct {
	input              ConnectedPeer
	worker             *peer.ConnectionWorker
	state              *peer.PeerState
	active             map[peer.Block]struct{}
	done               bool
	removed            bool
	availabilitySynced bool
}

// NewTransfer validates immutable transfer inputs and creates the scheduler.
// It performs no filesystem or network operation.
func NewTransfer(config TransferConfig) (*Transfer, error) {
	if config.Selection == nil {
		return nil, fmt.Errorf("%w: nil selection plan", ErrTransferConfig)
	}
	if config.Output == nil {
		return nil, fmt.Errorf("%w: nil output plan", ErrTransferConfig)
	}
	if len(config.Peers) == 0 {
		return nil, fmt.Errorf("%w: no connected peers", ErrTransferConfig)
	}
	if config.PrepareMode != storage.Overwrite && config.PrepareMode != storage.Resume {
		return nil, fmt.Errorf("%w: invalid prepare mode %d", ErrTransferConfig, config.PrepareMode)
	}
	if config.PieceCount == 0 || config.PieceLength == 0 || config.LastPieceLength == 0 {
		return nil, fmt.Errorf("%w: full torrent piece geometry is required", ErrTransferConfig)
	}
	if config.LastPieceLength > config.PieceLength {
		return nil, fmt.Errorf("%w: invalid full torrent piece geometry", ErrTransferConfig)
	}
	for _, mapping := range config.Selection.PiecePlans() {
		if mapping.Piece.Index < 0 || uint32(mapping.Piece.Index) >= config.PieceCount {
			return nil, fmt.Errorf("%w: selected piece %d is outside full torrent", ErrTransferConfig, mapping.Piece.Index)
		}
		length := mapping.Piece.Range.End - mapping.Piece.Range.Begin
		if length <= 0 || uint64(length) > uint64(config.PieceLength) {
			return nil, fmt.Errorf("%w: selected piece %d has invalid length", ErrTransferConfig, mapping.Piece.Index)
		}
		if uint32(mapping.Piece.Index+1) == config.PieceCount && uint32(length) != config.LastPieceLength {
			return nil, fmt.Errorf("%w: selected final piece length does not match full torrent", ErrTransferConfig)
		}
	}
	scheduler, err := NewScheduler(config.Selection, config.SchedulerConfig)
	if err != nil {
		return nil, err
	}
	stager := config.Stager
	if stager == nil {
		stager = storage.NewStager()
	}
	peers := make([]ConnectedPeer, len(config.Peers))
	copy(peers, config.Peers)
	for i := range peers {
		if peers[i].Conn == nil {
			return nil, fmt.Errorf("%w: peer %d has nil connection", ErrTransferPeer, i)
		}
		if peers[i].ID == "" {
			// Peer IDs are opaque bytes.  A string preserves every byte and is
			// suitable as a scheduler key without trusting it for endpoint
			// identity.
			peers[i].ID = string(peers[i].Handshake.PeerID[:])
		}
		if peers[i].ID == "" {
			return nil, fmt.Errorf("%w: peer %d has empty ID", ErrTransferPeer, i)
		}
		if peers[i].Endpoint.Port == 0 || !peers[i].Endpoint.Addr.IsValid() || peers[i].Endpoint.Addr.IsUnspecified() || peers[i].Endpoint.Addr.IsMulticast() {
			endpoint, ok := endpointFromConn(peers[i].Conn)
			if !ok {
				return nil, fmt.Errorf("%w: peer %d has invalid endpoint", ErrTransferPeer, i)
			}
			peers[i].Endpoint = endpoint
		}
	}
	return &Transfer{
		selection:       config.Selection,
		output:          config.Output,
		stager:          stager,
		scheduler:       scheduler,
		local:           config.LocalHandshake,
		peers:           peers,
		mode:            config.PrepareMode,
		pieceCount:      config.PieceCount,
		pieceLength:     config.PieceLength,
		lastPieceLength: config.LastPieceLength,
	}, nil
}

// Run downloads all wanted pieces and returns after every peer worker and the
// current staged workspace have been joined and cleaned.  A context
// cancellation is the primary result unless cleanup reports an additional
// error.
func (t *Transfer) Run(ctx context.Context) error {
	if t == nil {
		return ErrTransferConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Prepare is intentionally one short operation.  Finalize opens only the
	// selected files touched by each verified piece, while Prepare creates all
	// selected zero-length files and applies overwrite/resume semantics once.
	prepared, err := t.output.Prepare(t.mode)
	if err != nil {
		return err
	}
	if err := prepared.Close(); err != nil {
		return err
	}
	if err := t.stager.Start(ctx); err != nil {
		return t.stager.Cleanup(err)
	}

	peers := make([]*transferPeer, len(t.peers))
	for i := range t.peers {
		p, err := t.startPeer(ctx, t.peers[i])
		if err != nil {
			for _, started := range peers {
				if started != nil {
					_ = started.worker.Close()
				}
			}
			return t.stager.Cleanup(err)
		}
		peers[i] = p
	}

	var primary error
	for primary == nil && !t.scheduler.IsComplete() {
		if err := ctx.Err(); err != nil {
			primary = err
			break
		}
		if err := t.drive(ctx, peers); err != nil {
			primary = err
			break
		}
		if t.scheduler.IsComplete() {
			break
		}
		if countLive(peers) == 0 {
			primary = peer.ErrDisconnected
			break
		}
		event, index, ok := waitPeerEvent(ctx, peers)
		if !ok {
			if err := ctx.Err(); err != nil {
				primary = err
			} else {
				primary = peer.ErrDisconnected
			}
			break
		}
		if err := t.handleEvent(ctx, peers[index], event); err != nil {
			// A single endpoint failure is recoverable when another connected
			// peer can finish the work. Protocol failures blacklist only that
			// endpoint and follow the same reassignment path.
			if peers[index].done && countLive(peers) > 0 && !errors.Is(err, storage.ErrStagingFatal) {
				continue
			}
			primary = err
		}
	}

	// Closing workers is the cancellation/unblock mechanism for their raw
	// reads. Every worker joins before the private staging workspace is gone.
	for _, p := range peers {
		if p != nil {
			_ = p.worker.Close()
			if !p.removed {
				_ = t.scheduler.RemovePeer(p.input.ID)
				p.removed = true
			}
		}
	}
	return t.stager.Cleanup(primary)
}

func (t *Transfer) startPeer(ctx context.Context, input ConnectedPeer) (*transferPeer, error) {
	fast := peer.FastNegotiated(t.local.Reserved, input.Handshake.Reserved)
	state, err := peer.NewPeerStateWithConfig(peer.PeerStateConfig{
		PieceCount:      t.pieceCount,
		PieceLength:     t.pieceLength,
		LastPieceLength: t.lastPieceLength,
		Fast:            fast,
		ReqQ:            input.ReqQ,
		ReqQSet:         input.ReqQ != 0,
	})
	if err != nil {
		return nil, err
	}
	for _, index := range t.selection.WantedPieces() {
		if _, _, err := state.SetWanted(uint32(index), true); err != nil {
			return nil, err
		}
	}
	if err := t.scheduler.AddPeerWithLimit(input.ID, input.Endpoint, state.ReqQ()); err != nil {
		return nil, err
	}
	if fast {
		// The handshake has already completed, so this is the first and only
		// peer-wire frame sent before worker ownership begins.
		if err := peer.WriteInitialAvailability(input.Conn, t.local, input.Handshake); err != nil {
			_ = t.scheduler.RemovePeer(input.ID)
			return nil, err
		}
	}
	worker := peer.NewConnectionWorker(input.Conn, peer.ReadOptions{
		Fast:            fast,
		PieceCount:      t.pieceCount,
		ValidateIndices: true,
		PieceLength:     t.pieceLength,
		LastPieceLength: t.lastPieceLength,
	})
	worker.Start(ctx)
	return &transferPeer{input: input, worker: worker, state: state, active: make(map[peer.Block]struct{})}, nil
}

func (t *Transfer) drive(ctx context.Context, peers []*transferPeer) error {
	for _, p := range peers {
		if p == nil || p.done {
			continue
		}
		if t.scheduler.IsBlacklisted(p.input.Endpoint) {
			_ = t.disconnectPeer(p, fmt.Errorf("peer endpoint blacklisted"))
			continue
		}
		if p.state.Choked() {
			requestable := false
			for _, index := range t.selection.WantedPieces() {
				if p.state.CanRequest(uint32(index)) {
					requestable = true
					break
				}
			}
			if !requestable {
				continue
			}
		}
		// Admission is separate from assignment.  A stage must exist before
		// any request can be sent, and a failed admission is fatal storage
		// failure rather than a peer-local retry.
		for {
			offer, ok, err := t.scheduler.ReservePiece(p.input.ID)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			mapping, exists := t.selection.Piece(offer.PieceIndex)
			if !exists {
				_ = t.scheduler.RejectPiece(offer)
				return fmt.Errorf("%w: scheduler reserved unknown piece %d", ErrTransferConfig, offer.PieceIndex)
			}
			stage, err := t.stager.AdmitPiece(mapping.Piece)
			if err != nil {
				_ = t.scheduler.RejectPiece(offer)
				return err
			}
			if err := t.scheduler.AdmitPiece(offer); err != nil {
				_ = stage.Abort()
				return err
			}
			if !setStage(t, offer.PieceIndex, stage) {
				_ = stage.Abort()
				return fmt.Errorf("%w: duplicate staged piece %d", ErrTransferConfig, offer.PieceIndex)
			}
		}
		requests, err := t.scheduler.NextRequests(p.input.ID, p.state.ReqQ())
		if err != nil {
			return err
		}
		for _, request := range requests {
			if !p.state.CanRequest(request.Block.Index) {
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				continue
			}
			if err := p.state.AddRequest(request.Block); err != nil {
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				continue
			}
			p.active[request.Block] = struct{}{}
			message := peer.Message{ID: peer.RequestID, Payload: blockPayload(request.Block)}
			if err := p.worker.SendContext(ctx, message); err != nil {
				_ = p.state.CancelRequest(request.Block)
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				delete(p.active, request.Block)
				return err
			}
		}
	}
	return nil
}

func (t *Transfer) handleEvent(ctx context.Context, p *transferPeer, event peer.PeerEvent) error {
	if p.done {
		return nil
	}
	if event.Err != nil {
		return t.disconnectPeer(p, event.Err)
	}
	if event.Message.KeepAlive {
		return nil
	}
	effect, err := p.state.ApplyMessage(event.Message)
	if err != nil {
		if peer.IsProtocolViolation(err) {
			t.scheduler.SevereViolation(p.input.Endpoint)
			_ = p.worker.Close()
			return t.disconnectPeer(p, err)
		}
		return err
	}
	if event.Message.ID == peer.ChokeID && !p.state.Fast() {
		// BEP 3 choke releases ordinary outstanding requests in PeerState,
		// which leaves their scheduler assignments to be explicitly returned
		// to the pending set. Fast requests remain outstanding and therefore
		// stay assigned.
		for block := range p.active {
			if p.state.Requests().Outstanding(block) {
				continue
			}
			if err := t.scheduler.RejectBlock(p.input.ID, block); err != nil && !errors.Is(err, ErrBlockNotOutstanding) {
				return err
			}
			delete(p.active, block)
		}
	}
	if effect.Response != nil {
		if err := p.worker.SendContext(ctx, *effect.Response); err != nil {
			return t.disconnectPeer(p, err)
		}
	}
	switch event.Message.ID {
	case peer.BitfieldID, peer.HaveID, peer.HaveAllID, peer.HaveNoneID, peer.AllowedFastID, peer.ChokeID, peer.UnchokeID:
		if err := t.syncPeerAvailability(p); err != nil {
			return err
		}
	}
	if effect.InterestChanged {
		id := peer.NotInterestedID
		if effect.Interested {
			id = peer.InterestedID
		}
		if err := p.worker.SendContext(ctx, peer.Message{ID: id}); err != nil {
			return t.disconnectPeer(p, err)
		}
	}
	if effect.HasTerminal {
		block, ok := messageBlock(event.Message)
		if !ok {
			return fmt.Errorf("%w: terminal without block", ErrTransferConfig)
		}
		switch effect.Terminal {
		case peer.TerminalPiece:
			delete(p.active, block)
			data := event.Message.Payload[8:]
			stage := stageFor(t, int(block.Index))
			if stage == nil {
				return fmt.Errorf("%w: piece %d has no stage", ErrTransferConfig, block.Index)
			}
			if err := stage.WriteBlockContext(ctx, int64(block.Begin), data); err != nil {
				return err
			}
			result, err := t.scheduler.AcceptBlock(p.input.ID, block)
			if err != nil {
				return err
			}
			if result.Complete {
				return t.finalizePiece(ctx, int(block.Index))
			}
		case peer.TerminalReject:
			delete(p.active, block)
			if err := t.scheduler.RejectBlock(p.input.ID, block); err != nil {
				return err
			}
		case peer.TerminalLatePiece, peer.TerminalLateReject:
			// The request table consumed the bounded tombstone.  The payload
			// cannot be attributed to a current scheduler assignment.
		}
	}
	return nil
}

// syncPeerAvailability stores only pieces that the current peer state allows
// the scheduler to request. Ordinary availability is always required; while
// choked, the independent Allowed Fast set is required as well. Allowed Fast
// therefore never turns an unadvertised piece into an eligible piece, and a
// rare ordinary piece cannot starve an allowed one.
func (t *Transfer) syncPeerAvailability(p *transferPeer) error {
	indices := make([]int, 0)
	for _, index := range t.selection.WantedPieces() {
		if p.state.Availability(uint32(index)) && (!p.state.Choked() || p.state.AllowedFast(uint32(index))) {
			indices = append(indices, index)
		}
	}
	return t.scheduler.SetAvailability(p.input.ID, indices)
}

func (t *Transfer) finalizePiece(ctx context.Context, index int) error {
	snapshot, err := t.scheduler.Snapshot(index)
	if err != nil {
		return err
	}
	coverage := make([]storage.BlockCoverage, len(snapshot.Blocks))
	for i, block := range snapshot.Blocks {
		coverage[i] = storage.BlockCoverage{
			Begin:        int64(block.Block.Begin),
			Length:       int64(block.Block.Length),
			Contributors: []storage.Endpoint{{Addr: block.Endpoint.Addr, Port: block.Endpoint.Port}},
		}
	}
	_, err = t.stager.Finalize(ctx, storage.NewPieceSnapshot(snapshot.Piece, coverage), snapshot.Plan, t.output)
	if err != nil {
		if errors.Is(err, storage.ErrPieceHashMismatch) {
			delete(t.stages, index)
			_, verifyErr := t.scheduler.VerifyPiece(index, false)
			return verifyErr
		}
		return err
	}
	delete(t.stages, index)
	_, err = t.scheduler.VerifyPiece(index, true)
	return err
}

func (t *Transfer) disconnectPeer(p *transferPeer, cause error) error {
	if p.done {
		return nil
	}
	p.done = true
	_ = p.worker.Close()
	if !p.removed {
		_ = t.scheduler.RemovePeer(p.input.ID)
		p.removed = true
	}
	if cause == nil {
		cause = peer.ErrDisconnected
	}
	return cause
}

func setStage(t *Transfer, index int, stage *storage.PieceStage) bool {
	// The map is initialized lazily and accessed only by Run's coordinator
	// goroutine.
	if t.stages == nil {
		t.stages = make(map[int]*storage.PieceStage)
	}
	if _, exists := t.stages[index]; exists {
		return false
	}
	t.stages[index] = stage
	return true
}

func stageFor(t *Transfer, index int) *storage.PieceStage {
	if t == nil {
		return nil
	}
	return t.stages[index]
}

func endpointFromConn(conn net.Conn) (peer.Endpoint, bool) {
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok || addr == nil {
		return peer.Endpoint{}, false
	}
	ip, ok := netip.AddrFromSlice(addr.IP)
	if !ok || !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() || addr.Port <= 0 || addr.Port > 65535 {
		return peer.Endpoint{}, false
	}
	return peer.Endpoint{Addr: ip, Port: uint16(addr.Port)}, true
}

func countLive(peers []*transferPeer) int {
	n := 0
	for _, p := range peers {
		if p != nil && !p.done {
			n++
		}
	}
	return n
}

func waitPeerEvent(ctx context.Context, peers []*transferPeer) (peer.PeerEvent, int, bool) {
	cases := make([]reflect.SelectCase, 1, len(peers)+1)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	indices := make([]int, 0, len(peers))
	for index, p := range peers {
		if p == nil || p.done {
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.worker.Events())})
		indices = append(indices, index)
	}
	if len(indices) == 0 {
		return peer.PeerEvent{}, 0, false
	}
	chosen, value, ok := reflect.Select(cases)
	if chosen == 0 || !ok {
		return peer.PeerEvent{}, 0, false
	}
	return value.Interface().(peer.PeerEvent), indices[chosen-1], true
}

func messageBlock(message peer.Message) (peer.Block, bool) {
	if message.ID != peer.PieceID && message.ID != peer.RejectRequestID {
		return peer.Block{}, false
	}
	if message.ID == peer.PieceID {
		if len(message.Payload) < 9 {
			return peer.Block{}, false
		}
		return peer.Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: uint32(len(message.Payload) - 8)}, true
	}
	if len(message.Payload) != 12 {
		return peer.Block{}, false
	}
	return peer.Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:12])}, true
}

func blockPayload(block peer.Block) []byte {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	binary.BigEndian.PutUint32(payload[8:], block.Length)
	return payload
}
