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
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	ErrTransferConfig = errors.New("invalid transfer configuration")
	ErrTransferPeer   = errors.New("invalid transfer peer")
	// ErrNoPeer tells the bounded admission loop that discovery currently has
	// no handshaken candidate. It is retried with backoff.
	ErrNoPeer = errors.New("no connected peer is available")
)

// PeerAcquire returns one already-handshaken candidate. The callback must
// honor ctx so Transfer can join it during cancellation and shutdown. A
// temporary lack of candidates should return ErrNoPeer.
type PeerAcquire func(context.Context) (ConnectedPeer, error)

// PieceVerified is emitted after a staged piece has passed SHA-1 verification
// and its selected ranges have been committed to final output.
type PieceVerified struct {
	PieceIndex    int
	PieceBytes    int64
	SelectedBytes int64
}

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
	// ReqQSet preserves an explicitly advertised zero reqq. When false, a
	// missing reqq uses the local bounded default.
	ReqQSet bool
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
	// AcquirePeer is an optional bounded admission seam for tracker/dial
	// ownership. It runs independently so a slow discovery call never blocks
	// existing peer events. ReleasePeer returns a disconnected candidate's
	// dial slot to its owner.
	AcquirePeer PeerAcquire
	ReleasePeer func(ConnectedPeer)
	// InitialStrikes carries endpoint penalties from metadata discovery.
	InitialStrikes map[peer.Endpoint]int
	// OnEndpointBlacklisted propagates a new transfer-phase blacklist to the
	// candidate dialer. It is called by the transfer coordinator goroutine.
	OnEndpointBlacklisted func(peer.Endpoint)
	// ResumeComplete contains selected piece indices verified by the resume
	// phase. They are marked complete before any network worker starts.
	ResumeComplete []int
	// OnPieceVerified must return promptly; it is called after output commit.
	OnPieceVerified func(PieceVerified)
	// OnPayloadReceived is called synchronously for every well-framed Piece
	// body, before its terminal is accepted by PeerState or the scheduler. The
	// count is the file payload length, excluding the piece index and offset.
	OnPayloadReceived func(int64) error
	// BeforePeerShutdown runs once after scheduling stops and before workers
	// and the staging workspace are cleaned up. A prior Run error wins.
	BeforePeerShutdown func() error
	// Now is a deterministic clock seam for request timeout and peer replacement tests.
	Now func() time.Time
}

// Transfer owns one transfer phase.  It starts no goroutines until Run.
type Transfer struct {
	selection              *torrent.SelectionPlan
	output                 *storage.Plan
	stager                 *storage.Stager
	scheduler              *Scheduler
	local                  peer.Handshake
	peers                  []ConnectedPeer
	mode                   storage.PrepareMode
	stages                 map[int]*storage.PieceStage
	pieceCount             uint32
	pieceLength            uint32
	lastPieceLength        uint32
	acquirePeer            PeerAcquire
	releasePeer            func(ConnectedPeer)
	onPieceVerified        func(PieceVerified)
	onPayloadReceived      func(int64) error
	onEndpointBlacklisted  func(peer.Endpoint)
	beforePeerShutdown     func() error
	shutdownCallbackCalled bool
	now                    func() time.Time
	initialReleased        bool
}

type transferPeer struct {
	input      ConnectedPeer
	worker     *peer.ConnectionWorker
	state      *peer.PeerState
	extensions *peer.ExtensionState
	active     map[peer.Block]time.Time
	tombstoned map[peer.Block]struct{}
	done       bool
	removed    bool
	lastUseful time.Time
	released   bool
}

const (
	peerRequestTimeout = 30 * time.Second
	peerCancelTimeout  = 100 * time.Millisecond
	peerIdleLimit      = 60 * time.Second
	peerAcquireBase    = 100 * time.Millisecond
	peerAcquireMax     = 5 * time.Second
)

// NewTransfer validates immutable transfer inputs and creates the scheduler.
// It performs no filesystem or network operation.
func NewTransfer(config TransferConfig) (*Transfer, error) {
	if config.Selection == nil {
		return nil, fmt.Errorf("%w: nil selection plan", ErrTransferConfig)
	}
	if config.Output == nil {
		return nil, fmt.Errorf("%w: nil output plan", ErrTransferConfig)
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
	if err := scheduler.SeedStrikes(config.InitialStrikes); err != nil {
		return nil, err
	}
	seenResume := make(map[int]struct{}, len(config.ResumeComplete))
	for _, index := range config.ResumeComplete {
		if _, duplicate := seenResume[index]; duplicate {
			return nil, fmt.Errorf("%w: duplicate resume piece %d", ErrTransferConfig, index)
		}
		seenResume[index] = struct{}{}
		if err := scheduler.MarkComplete(index); err != nil {
			return nil, fmt.Errorf("%w: resume piece %d: %v", ErrTransferConfig, index, err)
		}
	}
	if len(config.Peers) == 0 && !scheduler.IsComplete() && config.AcquirePeer == nil {
		return nil, fmt.Errorf("%w: no connected peers", ErrTransferConfig)
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
		selection:             config.Selection,
		output:                config.Output,
		stager:                stager,
		scheduler:             scheduler,
		local:                 config.LocalHandshake,
		peers:                 peers,
		mode:                  config.PrepareMode,
		pieceCount:            config.PieceCount,
		pieceLength:           config.PieceLength,
		lastPieceLength:       config.LastPieceLength,
		acquirePeer:           config.AcquirePeer,
		releasePeer:           config.ReleasePeer,
		onPieceVerified:       config.OnPieceVerified,
		onPayloadReceived:     config.OnPayloadReceived,
		onEndpointBlacklisted: config.OnEndpointBlacklisted,
		beforePeerShutdown:    config.BeforePeerShutdown,
		now:                   config.Now,
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
		t.releaseInitialPeers()
		return err
	}
	// Resume may have verified every selected piece. In that case no staging,
	// peer worker, or discovery operation is needed.
	if t.scheduler.IsComplete() {
		t.releaseInitialPeers()
		return nil
	}

	// Prepare is intentionally one short operation.  Finalize opens only the
	// selected files touched by each verified piece, while Prepare creates all
	// selected zero-length files and applies overwrite/resume semantics once.
	prepared, err := t.output.Prepare(t.mode)
	if err != nil {
		t.releaseInitialPeers()
		return err
	}
	if err := prepared.Close(); err != nil {
		t.releaseInitialPeers()
		return err
	}
	if err := t.stager.Start(ctx); err != nil {
		t.releaseInitialPeers()
		return t.stager.Cleanup(err)
	}

	peers := make([]*transferPeer, len(t.peers))
	for i := range t.peers {
		p, err := t.startPeer(ctx, t.peers[i])
		if err != nil {
			startupErr := t.callBeforePeerShutdown(err)
			_ = t.peers[i].Conn.Close()
			t.releaseInput(t.peers[i])
			for _, pending := range t.peers[i+1:] {
				_ = pending.Conn.Close()
				t.releaseInput(pending)
			}
			for _, started := range peers {
				if started != nil {
					_ = started.worker.Close()
					t.release(started)
				}
			}
			return t.stager.Cleanup(startupErr)
		}
		peers[i] = p
	}

	runCtx, stopAcquire := context.WithCancel(ctx)
	var acquireWG sync.WaitGroup
	var acquired <-chan acquireResult
	if t.acquirePeer != nil {
		results := make(chan acquireResult, 1)
		acquired = results
		acquireWG.Add(1)
		go t.acquireLoop(runCtx, results, &acquireWG)
	}
	defer func() {
		stopAcquire()
		acquireWG.Wait()
		t.drainAcquired(acquired)
	}()
	replacementTicker := time.NewTicker(time.Second)
	defer replacementTicker.Stop()

	var primary error
	for primary == nil && !t.scheduler.IsComplete() {
		if err := ctx.Err(); err != nil {
			primary = err
			break
		}
		if err := t.drive(ctx, &peers); err != nil {
			if errors.Is(err, peer.ErrWorkerClosed) && (countLive(peers) > 0 || acquired != nil) {
				continue
			}
			primary = err
			break
		}
		if t.scheduler.IsComplete() {
			break
		}
		if countLive(peers) == 0 && acquired == nil {
			primary = peer.ErrDisconnected
			break
		}
		result := waitPeerEventWithAdmission(ctx, peers, acquired, replacementTicker.C)
		if result.Candidate != nil {
			if err := t.admitCandidate(ctx, &peers, result.Candidate); err != nil {
				primary = err
			}
			continue
		}
		if result.AdmissionErr != nil {
			primary = result.AdmissionErr
			break
		}
		if result.CandidateClosed {
			acquired = nil
			if countLive(peers) == 0 {
				primary = peer.ErrDisconnected
				break
			}
			continue
		}
		if result.PeerClosed {
			p := peers[result.Index]
			cause := t.workerCause(p, peer.ErrDisconnected)
			if err := t.disconnectPeer(p, cause); err != nil && countLive(peers) == 0 && acquired == nil {
				primary = err
			}
			continue
		}
		if result.ReplacementTick {
			if err := t.rotateUnproductive(ctx, &peers); err != nil {
				primary = err
			}
			continue
		}
		if !result.OK {
			if err := ctx.Err(); err != nil {
				primary = err
			} else {
				primary = peer.ErrDisconnected
			}
			break
		}
		if err := t.handleEventWithPeers(ctx, peers, peers[result.Index], result.Event); err != nil {
			// A single endpoint failure is recoverable when another connected
			// peer can finish the work. Protocol failures blacklist only that
			// endpoint and follow the same reassignment path.
			if peers[result.Index].done && (countLive(peers) > 0 || acquired != nil) && !errors.Is(err, storage.ErrStagingFatal) {
				continue
			}
			primary = err
		}
	}

	// Stop admission before the shutdown callback so no candidate can race the
	// callback with a newly acquired connection. The defer remains as a guard
	// for every earlier return path.
	stopAcquire()
	acquireWG.Wait()
	t.drainAcquired(acquired)
	primary = t.callBeforePeerShutdown(primary)

	// Closing workers is the cancellation/unblock mechanism for their raw
	// reads. Every worker joins before the private staging workspace is gone.
	for _, p := range peers {
		if p != nil {
			_ = p.worker.Close()
			t.release(p)
			if !p.removed {
				_ = t.scheduler.RemovePeer(p.input.ID)
				p.removed = true
			}
		}
	}
	return t.stager.Cleanup(primary)
}

func (t *Transfer) callBeforePeerShutdown(primary error) error {
	if t == nil || t.shutdownCallbackCalled {
		return primary
	}
	t.shutdownCallbackCalled = true
	if t.beforePeerShutdown != nil {
		if err := t.beforePeerShutdown(); err != nil && primary == nil {
			return err
		}
	}
	return primary
}

// Progress exposes the coordinator's verified selected-byte counters. The
// scheduler is single-owner, so callers must read this after Run or from the
// serialized OnPieceVerified callback rather than concurrently with Run.
func (t *Transfer) Progress() Progress {
	if t == nil {
		return Progress{}
	}
	return t.scheduler.Progress()
}

func (t *Transfer) startPeer(ctx context.Context, input ConnectedPeer) (*transferPeer, error) {
	fast := peer.FastNegotiated(t.local.Reserved, input.Handshake.Reserved)
	state, err := peer.NewPeerStateWithConfig(peer.PeerStateConfig{
		PieceCount:      t.pieceCount,
		PieceLength:     t.pieceLength,
		LastPieceLength: t.lastPieceLength,
		Fast:            fast,
		ReqQ:            input.ReqQ,
		ReqQSet:         input.ReqQSet,
	})
	if err != nil {
		return nil, err
	}
	if err := state.SetWantedPieces(t.selection.WantedPieces()); err != nil {
		return nil, err
	}
	if err := t.scheduler.AddPeerWithLimit(input.ID, input.Endpoint, state.ReqQ()); err != nil {
		return nil, err
	}
	if fast {
		// Fast requires Have None immediately after the BEP 3 handshake.
		if err := peer.WriteInitialAvailability(input.Conn, t.local, input.Handshake); err != nil {
			_ = t.scheduler.RemovePeer(input.ID)
			_ = input.Conn.Close()
			return nil, err
		}
	}
	var extensions *peer.ExtensionState
	var metadataID byte
	if t.local.Reserved[5]&metadataExtensionReservedBit != 0 && input.Handshake.Reserved[5]&metadataExtensionReservedBit != 0 {
		extensions = peer.NewExtensionState()
		metadataID, _ = extensions.LocalExtensionID(peer.UtMetadataExtension)
		if err := extensions.WriteHandshake(input.Conn); err != nil {
			_ = t.scheduler.RemovePeer(input.ID)
			_ = input.Conn.Close()
			return nil, err
		}
	}
	worker := peer.NewConnectionWorker(input.Conn, peer.ReadOptions{
		Fast:                fast,
		PieceCount:          t.pieceCount,
		ValidateIndices:     true,
		PieceLength:         t.pieceLength,
		LastPieceLength:     t.lastPieceLength,
		MetadataExtensionID: metadataID,
	})
	worker.Start(ctx)
	now := t.clock()
	return &transferPeer{
		input: input, worker: worker, state: state, extensions: extensions,
		active: make(map[peer.Block]time.Time), tombstoned: make(map[peer.Block]struct{}),
		lastUseful: now,
	}, nil
}

func (t *Transfer) drive(ctx context.Context, peers *[]*transferPeer) error {
	if err := t.rotateUnproductive(ctx, peers); err != nil {
		return err
	}
	for _, p := range *peers {
		if p == nil || p.done {
			continue
		}
		if t.scheduler.IsBlacklisted(p.input.Endpoint) {
			_ = t.disconnectPeer(p, fmt.Errorf("peer endpoint blacklisted"))
			continue
		}
		if p.state.RequestableCount() == 0 {
			continue
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
		requests, err := t.scheduler.nextRequestsExcluding(p.input.ID, p.state.ReqQ(), p.tombstoned)
		if err != nil {
			return err
		}
		for _, request := range requests {
			if !p.state.CanRequest(request.Block.Index) {
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				continue
			}
			// A worker can terminate between the event wait and this drive
			// pass. Return the peer-local closure before queueing another
			// command so its scheduler assignments are released together.
			if p.worker.Err() != nil {
				return t.disconnectWorker(p)
			}
			if err := p.state.AddRequest(request.Block); err != nil {
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				continue
			}
			p.active[request.Block] = t.clock()
			message := peer.Message{ID: peer.RequestID, Payload: blockPayload(request.Block)}
			if err := p.worker.SendContext(ctx, message); err != nil {
				if p.worker.Err() != nil || errors.Is(err, peer.ErrWorkerClosed) {
					return t.disconnectWorker(p)
				}
				_ = p.state.CancelRequest(request.Block)
				_ = t.scheduler.RejectBlock(p.input.ID, request.Block)
				delete(p.active, request.Block)
				return err
			}
		}
	}
	return nil
}

type acquireResult struct {
	peer ConnectedPeer
	err  error
}

func (t *Transfer) acquireLoop(ctx context.Context, results chan<- acquireResult, wg *sync.WaitGroup) {
	defer wg.Done()
	delay := peerAcquireBase
	for {
		input, err := t.acquirePeer(ctx)
		if err == nil {
			select {
			case results <- acquireResult{peer: input}:
			case <-ctx.Done():
				if input.Conn != nil {
					_ = input.Conn.Close()
				}
				t.releaseInput(input)
				return
			}
			delay = peerAcquireBase
			continue
		}
		var budgetErr *peer.EndpointBudgetError
		if errors.As(err, &budgetErr) {
			select {
			case results <- acquireResult{err: budgetErr}:
			case <-ctx.Done():
			}
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if delay < peerAcquireMax {
			delay *= 2
			if delay > peerAcquireMax {
				delay = peerAcquireMax
			}
		}
	}
}

func (t *Transfer) admitCandidate(ctx context.Context, peers *[]*transferPeer, input *ConnectedPeer) error {
	if input == nil {
		return nil
	}
	if input.Conn == nil {
		t.releaseInput(*input)
		return nil
	}
	if !validPeerEndpoint(input.Endpoint) {
		endpoint, ok := endpointFromConn(input.Conn)
		if !ok {
			_ = input.Conn.Close()
			t.releaseInput(*input)
			return nil
		}
		input.Endpoint = endpoint
	}
	stale := t.findUnproductive(*peers)
	if countLive(*peers) >= limits.ActivePeers {
		if stale == nil {
			_ = input.Conn.Close()
			t.releaseInput(*input)
			return nil
		}
		_ = t.disconnectPeer(stale, fmt.Errorf("peer replaced after inactivity"))
	}
	p, err := t.startPeer(ctx, *input)
	if err != nil {
		_ = input.Conn.Close()
		t.releaseInput(*input)
		return nil
	}
	*peers = append(*peers, p)
	if stale != nil && stale != p && !stale.done {
		_ = t.disconnectPeer(stale, fmt.Errorf("peer replaced after inactivity"))
	}
	return nil
}

func (t *Transfer) rotateUnproductive(ctx context.Context, peers *[]*transferPeer) error {
	if err := t.expireRequests(ctx, *peers); err != nil {
		return err
	}
	if stale := t.findUnproductive(*peers); stale != nil {
		// Retiring the stale connection first frees the dial slot. The next
		// AcquirePeer result can then be admitted without exceeding the active
		// peer bound.
		_ = t.disconnectPeer(stale, fmt.Errorf("peer replaced after inactivity"))
	}
	return nil
}

func (t *Transfer) findUnproductive(peers []*transferPeer) *transferPeer {
	now := t.clock()
	for _, p := range peers {
		if p == nil || p.done {
			continue
		}
		// A useful Allowed Fast piece remains requestable during this window,
		// but availability does not extend it without useful data.
		if now.Sub(p.lastUseful) >= peerIdleLimit {
			return p
		}
	}
	return nil
}

// expireRequests returns stalled assignments to the scheduler while retaining
// the peer's exact terminal obligation in a bounded tombstone. A timed-out
// tuple is excluded from this peer until its late Piece or Reject is consumed.
func (t *Transfer) expireRequests(ctx context.Context, peers []*transferPeer) error {
	now := t.clock()
	for _, p := range peers {
		if p == nil || p.done {
			continue
		}
		var cancelCtx context.Context
		var cancel context.CancelFunc
		for block, sentAt := range p.active {
			if now.Sub(sentAt) < peerRequestTimeout {
				continue
			}
			if err := p.state.TimeoutRequest(block); err != nil {
				// A full tombstone set cannot forget a terminal obligation. Closing
				// this connection releases its scheduler assignments without a strike.
				_ = t.disconnectPeer(p, err)
				break
			}
			p.tombstoned[block] = struct{}{}
			if cancelCtx == nil {
				cancelCtx, cancel = context.WithTimeout(ctx, peerCancelTimeout)
			}
			if err := p.worker.SendContext(cancelCtx, peer.Message{ID: peer.CancelID, Payload: blockPayload(block)}); err != nil {
				_ = t.disconnectPeer(p, err)
				break
			}
			if err := t.scheduler.RejectBlock(p.input.ID, block); err != nil && !errors.Is(err, ErrBlockNotOutstanding) {
				if cancel != nil {
					cancel()
				}
				return err
			}
			delete(p.active, block)
		}
		if cancel != nil {
			cancel()
		}
	}
	return nil
}

func (t *Transfer) clock() time.Time {
	if t != nil && t.now != nil {
		return t.now()
	}
	return time.Now()
}

func (t *Transfer) releaseInput(input ConnectedPeer) {
	if t != nil && t.releasePeer != nil {
		t.releasePeer(input)
	}
}

func (t *Transfer) releaseInitialPeers() {
	if t == nil || t.initialReleased {
		return
	}
	t.initialReleased = true
	for _, input := range t.peers {
		if input.Conn != nil {
			_ = input.Conn.Close()
		}
		t.releaseInput(input)
	}
}

func (t *Transfer) release(p *transferPeer) {
	if p == nil || p.released {
		return
	}
	p.released = true
	t.releaseInput(p.input)
}

func (t *Transfer) drainAcquired(candidates <-chan acquireResult) {
	if candidates == nil {
		return
	}
	for {
		select {
		case result, ok := <-candidates:
			if !ok {
				return
			}
			if result.err != nil {
				continue
			}
			if result.peer.Conn != nil {
				_ = result.peer.Conn.Close()
			}
			t.releaseInput(result.peer)
		default:
			return
		}
	}
}

func (t *Transfer) handleEvent(ctx context.Context, p *transferPeer, event peer.PeerEvent) error {
	return t.handleEventWithPeers(ctx, nil, p, event)
}

func (t *Transfer) handleEventWithPeers(ctx context.Context, peers []*transferPeer, p *transferPeer, event peer.PeerEvent) error {
	if p.done {
		return nil
	}
	if event.Err != nil {
		if peer.IsProtocolViolation(event.Err) {
			t.blacklistEndpoint(p.input.Endpoint)
		}
		return t.disconnectPeer(p, event.Err)
	}
	if event.Message.KeepAlive {
		return nil
	}
	if event.Message.ID == peer.ExtendedID {
		return t.handleExtensionMessage(ctx, p, event.Message)
	}
	// ConnectionWorker has already checked the Piece frame and its payload
	// bounds. Count file bytes before PeerState consumes the terminal so
	// corrupt, duplicate, and tombstoned late payloads are included even when
	// the scheduler later rejects the block.
	if event.Message.ID == peer.PieceID && len(event.Message.Payload) >= 9 && t.onPayloadReceived != nil {
		if err := t.onPayloadReceived(int64(len(event.Message.Payload) - 8)); err != nil {
			return err
		}
	}
	effect, err := p.state.ApplyMessage(event.Message)
	if err != nil {
		if errors.Is(err, peer.ErrTombstoneLimit) {
			// A bounded tombstone set cannot forget an older terminal
			// obligation. Close this peer without treating the condition as
			// corruption; the scheduler reassigns its live work.
			return t.disconnectPeer(p, err)
		}
		if peer.IsProtocolViolation(err) {
			t.blacklistEndpoint(p.input.Endpoint)
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
			if p.state.Requests().Tombstoned(block) {
				p.tombstoned[block] = struct{}{}
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
	if err := t.applyAvailabilityChanges(p.input.ID, effect); err != nil {
		return err
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
			delete(p.tombstoned, block)
			data := event.Message.Payload[8:]
			result, err := t.scheduler.AcceptBlock(p.input.ID, block)
			if err != nil {
				if errors.Is(err, ErrBlockNotOutstanding) {
					// A duplicate endgame response arrived before the winner's
					// cancel was observed by this peer. PeerState has already
					// consumed its exact request terminal, so discard the payload.
					return nil
				}
				return err
			}
			stage := stageFor(t, int(block.Index))
			if stage == nil {
				return fmt.Errorf("%w: piece %d has no stage", ErrTransferConfig, block.Index)
			}
			if err := stage.WriteBlockContext(ctx, int64(block.Begin), data); err != nil {
				return err
			}
			p.lastUseful = t.clock()
			if err := t.cancelRedundant(ctx, peers, result.Canceled); err != nil {
				return err
			}
			if result.Complete {
				return t.finalizePiece(ctx, int(block.Index))
			}
		case peer.TerminalReject:
			delete(p.active, block)
			delete(p.tombstoned, block)
			if err := t.scheduler.RejectBlock(p.input.ID, block); err != nil {
				if errors.Is(err, ErrBlockNotOutstanding) {
					// A redundant endgame request may have been settled by
					// another peer before this reject arrived.
					return nil
				}
				return err
			}
		case peer.TerminalLatePiece, peer.TerminalLateReject:
			// The request table consumed the bounded tombstone.  The payload
			// cannot be attributed to a current scheduler assignment.
			delete(p.tombstoned, block)
		}
	}
	return nil
}

func (t *Transfer) handleExtensionMessage(ctx context.Context, p *transferPeer, message peer.Message) error {
	if p.extensions == nil {
		return nil
	}
	event, err := p.extensions.ApplyMessage(message)
	if err != nil {
		if peer.IsProtocolViolation(err) {
			t.blacklistEndpoint(p.input.Endpoint)
		}
		return t.disconnectPeer(p, err)
	}
	if event.ID == peer.ExtensionHandshakeID {
		if reqQ, present := p.extensions.RemoteReqQ(); present {
			limit := p.state.SetReqQ(reqQ)
			if err := t.scheduler.SetPeerLimit(p.input.ID, limit); err != nil {
				return err
			}
		}
	}
	if event.Metadata != nil && event.Metadata.Type == peer.MetadataRequest && len(event.Response) != 0 {
		if err := p.worker.SendMetadataRejectContext(ctx, p.extensions, event.Metadata.Piece); err != nil {
			return t.disconnectPeer(p, err)
		}
	}
	return nil
}

// applyAvailabilityChanges keeps scheduler state aligned with the peer's
// requestable wanted pieces by applying only the bits changed by this message.
func (t *Transfer) applyAvailabilityChanges(peerID string, effect peer.StateEffect) error {
	for _, index := range effect.AvailabilityRemoved {
		if err := t.scheduler.SetPieceAvailability(peerID, int(index), false); err != nil {
			return err
		}
	}
	for _, index := range effect.AvailabilityAdded {
		if err := t.scheduler.SetPieceAvailability(peerID, int(index), true); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transfer) cancelRedundant(ctx context.Context, peers []*transferPeer, canceled []Request) error {
	for _, request := range canceled {
		var loser *transferPeer
		for _, candidate := range peers {
			if candidate != nil && !candidate.done && candidate.input.ID == request.Peer {
				loser = candidate
				break
			}
		}
		if loser == nil {
			continue
		}
		if err := loser.state.CancelRequest(request.Block); err != nil {
			if errors.Is(err, peer.ErrTombstoneLimit) {
				// Forgetting a terminal obligation is safe only after the
				// connection is closed, and this is not a corruption strike.
				_ = t.disconnectPeer(loser, fmt.Errorf("endgame tombstone cap: %w", err))
				continue
			}
			if !errors.Is(err, peer.ErrRequestMissing) {
				return err
			}
			continue
		}
		delete(loser.active, request.Block)
		loser.tombstoned[request.Block] = struct{}{}
		if err := loser.worker.SendContext(ctx, peer.Message{ID: peer.CancelID, Payload: blockPayload(request.Block)}); err != nil {
			_ = t.disconnectPeer(loser, err)
		}
	}
	return nil
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
	finalized, err := t.stager.Finalize(ctx, storage.NewPieceSnapshot(snapshot.Piece, coverage), snapshot.Plan, t.output)
	if err != nil {
		if errors.Is(err, storage.ErrPieceHashMismatch) {
			delete(t.stages, index)
			verification, verifyErr := t.scheduler.VerifyPiece(index, false)
			for _, endpoint := range verification.Blacklisted {
				t.notifyBlacklisted(endpoint)
			}
			return verifyErr
		}
	}
	return t.settleFinalizedPiece(index, snapshot.Piece, finalized, err)
}

func (t *Transfer) settleFinalizedPiece(index int, piece torrent.Piece, finalized storage.FinalizeResult, finalizeErr error) error {
	if finalizeErr != nil && !finalized.OutputCommitted {
		return finalizeErr
	}
	delete(t.stages, index)
	result, verifyErr := t.scheduler.VerifyPiece(index, true)
	if verifyErr == nil && result.SelectedBytes > 0 && t.onPieceVerified != nil {
		t.onPieceVerified(PieceVerified{PieceIndex: index, PieceBytes: piece.Range.End - piece.Range.Begin, SelectedBytes: result.SelectedBytes})
	}
	return errors.Join(finalizeErr, verifyErr)
}

func (t *Transfer) disconnectPeer(p *transferPeer, cause error) error {
	if p.done {
		return nil
	}
	p.done = true
	_ = p.worker.Close()
	t.release(p)
	if !p.removed {
		_ = t.scheduler.RemovePeer(p.input.ID)
		p.removed = true
	}
	if cause == nil {
		cause = peer.ErrDisconnected
	}
	return cause
}

func (t *Transfer) workerCause(p *transferPeer, fallback error) error {
	cause := fallback
	if p != nil && p.worker != nil && p.worker.Err() != nil {
		cause = p.worker.Err()
	}
	if peer.IsProtocolViolation(cause) {
		t.blacklistEndpoint(p.input.Endpoint)
	}
	return cause
}

func (t *Transfer) blacklistEndpoint(endpoint peer.Endpoint) {
	if t.scheduler.SevereViolation(endpoint) {
		t.notifyBlacklisted(endpoint)
	}
}

func (t *Transfer) notifyBlacklisted(endpoint peer.Endpoint) {
	if t.onEndpointBlacklisted != nil {
		t.onEndpointBlacklisted(endpoint)
	}
}

func (t *Transfer) disconnectWorker(p *transferPeer) error {
	cause := t.workerCause(p, peer.ErrWorkerClosed)
	_ = t.disconnectPeer(p, cause)
	// Keep the peer-local sentinel for Run's recovery decision while retaining
	// the worker's terminal cause for errors.Is and diagnostics.
	return fmt.Errorf("%w: %w", peer.ErrWorkerClosed, cause)
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

func validPeerEndpoint(endpoint peer.Endpoint) bool {
	return endpoint.Port != 0 && endpoint.Addr.IsValid() && !endpoint.Addr.IsUnspecified() && !endpoint.Addr.IsMulticast()
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

type peerWaitResult struct {
	Event           peer.PeerEvent
	Index           int
	Candidate       *ConnectedPeer
	AdmissionErr    error
	CandidateClosed bool
	PeerClosed      bool
	ReplacementTick bool
	OK              bool
}

func waitPeerEvent(ctx context.Context, peers []*transferPeer) (peer.PeerEvent, int, bool) {
	result := waitPeerEventWithAdmission(ctx, peers, nil, nil)
	return result.Event, result.Index, result.OK
}

// waitPeerEventWithAdmission keeps candidate admission in the same
// coordinator select as peer events. A slow AcquirePeer call is isolated in
// its own bounded worker, while useful existing connections remain serviced.
func waitPeerEventWithAdmission(ctx context.Context, peers []*transferPeer, candidates <-chan acquireResult, replacement <-chan time.Time) peerWaitResult {
	cases := make([]reflect.SelectCase, 1, len(peers)+3)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	candidateCase := -1
	if candidates != nil {
		candidateCase = len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(candidates)})
	}
	replacementCase := -1
	if replacement != nil {
		replacementCase = len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(replacement)})
	}
	indices := make([]int, 0, len(peers))
	for index, p := range peers {
		if p == nil || p.done {
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(p.worker.Events())})
		indices = append(indices, index)
	}
	if len(indices) == 0 && candidateCase < 0 && replacementCase < 0 {
		return peerWaitResult{}
	}
	chosen, value, ok := reflect.Select(cases)
	if chosen == 0 {
		return peerWaitResult{}
	}
	if chosen == candidateCase {
		if !ok {
			return peerWaitResult{CandidateClosed: true}
		}
		result := value.Interface().(acquireResult)
		if result.err != nil {
			return peerWaitResult{AdmissionErr: result.err, OK: true}
		}
		return peerWaitResult{Candidate: &result.peer, OK: true}
	}
	if chosen == replacementCase {
		if !ok {
			return peerWaitResult{}
		}
		return peerWaitResult{ReplacementTick: true, OK: true}
	}
	if !ok {
		workerOffset := 1
		if candidateCase >= 0 {
			workerOffset = 2
		}
		if replacementCase >= 0 {
			workerOffset++
		}
		return peerWaitResult{Index: indices[chosen-workerOffset], PeerClosed: true, OK: true}
	}
	workerOffset := 1
	if candidateCase >= 0 {
		workerOffset = 2
	}
	if replacementCase >= 0 {
		workerOffset++
	}
	return peerWaitResult{Event: value.Interface().(peer.PeerEvent), Index: indices[chosen-workerOffset], OK: true}
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
