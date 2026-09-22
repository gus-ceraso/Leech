// Package session contains the state owned by one transfer session.
package session

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"sort"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	ErrSchedulerConfig      = errors.New("invalid scheduler configuration")
	ErrUnknownPeer          = errors.New("unknown scheduler peer")
	ErrDuplicatePeer        = errors.New("scheduler peer already exists")
	ErrUnknownPiece         = errors.New("unknown scheduler piece")
	ErrPieceNotReady        = errors.New("piece is not ready for that operation")
	ErrUnknownReservation   = errors.New("unknown piece reservation")
	ErrUnknownBlock         = errors.New("unknown scheduler block")
	ErrBlockNotOutstanding  = errors.New("block is not outstanding for peer")
	ErrInvalidSchedulerPeer = errors.New("invalid scheduler peer")
	ErrInvalidSchedulerAddr = errors.New("invalid scheduler endpoint")
)

// ShuffleFunc randomizes a copy of a tie set. The scheduler never exposes the
// copy it gives to this function, so a test implementation may use a seeded
// PRNG without changing scheduler state. Production construction uses a
// cryptographically random Fisher-Yates implementation.
type ShuffleFunc func([]int) error

// Config bounds the coordinator's mutable state. Zero values select the
// supported local limits. MaxQueue is the number of outstanding assignments
// allowed by the session event/command path; the effective global request cap
// is the smaller of MaxGlobal and MaxQueue.
type Config struct {
	MaxPerPeer      int
	MaxGlobal       int
	MaxQueue        int
	MaxStagedPieces int
	MaxStagedBytes  int64
	// Streaming gives lower piece indices priority while retaining the
	// availability rule: a peer may use a later available piece when no
	// earlier piece is available to it.
	Streaming bool
	Shuffle   ShuffleFunc
}

// Request is one wire request selected by the coordinator. Offset is relative
// to the piece and never crosses a padding span or piece boundary. Endpoint
// identifies the resolved source for corruption attribution.
type Request struct {
	Peer     string
	Endpoint peer.Endpoint
	Block    peer.Block
}

// PieceOffer reserves staging admission for one whole wanted piece. A caller
// must pass this exact offer to AdmitPiece or RejectPiece. The reservation
// keeps staged-piece and staged-byte budgets from being overcommitted while a
// storage worker is creating its cache file.
type PieceOffer struct {
	PieceIndex    int
	Reservation   uint64
	PieceLength   uint32
	SelectedBytes int64
}

// BlockResult describes an accepted block terminal. Complete is true when the
// piece now has every non-padding block and can be passed to the finalizer for
// hash verification.
type BlockResult struct {
	PieceIndex int
	Complete   bool
	// Canceled contains duplicate assignments which lost the endgame race.
	// The transfer coordinator must preserve each request's peer-side
	// terminal obligation before sending a wire cancel.
	Canceled []Request
}

// VerificationResult is the coordinator's result after the finalizer checks a
// staged piece. Contributors contains each distinct resolved endpoint that
// supplied an accepted block. Invalid pieces are reset for a fresh admission.
// SelectedBytes is counted only once, after a valid verification/commit.
type VerificationResult struct {
	PieceIndex    int
	Verified      bool
	SelectedBytes int64
	Contributors  []peer.Endpoint
	Strikes       []peer.Endpoint
	Blacklisted   []peer.Endpoint
	Completed     bool
}

// BlockCoverage is a completed block and its resolved source endpoint. It has
// no payload bytes; S2 reads the block from its own staged file.
type BlockCoverage struct {
	Block    peer.Block
	Endpoint peer.Endpoint
}

// PieceSnapshot is an immutable copy of a fully received staged piece. The
// Plan supplies S2 with selected, non-selected, and padding intersections.
type PieceSnapshot struct {
	Piece  torrent.Piece
	Plan   torrent.PiecePlan
	Blocks []BlockCoverage
}

// Progress reports verified selected output bytes. The value is monotonic for
// a scheduler's lifetime, including when a later piece is found corrupt.
type Progress struct {
	Verified int64
	Selected int64
}

type blockState struct {
	block    peer.Block
	active   bool
	peerID   string
	endpoint peer.Endpoint
	// assignments is non-empty exactly when active. The legacy peerID and
	// endpoint fields retain the first assignment for cheap single-owner
	// access; assignments is authoritative once endgame duplicates exist.
	assignments map[string]peer.Endpoint
	done        bool
}

type pieceStage uint8

const (
	stageNone pieceStage = iota
	stageReserved
	stageAdmitted
)

type pieceState struct {
	plan         torrent.PiecePlan
	blocks       []blockState
	rarity       int
	stage        pieceStage
	reservation  uint64
	contributors map[peer.Endpoint]struct{}
	complete     bool
}

type schedulerPeer struct {
	endpoint  peer.Endpoint
	limit     int
	available []uint64
	active    int
}

// Scheduler is a single-owner, pure state coordinator. Callers must serialize
// all methods; it starts no goroutines and performs no network or filesystem
// I/O. Its state is deliberately bounded by Config and by limits.
type Scheduler struct {
	cfg       Config
	shuffle   ShuffleFunc
	pieces    map[int]*pieceState
	order     []int
	peers     map[string]*schedulerPeer
	strikes   map[peer.Endpoint]int
	blacklist map[peer.Endpoint]struct{}

	reservation       uint64
	active            int
	staged            int
	reserved          int
	stagedBytes       int64
	reservedBytes     int64
	verified          int64
	selected          int64
	pieceCount        int
	availabilityWords int
}

// NewScheduler validates and copies a selection plan into a fresh coordinator.
// Only wanted pieces are retained. Every non-padding range is represented by
// bounded block descriptors; payload bytes never enter this state object.
func NewScheduler(plan *torrent.SelectionPlan, config Config) (*Scheduler, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: nil selection plan", ErrSchedulerConfig)
	}
	cfg, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	s := &Scheduler{
		cfg:       cfg,
		shuffle:   cfg.Shuffle,
		pieces:    make(map[int]*pieceState),
		peers:     make(map[string]*schedulerPeer),
		strikes:   make(map[peer.Endpoint]int),
		blacklist: make(map[peer.Endpoint]struct{}),
	}
	for _, pp := range plan.PiecePlans() {
		if pp.Piece.Index < 0 || pp.Piece.Index >= limits.Pieces {
			return nil, fmt.Errorf("%w: piece index %d exceeds supported bounds", ErrSchedulerConfig, pp.Piece.Index)
		}
		if _, exists := s.pieces[pp.Piece.Index]; exists {
			return nil, fmt.Errorf("%w: duplicate piece %d", ErrSchedulerConfig, pp.Piece.Index)
		}
		blocks, selected, err := makeBlocks(pp)
		if err != nil {
			return nil, err
		}
		if len(blocks) == 0 {
			return nil, fmt.Errorf("%w: piece %d has no non-padding data", ErrSchedulerConfig, pp.Piece.Index)
		}
		p := &pieceState{
			plan:         pp,
			blocks:       blocks,
			contributors: make(map[peer.Endpoint]struct{}),
		}
		// Keep this check close to the derived value. SelectionPlan has already
		// validated it, but scheduler state is a trust boundary as well.
		if selected < 0 {
			return nil, fmt.Errorf("%w: negative selected bytes in piece %d", ErrSchedulerConfig, pp.Piece.Index)
		}
		s.selected += selected
		s.pieces[pp.Piece.Index] = p
		s.order = append(s.order, pp.Piece.Index)
		if pp.Piece.Index+1 > s.pieceCount {
			s.pieceCount = pp.Piece.Index + 1
		}
	}
	if len(s.pieces) == 0 {
		return nil, fmt.Errorf("%w: selection has no wanted pieces", ErrSchedulerConfig)
	}
	s.availabilityWords = (s.pieceCount + 63) / 64
	return s, nil
}

func normalizeConfig(config Config) (Config, error) {
	if config.MaxPerPeer == 0 {
		config.MaxPerPeer = limits.PeerRequests
	}
	if config.MaxGlobal == 0 {
		config.MaxGlobal = limits.GlobalRequests
	}
	if config.MaxQueue == 0 {
		config.MaxQueue = limits.SessionEvents
	}
	if config.MaxStagedPieces == 0 {
		config.MaxStagedPieces = limits.StagedPieces
	}
	if config.MaxStagedBytes == 0 {
		config.MaxStagedBytes = limits.StagedBytes
	}
	if config.MaxPerPeer < 1 || config.MaxPerPeer > limits.PeerRequests ||
		config.MaxGlobal < 1 || config.MaxGlobal > limits.GlobalRequests ||
		config.MaxQueue < 1 || config.MaxQueue > limits.SessionEvents ||
		config.MaxStagedPieces < 1 || config.MaxStagedPieces > limits.StagedPieces ||
		config.MaxStagedBytes < 1 || config.MaxStagedBytes > limits.StagedBytes {
		return Config{}, fmt.Errorf("%w: cap outside supported bounds", ErrSchedulerConfig)
	}
	if config.Shuffle == nil {
		config.Shuffle = cryptoShuffle
	}
	return config, nil
}

func cryptoShuffle(values []int) error {
	for i := len(values) - 1; i > 0; i-- {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return fmt.Errorf("randomize scheduler tie: %w", err)
		}
		j := int(n.Int64())
		values[i], values[j] = values[j], values[i]
	}
	return nil
}

func makeBlocks(pp torrent.PiecePlan) ([]blockState, int64, error) {
	begin, end := pp.Piece.Range.Begin, pp.Piece.Range.End
	if pp.Piece.Index < 0 || begin < 0 || end <= begin || end-begin > limits.PieceBytes {
		return nil, 0, fmt.Errorf("%w: invalid range for piece %d", ErrSchedulerConfig, pp.Piece.Index)
	}
	blocks := make([]blockState, 0)
	var selected int64
	var lastEnd int64 = begin
	for _, span := range pp.Data {
		if span.Range.Begin < begin || span.Range.End <= span.Range.Begin || span.Range.End > end || span.Range.Begin < lastEnd {
			return nil, 0, fmt.Errorf("%w: invalid data span in piece %d", ErrSchedulerConfig, pp.Piece.Index)
		}
		lastEnd = span.Range.End
	}
	for _, span := range pp.Selected {
		if span.Range.Begin < begin || span.Range.End <= span.Range.Begin || span.Range.End > end {
			return nil, 0, fmt.Errorf("%w: invalid selected span in piece %d", ErrSchedulerConfig, pp.Piece.Index)
		}
		var ok bool
		selected, ok = checkedAdd(selected, span.Range.End-span.Range.Begin)
		if !ok {
			return nil, 0, fmt.Errorf("%w: selected byte count overflows", ErrSchedulerConfig)
		}
	}
	for _, span := range pp.Data {
		for cursor := span.Range.Begin; cursor < span.Range.End; {
			remaining := span.Range.End - cursor
			n := remaining
			if n > limits.BlockBytes {
				n = limits.BlockBytes
			}
			offset := cursor - begin
			if offset < 0 || n <= 0 {
				return nil, 0, fmt.Errorf("%w: block arithmetic overflows", ErrSchedulerConfig)
			}
			blocks = append(blocks, blockState{block: peer.Block{Index: uint32(pp.Piece.Index), Begin: uint32(offset), Length: uint32(n)}})
			cursor += n
		}
	}
	return blocks, selected, nil
}

func checkedAdd(a, b int64) (int64, bool) {
	return limits.Add(a, b)
}

func selectedBytes(plan torrent.PiecePlan) int64 {
	var total int64
	for _, span := range plan.Selected {
		length := span.Range.End - span.Range.Begin
		if length <= 0 {
			return 0
		}
		var ok bool
		total, ok = checkedAdd(total, length)
		if !ok {
			return 0
		}
	}
	return total
}

// AddPeer adds a live connection with the default per-peer request cap.
func (s *Scheduler) AddPeer(id string, endpoint peer.Endpoint) error {
	return s.AddPeerWithLimit(id, endpoint, s.cfg.MaxPerPeer)
}

// SeedStrikes imports endpoint corruption penalties accumulated during an
// earlier phase, such as metadata acquisition. It must be called before the
// endpoint is admitted; counts at three are treated as an existing blacklist.
func (s *Scheduler) SeedStrikes(seed map[peer.Endpoint]int) error {
	if s == nil {
		return ErrSchedulerConfig
	}
	for endpoint, count := range seed {
		if !validEndpoint(endpoint) || count < 0 || count > 3 {
			return fmt.Errorf("%w: invalid seeded endpoint strike", ErrSchedulerConfig)
		}
	}
	for endpoint, count := range seed {
		s.strikes[endpoint] = count
		if count >= 3 {
			s.blacklist[endpoint] = struct{}{}
		}
	}
	return nil
}

// AddPeerWithLimit adds a live connection and applies the remote reqq hint
// after clamping it to the scheduler's local per-peer cap.
func (s *Scheduler) AddPeerWithLimit(id string, endpoint peer.Endpoint, limit int) error {
	if s == nil {
		return ErrSchedulerConfig
	}
	if id == "" {
		return fmt.Errorf("%w: empty peer ID", ErrInvalidSchedulerPeer)
	}
	if !validEndpoint(endpoint) {
		return ErrInvalidSchedulerAddr
	}
	if _, blocked := s.blacklist[endpoint]; blocked {
		return fmt.Errorf("%w: endpoint is blacklisted", ErrInvalidSchedulerPeer)
	}
	if _, exists := s.peers[id]; exists {
		return ErrDuplicatePeer
	}
	if limit < 0 {
		return fmt.Errorf("%w: request cap must not be negative", ErrSchedulerConfig)
	}
	if limit > s.cfg.MaxPerPeer {
		limit = s.cfg.MaxPerPeer
	}
	s.peers[id] = &schedulerPeer{endpoint: endpoint, limit: limit, available: make([]uint64, s.availabilityWords)}
	return nil
}

// RemovePeer releases requests owned by id. Admitted staged pieces remain in
// place so another connection can finish them without re-admitting storage.
func (s *Scheduler) RemovePeer(id string) error {
	p, ok := s.peers[id]
	if !ok {
		return ErrUnknownPeer
	}
	for _, piece := range s.pieces {
		for i := range piece.blocks {
			b := &piece.blocks[i]
			if !b.active || !hasAssignment(b, id) {
				continue
			}
			delete(b.assignments, id)
			s.active--
			p.active--
			refreshAssignments(b)
		}
	}
	for word, value := range p.available {
		for value != 0 {
			bit := bits.TrailingZeros64(value)
			index := word*64 + bit
			if piece := s.pieces[index]; piece != nil && piece.rarity > 0 {
				piece.rarity--
			}
			value &^= uint64(1) << bit
		}
	}
	delete(s.peers, id)
	return nil
}

// SetAvailability replaces the peer's ordinary advertised availability. Only
// wanted-piece indices are retained; duplicate indices are harmless.
func (s *Scheduler) SetAvailability(id string, indices []int) error {
	p, ok := s.peers[id]
	if !ok {
		return ErrUnknownPeer
	}
	for word := range p.available {
		value := p.available[word]
		for value != 0 {
			bit := bits.TrailingZeros64(value)
			index := word*64 + bit
			if piece := s.pieces[index]; piece != nil && piece.rarity > 0 {
				piece.rarity--
			}
			value &^= uint64(1) << bit
		}
		p.available[word] = 0
	}
	for _, index := range indices {
		piece, exists := s.pieces[index]
		if !exists {
			continue
		}
		if !hasBit(p.available, index) {
			setBit(p.available, index)
			piece.rarity++
		}
	}
	return nil
}

// SetPieceAvailability changes one ordinary availability bit.
func (s *Scheduler) SetPieceAvailability(id string, index int, available bool) error {
	p, ok := s.peers[id]
	if !ok {
		return ErrUnknownPeer
	}
	if _, exists := s.pieces[index]; !exists {
		return nil
	}
	has := hasBit(p.available, index)
	if available && !has {
		setBit(p.available, index)
		s.pieces[index].rarity++
	} else if !available && has {
		clearBit(p.available, index)
		if s.pieces[index].rarity > 0 {
			s.pieces[index].rarity--
		}
	}
	return nil
}

// ReservePiece chooses a rarest available wanted piece and reserves its whole
// stage budget. Call AdmitPiece after S2 has created the cache file.
func (s *Scheduler) ReservePiece(peerID string) (PieceOffer, bool, error) {
	p, ok := s.peers[peerID]
	if !ok {
		return PieceOffer{}, false, ErrUnknownPeer
	}
	if s.staged+s.reserved >= s.cfg.MaxStagedPieces {
		return PieceOffer{}, false, nil
	}
	index, ok, err := s.choosePiece(p, peerID, false, false)
	if err != nil || !ok {
		return PieceOffer{}, false, err
	}
	piece := s.pieces[index]
	length := piece.plan.Piece.Range.End - piece.plan.Piece.Range.Begin
	if length <= 0 || length > limits.PieceBytes {
		return PieceOffer{}, false, fmt.Errorf("%w: invalid piece length", ErrSchedulerConfig)
	}
	if s.stagedBytes+s.reservedBytes > s.cfg.MaxStagedBytes-length {
		return PieceOffer{}, false, nil
	}
	s.reservation++
	if s.reservation == 0 {
		s.reservation++
	}
	piece.stage = stageReserved
	piece.reservation = s.reservation
	s.reserved++
	s.reservedBytes += length
	return PieceOffer{PieceIndex: index, Reservation: s.reservation, PieceLength: uint32(length), SelectedBytes: selectedBytes(piece.plan)}, true, nil
}

// AdmitPiece completes a prior reservation after the cache stage is ready.
func (s *Scheduler) AdmitPiece(offer PieceOffer) error {
	piece, ok := s.pieces[offer.PieceIndex]
	if !ok {
		return ErrUnknownPiece
	}
	if piece.stage != stageReserved || piece.reservation != offer.Reservation {
		return ErrUnknownReservation
	}
	length := piece.plan.Piece.Range.End - piece.plan.Piece.Range.Begin
	if offer.PieceLength != uint32(length) {
		return ErrUnknownReservation
	}
	piece.stage = stageAdmitted
	piece.reservation = 0
	s.reserved--
	s.reservedBytes -= length
	s.staged++
	s.stagedBytes += length
	return nil
}

// RejectPiece releases a pending stage reservation when storage admission
// fails before any block is requested.
func (s *Scheduler) RejectPiece(offer PieceOffer) error {
	piece, ok := s.pieces[offer.PieceIndex]
	if !ok {
		return ErrUnknownPiece
	}
	if piece.stage != stageReserved || piece.reservation != offer.Reservation {
		return ErrUnknownReservation
	}
	length := piece.plan.Piece.Range.End - piece.plan.Piece.Range.Begin
	piece.stage = stageNone
	piece.reservation = 0
	s.reserved--
	s.reservedBytes -= length
	return nil
}

// NextRequests assigns at most max blocks to one peer. Normal scheduling gives
// each block one assignment; after endgame is entered, an eligible peer may
// receive a duplicate assignment within the same bounded request budgets.
func (s *Scheduler) NextRequests(peerID string, max int) ([]Request, error) {
	p, ok := s.peers[peerID]
	if !ok {
		return nil, ErrUnknownPeer
	}
	if s.IsBlacklisted(p.endpoint) {
		return nil, nil
	}
	if max <= 0 {
		return nil, nil
	}
	remaining := s.cfg.MaxGlobal - s.active
	if queue := s.cfg.MaxQueue - s.active; queue < remaining {
		remaining = queue
	}
	if peerCap := p.limit - p.active; peerCap < remaining {
		remaining = peerCap
	}
	if max < remaining {
		remaining = max
	}
	if remaining <= 0 {
		return nil, nil
	}
	requests := make([]Request, 0, remaining)
	endgame := s.endgameReady()
	for len(requests) < remaining {
		index, found, err := s.choosePiece(p, peerID, true, endgame)
		if err != nil {
			return nil, err
		}
		if !found {
			break
		}
		piece := s.pieces[index]
		for len(requests) < remaining {
			blockIndex := firstAssignableBlock(piece, peerID, endgame)
			if blockIndex < 0 {
				break
			}
			block := &piece.blocks[blockIndex]
			assignBlock(block, peerID, p.endpoint)
			p.active++
			s.active++
			requests = append(requests, Request{Peer: peerID, Endpoint: p.endpoint, Block: block.block})
		}
	}
	return requests, nil
}

// Endgame reports whether every block that remains for an admitted piece has
// at least one assignment. It is intentionally false while any wanted piece
// still needs staging or any block remains unassigned.
func (s *Scheduler) Endgame() bool {
	return s != nil && s.endgameReady()
}

func (s *Scheduler) endgameReady() bool {
	if s == nil || s.IsComplete() {
		return false
	}
	remaining := false
	for _, piece := range s.pieces {
		if piece.complete {
			continue
		}
		if piece.stage != stageAdmitted {
			return false
		}
		for _, block := range piece.blocks {
			if !block.done {
				remaining = true
				if !block.active {
					return false
				}
			}
		}
	}
	return remaining
}

func (s *Scheduler) choosePiece(p *schedulerPeer, peerID string, admittedOnly, endgame bool) (int, bool, error) {
	bestRarity := int(^uint(0) >> 1)
	bestIndex := int(^uint(0) >> 1)
	candidates := make([]int, 0)
	for _, index := range s.order {
		piece := s.pieces[index]
		if piece.complete || !peerHas(p, index) {
			continue
		}
		if admittedOnly {
			if piece.stage != stageAdmitted {
				continue
			}
		} else if piece.stage != stageNone {
			continue
		}
		if endgame {
			if !hasAssignableBlock(piece, peerID, true) {
				continue
			}
		} else if pendingBlocks(piece) == 0 {
			continue
		}
		rarity := s.rarity(index)
		if s.cfg.Streaming {
			if index < bestIndex {
				bestIndex = index
				candidates = candidates[:0]
			} else if index > bestIndex {
				continue
			}
		}
		if !s.cfg.Streaming && rarity < bestRarity {
			bestRarity = rarity
			candidates = candidates[:0]
		}
		if s.cfg.Streaming {
			candidates = append(candidates, index)
		} else if rarity == bestRarity {
			candidates = append(candidates, index)
		}
	}
	if len(candidates) == 0 {
		return 0, false, nil
	}
	if err := s.shuffle(candidates); err != nil {
		return 0, false, err
	}
	return candidates[0], true, nil
}

func peerHas(p *schedulerPeer, index int) bool {
	return hasBit(p.available, index)
}

func (s *Scheduler) rarity(index int) int {
	if piece := s.pieces[index]; piece != nil {
		return piece.rarity
	}
	return 0
}

func hasBit(words []uint64, index int) bool {
	if index < 0 || index/64 >= len(words) {
		return false
	}
	return words[index/64]&(uint64(1)<<uint(index%64)) != 0
}

func setBit(words []uint64, index int) {
	if index >= 0 && index/64 < len(words) {
		words[index/64] |= uint64(1) << uint(index%64)
	}
}

func clearBit(words []uint64, index int) {
	if index >= 0 && index/64 < len(words) {
		words[index/64] &^= uint64(1) << uint(index%64)
	}
}

func pendingBlocks(piece *pieceState) int {
	n := 0
	for _, block := range piece.blocks {
		if !block.done && !block.active {
			n++
		}
	}
	return n
}

func firstAssignableBlock(piece *pieceState, peerID string, endgame bool) int {
	for i := range piece.blocks {
		block := &piece.blocks[i]
		if block.done {
			continue
		}
		if !endgame {
			if !block.active {
				return i
			}
			continue
		}
		if !hasAssignment(block, peerID) {
			return i
		}
	}
	return -1
}

func hasAssignableBlock(piece *pieceState, peerID string, endgame bool) bool {
	for i := range piece.blocks {
		block := &piece.blocks[i]
		if block.done {
			continue
		}
		if !endgame && block.active {
			continue
		}
		if endgame && hasAssignment(block, peerID) {
			continue
		}
		return true
	}
	return false
}

func assignBlock(block *blockState, peerID string, endpoint peer.Endpoint) {
	if block.assignments == nil {
		block.assignments = make(map[string]peer.Endpoint)
	}
	block.assignments[peerID] = endpoint
	block.active = true
	if len(block.assignments) == 1 {
		block.peerID = peerID
		block.endpoint = endpoint
	}
}

func hasAssignment(block *blockState, peerID string) bool {
	if block == nil || !block.active {
		return false
	}
	_, ok := block.assignments[peerID]
	return ok
}

func refreshAssignments(block *blockState) {
	if len(block.assignments) == 0 {
		block.active = false
		block.peerID = ""
		block.endpoint = peer.Endpoint{}
		return
	}
	block.active = true
	if _, ok := block.assignments[block.peerID]; !ok {
		for id, endpoint := range block.assignments {
			block.peerID = id
			block.endpoint = endpoint
			break
		}
	}
}

func firstPendingBlock(piece *pieceState) int {
	for i := range piece.blocks {
		if !piece.blocks[i].done && !piece.blocks[i].active {
			return i
		}
	}
	return -1
}

// Snapshot returns a copy of the completed block coverage for a staged piece.
// The snapshot is valid until the next scheduler mutation, so callers should
// pass it directly to the serialized finalizer and then call VerifyPiece.
func (s *Scheduler) Snapshot(index int) (PieceSnapshot, error) {
	piece, ok := s.pieces[index]
	if !ok {
		return PieceSnapshot{}, ErrUnknownPiece
	}
	if piece.stage != stageAdmitted || !allDone(piece) {
		return PieceSnapshot{}, ErrPieceNotReady
	}
	snapshot := PieceSnapshot{
		Piece:  piece.plan.Piece,
		Plan:   clonePiecePlan(piece.plan),
		Blocks: make([]BlockCoverage, len(piece.blocks)),
	}
	for i, block := range piece.blocks {
		snapshot.Blocks[i] = BlockCoverage{Block: block.block, Endpoint: block.endpoint}
	}
	return snapshot, nil
}

func clonePiecePlan(plan torrent.PiecePlan) torrent.PiecePlan {
	clone := plan
	clone.Data = append([]torrent.FileRange(nil), plan.Data...)
	clone.Selected = append([]torrent.FileRange(nil), plan.Selected...)
	clone.Padding = append([]torrent.ByteRange(nil), plan.Padding...)
	return clone
}

// AcceptBlock settles one exact outstanding request and records its endpoint
// as a contributor. The caller supplies only terminal identity; payload bytes
// remain owned by S2.
func (s *Scheduler) AcceptBlock(peerID string, block peer.Block) (BlockResult, error) {
	p, ok := s.peers[peerID]
	if !ok {
		return BlockResult{}, ErrUnknownPeer
	}
	piece, b, ok := s.findActive(peerID, block)
	if !ok {
		return BlockResult{}, ErrBlockNotOutstanding
	}
	winnerEndpoint := b.assignments[peerID]
	losers := make([]Request, 0, len(b.assignments)-1)
	for loserID, endpoint := range b.assignments {
		if loserID == peerID {
			continue
		}
		losers = append(losers, Request{Peer: loserID, Endpoint: endpoint, Block: b.block})
		if loser := s.peers[loserID]; loser != nil {
			loser.active--
		}
		s.active--
	}
	sort.Slice(losers, func(i, j int) bool { return losers[i].Peer < losers[j].Peer })
	b.assignments = nil
	b.active = false
	b.done = true
	p.active--
	s.active--
	b.peerID = peerID
	b.endpoint = winnerEndpoint
	piece.contributors[winnerEndpoint] = struct{}{}
	return BlockResult{PieceIndex: int(block.Index), Complete: allDone(piece), Canceled: losers}, nil
}

// RejectBlock returns an exact outstanding block to the pending set without a
// corruption strike. Ordinary peer rejection, timeout, and compatibility
// behavior do not affect endpoint penalties.
func (s *Scheduler) RejectBlock(peerID string, block peer.Block) error {
	p, ok := s.peers[peerID]
	if !ok {
		return ErrUnknownPeer
	}
	_, b, ok := s.findActive(peerID, block)
	if !ok {
		return ErrBlockNotOutstanding
	}
	delete(b.assignments, peerID)
	refreshAssignments(b)
	p.active--
	s.active--
	return nil
}

func (s *Scheduler) findActive(peerID string, block peer.Block) (*pieceState, *blockState, bool) {
	piece, ok := s.pieces[int(block.Index)]
	if !ok {
		return nil, nil, false
	}
	for i := range piece.blocks {
		b := &piece.blocks[i]
		if b.block == block && hasAssignment(b, peerID) {
			return piece, b, true
		}
	}
	return nil, nil, false
}

func allDone(piece *pieceState) bool {
	for _, b := range piece.blocks {
		if !b.done {
			return false
		}
	}
	return true
}

// VerifyPiece settles a staged piece after S2 has verified and committed its
// complete cache image. A mismatch frees the stage budget, resets all blocks,
// and strikes each distinct endpoint that supplied an accepted block.
func (s *Scheduler) VerifyPiece(index int, valid bool) (VerificationResult, error) {
	piece, ok := s.pieces[index]
	if !ok {
		return VerificationResult{}, ErrUnknownPiece
	}
	if piece.stage != stageAdmitted || !allDone(piece) {
		return VerificationResult{}, ErrPieceNotReady
	}
	result := VerificationResult{PieceIndex: index, Verified: valid}
	result.Contributors = endpoints(piece.contributors)
	if valid {
		piece.complete = true
		result.SelectedBytes = selectedBytes(piece.plan)
		s.verified += result.SelectedBytes
		piece.contributors = make(map[peer.Endpoint]struct{})
		s.releaseStage(piece)
		result.Completed = s.verified == s.selected
		return result, nil
	}
	for _, endpoint := range result.Contributors {
		result.Strikes = append(result.Strikes, endpoint)
		if s.strike(endpoint) {
			result.Blacklisted = append(result.Blacklisted, endpoint)
		}
	}
	for i := range piece.blocks {
		piece.blocks[i].active = false
		piece.blocks[i].done = false
		piece.blocks[i].peerID = ""
		piece.blocks[i].endpoint = peer.Endpoint{}
	}
	piece.contributors = make(map[peer.Endpoint]struct{})
	s.releaseStage(piece)
	return result, nil
}

func (s *Scheduler) releaseStage(piece *pieceState) {
	length := piece.plan.Piece.Range.End - piece.plan.Piece.Range.Begin
	if piece.stage == stageAdmitted {
		s.staged--
		s.stagedBytes -= length
	}
	if piece.stage == stageReserved {
		s.reserved--
		s.reservedBytes -= length
	}
	piece.stage = stageNone
	piece.reservation = 0
}

func endpoints(set map[peer.Endpoint]struct{}) []peer.Endpoint {
	result := make([]peer.Endpoint, 0, len(set))
	for endpoint := range set {
		result = append(result, endpoint)
	}
	sort.Slice(result, func(i, j int) bool {
		if compare := result[i].Addr.Compare(result[j].Addr); compare != 0 {
			return compare < 0
		}
		return result[i].Port < result[j].Port
	})
	return result
}

func (s *Scheduler) strike(endpoint peer.Endpoint) bool {
	if !validEndpoint(endpoint) {
		return false
	}
	s.strikes[endpoint]++
	if s.strikes[endpoint] >= 3 {
		if _, already := s.blacklist[endpoint]; !already {
			s.blacklist[endpoint] = struct{}{}
			s.releaseEndpoint(endpoint)
			return true
		}
	}
	return false
}

// SevereViolation immediately blacklists an endpoint. It is idempotent and
// intentionally does not turn a protocol failure into a corruption strike.
func (s *Scheduler) SevereViolation(endpoint peer.Endpoint) bool {
	if !validEndpoint(endpoint) {
		return false
	}
	if _, already := s.blacklist[endpoint]; already {
		return false
	}
	s.blacklist[endpoint] = struct{}{}
	s.releaseEndpoint(endpoint)
	return true
}

func (s *Scheduler) releaseEndpoint(endpoint peer.Endpoint) {
	for peerID, p := range s.peers {
		if p.endpoint != endpoint {
			continue
		}
		for _, piece := range s.pieces {
			for i := range piece.blocks {
				block := &piece.blocks[i]
				if !hasAssignment(block, peerID) {
					continue
				}
				delete(block.assignments, peerID)
				refreshAssignments(block)
				p.active--
				s.active--
			}
		}
	}
}

func validEndpoint(endpoint peer.Endpoint) bool {
	addr := endpoint.Addr
	return endpoint.Port != 0 && addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast()
}

// MarkComplete records a piece verified by resume before transfer admission.
// It contributes selected progress and makes the piece ineligible for future
// scheduling. No cache budget is consumed.
func (s *Scheduler) MarkComplete(index int) error {
	piece, ok := s.pieces[index]
	if !ok {
		return ErrUnknownPiece
	}
	if piece.complete {
		return nil
	}
	if piece.stage != stageNone || anyActive(piece) {
		return ErrPieceNotReady
	}
	piece.complete = true
	s.verified += selectedBytes(piece.plan)
	return nil
}

func anyActive(piece *pieceState) bool {
	for _, b := range piece.blocks {
		if b.active {
			return true
		}
	}
	return false
}

// IsComplete reports whether every wanted piece has verified selected output.
func (s *Scheduler) IsComplete() bool { return s != nil && s.verified == s.selected }

// Progress returns the verified selected-byte counter and immutable total.
func (s *Scheduler) Progress() Progress {
	if s == nil {
		return Progress{}
	}
	return Progress{Verified: s.verified, Selected: s.selected}
}

// IsBlacklisted reports endpoint-scoped corruption/severe-violation state.
func (s *Scheduler) IsBlacklisted(endpoint peer.Endpoint) bool {
	if s == nil {
		return false
	}
	_, ok := s.blacklist[endpoint]
	return ok
}

// StrikeCount returns the current endpoint corruption count.
func (s *Scheduler) StrikeCount(endpoint peer.Endpoint) int { return s.strikes[endpoint] }

// ActiveRequests, StagedPieces, StagedBytes, and PendingPiece report bounded
// observations for transfer integration and tests.
func (s *Scheduler) ActiveRequests() int { return s.active }
func (s *Scheduler) StagedPieces() int   { return s.staged }
func (s *Scheduler) StagedBytes() int64  { return s.stagedBytes }

func (s *Scheduler) PendingPiece(index int) (bool, error) {
	piece, ok := s.pieces[index]
	if !ok {
		return false, ErrUnknownPiece
	}
	return !piece.complete && pendingBlocks(piece) > 0, nil
}

// Rarity reports the number of connected peers advertising a wanted piece.
func (s *Scheduler) Rarity(index int) (int, error) {
	piece, ok := s.pieces[index]
	if !ok {
		return 0, ErrUnknownPiece
	}
	return piece.rarity, nil
}
