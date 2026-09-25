package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrPeerStateConfig  = errors.New("invalid peer state configuration")
	ErrIncomingRequest  = errors.New("abusive repeated incoming request")
	ErrPieceUnavailable = errors.New("piece is not advertised by peer")
	ErrPieceNotAllowed  = errors.New("choked peer did not allow piece")
)

const maxSuggestions = 256

// PeerStateConfig contains immutable limits and negotiated state for one
// connection. Mutable availability, interest, and request state remain owned
// by the session coordinator.
type PeerStateConfig struct {
	PieceCount      uint32
	PieceLength     uint32
	LastPieceLength uint32
	Fast            bool
	ReqQ            uint32
	ReqQSet         bool
}

// StateEffect reports a decoded message's coordinator-visible consequence.
// Response is always a local control message; it never carries file payload.
type StateEffect struct {
	InterestChanged     bool
	Interested          bool
	AvailabilityAdded   []uint32
	AvailabilityRemoved []uint32
	SuggestedPiece      uint32
	HasSuggestion       bool
	Terminal            Terminal
	HasTerminal         bool
	Response            *Message
}

// IncomingDisposition records how a request from the remote peer is handled.
// Ignore is the required non-Fast behavior. Reject is a single Fast reject
// response. Violation asks the coordinator to close as a protocol violation.
type IncomingDisposition uint8

const (
	IncomingIgnore IncomingDisposition = iota
	IncomingReject
	IncomingViolation
)

// PeerState is one peer's protocol state. All methods must be called by the
// coordinator goroutine; no method starts a goroutine or touches a net.Conn.
type PeerState struct {
	pieceCount        uint32
	pieceLength       uint32
	lastPieceLength   uint32
	fast              bool
	choked            bool
	interested        bool
	initialSeen       bool
	reqQ              int
	availability      bitSet
	allowedFast       bitSet
	wanted            bitSet
	availabilityCount uint32
	wantedAvailable   uint32
	requestableCount  uint32
	suggestions       []uint32
	seenIncoming      map[Block]struct{}
	requests          *RequestTable
}

// NewPeerState creates coordinator-owned state with the supported request and
// availability bounds.
func NewPeerState(pieceCount uint32, fast bool) (*PeerState, error) {
	return NewPeerStateWithConfig(PeerStateConfig{PieceCount: pieceCount, Fast: fast})
}

func NewPeerStateWithConfig(config PeerStateConfig) (*PeerState, error) {
	if config.PieceCount > limits.Pieces {
		return nil, fmt.Errorf("%w: piece count %d exceeds %d", ErrPeerStateConfig, config.PieceCount, limits.Pieces)
	}
	if config.PieceLength > uint32(limits.PieceBytes) || config.LastPieceLength > uint32(limits.PieceBytes) {
		return nil, fmt.Errorf("%w: piece length exceeds %d", ErrPeerStateConfig, limits.PieceBytes)
	}
	if config.LastPieceLength != 0 && config.PieceLength != 0 && config.LastPieceLength > config.PieceLength {
		return nil, fmt.Errorf("%w: last piece is longer than normal piece", ErrPeerStateConfig)
	}
	requests := NewRequestTable(config.Fast)
	reqq := ClampReqQ(config.ReqQ)
	if !config.ReqQSet && config.ReqQ == 0 {
		reqq = limits.PeerRequests
	}
	return &PeerState{
		pieceCount:      config.PieceCount,
		pieceLength:     config.PieceLength,
		lastPieceLength: config.LastPieceLength,
		fast:            config.Fast,
		choked:          true,
		reqQ:            reqq,
		availability:    newBitSet(config.PieceCount),
		allowedFast:     newBitSet(config.PieceCount),
		wanted:          newBitSet(config.PieceCount),
		seenIncoming:    make(map[Block]struct{}, limits.PeerRequests),
		requests:        requests,
	}, nil
}

func (s *PeerState) Fast() bool       { return s != nil && s.fast }
func (s *PeerState) Choked() bool     { return s != nil && s.choked }
func (s *PeerState) Interested() bool { return s != nil && s.interested }
func (s *PeerState) PieceCount() uint32 {
	if s == nil {
		return 0
	}
	return s.pieceCount
}
func (s *PeerState) RequestableCount() uint32 {
	if s == nil {
		return 0
	}
	return s.requestableCount
}
func (s *PeerState) ReqQ() int {
	if s == nil {
		return 0
	}
	return s.reqQ
}

// SetReqQ applies a later, explicitly present remote reqq hint and returns the
// effective local pipeline cap. Use SetReqQHint when presence is optional.
func (s *PeerState) SetReqQ(reqq uint32) int {
	if s == nil {
		return 0
	}
	s.reqQ = ClampReqQ(reqq)
	return s.reqQ
}

// SetReqQHint preserves the distinction between a missing reqq dictionary
// key and an explicitly advertised zero. A missing hint leaves the supported
// local cap in place; an explicit zero permits no outstanding requests.
func (s *PeerState) SetReqQHint(reqq uint32, present bool) int {
	if s == nil {
		return 0
	}
	if !present {
		s.reqQ = limits.PeerRequests
		return s.reqQ
	}
	return s.SetReqQ(reqq)
}

func (s *PeerState) Requests() *RequestTable {
	if s == nil {
		return nil
	}
	return s.requests
}

// SetWanted updates the coordinator's wanted-piece view and returns the new
// interest state. Allowed Fast does not itself make a peer interesting.
func (s *PeerState) SetWanted(index uint32, wanted bool) (bool, bool, error) {
	if err := s.validIndex(index); err != nil {
		return false, s.interested, err
	}
	if s.wanted.has(index) == wanted {
		return false, s.interested, nil
	}
	s.wanted.set(index, wanted)
	if s.availability.has(index) {
		if wanted {
			s.wantedAvailable++
			if !s.choked || s.allowedFast.has(index) {
				s.requestableCount++
			}
		} else {
			s.wantedAvailable--
			if !s.choked || s.allowedFast.has(index) {
				s.requestableCount--
			}
		}
	}
	changed, interested := s.refreshInterestChange()
	return changed, interested, nil
}

// SetWantedPieces replaces the wanted set in one pass over the selected
// indices. Call it before registering the peer with a scheduler.
func (s *PeerState) SetWantedPieces(indices []int) error {
	if s == nil {
		return ErrPeerStateConfig
	}
	wanted := newBitSet(s.pieceCount)
	var wantedAvailable uint32
	for _, index := range indices {
		if index < 0 || uint64(index) >= uint64(s.pieceCount) {
			return fmt.Errorf("%w: wanted piece index %d outside piece count %d", ErrPeerStateConfig, index, s.pieceCount)
		}
		piece := uint32(index)
		if wanted.has(piece) {
			continue
		}
		wanted.set(piece, true)
		if s.availability.has(piece) {
			wantedAvailable++
		}
	}
	s.wanted = wanted
	s.wantedAvailable = wantedAvailable
	s.interested = wantedAvailable != 0
	if s.choked {
		s.requestableCount = bitAndCount(s.availability, s.allowedFast, wanted)
	} else {
		s.requestableCount = wantedAvailable
	}
	return nil
}

// SetChoked applies a remote choke. Fast keeps requests outstanding; ordinary
// peers release them while preserving bounded late-response obligations.
func (s *PeerState) SetChoked(choked bool) error {
	if s == nil {
		return ErrPeerStateConfig
	}
	if s.choked != choked {
		if choked {
			s.requestableCount = bitAndCount(s.availability, s.allowedFast, s.wanted)
		} else {
			s.requestableCount = s.wantedAvailable
		}
	}
	s.choked = choked
	if choked {
		return s.requests.Choke()
	}
	return nil
}

// CanRequest reports the BEP 3/BEP 6 availability rule before AddRequest is
// called. The request pipeline cap is enforced by AddRequest itself.
func (s *PeerState) CanRequest(index uint32) bool {
	if s == nil || index >= s.pieceCount || !s.availability.has(index) {
		return false
	}
	return !s.choked || s.allowedFast.has(index)
}

// AddRequest records an exact request if the remote is eligible for it.
func (s *PeerState) AddRequest(block Block) error {
	if s == nil {
		return ErrPeerStateConfig
	}
	if err := s.validBlock(block); err != nil {
		return err
	}
	if !s.CanRequest(block.Index) {
		if s.choked {
			return ErrPieceNotAllowed
		}
		return ErrPieceUnavailable
	}
	if s.requests.OutstandingCount() >= s.reqQ {
		return ErrRequestLimit
	}
	return s.requests.Add(block)
}

func (s *PeerState) CancelRequest(block Block) error  { return s.requests.Cancel(block) }
func (s *PeerState) TimeoutRequest(block Block) error { return s.requests.Timeout(block) }

// ApplyMessage consumes a decoded peer message. The wire worker performs
// framing and negotiated Fast validation first; this method repeats semantic
// checks so coordinator tests can safely apply decoded fixtures directly.
func (s *PeerState) ApplyMessage(message Message) (StateEffect, error) {
	if s == nil {
		return StateEffect{}, ErrPeerStateConfig
	}
	if err := ValidateMessage(message, ReadOptions{Fast: s.fast, ValidateIndices: true, PieceCount: s.pieceCount, PieceLength: s.pieceLength, LastPieceLength: s.lastPieceLength}); err != nil {
		return StateEffect{}, err
	}
	if message.KeepAlive {
		return StateEffect{}, nil
	}
	var effect StateEffect
	switch message.ID {
	case ChokeID:
		if !s.choked {
			s.appendChokeAvailabilityChanges(&effect, false)
		}
		if err := s.SetChoked(true); err != nil {
			return effect, err
		}
	case UnchokeID:
		if s.choked {
			s.appendChokeAvailabilityChanges(&effect, true)
		}
		if err := s.SetChoked(false); err != nil {
			return effect, err
		}
	case InterestedID, NotInterestedID:
		// Leech remains choked forever. Incoming reciprocal state has no
		// effect on the download state and is intentionally ignored.
	case HaveID:
		index := binary.BigEndian.Uint32(message.Payload)
		if !s.availability.has(index) {
			s.availability.set(index, true)
			s.availabilityCount++
			if s.wanted.has(index) {
				s.wantedAvailable++
				if !s.choked || s.allowedFast.has(index) {
					s.requestableCount++
					effect.AvailabilityAdded = append(effect.AvailabilityAdded, index)
				}
			}
		}
		// A peer may omit its initial Bitfield. Once it has sent an
		// incremental Have, a later initial availability frame is stale.
		s.initialSeen = true
		effect.InterestChanged, effect.Interested = s.refreshInterestChange()
	case HaveAllID:
		if s.initialSeen {
			// BEP 3 permits Bitfield only as the first availability frame.
			// Ignore a repeated Have All rather than restoring stale state.
			break
		}
		s.availability.fill()
		s.availabilityCount = s.pieceCount
		s.wantedAvailable = bitCount(s.wanted)
		if s.choked {
			s.requestableCount = bitAndCount(s.availability, s.allowedFast, s.wanted)
		} else {
			s.requestableCount = s.wantedAvailable
		}
		s.initialSeen = true
		s.appendCurrentAvailability(&effect, true)
		effect.InterestChanged, effect.Interested = s.refreshInterestChange()
	case HaveNoneID:
		if !s.fast {
			return effect, protocolError("peer state", "Have None without negotiated Fast")
		}
		// Have None is always a safe empty-state correction. In particular,
		// it must clear stale ordinary availability without clearing Allowed
		// Fast, even if a peer sent a duplicate initial frame. An already-empty
		// availability set is a constant-time no-op.
		if s.availabilityCount != 0 {
			s.appendCurrentAvailability(&effect, false)
			s.availability.clear()
			s.availabilityCount = 0
			s.wantedAvailable = 0
			s.requestableCount = 0
		}
		s.initialSeen = true
		effect.InterestChanged, effect.Interested = s.refreshInterestChange()
	case BitfieldID:
		if s.initialSeen {
			// The payload was still structurally validated above. Do not let
			// a late Bitfield restore availability cleared by Have None.
			break
		}
		if err := s.applyBitfield(message.Payload); err != nil {
			return effect, err
		}
		s.initialSeen = true
		s.appendCurrentAvailability(&effect, true)
		effect.InterestChanged, effect.Interested = s.refreshInterestChange()
	case AllowedFastID:
		if !s.fast {
			return effect, protocolError("peer state", "Allowed Fast without negotiated Fast")
		}
		index := binary.BigEndian.Uint32(message.Payload)
		if !s.allowedFast.has(index) {
			s.allowedFast.set(index, true)
			if s.choked && s.availability.has(index) && s.wanted.has(index) {
				s.requestableCount++
				effect.AvailabilityAdded = append(effect.AvailabilityAdded, index)
			}
		}
	case SuggestID:
		index := binary.BigEndian.Uint32(message.Payload)
		if len(s.suggestions) < maxSuggestions && !containsUint32(s.suggestions, index) {
			s.suggestions = append(s.suggestions, index)
		}
		effect.SuggestedPiece, effect.HasSuggestion = index, true
	case PieceID:
		block := Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: uint32(len(message.Payload) - 8)}
		terminal, err := s.requests.Terminal(block, false)
		if err != nil {
			return effect, terminalViolation(err)
		}
		effect.Terminal, effect.HasTerminal = terminal, true
	case RejectRequestID:
		block := Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:12])}
		terminal, err := s.requests.Terminal(block, true)
		if err != nil {
			return effect, terminalViolation(err)
		}
		effect.Terminal, effect.HasTerminal = terminal, true
	case RequestID:
		block := Block{Index: binary.BigEndian.Uint32(message.Payload[:4]), Begin: binary.BigEndian.Uint32(message.Payload[4:8]), Length: binary.BigEndian.Uint32(message.Payload[8:12])}
		disposition, err := s.IncomingRequest(block)
		if err != nil {
			return effect, err
		}
		if disposition == IncomingReject {
			response := Message{ID: RejectRequestID, Payload: blockPayload(block.Index, block.Begin, block.Length)}
			effect.Response = &response
		}
	case CancelID:
		// No upload request is ever queued, so a remote cancel cannot cause
		// storage access or a response.
	}
	return effect, nil
}

// IncomingRequest handles a request from the remote without accepting a
// payload source. It is safe to call this before a worker is connected.
func (s *PeerState) IncomingRequest(block Block) (IncomingDisposition, error) {
	if err := s.validBlock(block); err != nil {
		return IncomingViolation, err
	}
	if !s.fast {
		return IncomingIgnore, nil
	}
	if _, seen := s.seenIncoming[block]; seen || len(s.seenIncoming) >= limits.PeerRequests {
		return IncomingViolation, fmt.Errorf("%w: %w", protocolError("peer state", ErrIncomingRequest.Error()), ErrIncomingRequest)
	}
	s.seenIncoming[block] = struct{}{}
	return IncomingReject, nil
}

func terminalViolation(err error) error {
	if errors.Is(err, ErrUnexpectedTerminal) {
		return fmt.Errorf("%w: %w", protocolError("peer state", err.Error()), err)
	}
	return err
}

func (s *PeerState) Availability(index uint32) bool {
	return s != nil && index < s.pieceCount && s.availability.has(index)
}

func (s *PeerState) AllowedFast(index uint32) bool {
	return s != nil && index < s.pieceCount && s.allowedFast.has(index)
}

func (s *PeerState) Suggestions() []uint32 {
	if s == nil {
		return nil
	}
	result := make([]uint32, len(s.suggestions))
	copy(result, s.suggestions)
	return result
}

func (s *PeerState) validIndex(index uint32) error {
	if index >= s.pieceCount {
		return protocolError("peer state", fmt.Sprintf("piece index %d outside piece count %d", index, s.pieceCount))
	}
	return nil
}

func (s *PeerState) validBlock(block Block) error {
	if err := s.validIndex(block.Index); err != nil {
		return err
	}
	if err := validateBlockTuple(block); err != nil {
		return err
	}
	if s.pieceLength != 0 {
		length := s.pieceLength
		if block.Index+1 == s.pieceCount && s.lastPieceLength != 0 {
			length = s.lastPieceLength
		}
		if block.Begin >= length || block.Length > length-block.Begin {
			return protocolError("peer state", "block crosses piece boundary")
		}
	}
	return nil
}

func (s *PeerState) applyBitfield(payload []byte) error {
	want := (uint64(s.pieceCount) + 7) / 8
	if uint64(len(payload)) != want {
		return protocolError("peer state", fmt.Sprintf("bitfield length %d, want %d", len(payload), want))
	}
	// ValidateMessage applies the shared wire bitfield rule before this state
	// transition. This parser only needs to translate its valid bits.
	for index := uint32(0); index < s.pieceCount; index++ {
		if payload[index/8]&(1<<(7-index%8)) != 0 {
			s.availability.set(index, true)
			s.availabilityCount++
			if s.wanted.has(index) {
				s.wantedAvailable++
				if !s.choked || s.allowedFast.has(index) {
					s.requestableCount++
				}
			}
		}
	}
	return nil
}

func (s *PeerState) refreshInterestChange() (bool, bool) {
	old := s.interested
	current := s.wantedAvailable != 0
	s.interested = current
	return old != current, current
}

// appendCurrentAvailability reports scheduler availability for currently
// advertised wanted pieces. The caller uses it only when all of those bits
// have just changed in the same direction.
func (s *PeerState) appendCurrentAvailability(effect *StateEffect, available bool) {
	if s.wantedAvailable == 0 {
		return
	}
	for word, value := range s.availability {
		value &= s.wanted[word]
		if s.choked {
			value &= s.allowedFast[word]
		}
		s.appendAvailabilityWord(effect, word, value, available)
	}
}

// Choking removes non-Allowed-Fast availability from the scheduler; unchoking
// adds the same bits back. Allowed Fast remains an independent eligibility
// condition throughout.
func (s *PeerState) appendChokeAvailabilityChanges(effect *StateEffect, available bool) {
	if s.wantedAvailable == 0 {
		return
	}
	for word, value := range s.availability {
		value &= s.wanted[word] &^ s.allowedFast[word]
		s.appendAvailabilityWord(effect, word, value, available)
	}
}

func (s *PeerState) appendAvailabilityWord(effect *StateEffect, word int, value uint64, available bool) {
	for value != 0 {
		bit := bits.TrailingZeros64(value)
		index := uint32(word*64 + bit)
		if available {
			effect.AvailabilityAdded = append(effect.AvailabilityAdded, index)
		} else {
			effect.AvailabilityRemoved = append(effect.AvailabilityRemoved, index)
		}
		value &^= uint64(1) << uint(bit)
	}
}

func bitCount(set bitSet) uint32 {
	var count uint32
	for _, word := range set {
		count += uint32(bits.OnesCount64(word))
	}
	return count
}

func bitAndCount(a, b, c bitSet) uint32 {
	var count uint32
	for word, value := range a {
		count += uint32(bits.OnesCount64(value & b[word] & c[word]))
	}
	return count
}

type bitSet []uint64

func newBitSet(pieces uint32) bitSet { return make(bitSet, (uint64(pieces)+63)/64) }

func (b bitSet) set(index uint32, value bool) {
	word := index / 64
	bit := uint(index % 64)
	if int(word) >= len(b) {
		return
	}
	if value {
		b[word] |= uint64(1) << bit
	} else {
		b[word] &^= uint64(1) << bit
	}
}

func (b bitSet) has(index uint32) bool {
	word := index / 64
	if int(word) >= len(b) {
		return false
	}
	return b[word]&(uint64(1)<<uint(index%64)) != 0
}

func (b bitSet) clear() {
	for i := range b {
		b[i] = 0
	}
}

func (b bitSet) fill() {
	for i := range b {
		b[i] = ^uint64(0)
	}
}

func containsUint32(values []uint32, wanted uint32) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
