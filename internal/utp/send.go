package utp

import (
	"errors"
	"io"
	"math"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrSendClosed          = errors.New("utp: send state is closed")
	ErrSendQueueFull       = errors.New("utp: send queue is full")
	ErrSendAckOutOfRange   = errors.New("utp: acknowledgement is outside the send window")
	ErrSendUnexpectedSYN   = errors.New("utp: unexpected SYN on connected send state")
	ErrSendAlreadyStarted  = errors.New("utp: SYN has already been sent")
	ErrSendAlreadyFinished = errors.New("utp: FIN has already been sent")
	ErrSendReset           = errors.New("utp: peer reset the connection")
	ErrInvalidSendConfig   = errors.New("utp: invalid send configuration")
)

// SendConfig controls bounded sender state. Zero values select production
// bounds and the default congestion controller. MaxQueueBytes and MaxUnacked
// may be lowered for deterministic tests, but never raised above DESIGN §16.
type SendConfig struct {
	MaxQueueBytes int
	MaxUnacked    int
	RemoteWindow  uint32
	Congestion    CongestionConfig

	ConnectionID        uint16
	WindowSize          uint32
	AckNr               Sequence
	TimestampDifference Timestamp
}

// SendResult reports one receive-side transition. Actions contain only
// retransmissions or newly available data; U4 supplies receive ACK fields when
// it emits packets from ReceiveState.
type SendResult struct {
	Actions     []PacketAction
	AckedBytes  int
	LostPackets int
	Err         error
}

type sendPacket struct {
	packet            Packet
	sentAt            time.Time
	transmissions     int
	fastRetransmitted bool
}

// SendState is a bounded, socket-independent uTP sending state machine. It
// never starts a goroutine or owns a socket. A U4 adapter drives it by calling
// Produce after writes, Handle for received packets, and Tick from its I/O
// deadline loop.
type SendState struct {
	queue      []byte
	maxQueue   int
	maxUnacked int

	nextSeq    Sequence
	lastAck    Sequence
	highestSeq Sequence
	haveSent   bool

	unacked  map[Sequence]*sendPacket
	order    []Sequence
	inFlight uint32

	remoteWindow uint32
	connectionID uint16
	windowSize   uint32
	ackNr        Sequence
	tsDifference Timestamp

	congestion *CongestionController

	started  bool
	synSeq   Sequence
	finished bool
	finSeq   Sequence
	terminal error

	lastActivity  time.Time
	timeoutCount  uint
	duplicateAcks map[Sequence]uint8
	ackEvidence   map[Sequence]uint8
}

// NewSendState creates a sender using the fixed supported bounds.
func NewSendState(next Sequence) *SendState {
	state, err := NewSendStateWithConfig(next, SendConfig{})
	if err != nil {
		panic(err)
	}
	return state
}

// NewSendStateWithConfig creates a sender with lowered local bounds where
// requested. The returned state is ready to queue data; Start emits SYN when
// the U4 handshake is ready to begin.
func NewSendStateWithConfig(next Sequence, config SendConfig) (*SendState, error) {
	maxQueue := config.MaxQueueBytes
	if maxQueue == 0 {
		maxQueue = int(limits.UTPBufferBytes)
	}
	maxUnacked := config.MaxUnacked
	if maxUnacked == 0 {
		maxUnacked = limits.UTPUnackedPackets
	}
	if maxQueue < 1 || int64(maxQueue) > limits.UTPBufferBytes || maxUnacked < 1 || maxUnacked > limits.UTPUnackedPackets {
		return nil, ErrInvalidSendConfig
	}
	congestion, err := NewCongestionController(config.Congestion)
	if err != nil {
		return nil, err
	}
	remoteWindow := config.RemoteWindow
	if remoteWindow == 0 {
		remoteWindow = uint32(limits.UTPBufferBytes)
	}
	if remoteWindow > uint32(limits.UTPBufferBytes) {
		remoteWindow = uint32(limits.UTPBufferBytes)
	}
	localWindow := config.WindowSize
	if localWindow == 0 || localWindow > uint32(limits.UTPBufferBytes) {
		localWindow = uint32(limits.UTPBufferBytes)
	}
	return &SendState{
		maxQueue:      maxQueue,
		maxUnacked:    maxUnacked,
		nextSeq:       next,
		lastAck:       next.Add(^uint16(0)),
		unacked:       make(map[Sequence]*sendPacket),
		remoteWindow:  remoteWindow,
		connectionID:  config.ConnectionID,
		windowSize:    localWindow,
		ackNr:         config.AckNr,
		tsDifference:  config.TimestampDifference,
		congestion:    congestion,
		duplicateAcks: make(map[Sequence]uint8),
		ackEvidence:   make(map[Sequence]uint8),
	}, nil
}

// NewSendStateWithLimits mirrors ReceiveState's deterministic constructor for
// callers that only need smaller queue and unacknowledged-packet bounds.
func NewSendStateWithLimits(next Sequence, maxUnacked, maxQueueBytes int) (*SendState, error) {
	return NewSendStateWithConfig(next, SendConfig{MaxUnacked: maxUnacked, MaxQueueBytes: maxQueueBytes})
}

// Queue appends payload to the bounded send queue and returns the number of
// bytes accepted. If only a prefix fits, it returns that prefix with
// ErrSendQueueFull so callers can apply backpressure without an unbounded copy.
func (s *SendState) Queue(payload []byte) (int, error) {
	if s == nil || s.terminal != nil {
		return 0, ErrSendClosed
	}
	if s.finished {
		return 0, ErrSendAlreadyFinished
	}
	if len(payload) == 0 {
		return 0, nil
	}
	available := s.maxQueue - len(s.queue)
	if available <= 0 {
		return 0, ErrSendQueueFull
	}
	n := len(payload)
	if n > available {
		n = available
	}
	s.queue = append(s.queue, payload[:n]...)
	if n != len(payload) {
		return n, ErrSendQueueFull
	}
	return n, nil
}

// Enqueue is an explicit alias for Queue for callers that prefer queue
// terminology.
func (s *SendState) Enqueue(payload []byte) (int, error) { return s.Queue(payload) }

func (s *SendState) PendingBytes() int {
	if s == nil {
		return 0
	}
	return len(s.queue)
}

func (s *SendState) InFlightBytes() uint32 {
	if s == nil {
		return 0
	}
	return s.inFlight
}

func (s *SendState) UnackedPackets() int {
	if s == nil {
		return 0
	}
	return len(s.unacked)
}

func (s *SendState) RemoteWindow() uint32 {
	if s == nil {
		return 0
	}
	return s.remoteWindow
}

func (s *SendState) Congestion() *CongestionController {
	if s == nil {
		return nil
	}
	return s.congestion
}

func (s *SendState) MaxWindow() uint32 {
	if s == nil || s.congestion == nil {
		return 0
	}
	return s.congestion.MaxWindow()
}

func (s *SendState) PacketSize() int {
	if s == nil || s.congestion == nil {
		return 0
	}
	return s.congestion.PacketSize()
}

func (s *SendState) RTO() time.Duration {
	if s == nil || s.congestion == nil {
		return defaultInitialRTO
	}
	return s.congestion.RTO()
}

func (s *SendState) Started() bool { return s != nil && s.started }

func (s *SendState) FinSent() bool { return s != nil && s.finished }

// Finished reports whether a FIN was sent and its sequence number has been
// cumulatively or selectively acknowledged.
func (s *SendState) Finished() bool {
	return s != nil && s.finished && s.unacked[s.finSeq] == nil
}

// SetRemoteWindow applies the peer advertised byte window, clamped to the
// local supported buffer bound. A zero value is meaningful and pauses new
// transmissions until the peer reopens its window.
func (s *SendState) SetRemoteWindow(window uint32) {
	if s == nil {
		return
	}
	if window > uint32(limits.UTPBufferBytes) {
		window = uint32(limits.UTPBufferBytes)
	}
	s.remoteWindow = window
}

func (s *SendState) SetConnectionID(id uint16) {
	if s != nil {
		s.connectionID = id
	}
}
func (s *SendState) SetAckNumber(ack Sequence) {
	if s != nil {
		s.ackNr = ack
	}
}
func (s *SendState) SetWindowSize(window uint32) {
	if s == nil {
		return
	}
	if window > uint32(limits.UTPBufferBytes) {
		window = uint32(limits.UTPBufferBytes)
	}
	s.windowSize = window
}
func (s *SendState) SetTimestampDifference(value Timestamp) {
	if s != nil {
		s.tsDifference = value
	}
}

// Start sends one SYN. The SYN uses the next sequence number and remains in
// the bounded unacknowledged map until its cumulative ACK arrives.
func (s *SendState) Start(now time.Time) ([]PacketAction, error) {
	if s == nil || s.terminal != nil {
		return nil, ErrSendClosed
	}
	if s.started {
		return nil, ErrSendAlreadyStarted
	}
	s.started = true
	s.synSeq = s.nextSeq
	return []PacketAction{s.sendControl(Syn, now)}, nil
}

// Open is an alias for Start.
func (s *SendState) Open(now time.Time) ([]PacketAction, error) { return s.Start(now) }

// Finish sends FIN after queued data has been emitted. In-flight data may
// remain; sequence numbers preserve their ordering and FIN is retransmitted
// until cumulatively acknowledged.
func (s *SendState) Finish(now time.Time) ([]PacketAction, error) {
	if s == nil || s.terminal != nil {
		return nil, ErrSendClosed
	}
	if s.finished {
		return nil, ErrSendAlreadyFinished
	}
	if len(s.queue) != 0 {
		return nil, ErrSendQueueFull
	}
	if len(s.unacked) >= s.maxUnacked {
		return nil, ErrSendQueueFull
	}
	s.finished = true
	s.finSeq = s.nextSeq
	return []PacketAction{s.sendControl(Fin, now)}, nil
}

// Close transitions the sender to an orderly local terminal state. U4 should
// call Finish first when it needs a wire FIN; Close itself only prevents later
// queueing and production.
func (s *SendState) Close(err error) PacketAction {
	if s == nil {
		return PacketAction{Kind: ActionClose, Err: err}
	}
	if s.terminal == nil {
		if err == nil {
			err = io.EOF
		}
		s.terminal = err
	} else {
		err = s.terminal
	}
	return PacketAction{Kind: ActionClose, Err: err}
}

func (s *SendState) Err() error {
	if s == nil {
		return ErrSendClosed
	}
	return s.terminal
}

// Produce fills the available remote and congestion windows with new DATA
// packets. Each packet is retained as an immutable retransmission record.
func (s *SendState) Produce(now time.Time) []PacketAction {
	if s == nil || s.terminal != nil {
		return nil
	}
	return s.produce(now)
}

// Send is an alias for Produce.
func (s *SendState) Send(now time.Time) []PacketAction { return s.Produce(now) }

func (s *SendState) produce(now time.Time) []PacketAction {
	if s.remoteWindow == 0 || s.congestion.maxWindow == 0 {
		return nil
	}
	actions := make([]PacketAction, 0)
	for len(s.queue) != 0 && len(s.unacked) < s.maxUnacked {
		limit := uint64(s.remoteWindow)
		if uint64(s.congestion.maxWindow) < limit {
			limit = uint64(s.congestion.maxWindow)
		}
		if uint64(s.inFlight) >= limit {
			break
		}
		available := int(limit - uint64(s.inFlight))
		payloadSize := s.congestion.packet
		if payloadSize > available {
			payloadSize = available
		}
		if payloadSize > len(s.queue) {
			payloadSize = len(s.queue)
		}
		if payloadSize <= 0 {
			break
		}
		payload := cloneBytes(s.queue[:payloadSize])
		s.queue = s.queue[payloadSize:]
		packet := s.header(Data, now)
		packet.Payload = payload
		actions = append(actions, s.record(packet, now))
	}
	return actions
}

// Handle processes an incoming peer packet, including cumulative and
// selective ACKs, remote window updates, delay feedback, and RESET. It emits
// fast retransmissions immediately and then fills any newly opened window.
func (s *SendState) Handle(packet Packet, now time.Time) SendResult {
	result := SendResult{}
	if s == nil || s.terminal != nil {
		result.Err = ErrSendClosed
		return result
	}
	if err := packet.Validate(); err != nil {
		return s.failResult(err)
	}
	s.lastActivity = now
	s.SetRemoteWindow(packet.WindowSize)
	s.congestion.ObserveDelay(now, time.Duration(uint64(packet.TimestampDifference))*time.Microsecond, s.inFlight)
	if packet.Type == Reset {
		s.terminal = ErrSendReset
		result.Err = ErrSendReset
		result.Actions = []PacketAction{{Kind: ActionClose, Err: ErrSendReset}}
		return result
	}
	if packet.Type == Syn {
		return s.failResult(ErrSendUnexpectedSYN)
	}
	if err := s.applyACK(packet, now, &result); err != nil {
		return s.failResult(err)
	}
	s.compactOrder()
	result.Actions = append(result.Actions, s.produce(now)...)
	return result
}

// HandleAck is an alias that documents the common state-packet call site.
func (s *SendState) HandleAck(packet Packet, now time.Time) SendResult { return s.Handle(packet, now) }

// HandleACK preserves the conventional acronym spelling for callers at the
// packet adapter boundary.
func (s *SendState) HandleACK(packet Packet, now time.Time) SendResult { return s.Handle(packet, now) }

// OnPacket is an alias for Handle.
func (s *SendState) OnPacket(packet Packet, now time.Time) SendResult { return s.Handle(packet, now) }

func (s *SendState) applyACK(packet Packet, now time.Time, result *SendResult) error {
	if !s.haveSent {
		if packet.AckNr != s.lastAck {
			return ErrSendAckOutOfRange
		}
		return nil
	}
	advance, ok := SequenceDistance(s.lastAck, packet.AckNr)
	if !ok {
		return ErrSendAckOutOfRange
	}
	sentSpan, ok := SequenceDistance(s.lastAck, s.highestSeq)
	if !ok {
		return ErrSendAckOutOfRange
	}
	if uint32(advance) > uint32(sentSpan) && uint32(advance) < sequenceHalfRange {
		return ErrSendAckOutOfRange
	}
	progress := uint32(advance) != 0 && uint32(advance) <= uint32(sentSpan)
	if progress {
		for _, sequence := range s.order {
			if _, exists := s.unacked[sequence]; !exists {
				continue
			}
			distance, distanceOK := SequenceDistance(s.lastAck, sequence)
			if distanceOK && distance != 0 && uint32(distance) <= uint32(advance) {
				s.ackOne(sequence, now, result)
			}
		}
		s.lastAck = packet.AckNr
		clear(s.duplicateAcks)
		s.timeoutCount = 0
	} else if packet.AckNr == s.lastAck {
		s.duplicateAcks[packet.AckNr]++
	}

	// Selective ACK bits refer to ack_nr+2. Unknown bits outside our sent
	// range are ignored as required by BEP 29; only retained packets can alter
	// in-flight accounting or loss evidence.
	for _, sequence := range packet.SelectiveACKSequences() {
		record, exists := s.unacked[sequence]
		if !exists || record == nil {
			continue
		}
		for _, older := range s.order {
			candidate := s.unacked[older]
			if candidate == nil || older == sequence || !older.Before(sequence) {
				continue
			}
			if s.ackEvidence[older] < math.MaxUint8 {
				s.ackEvidence[older]++
			}
			if s.ackEvidence[older] >= 3 {
				s.retransmitLost(older, now, result)
			}
		}
		s.ackOne(sequence, now, result)
	}

	if duplicate := s.duplicateAcks[packet.AckNr]; duplicate >= 3 {
		candidate := packet.AckNr.Add(1)
		if _, exists := s.unacked[candidate]; exists {
			s.retransmitLost(candidate, now, result)
		}
	}
	return nil
}

func (s *SendState) ackOne(sequence Sequence, now time.Time, result *SendResult) {
	record := s.unacked[sequence]
	if record == nil {
		return
	}
	delete(s.unacked, sequence)
	if uint64(s.inFlight) >= uint64(len(record.packet.Payload)) {
		s.inFlight -= uint32(len(record.packet.Payload))
	} else {
		s.inFlight = 0
	}
	result.AckedBytes += len(record.packet.Payload)
	delete(s.ackEvidence, sequence)
	if record.transmissions == 1 && !record.sentAt.IsZero() && now.After(record.sentAt) {
		s.congestion.UpdateRTT(now.Sub(record.sentAt))
	}
}

func (s *SendState) compactOrder() {
	retained := s.order[:0]
	for _, sequence := range s.order {
		if s.unacked[sequence] != nil {
			retained = append(retained, sequence)
		}
	}
	s.order = retained
}

func (s *SendState) retransmitLost(sequence Sequence, now time.Time, result *SendResult) {
	record := s.unacked[sequence]
	if record == nil || record.fastRetransmitted {
		return
	}
	record.fastRetransmitted = true
	s.congestion.OnLoss()
	record.transmissions++
	record.sentAt = now
	record.packet.Timestamp = timestamp(now)
	record.packet.TimestampDifference = s.tsDifference
	s.lastActivity = now
	result.Actions = append(result.Actions, PacketAction{Kind: ActionSend, Packet: clonePacket(record.packet)})
	result.LostPackets++
}

// Tick checks the oldest outstanding packet and emits at most one timeout
// retransmission. The timeout is exponentially backed off and capped to keep
// duration arithmetic bounded. SYN and FIN records use the same path.
func (s *SendState) Tick(now time.Time) []PacketAction {
	if s == nil || s.terminal != nil || len(s.unacked) == 0 {
		return nil
	}
	oldest := s.oldest()
	if oldest == nil || oldest.sentAt.IsZero() {
		return nil
	}
	lastActivity := s.lastActivity
	if lastActivity.IsZero() {
		lastActivity = oldest.sentAt
	}
	backoff := time.Duration(1 << minUint(s.timeoutCount, 6))
	if now.Sub(lastActivity) < saturatingDuration(s.congestion.RTO(), backoff) {
		return nil
	}
	s.timeoutCount++
	s.congestion.OnTimeout()
	oldest.fastRetransmitted = true
	oldest.transmissions++
	oldest.sentAt = now
	oldest.packet.Timestamp = timestamp(now)
	oldest.packet.TimestampDifference = s.tsDifference
	s.lastActivity = now
	return []PacketAction{{Kind: ActionSend, Packet: clonePacket(oldest.packet)}}
}

func (s *SendState) oldest() *sendPacket {
	for _, sequence := range s.order {
		if record := s.unacked[sequence]; record != nil {
			return record
		}
	}
	return nil
}

func (s *SendState) sendControl(kind PacketType, now time.Time) PacketAction {
	packet := s.header(kind, now)
	return s.record(packet, now)
}

func (s *SendState) header(kind PacketType, now time.Time) Packet {
	sequence := s.nextSeq
	s.nextSeq = s.nextSeq.Add(1)
	return Packet{
		Type:                kind,
		ConnectionID:        s.connectionID,
		Timestamp:           timestamp(now),
		TimestampDifference: s.tsDifference,
		WindowSize:          s.windowSize,
		SeqNr:               sequence,
		AckNr:               s.ackNr,
	}
}

func (s *SendState) record(packet Packet, now time.Time) PacketAction {
	sequence := packet.SeqNr
	copyOf := clonePacket(packet)
	record := &sendPacket{packet: copyOf, sentAt: now, transmissions: 1}
	s.unacked[sequence] = record
	s.order = append(s.order, sequence)
	if !s.haveSent {
		s.haveSent = true
	}
	s.highestSeq = sequence
	if uint64(s.inFlight)+uint64(len(packet.Payload)) > math.MaxUint32 {
		s.inFlight = math.MaxUint32
	} else {
		s.inFlight += uint32(len(packet.Payload))
	}
	s.lastActivity = now
	return PacketAction{Kind: ActionSend, Packet: clonePacket(packet)}
}

func (s *SendState) failResult(err error) SendResult {
	s.terminal = err
	return SendResult{Err: err, Actions: []PacketAction{{Kind: ActionClose, Err: err}}}
}

func timestamp(now time.Time) Timestamp {
	return Timestamp(uint32(now.UnixNano() / int64(time.Microsecond)))
}

func clonePacket(packet Packet) Packet {
	clone := packet
	clone.Payload = cloneBytes(packet.Payload)
	if len(packet.Extensions) != 0 {
		clone.Extensions = make([]Extension, len(packet.Extensions))
		for index, extension := range packet.Extensions {
			clone.Extensions[index] = Extension{Type: extension.Type, Data: cloneBytes(extension.Data)}
		}
	}
	return clone
}

func minUint(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func saturatingDuration(duration time.Duration, multiplier time.Duration) time.Duration {
	if duration <= 0 || multiplier <= 0 {
		return duration
	}
	if duration > time.Duration(math.MaxInt64)/multiplier {
		return time.Duration(math.MaxInt64)
	}
	return duration * multiplier
}
