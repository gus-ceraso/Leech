package utp

import (
	"errors"
	"fmt"
	"io"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// The receive side owns packet payloads after Receive accepts them. A packet
// sequence number is a packet number, not a byte offset; the receive state
// therefore keeps a small map for packets that arrived past a gap and a byte
// queue for packets that are ready for the application.

var (
	ErrReceiveClosed   = errors.New("utp: receive state is closed")
	ErrReceiveReset    = errors.New("utp: peer reset the connection")
	ErrReceiveCanceled = errors.New("utp: receive state canceled")
	ErrReceiveSequence = errors.New("utp: ambiguous receive sequence")
	ErrReceiveAfterFIN = errors.New("utp: packet received after FIN")
	ErrUnexpectedSYN   = errors.New("utp: unexpected SYN on connected receive state")
)

const (
	// Extension data is one byte long and Selective ACK data must be a
	// multiple of four bytes. Keep the receive sequence distance within the
	// part of the ring that can be represented by one bounded SACK extension.
	maxSACKBytes     = 252
	maxSACKBits      = maxSACKBytes * 8
	maxReceiveOffset = maxSACKBits + 1 // ack+2 is SACK bit zero
)

// ReceiveResult describes one receive transition. Actions contain the ACK or
// terminal close that the socket owner should emit. WindowFull means the
// packet was a valid packet that could not be retained under local bounds;
// callers should continue using the advertised window and retry later.
type ReceiveResult struct {
	Actions    []PacketAction
	Accepted   bool
	Duplicate  bool
	WindowFull bool
}

type receivePacket struct {
	payload []byte
	fin     bool
}

// ReceiveState is the bounded, socket-independent receive half of a uTP
// stream. next is the first packet sequence number not yet cumulatively ACKed.
// maxPackets and maxBytes are local caps and may be lowered for deterministic
// tests, but never raised above the supported bounds.
type ReceiveState struct {
	next       Sequence
	ack        Sequence
	maxPackets int
	maxBytes   int

	pending      map[Sequence]receivePacket
	stream       []byte
	bufferedByte int

	finSeen  bool
	finSeq   Sequence
	finReady bool
	terminal error
}

// NewReceiveState creates a receiver whose first expected packet is next.
// The default caps are the fixed limits in DESIGN.md. The constructor cannot
// be used to expand those limits.
func NewReceiveState(next Sequence) *ReceiveState {
	state, err := newReceiveState(next, limits.UTPReorderPackets, int(limits.UTPBufferBytes))
	if err != nil {
		// The fixed constants above are part of the package contract. Keep the
		// public constructor infallible while retaining validation in the
		// lower-level constructor used by tests.
		panic(err)
	}
	return state
}

// NewReceiveStateWithLimits is useful for a caller that wants a smaller local
// receive window. It is also useful for deterministic state tests. A zero cap
// selects the corresponding default; neither cap may exceed the supported
// bound.
func NewReceiveStateWithLimits(next Sequence, maxPackets, maxBytes int) (*ReceiveState, error) {
	if maxPackets == 0 {
		maxPackets = limits.UTPReorderPackets
	}
	if maxBytes == 0 {
		maxBytes = int(limits.UTPBufferBytes)
	}
	return newReceiveState(next, maxPackets, maxBytes)
}

func newReceiveState(next Sequence, maxPackets, maxBytes int) (*ReceiveState, error) {
	if maxPackets < 1 || maxPackets > limits.UTPReorderPackets {
		return nil, fmt.Errorf("utp: receive packet cap %d outside 1..%d", maxPackets, limits.UTPReorderPackets)
	}
	if maxBytes < 1 || int64(maxBytes) > limits.UTPBufferBytes {
		return nil, fmt.Errorf("utp: receive byte cap %d outside 1..%d", maxBytes, limits.UTPBufferBytes)
	}
	return &ReceiveState{
		next:       next,
		ack:        next.Add(^uint16(0)),
		maxPackets: maxPackets,
		maxBytes:   maxBytes,
		pending:    make(map[Sequence]receivePacket),
	}, nil
}

// Receive handles one validated or decoded packet. State packets are handled
// by the send side and are ignored here. Data and FIN packets produce an ACK
// action; RESET produces a terminal close action. Invalid connected-state
// input produces an ActionClose and leaves the state terminal.
func (r *ReceiveState) Receive(packet Packet) ReceiveResult {
	if r == nil {
		return ReceiveResult{Actions: []PacketAction{{Kind: ActionClose, Err: ErrReceiveClosed}}}
	}
	if r.terminal != nil {
		return ReceiveResult{}
	}
	if err := packet.Validate(); err != nil {
		return r.fail(err)
	}

	switch packet.Type {
	case State:
		return ReceiveResult{}
	case Syn:
		return r.fail(ErrUnexpectedSYN)
	case Reset:
		r.terminal = ErrReceiveReset
		// A reset terminates the stream immediately. Retaining already
		// delivered bytes is harmless, but discard not-yet-ordered packets so
		// reset cannot leave a large hidden allocation alive.
		r.pending = make(map[Sequence]receivePacket)
		r.bufferedByte = len(r.stream)
		return ReceiveResult{Actions: []PacketAction{{Kind: ActionClose, Err: ErrReceiveReset}}}
	case Data, Fin:
		return r.receiveData(packet)
	default:
		return r.fail(ErrUnexpectedSYN)
	}
}

func (r *ReceiveState) receiveData(packet Packet) ReceiveResult {
	if r.finSeen {
		comparison, ok := CompareSequence(packet.SeqNr, r.finSeq)
		if !ok {
			return r.fail(ErrReceiveSequence)
		}
		if comparison > 0 {
			return r.fail(ErrReceiveAfterFIN)
		}
	}

	distance, ok := SequenceDistance(r.ack, packet.SeqNr)
	if !ok {
		return r.fail(ErrReceiveSequence)
	}
	if distance == 0 {
		// The cumulative ACK itself, or a retransmission of an already
		// delivered packet, is harmless and should elicit the current ACK.
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, Duplicate: true}
	}
	if uint32(distance) >= sequenceHalfRange {
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, Duplicate: true}
	}

	if distance == 1 {
		if len(packet.Payload) > r.maxBytes-r.bufferedByte {
			return ReceiveResult{Actions: []PacketAction{r.ackAction()}, WindowFull: true}
		}
		if packet.Type == Fin {
			r.finSeen = true
			r.finSeq = packet.SeqNr
		}
		r.acceptContiguous(packet)
		r.discardPostFIN()
		r.drainPending()
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, Accepted: true}
	}

	// A SACK extension can describe only maxReceiveOffset sequence distance,
	// and storing anything farther would make the receive state unverifiable to
	// the sender. Treat this as local pressure, with no allocation or close.
	if _, exists := r.pending[packet.SeqNr]; exists {
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, Duplicate: true}
	}
	if int(distance) > maxReceiveOffset || len(r.pending) >= r.maxPackets {
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, WindowFull: true}
	}
	if len(packet.Payload) > r.maxBytes-r.bufferedByte {
		return ReceiveResult{Actions: []PacketAction{r.ackAction()}, WindowFull: true}
	}

	r.pending[packet.SeqNr] = receivePacket{payload: cloneBytes(packet.Payload), fin: packet.Type == Fin}
	r.bufferedByte += len(packet.Payload)
	if packet.Type == Fin {
		r.finSeen = true
		r.finSeq = packet.SeqNr
		r.discardPostFIN()
	}
	return ReceiveResult{Actions: []PacketAction{r.ackAction()}, Accepted: true}
}

func (r *ReceiveState) acceptContiguous(packet Packet) {
	if len(packet.Payload) != 0 {
		r.appendStream(packet.Payload)
		r.bufferedByte += len(packet.Payload)
	}
	r.ack = packet.SeqNr
	r.next = packet.SeqNr.Add(1)
	if packet.Type == Fin {
		r.finReady = true
	}
}

func (r *ReceiveState) drainPending() {
	for {
		if r.finReady {
			return
		}
		packet, ok := r.pending[r.next]
		if !ok {
			return
		}
		delete(r.pending, r.next)
		if len(packet.payload) != 0 {
			r.appendStream(packet.payload)
		}
		r.ack = r.next
		r.next = r.next.Add(1)
		if packet.fin {
			r.finReady = true
		}
	}
}

// discardPostFIN removes packets that were buffered before the receiver knew
// the peer's eof_pkt. BEP 29 defines FIN as the final sequence number, so a
// packet after FIN can never become application data even when it arrived
// first. Releasing its payload also reopens the advertised receive window.
func (r *ReceiveState) discardPostFIN() {
	if !r.finSeen {
		return
	}
	for sequence, packet := range r.pending {
		comparison, ok := CompareSequence(r.finSeq, sequence)
		if !ok || comparison >= 0 {
			continue
		}
		delete(r.pending, sequence)
		r.bufferedByte -= len(packet.payload)
	}
}

// appendStream keeps the backing array within the receive byte cap. Go's
// append may otherwise grow geometrically past maxBytes even when the final
// retained stream would fit exactly at the cap.
func (r *ReceiveState) appendStream(data []byte) {
	needed := len(r.stream) + len(data)
	if needed > cap(r.stream) {
		stream := make([]byte, len(r.stream), needed)
		copy(stream, r.stream)
		r.stream = stream
	}
	r.stream = append(r.stream, data...)
}

func (r *ReceiveState) ackAction() PacketAction {
	return PacketAction{Kind: ActionSend, Packet: r.AckPacket()}
}

// AckPacket returns a fresh STATE packet describing the current cumulative
// ACK, selective ACK mask, and byte receive window. U4 fills connection and
// local sequence fields before encoding it.
func (r *ReceiveState) AckPacket() Packet {
	if r == nil {
		return Packet{Type: State}
	}
	packet := Packet{
		Type:       State,
		AckNr:      r.ack,
		WindowSize: r.WindowSize(),
	}
	if len(r.pending) == 0 {
		return packet
	}

	maxDistance := 0
	for sequence := range r.pending {
		distance, ok := SequenceDistance(r.ack, sequence)
		if !ok || distance < 2 || int(distance) > maxReceiveOffset {
			continue
		}
		if int(distance) > maxDistance {
			maxDistance = int(distance)
		}
	}
	if maxDistance == 0 {
		return packet
	}
	length := ((maxDistance-2)/8 + 1 + 3) / 4 * 4
	if length > maxSACKBytes {
		length = maxSACKBytes
	}
	mask := make([]byte, length)
	for sequence := range r.pending {
		distance, ok := SequenceDistance(r.ack, sequence)
		if !ok || distance < 2 || int(distance) > maxReceiveOffset {
			continue
		}
		bit := int(distance) - 2
		if bit/8 < len(mask) {
			mask[bit/8] |= 1 << uint(bit%8)
		}
	}
	packet.Extensions = []Extension{{Type: SelectiveACKExtension, Data: mask}}
	return packet
}

// Read consumes ordered bytes. Buffered bytes are returned before a terminal
// error, matching net.Conn behavior. FIN returns io.EOF after all preceding
// bytes have been consumed; RESET or cancellation returns its error then.
func (r *ReceiveState) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(r.stream) != 0 {
		n := copy(dst, r.stream)
		r.stream = r.stream[n:]
		r.bufferedByte -= n
		return n, nil
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	if r.finReady {
		return 0, io.EOF
	}
	return 0, nil
}

// Cancel transitions the receiver to a terminal state and returns the close
// action for its socket owner. A nil error is an orderly close and is observed
// by Read as EOF after already-buffered bytes.
func (r *ReceiveState) Cancel(err error) PacketAction {
	if r.terminal != nil {
		return PacketAction{Kind: ActionClose, Err: r.terminal}
	}
	if err == nil {
		r.terminal = io.EOF
	} else {
		r.terminal = err
	}
	r.pending = make(map[Sequence]receivePacket)
	r.bufferedByte = len(r.stream)
	return PacketAction{Kind: ActionClose, Err: err}
}

// WindowSize is the number of additional payload bytes that the receiver can
// retain. It is the uTP wnd_size value, and is safe to convert to uint32.
func (r *ReceiveState) WindowSize() uint32 {
	if r == nil || len(r.pending) >= r.maxPackets || r.bufferedByte >= r.maxBytes {
		return 0
	}
	return uint32(r.maxBytes - r.bufferedByte)
}

func (r *ReceiveState) AckNumber() Sequence {
	if r == nil {
		return 0
	}
	return r.ack
}

func (r *ReceiveState) NextSequence() Sequence {
	if r == nil {
		return 0
	}
	return r.next
}

// Buffered reports retained packet and byte counts, including ordered bytes
// that have not yet been consumed by Read.
func (r *ReceiveState) Buffered() (packets, bytes int) {
	if r == nil {
		return 0, 0
	}
	return len(r.pending), r.bufferedByte
}

func (r *ReceiveState) Finished() bool {
	return r != nil && r.finReady
}

func (r *ReceiveState) Err() error {
	if r == nil {
		return ErrReceiveClosed
	}
	return r.terminal
}

func (r *ReceiveState) fail(err error) ReceiveResult {
	r.terminal = err
	r.pending = make(map[Sequence]receivePacket)
	r.bufferedByte = len(r.stream)
	return ReceiveResult{Actions: []PacketAction{{Kind: ActionClose, Err: err}}}
}
