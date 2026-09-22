package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// ErrorClass tells the coordinator whether a wire error is a severe peer
// violation, a normal connection end, or a local compatibility limitation.
type ErrorClass uint8

const (
	ClassProtocolViolation ErrorClass = iota + 1
	ClassDisconnect
	ClassUnsupported
)

func (c ErrorClass) String() string {
	switch c {
	case ClassProtocolViolation:
		return "protocol violation"
	case ClassDisconnect:
		return "disconnect"
	case ClassUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

var (
	// ErrProtocolViolation marks malformed or impossible peer traffic.
	ErrProtocolViolation = errors.New("peer protocol violation")
	// ErrDisconnected marks EOF, truncation, and transport errors.
	ErrDisconnected = errors.New("peer disconnected")
	// ErrUnsupported marks a valid request for a capability this package does
	// not provide, including every outbound upload message.
	ErrUnsupported = errors.New("unsupported peer operation")
	// ErrInvalidMessage marks a locally constructed command that violates the
	// wire format before it reaches a connection.
	ErrInvalidMessage = errors.New("invalid peer message")
)

// WireError preserves the actionable class and the underlying I/O error.
type WireError struct {
	Kind ErrorClass
	Op   string
	Err  error
}

func (e *WireError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return e.Op + ": " + e.Kind.String()
	}
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Kind, e.Err)
}

func (e *WireError) Unwrap() error { return e.Err }

func (e *WireError) Is(target error) bool {
	switch target {
	case ErrProtocolViolation:
		return e != nil && e.Kind == ClassProtocolViolation
	case ErrDisconnected:
		return e != nil && e.Kind == ClassDisconnect
	case ErrUnsupported:
		return e != nil && e.Kind == ClassUnsupported
	default:
		return e != nil && e.Err != nil && errors.Is(e.Err, target)
	}
}

// ClassOf returns the peer-wire class carried by err. Unwrapped transport
// errors are ordinary disconnects because no peer violation was established.
func ClassOf(err error) ErrorClass {
	var wireErr *WireError
	if errors.As(err, &wireErr) {
		return wireErr.Kind
	}
	switch {
	case errors.Is(err, ErrProtocolViolation):
		return ClassProtocolViolation
	case errors.Is(err, ErrUnsupported):
		return ClassUnsupported
	case errors.Is(err, ErrDisconnected):
		return ClassDisconnect
	}
	return ClassDisconnect
}

func IsProtocolViolation(err error) bool { return errors.Is(err, ErrProtocolViolation) }
func IsDisconnect(err error) bool        { return errors.Is(err, ErrDisconnected) }
func IsUnsupported(err error) bool       { return errors.Is(err, ErrUnsupported) }

func protocolError(op, reason string) error {
	return &WireError{Kind: ClassProtocolViolation, Op: op, Err: fmt.Errorf("%w: %s", ErrProtocolViolation, reason)}
}

func disconnectError(op string, err error) error {
	if err == nil {
		err = ErrDisconnected
	}
	return &WireError{Kind: ClassDisconnect, Op: op, Err: err}
}

func unsupportedError(op, reason string) error {
	return &WireError{Kind: ClassUnsupported, Op: op, Err: fmt.Errorf("%w: %s", ErrUnsupported, reason)}
}

// Core message IDs from BEP 3 and BEP 6. IDs without outbound encoders are
// still decoded so the state machine can make its own peer-local decision.
const (
	ChokeID         byte = 0
	UnchokeID       byte = 1
	InterestedID    byte = 2
	NotInterestedID byte = 3
	HaveID          byte = 4
	BitfieldID      byte = 5
	RequestID       byte = 6
	PieceID         byte = 7
	CancelID        byte = 8
	SuggestID       byte = 13
	HaveAllID       byte = 14
	HaveNoneID      byte = 15
	RejectRequestID byte = 16
	AllowedFastID   byte = 17
)

// MaxPeerFrameBytes is the complete length-prefix value, including the
// message ID and excluding the four-byte length prefix itself.
const MaxPeerFrameBytes = limits.PeerFrameBytes

// Message is one decoded peer-wire frame. Payload excludes the one-byte ID.
// KeepAlive is true only for a zero-length frame.
type Message struct {
	ID        byte
	Payload   []byte
	KeepAlive bool
}

// ReadOptions enables validation that requires session context. A zero value
// still validates frame structure and all fixed message lengths.
type ReadOptions struct {
	Fast            bool
	PieceCount      uint32
	ValidateIndices bool
	PieceLength     uint32
	LastPieceLength uint32
}

// MessageOptions is retained as a descriptive alias for callers that prefer
// to name options after the decoded value.
type MessageOptions = ReadOptions

// ReadMessage reads one bounded frame. It does not require Fast negotiation
// context; use ReadMessageWithOptions when the handshake is available.
func ReadMessage(conn net.Conn) (Message, error) {
	return readMessage(conn, ReadOptions{}, false)
}

// ReadFrame is a framing-oriented alias for ReadMessage.
func ReadFrame(conn net.Conn) (Message, error) { return ReadMessage(conn) }

// ReadMessageWithOptions reads one bounded frame and validates reserved-bit
// dependent IDs and piece indices against the supplied connection state.
func ReadMessageWithOptions(conn net.Conn, opts ReadOptions) (Message, error) {
	return readMessage(conn, opts, true)
}

func readMessage(conn net.Conn, opts ReadOptions, enforceFast bool) (Message, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return Message{}, disconnectError("read frame length", err)
	}
	frameLength := binary.BigEndian.Uint32(prefix[:])
	if frameLength > MaxPeerFrameBytes {
		return Message{}, protocolError("read frame", fmt.Sprintf("length %d exceeds %d bytes", frameLength, MaxPeerFrameBytes))
	}
	if frameLength == 0 {
		return Message{KeepAlive: true}, nil
	}

	var id [1]byte
	if _, err := io.ReadFull(conn, id[:]); err != nil {
		return Message{}, disconnectError("read frame ID", err)
	}
	payloadLength := int(frameLength) - 1
	if fixed, ok := fixedPayloadLength(id[0]); ok && payloadLength != fixed {
		return Message{}, protocolError("read frame", fmt.Sprintf("message ID %d has payload length %d, want %d", id[0], payloadLength, fixed))
	}
	if isFastMessage(id[0]) && enforceFast && !opts.Fast {
		return Message{}, protocolError("read frame", fmt.Sprintf("Fast message ID %d without negotiated Fast", id[0]))
	}

	switch id[0] {
	case HaveID, SuggestID, AllowedFastID:
		payload, err := readPayload(conn, payloadLength)
		if err != nil {
			return Message{}, err
		}
		if err := validateMessagePayload(id[0], payload, opts); err != nil {
			return Message{}, err
		}
		return Message{ID: id[0], Payload: payload}, nil
	case RequestID, CancelID, RejectRequestID:
		payload, err := readPayload(conn, payloadLength)
		if err != nil {
			return Message{}, err
		}
		if err := validateMessagePayload(id[0], payload, opts); err != nil {
			return Message{}, err
		}
		return Message{ID: id[0], Payload: payload}, nil
	case PieceID:
		// Read the identifying fields before allocating the variable block so an
		// impossible piece index is rejected without a payload-sized allocation.
		if payloadLength < 9 { // index, begin, and at least one data byte
			return Message{}, protocolError("read piece", "piece payload is shorter than 9 bytes")
		}
		var head [8]byte
		if _, err := io.ReadFull(conn, head[:]); err != nil {
			return Message{}, disconnectError("read piece header", err)
		}
		index := binary.BigEndian.Uint32(head[:4])
		if err := validateIndex(index, opts); err != nil {
			return Message{}, err
		}
		payload := make([]byte, payloadLength)
		copy(payload, head[:])
		if _, err := io.ReadFull(conn, payload[8:]); err != nil {
			return Message{}, disconnectError("read piece payload", err)
		}
		if err := validateMessagePayload(id[0], payload, opts); err != nil {
			return Message{}, err
		}
		return Message{ID: id[0], Payload: payload}, nil
	case BitfieldID:
		if err := validateBitfieldLength(payloadLength, opts); err != nil {
			return Message{}, err
		}
		payload, err := readPayload(conn, payloadLength)
		if err != nil {
			return Message{}, err
		}
		if err := validateBitfieldSpareBits(payload, opts); err != nil {
			return Message{}, err
		}
		return Message{ID: id[0], Payload: payload}, nil
	default:
		// Unknown IDs are ignored by higher layers when their frame is bounded
		// and well framed. Retaining their payload keeps stream alignment.
		payload, err := readPayload(conn, payloadLength)
		if err != nil {
			return Message{}, err
		}
		if err := validateMessagePayload(id[0], payload, opts); err != nil {
			return Message{}, err
		}
		return Message{ID: id[0], Payload: payload}, nil
	}
}

func readPayload(conn net.Conn, n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, disconnectError("read frame payload", err)
	}
	return payload, nil
}

func fixedPayloadLength(id byte) (int, bool) {
	switch id {
	case ChokeID, UnchokeID, InterestedID, NotInterestedID, HaveAllID, HaveNoneID:
		return 0, true
	case HaveID, SuggestID, AllowedFastID:
		return 4, true
	case RequestID, CancelID, RejectRequestID:
		return 12, true
	default:
		return 0, false
	}
}

func isFastMessage(id byte) bool {
	switch id {
	case SuggestID, HaveAllID, HaveNoneID, RejectRequestID, AllowedFastID:
		return true
	default:
		return false
	}
}

func validateMessagePayload(id byte, payload []byte, opts ReadOptions) error {
	switch id {
	case HaveID, SuggestID, AllowedFastID:
		if len(payload) != 4 {
			return protocolError("validate message", "index payload has the wrong length")
		}
		return validateIndex(binary.BigEndian.Uint32(payload), opts)
	case RequestID, CancelID, RejectRequestID:
		if len(payload) != 12 {
			return protocolError("validate message", "block payload has the wrong length")
		}
		index := binary.BigEndian.Uint32(payload[:4])
		begin := binary.BigEndian.Uint32(payload[4:8])
		length := binary.BigEndian.Uint32(payload[8:12])
		if err := validateIndex(index, opts); err != nil {
			return err
		}
		return validateBlock(index, begin, length, opts)
	case PieceID:
		if len(payload) < 9 {
			return protocolError("validate piece", "piece payload is shorter than 9 bytes")
		}
		index := binary.BigEndian.Uint32(payload[:4])
		begin := binary.BigEndian.Uint32(payload[4:8])
		if err := validateIndex(index, opts); err != nil {
			return err
		}
		return validatePieceRange(index, begin, uint32(len(payload)-8), opts)
	default:
		return nil
	}
}

func validateIndex(index uint32, opts ReadOptions) error {
	if opts.ValidateIndices && index >= opts.PieceCount {
		return protocolError("validate index", fmt.Sprintf("piece index %d is outside piece count %d", index, opts.PieceCount))
	}
	return nil
}

func validateBlock(index, begin, length uint32, opts ReadOptions) error {
	if length == 0 || length > limits.BlockBytes {
		return protocolError("validate block", fmt.Sprintf("block length %d is outside 1..%d", length, limits.BlockBytes))
	}
	if begin > ^uint32(0)-length {
		return protocolError("validate block", "block offset overflows uint32")
	}
	return validatePieceRange(index, begin, length, opts)
}

func validatePieceRange(index, begin, length uint32, opts ReadOptions) error {
	if opts.PieceLength == 0 {
		return nil
	}
	pieceLength := opts.PieceLength
	if opts.ValidateIndices && index+1 == opts.PieceCount && opts.LastPieceLength != 0 {
		pieceLength = opts.LastPieceLength
	}
	if begin >= pieceLength || length > pieceLength-begin {
		return protocolError("validate block", "block crosses the piece boundary")
	}
	return nil
}

func validateBitfieldLength(n int, opts ReadOptions) error {
	if !opts.ValidateIndices {
		return nil
	}
	want := (uint64(opts.PieceCount) + 7) / 8
	if uint64(n) != want {
		return protocolError("validate bitfield", fmt.Sprintf("bitfield length %d, want %d", n, want))
	}
	return nil
}

func validateBitfieldSpareBits(payload []byte, opts ReadOptions) error {
	if !opts.ValidateIndices || len(payload) == 0 || opts.PieceCount%8 == 0 {
		return nil
	}
	spare := uint8(8 - opts.PieceCount%8)
	if payload[len(payload)-1]&(1<<spare-1) != 0 {
		return protocolError("validate bitfield", "spare bit is set")
	}
	return nil
}

// ValidateMessage applies the same message checks to an already decoded
// frame. Fast-dependent checks are always applied here because the caller
// explicitly supplied negotiated state.
func ValidateMessage(message Message, opts ReadOptions) error {
	if message.KeepAlive {
		if message.ID != 0 || len(message.Payload) != 0 {
			return protocolError("validate message", "keepalive has message data")
		}
		return nil
	}
	if _, ok := fixedPayloadLength(message.ID); ok {
		want, _ := fixedPayloadLength(message.ID)
		if len(message.Payload) != want {
			return protocolError("validate message", "fixed message has the wrong payload length")
		}
	}
	if isFastMessage(message.ID) && !opts.Fast {
		return protocolError("validate message", "Fast message without negotiated Fast")
	}
	if message.ID == BitfieldID {
		if err := validateBitfieldLength(len(message.Payload), opts); err != nil {
			return err
		}
		return validateBitfieldSpareBits(message.Payload, opts)
	}
	return validateMessagePayload(message.ID, message.Payload, opts)
}

func blockPayload(index, begin, length uint32) []byte {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:], length)
	return payload
}

func encodeSimple(id byte) []byte {
	return []byte{0, 0, 0, 1, id}
}

// EncodeChoke is the only choke-state transition exposed for local output.
func EncodeChoke() []byte { return encodeSimple(ChokeID) }

func EncodeInterested() []byte    { return encodeSimple(InterestedID) }
func EncodeNotInterested() []byte { return encodeSimple(NotInterestedID) }
func EncodeHaveNone() []byte      { return encodeSimple(HaveNoneID) }

// EncodeRequest encodes a bounded block request. It returns nil for an
// invalid block, preserving a simple byte-oriented API for fixtures.
func EncodeRequest(index, begin, length uint32) []byte {
	wire, err := EncodeOutbound(Message{ID: RequestID, Payload: blockPayload(index, begin, length)})
	if err != nil {
		return nil
	}
	return wire
}

func EncodeCancel(index, begin, length uint32) []byte {
	wire, err := EncodeOutbound(Message{ID: CancelID, Payload: blockPayload(index, begin, length)})
	if err != nil {
		return nil
	}
	return wire
}

func EncodeRejectRequest(index, begin, length uint32) []byte {
	wire, err := EncodeOutbound(Message{ID: RejectRequestID, Payload: blockPayload(index, begin, length)})
	if err != nil {
		return nil
	}
	return wire
}

// EncodeOutbound builds only commands Leech is allowed to send. In
// particular, Piece, metadata-data, Bitfield, Have, Have All, and Unchoke
// cannot be represented by this API.
func EncodeOutbound(message Message) ([]byte, error) {
	if message.KeepAlive {
		if message.ID != 0 || len(message.Payload) != 0 {
			return nil, fmt.Errorf("%w: keepalive has message data", ErrInvalidMessage)
		}
		return []byte{0, 0, 0, 0}, nil
	}
	if !outboundID(message.ID) {
		return nil, unsupportedError("encode outbound", fmt.Sprintf("message ID %d is not a permitted local command", message.ID))
	}
	if len(message.Payload)+1 > MaxPeerFrameBytes {
		return nil, fmt.Errorf("%w: frame exceeds %d bytes", ErrInvalidMessage, MaxPeerFrameBytes)
	}
	if err := validateOutboundPayload(message.ID, message.Payload); err != nil {
		return nil, err
	}
	wire := make([]byte, 4+1+len(message.Payload))
	binary.BigEndian.PutUint32(wire[:4], uint32(1+len(message.Payload)))
	wire[4] = message.ID
	copy(wire[5:], message.Payload)
	return wire, nil
}

// WriteMessage writes one permitted command, or a keepalive, to conn.
func WriteMessage(conn net.Conn, message Message) error {
	wire, err := EncodeOutbound(message)
	if err != nil {
		return err
	}
	if err := writeAll(conn, wire); err != nil {
		return disconnectError("write peer message", err)
	}
	return nil
}

func WriteKeepAlive(conn net.Conn) error { return WriteMessage(conn, Message{KeepAlive: true}) }
func WriteChoke(conn net.Conn) error     { return WriteMessage(conn, Message{ID: ChokeID}) }
func WriteInterested(conn net.Conn) error {
	return WriteMessage(conn, Message{ID: InterestedID})
}
func WriteNotInterested(conn net.Conn) error {
	return WriteMessage(conn, Message{ID: NotInterestedID})
}
func WriteRequest(conn net.Conn, index, begin, length uint32) error {
	return WriteMessage(conn, Message{ID: RequestID, Payload: blockPayload(index, begin, length)})
}
func WriteCancel(conn net.Conn, index, begin, length uint32) error {
	return WriteMessage(conn, Message{ID: CancelID, Payload: blockPayload(index, begin, length)})
}
func WriteRejectRequest(conn net.Conn, index, begin, length uint32) error {
	return WriteMessage(conn, Message{ID: RejectRequestID, Payload: blockPayload(index, begin, length)})
}

func outboundID(id byte) bool {
	switch id {
	case ChokeID, InterestedID, NotInterestedID, RequestID, CancelID, RejectRequestID, HaveNoneID:
		return true
	default:
		return false
	}
}

func validateOutboundPayload(id byte, payload []byte) error {
	want, ok := fixedPayloadLength(id)
	if ok && len(payload) != want {
		return fmt.Errorf("%w: message ID %d has payload length %d, want %d", ErrInvalidMessage, id, len(payload), want)
	}
	if id == RequestID || id == CancelID || id == RejectRequestID {
		if len(payload) != 12 {
			return fmt.Errorf("%w: block command must be 12 bytes", ErrInvalidMessage)
		}
		length := binary.BigEndian.Uint32(payload[8:])
		if length == 0 || length > limits.BlockBytes {
			return fmt.Errorf("%w: block length %d is outside 1..%d", ErrInvalidMessage, length, limits.BlockBytes)
		}
		begin := binary.BigEndian.Uint32(payload[4:8])
		if begin > ^uint32(0)-length {
			return fmt.Errorf("%w: block offset overflows uint32", ErrInvalidMessage)
		}
	}
	return nil
}

func writeAll(conn net.Conn, wire []byte) error {
	for len(wire) != 0 {
		n, err := conn.Write(wire)
		if n < 0 || n > len(wire) {
			return io.ErrShortWrite
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		wire = wire[n:]
	}
	return nil
}
