// Package utp contains the bounded, socket-independent pieces of Leech's
// outgoing uTP implementation.
package utp

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gus-ceraso/Leech/internal/limits"
)

const (
	// HeaderBytes is the fixed size of a version 1 uTP header.
	HeaderBytes = 20
	// ProtocolVersion is the only uTP protocol version accepted by Leech.
	ProtocolVersion uint8 = 1
)

// PacketType is the four-bit type field in a uTP header.
type PacketType uint8

const (
	Data  PacketType = 0
	Fin   PacketType = 1
	State PacketType = 2
	Reset PacketType = 3
	Syn   PacketType = 4
)

func (t PacketType) String() string {
	switch t {
	case Data:
		return "data"
	case Fin:
		return "fin"
	case State:
		return "state"
	case Reset:
		return "reset"
	case Syn:
		return "syn"
	default:
		return fmt.Sprintf("type(%d)", t)
	}
}

const (
	// SelectiveACKExtension is the BEP 29 extension code for selective ACKs.
	SelectiveACKExtension uint8 = 1
)

var (
	ErrShortPacket        = errors.New("utp: packet shorter than header")
	ErrDatagramTooLarge   = errors.New("utp: datagram exceeds 64 KiB")
	ErrUnsupportedVersion = errors.New("utp: unsupported protocol version")
	ErrUnknownPacketType  = errors.New("utp: unknown packet type")
	ErrMalformedExtension = errors.New("utp: malformed extension chain")
	ErrInvalidSACK        = errors.New("utp: invalid selective ACK extension")
	ErrInvalidPayload     = errors.New("utp: invalid payload for packet type")
	ErrExtensionTooLarge  = errors.New("utp: extension payload exceeds one-byte length")
)

// Extension is one node in the BEP 29 extension chain. Type is the type of
// this node, not the type of the next node. ParsePacket retains only SACK;
// unknown types are skipped after their framing has been checked.
type Extension struct {
	Type uint8
	Data []byte
}

// Packet is a decoded version 1 uTP datagram. Payload and supported extension
// Data are copied by ParsePacket and are owned by the returned Packet.
//
// A zero Version means version 1 when encoding, which keeps packet literals
// concise while still exposing the version received on the wire.
type Packet struct {
	Version             uint8
	Type                PacketType
	Extensions          []Extension
	ConnectionID        uint16
	Timestamp           Timestamp
	TimestampDifference Timestamp
	WindowSize          uint32
	SeqNr               Sequence
	AckNr               Sequence
	Payload             []byte
}

// PacketActionKind is the small transport boundary shared by future uTP send
// and receive state. State machines exchange complete packets or terminal
// errors; socket ownership stays with the U4 adapter.
type PacketActionKind uint8

const (
	ActionSend PacketActionKind = iota
	ActionClose
)

// PacketAction is a socket-independent result from a uTP state transition.
// Send is the only packet-producing action. Close carries the terminal error;
// a nil error means an orderly close.
type PacketAction struct {
	Kind   PacketActionKind
	Packet Packet
	Err    error
}

// ParsePacket decodes one complete, bounded uTP datagram.
func ParsePacket(data []byte) (Packet, error) {
	if len(data) < HeaderBytes {
		return Packet{}, ErrShortPacket
	}
	if len(data) > limits.DatagramBytes {
		return Packet{}, ErrDatagramTooLarge
	}

	version := data[0] & 0x0f
	if version != ProtocolVersion {
		return Packet{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	typeCode := PacketType(data[0] >> 4)
	if typeCode > Syn {
		return Packet{}, fmt.Errorf("%w: %d", ErrUnknownPacketType, typeCode)
	}

	packet := Packet{
		Version:             version,
		Type:                typeCode,
		ConnectionID:        binary.BigEndian.Uint16(data[2:4]),
		Timestamp:           Timestamp(binary.BigEndian.Uint32(data[4:8])),
		TimestampDifference: Timestamp(binary.BigEndian.Uint32(data[8:12])),
		WindowSize:          binary.BigEndian.Uint32(data[12:16]),
		SeqNr:               Sequence(binary.BigEndian.Uint16(data[16:18])),
		AckNr:               Sequence(binary.BigEndian.Uint16(data[18:20])),
	}

	offset := HeaderBytes
	nextExtension := data[1]
	seenSACK := false
	for nextExtension != 0 {
		if len(data)-offset < 2 {
			return Packet{}, fmt.Errorf("%w: truncated header", ErrMalformedExtension)
		}
		current := nextExtension
		nextExtension = data[offset]
		length := int(data[offset+1])
		offset += 2
		if length > len(data)-offset {
			return Packet{}, fmt.Errorf("%w: payload length %d exceeds datagram", ErrMalformedExtension, length)
		}
		if current == SelectiveACKExtension {
			if seenSACK || length < 4 || length%4 != 0 {
				return Packet{}, ErrInvalidSACK
			}
			seenSACK = true
		}
		if current == SelectiveACKExtension {
			packet.Extensions = append(packet.Extensions, Extension{
				Type: current, Data: cloneBytes(data[offset : offset+length]),
			})
		}
		offset += length
	}

	packet.Payload = cloneBytes(data[offset:])
	if err := packet.Validate(); err != nil {
		return Packet{}, err
	}
	return packet, nil
}

// DecodePacket is an explicit alias for ParsePacket for callers that prefer
// encode/decode terminology.
func DecodePacket(data []byte) (Packet, error) {
	return ParsePacket(data)
}

// Validate checks the packet invariants that are independent of connection
// state. It does not validate sequence numbers against a connection window.
func (p Packet) Validate() error {
	version := p.Version
	if version == 0 {
		version = ProtocolVersion
	}
	if version != ProtocolVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	if p.Type > Syn {
		return fmt.Errorf("%w: %d", ErrUnknownPacketType, p.Type)
	}
	if p.Type == Data && len(p.Payload) == 0 {
		return fmt.Errorf("%w: %s packet requires data", ErrInvalidPayload, p.Type)
	}
	if (p.Type == State || p.Type == Reset || p.Type == Syn) && len(p.Payload) != 0 {
		return fmt.Errorf("%w: %s packet cannot carry data", ErrInvalidPayload, p.Type)
	}
	seenSACK := false
	for _, extension := range p.Extensions {
		if extension.Type == 0 {
			return fmt.Errorf("%w: zero extension type", ErrMalformedExtension)
		}
		if len(extension.Data) > 255 {
			return ErrExtensionTooLarge
		}
		if extension.Type == SelectiveACKExtension {
			if seenSACK || len(extension.Data) < 4 || len(extension.Data)%4 != 0 {
				return ErrInvalidSACK
			}
			seenSACK = true
		}
	}
	length, ok := packetLength(p)
	if !ok || length > limits.DatagramBytes {
		return ErrDatagramTooLarge
	}
	return nil
}

// MarshalBinary encodes a packet as one bounded version 1 uTP datagram.
func (p Packet) MarshalBinary() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	length, _ := packetLength(p)
	encoded := make([]byte, length)
	version := p.Version
	if version == 0 {
		version = ProtocolVersion
	}
	encoded[0] = byte(p.Type)<<4 | version
	if len(p.Extensions) != 0 {
		encoded[1] = p.Extensions[0].Type
	}
	binary.BigEndian.PutUint16(encoded[2:4], p.ConnectionID)
	binary.BigEndian.PutUint32(encoded[4:8], uint32(p.Timestamp))
	binary.BigEndian.PutUint32(encoded[8:12], uint32(p.TimestampDifference))
	binary.BigEndian.PutUint32(encoded[12:16], p.WindowSize)
	binary.BigEndian.PutUint16(encoded[16:18], uint16(p.SeqNr))
	binary.BigEndian.PutUint16(encoded[18:20], uint16(p.AckNr))

	offset := HeaderBytes
	for index, extension := range p.Extensions {
		if index+1 < len(p.Extensions) {
			encoded[offset] = p.Extensions[index+1].Type
		}
		encoded[offset+1] = byte(len(extension.Data))
		offset += 2
		copy(encoded[offset:], extension.Data)
		offset += len(extension.Data)
	}
	copy(encoded[offset:], p.Payload)
	return encoded, nil
}

// EncodePacket is an explicit alias for MarshalBinary for callers that prefer
// encode/decode terminology.
func EncodePacket(p Packet) ([]byte, error) {
	return p.MarshalBinary()
}

// SelectiveACK returns a copy of the first selective ACK bitmask, if present.
func (p Packet) SelectiveACK() ([]byte, bool) {
	for _, extension := range p.Extensions {
		if extension.Type == SelectiveACKExtension {
			return cloneBytes(extension.Data), true
		}
	}
	return nil, false
}

// AckedBySelectiveACK reports whether seq is represented by this packet's
// selective ACK mask. The first bit acknowledges AckNr+2; bit zero of each
// byte represents the lower sequence number, as required by BEP 29.
func (p Packet) AckedBySelectiveACK(seq Sequence) bool {
	mask, ok := p.SelectiveACK()
	if !ok {
		return false
	}
	distance, ok := SequenceDistance(p.AckNr, seq)
	if !ok || distance < 2 {
		return false
	}
	bit := uint32(distance - 2)
	if bit >= uint32(len(mask))*8 {
		return false
	}
	return mask[bit/8]&(1<<uint(bit%8)) != 0
}

// SelectiveACKSequences expands the mask into acknowledged sequence numbers.
// Bits beyond the unambiguous half-ring are ignored.
func (p Packet) SelectiveACKSequences() []Sequence {
	mask, ok := p.SelectiveACK()
	if !ok {
		return nil
	}
	sequences := make([]Sequence, 0)
	for byteIndex, value := range mask {
		for bitIndex := 0; bitIndex < 8; bitIndex++ {
			if value&(1<<uint(bitIndex)) == 0 {
				continue
			}
			offset := uint32(byteIndex*8 + bitIndex + 2)
			if offset >= sequenceHalfRange {
				return sequences
			}
			sequence, _ := p.AckNr.Advance(offset)
			sequences = append(sequences, sequence)
		}
	}
	return sequences
}

func packetLength(p Packet) (int, bool) {
	length := int64(HeaderBytes) + int64(len(p.Payload))
	for _, extension := range p.Extensions {
		length += 2 + int64(len(extension.Data))
		if length > int64(limits.DatagramBytes) {
			return 0, false
		}
	}
	if length < 0 || length > int64(limits.DatagramBytes) {
		return 0, false
	}
	return int(length), true
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	copyOf := make([]byte, len(data))
	copy(copyOf, data)
	return copyOf
}
