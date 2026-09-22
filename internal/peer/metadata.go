package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

// Metadata messages are the BEP 9 message types carried inside ut_metadata.
type MetadataMessageType uint8

const (
	MetadataRequest MetadataMessageType = iota
	MetadataData
	MetadataReject
	MetadataUnknown MetadataMessageType = 255
)

var (
	ErrMetadataMessage = errors.New("invalid peer metadata message")
	ErrMetadataState   = errors.New("invalid peer metadata state")
)

const maxMetadataHeaderBytes = 4 << 10

// MetadataMessage is one parsed BEP 9 body. Block is populated only for data
// messages and is exactly one 16 KiB block, or the shorter final block.
type MetadataMessage struct {
	Type      MetadataMessageType
	Piece     uint32
	TotalSize int64
	Block     []byte
}

// ParseMetadataMessage parses a BEP 9 body without the BEP 10 extension ID.
// Requests and rejects must contain exactly their two defined dictionary
// fields. Data carries an exact binary block after its bounded bencoded
// header. Unknown message types are returned as MetadataUnknown so callers can
// ignore them for forward compatibility.
func ParseMetadataMessage(body []byte) (MetadataMessage, error) {
	if len(body) == 0 {
		return MetadataMessage{}, metadataProtocol("empty metadata message")
	}
	bound := bencode.DefaultLimits()
	bound.MaxBytes = MaxPeerFrameBytes
	bound.MaxValues = 16
	bound.MaxDictionaryEntries = 8
	bound.MaxContainerEntries = 8
	bound.MaxDepth = 4
	header, consumed, err := bencode.DecodePrefixWithLimits(body, bound)
	if err != nil {
		return MetadataMessage{}, metadataProtocol(fmt.Sprintf("malformed metadata header: %v", err))
	}
	if consumed > maxMetadataHeaderBytes {
		return MetadataMessage{}, metadataProtocol("metadata header exceeds the supported bound")
	}
	if header.Type != bencode.Dictionary {
		return MetadataMessage{}, metadataProtocol("metadata header is not a dictionary")
	}
	msgType, ok := header.Lookup("msg_type")
	if !ok || msgType.Type != bencode.Integer || msgType.Int < 0 || msgType.Int > 255 {
		return MetadataMessage{}, metadataProtocol("metadata msg_type is missing or invalid")
	}
	message := MetadataMessage{Type: MetadataMessageType(msgType.Int)}
	if message.Type != MetadataRequest && message.Type != MetadataData && message.Type != MetadataReject {
		// BEP 9 requires unknown message types to be ignored. The outer peer
		// frame already bounds the message, so no interpretation of optional
		// fields or binary data is needed here.
		message.Type = MetadataUnknown
		return message, nil
	}
	piece, ok := header.Lookup("piece")
	if !ok || piece.Type != bencode.Integer || piece.Int < 0 || piece.Int > int64(^uint32(0)) {
		return MetadataMessage{}, metadataProtocol("metadata piece is missing or invalid")
	}
	message.Piece = uint32(piece.Int)
	switch message.Type {
	case MetadataRequest, MetadataReject:
		if len(header.Dict) != 2 || consumed != len(body) {
			return MetadataMessage{}, metadataProtocol("metadata control message has extra data")
		}
		return message, nil
	case MetadataData:
		total, ok := header.Lookup("total_size")
		if !ok || total.Type != bencode.Integer || total.Int <= 0 {
			return MetadataMessage{}, metadataProtocol("metadata total_size is missing or invalid")
		}
		if total.Int > int64(limits.MetainfoBytes) {
			return MetadataMessage{}, metadataUnsupported("metadata total_size is outside the supported bound")
		}
		if len(header.Dict) != 3 {
			return MetadataMessage{}, metadataProtocol("metadata data header has extra fields")
		}
		message.TotalSize = total.Int
		block := body[consumed:]
		want, ok := metadataBlockLength(message.TotalSize, message.Piece)
		if !ok {
			return MetadataMessage{}, metadataProtocol("metadata piece is outside total_size")
		}
		if len(block) != want {
			return MetadataMessage{}, metadataProtocol(fmt.Sprintf("metadata block length %d, want %d", len(block), want))
		}
		message.Block = append([]byte(nil), block...)
		return message, nil
	default:
		return MetadataMessage{Type: MetadataUnknown}, nil
	}
}

// ParseMetadataData is a data-only helper. expectedTotalSize may be zero when
// the first block establishes the size; otherwise it must match total_size.
func ParseMetadataData(body []byte, expectedTotalSize int64) (MetadataMessage, error) {
	message, err := ParseMetadataMessage(body)
	if err != nil {
		return MetadataMessage{}, err
	}
	if message.Type != MetadataData {
		return MetadataMessage{}, metadataProtocol("metadata message is not data")
	}
	if expectedTotalSize != 0 && message.TotalSize != expectedTotalSize {
		return MetadataMessage{}, metadataProtocol("metadata total_size changed on one connection")
	}
	return message, nil
}

// ParseMetadataControl parses one request or reject and rejects data blocks.
func ParseMetadataControl(body []byte) (MetadataMessage, error) {
	message, err := ParseMetadataMessage(body)
	if err != nil {
		return MetadataMessage{}, err
	}
	if message.Type != MetadataRequest && message.Type != MetadataReject {
		return MetadataMessage{}, metadataProtocol("metadata message is not control")
	}
	return message, nil
}

// EncodeMetadataRequest builds a complete peer-wire metadata request using
// the remote peer's advertised extension ID. It is the only request encoder;
// no metadata-data encoder exists in this package.
func EncodeMetadataRequest(remoteID byte, piece uint32) ([]byte, error) {
	return encodeMetadataControl(remoteID, MetadataRequest, piece)
}

// EncodeMetadataReject builds a complete peer-wire metadata reject using the
// remote peer's advertised extension ID. It never reads storage.
func EncodeMetadataReject(remoteID byte, piece uint32) ([]byte, error) {
	return encodeMetadataControl(remoteID, MetadataReject, piece)
}

func encodeMetadataControl(remoteID byte, messageType MetadataMessageType, piece uint32) ([]byte, error) {
	if remoteID == ExtensionHandshakeID || messageType > MetadataReject {
		return nil, fmt.Errorf("%w: unusable metadata extension ID or message type", ErrInvalidMessage)
	}
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("msg_type"), Value: bencode.Value{Type: bencode.Integer, Int: int64(messageType)}},
		{Key: []byte("piece"), Value: bencode.Value{Type: bencode.Integer, Int: int64(piece)}},
	}}
	body, err := bencode.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode metadata control: %v", ErrInvalidMessage, err)
	}
	return encodeExtensionFrame(remoteID, body)
}

// WriteMetadataRequest writes one restricted metadata request.
func WriteMetadataRequest(conn interface{ Write([]byte) (int, error) }, remoteID byte, piece uint32) error {
	wire, err := EncodeMetadataRequest(remoteID, piece)
	if err != nil {
		return err
	}
	if err := writeBytes(conn, wire); err != nil {
		return disconnectError("write metadata request", err)
	}
	return nil
}

// WriteMetadataReject writes one restricted metadata reject and performs no
// storage access.
func WriteMetadataReject(conn interface{ Write([]byte) (int, error) }, remoteID byte, piece uint32) error {
	wire, err := EncodeMetadataReject(remoteID, piece)
	if err != nil {
		return err
	}
	if err := writeBytes(conn, wire); err != nil {
		return disconnectError("write metadata reject", err)
	}
	return nil
}

// MetadataAssembler collects validated blocks from one metadata supplier. It
// keeps at most the bounded BEP 9 metadata size and requires every block to
// advertise the same total_size.
type MetadataAssembler struct {
	totalSize int64
	blocks    [][]byte
	present   []bool
	count     int
}

// NewMetadataAssembler creates an assembler whose size is established by its
// first data block.
func NewMetadataAssembler() *MetadataAssembler { return &MetadataAssembler{} }

// NewMetadataAssemblerWithSize creates an assembler for a peer handshake's
// advertised metadata size. A zero size leaves the size open until Add.
func NewMetadataAssemblerWithSize(totalSize int64) (*MetadataAssembler, error) {
	a := &MetadataAssembler{}
	if totalSize != 0 {
		if err := a.setSize(totalSize); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Add parses and records one complete metadata data body.
func (a *MetadataAssembler) Add(body []byte) error {
	if a == nil {
		return fmt.Errorf("%w: nil assembler", ErrMetadataState)
	}
	message, err := ParseMetadataData(body, a.totalSize)
	if err != nil {
		return err
	}
	return a.AddMessage(message)
}

// AddMessage records an already validated data message. It accepts an exact
// duplicate block idempotently and rejects a conflicting duplicate.
func (a *MetadataAssembler) AddMessage(message MetadataMessage) error {
	if a == nil {
		return fmt.Errorf("%w: nil assembler", ErrMetadataState)
	}
	if message.Type != MetadataData || len(message.Block) == 0 {
		return metadataProtocol("assembler received a non-data message")
	}
	if a.totalSize == 0 {
		if err := a.setSize(message.TotalSize); err != nil {
			return err
		}
	} else if a.totalSize != message.TotalSize {
		return metadataProtocol("metadata total_size changed on one connection")
	}
	want, ok := metadataBlockLength(a.totalSize, message.Piece)
	if !ok || len(message.Block) != want {
		return metadataProtocol("metadata block does not match total_size")
	}
	if a.present[message.Piece] {
		if !bytes.Equal(a.blocks[message.Piece], message.Block) {
			return metadataProtocol("metadata block was supplied with conflicting bytes")
		}
		return nil
	}
	a.blocks[message.Piece] = append([]byte(nil), message.Block...)
	a.present[message.Piece] = true
	a.count++
	return nil
}

// TotalSize reports the established metadata size.
func (a *MetadataAssembler) TotalSize() (int64, bool) {
	if a == nil || a.totalSize == 0 {
		return 0, false
	}
	return a.totalSize, true
}

// Complete reports whether all blocks have arrived.
func (a *MetadataAssembler) Complete() bool {
	return a != nil && a.totalSize != 0 && a.count == len(a.blocks)
}

// Metadata returns the complete metadata bytes. The returned slice is a new
// bounded copy and is unavailable before all blocks arrive.
func (a *MetadataAssembler) Metadata() ([]byte, error) {
	if !a.Complete() {
		return nil, fmt.Errorf("%w: metadata is incomplete", ErrMetadataState)
	}
	result := make([]byte, int(a.totalSize))
	var offset int
	for _, block := range a.blocks {
		offset += copy(result[offset:], block)
	}
	return result, nil
}

func (a *MetadataAssembler) setSize(totalSize int64) error {
	if totalSize <= 0 || totalSize > int64(limits.MetainfoBytes) {
		return metadataProtocol("metadata total_size is outside the supported bound")
	}
	pieces64 := (totalSize + int64(limits.BlockBytes) - 1) / int64(limits.BlockBytes)
	if pieces64 <= 0 || pieces64 > int64(limits.MetainfoBytes/limits.BlockBytes+1) {
		return metadataProtocol("metadata block count is outside the supported bound")
	}
	a.totalSize = totalSize
	a.blocks = make([][]byte, int(pieces64))
	a.present = make([]bool, int(pieces64))
	return nil
}

func metadataBlockLength(totalSize int64, piece uint32) (int, bool) {
	if totalSize <= 0 || totalSize > int64(limits.MetainfoBytes) {
		return 0, false
	}
	pieces := (totalSize + int64(limits.BlockBytes) - 1) / int64(limits.BlockBytes)
	if int64(piece) >= pieces {
		return 0, false
	}
	remaining := totalSize - int64(piece)*int64(limits.BlockBytes)
	if remaining > int64(limits.BlockBytes) {
		remaining = int64(limits.BlockBytes)
	}
	return int(remaining), true
}

func metadataProtocol(reason string) error {
	return protocolError("peer metadata", reason)
}

func metadataUnsupported(reason string) error {
	return unsupportedError("peer metadata", reason)
}

func writeBytes(conn interface{ Write([]byte) (int, error) }, wire []byte) error {
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
