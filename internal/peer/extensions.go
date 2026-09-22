package peer

import (
	"errors"
	"fmt"
	"net"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

// ExtendedID is the fixed BEP 10 peer-wire message identifier. Extended
// messages are deliberately not accepted by EncodeOutbound: callers must use
// the restricted handshake and metadata encoders in this file and metadata.go.
const ExtendedID byte = 20

// ExtendedMessageID is an explicit alias for callers that name the core
// message before its extension payload.
const ExtendedMessageID = ExtendedID

const (
	ExtensionHandshakeID byte = 0
	UtMetadataExtension       = "ut_metadata"
	UtMetadataName            = UtMetadataExtension

	defaultUtMetadataID byte = 1

	// The peer frame bound is the outer safety limit. These smaller limits keep
	// extension maps and headers cheap even when a peer sends an otherwise
	// valid, near-maximum frame.
	maxExtensionHandshakeBytes = 64 << 10
	maxExtensionEntries        = 256
	maxExtensionNameBytes      = 128
)

var (
	ErrExtensionMessage = errors.New("invalid peer extension message")
	ErrExtensionState   = errors.New("invalid peer extension state")
)

// ExtensionHandshake is the supported portion of one BEP 10 handshake.
// Extensions contains only names understood by Leech. Unknown names are
// parsed and discarded, as required by BEP 10. MetadataSize is optional and
// reports the peer's advertised BEP 9 size when present.
type ExtensionHandshake struct {
	Extensions      map[string]byte
	MetadataSize    int64
	HasMetadataSize bool
}

// ExtensionState owns one connection's directional BEP 10 maps. localIDs
// are the IDs Leech advertises and therefore dispatches on receive. remoteIDs
// are the IDs the peer advertised and therefore the only IDs used for sends.
// All methods are coordinator-owned and are not concurrency-safe.
type ExtensionState struct {
	localID         byte
	remoteIDs       map[string]byte
	metadataSize    int64
	hasMetadataSize bool
}

// NewExtensionState creates the standard Leech extension map. ID 1 is local
// to this connection and is used only to dispatch received ut_metadata
// messages.
func NewExtensionState() *ExtensionState {
	return NewExtensionStateWithLocalID(defaultUtMetadataID)
}

// NewExtensionStateWithLocalID is useful for deterministic directionality
// tests and for a caller that assigns a different local ID. ID zero is the
// reserved BEP 10 handshake ID and falls back to the standard ID.
func NewExtensionStateWithLocalID(id byte) *ExtensionState {
	if id == ExtensionHandshakeID {
		id = defaultUtMetadataID
	}
	return &ExtensionState{localID: id, remoteIDs: make(map[string]byte)}
}

// LocalExtensionID returns the ID Leech advertised for name.
func (s *ExtensionState) LocalExtensionID(name string) (byte, bool) {
	if s == nil || name != UtMetadataExtension {
		return 0, false
	}
	return s.localID, true
}

// RemoteExtensionID returns the latest usable ID advertised by the peer.
func (s *ExtensionState) RemoteExtensionID(name string) (byte, bool) {
	if s == nil {
		return 0, false
	}
	id, ok := s.remoteIDs[name]
	return id, ok && id != ExtensionHandshakeID
}

// RemoteMetadataSize returns the latest optional BEP 9 metadata size.
func (s *ExtensionState) RemoteMetadataSize() (int64, bool) {
	if s == nil {
		return 0, false
	}
	return s.metadataSize, s.hasMetadataSize
}

// EncodeHandshake builds Leech's BEP 10 handshake as a complete peer-wire
// frame. It advertises only ut_metadata, which is needed for a peer to send
// metadata to Leech. There is intentionally no metadata data encoder.
func (s *ExtensionState) EncodeHandshake() ([]byte, error) {
	if s == nil || s.localID == ExtensionHandshakeID {
		return nil, fmt.Errorf("%w: missing local extension ID", ErrExtensionState)
	}
	return EncodeExtensionHandshake(s.localID)
}

// EncodeExtensionHandshake builds a complete BEP 10 handshake frame for one
// local ut_metadata ID. It cannot encode arbitrary extension payloads.
func EncodeExtensionHandshake(localID byte) ([]byte, error) {
	if localID == ExtensionHandshakeID {
		return nil, fmt.Errorf("%w: extension handshake ID is reserved", ErrInvalidMessage)
	}
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("m"), Value: bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
			{Key: []byte(UtMetadataExtension), Value: bencode.Value{Type: bencode.Integer, Int: int64(localID)}},
		}}},
	}}
	body, err := bencode.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode handshake: %v", ErrInvalidMessage, err)
	}
	return encodeExtensionFrame(ExtensionHandshakeID, body)
}

// ApplyHandshake applies the bencoded handshake body (without the BEP 10
// extended-message ID). Updates are additive: entries omitted by a repeated
// handshake retain their previous values, while a zero value disables one
// named extension.
func (s *ExtensionState) ApplyHandshake(body []byte) error {
	if s == nil {
		return fmt.Errorf("%w: nil state", ErrExtensionState)
	}
	handshake, err := ParseExtensionHandshake(body)
	if err != nil {
		return err
	}
	for name, id := range handshake.Extensions {
		if id == ExtensionHandshakeID {
			delete(s.remoteIDs, name)
		} else if name == UtMetadataExtension {
			s.remoteIDs[name] = id
		}
	}
	if handshake.HasMetadataSize {
		s.metadataSize = handshake.MetadataSize
		s.hasMetadataSize = true
	}
	return nil
}

// ParseExtensionHandshake parses one BEP 10 handshake body. Its result is
// detached from the input, so callers may reuse the wire buffer.
func ParseExtensionHandshake(body []byte) (ExtensionHandshake, error) {
	if len(body) == 0 || len(body) > maxExtensionHandshakeBytes {
		return ExtensionHandshake{}, extensionUnsupported("handshake size is outside the supported bound")
	}
	bound := bencode.DefaultLimits()
	bound.MaxBytes = maxExtensionHandshakeBytes
	bound.MaxValues = maxExtensionEntries * 2
	bound.MaxDictionaryEntries = maxExtensionEntries
	bound.MaxContainerEntries = maxExtensionEntries
	bound.MaxDepth = 8
	root, err := bencode.DecodeWithLimits(body, bound)
	if err != nil {
		return ExtensionHandshake{}, extensionProtocol(fmt.Sprintf("malformed handshake: %v", err))
	}
	if root.Type != bencode.Dictionary {
		return ExtensionHandshake{}, extensionProtocol("handshake is not a dictionary")
	}
	result := ExtensionHandshake{Extensions: make(map[string]byte)}
	if m, ok := root.Lookup("m"); ok {
		if m.Type != bencode.Dictionary {
			return ExtensionHandshake{}, extensionProtocol("extension map is not a dictionary")
		}
		if len(m.Dict) > maxExtensionEntries {
			return ExtensionHandshake{}, extensionProtocol("extension map has too many entries")
		}
		for _, entry := range m.Dict {
			if len(entry.Key) == 0 || len(entry.Key) > maxExtensionNameBytes {
				return ExtensionHandshake{}, extensionProtocol("extension name is outside the supported bound")
			}
			if entry.Value.Type != bencode.Integer || entry.Value.Int < 0 || entry.Value.Int > 255 {
				return ExtensionHandshake{}, extensionProtocol("extension ID is not a byte")
			}
			// Keep only known names. The map itself is bounded before this loop,
			// so an arbitrary set of unknown names cannot grow local state.
			if string(entry.Key) == UtMetadataExtension {
				result.Extensions[UtMetadataExtension] = byte(entry.Value.Int)
			}
		}
	}
	if size, ok := root.Lookup("metadata_size"); ok {
		if size.Type != bencode.Integer || size.Int <= 0 {
			return ExtensionHandshake{}, extensionProtocol("metadata_size is outside the supported bound")
		}
		if size.Int > int64(limits.MetainfoBytes) {
			return ExtensionHandshake{}, extensionUnsupported("metadata_size is outside the supported bound")
		}
		result.MetadataSize = size.Int
		result.HasMetadataSize = true
	}
	return result, nil
}

// ExtensionEvent is a bounded dispatch result. Payload excludes the one-byte
// BEP 10 extension ID and is owned by the caller. Response is populated only
// for one admissible metadata request and is a restricted reject frame.
type ExtensionEvent struct {
	Name     string
	ID       byte
	Payload  []byte
	Ignored  bool
	Metadata *MetadataMessage
	Response []byte
}

// ApplyMessage dispatches a decoded extended peer-wire frame. Unknown IDs and
// unknown extension names are ignored after ReadMessage has enforced the
// outer frame bound. A metadata request is parsed and receives exactly one
// reject only while the peer's remote ut_metadata ID is usable.
func (s *ExtensionState) ApplyMessage(message Message) (ExtensionEvent, error) {
	if s == nil {
		return ExtensionEvent{}, fmt.Errorf("%w: nil state", ErrExtensionState)
	}
	if message.KeepAlive || message.ID != ExtendedID || len(message.Payload) == 0 {
		return ExtensionEvent{}, extensionProtocol("not a bounded extended message")
	}
	extensionID := message.Payload[0]
	if extensionID == ExtensionHandshakeID {
		if err := s.ApplyHandshake(message.Payload[1:]); err != nil {
			return ExtensionEvent{}, err
		}
		return ExtensionEvent{ID: ExtensionHandshakeID, Name: "handshake"}, nil
	}
	if extensionID != s.localID {
		return ExtensionEvent{ID: extensionID, Ignored: true}, nil
	}
	event := ExtensionEvent{ID: extensionID, Name: UtMetadataExtension, Payload: message.Payload[1:]}
	if len(event.Payload) == 0 {
		return ExtensionEvent{}, extensionProtocol("extension payload is empty")
	}
	metadata, err := ParseMetadataMessage(event.Payload)
	if err != nil {
		return ExtensionEvent{}, err
	}
	event.Metadata = &metadata
	if metadata.Type == MetadataRequest {
		if remoteID, ok := s.RemoteExtensionID(UtMetadataExtension); ok {
			event.Response, err = EncodeMetadataReject(remoteID, metadata.Piece)
			if err != nil {
				return ExtensionEvent{}, err
			}
		}
	}
	return event, nil
}

// ApplyExtensionMessage is a descriptive alias for ApplyMessage.
func (s *ExtensionState) ApplyExtensionMessage(message Message) (ExtensionEvent, error) {
	return s.ApplyMessage(message)
}

// WriteHandshake writes the restricted local BEP 10 handshake frame.
func (s *ExtensionState) WriteHandshake(conn net.Conn) error {
	wire, err := s.EncodeHandshake()
	if err != nil {
		return err
	}
	if err := writeAll(conn, wire); err != nil {
		return disconnectError("write extension handshake", err)
	}
	return nil
}

func encodeExtensionFrame(extensionID byte, body []byte) ([]byte, error) {
	if len(body)+2 > MaxPeerFrameBytes {
		return nil, fmt.Errorf("%w: extension frame exceeds supported bounds", ErrInvalidMessage)
	}
	wire := make([]byte, 4+2+len(body))
	// The extension frame length includes the core ID and extension ID.
	putUint32(wire[:4], uint32(2+len(body)))
	wire[4] = ExtendedID
	wire[5] = extensionID
	copy(wire[6:], body)
	return wire, nil
}

func putUint32(dst []byte, value uint32) {
	dst[0] = byte(value >> 24)
	dst[1] = byte(value >> 16)
	dst[2] = byte(value >> 8)
	dst[3] = byte(value)
}

func extensionProtocol(reason string) error {
	return protocolError("peer extension", reason)
}

func extensionUnsupported(reason string) error {
	return unsupportedError("peer extension", reason)
}
