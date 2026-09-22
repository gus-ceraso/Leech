package peer

import (
	"bytes"
	"io"
	"net"
)

const (
	// ProtocolName is the fixed BitTorrent v1 peer protocol name.
	ProtocolName = "BitTorrent protocol"
	// HandshakeBytes is the encoded size of a v1 handshake.
	HandshakeBytes = 1 + len(ProtocolName) + 8 + 20 + 20
	// FastExtensionBit is the BEP 6 bit in the final reserved byte.
	FastExtensionBit byte = 0x04
)

// Handshake is the decoded fixed BitTorrent peer handshake.
//
// The reserved bytes are retained verbatim. Extension ownership belongs to
// the caller, while FastNegotiated provides the only bit interpreted by this
// package.
type Handshake struct {
	InfoHash [20]byte
	PeerID   [20]byte
	Reserved [8]byte
}

// Fast reports whether this handshake advertises BEP 6.
func (h Handshake) Fast() bool { return h.Reserved[7]&FastExtensionBit != 0 }

// FastNegotiated reports whether both sides advertised the Fast extension.
func FastNegotiated(local, remote [8]byte) bool {
	return local[7]&FastExtensionBit != 0 && remote[7]&FastExtensionBit != 0
}

// ReadHandshake reads and validates one fixed-size handshake. A nil expected
// hash or peer ID disables that corresponding equality check.
func ReadHandshake(conn net.Conn, expectedInfoHash, expectedPeerID *[20]byte) (Handshake, error) {
	var wire [HandshakeBytes]byte
	if _, err := io.ReadFull(conn, wire[:]); err != nil {
		return Handshake{}, disconnectError("read handshake", err)
	}
	if wire[0] != byte(len(ProtocolName)) || !bytes.Equal(wire[1:1+len(ProtocolName)], []byte(ProtocolName)) {
		return Handshake{}, protocolError("read handshake", "invalid protocol name")
	}

	var result Handshake
	copy(result.Reserved[:], wire[1+len(ProtocolName):1+len(ProtocolName)+8])
	copy(result.InfoHash[:], wire[1+len(ProtocolName)+8:1+len(ProtocolName)+8+20])
	copy(result.PeerID[:], wire[1+len(ProtocolName)+8+20:])
	if expectedInfoHash != nil && result.InfoHash != *expectedInfoHash {
		return Handshake{}, protocolError("read handshake", "info hash does not match")
	}
	if expectedPeerID != nil && result.PeerID != *expectedPeerID {
		return Handshake{}, protocolError("read handshake", "peer ID does not match")
	}
	return result, nil
}

// WriteHandshake writes one fixed-size BitTorrent v1 handshake. The caller
// controls reserved bits so BEP 10 and later extension owners can add their
// negotiated bits without changing the wire format here.
func WriteHandshake(conn net.Conn, infoHash, peerID [20]byte, reserved [8]byte) error {
	var wire [HandshakeBytes]byte
	wire[0] = byte(len(ProtocolName))
	copy(wire[1:], ProtocolName)
	copy(wire[1+len(ProtocolName):], reserved[:])
	copy(wire[1+len(ProtocolName)+8:], infoHash[:])
	copy(wire[1+len(ProtocolName)+8+20:], peerID[:])
	if err := writeAll(conn, wire[:]); err != nil {
		return disconnectError("write handshake", err)
	}
	return nil
}

// WriteFastHaveNone sends the required initial BEP 6 availability message
// after both handshakes have completed. It is a no-op unless both sides
// advertised Fast. No other initial availability message is produced here.
func WriteFastHaveNone(conn net.Conn, localReserved, remoteReserved [8]byte) error {
	if !FastNegotiated(localReserved, remoteReserved) {
		return nil
	}
	return WriteMessage(conn, Message{ID: HaveNoneID})
}

// WriteInitialAvailability is the handshake-oriented form of
// WriteFastHaveNone. Call it after the local and remote handshakes are known.
func WriteInitialAvailability(conn net.Conn, local, remote Handshake) error {
	return WriteFastHaveNone(conn, local.Reserved, remote.Reserved)
}
