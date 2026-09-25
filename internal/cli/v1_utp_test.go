package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
	"github.com/gus-ceraso/Leech/internal/utp"
)

func TestV1UTPWinnerCompletesCLITransfer(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	data := []byte("uTP path")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	trackerFixture := &v1Tracker{port: uint16(server.LocalAddr().(*net.UDPAddr).Port)}
	serverDone := make(chan error, 1)
	go func() { serverDone <- serveV1UTPPeer(server, [20]byte(infoHash), data) }()
	config := session.RunConfig{
		HTTP:    trackerFixture,
		UTPDial: utp.DialContext,
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("scripted TCP loser")
		},
		UTPHeadStart: 250 * time.Millisecond,
		Identity:     tracker.Identity{PeerID: [20]byte{0x51, 0x33}, Port: 49152},
		CacheRoot:    filepath.Join(t.TempDir(), "cache"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	opts := parseV1Options(t, "--output", output, torrentPath)
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config); err != nil {
		select {
		case serverErr := <-serverDone:
			t.Fatalf("CLI uTP transfer: %v; fixture: %v", err, serverErr)
		default:
			t.Fatalf("CLI uTP transfer: %v; server still waiting", err)
		}
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("uTP peer fixture: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("uTP peer fixture did not finish")
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("uTP output=%q err=%v", got, err)
	}
	urls, requests := trackerFixture.snapshot()
	assertV1TrackerTrace(t, urls, requests, []string{torrent.DefaultTracker}, int64(len(data)), false)
}

func serveV1UTPPeer(socket *net.UDPConn, infoHash [20]byte, payload []byte) error {
	_ = socket.SetReadDeadline(time.Now().Add(6 * time.Second))
	syn, address, err := readV1UTPPacket(socket)
	if err != nil {
		return fmt.Errorf("read SYN: %w", err)
	}
	if syn.Type != utp.Syn {
		return fmt.Errorf("first uTP packet type %v, want SYN", syn.Type)
	}
	recvID := syn.ConnectionID
	serverSeq := utp.Sequence(700)
	if err := writeV1UTPPacket(socket, address, utp.Packet{Type: utp.State, ConnectionID: recvID, SeqNr: serverSeq, AckNr: syn.SeqNr, WindowSize: 4 << 20}); err != nil {
		return err
	}
	handshakePacket, _, err := readV1UTPPacket(socket)
	if err != nil {
		return fmt.Errorf("read BEP3 handshake: %w", err)
	}
	if handshakePacket.Type != utp.Data || !validV1PeerHandshake(handshakePacket.Payload, infoHash) {
		return errors.New("invalid BEP3 handshake over uTP")
	}
	serverID := [20]byte{0x73, 0x31}
	remoteHandshake := v1BEP3Handshake(infoHash, serverID, peer.FastExtensionBit)
	serverSeq++
	clientAck := handshakePacket.SeqNr
	if err := writeV1UTPPacket(socket, address, utp.Packet{Type: utp.Data, ConnectionID: recvID, SeqNr: serverSeq, AckNr: clientAck, WindowSize: 4 << 20, Payload: remoteHandshake}); err != nil {
		return err
	}
	peerWire := append(v1RawMessage(peer.BitfieldID, []byte{0x80}), v1RawMessage(peer.UnchokeID, nil)...)
	var stream []byte
	for {
		packet, source, err := readV1UTPPacket(socket)
		if err != nil {
			return fmt.Errorf("read uTP peer data: %w", err)
		}
		if !sameV1UDPAddress(source, address) {
			continue
		}
		switch packet.Type {
		case utp.Reset, utp.Fin:
			return nil
		case utp.State:
			continue
		case utp.Data:
			clientAck = packet.SeqNr
			if err := writeV1UTPPacket(socket, address, utp.Packet{Type: utp.State, ConnectionID: recvID, SeqNr: serverSeq - 1, AckNr: clientAck, WindowSize: 4 << 20}); err != nil {
				return err
			}
			stream = append(stream, packet.Payload...)
			messages, rest, err := takeV1PeerFrames(stream)
			if err != nil {
				return err
			}
			stream = rest
			for _, message := range messages {
				if message.ID == peer.RequestID {
					if len(message.Payload) != 12 {
						return errors.New("invalid uTP piece request")
					}
					index := binary.BigEndian.Uint32(message.Payload[:4])
					begin := binary.BigEndian.Uint32(message.Payload[4:8])
					length := binary.BigEndian.Uint32(message.Payload[8:])
					if index != 0 || uint64(begin)+uint64(length) > uint64(len(payload)) || length == 0 {
						return fmt.Errorf("invalid uTP piece request index=%d begin=%d length=%d", index, begin, length)
					}
					piece := make([]byte, 8+int(length))
					binary.BigEndian.PutUint32(piece[:4], index)
					binary.BigEndian.PutUint32(piece[4:8], begin)
					copy(piece[8:], payload[begin:begin+length])
					return writeV1UTPPacket(socket, address, utp.Packet{Type: utp.Data, ConnectionID: recvID, SeqNr: serverSeq, AckNr: clientAck, WindowSize: 4 << 20, Payload: v1RawMessage(peer.PieceID, piece)})
				}
			}
			if len(peerWire) != 0 {
				serverSeq++
				if err := writeV1UTPPacket(socket, address, utp.Packet{Type: utp.Data, ConnectionID: recvID, SeqNr: serverSeq, AckNr: clientAck, WindowSize: 4 << 20, Payload: peerWire}); err != nil {
					return err
				}
				serverSeq++
				peerWire = nil
			}
		}
	}
}

func readV1UTPPacket(socket *net.UDPConn) (utp.Packet, *net.UDPAddr, error) {
	buffer := make([]byte, 64<<10)
	n, address, err := socket.ReadFromUDP(buffer)
	if err != nil {
		return utp.Packet{}, nil, err
	}
	packet, err := utp.ParsePacket(buffer[:n])
	return packet, address, err
}

func writeV1UTPPacket(socket *net.UDPConn, address *net.UDPAddr, packet utp.Packet) error {
	wire, err := utp.EncodePacket(packet)
	if err != nil {
		return err
	}
	_, err = socket.WriteToUDP(wire, address)
	return err
}

func sameV1UDPAddress(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.IP.Equal(b.IP)
}

func validV1PeerHandshake(wire []byte, infoHash [20]byte) bool {
	if len(wire) != peer.HandshakeBytes || wire[0] != byte(len(peer.ProtocolName)) || !bytes.Equal(wire[1:1+len(peer.ProtocolName)], []byte(peer.ProtocolName)) {
		return false
	}
	start := 1 + len(peer.ProtocolName) + 8
	return bytes.Equal(wire[start:start+20], infoHash[:])
}

func v1BEP3Handshake(infoHash, peerID [20]byte, fast byte) []byte {
	wire := make([]byte, peer.HandshakeBytes)
	wire[0] = byte(len(peer.ProtocolName))
	copy(wire[1:], peer.ProtocolName)
	wire[1+len(peer.ProtocolName)+7] = fast
	copy(wire[1+len(peer.ProtocolName)+8:], infoHash[:])
	copy(wire[1+len(peer.ProtocolName)+8+20:], peerID[:])
	return wire
}

func takeV1PeerFrames(stream []byte) ([]peer.Message, []byte, error) {
	var messages []peer.Message
	for len(stream) >= 4 {
		length := binary.BigEndian.Uint32(stream[:4])
		if length == 0 {
			messages = append(messages, peer.Message{KeepAlive: true})
			stream = stream[4:]
			continue
		}
		if length > 1<<20 {
			return nil, nil, fmt.Errorf("fixture peer frame length %d exceeds bound", length)
		}
		if uint32(len(stream)-4) < length {
			break
		}
		messages = append(messages, peer.Message{ID: stream[4], Payload: append([]byte(nil), stream[5:4+length]...)})
		stream = stream[4+length:]
	}
	return messages, stream, nil
}
