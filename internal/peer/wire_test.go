package peer

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

type chunkConn struct {
	reader *bytes.Reader
	writes bytes.Buffer
	chunk  int
}

func newChunkConn(wire []byte, chunk int) *chunkConn {
	return &chunkConn{reader: bytes.NewReader(wire), chunk: chunk}
}

func (c *chunkConn) Read(p []byte) (int, error) {
	if c.chunk > 0 && len(p) > c.chunk {
		p = p[:c.chunk]
	}
	return c.reader.Read(p)
}
func (c *chunkConn) Write(p []byte) (int, error) {
	if c.chunk > 0 && len(p) > c.chunk {
		p = p[:c.chunk]
	}
	return c.writes.Write(p)
}
func (c *chunkConn) Close() error                       { return nil }
func (c *chunkConn) LocalAddr() net.Addr                { return chunkAddr("local") }
func (c *chunkConn) RemoteAddr() net.Addr               { return chunkAddr("remote") }
func (c *chunkConn) SetDeadline(_ time.Time) error      { return nil }
func (c *chunkConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *chunkConn) SetWriteDeadline(_ time.Time) error { return nil }

type chunkAddr string

func (a chunkAddr) Network() string { return "chunk" }
func (a chunkAddr) String() string  { return string(a) }

func TestHandshakeRoundTripFragmented(t *testing.T) {
	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i)
		peerID[i] = byte(20 - i)
	}
	reserved := [8]byte{7: FastExtensionBit}

	writer := &chunkConn{chunk: 3}
	if err := WriteHandshake(writer, infoHash, peerID, reserved); err != nil {
		t.Fatalf("WriteHandshake: %v", err)
	}
	if got := writer.writes.Len(); got != HandshakeBytes {
		t.Fatalf("handshake length = %d, want %d", got, HandshakeBytes)
	}
	reader := newChunkConn(writer.writes.Bytes(), 2)
	got, err := ReadHandshake(reader, &infoHash, &peerID)
	if err != nil {
		t.Fatalf("ReadHandshake: %v", err)
	}
	if got.InfoHash != infoHash || got.PeerID != peerID || got.Reserved != reserved || !got.Fast() {
		t.Fatalf("decoded handshake = %#v", got)
	}
}

func TestHandshakeErrorsAreClassified(t *testing.T) {
	short := newChunkConn([]byte("short"), 1)
	if _, err := ReadHandshake(short, nil, nil); !IsDisconnect(err) || ClassOf(err) != ClassDisconnect {
		t.Fatalf("short handshake error = %v, class %v", err, ClassOf(err))
	}
	bad := make([]byte, HandshakeBytes)
	bad[0] = 19
	copy(bad[1:], "not BitTorrent protocol")
	if _, err := ReadHandshake(newChunkConn(bad, 4), nil, nil); !IsProtocolViolation(err) {
		t.Fatalf("bad handshake error = %v", err)
	}
}

func TestInitialFastAvailabilityIsSoleMessage(t *testing.T) {
	local := [8]byte{7: FastExtensionBit}
	remote := [8]byte{7: FastExtensionBit}
	capture := &chunkConn{}
	if err := WriteFastHaveNone(capture, local, remote); err != nil {
		t.Fatalf("WriteFastHaveNone: %v", err)
	}
	want := []byte{0, 0, 0, 1, HaveNoneID}
	if !bytes.Equal(capture.writes.Bytes(), want) {
		t.Fatalf("Have None wire = %x, want %x", capture.writes.Bytes(), want)
	}
	capture = &chunkConn{}
	if err := WriteFastHaveNone(capture, local, [8]byte{}); err != nil {
		t.Fatalf("unnegotiated Fast: %v", err)
	}
	if capture.writes.Len() != 0 {
		t.Fatalf("unnegotiated Fast emitted %x", capture.writes.Bytes())
	}
}

func TestFrameVectorsAndFragmentedIO(t *testing.T) {
	request := EncodeRequest(3, 16<<10, 16<<10)
	wantRequest := []byte{0, 0, 0, 13, RequestID, 0, 0, 0, 3, 0, 0, 0x40, 0, 0, 0, 0x40, 0}
	if !bytes.Equal(request, wantRequest) {
		t.Fatalf("request wire = %x, want %x", request, wantRequest)
	}
	for _, vector := range [][]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 1, ChokeID},
		request,
	} {
		message, err := ReadMessage(newChunkConn(vector, 1))
		if err != nil {
			t.Fatalf("ReadMessage(%x): %v", vector, err)
		}
		if len(vector) == 4 {
			if !message.KeepAlive {
				t.Fatalf("keepalive decoded as %#v", message)
			}
		} else if message.KeepAlive {
			t.Fatalf("message decoded as keepalive: %#v", message)
		}
	}
}

func TestFrameErrorsAndUnknownIDs(t *testing.T) {
	oversized := []byte{0, 0x10, 0, 1}
	if _, err := ReadMessage(newChunkConn(oversized, 1)); !IsProtocolViolation(err) {
		t.Fatalf("oversized frame error = %v", err)
	}
	malformed := []byte{0, 0, 0, 2, ChokeID, 0}
	if _, err := ReadMessage(newChunkConn(malformed, 1)); !IsProtocolViolation(err) {
		t.Fatalf("malformed frame error = %v", err)
	}
	truncated := []byte{0, 0, 0, 5, HaveID, 0, 0}
	if _, err := ReadMessage(newChunkConn(truncated, 1)); !IsDisconnect(err) {
		t.Fatalf("truncated frame error = %v", err)
	}
	unknown := []byte{0, 0, 0, 3, 0x12, 0xaa, 0xbb}
	message, err := ReadMessage(newChunkConn(unknown, 1))
	if err != nil || message.ID != 0x12 || !bytes.Equal(message.Payload, []byte{0xaa, 0xbb}) {
		t.Fatalf("unknown frame = %#v, err %v", message, err)
	}
}

func TestPieceBlockBoundIsCheckedBeforeAllocation(t *testing.T) {
	frameLength := 1 + 8 + limits.BlockBytes + 1
	wire := make([]byte, 4+1+8)
	binary.BigEndian.PutUint32(wire[:4], uint32(frameLength))
	wire[4] = PieceID
	if _, err := ReadMessage(newChunkConn(wire, 1)); !IsProtocolViolation(err) {
		t.Fatalf("oversized piece error = %v", err)
	}
}

func TestNegotiatedValidation(t *testing.T) {
	haveNone := []byte{0, 0, 0, 1, HaveNoneID}
	if _, err := ReadMessageWithOptions(newChunkConn(haveNone, 1), ReadOptions{}); !IsProtocolViolation(err) {
		t.Fatalf("Have None without Fast = %v", err)
	}
	message, err := ReadMessageWithOptions(newChunkConn(haveNone, 1), ReadOptions{Fast: true})
	if err != nil || message.ID != HaveNoneID {
		t.Fatalf("Have None with Fast = %#v, err %v", message, err)
	}
	var payload [13]byte
	binary.BigEndian.PutUint32(payload[:4], 9)
	binary.BigEndian.PutUint32(payload[4:8], 0)
	binary.BigEndian.PutUint32(payload[8:], 16<<10)
	frame := append([]byte{0, 0, 0, 13, RequestID}, payload[1:]...)
	if _, err := ReadMessageWithOptions(newChunkConn(frame, 2), ReadOptions{ValidateIndices: true, PieceCount: 2}); !IsProtocolViolation(err) {
		t.Fatalf("out-of-range request = %v", err)
	}
}

func TestOutboundAllowlist(t *testing.T) {
	allowed := []Message{
		{ID: ChokeID}, {ID: InterestedID}, {ID: NotInterestedID},
		{ID: RequestID, Payload: blockPayload(0, 0, limits.BlockBytes)},
		{ID: CancelID, Payload: blockPayload(0, 0, limits.BlockBytes)},
		{ID: RejectRequestID, Payload: blockPayload(0, 0, limits.BlockBytes)},
		{ID: HaveNoneID}, {KeepAlive: true},
	}
	for _, message := range allowed {
		if _, err := EncodeOutbound(message); err != nil {
			t.Errorf("allowed ID %d rejected: %v", message.ID, err)
		}
	}
	for _, id := range []byte{UnchokeID, HaveID, BitfieldID, PieceID, HaveAllID, AllowedFastID, 20} {
		if _, err := EncodeOutbound(Message{ID: id}); !IsUnsupported(err) {
			t.Errorf("forbidden ID %d error = %v", id, err)
		}
	}
	if got := EncodeRequest(0, 0, limits.BlockBytes+1); got != nil {
		t.Fatalf("oversized request encoded as %x", got)
	}
}

func TestShortWritesAreJoined(t *testing.T) {
	capture := &chunkConn{chunk: 1}
	if err := WriteRequest(capture, 1, 2, 3); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	message, err := ReadMessage(newChunkConn(capture.writes.Bytes(), 1))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if message.ID != RequestID || len(message.Payload) != 12 {
		t.Fatalf("decoded short write = %#v", message)
	}
}

func FuzzReadMessage(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 1, ChokeID})
	f.Fuzz(func(t *testing.T, wire []byte) {
		_, _ = ReadMessage(newChunkConn(wire, 3))
	})
}

func FuzzReadHandshake(f *testing.F) {
	f.Add(make([]byte, HandshakeBytes))
	f.Fuzz(func(t *testing.T, wire []byte) {
		_, _ = ReadHandshake(newChunkConn(wire, 3), nil, nil)
	})
}
