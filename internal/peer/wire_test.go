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
	unknown = append(unknown, 0, 0, 0, 1, ChokeID)
	conn := newChunkConn(unknown, 1)
	message, err := ReadMessage(conn)
	if err != nil || message.ID != 0x12 || len(message.Payload) != 0 {
		t.Fatalf("unknown frame = %#v, err %v", message, err)
	}
	message, err = ReadMessage(conn)
	if err != nil || message.ID != ChokeID || len(message.Payload) != 0 {
		t.Fatalf("frame following unknown ID = %#v, err %v", message, err)
	}
}

func TestUnknownCorePayloadIsDrainedWithoutRetention(t *testing.T) {
	payload := bytes.Repeat([]byte{0xa5}, MaxPeerFrameBytes-1)
	wire := make([]byte, 4+1+len(payload)+5)
	binary.BigEndian.PutUint32(wire[:4], uint32(1+len(payload)))
	wire[4] = 0x12
	copy(wire[5:], payload)
	copy(wire[5+len(payload):], []byte{0, 0, 0, 1, ChokeID})
	conn := newChunkConn(wire, 4096)
	message, err := ReadMessage(conn)
	if err != nil || message.ID != 0x12 || len(message.Payload) != 0 {
		t.Fatalf("large unknown frame = %#v, err %v", message, err)
	}
	message, err = ReadMessage(conn)
	if err != nil || message.ID != ChokeID {
		t.Fatalf("frame following large unknown ID = %#v, err %v", message, err)
	}
}

func TestExtendedMetadataDataRemainsAvailable(t *testing.T) {
	block := bytes.Repeat([]byte{0x5a}, limits.BlockBytes)
	body := metadataBody(t, MetadataData, 0, limits.BlockBytes, block)
	for _, extensionID := range []byte{1, 6} {
		wire := extensionFrame(extensionID, body)
		message, err := ReadMessageWithOptions(newChunkConn(wire, 7), ReadOptions{MetadataExtensionID: extensionID})
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != ExtendedID || len(message.Payload) != len(body)+1 || message.Payload[0] != extensionID {
			t.Fatalf("extended metadata frame = id %d, payload length %d", message.ID, len(message.Payload))
		}
		parsed, err := ParseMetadataData(message.Payload[1:], limits.BlockBytes)
		if err != nil || !bytes.Equal(parsed.Block, block) {
			t.Fatalf("parsed metadata block length %d, err %v", len(parsed.Block), err)
		}
	}
}

func TestExtendedHandshakeRemainsAvailable(t *testing.T) {
	body := extensionHandshakeBody(t, map[string]int64{"ut_metadata": 9}, 32769)
	message, err := ReadMessage(newChunkConn(extensionFrame(ExtensionHandshakeID, body), 3))
	if err != nil {
		t.Fatal(err)
	}
	state := NewExtensionState()
	if _, err := state.ApplyMessage(message); err != nil {
		t.Fatal(err)
	}
	if id, ok := state.RemoteExtensionID(UtMetadataExtension); !ok || id != 9 {
		t.Fatalf("remote ut_metadata ID = %d, enabled %t", id, ok)
	}
}

func TestValidFastAndBEP10WireSequenceReachesState(t *testing.T) {
	fast := ReadOptions{Fast: true, PieceCount: 8, ValidateIndices: true, PieceLength: 16 << 10}
	frames := [][]byte{
		{0, 0, 0, 2, BitfieldID, 0x80},
		{0, 0, 0, 5, AllowedFastID, 0, 0, 0, 0},
		{0, 0, 0, 1, HaveNoneID},
	}
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 8, PieceLength: 16 << 10, Fast: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		message, err := ReadMessageWithOptions(newChunkConn(frame, 2), fast)
		if err != nil {
			t.Fatalf("ReadMessageWithOptions(%x): %v", frame, err)
		}
		if _, err := state.ApplyMessage(message); err != nil {
			t.Fatalf("ApplyMessage(%x): %v", frame, err)
		}
	}
	if state.Availability(0) || !state.AllowedFast(0) || state.CanRequest(0) {
		t.Fatalf("Fast sequence state availability=%v allowed=%v canRequest=%v", state.Availability(0), state.AllowedFast(0), state.CanRequest(0))
	}

	// Canonical BEP 10 handshake advertising remote ut_metadata ID 9.
	body := []byte("d1:md11:ut_metadatai9ee13:metadata_sizei32769ee")
	frame := extensionFrame(ExtensionHandshakeID, body)
	message, err := ReadMessage(newChunkConn(frame, 3))
	if err != nil {
		t.Fatalf("ReadMessage(BEP 10): %v", err)
	}
	extensions := NewExtensionStateWithLocalID(7)
	if _, err := extensions.ApplyMessage(message); err != nil {
		t.Fatalf("ApplyMessage(BEP 10): %v", err)
	}
	if local, ok := extensions.LocalExtensionID(UtMetadataExtension); !ok || local != 7 {
		t.Fatalf("local ut_metadata ID = %d, %t", local, ok)
	}
	if remote, ok := extensions.RemoteExtensionID(UtMetadataExtension); !ok || remote != 9 {
		t.Fatalf("remote ut_metadata ID = %d, %t", remote, ok)
	}
	request, err := extensions.EncodeMetadataRequest(0)
	if err != nil || request[5] != 9 {
		t.Fatalf("metadata request = %x, %v; want remote ID 9", request, err)
	}
}

func TestUnknownExtendedPayloadIsDrainedWithoutRetention(t *testing.T) {
	body := bytes.Repeat([]byte{0x7b}, MaxPeerFrameBytes-2)
	wire := extensionFrame(99, body)
	wire = append(wire, 0, 0, 0, 1, ChokeID)
	conn := newChunkConn(wire, 4096)
	message, err := ReadMessage(conn)
	if err != nil || message.ID != ExtendedID || !bytes.Equal(message.Payload, []byte{99}) {
		t.Fatalf("unknown extension = %#v, err %v", message, err)
	}
	message, err = ReadMessage(conn)
	if err != nil || message.ID != ChokeID {
		t.Fatalf("frame following unknown extension = %#v, err %v", message, err)
	}
}

func extensionFrame(extensionID byte, body []byte) []byte {
	wire := make([]byte, 6+len(body))
	binary.BigEndian.PutUint32(wire[:4], uint32(2+len(body)))
	wire[4] = ExtendedID
	wire[5] = extensionID
	copy(wire[6:], body)
	return wire
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
	f.Add([]byte{0, 0, 0, 2, BitfieldID, 0x80})
	f.Add([]byte{0, 0, 0, 5, AllowedFastID, 0, 0, 0, 3})
	f.Add([]byte{0, 0, 0, 1, HaveNoneID})
	f.Add(extensionFrame(ExtensionHandshakeID, []byte("d1:md11:ut_metadatai9ee13:metadata_sizei32769ee")))
	f.Fuzz(func(t *testing.T, wire []byte) {
		_, _ = ReadMessage(newChunkConn(wire, 3))
	})
}

func FuzzReadHandshake(f *testing.F) {
	f.Add(make([]byte, HandshakeBytes))
	valid := &chunkConn{}
	if err := WriteHandshake(valid, [20]byte{1, 2, 3}, [20]byte{4, 5, 6}, [8]byte{7: FastExtensionBit}); err != nil {
		f.Fatal(err)
	}
	f.Add(valid.writes.Bytes())
	f.Fuzz(func(t *testing.T, wire []byte) {
		_, _ = ReadHandshake(newChunkConn(wire, 3), nil, nil)
	})
}
