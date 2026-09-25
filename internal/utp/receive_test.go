package utp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
)

func receiveData(seq Sequence, payload string) Packet {
	return Packet{Type: Data, SeqNr: seq, Payload: []byte(payload)}
}

func receiveFIN(seq Sequence) Packet {
	return Packet{Type: Fin, SeqNr: seq}
}

func TestReceiveOrderedReassemblyAndPartialRead(t *testing.T) {
	r := NewReceiveState(100)
	result := r.Receive(receiveData(101, "world"))
	if !result.Accepted || len(result.Actions) != 1 {
		t.Fatalf("out-of-order result = %+v", result)
	}
	action := result.Actions[0]
	if action.Kind != ActionSend || action.Packet.AckNr != 99 || action.Packet.WindowSize != uint32(limits.UTPBufferBytes-len("world")) {
		t.Fatalf("gap ACK = %+v", action)
	}
	mask, ok := action.Packet.SelectiveACK()
	if !ok || len(mask) != 4 || mask[0]&1 == 0 {
		t.Fatalf("gap SACK = %x, %v", mask, ok)
	}

	result = r.Receive(receiveData(100, "hello "))
	if !result.Accepted || r.AckNumber() != 101 || r.NextSequence() != 102 {
		t.Fatalf("in-order result = %+v, ack=%d next=%d", result, r.AckNumber(), r.NextSequence())
	}
	if _, ok := result.Actions[0].Packet.SelectiveACK(); ok {
		t.Fatal("SACK remained after the gap was filled")
	}

	buffer := make([]byte, 4)
	n, err := r.Read(buffer)
	if n != 4 || err != nil || string(buffer) != "hell" {
		t.Fatalf("partial read = %d, %q, %v", n, buffer, err)
	}
	n, err = r.Read(buffer)
	if n != 4 || err != nil || string(buffer[:n]) != "o wo" {
		t.Fatalf("second partial read = %d, %q, %v", n, buffer[:n], err)
	}
	n, err = r.Read(buffer)
	if n != 3 || err != nil || string(buffer[:n]) != "rld" {
		t.Fatalf("final data read = %d, %q, %v", n, buffer[:n], err)
	}
	if packets, bytes := r.Buffered(); packets != 0 || bytes != 0 || r.WindowSize() != uint32(limits.UTPBufferBytes) {
		t.Fatalf("after reads buffered=(%d,%d), window=%d", packets, bytes, r.WindowSize())
	}

	duplicate := r.Receive(receiveData(100, "hello "))
	if !duplicate.Duplicate || duplicate.Actions[0].Packet.AckNr != 101 {
		t.Fatalf("duplicate result = %+v", duplicate)
	}
}

func TestReceiveWraparoundAndSACK(t *testing.T) {
	r := NewReceiveState(0)
	result := r.Receive(receiveData(1, "b"))
	if !result.Accepted || result.Actions[0].Packet.AckNr != 0xffff {
		t.Fatalf("wrapped gap result = %+v", result)
	}
	if !result.Actions[0].Packet.AckedBySelectiveACK(1) {
		t.Fatal("wrapped SACK did not acknowledge sequence one")
	}
	result = r.Receive(receiveData(0, "a"))
	if !result.Accepted || r.AckNumber() != 1 {
		t.Fatalf("wrapped fill result = %+v, ack=%d", result, r.AckNumber())
	}
	got := make([]byte, 2)
	if n, err := r.Read(got); n != 2 || err != nil || string(got) != "ab" {
		t.Fatalf("wrapped stream = %d, %q, %v", n, got, err)
	}
}

func TestReceiveFINWaitsForMissingPackets(t *testing.T) {
	r := NewReceiveState(10)
	if result := r.Receive(receiveFIN(12)); !result.Accepted {
		t.Fatalf("FIN result = %+v", result)
	}
	if r.Finished() {
		t.Fatal("out-of-order FIN marked stream finished")
	}
	if result := r.Receive(receiveData(11, "b")); !result.Accepted {
		t.Fatalf("second gap result = %+v", result)
	}
	if result := r.Receive(receiveData(10, "a")); !result.Accepted || !r.Finished() {
		t.Fatalf("FIN fill result = %+v, finished=%v", result, r.Finished())
	}
	got := make([]byte, 2)
	if n, err := r.Read(got); n != 2 || err != nil || string(got) != "ab" {
		t.Fatalf("FIN stream = %d, %q, %v", n, got, err)
	}
	if n, err := r.Read(got); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("FIN EOF = %d, %v", n, err)
	}

	// Data after an accepted FIN is a protocol error, including when the FIN
	// was received before the missing packet that precedes it.
	r = NewReceiveState(20)
	if result := r.Receive(receiveFIN(22)); !result.Accepted {
		t.Fatalf("second FIN result = %+v", result)
	}
	result := r.Receive(receiveData(23, "bad"))
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionClose || !errors.Is(result.Actions[0].Err, ErrReceiveAfterFIN) {
		t.Fatalf("post-FIN result = %+v", result)
	}
}

func TestReceiveDropsDataPastFINWhenDataArrivesFirst(t *testing.T) {
	r, err := NewReceiveStateWithLimits(1, 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	if result := r.Receive(receiveData(2, "past-fin")); !result.Accepted {
		t.Fatalf("buffered post-FIN data = %+v", result)
	}
	if packets, bytes := r.Buffered(); packets != 1 || bytes != len("past-fin") {
		t.Fatalf("before FIN buffered=(%d,%d)", packets, bytes)
	}
	result := r.Receive(receiveFIN(1))
	if !result.Accepted || !r.Finished() {
		t.Fatalf("FIN result = %+v, finished=%v", result, r.Finished())
	}
	if packets, bytes := r.Buffered(); packets != 0 || bytes != 0 {
		t.Fatalf("post-FIN data was retained: buffered=(%d,%d)", packets, bytes)
	}
	if r.WindowSize() != 8 {
		t.Fatalf("window after dropping post-FIN data = %d", r.WindowSize())
	}
	if n, readErr := r.Read(make([]byte, 8)); n != 0 || !errors.Is(readErr, io.EOF) {
		t.Fatalf("post-FIN data reached application: n=%d err=%v", n, readErr)
	}
	if _, ok := result.Actions[0].Packet.SelectiveACK(); ok {
		t.Fatal("ACK retained a SACK for dropped post-FIN data")
	}
}

func TestReceiveFINBeforeDataAndDuplicateFIN(t *testing.T) {
	r := NewReceiveState(10)
	if result := r.Receive(receiveFIN(10)); !result.Accepted || !r.Finished() {
		t.Fatalf("in-order FIN = %+v, finished=%v", result, r.Finished())
	}
	duplicate := r.Receive(receiveFIN(10))
	if !duplicate.Duplicate || len(duplicate.Actions) != 1 || duplicate.Actions[0].Kind != ActionSend {
		t.Fatalf("duplicate FIN = %+v", duplicate)
	}
	postFIN := r.Receive(receiveData(11, "late"))
	if len(postFIN.Actions) != 1 || postFIN.Actions[0].Kind != ActionClose || !errors.Is(postFIN.Actions[0].Err, ErrReceiveAfterFIN) {
		t.Fatalf("DATA after FIN = %+v", postFIN)
	}
}

func TestReceiveDropsPostFINDataAcrossWraparound(t *testing.T) {
	r, err := NewReceiveStateWithLimits(0xffff, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if result := r.Receive(receiveData(0, "late")); !result.Accepted {
		t.Fatalf("wrapped post-FIN data = %+v", result)
	}
	result := r.Receive(receiveFIN(0xffff))
	if !result.Accepted || !r.Finished() {
		t.Fatalf("wrapped FIN = %+v, finished=%v", result, r.Finished())
	}
	if packets, bytes := r.Buffered(); packets != 0 || bytes != 0 {
		t.Fatalf("wrapped post-FIN data retained: buffered=(%d,%d)", packets, bytes)
	}
	if n, readErr := r.Read(make([]byte, 16)); n != 0 || !errors.Is(readErr, io.EOF) {
		t.Fatalf("wrapped post-FIN read = %d, %v", n, readErr)
	}
}

func TestReceiveWindowPressureAndReopening(t *testing.T) {
	r, err := NewReceiveStateWithLimits(1, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if result := r.Receive(receiveData(2, "bb")); !result.Accepted {
		t.Fatalf("first pending packet = %+v", result)
	}
	result := r.Receive(receiveData(3, "c"))
	if !result.WindowFull || result.Accepted {
		t.Fatalf("packet pressure result = %+v", result)
	}
	if packets, bytes := r.Buffered(); packets != 1 || bytes != 2 {
		t.Fatalf("packet pressure buffered=(%d,%d)", packets, bytes)
	}
	if r.WindowSize() != 0 {
		t.Fatalf("window under packet pressure = %d", r.WindowSize())
	}
	if result := r.Receive(receiveData(1, "a")); !result.Accepted || r.AckNumber() != 2 {
		t.Fatalf("gap fill result = %+v, ack=%d", result, r.AckNumber())
	}
	got := make([]byte, 3)
	if n, readErr := r.Read(got); n != 3 || readErr != nil || string(got) != "abb" {
		t.Fatalf("reopened stream = %d, %q, %v", n, got, readErr)
	}
	if r.WindowSize() != 3 {
		t.Fatalf("window after consume = %d", r.WindowSize())
	}
}

func TestReceiveResetAndCancellation(t *testing.T) {
	r := NewReceiveState(1)
	if result := r.Receive(receiveData(1, "data")); !result.Accepted {
		t.Fatalf("data before reset = %+v", result)
	}
	result := r.Receive(Packet{Type: Reset})
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionClose || !errors.Is(result.Actions[0].Err, ErrReceiveReset) {
		t.Fatalf("reset result = %+v", result)
	}
	got := make([]byte, 4)
	if n, err := r.Read(got); n != 4 || err != nil || string(got) != "data" {
		t.Fatalf("reset preserved bytes = %d, %q, %v", n, got, err)
	}
	if n, err := r.Read(got); n != 0 || !errors.Is(err, ErrReceiveReset) {
		t.Fatalf("reset read = %d, %v", n, err)
	}
	if result := r.Receive(receiveData(2, "ignored")); len(result.Actions) != 0 {
		t.Fatalf("receive after reset = %+v", result)
	}

	r = NewReceiveState(1)
	action := r.Cancel(context.Canceled)
	if action.Kind != ActionClose || !errors.Is(action.Err, context.Canceled) || !errors.Is(r.Err(), context.Canceled) {
		t.Fatalf("cancellation = %+v, err=%v", action, r.Err())
	}
	if n, err := r.Read(got); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation read = %d, %v", n, err)
	}

	r = NewReceiveState(1)
	action = r.Cancel(nil)
	if action.Kind != ActionClose || action.Err != nil {
		t.Fatalf("orderly cancellation action = %+v", action)
	}
	if n, err := r.Read(got); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("orderly cancellation read = %d, %v", n, err)
	}
}

func TestReceiveRejectsInvalidConnectedInput(t *testing.T) {
	r := NewReceiveState(1)
	result := r.Receive(Packet{Type: Syn})
	if len(result.Actions) != 1 || result.Actions[0].Kind != ActionClose || !errors.Is(result.Actions[0].Err, ErrUnexpectedSYN) {
		t.Fatalf("SYN result = %+v", result)
	}
	if got := r.Receive(receiveData(1, "data")); len(got.Actions) != 0 {
		t.Fatalf("state after invalid SYN = %+v", got)
	}
}

func TestReceiveCapsAndSACKBounds(t *testing.T) {
	if _, err := NewReceiveStateWithLimits(0, limits.UTPReorderPackets+1, 1); err == nil {
		t.Fatal("accepted packet cap above supported bound")
	}
	if _, err := NewReceiveStateWithLimits(0, 1, int(limits.UTPBufferBytes)+1); err == nil {
		t.Fatal("accepted byte cap above supported bound")
	}
	r := NewReceiveState(0)
	result := r.Receive(receiveData(Sequence(0xffff).Add(maxReceiveOffset), "x"))
	if !result.Accepted {
		t.Fatalf("maximum representable gap was rejected: %+v", result)
	}
	mask, ok := result.Actions[0].Packet.SelectiveACK()
	if !ok || len(mask) != maxSACKBytes || mask[maxSACKBytes-1]&(1<<7) == 0 {
		t.Fatalf("maximum SACK = %d bytes, %x", len(mask), mask)
	}
	if err := result.Actions[0].Packet.Validate(); err != nil {
		t.Fatalf("generated ACK invalid: %v", err)
	}
}

func TestReceiveDuplicateUsesCachedSACK(t *testing.T) {
	receiver := NewReceiveState(1)
	var result ReceiveResult
	for sequence := Sequence(2); sequence <= Sequence(maxReceiveOffset); sequence++ {
		result = receiver.Receive(receiveData(sequence, "x"))
		if !result.Accepted {
			t.Fatalf("sequence %d was not accepted: %+v", sequence, result)
		}
	}
	mask, ok := result.Actions[0].Packet.SelectiveACK()
	if !ok || len(mask) != maxSACKBytes {
		t.Fatalf("full reorder SACK = %d bytes, present=%t", len(mask), ok)
	}
	for index, value := range mask {
		if value != 0xff {
			t.Fatalf("SACK byte %d = %02x, want ff", index, value)
		}
	}
	if receiver.sackDirty {
		t.Fatal("SACK cache remained dirty after ACK")
	}
	cachedMask := &receiver.sack[0]
	wantWindow := result.Actions[0].Packet.WindowSize
	for attempt := 0; attempt < 1000; attempt++ {
		duplicate := receiver.Receive(receiveData(1000, "x"))
		if !duplicate.Duplicate || len(duplicate.Actions) != 1 {
			t.Fatalf("duplicate %d = %+v", attempt, duplicate)
		}
		ack := duplicate.Actions[0].Packet
		gotMask, ok := ack.SelectiveACK()
		if ack.AckNr != 0 || ack.WindowSize != wantWindow || !ok || !bytes.Equal(gotMask, mask) {
			t.Fatalf("duplicate %d ACK differs from cached receive state", attempt)
		}
		if receiver.sackDirty || &receiver.sack[0] != cachedMask {
			t.Fatalf("duplicate %d rebuilt the SACK mask", attempt)
		}
	}

	// Callers own the returned packet, including its extension data.
	mask[0] = 0
	if next, ok := receiver.AckPacket().SelectiveACK(); !ok || next[0] != 0xff {
		t.Fatal("caller mutated the receive state's cached SACK")
	}
	filled := receiver.Receive(receiveData(1, "x"))
	if !filled.Accepted || filled.Actions[0].Packet.AckNr != Sequence(maxReceiveOffset) {
		t.Fatalf("gap fill = %+v", filled)
	}
	if _, ok := filled.Actions[0].Packet.SelectiveACK(); ok {
		t.Fatal("SACK remained after the gap closed")
	}
}

func FuzzReceiveState(f *testing.F) {
	f.Add(uint16(0), uint8(Data), []byte("payload"))
	f.Add(uint16(0xffff), uint8(Fin), []byte(nil))
	f.Add(uint16(10), uint8(Reset), []byte(nil))
	f.Fuzz(func(t *testing.T, sequence uint16, typeCode uint8, payload []byte) {
		if len(payload) > 128 {
			payload = payload[:128]
		}
		state := NewReceiveState(0)
		packet := Packet{Type: PacketType(typeCode % 5), SeqNr: Sequence(sequence), Payload: payload}
		result := state.Receive(packet)
		for index, action := range result.Actions {
			if action.Kind == ActionSend {
				if err := action.Packet.Validate(); err != nil {
					t.Fatalf("action %d packet invalid: %v", index, err)
				}
			}
		}
		packets, bytes := state.Buffered()
		if packets < 0 || packets > limits.UTPReorderPackets || bytes < 0 || int64(bytes) > limits.UTPBufferBytes {
			t.Fatalf("receive bounds violated: packets=%d bytes=%d", packets, bytes)
		}
	})
}
