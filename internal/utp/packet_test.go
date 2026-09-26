package utp

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func TestPacketBEP29Vector(t *testing.T) {
	wire, err := hex.DecodeString("01021234010203040506070811223344fffeffff0103aabbcc0004010000007879")
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePacket(wire)
	if err != nil {
		t.Fatalf("ParsePacket: %v", err)
	}
	if got.Version != ProtocolVersion || got.Type != Data || got.ConnectionID != 0x1234 ||
		got.Timestamp != 0x01020304 || got.TimestampDifference != 0x05060708 ||
		got.WindowSize != 0x11223344 || got.SeqNr != Sequence(0xfffe) || got.AckNr != Sequence(0xffff) {
		t.Fatalf("decoded header: %+v", got)
	}
	if len(got.Extensions) != 1 || got.Extensions[0].Type != SelectiveACKExtension ||
		!bytes.Equal(got.Extensions[0].Data, []byte{1, 0, 0, 0}) {
		t.Fatalf("decoded extensions: %+v", got.Extensions)
	}
	if !bytes.Equal(got.Payload, []byte("xy")) {
		t.Fatalf("decoded payload %q", got.Payload)
	}
	encoded, err := got.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	// The unknown extension is valid framing, but is omitted on re-encoding.
	want, _ := hex.DecodeString("01011234010203040506070811223344fffeffff0004010000007879")
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encoded known fields differ:\n got  %x\n want %x", encoded, want)
	}
}

func TestSelectiveACKBitOrderAndWrap(t *testing.T) {
	packet := Packet{
		Type:  State,
		AckNr: Sequence(0xfffe),
		Extensions: []Extension{{
			Type: SelectiveACKExtension,
			Data: []byte{0x81, 0x00, 0x00, 0x00},
		}},
	}
	if !packet.AckedBySelectiveACK(Sequence(0)) {
		t.Fatal("bit zero did not acknowledge AckNr+2 across wrap")
	}
	if !packet.AckedBySelectiveACK(Sequence(7)) {
		t.Fatal("most significant bit did not acknowledge the eighth packet")
	}
	if packet.AckedBySelectiveACK(Sequence(1)) {
		t.Fatal("cleared SACK bit acknowledged a packet")
	}
	got := packet.SelectiveACKSequences()
	want := []Sequence{0, 7}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("SACK sequence expansion = %v, want %v", got, want)
	}
}

func TestPacketValidation(t *testing.T) {
	base := make([]byte, HeaderBytes)
	base[0] = byte(Data)<<4 | ProtocolVersion
	cases := []struct {
		name string
		wire []byte
		want error
	}{
		{name: "short", wire: base[:HeaderBytes-1], want: ErrShortPacket},
		{name: "version", wire: withByte(base, 0, 2), want: ErrUnsupportedVersion},
		{name: "type", wire: withByte(base, 0, 0xf1), want: ErrUnknownPacketType},
		{name: "data needs payload", wire: base, want: ErrInvalidPayload},
		{name: "state payload", wire: append(withByte(base, 0, byte(State)<<4|ProtocolVersion), 1), want: ErrInvalidPayload},
		{name: "extension header truncated", wire: append(withByte(withByte(base, 0, byte(Data)<<4|ProtocolVersion), 1, 1), 1), want: ErrMalformedExtension},
		{name: "extension payload truncated", wire: append(append(withByte(withByte(base, 0, byte(Data)<<4|ProtocolVersion), 1, 1), 0, 4), 1, 2), want: ErrMalformedExtension},
		{name: "short SACK", wire: append(append(withByte(withByte(base, 0, byte(State)<<4|ProtocolVersion), 1, SelectiveACKExtension), 0, 3), 1, 2, 3), want: ErrInvalidSACK},
		{name: "nonmultiple SACK", wire: append(append(withByte(withByte(base, 0, byte(State)<<4|ProtocolVersion), 1, SelectiveACKExtension), 0, 5), 1, 2, 3, 4, 5), want: ErrInvalidSACK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePacket(tc.wire)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
	oversized := make([]byte, (1<<16)+1)
	if _, err := ParsePacket(oversized); !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversized error = %v", err)
	}
}

func TestPacketCopiesInputBuffers(t *testing.T) {
	wire := make([]byte, HeaderBytes+2)
	wire[0] = byte(Data)<<4 | ProtocolVersion
	wire[HeaderBytes] = 4
	wire[HeaderBytes+1] = 5
	packet, err := ParsePacket(wire)
	if err != nil {
		t.Fatal(err)
	}
	wire[HeaderBytes] = 9
	if packet.Payload[0] != 4 {
		t.Fatalf("payload aliases input: %v", packet.Payload)
	}
}

func TestPacketActionContract(t *testing.T) {
	action := PacketAction{Kind: ActionSend, Packet: Packet{Type: State}}
	if action.Kind != ActionSend || action.Packet.Type != State || action.Err != nil {
		t.Fatalf("unexpected action: %+v", action)
	}
}

func withByte(input []byte, index int, value byte) []byte {
	output := append([]byte(nil), input...)
	output[index] = value
	return output
}

func FuzzParsePacketBounded(f *testing.F) {
	seed := make([]byte, HeaderBytes+1)
	seed[0] = byte(Data)<<4 | ProtocolVersion
	seed[HeaderBytes] = 1
	f.Add(seed)
	f.Add([]byte{0, 1, 2})
	f.Add([]byte{0x21, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 4, 0x81, 0, 0, 0})
	unknownChain := make([]byte, HeaderBytes+2*32757)
	unknownChain[0], unknownChain[1] = byte(State)<<4|ProtocolVersion, 2
	for i := HeaderBytes; i < len(unknownChain)-2; i += 2 {
		unknownChain[i] = 2
	}
	f.Add(unknownChain)
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > (1<<16)+1 {
			wire = wire[:(1<<16)+1]
		}
		_, _ = ParsePacket(wire)
	})
}
