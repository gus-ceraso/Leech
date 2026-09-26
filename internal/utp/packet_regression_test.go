package utp

import (
	"bytes"
	"errors"
	"testing"
)

func stateWire(firstExtension byte, chain ...byte) []byte {
	wire := make([]byte, HeaderBytes, HeaderBytes+len(chain))
	wire[0], wire[1] = byte(State)<<4|ProtocolVersion, firstExtension
	return append(wire, chain...)
}

func TestParsePacketSkipsMaximumUnknownChain(t *testing.T) {
	short := stateWire(2, 0, 0)
	long := make([]byte, HeaderBytes+2*32757)
	long[0], long[1] = byte(State)<<4|ProtocolVersion, 2
	for i := HeaderBytes; i < len(long)-2; i += 2 {
		long[i] = 2
	}
	if len(long) != 65534 {
		t.Fatalf("fixture length = %d", len(long))
	}
	packet, err := ParsePacket(long)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet.Extensions) != 0 || len(packet.Payload) != 0 {
		t.Fatalf("ignored extensions retained state: %+v", packet)
	}
	var parsed Packet
	var parseErr error
	shortAllocs := testing.AllocsPerRun(5, func() { parsed, parseErr = ParsePacket(short) })
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	longAllocs := testing.AllocsPerRun(5, func() { parsed, parseErr = ParsePacket(long) })
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if longAllocs > shortAllocs+1 {
		t.Fatalf("unknown chain allocations: short=%g, maximum=%g", shortAllocs, longAllocs)
	}
	_ = parsed
}

func TestParsePacketMixedUnknownAndSACK(t *testing.T) {
	mask := []byte{0x81, 0, 0, 0}
	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{"unknown before SACK", stateWire(2, 1, 0, 0, 4, 0x81, 0, 0, 0)},
		{"unknown after SACK", stateWire(1, 2, 4, 0x81, 0, 0, 0, 0, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packet, err := ParsePacket(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			if len(packet.Extensions) != 1 || packet.Extensions[0].Type != SelectiveACKExtension ||
				!bytes.Equal(packet.Extensions[0].Data, mask) {
				t.Fatalf("retained extensions = %+v", packet.Extensions)
			}
			for i := range tc.wire {
				tc.wire[i] = 0
			}
			if !bytes.Equal(packet.Extensions[0].Data, mask) {
				t.Fatalf("SACK aliases input: %x", packet.Extensions[0].Data)
			}
		})
	}
}

func TestParsePacketRejectsMalformedUnknownChains(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
		want error
	}{
		{"truncated unknown header", stateWire(2, 0), ErrMalformedExtension},
		{"truncated unknown body", stateWire(2, 0, 4, 1), ErrMalformedExtension},
		{"truncated next header", stateWire(2, 2, 0, 0), ErrMalformedExtension},
		{"invalid SACK after unknown", stateWire(2, 1, 0, 0, 3, 1, 2, 3), ErrInvalidSACK},
		{"duplicate SACK across unknown", stateWire(1, 2, 4, 1, 0, 0, 0, 1, 0, 0, 4, 1, 0, 0, 0), ErrInvalidSACK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParsePacket(tc.wire); !errors.Is(err, tc.want) {
				t.Fatalf("ParsePacket error = %v, want %v", err, tc.want)
			}
		})
	}
}
