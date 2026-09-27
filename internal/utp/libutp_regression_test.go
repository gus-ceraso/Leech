package utp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// These are independently specified BEP 29 packets, not copied libutp vectors.
// Reference behavior and source provenance are recorded in AUDIT.md.
func auditPacket(kind byte, id, seq, ack uint16, payload string) []byte {
	wire := make([]byte, 20+len(payload))
	wire[0] = kind<<4 | 1
	binary.BigEndian.PutUint16(wire[2:4], id)
	binary.BigEndian.PutUint32(wire[12:16], 1<<20)
	binary.BigEndian.PutUint16(wire[16:18], seq)
	binary.BigEndian.PutUint16(wire[18:20], ack)
	copy(wire[20:], payload)
	return wire
}

func TestConnStateDoesNotConsumeSequence(t *testing.T) {
	for _, sequence := range []uint16{500, 0, 65535} {
		t.Run(fmt.Sprint(sequence), func(t *testing.T) {
			server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			udp, err := net.DialUDP("udp4", nil, server.LocalAddr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, err := NewConn(ctx, udp)
			if err != nil {
				udp.Close()
				t.Fatal(err)
			}
			defer conn.Close()
			syn, addr, err := readRawPacket(server)
			if err != nil {
				t.Fatal(err)
			}
			if len(syn) != 20 || syn[0] != 0x41 || binary.BigEndian.Uint16(syn[16:18]) != 1 {
				t.Fatalf("SYN = %x", syn)
			}
			id := binary.BigEndian.Uint16(syn[2:4])
			state := auditPacket(2, id, sequence, 1, "")
			if _, err := server.WriteToUDP(state, addr); err != nil {
				t.Fatal(err)
			}
			if err := conn.waitEstablished(ctx); err != nil {
				t.Fatal(err)
			}
			if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			data, _, err := readRawPacket(server)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) != 24 || data[0] != 0x01 || binary.BigEndian.Uint16(data[2:4]) != id+1 || binary.BigEndian.Uint16(data[16:18]) != 2 {
				t.Fatalf("first outgoing DATA = %x", data)
			}
			if got := binary.BigEndian.Uint16(data[18:20]); got != sequence-1 {
				t.Fatalf("initial receive ACK = %d, want %d (STATE consumes no sequence)", got, sequence-1)
			}
			// A repeated SYN-ACK must not reset reception. The first DATA has
			// exactly the STATE sequence, including zero and wraparound.
			for _, wire := range [][]byte{
				auditPacket(0, id, sequence, 2, "A"),
				state,
				auditPacket(0, id, sequence, 2, "A"),
				auditPacket(0, id, sequence+1, 2, "B"),
				auditPacket(1, id, sequence+2, 2, ""),
			} {
				if _, err := server.WriteToUDP(wire, addr); err != nil {
					t.Fatal(err)
				}
			}
			got, err := io.ReadAll(conn)
			if err != nil || string(got) != "AB" {
				t.Fatalf("peer stream = %q, %v; want AB then EOF", got, err)
			}
		})
	}
}

func TestConnResetAcceptsBothConnectionIDs(t *testing.T) {
	for _, recvID := range []uint16{1234, 65535} {
		for _, id := range []uint16{recvID, recvID + 1} {
			t.Run(fmt.Sprintf("%d/%d", recvID, id), func(t *testing.T) {
				c := &Conn{recvID: recvID, sendID: recvID + 1, synSeq: 1,
					readWake: make(chan struct{}), writeWake: make(chan struct{}), estWake: make(chan struct{})}
				// Both IDs identify RESET, but sendID must not admit STATE or DATA.
				c.handleDatagram(auditPacket(2, c.sendID, 500, 1, ""))
				c.handleDatagram(auditPacket(0, c.sendID, 500, 1, "x"))
				c.handleDatagram(auditPacket(3, recvID+2, 500, 1, ""))
				c.handleDatagram(auditPacket(3, id, 500, 1, "invalid RESET payload"))
				if c.terminal != nil || c.established {
					t.Fatalf("unrelated/malformed packet changed state: %v", c.terminal)
				}
				c.handleDatagram(auditPacket(3, id, 500, 1, ""))
				if !errors.Is(c.terminal, ErrReceiveReset) {
					t.Fatalf("RESET ID %d: %v", id, c.terminal)
				}
			})
		}
	}
}

func TestSendDataIsNotDuplicateACKEvidence(t *testing.T) {
	s := testSendState(t, 4, 4)
	now := time.Unix(100, 0)
	_, _ = s.Queue([]byte("data"))
	s.Produce(now)
	for i := range 4 {
		p, err := ParsePacket(auditPacket(0, 1, uint16(500+i), 9, "peer payload"))
		if err != nil {
			t.Fatal(err)
		}
		result := s.Handle(p, now.Add(time.Duration(i+1)*time.Millisecond))
		if result.Err != nil || len(result.Actions) != 0 || result.LostPackets != 0 {
			t.Fatalf("DATA %d spuriously detected loss: %+v", i, result)
		}
	}
	// Actual repeated STATE ACKs still trigger BEP 29 fast retransmission.
	for i := 1; i <= 3; i++ {
		p, _ := ParsePacket(auditPacket(2, 1, 504, 9, ""))
		result := s.Handle(p, now.Add(time.Duration(i+4)*time.Millisecond))
		if result.Err != nil || (i < 3 && len(result.Actions) != 0) || (i == 3 && (len(result.Actions) != 1 || result.Actions[0].Packet.SeqNr != 10)) {
			t.Fatalf("STATE duplicate %d: %+v", i, result)
		}
	}
}

func TestSendTimeoutRequiresACKProgress(t *testing.T) {
	for _, traffic := range []string{"data", "stale-state", "new-send", "zero-window"} {
		t.Run(traffic, func(t *testing.T) {
			s := testSendState(t, 64, 4)
			now := time.Unix(100, 0)
			_, _ = s.Queue([]byte("data"))
			if traffic == "zero-window" {
				s.SetRemoteWindow(0)
				s.Tick(now) // Start the persist timer without an outstanding packet.
			} else {
				s.Produce(now)
			}
			for i := 1; i < 10; i++ {
				at := now.Add(time.Duration(i) * 100 * time.Millisecond)
				if traffic == "new-send" {
					_, _ = s.Queue([]byte("more"))
					s.Produce(at)
				} else {
					p := Packet{Type: State, AckNr: 8, WindowSize: 64}
					if traffic == "data" {
						p.Type, p.AckNr, p.Payload = Data, 9, []byte("incoming")
					} else if traffic == "zero-window" {
						p.AckNr, p.WindowSize = 9, 0
					}
					if result := s.Handle(p, at); result.Err != nil {
						t.Fatal(result.Err)
					}
				}
				if got := s.Tick(at); len(got) != 0 {
					t.Fatalf("early timeout at %v: %+v", at.Sub(now), got)
				}
			}
			got := s.Tick(now.Add(time.Second))
			if len(got) != 1 || got[0].Packet.SeqNr != 10 {
				t.Fatalf("%s postponed timeout/probe without ACK progress: %+v", traffic, got)
			}
		})
	}
}
