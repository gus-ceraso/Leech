package utp

import (
	"testing"
	"time"
)

func TestSendCongestionRequiresMeasuredDelayAndACKProgress(t *testing.T) {
	for _, missing := range []bool{true, false} {
		name := "unacknowledged-data"
		if missing {
			name = "missing-sample"
		}
		t.Run(name, func(t *testing.T) {
			s := testSendState(t, 4, 4)
			now := time.Unix(100, 0)
			_, _ = s.Queue([]byte("data"))
			s.Produce(now)
			p := Packet{Type: Data, AckNr: 9, Payload: []byte("peer"), WindowSize: 64, TimestampDifference: 1_000_000}
			if missing {
				p.Type, p.AckNr, p.Payload, p.TimestampDifference = State, 10, nil, 0
			}
			if result := s.Handle(p, now.Add(time.Millisecond)); result.Err != nil {
				t.Fatal(result.Err)
			}
			if s.MaxWindow() != 4 {
				t.Fatalf("window changed without measured ACK progress: %d", s.MaxWindow())
			}
			if _, ok := s.Congestion().BaseDelay(); missing && ok {
				t.Fatal("missing delay sample poisoned the baseline")
			}
			ack := Sequence(10)
			if missing {
				_, _ = s.Queue([]byte("more"))
				s.Produce(now.Add(2 * time.Millisecond))
				ack++
			}
			p = Packet{Type: State, AckNr: ack, WindowSize: 64, TimestampDifference: 1_000_000}
			if result := s.Handle(p, now.Add(3*time.Millisecond)); result.Err != nil || result.AckedBytes != 4 || s.MaxWindow() <= 4 {
				t.Fatalf("first measured ACK: %+v, window=%d", result, s.MaxWindow())
			}
			window := s.MaxWindow()
			if result := s.Handle(p, now.Add(4*time.Millisecond)); result.Err != nil || result.AckedBytes != 0 || s.MaxWindow() != window {
				t.Fatalf("duplicate ACK changed congestion credit: %+v, window=%d", result, s.MaxWindow())
			}
		})
	}
}

func TestSendDataSACKRetainsLossEvidence(t *testing.T) {
	s := testSendState(t, 20, 4)
	now := time.Unix(100, 0)
	_, _ = s.Queue([]byte("abcdefghijklmnopqrst"))
	s.Produce(now)
	// Independently encoded SACK: ACK 9, bits 0..2 acknowledge 11, 12, 13.
	wire := auditPacket(0, 1, 500, 9, "")
	wire[1] = 1
	wire = append(wire, 0, 4, 7, 0, 0, 0, 'p')
	packet, err := ParsePacket(wire)
	if err != nil {
		t.Fatal(err)
	}
	result := s.Handle(packet, now.Add(time.Millisecond))
	if result.Err != nil || result.AckedBytes != 12 || result.LostPackets != 1 || len(result.Actions) != 1 || result.Actions[0].Packet.SeqNr != 10 {
		t.Fatalf("DATA SACK failed to retransmit the hole: %+v", result)
	}
}

func TestSendSACKProgressResetsBackoffOnlyOnce(t *testing.T) {
	s := testSendState(t, 12, 4)
	now := time.Unix(100, 0)
	_, _ = s.Queue([]byte("abcdefghijkl"))
	s.Produce(now)
	if got := s.Tick(now.Add(time.Second)); len(got) != 1 {
		t.Fatalf("first timeout = %+v", got)
	}
	progress := now.Add(1100 * time.Millisecond)
	packet := Packet{Type: State, AckNr: 9, WindowSize: 64,
		Extensions: []Extension{{Type: SelectiveACKExtension, Data: []byte{1, 0, 0, 0}}}}
	result := s.Handle(packet, progress)
	if result.Err != nil || result.AckedBytes != 4 || s.timeoutCount != 0 {
		t.Fatalf("SACK did not reset backoff: %+v, backoff=%d", result, s.timeoutCount)
	}
	retryAt := progress.Add(s.RTO())
	result = s.Handle(packet, retryAt.Add(-time.Nanosecond))
	if result.Err != nil || result.AckedBytes != 0 || s.InFlightBytes() != 8 {
		t.Fatalf("repeated SACK released credit again: %+v, in-flight=%d", result, s.InFlightBytes())
	}
	if got := s.Tick(retryAt.Add(-time.Nanosecond)); len(got) != 0 {
		t.Fatalf("retry before progress-based deadline: %+v", got)
	}
	if got := s.Tick(retryAt); len(got) != 1 || got[0].Packet.SeqNr != 10 {
		t.Fatalf("duplicate SACK postponed retry: %+v", got)
	}
}
