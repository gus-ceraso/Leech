package utp

import (
	"errors"
	"testing"
	"time"
)

func testSendState(t *testing.T, initialWindow uint32, packetSize int) *SendState {
	t.Helper()
	return testSendStateWithQueue(t, initialWindow, packetSize, 64)
}

func testSendStateWithQueue(t *testing.T, initialWindow uint32, packetSize, maxQueue int) *SendState {
	t.Helper()
	state, err := NewSendStateWithConfig(10, SendConfig{
		MaxQueueBytes: maxQueue,
		MaxUnacked:    16,
		RemoteWindow:  64,
		Congestion: CongestionConfig{
			InitialWindow: initialWindow,
			InitialPacket: packetSize,
			MinPacket:     packetSize,
			MaxPacket:     packetSize,
			InitialRTO:    time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestSendQueueAndWindows(t *testing.T) {
	s := testSendStateWithQueue(t, 4, 4, 8)
	if n, err := s.Queue([]byte("0123456789")); n != 8 || !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("bounded queue = n=%d err=%v", n, err)
	}
	now := time.Unix(10, 0)
	actions := s.Produce(now)
	if len(actions) != 1 || len(actions[0].Packet.Payload) != 4 || actions[0].Packet.SeqNr != 10 {
		t.Fatalf("initial production = %+v", actions)
	}
	if n, err := s.Queue([]byte("z")); n != 0 || !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("in-flight queue credit = n=%d err=%v", n, err)
	}
	if got := s.InFlightBytes(); got != 4 {
		t.Fatalf("in-flight bytes = %d", got)
	}
	result := s.Handle(Packet{Type: State, AckNr: 10, WindowSize: 8}, now.Add(600*time.Millisecond))
	if result.Err != nil || result.AckedBytes != 4 || len(result.Actions) != 1 {
		t.Fatalf("ACK production = %+v", result)
	}
	if got := string(result.Actions[0].Packet.Payload); got != "4567" {
		t.Fatalf("next payload = %q", got)
	}
	if s.PendingBytes() != 0 || s.InFlightBytes() != 4 {
		t.Fatalf("post-ACK state pending=%d in-flight=%d", s.PendingBytes(), s.InFlightBytes())
	}
}

func TestSendIgnoresUnboundedStaleACKs(t *testing.T) {
	s := testSendState(t, 8, 4)
	if n, err := s.Queue([]byte("abcdefgh")); n != 8 || err != nil {
		t.Fatalf("queue = n=%d err=%v", n, err)
	}
	now := time.Unix(15, 0)
	if got := s.Produce(now); len(got) != 2 {
		t.Fatalf("production = %d", len(got))
	}
	if result := s.Handle(Packet{Type: State, AckNr: 11, WindowSize: 64}, now.Add(time.Millisecond)); result.Err != nil {
		t.Fatalf("initial ACK = %+v", result)
	}
	for index := 0; index < 100_000; index++ {
		offset := uint16(index%32767) + 1
		stale := s.lastAck.Add(^uint16(0) - (offset - 1))
		if stale == s.lastAck {
			continue
		}
		result := s.Handle(Packet{Type: State, AckNr: stale, WindowSize: 64}, now.Add(2*time.Millisecond))
		if result.Err != nil || len(result.Actions) != 0 {
			t.Fatalf("stale ACK %d result = %+v", stale, result)
		}
	}
	if s.duplicateAcks != 0 || s.UnackedPackets() != 0 {
		t.Fatalf("stale ACK state duplicates=%d unacked=%d", s.duplicateAcks, s.UnackedPackets())
	}
}

func TestSendActionSharesImmutablePayloadRecord(t *testing.T) {
	s := testSendState(t, 4, 4)
	if n, err := s.Queue([]byte("data")); n != 4 || err != nil {
		t.Fatalf("queue = n=%d err=%v", n, err)
	}
	actions := s.Produce(time.Unix(16, 0))
	if len(actions) != 1 {
		t.Fatalf("production = %d", len(actions))
	}
	record := s.unacked[actions[0].Packet.SeqNr]
	if record == nil || &record.packet.Payload[0] != &actions[0].Packet.Payload[0] {
		t.Fatal("action and retransmission record do not share payload storage")
	}
}

func TestSendSelectiveACKTriggersFastRetransmit(t *testing.T) {
	s := testSendState(t, 20, 4)
	if n, err := s.Queue([]byte("abcdefghijklmnopqrst")); n != 20 || err != nil {
		t.Fatalf("queue = n=%d err=%v", n, err)
	}
	now := time.Unix(20, 0)
	if got := s.Produce(now); len(got) != 5 {
		t.Fatalf("production count = %d", len(got))
	}
	mask := []byte{0x07, 0, 0, 0} // ack_nr+2, +3, and +4
	result := s.Handle(Packet{Type: State, AckNr: 10, WindowSize: 64, Extensions: []Extension{{Type: SelectiveACKExtension, Data: mask}}}, now.Add(10*time.Millisecond))
	if result.Err != nil || result.LostPackets != 1 || len(result.Actions) != 1 {
		t.Fatalf("SACK loss result = %+v", result)
	}
	if got := result.Actions[0].Packet.SeqNr; got != 11 {
		t.Fatalf("fast retransmit sequence = %d", got)
	}
	if got := string(result.Actions[0].Packet.Payload); got != "efgh" {
		t.Fatalf("fast retransmit payload = %q", got)
	}
	if s.Congestion().MaxWindow() >= 20 {
		t.Fatalf("loss did not reduce max window: %d", s.Congestion().MaxWindow())
	}
}

func TestSendTimeoutRetransmitsSYNAndBacksOff(t *testing.T) {
	s, err := NewSendStateWithConfig(10, SendConfig{
		MaxQueueBytes: 64,
		MaxUnacked:    16,
		RemoteWindow:  64,
		Congestion: CongestionConfig{
			InitialWindow: 1_200,
			InitialPacket: 1_200,
			MinPacket:     minimumPacketSize,
			MaxPacket:     1_200,
			InitialRTO:    time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(30, 0)
	actions, err := s.Start(now)
	if err != nil || len(actions) != 1 || actions[0].Packet.Type != Syn {
		t.Fatalf("SYN = actions=%+v err=%v", actions, err)
	}
	rto := s.Congestion().RTO()
	if got := s.Tick(now.Add(rto - time.Nanosecond)); len(got) != 0 {
		t.Fatalf("early timeout = %+v", got)
	}
	got := s.Tick(now.Add(rto))
	if len(got) != 1 || got[0].Packet.Type != Syn || got[0].Packet.SeqNr != actions[0].Packet.SeqNr {
		t.Fatalf("SYN retransmit = %+v", got)
	}
	if s.Congestion().PacketSize() != minimumPacketSize || s.Congestion().MaxWindow() != minimumPacketSize {
		t.Fatalf("timeout congestion state packet=%d window=%d", s.Congestion().PacketSize(), s.Congestion().MaxWindow())
	}
	if got := s.Tick(now.Add(2 * rto)); len(got) != 0 {
		t.Fatalf("backoff timeout fired too early = %+v", got)
	}
	if got := s.Tick(now.Add(3 * rto)); len(got) != 1 {
		t.Fatalf("backoff timeout = %+v", got)
	}
}

func TestCongestionDelayBucketsAndRTT(t *testing.T) {
	c, err := NewCongestionController(CongestionConfig{InitialWindow: 4, InitialPacket: 4, MinPacket: 4, MaxPacket: 16})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(40, 0)
	for index := 0; index < 100_000; index++ {
		c.ObserveDelay(now, 20*time.Millisecond, 4)
	}
	if len(c.buckets) != delayBucketCount {
		t.Fatalf("delay bucket bound = %d", len(c.buckets))
	}
	if base, ok := c.BaseDelay(); !ok || base != 20*time.Millisecond {
		t.Fatalf("base delay = %v, %v", base, ok)
	}
	beforePacket, beforeWindow := c.PacketSize(), c.MaxWindow()
	c.ObserveDelay(now.Add(time.Second), 250*time.Millisecond, 4)
	if c.PacketSize() >= beforePacket || c.MaxWindow() >= beforeWindow {
		t.Fatalf("high delay did not reduce controls packet=%d window=%d", c.PacketSize(), c.MaxWindow())
	}
	c.UpdateRTT(100 * time.Millisecond)
	if c.RTO() < minimumRTO {
		t.Fatalf("RTO below BEP minimum: %s", c.RTO())
	}
}

func FuzzSendStateBounded(f *testing.F) {
	f.Add(uint16(0), []byte("hello"), uint32(1))
	f.Add(uint16(65535), []byte("0123456789abcdef"), uint32(64))
	f.Fuzz(func(t *testing.T, initial uint16, payload []byte, window uint32) {
		if len(payload) > 256 {
			payload = payload[:256]
		}
		if window > 4096 {
			window = 4096
		}
		s, err := NewSendStateWithConfig(Sequence(initial), SendConfig{
			MaxQueueBytes: 64,
			MaxUnacked:    16,
			RemoteWindow:  64,
			Congestion: CongestionConfig{
				InitialWindow: 64,
				InitialPacket: 1,
				MinPacket:     1,
				MaxPacket:     1,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		s.SetRemoteWindow(window)
		_, _ = s.Queue(payload)
		now := time.Unix(50, 0)
		actions := s.Produce(now)
		wantSeq := Sequence(initial)
		var producedBytes uint32
		for _, action := range actions {
			if action.Kind != ActionSend {
				t.Fatalf("production action kind = %d", action.Kind)
			}
			if err := action.Packet.Validate(); err != nil {
				t.Fatalf("production packet = %v", err)
			}
			if action.Packet.SeqNr != wantSeq {
				t.Fatalf("production sequence = %d, want %d", action.Packet.SeqNr, wantSeq)
			}
			wantSeq = wantSeq.Add(1)
			producedBytes += uint32(len(action.Packet.Payload))
		}
		if s.PendingBytes() < 0 || s.PendingBytes() > 64 || s.UnackedPackets() > 16 || s.InFlightBytes() > 64 || s.InFlightBytes() != producedBytes || s.InFlightBytes()+uint32(s.PendingBytes()) > 64 {
			t.Fatalf("send bounds pending=%d unacked=%d in-flight=%d", s.PendingBytes(), s.UnackedPackets(), s.InFlightBytes())
		}
		if window == 0 && len(actions) != 0 {
			t.Fatalf("zero remote window produced %d packets", len(actions))
		}
	})
}
