package utp

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestZeroRemoteWindowProbeRecoversLostReopen(t *testing.T) {
	sender, err := NewSendStateWithConfig(10, SendConfig{
		MaxQueueBytes: 8,
		MaxUnacked:    4,
		RemoteWindow:  4,
		Congestion: CongestionConfig{
			InitialWindow: 4,
			InitialPacket: 4,
			MinPacket:     4,
			MaxPacket:     4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewReceiveStateWithLimits(10, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	link := newTestDatagramLink(
		testLinkStep{},           // first DATA
		testLinkStep{},           // zero-window ACK
		testLinkStep{Drop: true}, // lost reopen notice
		testLinkStep{},           // persist probe
		testLinkStep{Drop: true}, // lost probe ACK
		testLinkStep{},           // probe retransmission
		testLinkStep{},           // duplicate ACK with open window
		testLinkStep{},           // remaining DATA
		testLinkStep{},           // final ACK
	)
	now := time.Unix(200, 0)
	advance := func(delta time.Duration) {
		now = now.Add(delta)
		link.Advance(delta)
	}
	sendPacket := func(side int, packet Packet) {
		t.Helper()
		wire, err := packet.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		link.Send(side, wire)
		link.Advance(0)
	}
	recvPacket := func(side int) Packet {
		t.Helper()
		wire, ok := link.Recv(side)
		if !ok {
			t.Fatalf("endpoint %d received no packet", side)
		}
		packet, err := ParsePacket(wire)
		if err != nil {
			t.Fatal(err)
		}
		return packet
	}
	if n, err := sender.Queue([]byte("abcd")); n != 4 || err != nil {
		t.Fatalf("initial queue = %d, %v", n, err)
	}
	first := sender.Produce(now)
	if len(first) != 1 {
		t.Fatalf("initial DATA actions = %+v", first)
	}
	sendPacket(0, first[0].Packet)
	received := receiver.Receive(recvPacket(1))
	if !received.Accepted || len(received.Actions) != 1 || receiver.WindowSize() != 0 {
		t.Fatalf("receiver did not close window: %+v, window=%d", received, receiver.WindowSize())
	}
	sendPacket(1, received.Actions[0].Packet)
	advance(10 * time.Millisecond)
	closed := sender.Handle(recvPacket(0), now)
	if closed.Err != nil || sender.RemoteWindow() != 0 || sender.UnackedPackets() != 0 {
		t.Fatalf("zero-window ACK = %+v, window=%d, unacked=%d", closed, sender.RemoteWindow(), sender.UnackedPackets())
	}
	if n, err := sender.Queue([]byte("efgh")); n != 4 || err != nil {
		t.Fatalf("blocked queue = %d, %v", n, err)
	}
	if actions := sender.Produce(now); len(actions) != 0 {
		t.Fatalf("zero-window DATA = %+v", actions)
	}
	firstRead := make([]byte, 4)
	if n, err := receiver.Read(firstRead); n != 4 || err != nil || !bytes.Equal(firstRead, []byte("abcd")) {
		t.Fatalf("first read = %d, %q, %v", n, firstRead, err)
	}
	sendPacket(1, receiver.AckPacket()) // dropped reopening STATE
	if wire, ok := link.Recv(0); ok {
		t.Fatalf("reopen notice was delivered: %x", wire)
	}
	rto := sender.RTO()
	advance(rto - time.Nanosecond)
	if actions := sender.Tick(now); len(actions) != 0 {
		t.Fatalf("early probe = %+v", actions)
	}
	advance(time.Nanosecond)
	probe := sender.Tick(now)
	if len(probe) != 1 || probe[0].Packet.Type != Data || string(probe[0].Packet.Payload) != "e" || sender.PendingBytes() != 3 || sender.InFlightBytes() != 1 || sender.UnackedPackets() != 1 {
		t.Fatalf("bounded probe = %+v, pending=%d, in-flight=%d, unacked=%d", probe, sender.PendingBytes(), sender.InFlightBytes(), sender.UnackedPackets())
	}
	if actions := sender.Tick(now); len(actions) != 0 {
		t.Fatalf("immediate repeated probe = %+v", actions)
	}
	sendPacket(0, probe[0].Packet)
	received = receiver.Receive(recvPacket(1))
	if !received.Accepted || len(received.Actions) != 1 || receiver.WindowSize() != 3 {
		t.Fatalf("probe receipt = %+v, window=%d", received, receiver.WindowSize())
	}
	sendPacket(1, received.Actions[0].Packet) // dropped probe ACK
	advance(2*rto - time.Nanosecond)
	if actions := sender.Tick(now); len(actions) != 0 {
		t.Fatalf("early probe retransmission = %+v", actions)
	}
	advance(time.Nanosecond)
	retry := sender.Tick(now)
	if len(retry) != 1 || retry[0].Packet.SeqNr != probe[0].Packet.SeqNr || string(retry[0].Packet.Payload) != "e" || sender.InFlightBytes()+uint32(sender.PendingBytes()) > 8 {
		t.Fatalf("probe retransmission = %+v, in-flight=%d, pending=%d", retry, sender.InFlightBytes(), sender.PendingBytes())
	}
	sendPacket(0, retry[0].Packet)
	received = receiver.Receive(recvPacket(1))
	if !received.Duplicate || len(received.Actions) != 1 {
		t.Fatalf("probe duplicate = %+v", received)
	}
	sendPacket(1, received.Actions[0].Packet)
	advance(10 * time.Millisecond)
	reopened := sender.Handle(recvPacket(0), now)
	if reopened.Err != nil || reopened.AckedBytes != 1 || len(reopened.Actions) != 1 || string(reopened.Actions[0].Packet.Payload) != "fgh" || sender.RemoteWindow() != 3 {
		t.Fatalf("recovery after probe ACK = %+v, window=%d", reopened, sender.RemoteWindow())
	}
	sendPacket(0, reopened.Actions[0].Packet)
	received = receiver.Receive(recvPacket(1))
	if !received.Accepted || len(received.Actions) != 1 {
		t.Fatalf("remaining DATA = %+v", received)
	}
	sendPacket(1, received.Actions[0].Packet)
	advance(10 * time.Millisecond)
	final := sender.Handle(recvPacket(0), now)
	if final.Err != nil || final.AckedBytes != 3 || sender.PendingBytes() != 0 || sender.UnackedPackets() != 0 {
		t.Fatalf("final ACK = %+v, pending=%d, unacked=%d", final, sender.PendingBytes(), sender.UnackedPackets())
	}
	lastRead := make([]byte, 4)
	if n, err := receiver.Read(lastRead); n != 4 || err != nil || !bytes.Equal(lastRead, []byte("efgh")) {
		t.Fatalf("recovered stream = %d, %q, %v", n, lastRead, err)
	}
	if n, err := sender.Queue([]byte("z")); n != 1 || err != nil {
		t.Fatalf("queue before close = %d, %v", n, err)
	}
	sender.Close(context.Canceled)
	advance(10 * time.Minute)
	if actions := sender.Tick(now); len(actions) != 0 {
		t.Fatalf("closed sender probed: %+v", actions)
	}
	if n, err := sender.Queue([]byte("x")); n != 0 || !errors.Is(err, ErrSendClosed) {
		t.Fatalf("queue after close = %d, %v", n, err)
	}
}
