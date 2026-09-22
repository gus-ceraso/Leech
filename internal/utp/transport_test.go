package utp

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestStateStreamLossAndWrap drives independent send and receive fixtures
// through a small scripted datagram link. The fixtures do not call each other
// directly: every action is encoded, delayed, duplicated, or dropped before it
// reaches the other state machine.
func TestStateStreamLossAndWrap(t *testing.T) {
	client, err := NewSendStateWithConfig(Sequence(0xfffe), SendConfig{
		MaxQueueBytes: 64,
		MaxUnacked:    16,
		RemoteWindow:  64,
		Congestion: CongestionConfig{
			InitialWindow: 16,
			InitialPacket: 4,
			MinPacket:     4,
			MaxPacket:     4,
			InitialRTO:    time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewReceiveState(Sequence(0xfffe))
	if n, err := client.Queue([]byte("abcdefghijklmnop")); n != 16 || err != nil {
		t.Fatalf("queue = %d, %v", n, err)
	}
	now := time.Unix(100, 0)
	actions := client.Produce(now)
	if len(actions) != 4 {
		t.Fatalf("initial data packets = %d", len(actions))
	}

	// Drop seq 0. Deliver the packets around it out of order, then duplicate
	// seq 1 twice. The repeated ACK for ack_nr=65535 drives fast recovery of
	// the missing wrapped packet.
	bySequence := make(map[Sequence]Packet)
	for _, action := range actions {
		bySequence[action.Packet.SeqNr] = action.Packet
	}
	var deliver func(Packet, time.Time)
	deliver = func(packet Packet, at time.Time) {
		result := server.Receive(packet)
		if result.WindowFull {
			t.Fatalf("server window unexpectedly full for seq %d", packet.SeqNr)
		}
		for _, ack := range result.Actions {
			if ack.Kind != ActionSend {
				continue
			}
			ackResult := client.Handle(ack.Packet, at)
			if ackResult.Err != nil {
				t.Fatalf("client ACK handling: %v", ackResult.Err)
			}
			for _, retransmit := range ackResult.Actions {
				if retransmit.Kind == ActionSend && retransmit.Packet.Type == Data && retransmit.Packet.SeqNr == 0 {
					// Fast retransmission is delivered below after the scripted
					// duplicate ACKs have been processed.
					bySequence[retransmit.Packet.SeqNr] = retransmit.Packet
				}
			}
		}
	}
	deliver(bySequence[Sequence(0xffff)], now.Add(10*time.Millisecond))
	deliver(bySequence[Sequence(1)], now.Add(20*time.Millisecond))
	deliver(bySequence[Sequence(0xfffe)], now.Add(30*time.Millisecond))
	// The first seq 1 packet plus two deterministic duplicates produce three
	// duplicate ACKs after the cumulative ACK reaches 65535.
	deliver(bySequence[Sequence(1)], now.Add(40*time.Millisecond))
	deliver(bySequence[Sequence(1)], now.Add(50*time.Millisecond))
	if client.UnackedPackets() == 0 {
		t.Fatal("missing wrapped packet was acknowledged before recovery")
	}
	recovered := bySequence[Sequence(0)]
	if recovered.Type != Data || recovered.SeqNr != 0 {
		t.Fatalf("fast retransmission = %+v", recovered)
	}
	deliver(recovered, now.Add(60*time.Millisecond))

	if client.UnackedPackets() != 0 {
		t.Fatalf("unacked packets after recovery = %d", client.UnackedPackets())
	}
	got := make([]byte, 16)
	if n, err := server.Read(got); n != len(got) || err != nil {
		t.Fatalf("server stream = %d, %v", n, err)
	}
	if !bytes.Equal(got, []byte("abcdefghijklmnop")) {
		t.Fatalf("server stream = %q", got)
	}
}

func TestStateTransportTimeoutAndWindowReopen(t *testing.T) {
	client, err := NewSendStateWithConfig(1, SendConfig{
		MaxQueueBytes: 16,
		MaxUnacked:    4,
		RemoteWindow:  4,
		Congestion: CongestionConfig{
			InitialWindow: 4,
			InitialPacket: 4,
			MinPacket:     4,
			MaxPacket:     4,
			InitialRTO:    time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Queue([]byte("data")); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(200, 0)
	if got := client.Produce(now); len(got) != 1 {
		t.Fatalf("produced packets = %d", len(got))
	}
	if got := client.Tick(now.Add(999 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("early timeout = %+v", got)
	}
	if got := client.Tick(now.Add(time.Second)); len(got) != 1 || got[0].Packet.SeqNr != 1 {
		t.Fatalf("timeout retransmission = %+v", got)
	}

	receiver, err := NewReceiveStateWithLimits(0, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if result := receiver.Receive(Packet{Type: Data, SeqNr: 1, Payload: []byte("bbbb")}); !result.Accepted || receiver.WindowSize() != 0 {
		t.Fatalf("window fill = %+v, window=%d", result, receiver.WindowSize())
	}
	if result := receiver.Receive(Packet{Type: Data, SeqNr: 2, Payload: []byte("cccc")}); !result.WindowFull {
		t.Fatalf("window pressure = %+v", result)
	}
	if result := receiver.Receive(Packet{Type: Data, SeqNr: 0, Payload: []byte("aaaa")}); !result.Accepted {
		t.Fatalf("window reopen = %+v", result)
	}
	readBack := make([]byte, 8)
	if n, readErr := receiver.Read(readBack); n != 8 || readErr != nil {
		t.Fatalf("window-drain read = %d, %v", n, readErr)
	}
	if receiver.WindowSize() != 8 {
		t.Fatalf("window after read-free drain = %d", receiver.WindowSize())
	}
}

func TestConcurrentConnOperationsCloseUnderRace(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveEchoLoop(server) }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := DialContext(ctx, "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var done chan struct{} = make(chan struct{}, 3)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = conn.Write([]byte("ping"))
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		buffer := make([]byte, 4)
		for {
			if _, err := conn.Read(buffer); err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
		}
	}()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("concurrent operation remained blocked after Close")
		}
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveEchoLoop(server *net.UDPConn) error {
	packet, addr, err := readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Syn {
		return errors.New("first packet was not SYN")
	}
	if err := sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 500, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	seq := Sequence(501)
	for {
		packet, addr, err = readPacket(server)
		if err != nil {
			return err
		}
		switch packet.Type {
		case Data:
			if err := sendTo(server, Packet{Type: Data, ConnectionID: packet.ConnectionID - 1, SeqNr: seq, AckNr: packet.SeqNr, WindowSize: 4 << 20, Payload: append([]byte(nil), packet.Payload...)}, addr); err != nil {
				return err
			}
			seq++
		case Reset:
			return nil
		}
	}
}

func TestTransportPayloadActionsAreIndependentFixtures(t *testing.T) {
	state := NewSendState(10)
	if _, err := state.Queue([]byte("test")); err != nil {
		t.Fatal(err)
	}
	actions := state.Produce(time.Unix(300, 0))
	if len(actions) != 1 {
		t.Fatalf("actions = %d", len(actions))
	}
	wire, err := actions[0].Packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParsePacket(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Payload, []byte("test")) {
		t.Fatalf("encoded payload = %q", decoded.Payload)
	}
	decoded.Payload[0] = 'X'
	if got := string(actions[0].Packet.Payload); got != "test" {
		t.Fatalf("action payload changed through fixture decode: %q", got)
	}
	if result := state.Handle(Packet{Type: State, AckNr: 10, WindowSize: 4 << 20}, time.Unix(301, 0)); result.Err != nil {
		t.Fatalf("ACK handling: %v", result.Err)
	}
}
