package utp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
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

func TestConnCarriesBEP3PeerHandshake(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	var infoHash [20]byte
	var clientID [20]byte
	var serverID [20]byte
	for index := range infoHash {
		infoHash[index] = byte(index + 1)
		clientID[index] = byte(0xa0 + index)
		serverID[index] = byte(0xd0 + index)
	}
	go func() { serverErr <- servePeerHandshake(server, infoHash, clientID, serverID) }()

	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	reserved := [8]byte{0, 0, 0, 0, 0, 0, 0, peer.FastExtensionBit}
	if err := peer.WriteHandshake(conn, infoHash, clientID, reserved); err != nil {
		t.Fatalf("WriteHandshake over uTP: %v", err)
	}
	handshake, err := peer.ReadHandshake(conn, &infoHash, &serverID)
	if err != nil {
		t.Fatalf("ReadHandshake over uTP: %v", err)
	}
	if handshake.PeerID != serverID || handshake.Reserved != reserved {
		t.Fatalf("peer handshake = %+v", handshake)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnOutgoingHeadersTrackReceiveState(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveHeaderTracking(server) }()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Write([]byte("client")); n != len("client") || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnRetransmissionRefreshesReceiveHeaders(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveRetransmissionHeaders(server) }()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Write([]byte("retry")); n != len("retry") || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func serveRetransmissionHeaders(server *net.UDPConn) error {
	synWire, addr, err := readRawPacket(server)
	if err != nil {
		return err
	}
	syn, err := ParsePacket(synWire)
	if err != nil || syn.Type != Syn {
		return errors.New("retransmission fixture did not receive SYN")
	}
	if err := sendTo(server, Packet{Type: State, ConnectionID: syn.ConnectionID, Timestamp: timestamp(time.Now().Add(-time.Second)), SeqNr: 900, AckNr: syn.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	firstWire, addr, err := readRawPacket(server)
	if err != nil {
		return err
	}
	first, err := ParsePacket(firstWire)
	if err != nil || first.Type != Data {
		return errors.New("retransmission fixture did not receive first DATA")
	}
	// The peer data is deliberately ACK-stale, so the client retains the
	// first DATA packet while its receive ACK advances to sequence 901.
	if err := sendTo(server, Packet{Type: Data, ConnectionID: syn.ConnectionID, Timestamp: timestamp(time.Now().Add(-time.Second)), SeqNr: 901, AckNr: syn.SeqNr, WindowSize: 4 << 20, Payload: []byte("peer")}, addr); err != nil {
		return err
	}
	if _, _, err := readRawPacket(server); err != nil {
		return err
	}
	retryWire, _, err := readRawPacket(server)
	if err != nil {
		return err
	}
	retry, err := ParsePacket(retryWire)
	if err != nil || retry.Type != Data {
		return errors.New("retransmission fixture did not receive retransmitted DATA")
	}
	if retry.SeqNr != first.SeqNr || !bytes.Equal(retry.Payload, first.Payload) {
		return errors.New("retransmitted DATA changed sequence or payload")
	}
	if retry.AckNr != 901 {
		return errors.New("retransmitted DATA did not refresh receive ACK")
	}
	if retry.WindowSize != 4<<20-uint32(len("peer")) {
		return errors.New("retransmitted DATA did not refresh receive window")
	}
	if retry.Timestamp == first.Timestamp {
		return errors.New("retransmitted DATA timestamp was not refreshed")
	}
	if retry.TimestampDifference == 0 {
		return errors.New("retransmitted DATA timestamp difference was not retained")
	}
	return nil
}

func serveHeaderTracking(server *net.UDPConn) error {
	synWire, addr, err := readRawPacket(server)
	if err != nil {
		return err
	}
	syn, err := ParsePacket(synWire)
	if err != nil || syn.Type != Syn {
		return errors.New("header fixture did not receive SYN")
	}
	stateTimestamp := timestamp(time.Now().Add(-time.Second))
	if err := sendTo(server, Packet{Type: State, ConnectionID: syn.ConnectionID, Timestamp: stateTimestamp, SeqNr: 800, AckNr: syn.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	dataWire, addr, err := readRawPacket(server)
	if err != nil {
		return err
	}
	data, err := ParsePacket(dataWire)
	if err != nil || data.Type != Data {
		return errors.New("header fixture did not receive DATA")
	}
	if got := binary.BigEndian.Uint16(dataWire[2:4]); got != syn.ConnectionID+1 {
		return errors.New("DATA connection ID did not use SYN ID plus one")
	}
	if got := binary.BigEndian.Uint16(dataWire[18:20]); got != 800 {
		return errors.New("DATA ACK did not track handshake STATE")
	}
	if got := binary.BigEndian.Uint32(dataWire[12:16]); got != 4<<20 {
		return errors.New("DATA window did not track receive capacity")
	}
	if got := binary.BigEndian.Uint32(dataWire[8:12]); got == 0 {
		return errors.New("DATA timestamp difference was not measured")
	}
	serverDataTimestamp := timestamp(time.Now().Add(-time.Second))
	if err := sendTo(server, Packet{Type: Data, ConnectionID: syn.ConnectionID, Timestamp: serverDataTimestamp, SeqNr: 801, AckNr: data.SeqNr, WindowSize: 4 << 20, Payload: []byte("server")}, addr); err != nil {
		return err
	}
	ackWire, _, err := readRawPacket(server)
	if err != nil {
		return err
	}
	ack, err := ParsePacket(ackWire)
	if err != nil || ack.Type != State {
		return errors.New("header fixture did not receive STATE ACK")
	}
	if got := binary.BigEndian.Uint16(ackWire[2:4]); got != syn.ConnectionID+1 {
		return errors.New("STATE ACK connection ID mismatch")
	}
	if got := binary.BigEndian.Uint16(ackWire[16:18]); got != uint16(data.SeqNr+1) {
		return errors.New("STATE ACK sequence did not track sender sequence")
	}
	if got := binary.BigEndian.Uint16(ackWire[18:20]); got != 801 {
		return errors.New("STATE ACK did not track receive ACK")
	}
	if got := binary.BigEndian.Uint32(ackWire[8:12]); got == 0 {
		return errors.New("STATE ACK timestamp difference was not measured")
	}
	return nil
}

func readRawPacket(conn *net.UDPConn) ([]byte, *net.UDPAddr, error) {
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64<<10)
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil, nil, err
	}
	return append([]byte(nil), buf[:n]...), addr, nil
}

func servePeerHandshake(server *net.UDPConn, infoHash, expectedClientID, serverID [20]byte) error {
	packet, addr, err := readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Syn {
		return errors.New("first packet was not SYN")
	}
	if err := sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 700, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	packet, addr, err = readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Data {
		return errors.New("peer handshake was not a DATA packet")
	}
	want := make([]byte, 1+len(peer.ProtocolName)+8+20+20)
	want[0] = byte(len(peer.ProtocolName))
	copy(want[1:], peer.ProtocolName)
	// The client test advertises Fast in the final reserved byte.
	want[1+len(peer.ProtocolName)+7] = peer.FastExtensionBit
	copy(want[1+len(peer.ProtocolName)+8:], infoHash[:])
	copy(want[1+len(peer.ProtocolName)+8+20:], expectedClientID[:])
	if !bytes.Equal(packet.Payload, want) {
		return errors.New("client BEP3 handshake wire mismatch")
	}
	if err := sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID - 1, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	response := make([]byte, len(want))
	response[0] = byte(len(peer.ProtocolName))
	copy(response[1:], peer.ProtocolName)
	response[1+len(peer.ProtocolName)+7] = peer.FastExtensionBit
	copy(response[1+len(peer.ProtocolName)+8:], infoHash[:])
	copy(response[1+len(peer.ProtocolName)+8+20:], serverID[:])
	return sendTo(server, Packet{Type: Data, ConnectionID: packet.ConnectionID - 1, SeqNr: 701, AckNr: packet.SeqNr, WindowSize: 4 << 20, Payload: response}, addr)
}
