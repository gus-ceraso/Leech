package utp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
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

func TestConnWriteBackpressureWakesOnACK(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	const payloadBytes = (4 << 20) + (64 << 10)
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveAcknowledgeStream(server, payloadBytes) }()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{'p'}, payloadBytes)
	start := time.Now()
	n, err := conn.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("backpressured Write = %d/%d, %v", n, len(payload), err)
	}
	if elapsed := time.Since(start); elapsed >= 10*time.Second {
		t.Fatalf("backpressured Write reached deadline: %s", elapsed)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveAcknowledgeStream(server *net.UDPConn, wantBytes int) error {
	packet, addr, err := readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Syn {
		return errors.New("backpressure fixture did not receive SYN")
	}
	const peerWindow = 64 << 10
	if err := sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 1_000, AckNr: packet.SeqNr, WindowSize: peerWindow}, addr); err != nil {
		return err
	}
	var received int
	// The SYN's sequence number establishes the first DATA sequence. Do not
	// infer it from the first datagram observed: a scripted link may reorder
	// that packet before the first one.
	receiver := NewReceiveState(packet.SeqNr.Add(1))
	seen := make(map[Sequence]struct{})
	scratch := make([]byte, 64<<10)
	packets := 0
	for received < wantBytes {
		packet, addr, err = readPacket(server)
		if err != nil {
			pending, buffered := receiver.Buffered()
			return fmt.Errorf("read after %d packets, %d/%d bytes, ack=%d next=%d pending=%d buffered=%d: %w", packets, received, wantBytes, receiver.AckNumber(), receiver.NextSequence(), pending, buffered, err)
		}
		packets++
		if packet.Type != Data {
			continue
		}
		if result := receiver.Receive(packet); result.WindowFull || receiver.Err() != nil {
			return fmt.Errorf("backpressure fixture receive window failed at packet %d: %+v err=%v", packets, result, receiver.Err())
		}
		for {
			if n, _ := receiver.Read(scratch); n == 0 {
				break
			}
		}
		if _, exists := seen[packet.SeqNr]; !exists {
			seen[packet.SeqNr] = struct{}{}
			received += len(packet.Payload)
		}
		ack := receiver.AckPacket()
		ack.ConnectionID = packet.ConnectionID - 1
		ack.SeqNr = 1_000
		ack.WindowSize = peerWindow
		if err := sendTo(server, ack, addr); err != nil {
			return err
		}
	}
	if received != wantBytes {
		return errors.New("backpressure fixture received the wrong byte count")
	}
	return nil
}

type scriptedStreamStats struct {
	firstPacketDropped bool
	timeoutObserved    bool
	timeoutArrivals    int
	sackObserved       bool
	peerDataDuplicated bool
	peerDataReordered  bool
	peerDataDelayed    bool
	clientBytes        int
}

// TestConnScriptedStream exercises the complete Conn adapter against one
// deliberately small fake peer. The peer tracks sequence numbers itself and
// encodes cumulative and selective ACKs; it does not call SendState or
// ReceiveState. Client DATA has one dropped packet and one timeout, with a
// small advertised window forcing the writer to wait for ACK credit. Echo
// DATA is sent across a sequence wrap with delay, reordering, duplication,
// and an observed SACK from the client.
func TestConnScriptedStream(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	payload := make([]byte, 48<<10)
	for index := range payload {
		payload[index] = byte(index*31 + 7)
	}
	statsCh := make(chan scriptedStreamStats, 1)
	errCh := make(chan error, 1)
	go func() {
		stats, serveErr := serveScriptedStream(server, payload)
		statsCh <- stats
		errCh <- serveErr
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	conn, err := DialContext(ctx, "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(7 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Write(payload); n != len(payload) || err != nil {
		t.Fatalf("scripted stream Write = %d/%d, %v", n, len(payload), err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		select {
		case stats := <-statsCh:
			t.Fatalf("scripted stream ReadFull: %v; peer stats=%+v err=%v", err, stats, <-errCh)
		default:
			t.Fatalf("scripted stream ReadFull: %v; peer still running", err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("scripted stream echo changed payload")
	}

	stats := <-statsCh
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if !stats.firstPacketDropped || !stats.timeoutObserved || stats.timeoutArrivals < 2 {
		t.Fatalf("loss script did not exercise drop and timeout: %+v", stats)
	}
	if !stats.sackObserved || !stats.peerDataDuplicated || !stats.peerDataReordered || !stats.peerDataDelayed {
		t.Fatalf("script did not exercise SACK and peer scheduling: %+v", stats)
	}
	if stats.clientBytes != len(payload) {
		t.Fatalf("peer received %d/%d bytes", stats.clientBytes, len(payload))
	}
}

func serveScriptedStream(server *net.UDPConn, want []byte) (scriptedStreamStats, error) {
	var stats scriptedStreamStats
	syn, addr, err := readPacket(server)
	if err != nil {
		return stats, err
	}
	if syn.Type != Syn {
		return stats, errors.New("scripted peer did not receive SYN")
	}
	const peerWindow = 8 << 10
	// Start the peer sequence at 0xffff so the echoed stream crosses the ring.
	const peerSequence = Sequence(0xffff)
	if err := sendTo(server, Packet{
		Type:         State,
		ConnectionID: syn.ConnectionID,
		SeqNr:        peerSequence,
		AckNr:        syn.SeqNr,
		WindowSize:   peerWindow,
		Timestamp:    timestamp(time.Now()),
	}, addr); err != nil {
		return stats, err
	}

	first := syn.SeqNr.Add(1)
	ack := first.Add(^uint16(0))
	timeoutSeq := first.Add(4)
	firstDropped := false
	timeoutFirstSeen := false
	pending := make(map[Sequence][]byte)
	seen := make(map[Sequence]struct{})
	received := make([]byte, 0, len(want))
	for len(received) < len(want) {
		packet, packetAddr, readErr := readPacket(server)
		if readErr != nil {
			return stats, readErr
		}
		if packet.Type != Data {
			continue
		}
		if packet.ConnectionID != syn.ConnectionID+1 {
			return stats, errors.New("scripted peer received an unexpected connection ID")
		}

		if packet.SeqNr == first && !firstDropped {
			firstDropped = true
			stats.firstPacketDropped = true
			continue
		}
		if packet.SeqNr == timeoutSeq {
			stats.timeoutArrivals++
			if !timeoutFirstSeen {
				// Drop the first arrival and do not acknowledge later packets
				// while waiting. Sleeping past the initial RTO prevents SACK
				// evidence from triggering fast retransmission of this packet.
				timeoutFirstSeen = true
				time.Sleep(650 * time.Millisecond)
				continue
			}
			// The exact sequence returned after the RTO. Only now does this
			// fixture record timeout recovery and acknowledge the packet.
			stats.timeoutObserved = true
		}
		if _, exists := seen[packet.SeqNr]; exists {
			if err := sendScriptAck(server, packetAddr, syn.ConnectionID, ack, pending, peerWindow); err != nil {
				return stats, err
			}
			continue
		}
		seen[packet.SeqNr] = struct{}{}
		pending[packet.SeqNr] = append([]byte(nil), packet.Payload...)
		for {
			next := ack.Add(1)
			data, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			received = append(received, data...)
			ack = next
		}
		if err := sendScriptAck(server, packetAddr, syn.ConnectionID, ack, pending, peerWindow); err != nil {
			return stats, err
		}
	}
	stats.clientBytes = len(received)
	if !bytes.Equal(received, want) {
		return stats, errors.New("scripted peer reconstructed the wrong stream")
	}

	// Send one packet past the initial receive sequence first, and duplicate
	// it. The client must advertise that gap with a SACK before the missing
	// wrapped packet is released.
	const echoPayload = 512
	// Client DATA uses SYN ID plus one; packets sent back use the original
	// receive ID from the SYN.
	serverID := syn.ConnectionID
	peerData := make([]Packet, 0, (len(want)+echoPayload-1)/echoPayload)
	for offset, sequence := 0, peerSequence; offset < len(want); sequence = sequence.Add(1) {
		end := offset + echoPayload
		if end > len(want) {
			end = len(want)
		}
		peerData = append(peerData, Packet{
			Type:         Data,
			ConnectionID: serverID,
			SeqNr:        sequence,
			WindowSize:   4 << 20,
			Payload:      append([]byte(nil), want[offset:end]...),
		})
		offset = end
	}
	if len(peerData) < 3 {
		return stats, errors.New("scripted peer stream was too short")
	}
	stats.peerDataDuplicated = true
	stats.peerDataReordered = true
	stats.peerDataDelayed = true
	if err := sendTo(server, peerData[1], addr); err != nil {
		return stats, err
	}
	if err := sendTo(server, peerData[1], addr); err != nil {
		return stats, err
	}
	for {
		ackPacket, _, ackErr := readPacket(server)
		if ackErr != nil {
			return stats, ackErr
		}
		if ackPacket.Type == State && len(ackPacket.SelectiveACKSequences()) != 0 {
			stats.sackObserved = true
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	if err := sendTo(server, peerData[0], addr); err != nil {
		return stats, err
	}
	for index := 2; index < len(peerData); index++ {
		if index == 4 {
			// Hold one packet briefly, then send its successor to make the
			// reordering visible to the receive state.
			time.Sleep(10 * time.Millisecond)
			stats.peerDataDelayed = true
		}
		if err := sendTo(server, peerData[index], addr); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func sendScriptAck(server *net.UDPConn, addr *net.UDPAddr, connectionID uint16, ack Sequence, pending map[Sequence][]byte, window uint32) error {
	packet := Packet{
		Type:         State,
		ConnectionID: connectionID,
		Timestamp:    timestamp(time.Now()),
		AckNr:        ack,
		SeqNr:        0xffff,
		WindowSize:   window,
	}
	maxDistance := 0
	for sequence := range pending {
		distance, ok := SequenceDistance(ack, sequence)
		if ok && distance >= 2 && int(distance) <= maxReceiveOffset && int(distance) > maxDistance {
			maxDistance = int(distance)
		}
	}
	if maxDistance != 0 {
		length := ((maxDistance-2)/8 + 1 + 3) / 4 * 4
		mask := make([]byte, length)
		for sequence := range pending {
			distance, ok := SequenceDistance(ack, sequence)
			if !ok || distance < 2 || int(distance) > maxReceiveOffset {
				continue
			}
			bit := int(distance) - 2
			mask[bit/8] |= 1 << uint(bit%8)
		}
		packet.Extensions = []Extension{{Type: SelectiveACKExtension, Data: mask}}
	}
	return sendTo(server, packet, addr)
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
	seq := Sequence(500)
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

func TestRaceEndpointUsesActualUTPDialContext(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var infoHash, clientID, serverID [20]byte
	for index := range infoHash {
		infoHash[index] = byte(index + 1)
		clientID[index] = byte(0x40 + index)
		serverID[index] = byte(0x90 + index)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- servePeerHandshake(server, infoHash, clientID, serverID) }()
	local := peer.Handshake{InfoHash: infoHash, PeerID: clientID, Reserved: [8]byte{0, 0, 0, 0, 0, 0, 0, peer.FastExtensionBit}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := peer.RaceEndpoint(ctx, peer.Endpoint{
		Addr: netip.MustParseAddr("127.0.0.1"),
		Port: uint16(server.LocalAddr().(*net.UDPAddr).Port),
	}, peer.RaceConfig{
		LocalHandshake: local,
		UTPDial:        DialContext,
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("scripted TCP loser")
		},
		UTPHeadStart: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("RaceEndpoint: %v (server: %v)", err, <-serverErr)
	}
	if result.Transport != peer.TransportUTP || result.Conn == nil || result.Handshake.PeerID != serverID {
		t.Fatalf("race result = %+v", result)
	}
	if err := result.Conn.Close(); err != nil {
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
	// first DATA packet while its receive ACK advances to sequence 900.
	if err := sendTo(server, Packet{Type: Data, ConnectionID: syn.ConnectionID, Timestamp: timestamp(time.Now().Add(-time.Second)), SeqNr: 900, AckNr: syn.SeqNr, WindowSize: 4 << 20, Payload: []byte("peer")}, addr); err != nil {
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
	if retry.AckNr != 900 {
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
	if got := binary.BigEndian.Uint16(dataWire[18:20]); got != 799 {
		return errors.New("DATA ACK did not track handshake STATE")
	}
	if got := binary.BigEndian.Uint32(dataWire[12:16]); got != 4<<20 {
		return errors.New("DATA window did not track receive capacity")
	}
	if got := binary.BigEndian.Uint32(dataWire[8:12]); got == 0 {
		return errors.New("DATA timestamp difference was not measured")
	}
	serverDataTimestamp := timestamp(time.Now().Add(-time.Second))
	if err := sendTo(server, Packet{Type: Data, ConnectionID: syn.ConnectionID, Timestamp: serverDataTimestamp, SeqNr: 800, AckNr: data.SeqNr, WindowSize: 4 << 20, Payload: []byte("server")}, addr); err != nil {
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
	if got := binary.BigEndian.Uint16(ackWire[18:20]); got != 800 {
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
	if err := sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID - 1, SeqNr: 700, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr); err != nil {
		return err
	}
	response := make([]byte, len(want))
	response[0] = byte(len(peer.ProtocolName))
	copy(response[1:], peer.ProtocolName)
	response[1+len(peer.ProtocolName)+7] = peer.FastExtensionBit
	copy(response[1+len(peer.ProtocolName)+8:], infoHash[:])
	copy(response[1+len(peer.ProtocolName)+8+20:], serverID[:])
	return sendTo(server, Packet{Type: Data, ConnectionID: packet.ConnectionID - 1, SeqNr: 700, AckNr: packet.SeqNr, WindowSize: 4 << 20, Payload: response}, addr)
}
