package utp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func TestDialContextStreamAndAddresses(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	serverErr := make(chan error, 1)
	go func() { serverErr <- serveOne(t, server) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := DialContext(ctx, "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		t.Fatal("connection addresses are nil")
	}
	if _, ok := conn.LocalAddr().(*net.UDPAddr); !ok {
		t.Fatalf("local address type = %T", conn.LocalAddr())
	}
	if _, ok := conn.RemoteAddr().(*net.UDPAddr); !ok {
		t.Fatalf("remote address type = %T", conn.RemoteAddr())
	}

	want := []byte("hello over utp")
	if n, err := conn.Write(want); n != len(want) || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo = %q, want %q", got, want)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnReadDeadlineAndCloseUnblock(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() { serverErr <- serveHandshakeOnly(t, server) }()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = <-serverErr }()

	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = conn.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read error = %v, want deadline", err)
	}
	if time.Since(start) < 35*time.Millisecond {
		t.Fatalf("deadline fired too early: %s", time.Since(start))
	}

	readDone := make(chan error, 1)
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	go func() { _, err := conn.Read(make([]byte, 1)); readDone <- err }()
	time.Sleep(20 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("read after close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read remained blocked after Close")
	}
}

func TestDialContextCancellationJoinsWorker(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = DialContext(ctx, "utp4", server.LocalAddr().String())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DialContext error = %v, want deadline", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("canceled dial took too long: %s", time.Since(start))
	}
}

func TestConnRemoteFINReturnsEOF(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() {
		packet, addr, err := readPacket(server)
		if err == nil {
			err = sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 300, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr)
		}
		if err == nil {
			err = sendTo(server, Packet{Type: Fin, ConnectionID: packet.ConnectionID, SeqNr: 301, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr)
		}
		serverErr <- err
	}()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read after FIN = %v, want EOF", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnRemoteResetUnblocksRead(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverErr := make(chan error, 1)
	go func() {
		packet, addr, err := readPacket(server)
		if err == nil {
			err = sendTo(server, Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 400, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr)
		}
		if err == nil {
			err = sendTo(server, Packet{Type: Reset, ConnectionID: packet.ConnectionID, AckNr: packet.SeqNr, WindowSize: 4 << 20}, addr)
		}
		serverErr <- err
	}()
	conn, err := DialContext(context.Background(), "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.Read(make([]byte, 1))
	if !errors.Is(err, ErrReceiveReset) && !errors.Is(err, ErrSendReset) {
		t.Fatalf("Read after RESET = %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveHandshakeOnly(t *testing.T, server *net.UDPConn) error {
	t.Helper()
	packet, addr, err := readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Syn {
		return errors.New("first packet was not SYN")
	}
	state := Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: 200, AckNr: packet.SeqNr, WindowSize: 4 << 20}
	wire, _ := state.MarshalBinary()
	if _, err := server.WriteToUDP(wire, addr); err != nil {
		return err
	}
	return nil
}

func serveOne(t *testing.T, server *net.UDPConn) error {
	t.Helper()
	packet, addr, err := readPacket(server)
	if err != nil {
		return err
	}
	if packet.Type != Syn {
		return errors.New("first packet was not SYN")
	}
	serverSeq := Sequence(200)
	state := Packet{Type: State, ConnectionID: packet.ConnectionID, SeqNr: serverSeq, AckNr: packet.SeqNr, WindowSize: 4 << 20}
	if err := sendTo(server, state, addr); err != nil {
		return err
	}
	dataSeq := serverSeq.Add(1)
	for {
		packet, addr, err = readPacket(server)
		if err != nil {
			return err
		}
		switch packet.Type {
		case Data:
			if err := sendTo(server, Packet{
				Type:         Data,
				ConnectionID: packet.ConnectionID - 1,
				SeqNr:        dataSeq,
				AckNr:        packet.SeqNr,
				WindowSize:   4 << 20,
				Payload:      append([]byte(nil), packet.Payload...),
			}, addr); err != nil {
				return err
			}
			return nil
		case State:
			// The client may acknowledge the handshake before its write.
		}
	}
}

func sendTo(conn *net.UDPConn, packet Packet, addr *net.UDPAddr) error {
	wire, err := packet.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = conn.WriteToUDP(wire, addr)
	return err
}

func readPacket(conn *net.UDPConn) (Packet, *net.UDPAddr, error) {
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64<<10)
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return Packet{}, nil, err
	}
	packet, err := ParsePacket(buf[:n])
	return packet, addr, err
}

func FuzzConnectedTransportTransitions(f *testing.F) {
	seed, _ := Packet{Type: Data, SeqNr: 1, AckNr: 0, Payload: []byte("x")}.MarshalBinary()
	f.Add(seed)
	f.Add([]byte{0, 1, 2})
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 64<<10 {
			wire = wire[:64<<10]
		}
		packet, err := ParsePacket(wire)
		if err != nil {
			return
		}
		receive := NewReceiveState(0)
		send := NewSendState(0)
		_ = receive.Receive(packet)
		_ = send.Handle(packet, time.Unix(0, 0))
		if packets, bytes := receive.Buffered(); packets < 0 || bytes < 0 {
			t.Fatalf("negative receive bounds: packets=%d bytes=%d", packets, bytes)
		}
		if send.UnackedPackets() < 0 || send.PendingBytes() < 0 {
			t.Fatalf("negative send bounds: packets=%d bytes=%d", send.UnackedPackets(), send.PendingBytes())
		}
	})
}
