package utp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

type writeFixture struct {
	conn   *Conn
	server *net.UDPConn
	addr   *net.UDPAddr
	id     uint16
}

func newWriteFixture(t *testing.T) writeFixture {
	t.Helper()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	type handshakeResult struct {
		addr *net.UDPAddr
		id   uint16
		err  error
	}
	handshake := make(chan handshakeResult, 1)
	go func() {
		syn, addr, err := readPacket(server)
		if err == nil && syn.Type != Syn {
			err = errors.New("fixture expected SYN")
		}
		if err == nil {
			err = sendTo(server, Packet{Type: State, ConnectionID: syn.ConnectionID,
				SeqNr: 200, AckNr: syn.SeqNr, WindowSize: 4 << 20}, addr)
		}
		handshake <- handshakeResult{addr: addr, id: syn.ConnectionID, err: err}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := DialContext(ctx, "utp4", server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.(*Conn)
	t.Cleanup(func() { _ = conn.Close() })
	result := <-handshake
	if result.err != nil {
		t.Fatal(result.err)
	}
	return writeFixture{conn: conn, server: server, addr: result.addr, id: result.id}
}

func (f writeFixture) readData(t *testing.T) Packet {
	t.Helper()
	for {
		packet, _, err := readPacket(f.server)
		if err != nil {
			t.Fatal(err)
		}
		if packet.Type == Data {
			return packet
		}
		if packet.Type != State {
			t.Fatalf("fixture received %s before data", packet.Type)
		}
	}
}

func (f writeFixture) limitQueue(bytes int) {
	f.conn.mu.Lock()
	f.conn.send.maxBuffered = bytes
	f.conn.mu.Unlock()
}

func TestConnWritePastDeadlineWithFreeCapacity(t *testing.T) {
	f := newWriteFixture(t)
	if err := f.conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := f.conn.Write([]byte("twelve bytes")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expired Write = %d, %v; want 0, deadline", n, err)
	}
	f.conn.mu.Lock()
	buffered := f.conn.send.PendingBytes() + int(f.conn.send.InFlightBytes())
	f.conn.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("expired Write queued %d bytes", buffered)
	}
	if err := f.conn.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if n, err := f.conn.Write([]byte("ok")); n != 2 || err != nil {
		t.Fatalf("Write after clearing deadline = %d, %v", n, err)
	}
	if data := f.readData(t); !bytes.Equal(data.Payload, []byte("ok")) {
		t.Fatalf("queued data after clearing deadline = %q", data.Payload)
	}
}

func TestConnWriteReturnsAcceptedPrefixAtExpiry(t *testing.T) {
	f := newWriteFixture(t)
	f.limitQueue(8)
	result := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := f.conn.Write([]byte("123456789"))
		result <- struct {
			n   int
			err error
		}{n, err}
	}()
	if data := f.readData(t); !bytes.Equal(data.Payload, []byte("12345678")) {
		t.Fatalf("first accepted prefix = %q", data.Payload)
	}
	if err := f.conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.n != 8 || !errors.Is(got.err, os.ErrDeadlineExceeded) {
			t.Fatalf("partial Write = %d, %v; want 8, deadline", got.n, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("expired partial Write remained blocked")
	}
}

func TestConnWriteDeadlineExtendedOrClearedWhileBlocked(t *testing.T) {
	for _, tc := range []struct {
		name string
		next func(time.Time) time.Time
	}{
		{"extended", func(old time.Time) time.Time { return old.Add(3 * time.Second) }},
		{"cleared", func(time.Time) time.Time { return time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWriteFixture(t)
			f.limitQueue(8)
			oldDeadline := time.Now().Add(300 * time.Millisecond)
			if err := f.conn.SetWriteDeadline(oldDeadline); err != nil {
				t.Fatal(err)
			}
			result := make(chan struct {
				n   int
				err error
			}, 1)
			go func() {
				n, err := f.conn.Write([]byte("123456789"))
				result <- struct {
					n   int
					err error
				}{n, err}
			}()
			first := f.readData(t)
			if err := f.conn.SetWriteDeadline(tc.next(oldDeadline)); err != nil {
				t.Fatal(err)
			}
			if wait := time.Until(oldDeadline.Add(20 * time.Millisecond)); wait > 0 {
				time.Sleep(wait)
			}
			select {
			case got := <-result:
				t.Fatalf("Write finished before ACK after deadline change: %d, %v", got.n, got.err)
			default:
			}
			if err := sendTo(f.server, Packet{Type: State, ConnectionID: f.id,
				SeqNr: 200, AckNr: first.SeqNr, WindowSize: 4 << 20}, f.addr); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-result:
				if got.n != 9 || got.err != nil {
					t.Fatalf("Write after deadline change = %d, %v", got.n, got.err)
				}
			case <-time.After(time.Second):
				t.Fatal("Write did not resume after ACK")
			}
		})
	}
}

func TestConnBlockedWriteWakesOnCloseOrReset(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "close"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			f := newWriteFixture(t)
			f.limitQueue(8)
			result := make(chan struct {
				n   int
				err error
			}, 1)
			go func() {
				n, err := f.conn.Write([]byte("123456789"))
				result <- struct {
					n   int
					err error
				}{n, err}
			}()
			_ = f.readData(t)
			if reset {
				if err := sendTo(f.server, Packet{Type: Reset, ConnectionID: f.id}, f.addr); err != nil {
					t.Fatal(err)
				}
			} else if err := f.conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-result:
				if got.n != 8 || !(errors.Is(got.err, net.ErrClosed) || errors.Is(got.err, ErrReceiveReset) || errors.Is(got.err, ErrSendReset)) {
					t.Fatalf("blocked Write after %s = %d, %v", name, got.n, got.err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked Write did not wake")
			}
		})
	}
}

func TestConnChecksIDBeforeExtensionAndStateWork(t *testing.T) {
	c := &Conn{recvID: 0x1234, readWake: make(chan struct{}), writeWake: make(chan struct{}), estWake: make(chan struct{})}
	unrelated := make([]byte, HeaderBytes+2*32757)
	unrelated[0], unrelated[1] = byte(State)<<4|ProtocolVersion, 2
	for i := HeaderBytes; i < len(unrelated)-2; i += 2 {
		unrelated[i] = 2
	}
	binary.BigEndian.PutUint16(unrelated[2:4], 0x1235)
	c.mu.Lock()
	done := make(chan struct{})
	go func() { c.handleDatagram(unrelated); close(done) }()
	select {
	case <-done:
		c.mu.Unlock()
	case <-time.After(time.Second):
		c.mu.Unlock()
		<-done
		t.Fatal("unrelated connection ID entered state processing")
	}
	malformed := stateWire(1, 0, 3, 1, 2, 3)
	malformed[0] = byte(Reset)<<4 | ProtocolVersion
	binary.BigEndian.PutUint16(malformed[2:4], c.recvID)
	c.handleDatagram(malformed)
	if c.terminal != nil {
		t.Fatalf("malformed active reset changed state: %v", c.terminal)
	}
	valid := stateWire(0)
	valid[0] = byte(Reset)<<4 | ProtocolVersion
	binary.BigEndian.PutUint16(valid[2:4], c.recvID)
	c.handleDatagram(valid)
	if !errors.Is(c.terminal, ErrReceiveReset) {
		t.Fatalf("active reset was ignored: %v", c.terminal)
	}
}
