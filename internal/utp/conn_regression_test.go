package utp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
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

// sendIndependentData sends one independently encoded BEP 29 DATA packet and
// waits until the corresponding ACK proves ReceiveState accepted it.
func (f writeFixture) sendIndependentData(t *testing.T, payload []byte) {
	t.Helper()
	const seq = uint16(201) // handshake STATE sequence 200, then DATA sequence 201
	wire := make([]byte, HeaderBytes+len(payload))
	wire[0] = byte(Data)<<4 | ProtocolVersion
	binary.BigEndian.PutUint16(wire[2:4], f.id)
	binary.BigEndian.PutUint32(wire[12:16], 4<<20)
	binary.BigEndian.PutUint16(wire[16:18], seq)
	copy(wire[HeaderBytes:], payload)
	if _, err := f.server.WriteToUDP(wire, f.addr); err != nil {
		t.Fatal(err)
	}
	for {
		packet, _, err := readPacket(f.server)
		if err != nil {
			t.Fatal(err)
		}
		if packet.Type == State && packet.AckNr == Sequence(seq) {
			return
		}
	}
}

func TestConnReadExpiredDeadlinePreservesBufferedData(t *testing.T) {
	for _, mode := range []string{"cleared", "extended"} {
		t.Run(mode, func(t *testing.T) {
			f := newWriteFixture(t)
			want := []byte("independent DATA bytes")
			f.sendIndependentData(t, want)
			if err := f.conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, len(want))
			if n, err := f.conn.Read(buf); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("expired Read = %d, %v; want 0, deadline", n, err)
			}
			deadline := time.Time{}
			if mode == "extended" {
				deadline = time.Now().Add(time.Second)
			}
			if err := f.conn.SetReadDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			if n, err := f.conn.Read(buf); n != len(want) || err != nil || !bytes.Equal(buf, want) {
				t.Fatalf("Read after %s = %d, %v, %q; want %q", mode, n, err, buf, want)
			}
			f.conn.mu.Lock()
			_, buffered := f.conn.recv.Buffered()
			f.conn.mu.Unlock()
			if buffered != 0 {
				t.Fatalf("Read left %d duplicate buffered bytes", buffered)
			}
		})
	}
}

func TestConnReadEmptyBufferDeadline(t *testing.T) {
	f := newWriteFixture(t)
	if err := f.conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := f.conn.Read(make([]byte, 1)); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("empty Read = %d, %v; want 0, deadline", n, err)
	}
}

func TestConnReadConcurrentDeadlineChanges(t *testing.T) {
	f := newWriteFixture(t)
	stop := make(chan struct{})
	setterDone := make(chan struct{})
	go func() {
		defer close(setterDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = f.conn.SetReadDeadline(time.Now().Add(time.Second))
			_ = f.conn.SetReadDeadline(time.Time{})
		}
	}()
	want := []byte("concurrent deadline update")
	result := make(chan struct {
		n    int
		err  error
		data []byte
	}, 1)
	go func() {
		buf := make([]byte, len(want))
		n, err := f.conn.Read(buf)
		result <- struct {
			n    int
			err  error
			data []byte
		}{n, err, buf}
	}()
	f.sendIndependentData(t, want)
	close(stop)
	<-setterDone
	select {
	case got := <-result:
		if got.n != len(want) || got.err != nil || !bytes.Equal(got.data, want) {
			t.Fatalf("Read under concurrent deadline changes = %d, %v, %q", got.n, got.err, got.data)
		}
	case <-time.After(time.Second):
		t.Fatal("Read did not complete after concurrent deadline changes")
	}
}

func TestConnReadDeadlineChangesWhileBlocked(t *testing.T) {
	for _, mode := range []string{"shortened", "extended", "cleared"} {
		t.Run(mode, func(t *testing.T) {
			f := newWriteFixture(t)
			initial := time.Now().Add(300 * time.Millisecond)
			if mode == "shortened" {
				initial = time.Now().Add(3 * time.Second)
			}
			if err := f.conn.SetReadDeadline(initial); err != nil {
				t.Fatal(err)
			}
			type readResult struct {
				n    int
				err  error
				data []byte
			}
			result := make(chan readResult, 1)
			readID := make(chan uint64, 1)
			want := []byte("read after deadline change")
			go func() {
				var stack [64]byte
				nstack := runtime.Stack(stack[:], false)
				var id uint64
				if _, err := fmt.Sscanf(string(stack[:nstack]), "goroutine %d ", &id); err != nil {
					panic(err)
				}
				readID <- id
				buf := make([]byte, len(want))
				n, err := f.conn.Read(buf)
				result <- readResult{n: n, err: err, data: buf}
			}()
			id := <-readID
			waitForReadStack(t, id, true)

			if mode == "shortened" {
				next := time.Now().Add(80 * time.Millisecond)
				if err := f.conn.SetReadDeadline(next); err != nil {
					t.Fatal(err)
				}
				waitUntil(t, next)
				select {
				case got := <-result:
					if got.n != 0 || !errors.Is(got.err, os.ErrDeadlineExceeded) {
						t.Fatalf("shortened blocked Read = %d, %v", got.n, got.err)
					}
				case <-time.After(time.Second):
					t.Fatal("shortened deadline did not wake Read")
				}
				return
			}

			// Hold mu across the original timer firing. Once the stack barrier
			// shows Read has returned from waitForWake and is blocked reacquiring
			// mu, apply the same protected state change as SetReadDeadline. This
			// forces the expired-timer path to recheck the replacement deadline.
			f.conn.mu.Lock()
			locked := true
			defer func() {
				if locked {
					f.conn.mu.Unlock()
				}
			}()
			waitUntil(t, initial)
			waitForReadStack(t, id, false)
			if mode == "extended" {
				f.conn.readDeadline = initial.Add(time.Second)
			} else {
				f.conn.readDeadline = time.Time{}
			}
			signal(&f.conn.readWake)
			f.conn.mu.Unlock()
			locked = false

			select {
			case got := <-result:
				t.Fatalf("Read returned at old deadline: %d, %v", got.n, got.err)
			default:
			}
			f.sendIndependentData(t, want)
			select {
			case got := <-result:
				if got.n != len(want) || got.err != nil || !bytes.Equal(got.data, want) {
					t.Fatalf("Read after %s = %d, %v, %q", mode, got.n, got.err, got.data)
				}
			case <-time.After(time.Second):
				t.Fatal("Read did not wake for DATA")
			}
		})
	}
}

func waitUntil(t *testing.T, deadline time.Time) {
	t.Helper()
	wait := time.Until(deadline)
	if wait <= 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	<-timer.C
}

// waitForReadStack synchronizes on the read goroutine's actual wait state,
// rather than assuming it entered Read because its caller was started.
func waitForReadStack(t *testing.T, id uint64, waiting bool) {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	marker := fmt.Sprintf("goroutine %d ", id)
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		all := string(buf[:n])
		start := strings.Index(all, marker)
		if start >= 0 {
			end := strings.Index(all[start+len(marker):], "\ngoroutine ")
			stack := all[start:]
			if end >= 0 {
				stack = all[start : start+len(marker)+end]
			}
			headerEnd := strings.IndexByte(stack, '\n')
			inWait := strings.Contains(stack, "waitForWake") && headerEnd >= 0 && strings.Contains(stack[:headerEnd], "[select]")
			inReadLock := strings.Contains(stack, "Conn).Read") && strings.Contains(stack, "sync.(*Mutex).Lock")
			if waiting && inWait || !waiting && inReadLock && !strings.Contains(stack, "waitForWake") {
				return
			}
		}
		select {
		case <-timeout.C:
			t.Fatalf("read goroutine %d did not reach expected wait state (waiting=%t):\n%s", id, waiting, all)
		default:
			runtime.Gosched()
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
