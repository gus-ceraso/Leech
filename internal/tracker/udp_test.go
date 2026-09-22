package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

type fixtureResolver struct{ ips []net.IPAddr }

func (r fixtureResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return append([]net.IPAddr(nil), r.ips...), nil
}

type fixtureDialer struct {
	conn    *fixtureConn
	factory func() net.Conn
}

func (d fixtureDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	if d.factory != nil {
		return d.factory(), nil
	}
	return d.conn, nil
}

type fixtureConn struct {
	mu       sync.Mutex
	reads    chan []byte
	closed   chan struct{}
	closeOne sync.Once
	onWrite  func([]byte)
	writes   [][]byte
}

func newFixtureConn(onWrite func([]byte)) *fixtureConn {
	return &fixtureConn{reads: make(chan []byte, 32), closed: make(chan struct{}), onWrite: onWrite}
}

func (c *fixtureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.mu.Unlock()
	if c.onWrite != nil {
		c.onWrite(append([]byte(nil), p...))
	}
	return len(p), nil
}

func (c *fixtureConn) Read(p []byte) (int, error) {
	select {
	case data := <-c.reads:
		copy(p, data)
		return len(data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *fixtureConn) Close() error {
	c.closeOne.Do(func() { close(c.closed) })
	return nil
}

func (c *fixtureConn) LocalAddr() net.Addr              { return fixtureAddr("local") }
func (c *fixtureConn) RemoteAddr() net.Addr             { return fixtureAddr("remote") }
func (c *fixtureConn) SetDeadline(time.Time) error      { return nil }
func (c *fixtureConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fixtureConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fixtureConn) push(packet []byte)               { c.reads <- append([]byte(nil), packet...) }
func (c *fixtureConn) writeCount() int                  { c.mu.Lock(); defer c.mu.Unlock(); return len(c.writes) }
func (c *fixtureConn) writeAt(index int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.writes[index]...)
}

type fixtureAddr string

func (a fixtureAddr) Network() string { return "udp" }
func (a fixtureAddr) String() string  { return string(a) }

type fixtureTimer struct {
	mu      sync.Mutex
	ch      chan time.Time
	stopped bool
	fired   bool
}

func (t *fixtureTimer) Chan() <-chan time.Time { return t.ch }
func (t *fixtureTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

type fixtureClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fixtureTimer
	changed chan struct{}
}

func newFixtureClock() *fixtureClock {
	return &fixtureClock{now: time.Unix(100, 0), changed: make(chan struct{}, 64)}
}

func (c *fixtureClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fixtureClock) NewTimer(time.Duration) Timer {
	t := &fixtureTimer{ch: make(chan time.Time, 1)}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	c.changed <- struct{}{}
	return t
}
func (c *fixtureClock) nextActive(t *testing.T) *fixtureTimer {
	t.Helper()
	for {
		c.mu.Lock()
		for _, timer := range c.timers {
			timer.mu.Lock()
			active := !timer.stopped && !timer.fired
			timer.mu.Unlock()
			if active {
				c.mu.Unlock()
				return timer
			}
		}
		c.mu.Unlock()
		select {
		case <-c.changed:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for an active timer")
		}
	}
}
func (c *fixtureClock) activeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, timer := range c.timers {
		timer.mu.Lock()
		if !timer.stopped && !timer.fired {
			count++
		}
		timer.mu.Unlock()
	}
	return count
}
func (c *fixtureClock) fire(t *fixtureTimer) {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return
	}
	t.fired = true
	t.mu.Unlock()
	t.ch <- c.Now()
}
func (c *fixtureClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testRequest() AnnounceRequest {
	var info, peer [20]byte
	for i := range info {
		info[i] = byte(i)
		peer[i] = byte(20 - i)
	}
	return AnnounceRequest{
		InfoHash: info, PeerID: peer, Downloaded: 1, Left: 2, Uploaded: 0,
		Event: EventStarted, Key: 0x11223344, NumWant: -1, Port: 49152,
	}
}

func connectResponse(packet []byte, id uint64) []byte {
	result := make([]byte, 16)
	binary.BigEndian.PutUint32(result[0:4], 0)
	binary.BigEndian.PutUint32(result[4:8], binary.BigEndian.Uint32(packet[12:16]))
	binary.BigEndian.PutUint64(result[8:16], id)
	return result
}

func announceResponse(packet []byte, peers ...netip.AddrPort) []byte {
	result := make([]byte, 20+6*len(peers))
	binary.BigEndian.PutUint32(result[0:4], 1)
	binary.BigEndian.PutUint32(result[4:8], binary.BigEndian.Uint32(packet[12:16]))
	binary.BigEndian.PutUint32(result[8:12], 60)
	binary.BigEndian.PutUint32(result[12:16], 4)
	binary.BigEndian.PutUint32(result[16:20], 5)
	for i, peer := range peers {
		a := peer.Addr().As4()
		copy(result[20+i*6:], a[:])
		binary.BigEndian.PutUint16(result[24+i*6:], peer.Port())
	}
	return result
}

func TestUDPAnnounceWireAndURLData(t *testing.T) {
	var announce []byte
	var conn *fixtureConn
	conn = newFixtureConn(func(packet []byte) {
		switch binary.BigEndian.Uint32(packet[8:12]) {
		case 0:
			conn.push(connectResponse(packet, 0x0102030405060708))
		case 1:
			announce = packet
			conn.push(announceResponse(packet, netip.MustParseAddrPort("127.0.0.1:6881")))
		}
	})
	clock := newFixtureClock()
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{conn: conn},
		Clock:    clock,
		Random:   bytesReader{0, 0, 0, 7, 0, 0, 0, 8},
	})
	result, err := client.Announce(context.Background(), "udp://tracker.test:6969/dir?a=b&c=d", testRequest())
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
	if !result.Transmitted || len(result.Peers) != 1 || result.Peers[0].String() != "127.0.0.1:6881" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := binary.BigEndian.Uint64(announce[0:8]); got != 0x0102030405060708 {
		t.Fatalf("connection ID = %#x", got)
	}
	if got := binary.BigEndian.Uint32(announce[8:12]); got != 1 {
		t.Fatalf("announce action = %d", got)
	}
	if got := binary.BigEndian.Uint32(announce[80:84]); got != uint32(EventStarted) {
		t.Fatalf("event = %d", got)
	}
	if got := binary.BigEndian.Uint16(announce[96:98]); got != 49152 {
		t.Fatalf("port = %d", got)
	}
	wantURL := append([]byte{2, 12}, []byte("/dir?a=b&c=d")...)
	if got := announce[98:]; string(got) != string(wantURL) {
		t.Fatalf("URLData = %x, want %x", got, wantURL)
	}
}

func TestUDPMismatchedTransactionIsLocalFailure(t *testing.T) {
	var conn *fixtureConn
	conn = newFixtureConn(func(packet []byte) {
		if binary.BigEndian.Uint32(packet[8:12]) == 0 {
			response := connectResponse(packet, 7)
			binary.BigEndian.PutUint32(response[4:8], binary.BigEndian.Uint32(packet[12:16])+1)
			conn.push(response)
		}
	})
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{conn: conn},
		Clock:    newFixtureClock(),
		Random:   bytesReader{0, 0, 0, 1},
	})
	result, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
	if err == nil || result.Transmitted {
		t.Fatalf("got result=%+v err=%v", result, err)
	}
	var trackerErr *Error
	if !errors.As(err, &trackerErr) || trackerErr.Code != ErrorTransaction || !trackerErr.Transmitted {
		t.Fatalf("error = %v", err)
	}
}

func TestUDPConnectionIDReuseAndExpiry(t *testing.T) {
	var connects, announces int
	newConn := func() net.Conn {
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			switch binary.BigEndian.Uint32(packet[8:12]) {
			case 0:
				connects++
				conn.push(connectResponse(packet, uint64(connects)))
			case 1:
				announces++
				conn.push(announceResponse(packet))
			}
		})
		return conn
	}
	clock := newFixtureClock()
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{factory: newConn},
		Clock:    clock,
		Random:   bytesReader{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4},
	})
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err != nil {
		t.Fatal(err)
	}
	clock.advance(59 * time.Second)
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err != nil {
		t.Fatal(err)
	}
	if connects != 1 || announces != 2 {
		t.Fatalf("before expiry: connects=%d announces=%d", connects, announces)
	}
	clock.advance(2 * time.Second)
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err != nil {
		t.Fatal(err)
	}
	if connects != 2 || announces != 3 {
		t.Fatalf("after expiry: connects=%d announces=%d", connects, announces)
	}
}

func TestUDPBEP15RetrySchedule(t *testing.T) {
	connectResponded := false
	var conn *fixtureConn
	conn = newFixtureConn(func(packet []byte) {
		if binary.BigEndian.Uint32(packet[8:12]) == 0 && !connectResponded {
			connectResponded = true
			conn.push(connectResponse(packet, 1))
		}
	})
	clock := newFixtureClock()
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{conn: conn},
		Clock:    clock,
		Random:   bytesReader{0, 0, 0, 1, 0, 0, 0, 2},
	})
	done := make(chan error, 1)
	go func() {
		_, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for (conn.writeCount() < 2 || clock.activeCount() != 1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if conn.writeCount() < 2 || clock.activeCount() != 1 {
		t.Fatalf("announce did not start cleanly (writes=%d active timers=%d)", conn.writeCount(), clock.activeCount())
	}
	for i := 0; i < maxTransactionRetries+2; i++ {
		timer := clock.nextActive(t)
		clock.fire(timer)
		if i < maxTransactionRetries {
			wantWrites := i + 3 // connect, initial announce, and this retry
			deadline := time.Now().Add(time.Second)
			for conn.writeCount() < wantWrites && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if conn.writeCount() < wantWrites {
				t.Fatalf("retry %d did not write", i)
			}
		}
	}
	select {
	case err := <-done:
		var trackerErr *Error
		if !errors.As(err, &trackerErr) || trackerErr.Code != ErrorTimeout || !trackerErr.Transmitted {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("announce did not finish after retries (writes=%d)", conn.writeCount())
	}
	// One connect write plus the initial announce and eight retransmissions.
	if got := conn.writeCount(); got != maxTransactionRetries+3 {
		t.Fatalf("writes = %d, want %d", got, maxTransactionRetries+3)
	}
}

func TestUDPAnnounceIPv6Peers(t *testing.T) {
	packet := make([]byte, 20+18)
	binary.BigEndian.PutUint32(packet[0:4], 1)
	binary.BigEndian.PutUint32(packet[4:8], 9)
	binary.BigEndian.PutUint32(packet[8:12], 1)
	addr := netip.MustParseAddr("2001:db8::1").As16()
	copy(packet[20:36], addr[:])
	binary.BigEndian.PutUint16(packet[36:38], 6881)
	result, err := parseAnnounceResponse(packet, 9, false)
	if err != nil || len(result.Peers) != 1 || result.Peers[0].String() != "[2001:db8::1]:6881" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestUDPCancelIsPrompt(t *testing.T) {
	conn := newFixtureConn(nil)
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{conn: conn},
		Clock:    newFixtureClock(),
		Random:   bytesReader{0, 0, 0, 1},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Announce(ctx, "udp://tracker.test:1", testRequest()); done <- err }()
	for conn.writeCount() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt transaction")
	}
}

func TestUDPClientCloseInterruptsInFlightExchange(t *testing.T) {
	var conn *fixtureConn
	responded := false
	conn = newFixtureConn(func(packet []byte) {
		if binary.BigEndian.Uint32(packet[8:12]) == 0 && !responded {
			responded = true
			conn.push(connectResponse(packet, 1))
		}
	})
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   fixtureDialer{conn: conn},
		Clock:    newFixtureClock(),
		Random:   bytesReader{0, 0, 0, 1, 0, 0, 0, 2},
	})
	done := make(chan error, 1)
	go func() {
		_, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for conn.writeCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if conn.writeCount() < 2 {
		t.Fatal("announce did not reach an in-flight exchange")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("announce unexpectedly succeeded after client close")
		}
	case <-time.After(time.Second):
		t.Fatal("client close did not interrupt announce")
	}
}

func FuzzParseAnnounceResponse(f *testing.F) {
	f.Add(make([]byte, 20))
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, packet []byte) {
		_, _ = parseAnnounceResponse(packet, 2, true)
		_, _ = parseAnnounceResponse(packet, 2, false)
	})
}

type bytesReader []byte

func (r bytesReader) Read(p []byte) (int, error) {
	if len(r) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r)
	return n, nil
}
