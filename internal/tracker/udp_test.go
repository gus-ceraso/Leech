package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureResolver struct{ ips []net.IPAddr }

func (r fixtureResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return append([]net.IPAddr(nil), r.ips...), nil
}

type fixtureResolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f fixtureResolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

type signaledContext struct {
	done    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newSignaledContext() *signaledContext {
	return &signaledContext{done: make(chan struct{}), entered: make(chan struct{})}
}
func (c *signaledContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *signaledContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.done
}
func (c *signaledContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}
func (c *signaledContext) Value(any) any { return nil }
func (c *signaledContext) cancel()       { close(c.done) }

type fixtureDialer struct {
	conn    *fixtureConn
	factory func() net.Conn
}

type fixtureDialFunc func(context.Context, string, string) (net.Conn, error)

func (f fixtureDialFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

func (d fixtureDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	if d.factory != nil {
		return d.factory(), nil
	}
	return d.conn, nil
}

type blockingDialer struct {
	entered chan struct{}
	release chan struct{}
	conn    net.Conn
}

func (d *blockingDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	close(d.entered)
	<-d.release
	return d.conn, nil
}

type fixtureConn struct {
	mu          sync.Mutex
	reads       chan []byte
	closed      chan struct{}
	closeOne    sync.Once
	onWrite     func([]byte)
	writes      [][]byte
	activeReads atomic.Int32
	onClose     func()
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
	c.activeReads.Add(1)
	defer c.activeReads.Add(-1)
	select {
	case data := <-c.reads:
		copy(p, data)
		return len(data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *fixtureConn) Close() error {
	c.closeOne.Do(func() {
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	})
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
	delay   time.Duration
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
func (c *fixtureClock) NewTimer(delay time.Duration) Timer {
	t := &fixtureTimer{ch: make(chan time.Time, 1), delay: delay}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	select {
	case c.changed <- struct{}{}:
	default:
	}
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
func (c *fixtureClock) nextActiveDelay(t *testing.T, delay time.Duration) *fixtureTimer {
	t.Helper()
	for {
		c.mu.Lock()
		for _, timer := range c.timers {
			timer.mu.Lock()
			active := !timer.stopped && !timer.fired && timer.delay == delay
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
			t.Fatalf("timed out waiting for an active %v timer", delay)
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

func announceResponse6(packet []byte, peer netip.AddrPort) []byte {
	response := append(announceResponse(packet), make([]byte, 18)...)
	addr := peer.Addr().As16()
	copy(response[20:36], addr[:])
	binary.BigEndian.PutUint16(response[36:38], peer.Port())
	return response
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

func TestUDPAnnounceFamiliesTransmitIndependently(t *testing.T) {
	v4Started := make(chan struct{})
	v4Announced := make(chan []byte, 1)
	v6Announced := make(chan []byte, 1)
	var v4, v6 *fixtureConn
	v4 = newFixtureConn(func(packet []byte) {
		switch binary.BigEndian.Uint32(packet[8:12]) {
		case 0:
			v4.push(connectResponse(packet, 1))
		case 1:
			v4Announced <- packet
			close(v4Started)
		}
	})
	v6 = newFixtureConn(func(packet []byte) {
		switch binary.BigEndian.Uint32(packet[8:12]) {
		case 0:
			v6.push(connectResponse(packet, 2))
		case 1:
			v6Announced <- packet
			v6.push(announceResponse6(packet, netip.MustParseAddrPort("[2001:db8::2]:6881")))
		}
	})
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("::1")}}},
		Dialer: fixtureDialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
			switch network {
			case "udp4":
				return v4, nil
			case "udp6":
				select {
				case <-v4Started:
					return v6, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, errors.New("unexpected address family")
		}),
		Clock:  newFixtureClock(),
		Random: bytesReader{0, 0, 0, 1},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type announceOutcome struct {
		result AnnounceResult
		err    error
	}
	done := make(chan announceOutcome, 1)
	go func() {
		result, err := client.Announce(ctx, "udp://tracker.test:6969/dir?a=b", testRequest())
		done <- announceOutcome{result, err}
	}()
	var first, second []byte
	select {
	case first = <-v4Announced:
	case <-time.After(time.Second):
		t.Fatal("IPv4 announce did not start")
	}
	select {
	case second = <-v6Announced:
	case <-time.After(time.Second):
		t.Fatal("IPv6 announce was blocked by the silent IPv4 transaction")
	}
	if want := string(append([]byte{2, 8}, []byte("/dir?a=b")...)); string(first[98:]) != want || string(second[98:]) != want {
		t.Fatalf("family URL data differs: IPv4=%x IPv6=%x", first[98:], second[98:])
	}
	if string(first[16:]) != string(second[16:]) {
		t.Fatal("families announced different torrent identity or counters")
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("announce: %v", got.err)
		}
		if !got.result.Transmitted || got.result.Interval != time.Minute || got.result.Leechers != 4 || got.result.Seeders != 5 || len(got.result.Peers) != 1 || got.result.Peers[0].String() != "[2001:db8::2]:6881" {
			t.Fatalf("aggregate result: %+v", got.result)
		}
		if len(got.result.Families) != 2 || !got.result.Families[0].Endpoint.Addr().Is4() || !got.result.Families[1].Endpoint.Addr().Is6() || !got.result.Families[0].Transmitted || !got.result.Families[1].Transmitted || got.result.Families[1].Err != nil {
			t.Fatalf("family order or accounting: %+v", got.result.Families)
		}
		if !errors.Is(got.result.Families[0].Err, context.Canceled) || v4.activeReads.Load() != 0 {
			t.Fatalf("silent IPv4 transaction was not canceled and joined: %+v", got.result.Families[0])
		}
	case <-time.After(time.Second):
		t.Fatal("IPv4 retry schedule held the usable IPv6 response")
	}
}

func TestUDPAnnounceGraceBoundsStalledFamilyConnect(t *testing.T) {
	clock := newFixtureClock()
	v4Started := make(chan struct{})
	var v4, v6 *fixtureConn
	v4 = newFixtureConn(func(packet []byte) {
		if binary.BigEndian.Uint32(packet[8:12]) == 0 {
			close(v4Started)
		}
	})
	v6 = newFixtureConn(func(packet []byte) {
		switch binary.BigEndian.Uint32(packet[8:12]) {
		case 0:
			<-v4Started
			v6.push(connectResponse(packet, 2))
		case 1:
			v6.push(announceResponse6(packet, netip.MustParseAddrPort("[2001:db8::3]:6882")))
		}
	})
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("::1")}}},
		Dialer: fixtureDialFunc(func(_ context.Context, network, _ string) (net.Conn, error) {
			switch network {
			case "udp4":
				return v4, nil
			case "udp6":
				return v6, nil
			}
			return nil, errors.New("unexpected address family")
		}),
		Clock:  clock,
		Random: bytesReader{0, 0, 0, 1},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type announceOutcome struct {
		result AnnounceResult
		err    error
	}
	done := make(chan announceOutcome, 1)
	go func() {
		result, err := client.Announce(ctx, "udp://tracker.test:1", testRequest())
		done <- announceOutcome{result, err}
	}()
	clock.fire(clock.nextActiveDelay(t, familyAnnounceGrace))
	select {
	case got := <-done:
		if got.err != nil || !got.result.Transmitted || got.result.Interval != time.Minute || len(got.result.Peers) != 1 || got.result.Peers[0].String() != "[2001:db8::3]:6882" {
			t.Fatalf("usable response after grace: result=%+v err=%v", got.result, got.err)
		}
		if len(got.result.Families) != 2 || got.result.Families[0].Transmitted || !got.result.Families[1].Transmitted || !errors.Is(got.result.Families[0].Err, context.Canceled) {
			t.Fatalf("family accounting after grace: %+v", got.result.Families)
		}
		if v4.writeCount() != 1 || v4.activeReads.Load() != 0 {
			t.Fatal("stalled connect kept retrying or its reader was not joined")
		}
		select {
		case <-v4.closed:
		default:
			t.Fatal("stalled connect socket remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("stalled connect held the usable IPv6 response past the grace")
	}
}

func TestUDPAnnounceCancellationJoinsBothFamilies(t *testing.T) {
	announced := make(chan string, 2)
	newFamily := func(network string) *fixtureConn {
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			switch binary.BigEndian.Uint32(packet[8:12]) {
			case 0:
				conn.push(connectResponse(packet, 1))
			case 1:
				announced <- network
			}
		})
		return conn
	}
	v4, v6 := newFamily("udp4"), newFamily("udp6")
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("::1")}, {IP: net.ParseIP("127.0.0.1")}}},
		Dialer: fixtureDialFunc(func(_ context.Context, network, _ string) (net.Conn, error) {
			switch network {
			case "udp4":
				return v4, nil
			case "udp6":
				return v6, nil
			}
			return nil, errors.New("unexpected address family")
		}),
		Clock:  newFixtureClock(),
		Random: bytesReader{0, 0, 0, 1},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type announceOutcome struct {
		result AnnounceResult
		err    error
	}
	done := make(chan announceOutcome, 1)
	go func() {
		result, err := client.Announce(ctx, "udp://tracker.test:1", testRequest())
		done <- announceOutcome{result, err}
	}()
	for range 2 {
		select {
		case <-announced:
		case <-time.After(time.Second):
			t.Fatal("both families did not transmit before cancellation")
		}
	}
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got.err, context.Canceled) || !got.result.Transmitted || len(got.result.Families) != 2 {
			t.Fatalf("canceled result=%+v err=%v", got.result, got.err)
		}
		if !got.result.Families[0].Endpoint.Addr().Is6() || !got.result.Families[1].Endpoint.Addr().Is4() {
			t.Fatalf("family order = %+v", got.result.Families)
		}
		for _, family := range got.result.Families {
			if !family.Transmitted || !errors.Is(family.Err, context.Canceled) {
				t.Fatalf("family after cancellation = %+v", family)
			}
		}
		if v4.activeReads.Load() != 0 || v6.activeReads.Load() != 0 {
			t.Fatal("announce returned before both socket readers joined")
		}
		select {
		case <-v4.closed:
		default:
			t.Fatal("IPv4 socket remains open after cancellation")
		}
		select {
		case <-v6.closed:
		default:
			t.Fatal("IPv6 socket remains open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join both families")
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

func TestUDPRotatingEndpointsRetainBoundedSessions(t *testing.T) {
	const endpointCount = udpSessionCapacity + 2
	var selected net.IPAddr
	resolver := fixtureResolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{selected}, nil
	})
	var created []*fixtureConn
	var open atomic.Int32
	dialer := fixtureDialFunc(func(_ context.Context, _ string, address string) (net.Conn, error) {
		seq := len(created) + 1
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			switch binary.BigEndian.Uint32(packet[8:12]) {
			case 0:
				conn.push(connectResponse(packet, uint64(seq)))
			case 1:
				conn.push(announceResponse(packet))
			}
		})
		conn.onClose = func() { open.Add(-1) }
		created = append(created, conn)
		open.Add(1)
		return conn, nil
	})
	client := NewUDPClient(Config{Resolver: resolver, Dialer: dialer, Clock: newFixtureClock()})
	defer client.Close()

	addresses := make([]net.IPAddr, endpointCount)
	for i := range addresses {
		addresses[i] = net.IPAddr{IP: net.IPv4(10, byte(i>>8), byte(i), 1)}
		selected = addresses[i]
		result, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		if err != nil || !result.Transmitted {
			t.Fatalf("announce endpoint %d: result=%+v err=%v", i, result, err)
		}
		if got := len(client.sessions); got > udpSessionCapacity {
			t.Fatalf("retained %d sessions, capacity %d", got, udpSessionCapacity)
		}
		if got := open.Load(); got > udpSessionCapacity {
			t.Fatalf("owned %d open sockets, capacity %d", got, udpSessionCapacity)
		}
	}
	if len(created) != endpointCount || open.Load() != udpSessionCapacity {
		t.Fatalf("created=%d open=%d, want %d and %d", len(created), open.Load(), endpointCount, udpSessionCapacity)
	}
	evicted := -1
	for i, conn := range created {
		select {
		case <-conn.closed:
			if evicted < 0 {
				evicted = i
			}
		default:
		}
	}
	if evicted < 0 {
		t.Fatal("no endpoint socket was retired")
	}

	// The most recently retained endpoint keeps its connection ID and socket.
	selected = addresses[endpointCount-1]
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err != nil {
		t.Fatal(err)
	}
	if len(created) != endpointCount || binary.BigEndian.Uint64(created[endpointCount-1].writeAt(1)[:8]) != uint64(endpointCount) {
		t.Fatal("retained endpoint did not reuse its connection ID")
	}

	// An evicted endpoint reconnects and receives a fresh ID.
	selected = addresses[evicted]
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err != nil {
		t.Fatal(err)
	}
	if len(created) != endpointCount+1 || binary.BigEndian.Uint64(created[endpointCount].writeAt(1)[:8]) != endpointCount+1 {
		t.Fatal("evicted endpoint did not reconnect with a fresh connection ID")
	}
	if got := open.Load(); got != udpSessionCapacity {
		t.Fatalf("open sockets after replacement = %d, want %d", got, udpSessionCapacity)
	}
}

func TestUDPTransactionLockWaiterKeepsSessionFromEviction(t *testing.T) {
	client := NewUDPClient(Config{})
	endpoint := netip.MustParseAddrPort("192.0.2.1:1")
	key := sessionKey(&udpSession{endpoint: endpoint})
	active, err := client.acquireSession(context.Background(), key, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := client.acquireSession(context.Background(), key, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var retained []*udpSession
	for i := 0; i < udpSessionCapacity-2; i++ {
		addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: addr}), addr)
		if err != nil {
			t.Fatal(err)
		}
		retained = append(retained, s)
	}
	spareEndpoint := netip.MustParseAddrPort("192.0.2.254:1")
	spare, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: spareEndpoint}), spareEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	client.releaseSession(active)
	client.releaseSession(spare)

	target := netip.MustParseAddrPort("198.51.100.1:1")
	got, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: target}), target)
	if err != nil {
		t.Fatal(err)
	}
	if client.sessions[key] != waiter || waiter.users != 1 {
		t.Fatal("session with a transaction-lock waiter was evicted")
	}
	client.releaseSession(waiter)
	client.releaseSession(got)
	for _, s := range retained {
		client.releaseSession(s)
	}
	_ = client.Close()
}

func TestUDPBusySessionCapacityWaitIsCancelableAndCloseWakes(t *testing.T) {
	client := NewUDPClient(Config{})
	held := make([]*udpSession, 0, udpSessionCapacity)
	for i := 0; i < udpSessionCapacity; i++ {
		endpoint := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: endpoint}), endpoint)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, s)
	}
	ctx := newSignaledContext()
	wait := make(chan error, 1)
	go func() {
		endpoint := netip.MustParseAddrPort("192.0.2.1:1")
		_, err := client.acquireSession(ctx, sessionKey(&udpSession{endpoint: endpoint}), endpoint)
		wait <- err
	}()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("capacity waiter did not reach its cancellable wait")
	}
	ctx.cancel()
	select {
	case err := <-wait:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("capacity wait error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled capacity waiter did not return")
	}
	if len(client.sessions) != udpSessionCapacity {
		t.Fatalf("capacity wait grew cache to %d", len(client.sessions))
	}

	closeCtx := newSignaledContext()
	closeWait := make(chan error, 1)
	go func() {
		endpoint := netip.MustParseAddrPort("192.0.2.2:1")
		_, err := client.acquireSession(closeCtx, sessionKey(&udpSession{endpoint: endpoint}), endpoint)
		closeWait <- err
	}()
	select {
	case <-closeCtx.entered:
	case <-time.After(time.Second):
		t.Fatal("close waiter did not reach its cancellable wait")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closeWait:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("close-woken waiter error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client close did not wake capacity waiter")
	}
	for _, s := range held {
		client.releaseSession(s)
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

func TestUDPAnnounceRefreshesConnectionIDAtExpiredRetry(t *testing.T) {
	clock := newFixtureClock()
	clock.now = time.Unix(0, 0)
	var conns []*fixtureConn
	created := make(chan *fixtureConn, 2)
	var events []string
	var refreshAt time.Time
	dialer := fixtureDialer{factory: func() net.Conn {
		index := len(conns)
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			action := binary.BigEndian.Uint32(packet[8:12])
			if index == 0 {
				switch action {
				case 0:
					clock.advance(time.Second) // connection ID arrives at t=1
					conn.push(connectResponse(packet, 1))
				case 1:
					if len(events) == 0 {
						clock.advance(time.Second) // first announce is sent at t=2
					}
					events = append(events, "announce-old")
				}
				return
			}
			switch action {
			case 0:
				events = append(events, "connect-refresh")
				refreshAt = clock.Now()
				conn.push(connectResponse(packet, 2))
			case 1:
				events = append(events, "announce-fresh")
				conn.push(announceResponse(packet))
			}
		})
		conns = append(conns, conn)
		created <- conn
		return conn
	}}
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   dialer,
		Clock:    clock,
		Random:   bytesReader{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3},
	})
	done := make(chan error, 1)
	go func() {
		_, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		done <- err
	}()
	oldConn := waitFixtureConn(t, created)
	waitFixtureWrites(t, oldConn, 2)
	for i, delay := range []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second} {
		timer := clock.nextActive(t)
		clock.advance(delay)
		clock.fire(timer)
		if i < 2 {
			waitFixtureWrites(t, oldConn, 3+i)
		}
	}
	newConn := waitFixtureConn(t, created)
	waitFixtureWrites(t, newConn, 2)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("announce: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("announce did not finish after refreshing the connection ID")
	}
	if got := binary.BigEndian.Uint64(newConn.writeAt(1)[:8]); got != 2 {
		t.Fatalf("refreshed announce connection ID = %d, want 2", got)
	}
	if len(events) != 5 || events[0] != "announce-old" || events[1] != "announce-old" || events[2] != "announce-old" || events[3] != "connect-refresh" || events[4] != "announce-fresh" {
		t.Fatalf("packet order = %v", events)
	}
	if want := time.Unix(0, 0).Add(107 * time.Second); !refreshAt.Equal(want) {
		t.Fatalf("refresh connect sent at %v, want %v", refreshAt, want)
	}
	if got := oldConn.writeCount(); got != 4 {
		t.Fatalf("old socket writes = %d, want connect + initial and two pre-expiry retries", got)
	}
}

func TestUDPCancelDuringAnnounceConnectionRefresh(t *testing.T) {
	clock := newFixtureClock()
	clock.now = time.Unix(0, 0)
	var conns []*fixtureConn
	created := make(chan *fixtureConn, 2)
	dialer := fixtureDialer{factory: func() net.Conn {
		index := len(conns)
		firstAnnounce := true
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			switch binary.BigEndian.Uint32(packet[8:12]) {
			case 0:
				if index == 0 {
					clock.advance(time.Second)
					conn.push(connectResponse(packet, 1))
				}
			case 1:
				if index == 0 && firstAnnounce {
					clock.advance(time.Second)
					firstAnnounce = false
				}
			}
		})
		conns = append(conns, conn)
		created <- conn
		return conn
	}}
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   dialer,
		Clock:    clock,
		Random:   bytesReader{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Announce(ctx, "udp://tracker.test:1", testRequest()); done <- err }()
	oldConn := waitFixtureConn(t, created)
	waitFixtureWrites(t, oldConn, 2)
	for i, delay := range []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second} {
		timer := clock.nextActive(t)
		clock.advance(delay)
		clock.fire(timer)
		if i < 2 {
			waitFixtureWrites(t, oldConn, 3+i)
		}
	}
	newConn := waitFixtureConn(t, created)
	waitFixtureWrites(t, newConn, 1)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("announce error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join the reconnect transaction")
	}
	select {
	case <-newConn.closed:
	default:
		t.Fatal("reconnect socket remains open after cancellation")
	}
}

func waitFixtureConn(t *testing.T, created <-chan *fixtureConn) *fixtureConn {
	t.Helper()
	select {
	case conn := <-created:
		return conn
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a tracker socket")
		return nil
	}
}

func waitFixtureWrites(t *testing.T, conn *fixtureConn, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if conn.writeCount() >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("socket writes = %d, want at least %d", conn.writeCount(), count)
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

func TestUDPClientCloseDuringDialDoesNotInstallSocket(t *testing.T) {
	dialer := &blockingDialer{entered: make(chan struct{}), release: make(chan struct{}), conn: newFixtureConn(nil)}
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer:   dialer,
		Clock:    newFixtureClock(),
		Random:   bytesReader{0, 0, 0, 1},
	})
	done := make(chan error, 1)
	go func() {
		_, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		done <- err
	}()
	select {
	case <-dialer.entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close during dial: %v", err)
	}
	close(dialer.release)
	select {
	case err := <-done:
		var trackerErr *Error
		if !errors.As(err, &trackerErr) || trackerErr.Code != ErrorClosed {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("announce did not finish after close during dial")
	}
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err == nil {
		t.Fatal("announce after close unexpectedly succeeded")
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
