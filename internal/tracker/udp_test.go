package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"runtime"
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
	done       chan struct{}
	entered    chan struct{}
	once       sync.Once
	cancelOnce sync.Once
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
func (c *signaledContext) cancel()       { c.cancelOnce.Do(func() { close(c.done) }) }

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
	const endpointCount = 2*udpSessionCapacity + 1
	busyEndpoint := netip.MustParseAddrPort("[2001:db8::ffff]:1")
	busyIP := net.IPAddr{IP: net.ParseIP("2001:db8::ffff")}
	var selectionMu sync.RWMutex
	selected := busyIP
	resolver := fixtureResolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		selectionMu.RLock()
		ip := net.IPAddr{IP: append(net.IP(nil), selected.IP...), Zone: selected.Zone}
		selectionMu.RUnlock()
		return []net.IPAddr{ip}, nil
	})
	setSelected := func(ip net.IPAddr) {
		selectionMu.Lock()
		selected = net.IPAddr{IP: append(net.IP(nil), ip.IP...), Zone: ip.Zone}
		selectionMu.Unlock()
	}
	type socketRecord struct {
		endpoint netip.AddrPort
		id       uint64
		conn     *fixtureConn
	}
	var socketsMu sync.Mutex
	var sockets []socketRecord
	var open, maxOpen, maxSessions atomic.Int32
	recordMaximum := func(counter *atomic.Int32, value int32) {
		for old := counter.Load(); value > old; old = counter.Load() {
			if counter.CompareAndSwap(old, value) {
				return
			}
		}
	}
	busyAnnounceEntered := make(chan struct{})
	var busyAnnounceOnce sync.Once
	busyRelease := make(chan struct{})
	var busyReleaseOnce sync.Once
	releaseBusy := func() { busyReleaseOnce.Do(func() { close(busyRelease) }) }
	clock := newFixtureClock()
	var client *UDPClient
	dialer := fixtureDialFunc(func(_ context.Context, network, address string) (net.Conn, error) {
		endpoint, err := netip.ParseAddrPort(address)
		if err != nil {
			return nil, err
		}
		if endpoint.Addr().Is4() && network != "udp4" || endpoint.Addr().Is6() && network != "udp6" {
			return nil, errors.New("UDP socket family does not match endpoint")
		}
		socketsMu.Lock()
		id := uint64(len(sockets) + 1)
		var conn *fixtureConn
		conn = newFixtureConn(func(packet []byte) {
			tx := packet[12:16]
			switch binary.BigEndian.Uint32(packet[8:12]) {
			case 0:
				// BEP 15 connect response: action, transaction ID, connection ID.
				conn.push([]byte{
					0, 0, 0, 0, tx[0], tx[1], tx[2], tx[3],
					byte(id >> 56), byte(id >> 48), byte(id >> 40), byte(id >> 32),
					byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id),
				})
			case 1:
				if endpoint == busyEndpoint {
					busyAnnounceOnce.Do(func() { close(busyAnnounceEntered) })
					<-busyRelease
				}
				if endpoint.Addr().Is4() {
					// Independent BEP 15 IPv4 announce response and one compact peer.
					conn.push([]byte{
						0, 0, 0, 1, tx[0], tx[1], tx[2], tx[3],
						0, 0, 0, 60, 0, 0, 0, 4, 0, 0, 0, 5,
						203, 0, 113, 9, 0x1a, 0xe1,
					})
				} else {
					// Independent BEP 15 IPv6 announce response and one 18-byte peer.
					conn.push([]byte{
						0, 0, 0, 1, tx[0], tx[1], tx[2], tx[3],
						0, 0, 0, 60, 0, 0, 0, 4, 0, 0, 0, 5,
						0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0,
						0, 0, 0, 0, 0, 0, 0, 0x99, 0x1a, 0xe2,
					})
				}
			}
		})
		conn.onClose = func() { open.Add(-1) }
		sockets = append(sockets, socketRecord{endpoint: endpoint, id: id, conn: conn})
		socketsMu.Unlock()
		currentOpen := open.Add(1)
		recordMaximum(&maxOpen, currentOpen)
		client.mu.Lock()
		currentSessions := len(client.sessions)
		client.mu.Unlock()
		recordMaximum(&maxSessions, int32(currentSessions))
		return conn, nil
	})
	client = NewUDPClient(Config{Resolver: resolver, Dialer: dialer, Clock: clock})
	var busyJoined bool
	busyDone := make(chan struct {
		result AnnounceResult
		err    error
	}, 1)
	t.Cleanup(func() {
		releaseBusy()
		if !busyJoined {
			select {
			case <-busyDone:
			case <-time.After(time.Second):
				t.Error("timed out joining the held IPv6 announce")
			}
		}
		if err := client.Close(); err != nil {
			t.Errorf("client close: %v", err)
		}
	})

	addresses := make([]net.IPAddr, endpointCount)
	endpoints := make([]netip.AddrPort, endpointCount)
	for i := range addresses {
		var ip net.IP
		var addr netip.Addr
		if i%2 == 0 {
			raw := [4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}
			ip, addr = net.IP(raw[:]), netip.AddrFrom4(raw)
		} else {
			raw := [16]byte{0x20, 0x01, 0x0d, 0xb8}
			binary.BigEndian.PutUint64(raw[8:], uint64(i+1))
			ip, addr = net.IP(raw[:]), netip.AddrFrom16(raw)
		}
		addresses[i] = net.IPAddr{IP: ip}
		endpoints[i] = netip.AddrPortFrom(addr, 1)
	}

	validateResult := func(result AnnounceResult, endpoint netip.AddrPort) {
		t.Helper()
		if !result.Transmitted || result.Interval != time.Minute || len(result.Families) != 1 || result.Families[0].Endpoint != endpoint || result.Families[0].Err != nil {
			t.Fatalf("announce %s result = %+v", endpoint, result)
		}
		wantPeer := netip.MustParseAddrPort("[2001:db8::99]:6882")
		if endpoint.Addr().Is4() {
			wantPeer = netip.MustParseAddrPort("203.0.113.9:6881")
		}
		if len(result.Peers) != 1 || result.Peers[0] != wantPeer {
			t.Fatalf("announce %s peers = %v, want %s", endpoint, result.Peers, wantPeer)
		}
	}
	ipForEndpoint := func(endpoint netip.AddrPort) net.IPAddr {
		return net.IPAddr{IP: net.IP(endpoint.Addr().AsSlice())}
	}
	announce := func(ip net.IPAddr, endpoint netip.AddrPort) AnnounceResult {
		t.Helper()
		setSelected(ip)
		result, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		if err != nil {
			t.Fatalf("announce endpoint %s: %v", endpoint, err)
		}
		validateResult(result, endpoint)
		return result
	}
	snapshotSockets := func() []socketRecord {
		socketsMu.Lock()
		defer socketsMu.Unlock()
		return append([]socketRecord(nil), sockets...)
	}
	checkBounds := func() {
		t.Helper()
		client.mu.Lock()
		retained := len(client.sessions)
		client.mu.Unlock()
		recordMaximum(&maxSessions, int32(retained))
		if retained > udpSessionCapacity {
			t.Fatalf("retained %d sessions, capacity %d", retained, udpSessionCapacity)
		}
		if got := open.Load(); got > udpSessionCapacity {
			t.Fatalf("owned %d open sockets, capacity %d", got, udpSessionCapacity)
		}
	}
	closed := func(conn *fixtureConn) bool {
		select {
		case <-conn.closed:
			return true
		default:
			return false
		}
	}
	findSocket := func(endpoint netip.AddrPort) socketRecord {
		t.Helper()
		for _, socket := range snapshotSockets() {
			if socket.endpoint == endpoint {
				return socket
			}
		}
		t.Fatalf("no fixture socket for %s", endpoint)
		return socketRecord{}
	}
	findLatestSocket := func(endpoint netip.AddrPort) socketRecord {
		t.Helper()
		all := snapshotSockets()
		for i := len(all) - 1; i >= 0; i-- {
			if all[i].endpoint == endpoint {
				return all[i]
			}
		}
		t.Fatalf("no fixture socket for %s", endpoint)
		return socketRecord{}
	}
	verifyNewSocket := func(socket socketRecord) {
		t.Helper()
		if socket.conn.writeCount() != 2 {
			t.Fatalf("fresh socket for %s writes = %d, want connect then announce", socket.endpoint, socket.conn.writeCount())
		}
		connect, announce := socket.conn.writeAt(0), socket.conn.writeAt(1)
		if binary.BigEndian.Uint32(connect[8:12]) != 0 || binary.BigEndian.Uint32(announce[8:12]) != 1 || binary.BigEndian.Uint64(announce[:8]) != socket.id {
			t.Fatalf("fresh socket for %s did not connect before announcing with ID %d", socket.endpoint, socket.id)
		}
	}

	// Hold one IPv6 transaction active while more than twice the cache capacity
	// in mixed-family endpoints rotate through the actual production bound.
	setSelected(busyIP)
	go func() {
		result, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest())
		busyDone <- struct {
			result AnnounceResult
			err    error
		}{result, err}
	}()
	select {
	case <-busyAnnounceEntered:
	case <-time.After(time.Second):
		t.Fatal("IPv6 transaction did not become busy")
	}
	busyKey := sessionKey(&udpSession{endpoint: busyEndpoint})
	client.mu.Lock()
	busySession := client.sessions[busyKey]
	busyUsers := 0
	if busySession != nil {
		busyUsers = busySession.users
	}
	client.mu.Unlock()
	if busySession == nil || busyUsers != 1 {
		t.Fatalf("busy IPv6 session = %p with %d users, want one active user", busySession, busyUsers)
	}
	busySocket := findSocket(busyEndpoint)

	for i, endpoint := range endpoints {
		announce(addresses[i], endpoint)
		checkBounds()
		client.mu.Lock()
		stillBusy := client.sessions[busyKey] == busySession && busySession.users == 1
		client.mu.Unlock()
		if !stillBusy || closed(busySocket.conn) {
			t.Fatal("capacity pressure retired the active IPv6 session")
		}
	}
	if got := len(snapshotSockets()); got != endpointCount+1 || open.Load() != udpSessionCapacity {
		t.Fatalf("rotated endpoints: created=%d open=%d, want %d and %d", got, open.Load(), endpointCount+1, udpSessionCapacity)
	}
	if maxOpen.Load() > udpSessionCapacity || maxSessions.Load() > udpSessionCapacity {
		t.Fatalf("observed peak sessions=%d open sockets=%d, capacity=%d", maxSessions.Load(), maxOpen.Load(), udpSessionCapacity)
	}

	// Pressure has verified the busy IPv6 entry was not evicted. Finish and
	// join it before selecting idle retained endpoints from either family.
	releaseBusy()
	select {
	case outcome := <-busyDone:
		busyJoined = true
		if outcome.err != nil {
			t.Fatalf("held IPv6 announce: %v", outcome.err)
		}
		validateResult(outcome.result, busyEndpoint)
	case <-time.After(time.Second):
		t.Fatal("held IPv6 announce did not finish after release")
	}
	checkBounds()
	client.mu.Lock()
	busyUsers = busySession.users
	client.mu.Unlock()
	if busyUsers != 0 {
		t.Fatalf("joined IPv6 announce leaked %d session users", busyUsers)
	}

	client.mu.Lock()
	retained4, retained6 := 0, 0
	for _, session := range client.sessions {
		session.connMu.RLock()
		conn := session.conn
		session.connMu.RUnlock()
		fixture, ok := conn.(*fixtureConn)
		if !ok || closed(fixture) {
			client.mu.Unlock()
			t.Fatalf("retained endpoint %s has no open fixture socket", session.endpoint)
		}
		if session.endpoint.Addr().Is4() {
			retained4++
		} else {
			retained6++
		}
	}
	client.mu.Unlock()
	if retained4 == 0 || retained6 == 0 || retained4+retained6 != udpSessionCapacity || int32(retained4+retained6) != open.Load() {
		t.Fatalf("retained family sessions: IPv4=%d IPv6=%d open=%d, want both within total capacity %d", retained4, retained6, open.Load(), udpSessionCapacity)
	}
	retired4, retired6 := 0, 0
	evicted := make(map[bool]netip.AddrPort, 2)
	for _, socket := range snapshotSockets() {
		if !closed(socket.conn) {
			continue
		}
		family := socket.endpoint.Addr().Is4()
		if family {
			retired4++
		} else {
			retired6++
		}
		if socket.endpoint != busyEndpoint {
			if _, found := evicted[family]; !found {
				evicted[family] = socket.endpoint
			}
		}
		client.mu.Lock()
		stillOwned := false
		if session := client.sessions[sessionKey(&udpSession{endpoint: socket.endpoint})]; session != nil {
			session.connMu.RLock()
			stillOwned = session.conn == socket.conn
			session.connMu.RUnlock()
		}
		client.mu.Unlock()
		if stillOwned {
			t.Fatalf("retired socket for %s remains owned", socket.endpoint)
		}
	}
	if retired4 == 0 || retired6 == 0 || retired4+retired6 < endpointCount+1-udpSessionCapacity {
		t.Fatalf("retired family sockets: IPv4=%d IPv6=%d", retired4, retired6)
	}

	// Select actual idle sessions under the client lock; eviction order is
	// intentionally arbitrary, so no particular rotating endpoint is promised.
	retained := make(map[bool]netip.AddrPort, 2)
	client.mu.Lock()
	for _, session := range client.sessions {
		if session.users != 0 {
			continue
		}
		family := session.endpoint.Addr().Is4()
		if _, found := retained[family]; !found {
			retained[family] = session.endpoint
		}
	}
	client.mu.Unlock()
	retainedV4, hasV4 := retained[true]
	retainedV6, hasV6 := retained[false]
	if !hasV4 || !hasV6 {
		t.Fatalf("no idle retained endpoint for both families: IPv4=%v IPv6=%v", hasV4, hasV6)
	}
	for _, endpoint := range []netip.AddrPort{retainedV6, retainedV4} {
		oldSocket := findSocket(endpoint)
		if closed(oldSocket.conn) {
			t.Fatalf("retained endpoint %s was retired", endpoint)
		}
		writes := oldSocket.conn.writeCount()
		announce(ipForEndpoint(endpoint), endpoint)
		if oldSocket.conn.writeCount() != writes+1 || binary.BigEndian.Uint64(oldSocket.conn.writeAt(writes)[:8]) != oldSocket.id {
			t.Fatalf("retained endpoint %s did not reuse connection ID %d", endpoint, oldSocket.id)
		}
	}
	if got := len(snapshotSockets()); got != endpointCount+1 {
		t.Fatalf("retained revisits opened %d sockets, want none", got-endpointCount-1)
	}

	// Expired IPv4 and IPv6 IDs must reconnect before the next announce.
	clock.advance(connectionLife + time.Second)
	for _, endpoint := range []netip.AddrPort{retainedV6, retainedV4} {
		oldSocket := findSocket(endpoint)
		before := len(snapshotSockets())
		announce(ipForEndpoint(endpoint), endpoint)
		freshSocket := findLatestSocket(endpoint)
		if len(snapshotSockets()) != before+1 || freshSocket.conn == oldSocket.conn || !closed(oldSocket.conn) {
			t.Fatalf("expired endpoint %s did not replace its old socket", endpoint)
		}
		verifyNewSocket(freshSocket)
		checkBounds()
	}

	// At least one retired endpoint from each family reconnects with a new ID.
	for _, is4 := range []bool{true, false} {
		endpoint, ok := evicted[is4]
		if !ok {
			t.Fatalf("no retired %s endpoint to revisit", map[bool]string{true: "IPv4", false: "IPv6"}[is4])
		}
		oldSocket := findSocket(endpoint)
		client.mu.Lock()
		stillRetained := client.sessions[sessionKey(&udpSession{endpoint: endpoint})] != nil
		client.mu.Unlock()
		if stillRetained {
			t.Fatalf("evicted endpoint %s remains in the cache", endpoint)
		}
		before := len(snapshotSockets())
		announce(ipForEndpoint(endpoint), endpoint)
		freshSocket := findLatestSocket(endpoint)
		if len(snapshotSockets()) != before+1 || freshSocket.conn == oldSocket.conn || freshSocket.id == oldSocket.id {
			t.Fatalf("evicted endpoint %s did not reconnect with a fresh ID", endpoint)
		}
		verifyNewSocket(freshSocket)
		checkBounds()
	}
	if maxOpen.Load() > udpSessionCapacity || maxSessions.Load() > udpSessionCapacity {
		t.Fatalf("replacement exceeded capacity: peak sessions=%d open sockets=%d", maxSessions.Load(), maxOpen.Load())
	}
	client.mu.Lock()
	for _, session := range client.sessions {
		if session.users != 0 {
			client.mu.Unlock()
			t.Fatalf("endpoint %s leaked %d session users", session.endpoint, session.users)
		}
	}
	client.mu.Unlock()
	for _, socket := range snapshotSockets() {
		if reads := socket.conn.activeReads.Load(); reads != 0 {
			t.Fatalf("fixture socket for %s has %d active readers", socket.endpoint, reads)
		}
	}
}

func TestUDPTransactionLockWaiterKeepsSessionFromEviction(t *testing.T) {
	endpoint := netip.MustParseAddrPort("127.0.0.1:1")
	var conn *fixtureConn
	conn = newFixtureConn(func(packet []byte) {
		switch binary.BigEndian.Uint32(packet[8:12]) {
		case 0:
			conn.push(connectResponse(packet, 1))
		case 1:
			conn.push(announceResponse(packet))
		}
	})
	resolved := make(chan struct{}, 2)
	client := NewUDPClient(Config{
		Resolver: fixtureResolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
			resolved <- struct{}{}
			return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
		}),
		Dialer: fixtureDialer{conn: conn},
		Clock:  newFixtureClock(),
	})
	defer client.Close()
	trackerURL := "udp://tracker.test:1"
	if _, err := client.Announce(context.Background(), trackerURL, testRequest()); err != nil {
		t.Fatal(err)
	}
	<-resolved
	key := sessionKey(&udpSession{endpoint: endpoint})
	client.mu.Lock()
	active := client.sessions[key]
	client.mu.Unlock()
	if active == nil {
		t.Fatal("successful announce did not retain its session")
	}

	// Hold the real transaction lock while another production Announce call
	// acquires a session reference and blocks on that lock.
	active.mu.Lock()
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	defer cancelWaiter()
	waiterDone := make(chan error, 1)
	go func() {
		_, err := client.Announce(waiterCtx, trackerURL, testRequest())
		waiterDone <- err
	}()
	select {
	case <-resolved:
	case <-time.After(time.Second):
		active.mu.Unlock()
		cancelWaiter()
		<-waiterDone
		t.Fatal("waiting announce did not resolve")
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		client.mu.Lock()
		users := active.users
		client.mu.Unlock()
		if users == 1 {
			break
		}
		select {
		case <-deadline.C:
			active.mu.Unlock()
			cancelWaiter()
			err := <-waiterDone
			if err != nil {
				t.Fatalf("announce returned before reserving the held transaction lock: %v", err)
			}
			t.Fatal("announce did not reserve the session before waiting for its transaction lock")
		default:
			runtime.Gosched()
		}
	}

	// Fill every remaining slot; exactly one unrelated session is then made
	// evictable to apply endpoint pressure while the announce waits on active.mu.
	var retained []*udpSession
	for i := 0; i < udpSessionCapacity-1; i++ {
		addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: addr}), addr)
		if err != nil {
			active.mu.Unlock()
			cancelWaiter()
			<-waiterDone
			t.Fatal(err)
		}
		retained = append(retained, s)
	}
	client.releaseSession(retained[len(retained)-1])
	target := netip.MustParseAddrPort("198.51.100.1:1")
	pressure, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: target}), target)
	if err != nil {
		active.mu.Unlock()
		cancelWaiter()
		<-waiterDone
		t.Fatal(err)
	}
	client.mu.Lock()
	stillRetained := client.sessions[key] == active && active.users == 1
	client.mu.Unlock()
	select {
	case <-conn.closed:
		stillRetained = false
	default:
	}

	active.mu.Unlock()
	var waiterErr error
	select {
	case waiterErr = <-waiterDone:
	case <-time.After(time.Second):
		_ = client.Close()
		<-waiterDone
		t.Fatal("waiting announce did not complete after lock release")
	}
	if waiterErr != nil {
		t.Fatalf("waiting announce: %v", waiterErr)
	}
	if !stillRetained {
		t.Fatal("active session or socket was retired while its transaction-lock waiter owned it")
	}
	client.releaseSession(pressure)
	for _, s := range retained[:len(retained)-1] {
		client.releaseSession(s)
	}
}

func TestUDPRetiringEndpointWaitsForSocketClose(t *testing.T) {
	client := NewUDPClient(Config{})
	retiringEndpoint := netip.MustParseAddrPort("192.0.2.10:1")
	retiringKey := sessionKey(&udpSession{endpoint: retiringEndpoint})
	retiring, err := client.acquireSession(context.Background(), retiringKey, retiringEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(closeRelease) }) }
	conn := newFixtureConn(nil)
	conn.onClose = func() { close(closeEntered); <-closeRelease }
	retiring.conn = conn

	var firstDone chan struct {
		s   *udpSession
		err error
	}
	var thirdDone chan struct {
		s   *udpSession
		err error
	}
	var cancelDone chan error
	var thirdCtx *signaledContext
	firstJoined, thirdJoined, cancelJoined := false, false, false
	cancelCtx := newSignaledContext()
	joinSession := func(done <-chan struct {
		s   *udpSession
		err error
	}) bool {
		select {
		case <-done:
			return true
		case <-time.After(time.Second):
			t.Errorf("timed out joining UDP session acquisition")
			return false
		}
	}
	t.Cleanup(func() {
		releaseClose()
		_ = client.Close()
		cancelCtx.cancel()
		if thirdCtx != nil {
			thirdCtx.cancel()
		}
		if firstDone != nil && !firstJoined {
			firstJoined = joinSession(firstDone)
		}
		if cancelDone != nil && !cancelJoined {
			select {
			case <-cancelDone:
			case <-time.After(time.Second):
				t.Errorf("timed out joining canceled endpoint waiter")
			}
		}
		if thirdDone != nil && !thirdJoined {
			thirdJoined = joinSession(thirdDone)
		}
	})

	busy := make([]*udpSession, 0, udpSessionCapacity-1)
	for i := 0; i < udpSessionCapacity-1; i++ {
		endpoint := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: endpoint}), endpoint)
		if err != nil {
			t.Fatal(err)
		}
		busy = append(busy, s)
	}
	client.releaseSession(retiring)

	firstEndpoint := netip.MustParseAddrPort("198.51.100.1:1")
	firstDone = make(chan struct {
		s   *udpSession
		err error
	}, 1)
	go func() {
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: firstEndpoint}), firstEndpoint)
		firstDone <- struct {
			s   *udpSession
			err error
		}{s, err}
	}()
	select {
	case <-closeEntered:
	case <-time.After(time.Second):
		t.Fatal("first eviction did not enter socket close")
	}

	// Retire a different unused entry while the first socket is still closing.
	client.releaseSession(busy[0])
	secondEndpoint := netip.MustParseAddrPort("198.51.100.2:1")
	second, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: secondEndpoint}), secondEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	client.releaseSession(second)

	// Same-endpoint acquisition must wait rather than overwrite the retiring
	// map entry after the second retirement opens a map slot. Its wait is
	// cancellable even while the old endpoint remains mapped.
	cancelDone = make(chan error, 1)
	go func() {
		_, err := client.acquireSession(cancelCtx, retiringKey, retiringEndpoint)
		cancelDone <- err
	}()
	select {
	case <-cancelCtx.entered:
	case <-time.After(time.Second):
		t.Fatal("same-endpoint caller did not wait on retirement")
	}
	cancelCtx.cancel()
	select {
	case err := <-cancelDone:
		cancelJoined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("same-endpoint cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-endpoint cancellation did not return")
	}

	thirdCtx = newSignaledContext()
	thirdDone = make(chan struct {
		s   *udpSession
		err error
	}, 1)
	go func() {
		s, err := client.acquireSession(thirdCtx, retiringKey, retiringEndpoint)
		thirdDone <- struct {
			s   *udpSession
			err error
		}{s, err}
	}()
	select {
	case <-thirdCtx.entered:
	case <-time.After(time.Second):
		t.Fatal("same-endpoint caller did not wait on retirement")
	}
	select {
	case value := <-thirdDone:
		thirdJoined = true
		t.Fatalf("same-endpoint caller acquired before close: %p", value.s)
	default:
	}
	client.mu.Lock()
	mapped := client.sessions[retiringKey]
	client.mu.Unlock()
	if mapped != retiring {
		t.Fatal("retiring endpoint was overwritten before its socket closed")
	}

	releaseClose()
	select {
	case value := <-firstDone:
		firstJoined = true
		if value.err != nil || value.s == nil {
			t.Fatalf("first acquisition: session=%p err=%v", value.s, value.err)
		}
	case <-time.After(time.Second):
		t.Fatal("first acquisition did not resume after close")
	}
	select {
	case value := <-thirdDone:
		thirdJoined = true
		if value.err != nil || value.s == retiring {
			t.Fatalf("same-endpoint acquisition: session=%p err=%v", value.s, value.err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-endpoint acquisition did not resume after close")
	}
	_ = client.Close()
}

func TestUDPCloseRacingSocketEviction(t *testing.T) {
	client := NewUDPClient(Config{})
	endpoint := netip.MustParseAddrPort("192.0.2.20:1")
	retiring, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: endpoint}), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(closeRelease) }) }
	conn := newFixtureConn(nil)
	conn.onClose = func() { close(closeEntered); <-closeRelease }
	retiring.conn = conn
	var result chan error
	resultJoined := false
	t.Cleanup(func() {
		releaseClose()
		_ = client.Close()
		if result != nil && !resultJoined {
			select {
			case <-result:
			case <-time.After(time.Second):
				t.Errorf("timed out joining eviction after client close")
			}
		}
	})
	busy := make([]*udpSession, 0, udpSessionCapacity-1)
	for i := 0; i < udpSessionCapacity-1; i++ {
		addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: addr}), addr)
		if err != nil {
			t.Fatal(err)
		}
		busy = append(busy, s)
	}
	client.releaseSession(retiring)
	newEndpoint := netip.MustParseAddrPort("198.51.100.20:1")
	result = make(chan error, 1)
	go func() {
		_, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: newEndpoint}), newEndpoint)
		result <- err
	}()
	select {
	case <-closeEntered:
	case <-time.After(time.Second):
		t.Fatal("eviction did not enter socket close")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	releaseClose()
	select {
	case err := <-result:
		resultJoined = true
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("acquisition after close race = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("eviction did not finish after client close")
	}
	for _, s := range busy {
		client.releaseSession(s)
	}
}

func TestUDPFailedAnnounceReleasesCapacityForReuse(t *testing.T) {
	client := NewUDPClient(Config{
		Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}},
		Dialer: fixtureDialFunc(func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("injected dial failure")
		}),
		Clock: newFixtureClock(),
	})
	defer client.Close()
	if _, err := client.Announce(context.Background(), "udp://tracker.test:1", testRequest()); err == nil {
		t.Fatal("announce unexpectedly succeeded after injected dial failure")
	}
	failedEndpoint := netip.MustParseAddrPort("127.0.0.1:1")
	failedKey := sessionKey(&udpSession{endpoint: failedEndpoint})
	client.mu.Lock()
	failed := client.sessions[failedKey]
	users := 0
	if failed != nil {
		users = failed.users
	}
	client.mu.Unlock()
	if failed == nil || users != 0 {
		t.Fatalf("failed announce session = %p, users = %d; want retained and unused", failed, users)
	}

	// Keep every other cache entry busy. Capacity can be recovered only if the
	// failed announce released its own reservation and that exact entry retires.
	busy := make([]*udpSession, 0, udpSessionCapacity-1)
	for i := 0; i < udpSessionCapacity-1; i++ {
		endpoint := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(i), 0, 1}), 1)
		s, err := client.acquireSession(context.Background(), sessionKey(&udpSession{endpoint: endpoint}), endpoint)
		if err != nil {
			t.Fatalf("fill capacity after failed announce at %d: %v", i, err)
		}
		busy = append(busy, s)
	}
	target := netip.MustParseAddrPort("192.0.2.90:1")
	ctx := newSignaledContext()
	type acquired struct {
		s   *udpSession
		err error
	}
	result := make(chan acquired, 1)
	go func() {
		s, err := client.acquireSession(ctx, sessionKey(&udpSession{endpoint: target}), target)
		result <- acquired{s, err}
	}()
	var got *udpSession
	select {
	case value := <-result:
		if value.err != nil {
			t.Fatalf("capacity recovery: %v", value.err)
		}
		got = value.s
	case <-ctx.entered:
		ctx.cancel()
		<-result
		t.Fatal("failed announce reservation remained busy while every other session was in use")
	case <-time.After(time.Second):
		ctx.cancel()
		<-result
		t.Fatal("capacity recovery did not finish")
	}
	client.mu.Lock()
	stillMapped := client.sessions[failedKey] == failed
	client.mu.Unlock()
	if stillMapped {
		t.Fatal("failed announce session was not the entry retired to recover capacity")
	}
	client.releaseSession(got)
	for _, s := range busy {
		client.releaseSession(s)
	}
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
