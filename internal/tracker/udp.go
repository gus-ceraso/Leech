// Package tracker contains the wire clients used to obtain peer endpoints.
package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

const (
	protocolID            uint64 = 0x41727101980
	connectionLife               = time.Minute
	maxTransactionRetries        = 8
	minAnnouncePort              = 49152
)

// Event is the BEP 15 announce event.
type Event uint32

const (
	EventNone      Event = 0
	EventCompleted Event = 1
	EventStarted   Event = 2
	EventStopped   Event = 3
)

// AnnounceRequest contains the fields shared by HTTP and UDP announces.
// InfoHash and PeerID must contain the raw 20-byte values, not their encoded
// forms. NumWant may be -1 to request the tracker's default.
type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Downloaded int64
	Left       int64
	Uploaded   int64
	Event      Event
	Key        uint32
	NumWant    int32
	Port       uint16
}

// FamilyResult is the result of one announce sent to one resolved tracker
// address. Err is retained so a dual-stack tracker can report one usable
// family even when the other family failed.
type FamilyResult struct {
	Endpoint    netip.AddrPort
	Interval    time.Duration
	Leechers    uint32
	Seeders     uint32
	Peers       []netip.AddrPort
	Transmitted bool
	Err         error
}

// AnnounceResult combines all successfully resolved tracker families.
// Interval is the largest successful interval, so callers never accidentally
// schedule the tracker earlier than one of its family responses permits.
type AnnounceResult struct {
	Interval    time.Duration
	Leechers    uint32
	Seeders     uint32
	Peers       []netip.AddrPort
	Families    []FamilyResult
	Transmitted bool
}

// ErrorCode identifies the local reason a UDP tracker transaction failed.
type ErrorCode string

const (
	ErrorInvalidURL  ErrorCode = "invalid-url"
	ErrorResolve     ErrorCode = "resolve"
	ErrorDial        ErrorCode = "dial"
	ErrorWrite       ErrorCode = "write"
	ErrorRead        ErrorCode = "read"
	ErrorTimeout     ErrorCode = "timeout"
	ErrorMalformed   ErrorCode = "malformed"
	ErrorTransaction ErrorCode = "transaction"
	ErrorAction      ErrorCode = "action"
	ErrorTracker     ErrorCode = "tracker"
	ErrorCanceled    ErrorCode = "canceled"
	ErrorClosed      ErrorCode = "closed"
)

var (
	ErrInvalidURL   = errors.New("invalid UDP tracker URL")
	ErrTimeout      = errors.New("UDP tracker transaction timed out")
	ErrMalformed    = errors.New("malformed UDP tracker response")
	ErrClientClosed = errors.New("UDP tracker client is closed")
)

// Error reports a tracker-local transaction failure. Transmitted is true when
// the operation's complete packet was accepted by the UDP socket, even if no
// response was received or the response was invalid.
type Error struct {
	Code        ErrorCode
	Operation   string
	Transmitted bool
	Err         error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return string(e.Code) + " UDP tracker " + e.Operation + " failure"
	}
	return string(e.Code) + " UDP tracker " + e.Operation + " failure: " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Resolver and Dialer are small seams for deterministic local tracker tests.
// A resolver is deliberately not filtered: tracker-server addresses may be
// loopback, private, or otherwise local. Peer filtering happens elsewhere.
type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

// Timer and Clock make BEP 15's long retransmission schedule testable without
// real-time sleeps. Timers use the same semantics as time.Timer.
type Timer interface {
	Chan() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Config supplies optional dependencies for UDPClient. A zero Config uses the
// standard resolver, net.Dialer, crypto/rand, and wall clock.
type Config struct {
	Resolver Resolver
	Dialer   Dialer
	Clock    Clock
	Random   io.Reader
}

// UDPClient maintains one BEP 15 connection ID per resolved tracker endpoint.
// The connection ID is reused for up to one minute after receipt, as required
// by BEP 15. Client is safe for independent tracker calls.
type UDPClient struct {
	resolver Resolver
	dialer   Dialer
	clock    Clock
	random   io.Reader

	mu       sync.Mutex
	randomMu sync.Mutex
	sessions map[string]*udpSession
	closed   bool
}

type udpSession struct {
	mu       sync.Mutex
	connMu   sync.RWMutex
	endpoint netip.AddrPort
	conn     net.Conn
	connID   uint64
	expires  time.Time
}

// NewUDPClient constructs a UDP tracker client.
func NewUDPClient(cfg Config) *UDPClient {
	resolver := cfg.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dialer := cfg.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	random := cfg.Random
	if random == nil {
		random = rand.Reader
	}
	return &UDPClient{
		resolver: resolver,
		dialer:   dialer,
		clock:    clock,
		random:   random,
		sessions: make(map[string]*udpSession),
	}
}

// Close closes all retained tracker sockets. It is safe to call more than
// once. Any in-flight transaction observes the resulting read error.
func (c *UDPClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	sessions := make([]*udpSession, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.sessions = make(map[string]*udpSession)
	c.mu.Unlock()

	var first error
	for _, s := range sessions {
		// Closing a net.Conn is safe concurrently with Read and Write. Do not
		// wait for s.mu here: an in-flight exchange owns that lock and must be
		// interrupted so its reader and cancellation watcher can join.
		s.connMu.RLock()
		conn := s.conn
		s.connMu.RUnlock()
		if conn != nil {
			if err := conn.Close(); err != nil && first == nil {
				first = err
			}
		}
		s.connMu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.connMu.Unlock()
	}
	return first
}

// Announce resolves a UDP tracker URL once per address family and announces
// to one address in each family. A valid response from either family is a
// successful result; every family attempt remains visible in Families.
func (c *UDPClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) (AnnounceResult, error) {
	var result AnnounceResult
	if c.isClosed() {
		return result, &Error{Code: ErrorClosed, Operation: "announce", Err: ErrClientClosed}
	}
	u, err := parseUDPURL(trackerURL)
	if err != nil {
		return result, &Error{Code: ErrorInvalidURL, Operation: "resolve", Err: err}
	}
	if err := validateAnnounceRequest(req); err != nil {
		return result, &Error{Code: ErrorInvalidURL, Operation: "announce", Err: err}
	}
	port, _ := strconv.ParseUint(u.Port(), 10, 16)
	addresses, err := c.resolve(ctx, u.Hostname(), uint16(port))
	if err != nil {
		return result, &Error{Code: ErrorResolve, Operation: "resolve", Err: err}
	}
	if len(addresses) == 0 {
		return result, &Error{Code: ErrorResolve, Operation: "resolve", Err: errors.New("tracker hostname has no IPv4 or IPv6 address")}
	}

	urlData := udpURLData(u)
	if !urlDataFitsDatagram(urlData) {
		return result, &Error{Code: ErrorInvalidURL, Operation: "announce", Err: errors.New("UDP tracker URL data exceeds datagram limit")}
	}

	var firstErr error
	for _, endpoint := range addresses {
		if err := ctx.Err(); err != nil {
			return result, &Error{Code: ErrorCanceled, Operation: "announce", Transmitted: result.Transmitted, Err: err}
		}
		family := FamilyResult{Endpoint: endpoint}
		response, transmitted, familyErr := c.announceFamily(ctx, endpoint, req, urlData)
		family.Transmitted = transmitted
		family.Err = familyErr
		family.Interval = response.Interval
		family.Leechers = response.Leechers
		family.Seeders = response.Seeders
		family.Peers = response.Peers
		result.Families = append(result.Families, family)
		result.Transmitted = result.Transmitted || transmitted
		if familyErr != nil {
			if firstErr == nil {
				firstErr = familyErr
			}
			continue
		}
		if response.Interval > result.Interval {
			result.Interval = response.Interval
		}
		result.Leechers += response.Leechers
		result.Seeders += response.Seeders
		result.Peers = appendUniquePeers(result.Peers, response.Peers)
	}
	if result.Interval > 0 {
		return result, nil
	}
	if firstErr == nil {
		firstErr = errors.New("UDP tracker returned no usable family response")
	}
	return result, firstErr
}

func (c *UDPClient) resolve(ctx context.Context, host string, port uint16) ([]netip.AddrPort, error) {
	ips, err := c.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	result := make([]netip.AddrPort, 0, 2)
	seen4, seen6 := false, false
	for _, ip := range ips {
		if v4 := ip.IP.To4(); v4 != nil {
			if seen4 {
				continue
			}
			var a [4]byte
			copy(a[:], v4)
			result = append(result, netip.AddrPortFrom(netip.AddrFrom4(a), port))
			seen4 = true
			continue
		}
		if v6 := ip.IP.To16(); v6 != nil {
			if seen6 {
				continue
			}
			var a [16]byte
			copy(a[:], v6)
			addr := netip.AddrFrom16(a)
			if ip.Zone != "" {
				addr = addr.WithZone(ip.Zone)
			}
			result = append(result, netip.AddrPortFrom(addr, port))
			seen6 = true
		}
	}
	return result, nil
}

func (c *UDPClient) announceFamily(ctx context.Context, endpoint netip.AddrPort, req AnnounceRequest, urlData string) (udpAnnounceResponse, bool, error) {
	network := "udp6"
	if endpoint.Addr().Is4() {
		network = "udp4"
	}
	key := network + "|" + endpoint.Addr().String() + "|" + strconv.Itoa(int(endpoint.Port()))
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return udpAnnounceResponse{}, false, &Error{Code: ErrorClosed, Operation: "announce", Err: ErrClientClosed}
	}
	s := c.sessions[key]
	if s == nil {
		s = &udpSession{endpoint: endpoint}
		c.sessions[key] = s
	}
	c.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := c.ensureConnection(ctx, s, network); err != nil {
		return udpAnnounceResponse{}, false, err
	}
	packet, err := c.buildAnnounce(s.connID, req, urlData)
	if err != nil {
		return udpAnnounceResponse{}, false, &Error{Code: ErrorInvalidURL, Operation: "announce", Err: err}
	}
	tx, err := c.transactionID()
	if err != nil {
		return udpAnnounceResponse{}, false, &Error{Code: ErrorWrite, Operation: "announce", Err: err}
	}
	binary.BigEndian.PutUint32(packet[12:16], tx)
	s.connMu.RLock()
	conn := s.conn
	s.connMu.RUnlock()
	if conn == nil {
		return udpAnnounceResponse{}, false, &Error{Code: ErrorDial, Operation: "announce", Err: errors.New("tracker socket closed")}
	}
	response, transmitted, err := c.exchangeWithRefresh(ctx, conn, packet, tx, 1, "announce", func() bool {
		return !c.clock.Now().Before(s.expires)
	}, func() (net.Conn, []byte, error) {
		if err := c.ensureConnection(ctx, s, network); err != nil {
			return nil, nil, err
		}
		packet, err := c.buildAnnounce(s.connID, req, urlData)
		if err != nil {
			return nil, nil, &Error{Code: ErrorInvalidURL, Operation: "announce", Err: err}
		}
		binary.BigEndian.PutUint32(packet[12:16], tx)
		s.connMu.RLock()
		conn := s.conn
		s.connMu.RUnlock()
		if conn == nil {
			return nil, nil, &Error{Code: ErrorDial, Operation: "announce", Err: errors.New("tracker socket closed")}
		}
		return conn, packet, nil
	})
	if err != nil {
		if transmitted && (errors.Is(err, ErrTimeout) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			c.invalidateLocked(s)
		}
		return udpAnnounceResponse{}, transmitted, err
	}
	parsed, err := parseAnnounceResponse(response, tx, endpoint.Addr().Is4())
	if err != nil {
		return udpAnnounceResponse{}, transmitted, err
	}
	return parsed, transmitted, nil
}

func (c *UDPClient) ensureConnection(ctx context.Context, s *udpSession, network string) error {
	if c.isClosed() {
		return &Error{Code: ErrorClosed, Operation: "connect", Err: ErrClientClosed}
	}
	now := c.clock.Now()
	s.connMu.RLock()
	conn := s.conn
	s.connMu.RUnlock()
	if conn != nil && now.Before(s.expires) {
		return nil
	}
	if conn != nil {
		_ = conn.Close()
		s.connMu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.connMu.Unlock()
	}
	conn, err := c.dialer.DialContext(ctx, network, s.endpoint.String())
	if err != nil {
		return &Error{Code: ErrorDial, Operation: "connect", Err: err}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return &Error{Code: ErrorClosed, Operation: "connect", Err: ErrClientClosed}
	}
	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()
	c.mu.Unlock()
	tx, err := c.transactionID()
	if err != nil {
		_ = conn.Close()
		s.connMu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.connMu.Unlock()
		return &Error{Code: ErrorWrite, Operation: "connect", Err: err}
	}
	packet := make([]byte, 16)
	binary.BigEndian.PutUint64(packet[0:8], protocolID)
	binary.BigEndian.PutUint32(packet[8:12], 0)
	binary.BigEndian.PutUint32(packet[12:16], tx)
	response, transmitted, err := c.exchange(ctx, conn, packet, tx, 0, "connect")
	if err != nil {
		_ = conn.Close()
		s.connMu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.connMu.Unlock()
		if e, ok := err.(*Error); ok {
			e.Transmitted = transmitted
		}
		return err
	}
	if len(response) < 16 {
		_ = conn.Close()
		s.connMu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.connMu.Unlock()
		return &Error{Code: ErrorMalformed, Operation: "connect", Err: ErrMalformed}
	}
	s.connID = binary.BigEndian.Uint64(response[8:16])
	s.expires = c.clock.Now().Add(connectionLife)
	return nil
}

func (c *UDPClient) transactionID() (uint32, error) {
	var b [4]byte
	c.randomMu.Lock()
	_, err := io.ReadFull(c.random, b[:])
	c.randomMu.Unlock()
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func (c *UDPClient) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// exchange sends one packet and waits with the BEP 15 schedule: the initial
// packet is sent immediately, then retries after 15, 30, 60, ... seconds,
// through 15*2^8 seconds. A complete Write marks the transaction transmitted
// before any response parsing occurs.
func (c *UDPClient) exchange(ctx context.Context, conn net.Conn, packet []byte, tx uint32, action uint32, operation string) ([]byte, bool, error) {
	return c.exchangeWithRefresh(ctx, conn, packet, tx, action, operation, nil, nil)
}

// exchangeWithRefresh keeps the announce transaction's retry counter while
// allowing an expired connection ID to be refreshed at a retransmission.
// refresh runs only after the current reader has been joined, so the exchange
// never has competing readers on the old and replacement sockets.
func (c *UDPClient) exchangeWithRefresh(ctx context.Context, conn net.Conn, packet []byte, tx uint32, action uint32, operation string, needsRefresh func() bool, refresh func() (net.Conn, []byte, error)) ([]byte, bool, error) {
	if len(packet) > limits.DatagramBytes {
		return nil, false, &Error{Code: ErrorWrite, Operation: operation, Err: errors.New("UDP datagram exceeds limit")}
	}
	if err := ctx.Err(); err != nil {
		return nil, false, &Error{Code: ErrorCanceled, Operation: operation, Err: err}
	}
	transmitted := false
	write := func() error {
		n, err := conn.Write(packet)
		if n == len(packet) {
			transmitted = true
		}
		if err != nil {
			return err
		}
		if n != len(packet) {
			return io.ErrShortWrite
		}
		return nil
	}
	if err := write(); err != nil {
		return nil, transmitted, &Error{Code: ErrorWrite, Operation: operation, Transmitted: transmitted, Err: err}
	}

	workers := startExchangeIO(ctx, conn)
	closeForCleanup := false
	defer func() {
		if closeForCleanup && conn != nil {
			_ = conn.Close()
		}
		if workers != nil {
			workers.stop()
		}
	}()

	attempt := 0
	finalRetrySent := false
	for {
		timer := c.clock.NewTimer(retryDelay(attempt))
		select {
		case result := <-workers.reads:
			_ = timer.Stop()
			if result.err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					closeForCleanup = true
					return nil, transmitted, &Error{Code: ErrorCanceled, Operation: operation, Transmitted: transmitted, Err: ctxErr}
				}
				return nil, transmitted, &Error{Code: ErrorRead, Operation: operation, Transmitted: transmitted, Err: result.err}
			}
			if len(result.packet) > limits.DatagramBytes {
				return nil, transmitted, &Error{Code: ErrorMalformed, Operation: operation, Transmitted: transmitted, Err: errors.New("UDP datagram exceeds limit")}
			}
			if len(result.packet) < 8 {
				return nil, transmitted, &Error{Code: ErrorMalformed, Operation: operation, Transmitted: transmitted, Err: ErrMalformed}
			}
			responseTx := binary.BigEndian.Uint32(result.packet[4:8])
			if responseTx != tx {
				return nil, transmitted, &Error{Code: ErrorTransaction, Operation: operation, Transmitted: transmitted, Err: fmt.Errorf("got transaction %d, want %d", responseTx, tx)}
			}
			responseAction := binary.BigEndian.Uint32(result.packet[0:4])
			if responseAction == 3 {
				message := strings.TrimSpace(string(result.packet[8:]))
				return nil, transmitted, &Error{Code: ErrorTracker, Operation: operation, Transmitted: transmitted, Err: errors.New(message)}
			}
			if responseAction != action {
				return nil, transmitted, &Error{Code: ErrorAction, Operation: operation, Transmitted: transmitted, Err: fmt.Errorf("got action %d, want %d", responseAction, action)}
			}
			return result.packet, transmitted, nil
		case <-timer.Chan():
			if finalRetrySent {
				closeForCleanup = true
				return nil, transmitted, &Error{Code: ErrorTimeout, Operation: operation, Transmitted: transmitted, Err: ErrTimeout}
			}
			if err := ctx.Err(); err != nil {
				closeForCleanup = true
				return nil, transmitted, &Error{Code: ErrorCanceled, Operation: operation, Transmitted: transmitted, Err: err}
			}
			if refresh != nil && needsRefresh != nil && needsRefresh() {
				// Stop the old reader before it can race a response from the
				// replacement socket.
				_ = conn.Close()
				workers.stop()
				workers = nil
				newConn, newPacket, err := refresh()
				if err != nil {
					closeForCleanup = true
					return nil, transmitted, err
				}
				conn, packet = newConn, newPacket
				if err := write(); err != nil {
					closeForCleanup = true
					return nil, transmitted, &Error{Code: ErrorWrite, Operation: operation, Transmitted: transmitted, Err: err}
				}
				workers = startExchangeIO(ctx, conn)
			} else if err := write(); err != nil {
				closeForCleanup = true
				return nil, transmitted, &Error{Code: ErrorWrite, Operation: operation, Transmitted: transmitted, Err: err}
			}
			if attempt == maxTransactionRetries {
				// Exponent 8 is itself a retransmission. Keep waiting for one
				// final 15*2^8 window before reporting timeout.
				finalRetrySent = true
			} else {
				attempt++
			}
		case <-ctx.Done():
			closeForCleanup = true
			return nil, transmitted, &Error{Code: ErrorCanceled, Operation: operation, Transmitted: transmitted, Err: ctx.Err()}
		}
	}
}

type exchangeIO struct {
	reads     chan readResult
	readDone  chan struct{}
	abort     chan struct{}
	watchDone chan struct{}
}

func startExchangeIO(ctx context.Context, conn net.Conn) *exchangeIO {
	io := &exchangeIO{
		reads: make(chan readResult, 1), readDone: make(chan struct{}),
		abort: make(chan struct{}), watchDone: make(chan struct{}),
	}
	go func() {
		defer close(io.readDone)
		readPacket(conn, io.reads)
	}()
	go func() {
		defer close(io.watchDone)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-io.abort:
		}
	}()
	return io
}

func (io *exchangeIO) stop() {
	close(io.abort)
	<-io.readDone
	<-io.watchDone
}

type readResult struct {
	packet []byte
	err    error
}

func readPacket(conn net.Conn, result chan<- readResult) {
	buffer := make([]byte, limits.DatagramBytes+1)
	n, err := conn.Read(buffer)
	if err != nil {
		result <- readResult{err: err}
		return
	}
	packet := append([]byte(nil), buffer[:n]...)
	result <- readResult{packet: packet}
}

type udpAnnounceResponse struct {
	Interval time.Duration
	Leechers uint32
	Seeders  uint32
	Peers    []netip.AddrPort
}

func parseAnnounceResponse(packet []byte, tx uint32, v4 bool) (udpAnnounceResponse, error) {
	if len(packet) < 20 {
		return udpAnnounceResponse{}, &Error{Code: ErrorMalformed, Operation: "announce", Err: ErrMalformed}
	}
	if binary.BigEndian.Uint32(packet[4:8]) != tx {
		return udpAnnounceResponse{}, &Error{Code: ErrorTransaction, Operation: "announce", Err: errors.New("announce transaction mismatch")}
	}
	if binary.BigEndian.Uint32(packet[0:4]) != 1 {
		return udpAnnounceResponse{}, &Error{Code: ErrorAction, Operation: "announce", Err: errors.New("announce action mismatch")}
	}
	seconds := binary.BigEndian.Uint32(packet[8:12])
	if seconds < limits.MinTrackerSeconds || seconds > limits.MaxTrackerSeconds {
		return udpAnnounceResponse{}, &Error{Code: ErrorMalformed, Operation: "announce", Err: errors.New("tracker interval outside supported range")}
	}
	stride := 18
	if v4 {
		stride = 6
	}
	peersBytes := packet[20:]
	if len(peersBytes)%stride != 0 {
		return udpAnnounceResponse{}, &Error{Code: ErrorMalformed, Operation: "announce", Err: errors.New("compact peer list has incomplete endpoint")}
	}
	peers := make([]netip.AddrPort, 0, len(peersBytes)/stride)
	for offset := 0; offset < len(peersBytes); offset += stride {
		var addr netip.Addr
		if v4 {
			var raw [4]byte
			copy(raw[:], peersBytes[offset:offset+4])
			addr = netip.AddrFrom4(raw)
		} else {
			var raw [16]byte
			copy(raw[:], peersBytes[offset:offset+16])
			addr = netip.AddrFrom16(raw)
		}
		port := binary.BigEndian.Uint16(peersBytes[offset+stride-2 : offset+stride])
		if port == 0 || !addr.IsValid() || addr.IsUnspecified() || addr.IsMulticast() {
			return udpAnnounceResponse{}, &Error{Code: ErrorMalformed, Operation: "announce", Err: errors.New("invalid compact peer endpoint")}
		}
		peers = append(peers, netip.AddrPortFrom(addr, port))
	}
	return udpAnnounceResponse{
		Interval: time.Duration(seconds) * time.Second,
		Leechers: binary.BigEndian.Uint32(packet[12:16]),
		Seeders:  binary.BigEndian.Uint32(packet[16:20]),
		Peers:    peers,
	}, nil
}

func (c *UDPClient) buildAnnounce(connectionID uint64, req AnnounceRequest, urlData string) ([]byte, error) {
	if !urlDataFitsDatagram(urlData) {
		return nil, errors.New("UDP tracker URL data exceeds datagram limit")
	}
	options := encodeURLData(urlData)
	packet := make([]byte, 98+len(options))
	binary.BigEndian.PutUint64(packet[0:8], connectionID)
	binary.BigEndian.PutUint32(packet[8:12], 1)
	copy(packet[16:36], req.InfoHash[:])
	copy(packet[36:56], req.PeerID[:])
	if req.Downloaded < 0 || req.Left < 0 {
		return nil, errors.New("announce counters cannot be negative")
	}
	if req.Uploaded != 0 {
		return nil, errors.New("uploaded must be zero for the download-only client")
	}
	binary.BigEndian.PutUint64(packet[56:64], uint64(req.Downloaded))
	binary.BigEndian.PutUint64(packet[64:72], uint64(req.Left))
	binary.BigEndian.PutUint64(packet[72:80], uint64(req.Uploaded))
	binary.BigEndian.PutUint32(packet[80:84], uint32(req.Event))
	// The IP address field is always zero. The source family selects the
	// response format, and Leech does not send spoofable IP parameters.
	binary.BigEndian.PutUint32(packet[84:88], 0)
	binary.BigEndian.PutUint32(packet[88:92], req.Key)
	binary.BigEndian.PutUint32(packet[92:96], uint32(req.NumWant))
	binary.BigEndian.PutUint16(packet[96:98], req.Port)
	copy(packet[98:], options)
	if len(packet) > limits.DatagramBytes {
		return nil, errors.New("announce datagram exceeds limit")
	}
	return packet, nil
}

func encodeURLData(value string) []byte {
	if value == "" {
		return []byte{2, 0}
	}
	result := make([]byte, 0, len(value)+2+(len(value)/255)*2)
	for len(value) > 0 {
		n := len(value)
		if n > 255 {
			n = 255
		}
		result = append(result, 2, byte(n))
		result = append(result, value[:n]...)
		value = value[n:]
	}
	return result
}

func urlDataFitsDatagram(value string) bool {
	if len(value) > limits.DatagramBytes-98 {
		return false
	}
	chunks := (len(value) + 254) / 255
	return len(value)+2*chunks <= limits.DatagramBytes-98
}

func parseUDPURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "udp" || u.Hostname() == "" || u.Port() == "" || u.User != nil {
		return nil, ErrInvalidURL
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, ErrInvalidURL
	}
	return u, nil
}

func udpURLData(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		path = u.Path
	}
	if u.RawQuery == "" {
		return path
	}
	return path + "?" + u.RawQuery
}

func validateAnnounceRequest(req AnnounceRequest) error {
	if req.Downloaded < 0 || req.Left < 0 {
		return errors.New("announce counters cannot be negative")
	}
	if req.Uploaded != 0 {
		return errors.New("uploaded must be zero for the download-only client")
	}
	if req.Port < minAnnouncePort {
		return errors.New("announce port is outside the dynamic range")
	}
	if req.Event > EventStopped {
		return errors.New("invalid announce event")
	}
	return nil
}

func appendUniquePeers(dst, src []netip.AddrPort) []netip.AddrPort {
	seen := make(map[netip.AddrPort]struct{}, len(dst)+len(src))
	for _, peer := range dst {
		seen[peer] = struct{}{}
	}
	for _, peer := range src {
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		dst = append(dst, peer)
	}
	return dst
}

func retryDelay(attempt int) time.Duration {
	if attempt > maxTransactionRetries {
		attempt = maxTransactionRetries
	}
	return 15 * time.Second * time.Duration(uint64(1)<<attempt)
}

func (c *UDPClient) invalidateLocked(s *udpSession) {
	s.connMu.Lock()
	conn := s.conn
	s.conn = nil
	s.connID = 0
	s.expires = time.Time{}
	s.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{timer: time.NewTimer(d)} }

type realTimer struct{ timer *time.Timer }

func (t realTimer) Chan() <-chan time.Time { return t.timer.C }

func (t realTimer) Stop() bool { return t.timer.Stop() }
