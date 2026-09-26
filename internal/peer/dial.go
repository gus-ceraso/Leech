package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrDialConfig      = errors.New("invalid peer dial configuration")
	ErrDialUnavailable = errors.New("peer transport dialer is unavailable")
	ErrRaceLimit       = errors.New("concurrent endpoint race limit reached")
	ErrPeerIDCollision = errors.New("peer ID is already connected")
	ErrLivePeerLimit   = errors.New("live peer limit reached")
	ErrEndpointBudget  = errors.New("distinct peer endpoint attempt budget exhausted")
	// ErrExpectedPeerIDMismatch means a tracker assertion did not match the
	// handshake. It is candidate metadata failure, not peer-origin misconduct.
	ErrExpectedPeerIDMismatch = errors.New("tracker-supplied peer ID does not match handshake")
)

// EndpointBudgetError reports that the run has attempted its maximum number
// of distinct resolved IP:port endpoints.
type EndpointBudgetError struct {
	Limit int
}

func (e *EndpointBudgetError) Error() string {
	if e == nil {
		return ErrEndpointBudget.Error()
	}
	return fmt.Sprintf("%s (%d)", ErrEndpointBudget, e.Limit)
}

func (e *EndpointBudgetError) Is(target error) bool { return target == ErrEndpointBudget }

const defaultUTPHeadStart = 100 * time.Millisecond

// Transport identifies the transport that won a handshake race.
type Transport uint8

const (
	TransportTCP Transport = iota + 1
	TransportUTP
)

func (t Transport) String() string {
	switch t {
	case TransportTCP:
		return "tcp"
	case TransportUTP:
		return "utp"
	default:
		return "unknown"
	}
}

// DialFunc opens one exact network/address pair. Both P3 transport dialers
// receive the same resolved literal address, so a DNS change cannot alter the
// endpoint identity during a race.
type DialFunc func(context.Context, string, string) (net.Conn, error)

// RaceTimer and RaceClock make the uTP head-start timer deterministic without
// putting a time dependency into peer wire parsing.
type RaceTimer interface {
	Chan() <-chan time.Time
	Stop() bool
}

type RaceClock interface {
	NewTimer(time.Duration) RaceTimer
}

type realRaceClock struct{}

func (realRaceClock) NewTimer(delay time.Duration) RaceTimer {
	return realRaceTimer{Timer: time.NewTimer(delay)}
}

type realRaceTimer struct{ *time.Timer }

func (t realRaceTimer) Chan() <-chan time.Time { return t.C }

// RaceConfig supplies the local handshake and transport seams. A nil UTP
// dialer disables that attempt, which keeps P3 usable before L2 binds the
// in-tree uTP implementation. TCP defaults to net.Dialer when omitted.
type RaceConfig struct {
	LocalHandshake Handshake
	ExpectedPeerID *[20]byte
	TCPDial        DialFunc
	UTPDial        DialFunc
	Clock          RaceClock
	UTPHeadStart   time.Duration
}

// DialManager is the bounded lifecycle wrapper around RaceEndpoint. It
// acquires one race slot before starting transport goroutines, checks endpoint
// backoff, and optionally transfers a successful race into PeerRegistry. A
// caller that only needs a handshake can use Race; a transfer coordinator
// should use Dial and Release so live-peer slots stay bounded as well.
type DialManager struct {
	raceConfig RaceConfig
	races      chan struct{}
	liveSlots  chan struct{}
	registry   *PeerRegistry
	backoff    *EndpointBackoff
	now        func() time.Time
}

// DialManagerConfig supplies bounded dial-manager dependencies. Zero limits
// use the supported endpoint-race and active-peer bounds.
type DialManagerConfig struct {
	Race         RaceConfig
	MaxRaces     int
	MaxLivePeers int
	Registry     *PeerRegistry
	Backoff      *EndpointBackoff
	Now          func() time.Time
}

func NewDialManager(config DialManagerConfig) (*DialManager, error) {
	maxRaces := config.MaxRaces
	if maxRaces == 0 {
		maxRaces = limits.EndpointRaces
	}
	if maxRaces < 1 || maxRaces > limits.EndpointRaces {
		return nil, ErrDialConfig
	}
	maxLive := config.MaxLivePeers
	if maxLive == 0 {
		maxLive = limits.ActivePeers
	}
	if maxLive < 1 || maxLive > limits.ActivePeers {
		return nil, ErrDialConfig
	}
	registry := config.Registry
	if registry == nil {
		var err error
		registry, err = NewPeerRegistry(maxLive)
		if err != nil {
			return nil, err
		}
	}
	backoff := config.Backoff
	if backoff == nil {
		backoff = NewEndpointBackoff()
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &DialManager{
		raceConfig: config.Race,
		races:      make(chan struct{}, maxRaces),
		liveSlots:  make(chan struct{}, maxLive),
		registry:   registry,
		backoff:    backoff,
		now:        now,
	}, nil
}

// Race waits for one bounded race slot, then returns a successful validated
// handshake. No live-peer slot is held because ownership remains with the
// caller until it admits the result.
func (m *DialManager) Race(ctx context.Context, candidate ResolvedCandidate) (HandshakeResult, error) {
	if m == nil {
		return HandshakeResult{}, ErrDialConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	endpoint, err := NormalizeEndpoint(candidate.Endpoint)
	if err != nil {
		return HandshakeResult{}, err
	}
	if !m.backoff.Ready(endpoint, m.now()) {
		if m.backoff.IsBlacklisted(endpoint) {
			return HandshakeResult{}, fmt.Errorf("%w: endpoint is blacklisted", ErrCandidate)
		}
		return HandshakeResult{}, fmt.Errorf("%w: endpoint backoff is active", ErrCandidate)
	}
	select {
	case m.races <- struct{}{}:
	case <-ctx.Done():
		return HandshakeResult{}, ctx.Err()
	}
	defer func() { <-m.races }()
	if err := ctx.Err(); err != nil {
		return HandshakeResult{}, err
	}
	config := m.raceConfig
	if candidate.HasExpectedID {
		expected := candidate.ExpectedPeerID
		config.ExpectedPeerID = &expected
	}
	if err := m.backoff.beginAttempt(endpoint, m.now()); err != nil {
		return HandshakeResult{}, err
	}
	result, err := RaceEndpoint(ctx, endpoint, config)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			if IsProtocolViolation(err) {
				m.backoff.Blacklist(endpoint)
			} else {
				m.backoff.RecordFailure(endpoint, m.now())
			}
		}
		return HandshakeResult{}, err
	}
	m.backoff.RecordSuccess(endpoint)
	return result, nil
}

// Dial performs a full bounded race and admits the winner into the live-peer
// registry. On success, the returned LivePeer owns its connection. On every
// failure, any connection from the race is closed by the relevant owner.
func (m *DialManager) Dial(ctx context.Context, candidate ResolvedCandidate) (*LivePeer, error) {
	if m == nil {
		return nil, ErrDialConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case m.liveSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	peerResult, err := m.Race(ctx, candidate)
	if err != nil {
		<-m.liveSlots
		return nil, err
	}
	peer, err := m.registry.Admit(peerResult)
	if err != nil {
		<-m.liveSlots
		return nil, err
	}
	return peer, nil
}

// Release closes and releases a peer admitted by Dial. A stale release is a
// no-op, so a later connection with the same ID cannot lose its live slot.
func (m *DialManager) Release(peer *LivePeer) bool {
	if m == nil || peer == nil {
		return false
	}
	if !m.registry.Release(peer.ID, peer.Conn) {
		return false
	}
	_ = peer.Conn.Close()
	<-m.liveSlots
	return true
}

func (m *DialManager) Registry() *PeerRegistry {
	if m == nil {
		return nil
	}
	return m.registry
}

// HandshakeResult is a connected net.Conn whose remote BEP 3 handshake has
// already validated the local info hash and optional expected peer ID. A
// successful RaceEndpoint transfers Conn ownership to the caller; failed
// attempts and losers are closed before the function returns.
type HandshakeResult struct {
	Endpoint  Endpoint
	Transport Transport
	Conn      net.Conn
	Handshake Handshake
}

// AttemptError records one transport's bounded race outcome.
type AttemptError struct {
	Transport Transport
	Err       error
}

// RaceError reports that neither attempted transport completed a valid
// handshake. It deliberately carries disconnect/protocol classes from each
// attempt without assigning corruption strikes.
type RaceError struct {
	Endpoint Endpoint
	Attempts []AttemptError
}

func (e *RaceError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if len(e.Attempts) == 0 {
		return fmt.Sprintf("peer %s: handshake race failed", e.Endpoint)
	}
	return fmt.Sprintf("peer %s: handshake race failed (%d attempts)", e.Endpoint, len(e.Attempts))
}

func (e *RaceError) Unwrap() error {
	if e == nil || len(e.Attempts) == 0 {
		return nil
	}
	errs := make([]error, 0, len(e.Attempts))
	for _, attempt := range e.Attempts {
		if attempt.Err != nil {
			errs = append(errs, attempt.Err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// RaceEndpoint starts uTP, gives it a short head start, then starts TCP for
// the exact same resolved endpoint. A transport wins only after both writing
// Leech's handshake and reading a valid remote handshake. Every attempt is
// canceled, closed, and joined before this function returns.
func RaceEndpoint(ctx context.Context, endpoint Endpoint, config RaceConfig) (HandshakeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return HandshakeResult{}, err
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return HandshakeResult{}, err
	}
	if config.TCPDial == nil && config.UTPDial == nil {
		return HandshakeResult{}, ErrDialUnavailable
	}
	if config.Clock == nil {
		config.Clock = realRaceClock{}
	}
	if config.UTPHeadStart < 0 {
		return HandshakeResult{}, fmt.Errorf("%w: negative uTP head start", ErrDialConfig)
	}
	if config.UTPHeadStart == 0 {
		config.UTPHeadStart = defaultUTPHeadStart
	}
	if config.TCPDial == nil {
		config.TCPDial = (&net.Dialer{}).DialContext
	}

	network, address := endpointDialAddress(endpoint)
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan raceAttemptResult, 2)
	var attempts sync.WaitGroup
	started := 0
	start := func(transport Transport, dial DialFunc) {
		if dial == nil {
			return
		}
		started++
		attempts.Add(1)
		go func() {
			defer attempts.Done()
			results <- runRaceAttempt(raceCtx, transport, network, address, dial, config)
		}()
	}
	start(TransportUTP, config.UTPDial)

	var timer RaceTimer
	if started > 0 {
		timer = config.Clock.NewTimer(config.UTPHeadStart)
	}
	startTCP := false
	if config.UTPDial == nil {
		startTCP = true
	}
	if startTCP {
		start(TransportTCP, config.TCPDial)
	}

	attemptErrors := make([]AttemptError, 0, 2)
	var winner *raceAttemptResult
	completed := 0
	for completed < started || (!startTCP && timer != nil) {
		select {
		case <-ctx.Done():
			cancel()
			attempts.Wait()
			closeRaceResults(results, nil)
			if timer != nil {
				timer.Stop()
			}
			return HandshakeResult{}, ctx.Err()
		case <-timerChan(timer, startTCP):
			if !startTCP {
				startTCP = true
				start(TransportTCP, config.TCPDial)
			}
		case result := <-results:
			completed++
			if result.err == nil && result.conn != nil {
				winner = &result
				cancel()
				attempts.Wait()
				closeRaceResults(results, winner.conn)
				if timer != nil {
					timer.Stop()
				}
				return HandshakeResult{Endpoint: endpoint, Transport: winner.transport, Conn: winner.conn, Handshake: winner.handshake}, nil
			}
			attemptErrors = append(attemptErrors, AttemptError{Transport: result.transport, Err: result.err})
		}
	}
	if timer != nil {
		timer.Stop()
	}
	if err := ctx.Err(); err != nil {
		return HandshakeResult{}, err
	}
	return HandshakeResult{}, &RaceError{Endpoint: endpoint, Attempts: attemptErrors}
}

func timerChan(timer RaceTimer, tcpStarted bool) <-chan time.Time {
	if timer == nil || tcpStarted {
		return nil
	}
	return timer.Chan()
}

type raceAttemptResult struct {
	transport Transport
	conn      net.Conn
	handshake Handshake
	err       error
}

func runRaceAttempt(ctx context.Context, transport Transport, network, address string, dial DialFunc, config RaceConfig) raceAttemptResult {
	conn, err := dial(ctx, networkForTransport(transport, network), address)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return raceAttemptResult{transport: transport, err: disconnectError(transport.String()+" dial", err)}
	}
	if conn == nil {
		return raceAttemptResult{transport: transport, err: disconnectError(transport.String()+" dial", ErrDisconnected)}
	}
	stopWatch := make(chan struct{})
	var watch sync.WaitGroup
	watch.Add(1)
	go func() {
		defer watch.Done()
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopWatch:
		}
	}()

	if err := WriteHandshake(conn, config.LocalHandshake.InfoHash, config.LocalHandshake.PeerID, config.LocalHandshake.Reserved); err != nil {
		close(stopWatch)
		watch.Wait()
		_ = conn.Close()
		return raceAttemptResult{transport: transport, err: err}
	}
	remote, err := ReadHandshake(conn, &config.LocalHandshake.InfoHash, nil)
	close(stopWatch)
	watch.Wait()
	if err != nil {
		_ = conn.Close()
		return raceAttemptResult{transport: transport, err: err}
	}
	// The peer ID came from an untrusted tracker, so its mismatch is a
	// candidate metadata failure rather than a peer protocol violation.
	if config.ExpectedPeerID != nil && remote.PeerID != *config.ExpectedPeerID {
		_ = conn.Close()
		return raceAttemptResult{transport: transport, err: ErrExpectedPeerIDMismatch}
	}
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return raceAttemptResult{transport: transport, err: ctx.Err()}
	default:
	}
	return raceAttemptResult{transport: transport, conn: conn, handshake: remote}
}

func networkForTransport(transport Transport, network string) string {
	if transport == TransportUTP {
		if network == "tcp4" {
			return "utp4"
		}
		return "utp6"
	}
	return network
}

func endpointDialAddress(endpoint Endpoint) (string, string) {
	if endpoint.Addr.Is4() {
		return "tcp4", netipAddrPort(endpoint)
	}
	return "tcp6", netipAddrPort(endpoint)
}

func netipAddrPort(endpoint Endpoint) string {
	return net.JoinHostPort(endpoint.Addr.String(), strconv.Itoa(int(endpoint.Port)))
}

func closeRaceResults(results <-chan raceAttemptResult, winner net.Conn) {
	for {
		select {
		case result := <-results:
			if result.conn != nil && result.conn != winner {
				_ = result.conn.Close()
			}
		default:
			return
		}
	}
}

// LivePeer is the value retained by PeerRegistry after a successful race.
type LivePeer struct {
	ID        [20]byte
	Endpoint  Endpoint
	Transport Transport
	Conn      net.Conn
	Handshake Handshake
}

// PeerRegistry owns live peer-ID collision handling. IDs are never used for
// candidate deduplication, and a rejected collision never blacklists either
// endpoint. Established insertion order defines the older connection.
type PeerRegistry struct {
	mu       sync.Mutex
	maxPeers int
	peers    map[[20]byte]*LivePeer
}

func NewPeerRegistry(maxPeers int) (*PeerRegistry, error) {
	if maxPeers == 0 {
		maxPeers = limits.ActivePeers
	}
	if maxPeers < 1 || maxPeers > limits.ActivePeers {
		return nil, ErrDialConfig
	}
	return &PeerRegistry{maxPeers: maxPeers, peers: make(map[[20]byte]*LivePeer)}, nil
}

// Admit retains result if its peer ID is new. On collision or capacity
// exhaustion it closes result.Conn and returns an actionable error. The old
// connection is untouched.
func (r *PeerRegistry) Admit(result HandshakeResult) (*LivePeer, error) {
	if r == nil || result.Conn == nil {
		if result.Conn != nil {
			_ = result.Conn.Close()
		}
		return nil, ErrDialConfig
	}
	endpoint, err := NormalizeEndpoint(result.Endpoint)
	if err != nil {
		_ = result.Conn.Close()
		return nil, err
	}
	peer := &LivePeer{ID: result.Handshake.PeerID, Endpoint: endpoint, Transport: result.Transport, Conn: result.Conn, Handshake: result.Handshake}
	r.mu.Lock()
	if _, exists := r.peers[peer.ID]; exists {
		r.mu.Unlock()
		_ = result.Conn.Close()
		return nil, ErrPeerIDCollision
	}
	if len(r.peers) >= r.maxPeers {
		r.mu.Unlock()
		_ = result.Conn.Close()
		return nil, ErrLivePeerLimit
	}
	r.peers[peer.ID] = peer
	r.mu.Unlock()
	return peer, nil
}

// Release removes exactly the retained connection for id. Passing a
// different connection is a no-op, preventing an old worker's cleanup from
// releasing a newer connection that was admitted after it. It does not close
// the connection; DialManager.Release provides the close-and-release path.
func (r *PeerRegistry) Release(id [20]byte, conn net.Conn) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	peer, ok := r.peers[id]
	if ok && (conn == nil || peer.Conn == conn) {
		delete(r.peers, id)
	}
	r.mu.Unlock()
	return ok && (conn == nil || peer.Conn == conn)
}

func (r *PeerRegistry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	n := len(r.peers)
	r.mu.Unlock()
	return n
}

func (r *PeerRegistry) Lookup(id [20]byte) (*LivePeer, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	peer, ok := r.peers[id]
	r.mu.Unlock()
	return peer, ok
}
