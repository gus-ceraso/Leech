package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

const (
	trackerRetryBase       = time.Second
	trackerRetryMax        = time.Hour
	trackerRetryJitterPart = 20 // percent of the delay, in either direction
	trackerEarlyRerequest  = time.Second
	finalAnnounceTimeout   = 15 * time.Second
)

// Phase identifies one independent announce phase. A phase always starts with
// event=started, even when the same session previously ran a metadata phase.
type Phase uint8

const (
	MetadataPhase Phase = iota
	TransferPhase
)

// SnapshotSource supplies a current accounting view. Implementations should
// return a consistent point-in-time value and remain safe for concurrent calls
// from independent tracker workers.
type SnapshotSource func(context.Context) (Snapshot, error)

// TrackerHTTP and TrackerUDP are the small protocol seams used by the loop.
// They match HTTPClient and UDPClient and allow deterministic local tests.
type TrackerHTTP interface {
	Announce(context.Context, string, AnnounceRequest) (HTTPAnnounceResult, error)
}

type TrackerUDP interface {
	Announce(context.Context, string, AnnounceRequest) (AnnounceResult, error)
}

// TrackerPeer is a normalized peer endpoint returned by either protocol. Host
// is an IP literal for compact responses and the bounded hostname supplied by
// a dictionary response. Resolution and peer endpoint filtering remain with
// candidate admission.
type TrackerPeer struct {
	Host   string
	Port   uint16
	PeerID [20]byte
	HasID  bool
}

// Update reports one protocol transaction. Transmitted and Activated are
// deliberately separate: a complete request can be sent without receiving a
// valid response, and a response is what activates a tracker.
type Update struct {
	Tracker     string
	Phase       Phase
	Request     AnnounceRequest
	Peers       []TrackerPeer
	Interval    time.Duration
	Transmitted bool
	Activated   bool
	Err         error
}

// TrackerSetConfig supplies one session's tracker state. Trackers must be the
// already normalized, unique source list (including the mandatory default).
// A zero Clock or protocol client selects the production implementation.
type TrackerSetConfig struct {
	InfoHash  [20]byte
	Trackers  []string
	Identity  Identity
	HTTP      TrackerHTTP
	UDP       TrackerUDP
	Clock     Clock
	Random    io.Reader
	Snapshot  SnapshotSource
	NeedPeers func() bool
	NumWant   int32
	// OnUpdate runs synchronously on the tracker worker. It must be
	// nonblocking; session coordinators should enqueue into their bounded,
	// cancellation-aware event queue before returning.
	OnUpdate func(Update)
}

// TrackerSet owns independent lifecycle state for each configured tracker and
// shares one session identity across phases and address families.
type TrackerSet struct {
	infoHash  [20]byte
	identity  Identity
	trackers  []string
	http      TrackerHTTP
	udp       TrackerUDP
	clock     Clock
	random    io.Reader
	snapshot  SnapshotSource
	needPeers func() bool
	numWant   int32
	onUpdate  func(Update)

	mu       sync.Mutex
	disabled map[string]bool
	active   bool
	current  *PhaseRun
	closed   bool
	ownedUDP *UDPClient
}

// NewTrackerSet creates the session-wide tracker owner. It does not perform
// network I/O or touch the announced port.
func NewTrackerSet(cfg TrackerSetConfig) (*TrackerSet, error) {
	if len(cfg.Trackers) == 0 {
		return nil, errors.New("tracker set is empty")
	}
	trackers := make([]string, 0, len(cfg.Trackers))
	seen := make(map[string]struct{}, len(cfg.Trackers))
	for _, raw := range cfg.Trackers {
		if raw == "" {
			return nil, errors.New("tracker URL is empty")
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		if _, err := url.Parse(raw); err != nil {
			return nil, fmt.Errorf("invalid tracker URL: %w", err)
		}
		seen[raw] = struct{}{}
		trackers = append(trackers, raw)
	}
	if len(trackers) == 0 {
		return nil, errors.New("tracker set is empty")
	}
	if len(trackers) > limits.Trackers {
		return nil, errors.New("too many unique trackers")
	}
	for _, tracker := range trackers {
		scheme := trackerScheme(tracker)
		if scheme != "http" && scheme != "https" && scheme != "udp" {
			return nil, fmt.Errorf("unsupported tracker scheme %q", scheme)
		}
	}

	identity := cfg.Identity
	if identity.Port < minAnnouncePort {
		var err error
		identity, err = GenerateIdentity(cfg.Random)
		if err != nil {
			return nil, fmt.Errorf("generate tracker identity: %w", err)
		}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	random := cfg.Random
	if random == nil {
		random = rand.Reader
	}
	httpClient := cfg.HTTP
	if httpClient == nil {
		httpClient = NewHTTPClient(HTTPConfig{})
	}
	udpClient := cfg.UDP
	var ownedUDP *UDPClient
	if udpClient == nil {
		ownedUDP = NewUDPClient(Config{})
		udpClient = ownedUDP
	}
	numWant := cfg.NumWant
	if numWant == 0 {
		numWant = -1
	}
	return &TrackerSet{
		infoHash:  cfg.InfoHash,
		identity:  identity,
		trackers:  trackers,
		http:      httpClient,
		udp:       udpClient,
		clock:     clock,
		random:    random,
		snapshot:  cfg.Snapshot,
		needPeers: cfg.NeedPeers,
		numWant:   numWant,
		onUpdate:  cfg.OnUpdate,
		disabled:  make(map[string]bool),
		ownedUDP:  ownedUDP,
	}, nil
}

// Identity returns the immutable session identity.
func (s *TrackerSet) Identity() Identity {
	if s == nil {
		return Identity{}
	}
	return s.identity
}

// Trackers returns the deduplicated tracker order used by this set.
func (s *TrackerSet) Trackers() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.trackers...)
}

// PhaseRun is one metadata or transfer announce phase.
type PhaseRun struct {
	set    *TrackerSet
	phase  Phase
	ctx    context.Context
	cancel context.CancelFunc
	state  []*trackerState
	wg     sync.WaitGroup

	mu           sync.Mutex
	finalStarted bool
	finalDone    chan struct{}
	finalErr     error
}

type trackerState struct {
	tracker            string
	permanent          bool
	startedTransmitted bool
}

// Start begins one phase. The returned run must be finalized before another
// phase starts; finalization cancels and joins regular loops before events.
func (s *TrackerSet) Start(ctx context.Context, phase Phase) (*PhaseRun, error) {
	if s == nil {
		return nil, errors.New("nil tracker set")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("tracker set is closed")
	}
	if s.active {
		s.mu.Unlock()
		return nil, errors.New("tracker phase is already active")
	}
	s.active = true
	states := make([]*trackerState, 0, len(s.trackers))
	for _, tracker := range s.trackers {
		states = append(states, &trackerState{tracker: tracker, permanent: s.disabled[tracker]})
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &PhaseRun{set: s, phase: phase, ctx: runCtx, cancel: cancel, state: states, finalDone: make(chan struct{})}
	s.current = run
	for _, state := range states {
		if state.permanent {
			continue
		}
		run.wg.Add(1)
		go run.loop(state)
	}
	s.mu.Unlock()
	return run, nil
}

// Close ends the active phase with a stopped sequence, then closes the UDP
// client created by NewTrackerSet. Injected protocol clients remain owned by
// their caller. Close is idempotent and prevents later phases from starting.
func (s *TrackerSet) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		owned := s.ownedUDP
		s.mu.Unlock()
		if owned != nil {
			return owned.Close()
		}
		return nil
	}
	s.closed = true
	run := s.current
	owned := s.ownedUDP
	s.mu.Unlock()
	var errs []error
	if run != nil {
		if err := run.Finalize(ctx, false); err != nil {
			errs = append(errs, err)
		}
	}
	if owned != nil {
		if err := owned.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Wait joins regular loops after their context has been canceled. It does not
// send final events; callers that own shutdown should use Finalize.
func (r *PhaseRun) Wait() {
	if r == nil {
		return
	}
	r.cancel()
	r.wg.Wait()
}

// Done closes after finalization, including one-shot event attempts.
func (r *PhaseRun) Done() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.finalDone
}

// Finalize cancels and joins all normal announce loops, then sends at most one
// completed and one stopped event per eligible tracker. Final errors are
// returned for diagnostics and must not replace the session's primary result.
func (r *PhaseRun) Finalize(ctx context.Context, fullCompletion bool) error {
	if r == nil {
		return errors.New("nil tracker run")
	}
	r.mu.Lock()
	if r.finalStarted {
		done := r.finalDone
		r.mu.Unlock()
		<-done
		r.mu.Lock()
		err := r.finalErr
		r.mu.Unlock()
		return err
	}
	r.finalStarted = true
	r.mu.Unlock()
	// Metadata discovery never represents completed torrent content, even if a
	// caller accidentally requests the full-transfer final sequence.
	fullCompletion = fullCompletion && r.phase == TransferPhase

	r.cancel()
	r.wg.Wait()
	finalCtx, cancelFinal := boundedFinalContext(ctx)
	defer cancelFinal()

	type finalResult struct {
		tracker string
		errs    []error
	}
	results := make(chan finalResult, len(r.state))
	var finalWG sync.WaitGroup
	for _, state := range r.state {
		if !state.startedTransmitted || state.permanent {
			continue
		}
		finalWG.Add(1)
		go func(state *trackerState) {
			defer finalWG.Done()
			var errs []error
			if fullCompletion {
				if err := r.sendOne(finalCtx, state, EventCompleted); err != nil {
					errs = append(errs, fmt.Errorf("tracker %s completed: %w", trackerLabel(state.tracker), err))
				}
			}
			if err := r.sendOne(finalCtx, state, EventStopped); err != nil {
				errs = append(errs, fmt.Errorf("tracker %s stopped: %w", trackerLabel(state.tracker), err))
			}
			results <- finalResult{tracker: state.tracker, errs: errs}
		}(state)
	}
	finalWG.Wait()
	close(results)
	var errs []error
	for result := range results {
		errs = append(errs, result.errs...)
	}
	err := errors.Join(errs...)
	r.mu.Lock()
	r.finalErr = err
	r.mu.Unlock()
	close(r.finalDone)
	r.set.mu.Lock()
	r.set.active = false
	r.set.mu.Unlock()
	return err
}

// boundedFinalContext deliberately detaches final tracker events from the
// phase context. A canceled transfer must still make its best-effort stopped
// announcements. An explicit caller deadline is retained as an upper bound;
// otherwise final events receive their own short deadline.
func boundedFinalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	now := time.Now()
	deadline := now.Add(finalAnnounceTimeout)
	if ctx != nil {
		if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.After(now) && parentDeadline.Before(deadline) {
			deadline = parentDeadline
		}
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (r *PhaseRun) loop(state *trackerState) {
	defer r.wg.Done()
	attempt := 0
	startedSent := false
	next := r.set.clock.Now()
	for {
		if !waitTimer(r.ctx, r.set.clock, next) {
			return
		}
		event := EventStarted
		if startedSent {
			event = EventNone
		}
		if !r.beginNormal() {
			return
		}
		snapshot := Snapshot{Metadata: r.phase == MetadataPhase}
		var snapshotErr error
		if r.set.snapshot != nil {
			snapshot, snapshotErr = r.set.snapshot(r.ctx)
		}
		// The phase owns the sentinel rule. A provider supplies counters only;
		// it cannot accidentally switch metadata discovery to exact accounting.
		snapshot.Metadata = r.phase == MetadataPhase
		request, reqErr := snapshot.Announce(r.set.identity, r.set.infoHash, event, r.set.numWant)
		if snapshotErr != nil {
			reqErr = snapshotErr
		}
		var update Update
		if reqErr != nil {
			update = Update{Tracker: state.tracker, Phase: r.phase, Request: request, Err: reqErr}
		} else {
			update = r.announce(r.ctx, state, request)
		}
		r.endNormal()
		r.notify(update)
		if event == EventStarted && update.Transmitted {
			startedSent = true
			state.startedTransmitted = true
		}
		if reqErr != nil {
			return
		}
		if update.Err == nil && update.Activated {
			attempt = 0
			interval := update.Interval
			next = addClock(r.set.clock.Now(), interval)
			if r.set.isHTTP(state.tracker) && r.set.needPeers != nil && r.set.needPeers() && interval > trackerEarlyRerequest {
				early := addClock(r.set.clock.Now(), trackerEarlyRerequest)
				if early.Before(next) {
					next = early
				}
			}
			continue
		}
		if isPermanent(update.Err, update.Interval) {
			state.permanent = true
			r.set.mu.Lock()
			r.set.disabled[state.tracker] = true
			r.set.mu.Unlock()
			return
		}
		attempt++
		backoff := retryBackoff(attempt, r.set.random)
		if retry, ok := retryAfter(update.Err); ok && retry > backoff {
			backoff = retry
		}
		candidate := addClock(r.set.clock.Now(), backoff)
		if candidate.Before(next) {
			candidate = next
		}
		next = candidate
	}
}

func (r *PhaseRun) announce(ctx context.Context, state *trackerState, request AnnounceRequest) Update {
	update := Update{Tracker: state.tracker, Phase: r.phase, Request: request}
	scheme := trackerScheme(state.tracker)
	switch scheme {
	case "http", "https":
		result, err := r.set.http.Announce(ctx, state.tracker, request)
		update.Interval, update.Transmitted, update.Err = result.Interval, result.Transmitted, err
		update.Activated = err == nil && result.Interval >= limits.MinTrackerSeconds*time.Second && result.Interval <= limits.MaxTrackerSeconds*time.Second
		for _, peer := range result.Peers {
			update.Peers = append(update.Peers, TrackerPeer{Host: peer.Host, Port: peer.Port, PeerID: peer.PeerID, HasID: peer.HasID})
		}
		if err == nil && !update.Activated {
			update.Err = errors.New("tracker returned an invalid interval")
		}
	case "udp":
		result, err := r.set.udp.Announce(ctx, state.tracker, request)
		update.Interval, update.Transmitted, update.Err = result.Interval, result.Transmitted, err
		update.Activated = err == nil && result.Interval >= limits.MinTrackerSeconds*time.Second && result.Interval <= limits.MaxTrackerSeconds*time.Second
		for _, peer := range result.Peers {
			update.Peers = append(update.Peers, TrackerPeer{Host: peer.Addr().String(), Port: peer.Port()})
		}
		if err == nil && !update.Activated {
			update.Err = errors.New("tracker returned an invalid interval")
		}
	default:
		update.Err = errors.New("unsupported tracker scheme")
	}
	return update
}

func (r *PhaseRun) sendOne(ctx context.Context, state *trackerState, event Event) error {
	snapshot := Snapshot{Metadata: r.phase == MetadataPhase}
	if r.set.snapshot != nil {
		var err error
		snapshot, err = r.set.snapshot(ctx)
		if err != nil {
			return err
		}
	}
	snapshot.Metadata = r.phase == MetadataPhase
	request, err := snapshot.Announce(r.set.identity, r.set.infoHash, event, r.set.numWant)
	if err != nil {
		return err
	}
	update := r.announce(ctx, state, request)
	r.notify(update)
	if update.Err != nil {
		return update.Err
	}
	if !update.Transmitted {
		return errors.New("final tracker request was not transmitted")
	}
	return nil
}

func (r *PhaseRun) notify(update Update) {
	if r.set.onUpdate == nil {
		return
	}
	update.Peers = append([]TrackerPeer(nil), update.Peers...)
	r.set.onUpdate(update)
}

func (r *PhaseRun) beginNormal() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.finalStarted
}

func (r *PhaseRun) endNormal() {}

func (s *TrackerSet) isHTTP(tracker string) bool {
	scheme := trackerScheme(tracker)
	return scheme == "http" || scheme == "https"
}

func trackerScheme(raw string) string {
	index := strings.IndexByte(raw, ':')
	if index <= 0 {
		return ""
	}
	return strings.ToLower(raw[:index])
}

func trackerLabel(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return trackerScheme(raw)
	}
	return strings.ToLower(u.Scheme) + "://" + u.Hostname()
}

func waitTimer(ctx context.Context, clock Clock, until time.Time) bool {
	d := until.Sub(clock.Now())
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.Chan():
		return true
	case <-ctx.Done():
		return false
	}
}

func addClock(now time.Time, duration time.Duration) time.Time {
	if duration <= 0 {
		return now
	}
	return now.Add(duration)
}

func retryBackoff(attempt int, random io.Reader) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := trackerRetryBase
	for i := 1; i < attempt && d < trackerRetryMax; i++ {
		if d > trackerRetryMax/2 {
			d = trackerRetryMax
		} else {
			d *= 2
		}
	}
	if d >= trackerRetryMax {
		return trackerRetryMax
	}
	var raw [8]byte
	if random == nil {
		return d
	}
	if _, err := io.ReadFull(random, raw[:]); err != nil {
		return d
	}
	// Map to [80%, 120%) of the capped exponential delay.
	spread := int64(d) * trackerRetryJitterPart / 100
	if spread == 0 {
		return d
	}
	value := int64(binary.BigEndian.Uint64(raw[:]) % uint64(2*spread))
	return time.Duration(int64(d) - spread + value)
}

func retryAfter(err error) (time.Duration, bool) {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.RetryAfter > 0 {
		return httpErr.RetryAfter, true
	}
	return 0, false
}

func isPermanent(err error, interval time.Duration) bool {
	if err == nil {
		return interval < limits.MinTrackerSeconds*time.Second || interval > limits.MaxTrackerSeconds*time.Second
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Class == HTTPFailureDefinitive || httpErr.Class == HTTPFailureNever || httpErr.Class == HTTPFailureInvalidDelay
	}
	var udpErr *Error
	if errors.As(err, &udpErr) {
		if udpErr.Code == ErrorInvalidURL {
			return true
		}
		if udpErr.Code == ErrorMalformed && strings.Contains(udpErr.Error(), "interval") {
			return true
		}
	}
	return false
}
