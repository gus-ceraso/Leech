package peer

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrCandidateConfig = errors.New("invalid candidate configuration")
	ErrCandidate       = errors.New("invalid peer candidate")
	// ErrCandidateLimit is retained for source compatibility; a full pool now
	// replaces its least-recently announced endpoint instead of returning it.
	ErrCandidateLimit       = errors.New("peer candidate limit reached")
	ErrCandidateDNS         = errors.New("peer candidate resolution failed")
	ErrEndpointBudgetConfig = errors.New("endpoint attempt limit is outside the supported range")
)

// CandidateSource is the bounded numeric identity of a peer source. Session
// code assigns tracker IDs once per tracker update; entries retain only this
// fixed-size value.
type CandidateSource uint16

const (
	// DefaultCandidateSource is used by Add and Admit when the caller has no
	// source identity.
	DefaultCandidateSource CandidateSource = iota
	// MagnetCandidateSource is reserved for magnet-embedded peers.
	MagnetCandidateSource
	// FirstTrackerCandidateSource is the first ID available to tracker sources.
	FirstTrackerCandidateSource
)

// Candidate is an endpoint received from a tracker or a magnet. Host may be
// an IP literal or a DNS name. A tracker-supplied peer ID is only an optional
// expected handshake value; it never participates in endpoint deduplication.
type Candidate struct {
	Host           string
	Port           uint16
	ExpectedPeerID [20]byte
	HasExpectedID  bool
}

// ResolvedCandidate is a candidate after DNS resolution. Endpoint is the
// identity used by both transports and is therefore the only candidate key.
type ResolvedCandidate struct {
	Endpoint       Endpoint
	ExpectedPeerID [20]byte
	HasExpectedID  bool
	// Source is the source that currently owns this candidate's pool slot.
	// Duplicate announcements refresh the expected peer ID without changing
	// slot ownership.
	Source CandidateSource
}

// String renders an endpoint in the standard host:port form used by dialers
// and diagnostics.
func (e Endpoint) String() string {
	if !e.Addr.IsValid() {
		return "<invalid>"
	}
	return netip.AddrPortFrom(e.Addr, e.Port).String()
}

// Resolver is deliberately small so DNS results can be bounded in tests and
// production. It matches net.Resolver and the tracker resolver seam.
type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// CandidatePoolConfig controls bounded candidate admission. Zero values use
// the supported limits and the system resolver.
type CandidatePoolConfig struct {
	Resolver      Resolver
	MaxCandidates int
	MaxDNSAnswers int
}

// CandidatePool owns one deduplicated, bounded set of resolved endpoints.
// It is safe for tracker and magnet producers to call concurrently. The
// session coordinator still owns when admitted candidates are dialed.
type CandidatePool struct {
	resolver      Resolver
	maxCandidates int
	maxDNSAnswers int

	mu          sync.Mutex
	candidates  map[Endpoint]*list.Element
	sourceCount map[CandidateSource]int
	sourceOrder map[CandidateSource]*list.List
	order       list.List
}

// NewCandidatePool validates bounds and returns an empty candidate set.
func NewCandidatePool(config CandidatePoolConfig) (*CandidatePool, error) {
	maxCandidates := config.MaxCandidates
	if maxCandidates == 0 {
		maxCandidates = limits.Candidates
	}
	maxDNSAnswers := config.MaxDNSAnswers
	if maxDNSAnswers == 0 {
		maxDNSAnswers = limits.DNSAnswers
	}
	if maxCandidates < 1 || maxCandidates > limits.Candidates || maxDNSAnswers < 1 || maxDNSAnswers > limits.DNSAnswers {
		return nil, ErrCandidateConfig
	}
	resolver := config.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &CandidatePool{
		resolver:      resolver,
		maxCandidates: maxCandidates,
		maxDNSAnswers: maxDNSAnswers,
		candidates:    make(map[Endpoint]*list.Element),
		sourceCount:   make(map[CandidateSource]int),
		sourceOrder:   make(map[CandidateSource]*list.List),
	}, nil
}

// ResolveCandidate validates and resolves one candidate. Results are ordered
// by the resolver's answer order, with duplicate addresses removed. DNS
// answers after the supported bound are ignored before endpoint admission.
func ResolveCandidate(ctx context.Context, resolver Resolver, candidate Candidate) ([]ResolvedCandidate, error) {
	return resolveCandidate(ctx, resolver, candidate, limits.DNSAnswers)
}

func resolveCandidate(ctx context.Context, resolver Resolver, candidate Candidate, maxDNSAnswers int) ([]ResolvedCandidate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxDNSAnswers < 1 || maxDNSAnswers > limits.DNSAnswers {
		return nil, ErrCandidateConfig
	}
	if candidate.Port == 0 {
		return nil, fmt.Errorf("%w: port must be between 1 and 65535", ErrCandidate)
	}
	if candidate.Host == "" {
		return nil, fmt.Errorf("%w: empty host", ErrCandidate)
	}
	if addr, err := netip.ParseAddr(candidate.Host); err == nil {
		endpoint, ok := normalizedEndpoint(addr, candidate.Port)
		if !ok {
			return nil, fmt.Errorf("%w: address is unspecified or multicast", ErrCandidate)
		}
		return []ResolvedCandidate{withExpected(endpoint, candidate)}, nil
	}
	if !validCandidateHost(candidate.Host) {
		return nil, fmt.Errorf("%w: malformed hostname", ErrCandidate)
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	answers, err := resolver.LookupIPAddr(ctx, candidate.Host)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCandidateDNS, err)
	}
	if len(answers) > maxDNSAnswers {
		answers = answers[:maxDNSAnswers]
	}
	seen := make(map[Endpoint]struct{}, len(answers))
	result := make([]ResolvedCandidate, 0, len(answers))
	for _, answer := range answers {
		addr, ok := netip.AddrFromSlice(answer.IP)
		if !ok {
			continue
		}
		if answer.Zone != "" {
			addr = addr.WithZone(answer.Zone)
		}
		endpoint, ok := normalizedEndpoint(addr, candidate.Port)
		if !ok {
			continue
		}
		if _, exists := seen[endpoint]; exists {
			continue
		}
		seen[endpoint] = struct{}{}
		result = append(result, withExpected(endpoint, candidate))
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: no usable addresses", ErrCandidateDNS)
	}
	return result, nil
}

func validCandidateHost(host string) bool {
	if host == "" || len(host) > limits.PathBytes || host != strings.TrimSpace(host) {
		return false
	}
	for _, r := range host {
		if unicode.IsSpace(r) || unicode.IsControl(r) || strings.ContainsRune("/;?#\\[]%", r) {
			return false
		}
	}
	return true
}

// NormalizeEndpoint validates an already resolved endpoint. IPv4-mapped IPv6
// values are unmapped so they share identity with their IPv4 form.
func NormalizeEndpoint(endpoint Endpoint) (Endpoint, error) {
	normalized, ok := normalizedEndpoint(endpoint.Addr, endpoint.Port)
	if !ok {
		return Endpoint{}, fmt.Errorf("%w: endpoint is invalid", ErrCandidate)
	}
	return normalized, nil
}

func normalizedEndpoint(addr netip.Addr, port uint16) (Endpoint, bool) {
	addr = addr.Unmap()
	if port == 0 || !addr.IsValid() || addr.IsUnspecified() || addr.IsMulticast() ||
		(!addr.IsGlobalUnicast() && !addr.IsLoopback() && !addr.IsLinkLocalUnicast()) {
		return Endpoint{}, false
	}
	return Endpoint{Addr: addr, Port: port}, true
}

func withExpected(endpoint Endpoint, candidate Candidate) ResolvedCandidate {
	return ResolvedCandidate{Endpoint: endpoint, ExpectedPeerID: candidate.ExpectedPeerID, HasExpectedID: candidate.HasExpectedID}
}

// Admit resolves and adds one candidate. Duplicate endpoints are refreshed
// with the newest optional expected ID and return false.
func (p *CandidatePool) Admit(ctx context.Context, candidate Candidate) ([]ResolvedCandidate, error) {
	return p.AdmitFrom(ctx, DefaultCandidateSource, candidate)
}

// AdmitFrom resolves and adds one candidate under source's capacity share.
func (p *CandidatePool) AdmitFrom(ctx context.Context, source CandidateSource, candidate Candidate) ([]ResolvedCandidate, error) {
	if p == nil {
		return nil, ErrCandidateConfig
	}
	resolved, err := resolveCandidate(ctx, p.resolver, candidate, p.maxDNSAnswers)
	if err != nil {
		return nil, err
	}
	added := make([]ResolvedCandidate, 0, len(resolved))
	for _, item := range resolved {
		item.Source = source
		ok, err := p.AddFrom(source, item)
		if err != nil {
			return added, err
		}
		if ok {
			added = append(added, item)
		}
	}
	return added, nil
}

// Add inserts or refreshes one already-resolved candidate under the default
// source. It performs the same endpoint normalization as NormalizeEndpoint,
// so callers cannot bypass admission filtering by skipping DNS. A duplicate
// refreshes its optional expected ID and moves to the newest position.
func (p *CandidatePool) Add(candidate ResolvedCandidate) (bool, error) {
	return p.AddFrom(DefaultCandidateSource, candidate)
}

// AddFrom inserts or refreshes one already-resolved candidate under source's
// capacity share. When full, admission evicts from the most represented other
// source if source is underrepresented; otherwise it replaces the oldest
// candidate owned by source. This lets a lone source use the full pool while
// preventing repeated full announcements from one source from cycling out all
// candidates admitted by another. Slot ownership is independent of endpoint
// health, live connections, and strikes.
func (p *CandidatePool) AddFrom(source CandidateSource, candidate ResolvedCandidate) (bool, error) {
	if p == nil {
		return false, ErrCandidateConfig
	}
	endpoint, err := NormalizeEndpoint(candidate.Endpoint)
	if err != nil {
		return false, err
	}
	candidate.Endpoint = endpoint
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, exists := p.candidates[endpoint]; exists {
		entry := current.Value.(*candidatePoolEntry)
		entry.candidate.ExpectedPeerID = candidate.ExpectedPeerID
		entry.candidate.HasExpectedID = candidate.HasExpectedID
		// Keep the incumbent source's capacity ownership. A duplicate doesn't
		// consume another pool slot or transfer the incumbent's share.
		p.order.MoveToBack(current)
		p.sourceOrder[entry.candidate.Source].MoveToBack(entry.sourceElement)
		return false, nil
	}
	if len(p.candidates) >= p.maxCandidates {
		p.evictForSourceLocked(source)
	}
	candidate.Source = source
	entry := &candidatePoolEntry{candidate: candidate}
	element := p.order.PushBack(entry)
	entry.globalElement = element
	if p.sourceOrder[source] == nil {
		p.sourceOrder[source] = &list.List{}
	}
	entry.sourceElement = p.sourceOrder[source].PushBack(entry)
	p.candidates[endpoint] = element
	p.sourceCount[source]++
	return true, nil
}

func (p *CandidatePool) evictForSourceLocked(source CandidateSource) {
	ownCount := p.sourceCount[source]
	maxOtherCount := 0
	var maxOtherSource CandidateSource
	hasOther := false
	for other, count := range p.sourceCount {
		if other == source {
			continue
		}
		if !hasOther || count > maxOtherCount || count == maxOtherCount && other < maxOtherSource {
			maxOtherCount, maxOtherSource = count, other
			hasOther = true
		}
	}

	// If source is new or underrepresented, remove the oldest candidate from
	// the most represented other source. The bounded source map chooses a
	// deterministic source on ties; its per-source LRU provides the victim in
	// constant time. Otherwise replace source's own oldest candidate.
	preferOther := ownCount == 0 || maxOtherCount > ownCount
	target := source
	if preferOther && hasOther {
		target = maxOtherSource
	}
	if sourceOrder := p.sourceOrder[target]; sourceOrder != nil && sourceOrder.Front() != nil {
		p.removeEntryLocked(sourceOrder.Front().Value.(*candidatePoolEntry))
	}
}

func (p *CandidatePool) removeEntryLocked(entry *candidatePoolEntry) {
	delete(p.candidates, entry.candidate.Endpoint)
	p.order.Remove(entry.globalElement)
	source := entry.candidate.Source
	perSource := p.sourceOrder[source]
	perSource.Remove(entry.sourceElement)
	p.sourceCount[source]--
	if p.sourceCount[source] == 0 {
		delete(p.sourceCount, source)
		delete(p.sourceOrder, source)
	}
}

type candidatePoolEntry struct {
	candidate     ResolvedCandidate
	globalElement *list.Element
	sourceElement *list.Element
}

// Snapshot returns an independent newest-announcement-first candidate slice.
// The order is deterministic for a given admission sequence and lets fresh
// tracker responses be attempted before older candidates.
func (p *CandidatePool) Snapshot() []ResolvedCandidate {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	result := make([]ResolvedCandidate, 0, len(p.candidates))
	for element := p.order.Back(); element != nil; element = element.Prev() {
		result = append(result, element.Value.(*candidatePoolEntry).candidate)
	}
	p.mu.Unlock()
	return result
}

func (p *CandidatePool) Len() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	n := len(p.candidates)
	p.mu.Unlock()
	return n
}

// EndpointBackoff tracks ordinary dial failures independently of transport,
// candidate source, and peer ID. Ordinary failure state is bounded; blacklist
// entries persist for the run. It does not record corruption strikes; protocol
// and piece penalties remain coordinator-owned.
type EndpointBackoff struct {
	mu           sync.Mutex
	states       map[Endpoint]*list.Element
	blacklisted  map[Endpoint]struct{}
	attempted    map[Endpoint]struct{}
	attemptLimit int
	budgetErr    *EndpointBudgetError
	order        list.List
}

type endpointBackoffState struct {
	failures  uint8
	notBefore time.Time
}

type endpointBackoffEntry struct {
	endpoint Endpoint
	state    endpointBackoffState
}

const (
	endpointFailureBase = time.Second
	endpointFailureMax  = 5 * time.Minute
	// Match the ordinary-state cap to the maximum number of retained candidates.
	maxEndpointFailureStates = limits.Candidates
)

// NewEndpointBackoff creates empty endpoint health state.
func NewEndpointBackoff() *EndpointBackoff {
	return newEndpointBackoff(limits.EndpointAttempts)
}

// NewEndpointBackoffWithLimit creates an endpoint backoff with a smaller
// distinct-endpoint attempt limit. Production callers should use
// NewEndpointBackoff, which fixes the limit at the supported run-wide bound.
func NewEndpointBackoffWithLimit(limit int) (*EndpointBackoff, error) {
	if limit < 1 || limit > limits.EndpointAttempts {
		return nil, ErrEndpointBudgetConfig
	}
	return newEndpointBackoff(limit), nil
}

func newEndpointBackoff(limit int) *EndpointBackoff {
	return &EndpointBackoff{
		states:       make(map[Endpoint]*list.Element),
		blacklisted:  make(map[Endpoint]struct{}),
		attempted:    make(map[Endpoint]struct{}),
		attemptLimit: limit,
		budgetErr:    &EndpointBudgetError{Limit: limit},
	}
}

// beginAttempt reserves an endpoint identity before its first transport race.
// Retries of an already-attempted endpoint remain allowed without using more
// budget. Callers hold their race slot before invoking this method.
func (b *EndpointBackoff) beginAttempt(endpoint Endpoint, now time.Time) error {
	if b == nil {
		return nil
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.blacklisted[endpoint]; ok {
		return fmt.Errorf("%w: endpoint is blacklisted", ErrCandidate)
	}
	if element := b.states[endpoint]; element != nil {
		state := element.Value.(*endpointBackoffEntry).state
		if now.Before(state.notBefore) {
			return fmt.Errorf("%w: endpoint backoff is active", ErrCandidate)
		}
	}
	if _, exists := b.attempted[endpoint]; exists {
		return nil
	}
	if len(b.attempted) >= b.attemptLimit {
		return b.budgetErr
	}
	b.attempted[endpoint] = struct{}{}
	return nil
}

// Ready reports whether an endpoint may be attempted at now. A zero now uses
// time.Now, which keeps normal callers concise while deterministic tests pass a
// fixed value explicitly.
func (b *EndpointBackoff) Ready(endpoint Endpoint, now time.Time) bool {
	if b == nil {
		return false
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.mu.Lock()
	_, blacklisted := b.blacklisted[endpoint]
	element := b.states[endpoint]
	var state endpointBackoffState
	if element != nil {
		state = element.Value.(*endpointBackoffEntry).state
	}
	b.mu.Unlock()
	return !blacklisted && !now.Before(state.notBefore)
}

// RecordFailure records one ordinary failure and returns the resulting
// not-before time. The bounded exponential delay is shared by TCP and uTP.
func (b *EndpointBackoff) RecordFailure(endpoint Endpoint, now time.Time) time.Time {
	if b == nil {
		return now
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return now
	}
	if now.IsZero() {
		now = time.Now()
	}
	b.mu.Lock()
	if _, blacklisted := b.blacklisted[endpoint]; blacklisted {
		b.mu.Unlock()
		return now
	}
	element := b.states[endpoint]
	var state endpointBackoffState
	if element != nil {
		state = element.Value.(*endpointBackoffEntry).state
	}
	if state.failures < 8 {
		state.failures++
	}
	delay := endpointFailureBase
	for i := uint8(1); i < state.failures && delay < endpointFailureMax; i++ {
		delay *= 2
		if delay >= endpointFailureMax {
			delay = endpointFailureMax
			break
		}
	}
	state.notBefore = now.Add(delay)
	if element == nil {
		if len(b.states) >= maxEndpointFailureStates {
			oldest := b.order.Front()
			entry := oldest.Value.(*endpointBackoffEntry)
			delete(b.states, entry.endpoint)
			b.order.Remove(oldest)
		}
		element = b.order.PushBack(&endpointBackoffEntry{endpoint: endpoint, state: state})
		b.states[endpoint] = element
	} else {
		entry := element.Value.(*endpointBackoffEntry)
		entry.state = state
		b.order.MoveToBack(element)
	}
	b.mu.Unlock()
	return state.notBefore
}

// RecordSuccess clears ordinary failure backoff. It never clears a blacklist.
func (b *EndpointBackoff) RecordSuccess(endpoint Endpoint) {
	if b == nil {
		return
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return
	}
	b.mu.Lock()
	b.removeFailureStateLocked(endpoint)
	b.mu.Unlock()
}

// Blacklist permanently blocks an endpoint for the current run. The caller
// decides whether the reason is a severe protocol violation or a thresholded
// corruption strike; both reasons share endpoint identity.
func (b *EndpointBackoff) Blacklist(endpoint Endpoint) {
	if b == nil {
		return
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return
	}
	b.mu.Lock()
	b.blacklisted[endpoint] = struct{}{}
	b.removeFailureStateLocked(endpoint)
	b.mu.Unlock()
}

func (b *EndpointBackoff) removeFailureStateLocked(endpoint Endpoint) {
	if element := b.states[endpoint]; element != nil {
		delete(b.states, endpoint)
		b.order.Remove(element)
	}
}

func (b *EndpointBackoff) IsBlacklisted(endpoint Endpoint) bool {
	if b == nil {
		return false
	}
	endpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return false
	}
	b.mu.Lock()
	_, blocked := b.blacklisted[endpoint]
	b.mu.Unlock()
	return blocked
}
