package peer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

type candidateResolver struct {
	answers []net.IPAddr
	err     error
}

func backoffTestEndpoint(index int) Endpoint {
	value := uint32(index)
	addr := netip.AddrFrom4([4]byte{10, byte(value >> 16), byte(value >> 8), byte(value)})
	return Endpoint{Addr: addr, Port: 6881}
}

func (r candidateResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return r.answers, r.err
}

func TestResolveCandidateFiltersAndDeduplicates(t *testing.T) {
	resolver := candidateResolver{answers: []net.IPAddr{
		{IP: net.ParseIP("0.0.0.0")},
		{IP: net.ParseIP("224.0.0.1")},
		{IP: net.ParseIP("127.0.0.1")},
		{IP: net.ParseIP("127.0.0.1")},
		{IP: net.ParseIP("10.0.0.2")},
	}}
	got, err := ResolveCandidate(context.Background(), resolver, Candidate{Host: "peer.test", Port: 6881})
	if err != nil {
		t.Fatalf("ResolveCandidate: %v", err)
	}
	if len(got) != 2 || got[0].Endpoint.String() != "127.0.0.1:6881" || got[1].Endpoint.String() != "10.0.0.2:6881" {
		t.Fatalf("resolved candidates = %#v", got)
	}
}

func TestCandidatePoolBoundsDNSAndEndpointSet(t *testing.T) {
	resolver := candidateResolver{answers: []net.IPAddr{
		{IP: net.ParseIP("192.0.2.1")},
		{IP: net.ParseIP("192.0.2.2")},
		{IP: net.ParseIP("192.0.2.3")},
	}}
	pool, err := NewCandidatePool(CandidatePoolConfig{Resolver: resolver, MaxDNSAnswers: 2, MaxCandidates: 2})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	added, err := pool.Admit(context.Background(), Candidate{Host: "peer.test", Port: 1})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if len(added) != 2 || pool.Len() != 2 {
		t.Fatalf("added=%d pool=%d", len(added), pool.Len())
	}
	if duplicate, err := pool.Admit(context.Background(), Candidate{Host: "peer.test", Port: 1}); err != nil || len(duplicate) != 0 {
		t.Fatalf("duplicate admission = %#v, %v", duplicate, err)
	}
}

func TestCandidatePoolEvictsLeastRecentlyAnnouncedEndpoint(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 2})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	first := ResolvedCandidate{Endpoint: Endpoint{Addr: netip.MustParseAddr("192.0.2.1"), Port: 1}}
	second := ResolvedCandidate{Endpoint: Endpoint{Addr: netip.MustParseAddr("192.0.2.2"), Port: 1}}
	later := ResolvedCandidate{Endpoint: Endpoint{Addr: netip.MustParseAddr("192.0.2.3"), Port: 1}}
	backoff := NewEndpointBackoff()
	backoff.Blacklist(first.Endpoint)
	for _, candidate := range []ResolvedCandidate{first, second} {
		if added, err := pool.Add(candidate); err != nil || !added {
			t.Fatalf("Add(%s) = %t, %v", candidate.Endpoint, added, err)
		}
	}

	// Reannouncing the first endpoint refreshes its age, so the next distinct
	// peer replaces the still-stale second endpoint.
	if added, err := pool.Add(first); err != nil || added {
		t.Fatalf("refresh first = %t, %v", added, err)
	}
	if added, err := pool.Add(later); err != nil || !added {
		t.Fatalf("Add(later) = %t, %v", added, err)
	}
	got := pool.Snapshot()
	if len(got) != 2 || got[0].Endpoint != later.Endpoint || got[1].Endpoint != first.Endpoint {
		t.Fatalf("pool after replacement = %#v", got)
	}
	if pool.Len() != 2 {
		t.Fatalf("pool size = %d, want 2", pool.Len())
	}
	if !backoff.IsBlacklisted(first.Endpoint) {
		t.Fatal("candidate eviction erased endpoint blacklist state")
	}
}

func TestCandidatePoolTriesLaterPeerBeforeFullOlderPool(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	// The first tracker fills every supported candidate slot with endpoints
	// that the deterministic dialer will consider unreachable.
	for index := 0; index < limits.Candidates; index++ {
		addr := netip.AddrFrom4([4]byte{10, byte(index >> 8), byte(index), 1})
		if _, err := pool.Add(ResolvedCandidate{Endpoint: Endpoint{Addr: addr, Port: 6881}}); err != nil {
			t.Fatalf("add initial candidate %d: %v", index, err)
		}
	}
	good := ResolvedCandidate{Endpoint: Endpoint{Addr: netip.MustParseAddr("203.0.113.1"), Port: 51413}, Source: DefaultCandidateSource}
	// A later tracker can replace a stale entry and should be dialed first.
	if _, err := pool.Add(good); err != nil {
		t.Fatalf("add later tracker candidate: %v", err)
	}
	snapshot := pool.Snapshot()
	if len(snapshot) != limits.Candidates {
		t.Fatalf("pool size = %d, want %d", len(snapshot), limits.Candidates)
	}
	if snapshot[0] != good {
		t.Fatalf("first candidate = %+v, want later tracker peer %+v", snapshot[0], good)
	}

	local := testHandshake()
	remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{8}}
	goodDial := handshakePipeDial(t, local, remote)
	var attempted []string
	manager, err := NewDialManager(DialManagerConfig{Race: RaceConfig{
		LocalHandshake: local,
		TCPDial: func(ctx context.Context, network, address string) (net.Conn, error) {
			attempted = append(attempted, address)
			if address != good.Endpoint.String() {
				return nil, errors.New("older candidate is unreachable")
			}
			return goodDial(ctx, network, address)
		},
	}})
	if err != nil {
		t.Fatalf("NewDialManager: %v", err)
	}
	connected, err := manager.Race(context.Background(), snapshot[0])
	if err != nil {
		t.Fatalf("dial first snapshot candidate: %v", err)
	}
	_ = connected.Conn.Close()
	if len(attempted) != 1 || attempted[0] != good.Endpoint.String() {
		t.Fatalf("dial attempts = %v, want the later peer first", attempted)
	}
}

func TestCandidatePoolKeepsCapacityFairAcrossSources(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 3})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	endpoint := func(last byte) ResolvedCandidate {
		return ResolvedCandidate{Endpoint: Endpoint{Addr: netip.AddrFrom4([4]byte{192, 0, 2, last}), Port: 51413}}
	}
	from := func(source string, candidate ResolvedCandidate) bool {
		t.Helper()
		added, err := pool.AddFrom(source, candidate)
		if err != nil {
			t.Fatalf("AddFrom(%q, %s): %v", source, candidate.Endpoint, err)
		}
		return added
	}
	a := []ResolvedCandidate{endpoint(1), endpoint(2), endpoint(3)}
	for _, candidate := range a {
		if !from("tracker-A", candidate) {
			t.Fatalf("first admission of %s was not added", candidate.Endpoint)
		}
	}
	good := endpoint(4)
	if !from("tracker-B", good) {
		t.Fatal("new source's endpoint was not admitted")
	}

	// A full reannouncement cycles the one A endpoint displaced by B. It must
	// not cycle B out, regardless of how many times A repeats the same set.
	for round := 0; round < 5; round++ {
		for _, candidate := range a {
			from("tracker-A", candidate)
		}
		if !candidatePresent(pool.Snapshot(), good.Endpoint) {
			t.Fatalf("tracker-B endpoint evicted during A reannouncement round %d", round)
		}
	}

	// A source with fewer owned slots can grow by evicting from the
	// overrepresented source.
	bSecond := endpoint(5)
	if !from("tracker-B", bSecond) {
		t.Fatal("underrepresented tracker-B did not grow")
	}
	if !candidatePresent(pool.Snapshot(), good.Endpoint) || !candidatePresent(pool.Snapshot(), bSecond.Endpoint) {
		t.Fatalf("tracker-B candidates did not both remain after growth: %+v", pool.Snapshot())
	}

	// Duplicates retain endpoint identity and capacity ownership while the most
	// recent tracker-supplied expected peer ID replaces the stale value.
	refreshedID := [20]byte{7, 8, 9}
	duplicate := a[2]
	duplicate.ExpectedPeerID = refreshedID
	duplicate.HasExpectedID = true
	if from("tracker-B", duplicate) {
		t.Fatal("duplicate endpoint consumed another pool slot")
	}
	var refreshed *ResolvedCandidate
	for _, candidate := range pool.Snapshot() {
		if candidate.Endpoint == duplicate.Endpoint {
			copy := candidate
			refreshed = &copy
			break
		}
	}
	if refreshed == nil || !refreshed.HasExpectedID || refreshed.ExpectedPeerID != refreshedID || refreshed.Source != "tracker-A" {
		t.Fatalf("duplicate refresh = %+v, want refreshed ID and incumbent source ownership", refreshed)
	}
	if pool.Len() != 3 {
		t.Fatalf("pool size after duplicate refresh = %d, want 3", pool.Len())
	}
}

func TestCandidatePoolMaintainsPerSourceOldestIndex(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 3})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	candidate := func(last byte) ResolvedCandidate {
		return ResolvedCandidate{Endpoint: Endpoint{Addr: netip.AddrFrom4([4]byte{192, 0, 2, last}), Port: 51413}}
	}
	for _, last := range []byte{1, 2, 3} {
		if _, err := pool.AddFrom("tracker-A", candidate(last)); err != nil {
			t.Fatalf("add A%d: %v", last, err)
		}
	}
	if _, err := pool.AddFrom("tracker-B", candidate(4)); err != nil {
		t.Fatalf("add B4: %v", err)
	}
	assertOldest := func(source string, want Endpoint) {
		t.Helper()
		queue := pool.sourceOrder[source]
		if queue == nil || queue.Front() == nil {
			t.Fatalf("source %q has no per-source LRU entry", source)
		}
		entry := queue.Front().Value.(*candidatePoolEntry)
		if entry.candidate.Endpoint != want || pool.sourceCount[source] != queue.Len() {
			t.Fatalf("source %q oldest/count/index = %s/%d/%d, want oldest %s", source, entry.candidate.Endpoint, pool.sourceCount[source], queue.Len(), want)
		}
	}
	assertOldest("tracker-A", candidate(2).Endpoint)
	if _, err := pool.AddFrom("tracker-A", candidate(2)); err != nil {
		t.Fatalf("refresh A2: %v", err)
	}
	assertOldest("tracker-A", candidate(3).Endpoint)
	if _, err := pool.AddFrom("tracker-A", candidate(1)); err != nil {
		t.Fatalf("re-admit A1: %v", err)
	}
	// A owns two of three slots, so this admission evicts its source-local
	// oldest entry without scanning the global candidate order.
	assertOldest("tracker-A", candidate(2).Endpoint)
	if candidatePresent(pool.Snapshot(), candidate(3).Endpoint) {
		t.Fatal("source-local oldest candidate A3 was not evicted")
	}
}

func candidatePresent(candidates []ResolvedCandidate, endpoint Endpoint) bool {
	for _, candidate := range candidates {
		if candidate.Endpoint == endpoint {
			return true
		}
	}
	return false
}

func TestCandidatePoolRefreshClearsStaleExpectedPeerID(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 2})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	candidate := Candidate{Host: "192.0.2.9", Port: 51413, ExpectedPeerID: [20]byte{1}, HasExpectedID: true}
	if _, err := pool.Admit(context.Background(), candidate); err != nil {
		t.Fatalf("Admit with expected ID: %v", err)
	}
	if _, err := pool.Admit(context.Background(), Candidate{Host: candidate.Host, Port: candidate.Port}); err != nil {
		t.Fatalf("Admit without expected ID: %v", err)
	}
	got := pool.Snapshot()
	if len(got) != 1 || got[0].HasExpectedID {
		t.Fatalf("refreshed candidate = %#v, want no expected peer ID", got)
	}
}

func TestResolveCandidateRejectsInvalidEndpoint(t *testing.T) {
	for _, candidate := range []Candidate{
		{Host: "0.0.0.0", Port: 80},
		{Host: "255.255.255.255", Port: 80},
		{Host: "224.0.0.1", Port: 80},
		{Host: "127.0.0.1", Port: 0},
	} {
		if _, err := ResolveCandidate(context.Background(), nil, candidate); err == nil {
			t.Fatalf("ResolveCandidate(%+v) succeeded", candidate)
		}
	}
	if _, err := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr("::"), Port: 80}); err == nil {
		t.Fatal("unspecified endpoint accepted")
	}
}

func TestEndpointBackoffIsTransportIndependentAndBounded(t *testing.T) {
	backoff := NewEndpointBackoff()
	ep := Endpoint{Addr: netip.MustParseAddr("192.0.2.1"), Port: 6881}
	now := time.Unix(100, 0)
	if !backoff.Ready(ep, now) {
		t.Fatal("new endpoint not ready")
	}
	first := backoff.RecordFailure(ep, now)
	if first.Sub(now) != time.Second || backoff.Ready(ep, now) {
		t.Fatalf("first failure: notBefore=%v ready=%v", first, backoff.Ready(ep, now))
	}
	second := backoff.RecordFailure(ep, first)
	if second.Sub(first) != 2*time.Second {
		t.Fatalf("second delay = %v", second.Sub(first))
	}
	backoff.RecordSuccess(ep)
	if !backoff.Ready(ep, now) {
		t.Fatal("success did not clear failure delay")
	}
	backoff.Blacklist(ep)
	if backoff.Ready(ep, now) || !backoff.IsBlacklisted(ep) {
		t.Fatal("blacklist not enforced")
	}
}

func TestEndpointBackoffOrdinaryStateIsBoundedUnderCandidateChurn(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: limits.Candidates})
	if err != nil {
		t.Fatalf("NewCandidatePool: %v", err)
	}
	backoff := NewEndpointBackoff()
	now := time.Unix(100, 0)
	const churnCount = 2 * maxEndpointFailureStates
	for index := 1; index <= churnCount; index++ {
		endpoint := backoffTestEndpoint(index)
		if _, err := pool.Add(ResolvedCandidate{Endpoint: endpoint}); err != nil {
			t.Fatalf("add candidate %d: %v", index, err)
		}
		backoff.RecordFailure(endpoint, now)
	}
	if pool.Len() != limits.Candidates {
		t.Fatalf("candidate count = %d, want %d", pool.Len(), limits.Candidates)
	}
	if len(backoff.states) != maxEndpointFailureStates {
		t.Fatalf("ordinary backoff entries = %d, want cap %d", len(backoff.states), maxEndpointFailureStates)
	}
	if !backoff.Ready(backoffTestEndpoint(1), now) {
		t.Fatal("oldest failure state was not evicted after candidate churn")
	}
	if backoff.Ready(backoffTestEndpoint(churnCount), now) {
		t.Fatal("most recent failure lost its ordinary backoff")
	}
}

func TestEndpointBackoffSuccessChurnDoesNotRetainOrdinaryState(t *testing.T) {
	backoff := NewEndpointBackoff()
	now := time.Unix(100, 0)
	const churnCount = 2 * maxEndpointFailureStates
	for index := 1; index <= churnCount; index++ {
		backoff.RecordSuccess(backoffTestEndpoint(index))
	}
	if len(backoff.states) != 0 {
		t.Fatalf("success-only churn retained %d ordinary entries", len(backoff.states))
	}

	endpoint := backoffTestEndpoint(1)
	backoff.RecordFailure(endpoint, now)
	backoff.RecordSuccess(endpoint)
	if len(backoff.states) != 0 || !backoff.Ready(endpoint, now) {
		t.Fatalf("success did not clear ordinary state: entries=%d ready=%t", len(backoff.states), backoff.Ready(endpoint, now))
	}
}

func TestEndpointBackoffBlacklistSurvivesOrdinaryStateChurn(t *testing.T) {
	backoff := NewEndpointBackoff()
	blocked := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 6881}
	backoff.Blacklist(blocked)
	now := time.Unix(100, 0)
	const churnCount = 2 * maxEndpointFailureStates
	for index := 1; index <= churnCount; index++ {
		backoff.RecordFailure(backoffTestEndpoint(index), now)
	}
	if !backoff.IsBlacklisted(blocked) || backoff.Ready(blocked, now) {
		t.Fatal("ordinary-state eviction cleared an endpoint blacklist")
	}
	backoff.RecordSuccess(blocked)
	if !backoff.IsBlacklisted(blocked) || backoff.Ready(blocked, now) {
		t.Fatal("success cleared an endpoint blacklist")
	}
	if len(backoff.states) > maxEndpointFailureStates {
		t.Fatalf("ordinary backoff entries = %d, over cap %d", len(backoff.states), maxEndpointFailureStates)
	}
}
