package peer

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
)

type candidateResolver struct {
	answers []net.IPAddr
	err     error
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
