package peer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestAuditZonedLoopbackAliasesBypassEndpointBlacklist(t *testing.T) {
	pool, err := NewCandidatePool(CandidatePoolConfig{MaxCandidates: 1})
	if err != nil {
		t.Fatal(err)
	}
	variants := []string{"::1", "::1%anything", "::1%another-zone"}
	var canonical Endpoint
	for i, host := range variants {
		candidate := Candidate{Host: host, Port: 6881, HasExpectedID: true, ExpectedPeerID: [20]byte{byte(i + 1)}}
		resolved, err := ResolveCandidate(context.Background(), nil, candidate)
		if err != nil || len(resolved) != 1 {
			t.Fatalf("resolve %q = %#v, %v", host, resolved, err)
		}
		if i == 0 {
			canonical = resolved[0].Endpoint
		} else if resolved[0].Endpoint != canonical {
			t.Fatalf("%q identity = %s, want %s", host, resolved[0].Endpoint, canonical)
		}
		added, err := pool.Admit(context.Background(), candidate)
		if err != nil || (i == 0 && len(added) != 1) || (i > 0 && len(added) != 0) {
			t.Fatalf("pool admission %q = %#v, %v", host, added, err)
		}
	}
	if pool.Len() != 1 || pool.Snapshot()[0].ExpectedPeerID[0] != 3 {
		t.Fatalf("aliases created separate candidates or retained stale peer ID: %#v", pool.Snapshot())
	}
	resolver := candidateResolver{answers: []net.IPAddr{
		{IP: net.ParseIP("::1"), Zone: "anything"},
		{IP: net.ParseIP("::1")},
		{IP: net.ParseIP("::1"), Zone: "another-zone"},
	}}
	resolved, err := ResolveCandidate(context.Background(), resolver, Candidate{Host: "fixture.invalid", Port: 6881})
	if err != nil || len(resolved) != 1 || resolved[0].Endpoint != canonical {
		t.Fatalf("resolver loopback aliases = %#v, %v", resolved, err)
	}
	backoff, err := NewEndpointBackoffWithLimit(1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000, 0)
	if err := backoff.beginAttempt(canonical, now); err != nil {
		t.Fatal(err)
	}
	alias := Endpoint{Addr: netip.MustParseAddr("::1%anything"), Port: 6881}
	until := backoff.RecordFailure(alias, now)
	if backoff.Ready(Endpoint{Addr: netip.MustParseAddr("::1%another-zone"), Port: 6881}, now) {
		t.Fatal("zoned alias bypassed ordinary backoff")
	}
	if err := backoff.beginAttempt(alias, until); err != nil || len(backoff.attempted) != 1 {
		t.Fatalf("alias retry = %v, distinct attempts=%d", err, len(backoff.attempted))
	}
	backoff.Blacklist(alias)
	if !backoff.IsBlacklisted(canonical) || backoff.Ready(canonical, until) {
		t.Fatal("zoned alias bypassed run-long blacklist")
	}
}

func TestEndpointZonesUseOnlyMeaningfulInterfaceScope(t *testing.T) {
	interfaces := map[int]*net.Interface{
		2: {Index: 2, Name: "primary"},
		3: {Index: 3, Name: "other"},
	}
	byName := func(name string) (*net.Interface, error) {
		switch name {
		case "primary", "alias":
			return &net.Interface{Index: 2, Name: name}, nil
		case "other":
			return interfaces[3], nil
		default:
			return nil, errors.New("no such interface")
		}
	}
	byIndex := func(index int) (*net.Interface, error) {
		if ifi := interfaces[index]; ifi != nil {
			return ifi, nil
		}
		return nil, errors.New("no such interface")
	}
	address := func(host string) (Endpoint, bool) {
		return normalizedEndpointWithInterfaces(netip.MustParseAddr(host), 6881, byName, byIndex)
	}
	canonical, ok := address("fe80::1%primary")
	if !ok || canonical.Addr.Zone() != "primary" {
		t.Fatalf("canonical link-local scope = %s, %t", canonical, ok)
	}
	for _, host := range []string{"fe80::1%alias", "fe80::1%2", "fe80::1%02", "fe80::1%2suffix"} {
		if got, ok := address(host); !ok || got != canonical {
			t.Fatalf("%q identity = %s, %t; want %s", host, got, ok, canonical)
		}
	}
	if other, ok := address("fe80::1%other"); !ok || other == canonical || other.Addr.Zone() != "other" {
		t.Fatalf("different scope = %s, %t", other, ok)
	}
	for _, host := range []string{"fe80::1%missing", "fe80::1%99", "fe80::1%0"} {
		if got, ok := address(host); ok {
			t.Fatalf("invalid interface %q admitted as %s", host, got)
		}
	}
	for _, host := range []string{"::1%missing", "fd00::1%missing", "2001:db8::1%missing"} {
		got, ok := address(host)
		if !ok || got.Addr.Zone() != "" {
			t.Fatalf("irrelevant zone %q retained: %s, %t", host, got, ok)
		}
	}
	// Go resolves names before parsing a leading decimal index. An interface
	// literally named "2" therefore takes precedence over numeric index 2.
	nameFirst := func(name string) (*net.Interface, error) {
		if name == "2" {
			return interfaces[3], nil
		}
		return byName(name)
	}
	got, ok := normalizedEndpointWithInterfaces(netip.MustParseAddr("fe80::1%2"), 6881, nameFirst, byIndex)
	if !ok || got.Addr.Zone() != "other" {
		t.Fatalf("numeric interface name precedence = %s, %t", got, ok)
	}
}

func localScopedInterface(t *testing.T) net.Interface {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Skipf("local interface lookup unavailable: %v", err)
	}
	for _, ifi := range interfaces {
		if ifi.Index > 0 && ifi.Name != "" {
			return ifi
		}
	}
	t.Skip("no named local interface")
	return net.Interface{}
}

func TestEndpointResolverAndDialShareScopedAliases(t *testing.T) {
	ifi := localScopedInterface(t)
	name := "fe80::1%" + ifi.Name
	numeric := "fe80::1%" + strconv.Itoa(ifi.Index)
	nameEndpoint, err := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr(name), Port: 6881})
	if err != nil {
		t.Fatal(err)
	}
	numericEndpoint, err := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr(numeric), Port: 6881})
	if err != nil || numericEndpoint != nameEndpoint {
		t.Fatalf("name/numeric endpoints = %s / %s, %v", nameEndpoint, numericEndpoint, err)
	}
	resolver := candidateResolver{answers: []net.IPAddr{
		{IP: net.ParseIP("fe80::1"), Zone: ifi.Name},
		{IP: net.ParseIP("fe80::1"), Zone: strconv.Itoa(ifi.Index)},
	}}
	resolved, err := ResolveCandidate(context.Background(), resolver, Candidate{Host: "fixture.invalid", Port: 6881})
	if err != nil || len(resolved) != 1 || resolved[0].Endpoint != nameEndpoint {
		t.Fatalf("scoped resolver aliases = %#v, %v", resolved, err)
	}
	pool, err := NewCandidatePool(CandidatePoolConfig{Resolver: resolver, MaxCandidates: 2})
	if err != nil {
		t.Fatal(err)
	}
	if added, err := pool.Admit(context.Background(), Candidate{Host: "fixture.invalid", Port: 6881}); err != nil || len(added) != 1 {
		t.Fatalf("resolver admission = %#v, %v", added, err)
	}
	if added, err := pool.Admit(context.Background(), Candidate{Host: numeric, Port: 6881}); err != nil || len(added) != 0 || pool.Len() != 1 {
		t.Fatalf("numeric alias admission = %#v, %v", added, err)
	}
	backoff, err := NewEndpointBackoffWithLimit(1)
	if err != nil {
		t.Fatal(err)
	}
	local := testHandshake()
	remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{9}}
	var callsMu sync.Mutex
	var calls []string
	secondAttempt := false
	config := RaceConfig{
		LocalHandshake: local,
		UTPHeadStart:   time.Millisecond,
		UTPDial: func(_ context.Context, network, address string) (net.Conn, error) {
			callsMu.Lock()
			calls = append(calls, network+" "+address)
			second := secondAttempt
			callsMu.Unlock()
			if !second {
				return nil, errors.New("uTP unavailable")
			}
			client, server := net.Pipe()
			pipeServer(t, server, local, remote, 0)
			return client, nil
		},
		TCPDial: func(_ context.Context, network, address string) (net.Conn, error) {
			callsMu.Lock()
			calls = append(calls, network+" "+address)
			second := secondAttempt
			callsMu.Unlock()
			if second {
				return nil, errors.New("TCP unavailable")
			}
			client, server := net.Pipe()
			pipeServer(t, server, local, remote, 0)
			return client, nil
		},
	}
	now := time.Unix(1_000, 0)
	manager, err := NewDialManager(DialManagerConfig{Race: config, Backoff: backoff, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for i, ep := range []Endpoint{nameEndpoint, numericEndpoint} {
		callsMu.Lock()
		secondAttempt = i == 1
		callsMu.Unlock()
		result, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: ep})
		if err != nil || result.Endpoint != nameEndpoint {
			t.Fatalf("race %d = %#v, %v", i, result, err)
		}
		want := TransportTCP
		if i == 1 {
			want = TransportUTP
		}
		if result.Transport != want {
			t.Fatalf("race %d transport = %v, want %v", i, result.Transport, want)
		}
		_ = result.Conn.Close()
	}
	if len(backoff.attempted) != 1 {
		t.Fatalf("scoped aliases consumed %d attempts", len(backoff.attempted))
	}
	callsMu.Lock()
	wantAddress := nameEndpoint.String()
	for _, call := range calls {
		if call != "utp6 "+wantAddress && call != "tcp6 "+wantAddress {
			callsMu.Unlock()
			t.Fatalf("dial used noncanonical scoped address: %q", call)
		}
	}
	callsMu.Unlock()
	if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: Endpoint{Addr: nameEndpoint.Addr, Port: 6882}}); !errors.Is(err, ErrEndpointBudget) {
		t.Fatalf("distinct port budget error = %v", err)
	}
	backoff.Blacklist(numericEndpoint)
	if !backoff.IsBlacklisted(nameEndpoint) {
		t.Fatal("numeric alias did not share blacklist")
	}
	if _, err := manager.Race(context.Background(), ResolvedCandidate{Endpoint: nameEndpoint}); !errors.Is(err, ErrCandidate) {
		t.Fatalf("blacklisted alias race = %v", err)
	}
	if _, err := ResolveCandidate(context.Background(), nil, Candidate{Host: "fe80::1%nonexistent-interface", Port: 6881}); !errors.Is(err, ErrCandidate) || IsProtocolViolation(err) {
		t.Fatalf("invalid interface error = %v, want ordinary candidate error", err)
	}
}

func TestLinuxZonedLoopbackRoutesLikeUnzoned(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux routing check")
	}
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("local IPv6 unavailable: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	for _, host := range []string{"::1", "::1%anything", "::1%another-zone"} {
		address := net.JoinHostPort(host, strconv.Itoa(port))
		conn, err := net.DialTimeout("tcp6", address, time.Second)
		if err != nil {
			t.Fatalf("dial local %s: %v", address, err)
		}
		if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		accepted, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		_ = accepted.Close()
		_ = conn.Close()
	}
}

func TestEndpointDistinctPrivateAndLoopbackFamiliesRemainAccepted(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.2", "::1", "fd00::2", "2001:db8::2"} {
		ep, err := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr(address), Port: 6881})
		if err != nil || ep.Addr.String() != address {
			t.Fatalf("accepted endpoint %s = %s, %v", address, ep, err)
		}
	}
	for _, pair := range [][2]string{{"::1", "::1%ignored"}, {"fd00::2", "fd00::2%ignored"}, {"2001:db8::2", "2001:db8::2%ignored"}} {
		first, _ := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr(pair[0]), Port: 6881})
		second, _ := NormalizeEndpoint(Endpoint{Addr: netip.MustParseAddr(pair[1]), Port: 6881})
		if first != second {
			t.Fatalf("irrelevant zone changed identity: %s != %s", first, second)
		}
	}
}
