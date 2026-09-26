package session

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestEndpointAliasesShareMetadataAndTransferStrikes(t *testing.T) {
	cases := []struct {
		name  string
		hosts []string
	}{
		{"loopback zones", []string{"::1", "::1%anything", "::1%another-zone"}},
	}
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 && iface.Index > 0 && iface.Name != "" {
			name := "fe80::1%" + iface.Name
			cases = append(cases, struct {
				name  string
				hosts []string
			}{"link-local interface aliases", []string{name, "fe80::1%" + strconv.Itoa(iface.Index), name}})
			break
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := peer.NewCandidatePool(peer.CandidatePoolConfig{})
			if err != nil {
				t.Fatal(err)
			}
			resolve := func(host string) peer.Endpoint {
				t.Helper()
				resolved, err := peer.ResolveCandidate(context.Background(), nil, peer.Candidate{Host: host, Port: 6881})
				if err != nil || len(resolved) != 1 {
					t.Fatalf("resolve %q = %v, %v", host, resolved, err)
				}
				if _, err := pool.Add(resolved[0]); err != nil {
					t.Fatal(err)
				}
				return resolved[0].Endpoint
			}
			canonical := resolve(tc.hosts[0])
			plan, err := torrent.Select(torrent.Metainfo{
				Name: "fixture", TotalLength: 1, PieceLength: 1,
				Files:  []torrent.File{regularFile(0, "fixture", 0, 1)},
				Pieces: []torrent.Piece{piece(0, 0, 1)},
			}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
			if err != nil {
				t.Fatal(err)
			}
			// Import one metadata corruption strike, then reconnect twice to
			// download a complete but invalid file piece under different aliases.
			if err := s.SeedStrikes(map[peer.Endpoint]int{canonical: 1}); err != nil {
				t.Fatal(err)
			}
			for i, host := range tc.hosts[1:] {
				endpoint := resolve(host)
				id := "connection-" + strconv.Itoa(i)
				if err := s.AddPeer(id, endpoint); err != nil {
					t.Fatal(err)
				}
				if err := s.SetAvailability(id, []int{0}); err != nil {
					t.Fatal(err)
				}
				offer, ok, err := s.ReservePiece(id)
				if err != nil || !ok {
					t.Fatalf("reserve = %v, %t, %v", offer, ok, err)
				}
				if err := s.AdmitPiece(offer); err != nil {
					t.Fatal(err)
				}
				requests, err := s.NextRequests(id, 1)
				if err != nil || len(requests) != 1 {
					t.Fatalf("requests = %v, %v", requests, err)
				}
				if _, err := s.AcceptBlock(id, requests[0].Block); err != nil {
					t.Fatal(err)
				}
				if _, err := s.VerifyPiece(0, false); err != nil {
					t.Fatal(err)
				}
				if got := s.StrikeCount(canonical); got != i+2 {
					t.Fatalf("strike count after alias %q = %d, want %d", host, got, i+2)
				}
				if err := s.RemovePeer(id); err != nil {
					t.Fatal(err)
				}
			}
			if pool.Len() != 1 {
				t.Fatalf("aliases created %d candidate identities", pool.Len())
			}
			for _, host := range tc.hosts {
				endpoint := resolve(host)
				if !s.IsBlacklisted(endpoint) || s.StrikeCount(endpoint) != 3 {
					t.Fatalf("alias %q lost its run-long corruption penalty", host)
				}
				if err := s.AddPeer("blocked", endpoint); !errors.Is(err, ErrInvalidSchedulerPeer) {
					t.Fatalf("blacklisted alias admission = %v", err)
				}
			}
		})
	}
}
