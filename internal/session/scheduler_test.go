package session

import (
	"errors"
	"net/netip"
	"reflect"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func schedulerPlan(t *testing.T, files []torrent.File, pieces []torrent.Piece, patterns []string) *torrent.SelectionPlan {
	t.Helper()
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	plan, err := torrent.Select(torrent.Metainfo{TotalLength: total, Files: files, Pieces: pieces}, patterns, nil)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func regularFile(index int, path string, begin, end int64) torrent.File {
	return torrent.File{Index: index, Path: path, Range: torrent.ByteRange{Begin: begin, End: end}, Kind: torrent.RegularFile}
}

func piece(index int, begin, end int64) torrent.Piece {
	return torrent.Piece{Index: index, Range: torrent.ByteRange{Begin: begin, End: end}}
}

func endpoint(last byte) peer.Endpoint {
	return peer.Endpoint{Addr: netip.AddrFrom4([4]byte{192, 0, 2, last}), Port: 6881}
}

func keepTieOrder(values []int) error { return nil }

func TestSchedulerRarestFirstAndPaddingCoverage(t *testing.T) {
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "a", 0, 32768), regularFile(1, "b", 32768, 65536)},
		[]torrent.Piece{piece(0, 0, 32768), piece(1, 32768, 65536)}, nil)
	s, err := NewScheduler(plan, Config{MaxPerPeer: 4, MaxGlobal: 4, MaxQueue: 4, MaxStagedPieces: 2, MaxStagedBytes: 65536, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("one", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("two", endpoint(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("one", []int{0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("two", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("one")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v, want piece", offer, err)
	}
	if offer.PieceIndex != 1 {
		t.Fatalf("reserved piece = %d, want rarest piece 1", offer.PieceIndex)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := s.NextRequests("one", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || requests[0].Block.Begin != 0 || requests[1].Block.Begin != 16384 {
		t.Fatalf("requests = %#v, want both 16KiB blocks", requests)
	}
	for _, request := range requests {
		if request.Block.Length != 16384 || request.Block.Index != 1 {
			t.Fatalf("invalid request block %#v", request.Block)
		}
		if _, err := s.AcceptBlock("one", request.Block); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := s.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Blocks) != 2 || snapshot.Blocks[0].Endpoint != endpoint(1) {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	result, err := s.VerifyPiece(1, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedBytes != 32768 || result.Completed || s.IsComplete() || s.Progress().Verified != 32768 {
		t.Fatalf("completion result = %#v, progress = %#v", result, s.Progress())
	}

	paddingPlan := schedulerPlan(t,
		[]torrent.File{
			regularFile(0, "selected", 0, 10000),
			{Index: 1, Range: torrent.ByteRange{Begin: 10000, End: 20000}, Kind: torrent.PaddingFile},
			regularFile(2, "tail", 20000, 40000),
		},
		[]torrent.Piece{piece(0, 0, 40000)}, []string{"selected"})
	ps, err := NewScheduler(paddingPlan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.AddPeer("p", endpoint(3)); err != nil {
		t.Fatal(err)
	}
	if err := ps.SetAvailability("p", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err = ps.ReservePiece("p")
	if err != nil || !ok {
		t.Fatalf("padding reserve = %#v, %v", offer, err)
	}
	if err := ps.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err = ps.NextRequests("p", 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []peer.Block{{Index: 0, Begin: 0, Length: 10000}, {Index: 0, Begin: 20000, Length: 16384}, {Index: 0, Begin: 36384, Length: 3616}}
	got := make([]peer.Block, len(requests))
	for i, request := range requests {
		got[i] = request.Block
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("padding requests = %#v, want %#v", got, want)
	}
}

func TestSchedulerIgnoresUnwantedAndOutOfRangeAvailability(t *testing.T) {
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "skip", 0, 32768), regularFile(1, "wanted", 32768, 65536)},
		[]torrent.Piece{piece(0, 0, 32768), piece(1, 32768, 65536)}, []string{"wanted"})
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("p", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("p", []int{-1, 0, 1, 999999999}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("p")
	if err != nil || !ok || offer.PieceIndex != 1 {
		t.Fatalf("offer = %#v, ok=%v, err=%v", offer, ok, err)
	}
}

func TestSchedulerSeedsMetadataStrikesBeforeAdmission(t *testing.T) {
	plan := schedulerPlan(t, []torrent.File{regularFile(0, "a", 0, 32768)}, []torrent.Piece{piece(0, 0, 32768)}, nil)
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SeedStrikes(map[peer.Endpoint]int{endpoint(1): 2, endpoint(2): 3}); err != nil {
		t.Fatal(err)
	}
	if s.StrikeCount(endpoint(1)) != 2 || !s.IsBlacklisted(endpoint(2)) {
		t.Fatalf("seeded state = %d/%v", s.StrikeCount(endpoint(1)), s.IsBlacklisted(endpoint(2)))
	}
	if err := s.AddPeer("blocked", endpoint(2)); !errors.Is(err, ErrInvalidSchedulerPeer) {
		t.Fatalf("blacklisted endpoint admission = %v", err)
	}
	if err := s.SeedStrikes(map[peer.Endpoint]int{endpoint(3): 4}); !errors.Is(err, ErrSchedulerConfig) {
		t.Fatalf("invalid seed = %v", err)
	}
}

func TestSchedulerReassignsAndEnforcesRequestCaps(t *testing.T) {
	plan := schedulerPlan(t, []torrent.File{regularFile(0, "a", 0, 32768)}, []torrent.Piece{piece(0, 0, 32768)}, nil)
	s, err := NewScheduler(plan, Config{MaxPerPeer: 1, MaxGlobal: 1, MaxQueue: 1, MaxStagedPieces: 1, MaxStagedBytes: 32768, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("one", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("two", endpoint(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("one", []int{0}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("two", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("one")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	requests, err := s.NextRequests("one", 4)
	if err != nil || len(requests) != 1 {
		t.Fatalf("one requests = %#v, %v", requests, err)
	}
	assigned := requests[0].Block
	if got, err := s.NextRequests("two", 4); err != nil || len(got) != 0 {
		t.Fatalf("global cap requests = %#v, %v", got, err)
	}
	if err := s.RemovePeer("one"); err != nil {
		t.Fatal(err)
	}
	requests, err = s.NextRequests("two", 4)
	if err != nil || len(requests) != 1 || requests[0].Block != assigned {
		t.Fatalf("reassigned requests = %#v, %v", requests, err)
	}
	if !s.SevereViolation(endpoint(2)) {
		t.Fatal("severe violation did not blacklist endpoint")
	}
	if s.ActiveRequests() != 0 {
		t.Fatalf("active requests = %d, want zero", s.ActiveRequests())
	}
	if got, err := s.NextRequests("two", 1); err != nil || len(got) != 0 {
		t.Fatalf("blacklisted peer requests = %#v, %v", got, err)
	}
}

func TestSchedulerStreamingPrioritizesEarlierAdmittedPiece(t *testing.T) {
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "a", 0, 32768), regularFile(1, "b", 32768, 65536)},
		[]torrent.Piece{piece(0, 0, 32768), piece(1, 32768, 65536)}, nil)
	s, err := NewScheduler(plan, Config{Streaming: true, MaxStagedPieces: 2, MaxStagedBytes: 65536, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		if err := s.AddPeer(id, endpoint(byte(len(id)))); err != nil {
			t.Fatal(err)
		}
		if err := s.SetAvailability(id, []int{0, 1}); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := s.ReservePiece("one")
	if err != nil || !ok || first.PieceIndex != 0 {
		t.Fatalf("first offer = %#v, %v, %v", first, ok, err)
	}
	if err := s.AdmitPiece(first); err != nil {
		t.Fatal(err)
	}
	second, ok, err := s.ReservePiece("two")
	if err != nil || !ok || second.PieceIndex != 1 {
		t.Fatalf("second offer = %#v, %v, %v", second, ok, err)
	}
	if err := s.AdmitPiece(second); err != nil {
		t.Fatal(err)
	}
	requests, err := s.NextRequests("two", 1)
	if err != nil || len(requests) != 1 || requests[0].Block.Index != 0 {
		t.Fatalf("stream request = %#v, %v; want piece 0", requests, err)
	}

	// A useful connection that cannot serve the current sequential piece may
	// continue with a later available piece rather than idling.
	if err := s.RejectBlock("two", requests[0].Block); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("two", []int{1}); err != nil {
		t.Fatal(err)
	}
	requests, err = s.NextRequests("two", 1)
	if err != nil || len(requests) != 1 || requests[0].Block.Index != 1 {
		t.Fatalf("fallback request = %#v, %v; want piece 1", requests, err)
	}
}

func TestSchedulerEndgameWinnerCancelsDuplicateAssignments(t *testing.T) {
	plan := schedulerPlan(t, []torrent.File{regularFile(0, "a", 0, 32768)}, []torrent.Piece{piece(0, 0, 32768)}, nil)
	s, err := NewScheduler(plan, Config{MaxPerPeer: 4, MaxGlobal: 4, MaxQueue: 4, MaxStagedPieces: 1, MaxStagedBytes: 32768, Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id string
		ep peer.Endpoint
	}{{"one", endpoint(1)}, {"two", endpoint(2)}} {
		if err := s.AddPeer(item.id, item.ep); err != nil {
			t.Fatal(err)
		}
		if err := s.SetAvailability(item.id, []int{0}); err != nil {
			t.Fatal(err)
		}
	}
	offer, ok, err := s.ReservePiece("one")
	if err != nil || !ok {
		t.Fatalf("offer = %#v, %v", offer, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	primary, err := s.NextRequests("one", 2)
	if err != nil || len(primary) != 2 || !s.Endgame() {
		t.Fatalf("primary = %#v, endgame=%v, err=%v", primary, s.Endgame(), err)
	}
	duplicates, err := s.NextRequests("two", 2)
	if err != nil || len(duplicates) != 2 {
		t.Fatalf("duplicates = %#v, err=%v", duplicates, err)
	}
	for _, request := range primary {
		result, err := s.AcceptBlock("one", request.Block)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Canceled) != 1 || result.Canceled[0].Peer != "two" || result.Canceled[0].Block != request.Block {
			t.Fatalf("winner result = %#v for %#v", result, request)
		}
	}
	if s.ActiveRequests() != 0 {
		t.Fatalf("active requests = %d, want 0", s.ActiveRequests())
	}
	if s.Endgame() {
		t.Fatal("endgame should end after all blocks have a winner")
	}
}

func TestSchedulerMixedContributorsStrikeByEndpoint(t *testing.T) {
	plan := schedulerPlan(t, []torrent.File{regularFile(0, "a", 0, 32768)}, []torrent.Piece{piece(0, 0, 32768)}, nil)
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	for id, ep := range map[string]peer.Endpoint{"one": endpoint(1), "two": endpoint(2)} {
		if err := s.AddPeer(id, ep); err != nil {
			t.Fatal(err)
		}
		if err := s.SetAvailability(id, []int{0}); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 1; attempt <= 3; attempt++ {
		offer, ok, err := s.ReservePiece("one")
		if err != nil || !ok {
			t.Fatalf("attempt %d reserve = %#v, %v", attempt, offer, err)
		}
		if err := s.AdmitPiece(offer); err != nil {
			t.Fatal(err)
		}
		first, err := s.NextRequests("one", 1)
		if err != nil || len(first) != 1 {
			t.Fatalf("attempt %d first = %#v, %v", attempt, first, err)
		}
		second, err := s.NextRequests("two", 1)
		if err != nil || len(second) != 1 {
			t.Fatalf("attempt %d second = %#v, %v", attempt, second, err)
		}
		if _, err := s.AcceptBlock("one", first[0].Block); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcceptBlock("two", second[0].Block); err != nil {
			t.Fatal(err)
		}
		result, err := s.VerifyPiece(0, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Contributors) != 2 || len(result.Strikes) != 2 {
			t.Fatalf("attempt %d result = %#v", attempt, result)
		}
		if attempt < 3 && (s.IsBlacklisted(endpoint(1)) || s.IsBlacklisted(endpoint(2))) {
			t.Fatalf("endpoint blacklisted too early on attempt %d", attempt)
		}
	}
	if !s.IsBlacklisted(endpoint(1)) || !s.IsBlacklisted(endpoint(2)) || s.StrikeCount(endpoint(1)) != 3 {
		t.Fatalf("blacklist/strikes = %v/%v and %d", s.IsBlacklisted(endpoint(1)), s.IsBlacklisted(endpoint(2)), s.StrikeCount(endpoint(1)))
	}
	if s.SevereViolation(endpoint(1)) {
		t.Fatal("severe violation should be idempotent after blacklist")
	}
	if err := s.AddPeer("new", endpoint(1)); !errors.Is(err, ErrInvalidSchedulerPeer) {
		t.Fatalf("blacklisted add error = %v", err)
	}
}

func FuzzSchedulerEvents(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})
	f.Add([]byte{255, 0, 255, 1})
	plan := schedulerPlanForFuzz()
	f.Fuzz(func(t *testing.T, events []byte) {
		s, err := NewScheduler(plan, Config{MaxPerPeer: 2, MaxGlobal: 4, MaxQueue: 4, MaxStagedPieces: 1, MaxStagedBytes: 32768, Shuffle: keepTieOrder})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AddPeer("p", endpoint(1)); err != nil {
			t.Fatal(err)
		}
		if err := s.SetAvailability("p", []int{0}); err != nil {
			t.Fatal(err)
		}
		offer, ok, err := s.ReservePiece("p")
		if err != nil || !ok {
			t.Fatal(err)
		}
		if err := s.AdmitPiece(offer); err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			requests, _ := s.NextRequests("p", int(event%4))
			if len(requests) > 0 && event&1 == 0 {
				_, _ = s.AcceptBlock("p", requests[0].Block)
			} else if len(requests) > 0 {
				_ = s.RejectBlock("p", requests[0].Block)
			}
			if s.active < 0 || s.active > 4 || s.staged < 0 || s.staged > 1 || s.stagedBytes < 0 || s.stagedBytes > 32768 {
				t.Fatalf("state out of bounds: %#v", s)
			}
		}
	})
}

func FuzzSchedulerEndgameEvents(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{255, 0, 17, 8})
	plan := schedulerPlanForFuzz()
	f.Fuzz(func(t *testing.T, events []byte) {
		s, err := NewScheduler(plan, Config{MaxPerPeer: 2, MaxGlobal: 4, MaxQueue: 4, MaxStagedPieces: 1, MaxStagedBytes: 32768, Shuffle: keepTieOrder})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			id string
			ep peer.Endpoint
		}{{"one", endpoint(1)}, {"two", endpoint(2)}} {
			if err := s.AddPeer(item.id, item.ep); err != nil {
				t.Fatal(err)
			}
			if err := s.SetAvailability(item.id, []int{0}); err != nil {
				t.Fatal(err)
			}
		}
		offer, ok, err := s.ReservePiece("one")
		if err != nil || !ok {
			t.Fatal(err)
		}
		if err := s.AdmitPiece(offer); err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			one, _ := s.NextRequests("one", int(event%3))
			two, _ := s.NextRequests("two", int(event>>2)%3)
			if event&1 == 0 && len(one) > 0 {
				_, _ = s.AcceptBlock("one", one[0].Block)
			} else if len(one) > 0 {
				_ = s.RejectBlock("one", one[0].Block)
			}
			if event&2 == 0 && len(two) > 0 {
				_, _ = s.AcceptBlock("two", two[0].Block)
			} else if len(two) > 0 {
				_ = s.RejectBlock("two", two[0].Block)
			}
			if event&4 != 0 {
				_ = s.SetPieceAvailability("one", 0, event&8 != 0)
			}
			if s.active < 0 || s.active > 4 || s.pieces[0].blocks[0].active && len(s.pieces[0].blocks[0].assignments) == 0 {
				t.Fatalf("invalid endgame state: %#v", s)
			}
		}
	})
}

func schedulerPlanForFuzz() *torrent.SelectionPlan {
	plan, err := torrent.Select(torrent.Metainfo{TotalLength: 32768, Files: []torrent.File{regularFile(0, "a", 0, 32768)}, Pieces: []torrent.Piece{piece(0, 0, 32768)}}, nil, nil)
	if err != nil {
		panic(err)
	}
	return plan
}

func TestSchedulerSkipsPeerTombstonesWithoutStarvingOtherBlocks(t *testing.T) {
	length := int64(2 * limits.BlockBytes)
	plan := schedulerPlan(t,
		[]torrent.File{regularFile(0, "a", 0, length)},
		[]torrent.Piece{piece(0, 0, length)}, nil)
	s, err := NewScheduler(plan, Config{Shuffle: keepTieOrder})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddPeer("peer", endpoint(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAvailability("peer", []int{0}); err != nil {
		t.Fatal(err)
	}
	offer, ok, err := s.ReservePiece("peer")
	if err != nil || !ok {
		t.Fatalf("reserve = %#v, %v", offer, err)
	}
	if err := s.AdmitPiece(offer); err != nil {
		t.Fatal(err)
	}
	excluded := map[peer.Block]struct{}{{Index: 0, Begin: 0, Length: limits.BlockBytes}: {}}
	requests, err := s.nextRequestsExcluding("peer", 2, excluded)
	if err != nil || len(requests) != 1 {
		t.Fatalf("requests excluding tombstone = %#v, %v", requests, err)
	}
	if got := requests[0].Block; got != (peer.Block{Index: 0, Begin: limits.BlockBytes, Length: limits.BlockBytes}) {
		t.Fatalf("request = %#v, want second block", got)
	}
}
