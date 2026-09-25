package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func FuzzSchedulerStrikeAccounting(f *testing.F) {
	f.Add([]byte{3, 0, 0, 0})
	f.Add([]byte{1, 1, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 16 {
			input = input[:16]
		}
		attempts := 1
		if len(input) != 0 {
			attempts += int(input[0] % 3)
		}
		shared := len(input) > 1 && input[1]&1 != 0
		plan := schedulerPlanForFuzz()
		scheduler, err := NewScheduler(plan, Config{MaxPerPeer: 2, MaxGlobal: 4, MaxQueue: 4, MaxStagedPieces: 1, MaxStagedBytes: 32768, Shuffle: keepTieOrder})
		if err != nil {
			t.Fatal(err)
		}
		secondEndpoint := endpoint(2)
		if shared {
			secondEndpoint = endpoint(1)
		}
		for _, candidate := range []struct {
			id       string
			endpoint peer.Endpoint
		}{{"first", endpoint(1)}, {"second", secondEndpoint}} {
			if err := scheduler.AddPeer(candidate.id, candidate.endpoint); err != nil {
				t.Fatal(err)
			}
			if err := scheduler.SetAvailability(candidate.id, []int{0}); err != nil {
				t.Fatal(err)
			}
		}
		for round := 0; round < attempts; round++ {
			offer, ok, err := scheduler.ReservePiece("first")
			if err != nil || !ok {
				t.Fatalf("round %d reserve = %#v, %v", round, offer, err)
			}
			if err := scheduler.AdmitPiece(offer); err != nil {
				t.Fatal(err)
			}
			first, err := scheduler.NextRequests("first", 1)
			if err != nil || len(first) != 1 {
				t.Fatalf("round %d first requests = %#v, %v", round, first, err)
			}
			second, err := scheduler.NextRequests("second", 1)
			if err != nil || len(second) != 1 {
				t.Fatalf("round %d second requests = %#v, %v", round, second, err)
			}
			if _, err := scheduler.AcceptBlock("first", first[0].Block); err != nil {
				t.Fatal(err)
			}
			if _, err := scheduler.AcceptBlock("second", second[0].Block); err != nil {
				t.Fatal(err)
			}
			verification, err := scheduler.VerifyPiece(0, false)
			if err != nil {
				t.Fatal(err)
			}
			wantContributors := 2
			if shared {
				wantContributors = 1
			}
			if len(verification.Contributors) != wantContributors || len(verification.Strikes) != wantContributors {
				t.Fatalf("round %d contributors/strikes = %v/%v, want %d", round, verification.Contributors, verification.Strikes, wantContributors)
			}
		}
		if got := scheduler.StrikeCount(endpoint(1)); got != attempts {
			t.Fatalf("first endpoint strikes=%d, want %d", got, attempts)
		}
		if shared {
			if got := scheduler.StrikeCount(endpoint(2)); got != 0 {
				t.Fatalf("unused endpoint strikes=%d", got)
			}
		} else if got := scheduler.StrikeCount(endpoint(2)); got != attempts {
			t.Fatalf("second endpoint strikes=%d, want %d", got, attempts)
		}
		if scheduler.IsBlacklisted(endpoint(1)) != (attempts == 3) {
			t.Fatalf("first endpoint blacklist=%v after %d strikes", scheduler.IsBlacklisted(endpoint(1)), attempts)
		}
	})
}

type shutdownHTTPFixture struct {
	transmitted bool
	started     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	requests    []tracker.AnnounceRequest
}

func (f *shutdownHTTPFixture) Announce(ctx context.Context, _ string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	if request.Event == tracker.EventStarted {
		f.once.Do(func() { close(f.started) })
		<-ctx.Done()
		return tracker.HTTPAnnounceResult{Transmitted: f.transmitted}, ctx.Err()
	}
	return tracker.HTTPAnnounceResult{Transmitted: true, Interval: time.Second}, nil
}

func (f *shutdownHTTPFixture) snapshot() []tracker.AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tracker.AnnounceRequest(nil), f.requests...)
}

func FuzzRunMetadataCancellationShutdown(f *testing.F) {
	f.Add(byte(0))
	f.Add(byte(1))
	f.Fuzz(func(t *testing.T, mode byte) {
		fixture := &shutdownHTTPFixture{transmitted: mode&1 == 0, started: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		raw := "magnet:?xt=urn:btih:0000000000000000000000000000000000000000"
		go func() {
			_, err := RunSource(ctx, raw, RunConfig{HTTP: fixture, UTPHeadStart: time.Millisecond})
			done <- err
		}()
		select {
		case <-fixture.started:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("metadata tracker did not enter its started request")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunSource error = %v, want context cancellation", err)
			}
		case <-time.After(time.Second):
			t.Fatal("session shutdown did not join tracker workers")
		}
		requests := fixture.snapshot()
		started, stopped := 0, 0
		for _, request := range requests {
			if request.Uploaded != 0 {
				t.Fatalf("shutdown announce uploaded=%d", request.Uploaded)
			}
			if request.Event == tracker.EventStarted {
				started++
			}
			if request.Event == tracker.EventStopped {
				stopped++
			}
		}
		if started != 1 {
			t.Fatalf("started announces=%d, want 1", started)
		}
		wantStopped := 0
		if fixture.transmitted {
			wantStopped = 1
		}
		if stopped != wantStopped {
			t.Fatalf("stopped announces=%d, want %d for transmitted=%v", stopped, wantStopped, fixture.transmitted)
		}
	})
}
