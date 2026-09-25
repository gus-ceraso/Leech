package tracker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type loopHTTP struct {
	mu        sync.Mutex
	requests  []AnnounceRequest
	responses []loopHTTPResponse
}

type loopHTTPResponse struct {
	result HTTPAnnounceResult
	err    error
}

type gatedStartedHTTP struct {
	gate <-chan struct{}
	base *loopHTTP
}

func (f gatedStartedHTTP) Announce(ctx context.Context, tracker string, request AnnounceRequest) (HTTPAnnounceResult, error) {
	if request.Event == EventStarted {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return HTTPAnnounceResult{}, ctx.Err()
		}
	}
	return f.base.Announce(ctx, tracker, request)
}

type loopUDP struct {
	mu       sync.Mutex
	requests []AnnounceRequest
	result   AnnounceResult
	err      error
}

func (f *loopUDP) Announce(_ context.Context, _ string, request AnnounceRequest) (AnnounceResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	result, err := f.result, f.err
	f.mu.Unlock()
	return result, err
}

func (f *loopUDP) snapshot() []AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AnnounceRequest(nil), f.requests...)
}

func (f *loopHTTP) Announce(_ context.Context, _ string, request AnnounceRequest) (HTTPAnnounceResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	if len(f.responses) == 0 {
		return HTTPAnnounceResult{Interval: time.Second, Transmitted: true}, nil
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	return response.result, response.err
}

func (f *loopHTTP) snapshot() []AnnounceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AnnounceRequest(nil), f.requests...)
}

func TestTrackerPhaseFinalizationOrderingAndIndependentTrackers(t *testing.T) {
	fake := &loopHTTP{}
	clock := newFixtureClock()
	updates := make(chan Update, 32)
	set, err := NewTrackerSet(TrackerSetConfig{
		InfoHash: [20]byte{3},
		Trackers: []string{"http://one.test/announce", "http://one.test/announce", "https://two.test/announce"},
		Identity: Identity{PeerID: [20]byte{4}, Key: 5, Port: 49152},
		HTTP:     fake,
		Clock:    clock,
		Random:   bytesReader(make([]byte, 64)),
		Snapshot: func(context.Context) (Snapshot, error) { return Snapshot{Downloaded: 7, Retained: 2, Total: 10}, nil },
		OnUpdate: func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string][]Event)
	for len(seen) < 2 || len(seen["http://one.test/announce"]) < 1 || len(seen["https://two.test/announce"]) < 1 {
		select {
		case update := <-updates:
			seen[update.Tracker] = append(seen[update.Tracker], update.Request.Event)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for independent started announces")
		}
	}
	if err := run.Finalize(context.Background(), true); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	for len(seen["http://one.test/announce"]) < 3 || len(seen["https://two.test/announce"]) < 3 {
		select {
		case update := <-updates:
			seen[update.Tracker] = append(seen[update.Tracker], update.Request.Event)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for final events")
		}
	}
	for _, tracker := range []string{"http://one.test/announce", "https://two.test/announce"} {
		got := seen[tracker]
		want := []Event{EventStarted, EventCompleted, EventStopped}
		for i, event := range want {
			if got[i] != event {
				t.Fatalf("%s events = %v, want %v", tracker, got, want)
			}
		}
	}
	requests := fake.snapshot()
	if len(requests) != 6 {
		t.Fatalf("normal announce continued after finalization: %d requests", len(requests))
	}
	for _, request := range requests {
		if request.Uploaded != 0 || request.Left != 8 || request.Port != 49152 {
			t.Fatalf("request accounting/identity = %+v", request)
		}
	}
}

func TestTrackerSetCountsUniqueURLsForBound(t *testing.T) {
	trackers := make([]string, 0, 70)
	for i := 0; i < 65; i++ {
		trackers = append(trackers, "http://repeat.test/announce")
	}
	set, err := NewTrackerSet(TrackerSetConfig{Trackers: trackers, Identity: Identity{Port: 49152}})
	if err != nil {
		t.Fatal(err)
	}
	if got := set.Trackers(); len(got) != 1 || got[0] != trackers[0] {
		t.Fatalf("deduplicated trackers = %v", got)
	}
}

func TestHTTPDepletionCanRerequestBeforeInterval(t *testing.T) {
	fake := &loopHTTP{responses: []loopHTTPResponse{{result: HTTPAnnounceResult{Interval: time.Minute, Transmitted: true}}}}
	clock := newFixtureClock()
	updates := make(chan Update, 4)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers:  []string{"http://early.test/announce"},
		Identity:  Identity{Port: 49152},
		HTTP:      fake,
		Clock:     clock,
		NeedPeers: func() bool { return true },
		OnUpdate:  func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Finalize(context.Background(), false)
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial HTTP announce")
	}
	timer := clock.nextActive(t)
	clock.advance(trackerEarlyRerequest)
	clock.fire(timer)
	select {
	case update := <-updates:
		if update.Request.Event != EventNone {
			t.Fatalf("early rerequest event = %v", update.Request.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP early rerequest")
	}
	if got := len(fake.snapshot()); got != 2 {
		t.Fatalf("HTTP announces = %d, want 2", got)
	}
}

func TestUDPWaitsForIntervalEvenWhenPeersAreNeeded(t *testing.T) {
	fake := &loopUDP{result: AnnounceResult{Interval: time.Minute, Transmitted: true}}
	clock := newFixtureClock()
	updates := make(chan Update, 4)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers:  []string{"udp://interval.test:6969/announce"},
		Identity:  Identity{Port: 49152},
		UDP:       fake,
		Clock:     clock,
		NeedPeers: func() bool { return true },
		OnUpdate:  func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Finalize(context.Background(), false)
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial UDP announce")
	}
	timer := clock.nextActive(t)
	clock.advance(59 * time.Second)
	select {
	case update := <-updates:
		t.Fatalf("UDP announced before interval: %+v", update)
	case <-time.After(20 * time.Millisecond):
	}
	clock.advance(time.Second)
	clock.fire(timer)
	select {
	case update := <-updates:
		if update.Request.Event != EventNone {
			t.Fatalf("UDP regular event = %v", update.Request.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for UDP interval announce")
	}
	if got := len(fake.snapshot()); got != 2 {
		t.Fatalf("UDP announces = %d, want 2", got)
	}
}

func TestBEP31RetryDelayIsNotShortened(t *testing.T) {
	fake := &loopHTTP{responses: []loopHTTPResponse{{
		result: HTTPAnnounceResult{Transmitted: true},
		err:    &HTTPError{Class: HTTPFailureTransient, Code: HTTPErrorTracker, RetryAfter: 5 * time.Minute},
	}}}
	clock := newFixtureClock()
	updates := make(chan Update, 4)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://retry.test/announce"},
		Identity: Identity{Port: 49152}, HTTP: fake, Clock: clock,
		OnUpdate: func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Finalize(context.Background(), false)
	select {
	case update := <-updates:
		if update.Request.Event != EventStarted || !update.Transmitted {
			t.Fatalf("retry update = %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry response")
	}
	clock.advance(4 * time.Minute)
	select {
	case update := <-updates:
		t.Fatalf("retry announced before BEP31 delay: %+v", update)
	case <-time.After(20 * time.Millisecond):
	}
	timer := clock.nextActive(t)
	clock.advance(time.Minute)
	clock.fire(timer)
	select {
	case update := <-updates:
		if !update.Activated {
			t.Fatalf("retry did not activate: %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed retry")
	}
}

func FuzzTrackerAccountingPhase(f *testing.F) {
	f.Add(int64(0), int64(0), int64(0), true)
	f.Add(int64(12), int64(4), int64(10), false)
	f.Fuzz(func(t *testing.T, downloaded, retained, total int64, metadata bool) {
		snapshot := Snapshot{Downloaded: downloaded, Retained: retained, Total: total, Metadata: metadata}
		request, err := snapshot.Announce(Identity{Port: 49152}, [20]byte{1}, EventNone, -1)
		if err != nil {
			return
		}
		if request.Uploaded != 0 || request.Downloaded < 0 || request.Left < 0 {
			t.Fatalf("invalid request from valid snapshot: %+v", request)
		}
		if metadata && request.Left != 1 {
			t.Fatalf("metadata left = %d", request.Left)
		}
		if !metadata && request.Left != total-retained {
			t.Fatalf("transfer left = %d, want %d", request.Left, total-retained)
		}
	})
}

func FuzzTrackerFinalEventOrdering(f *testing.F) {
	f.Add(uint8(0), false, true)
	f.Add(uint8(1), true, true)
	f.Add(uint8(1), true, false)
	f.Fuzz(func(t *testing.T, phaseByte uint8, fullCompletion, startedTransmitted bool) {
		phase := Phase(phaseByte % 2)
		// Finalize is idempotent, so a duplicate call must describe the same
		// terminal sequence rather than append another event.
		first := []Event(nil)
		if startedTransmitted {
			first = finalEventSequence(phase, fullCompletion)
		}
		second := append([]Event(nil), first...)
		if len(first) != len(second) {
			t.Fatalf("duplicate finalization changed sequence: %v, %v", first, second)
		}
		for i, event := range first {
			if event != EventCompleted && event != EventStopped {
				t.Fatalf("final event %d = %v", i, event)
			}
			if i > 0 && first[i-1] == EventStopped {
				t.Fatalf("normal/final event follows stopped: %v", first)
			}
		}
		if len(first) > 0 && first[len(first)-1] != EventStopped {
			t.Fatalf("final sequence does not end stopped: %v", first)
		}
		if phase == MetadataPhase && containsEvent(first, EventCompleted) {
			t.Fatalf("metadata sequence completed: %v", first)
		}
	})
}

func containsEvent(events []Event, want Event) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

func TestTrackerPermanentFailureDisablesOnlyThatTrackerAcrossPhases(t *testing.T) {
	fake := &loopHTTP{responses: []loopHTTPResponse{{result: HTTPAnnounceResult{Transmitted: true}, err: &HTTPError{Class: HTTPFailureNever, Code: HTTPErrorTracker}}}}
	clock := newFixtureClock()
	updates := make(chan Update, 8)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://disabled.test/announce"},
		Identity: Identity{Port: 49152},
		HTTP:     fake, Clock: clock, Random: bytesReader(make([]byte, 64)),
		OnUpdate: func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := set.Start(context.Background(), MetadataPhase)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-updates:
		var httpErr *HTTPError
		if !update.Transmitted || update.Activated || !errors.As(update.Err, &httpErr) {
			t.Fatalf("failure update = %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for permanent failure")
	}
	if err := first.Finalize(context.Background(), false); err != nil {
		// The transmitted started request is permanently disabled, so no
		// stopped request is eligible and finalization remains successful.
		t.Fatalf("finalize permanent failure: %v", err)
	}
	second, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Finalize(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := len(fake.snapshot()); got != 1 {
		t.Fatalf("disabled tracker was retried in next phase: %d requests", got)
	}
}

func TestTransmittedStartedWithoutActivationStillGetsStopped(t *testing.T) {
	fake := &loopHTTP{responses: []loopHTTPResponse{{
		result: HTTPAnnounceResult{Transmitted: true},
		err:    &HTTPError{Class: HTTPFailureTransient, Code: HTTPErrorTimeout},
	}}}
	updates := make(chan Update, 8)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://lost-response.test/announce"},
		Identity: Identity{Port: 49152},
		HTTP:     fake,
		OnUpdate: func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-updates:
		if update.Request.Event != EventStarted || !update.Transmitted || update.Activated {
			t.Fatalf("started update = %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started transmission")
	}
	if err := run.Finalize(context.Background(), false); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	requests := fake.snapshot()
	if len(requests) != 2 || requests[0].Event != EventStarted || requests[1].Event != EventStopped {
		t.Fatalf("requests = %+v, want started then stopped", requests)
	}
}

func TestStartedTransmissionRecordedBeforeUpdate(t *testing.T) {
	gate := make(chan struct{})
	observed := make(chan bool, 1)
	base := &loopHTTP{}
	var run *PhaseRun
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://tracker.test/announce"},
		Identity: Identity{Port: 49152},
		HTTP:     gatedStartedHTTP{gate: gate, base: base},
		OnUpdate: func(update Update) {
			if update.Request.Event == EventStarted {
				observed <- run.state[0].startedTransmitted
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err = set.Start(context.Background(), MetadataPhase)
	if err != nil {
		t.Fatal(err)
	}
	close(gate)
	select {
	case recorded := <-observed:
		if !recorded {
			t.Fatal("started update reached discovery before transmission was recorded")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started update")
	}
	if err := run.Finalize(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	requests := base.snapshot()
	if len(requests) != 2 || requests[0].Event != EventStarted || requests[1].Event != EventStopped {
		t.Fatalf("tracker events = %+v, want started then stopped", requests)
	}
}

type contextCheckingHTTP struct {
	base    *loopHTTP
	stopped chan finalContextObservation
}

type finalContextObservation struct {
	err      error
	deadline time.Time
	has      bool
}

func (f *contextCheckingHTTP) Announce(ctx context.Context, tracker string, request AnnounceRequest) (HTTPAnnounceResult, error) {
	if request.Event == EventStopped {
		deadline, has := ctx.Deadline()
		f.stopped <- finalContextObservation{err: ctx.Err(), deadline: deadline, has: has}
	}
	return f.base.Announce(ctx, tracker, request)
}

func TestFinalEventsUseIndependentBoundedContext(t *testing.T) {
	phaseCtx, cancelPhase := context.WithCancel(context.Background())
	defer cancelPhase()
	fake := &contextCheckingHTTP{base: &loopHTTP{}, stopped: make(chan finalContextObservation, 1)}
	started := make(chan Update, 1)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://independent-final.test/announce"},
		Identity: Identity{Port: 49152},
		HTTP:     fake,
		OnUpdate: func(update Update) { started <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(phaseCtx, TransferPhase)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-started:
		if !update.Transmitted {
			t.Fatalf("started update = %+v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started announce")
	}
	cancelPhase()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if err := run.Finalize(expired, false); err != nil {
		t.Fatalf("finalize with canceled phase context: %v", err)
	}
	select {
	case observed := <-fake.stopped:
		if observed.err != nil {
			t.Fatalf("stopped used canceled phase context: %v", observed.err)
		}
		if remaining := time.Until(observed.deadline); !observed.has || remaining <= 0 || remaining > finalAnnounceTimeout {
			t.Fatalf("stopped deadline = %v, has=%v, remaining=%v; want bounded live deadline", observed.deadline, observed.has, remaining)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped announce")
	}
}

func TestMetadataPhaseNeverSendsCompleted(t *testing.T) {
	fake := &loopHTTP{}
	updates := make(chan Update, 4)
	set, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"http://metadata.test/announce"},
		Identity: Identity{Port: 49152},
		HTTP:     fake,
		OnUpdate: func(update Update) { updates <- update },
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := set.Start(context.Background(), MetadataPhase)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metadata started announce")
	}
	if err := run.Finalize(context.Background(), true); err != nil {
		t.Fatalf("finalize metadata phase: %v", err)
	}
	requests := fake.snapshot()
	if len(requests) != 2 || requests[0].Event != EventStarted || requests[1].Event != EventStopped {
		t.Fatalf("metadata final events = %+v, want started then stopped", requests)
	}
	for _, request := range requests {
		if request.Left != 1 {
			t.Fatalf("metadata left = %d, want 1", request.Left)
		}
	}
}

func TestTrackerSetCloseOwnsOnlyDefaultUDPClient(t *testing.T) {
	owned, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"udp://owned.test:6969/announce"},
		Identity: Identity{Port: 49152},
	})
	if err != nil {
		t.Fatal(err)
	}
	if owned.ownedUDP == nil {
		t.Fatal("default UDP client was not recorded as owned")
	}
	if err := owned.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !owned.ownedUDP.isClosed() {
		t.Fatal("owned UDP client remained open")
	}
	if _, err := owned.Start(context.Background(), TransferPhase); err == nil {
		t.Fatal("closed tracker set accepted a new phase")
	}

	provided := NewUDPClient(Config{})
	injected, err := NewTrackerSet(TrackerSetConfig{
		Trackers: []string{"udp://provided.test:6969/announce"},
		Identity: Identity{Port: 49152},
		UDP:      provided,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := injected.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if provided.isClosed() {
		t.Fatal("injected UDP client was closed by tracker set")
	}
	_ = provided.Close()
}
