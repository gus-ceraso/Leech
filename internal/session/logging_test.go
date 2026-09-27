package session

import (
	"context"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/peer"
)

func TestAssignmentDiagnosticsCoalesceStablePostDriveReasons(t *testing.T) {
	for _, test := range []struct {
		name      string
		cfg       Config
		available int
		want      string
	}{
		{"piece limit", Config{MaxStagedPieces: 1, MaxStagedBytes: 2}, 1, "staged piece limit"},
		{"byte limit", Config{MaxStagedPieces: 2, MaxStagedBytes: 1}, 1, "staged byte limit"},
		{"in flight", Config{MaxStagedPieces: 1, MaxStagedBytes: 2}, 0, "no assignable blocks"},
		{"global limit", Config{MaxStagedPieces: 2, MaxStagedBytes: 2, MaxGlobal: 1}, 1, "global request limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, meta := reviewPiecePlan(t, 2, 1)
			transfer, p, _ := reviewTransfer(t, plan, meta, test.cfg)
			if err := transfer.scheduler.AddPeer("holder", endpoint(63)); err != nil {
				t.Fatal(err)
			}
			if err := transfer.scheduler.SetAvailability("holder", []int{0}); err != nil {
				t.Fatal(err)
			}
			reviewAdmitStage(t, transfer, "holder")
			if requests, err := transfer.scheduler.NextRequests("holder", 1); err != nil || len(requests) != 1 {
				t.Fatalf("holder requests = %v, %v", requests, err)
			}
			reviewAdvertise(t, transfer, p, test.available)
			var reasons []string
			transfer.onDiagnostic = func(event Diagnostic) {
				if strings.HasPrefix(event.Detail, "assignment blocked:") {
					reasons = append(reasons, event.Detail)
				}
			}
			live := []*transferPeer{p}
			drive := func() {
				t.Helper()
				for range 100 {
					if err := transfer.drive(context.Background(), &live); err != nil {
						t.Fatal(err)
					}
				}
			}
			drive()
			if len(reasons) != 1 || reasons[0] != "assignment blocked: "+test.want {
				t.Fatalf("stable reason repeated or mislabeled: %v", reasons)
			}
			for _, id := range []byte{peer.ChokeID, peer.UnchokeID} {
				if err := transfer.handleEvent(context.Background(), p, peer.PeerEvent{Message: peer.Message{ID: id}}); err != nil {
					t.Fatal(err)
				}
				drive()
			}
			if len(reasons) != 3 || reasons[1] != "assignment blocked: remote choking" || reasons[2] != reasons[0] {
				t.Fatalf("real transitions lost: %v", reasons)
			}
		})
	}
}
