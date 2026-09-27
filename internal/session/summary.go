package session

import (
	"context"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

// RunSummary is a final snapshot, including on failure. Payload includes late,
// duplicate, and corrupt file bytes; verified bytes include reused output.
// TransferElapsed excludes metadata, resume, and final tracker announcements.
// UsefulConnections counts transfer connections that staged an accepted block,
// not unique endpoints or necessarily verified contributions.
type RunSummary struct {
	Elapsed, TransferElapsed time.Duration
	VerifiedSelectedBytes    int64
	SelectedBytes            int64
	ReceivedPayloadBytes     int64
	UsefulConnections        uint64
	SkippedTrackers          torrent.TrackerSkips
	Races                    uint64
	TCP, UTP                 TransportTotals
}

// TransportTotals count attempts, not endpoints. Succeeded includes valid
// handshakes that lost the race or were rejected by live-peer admission.
type TransportTotals struct {
	Wins, Succeeded, Failed, Canceled uint64
}

func (t *TransportTotals) record(attempt peer.AttemptObservation, winner bool) {
	if winner {
		t.Wins = saturatingAdd(t.Wins, 1)
	}
	switch attempt.Outcome {
	case peer.AttemptSucceeded:
		t.Succeeded = saturatingAdd(t.Succeeded, 1)
	case peer.AttemptFailed:
		t.Failed = saturatingAdd(t.Failed, 1)
	case peer.AttemptCanceled:
		t.Canceled = saturatingAdd(t.Canceled, 1)
	}
}

func (c *coordinator) runSummary() RunSummary {
	// All producers are joined before the final snapshot. Race totals are
	// recorded before lossy CLI reporting, independently of callback presence.
	result := c.summary
	if c.account != nil {
		account, _ := c.account.Snapshot(context.Background(), false)
		result.ReceivedPayloadBytes = account.Downloaded
	}
	return result
}
