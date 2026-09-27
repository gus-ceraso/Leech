package cli

import (
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
)

func reportRunSummary(reporter *Reporter, summary session.RunSummary, download bool) {
	if download {
		var rate uint64
		if summary.TransferElapsed > 0 && summary.ReceivedPayloadBytes > 0 {
			// Float division avoids overflowing the byte counter when converting
			// nanoseconds. Clamp before conversion for extremely short fixtures.
			value := float64(summary.ReceivedPayloadBytes) / summary.TransferElapsed.Seconds()
			if value >= float64(^uint64(0)) {
				rate = ^uint64(0)
			} else {
				rate = uint64(value)
			}
		}
		_ = reporter.Info("summary: verified=%d/%d bytes elapsed=%s transfer=%s payload=%d bytes avg-payload=%d B/s useful-connections=%d skipped-trackers=%d(invalid-url=%d unsupported-scheme=%d)",
			summary.VerifiedSelectedBytes, summary.SelectedBytes, summary.Elapsed.Round(time.Millisecond), summary.TransferElapsed.Round(time.Millisecond),
			summary.ReceivedPayloadBytes, rate, summary.UsefulConnections, summary.SkippedTrackers.Total(), summary.SkippedTrackers.InvalidURL, summary.SkippedTrackers.UnsupportedScheme)
	}
	_ = reporter.Debug("transport totals: races=%d tcp-wins=%d tcp-handshakes=%d tcp-failed=%d tcp-canceled=%d utp-wins=%d utp-handshakes=%d utp-failed=%d utp-canceled=%d",
		summary.Races, summary.TCP.Wins, summary.TCP.Succeeded, summary.TCP.Failed, summary.TCP.Canceled,
		summary.UTP.Wins, summary.UTP.Succeeded, summary.UTP.Failed, summary.UTP.Canceled)
}
