package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	// ErrDownloadUnavailable is retained for callers that used the pre-session
	// boundary. The executable now routes downloads through session.Run.
	ErrDownloadUnavailable = errors.New("download session is not available")
	// ErrRemoteListingUnavailable is retained as a compatibility spelling.
	ErrRemoteListingUnavailable = errors.New("remote metadata listing is not available")
)

// Run executes one command with a background context and no diagnostics.
func Run(opts Options, stdout io.Writer) error {
	return RunContext(context.Background(), opts, stdout, io.Discard)
}

// RunContext executes one command. Standard output remains reserved for file
// listings; all progress and diagnostics go to stderr through Reporter.
func RunContext(ctx context.Context, opts Options, stdout, stderr io.Writer) error {
	return RunWithSession(ctx, opts, stdout, stderr, session.RunConfig{})
}

// RunWithSession executes one command with injected session dependencies. It
// is the deterministic acceptance seam for local trackers, resolvers, and
// transport dialers; the executable uses RunContext with production defaults.
func RunWithSession(ctx context.Context, opts Options, stdout, stderr io.Writer, dependencies session.RunConfig) error {
	return runWithReporter(ctx, opts, stdout, dependencies, NewReporter(opts.LogLevel, stderr))
}

func runWithReporter(ctx context.Context, opts Options, stdout io.Writer, dependencies session.RunConfig, reporter *Reporter) error {
	if stdout == nil {
		return fmt.Errorf("cli: nil output writer")
	}
	source, err := torrent.ParseSource(opts.Source)
	if err != nil {
		reporter.PrimaryFailure(err, false)
		return err
	}
	dependencies.Source = source
	dependencies.SourceRaw = opts.Source
	dependencies.OutputDir = opts.Output
	dependencies.Patterns = append([]string(nil), opts.Files...)
	dependencies.ListFiles = opts.ListFiles
	dependencies.Resume = opts.Resume
	dependencies.Streaming = opts.Stream
	dependencies.Timeout = opts.Timeout
	statusEnabled := reporter.statusEnabled()
	diagnostics := newDiagnosticQueue()
	reports := newReportQueue()
	secondaries := &secondaryFailures{}
	var statusMu sync.Mutex
	var activeStatus Status
	active, pending, statusQueued := false, false, false
	statusAt := time.Time{}
	statusGeneration, statusVersion := uint64(0), uint64(0)
	setStatus := func(snapshot Status, phaseEntry bool) {
		if !statusEnabled {
			return
		}
		snapshot.Phase = SanitizeDiagnostic(snapshot.Phase, 64)
		statusMu.Lock()
		activeStatus = snapshot
		active = true
		pending = true
		statusAt = reporter.currentTime()
		statusVersion++
		if phaseEntry {
			reports.enqueuePhaseStatus(snapshot, statusAt, statusVersion)
		} else if !statusQueued {
			statusQueued = reports.enqueueStatus(statusGeneration, activeStatus, statusAt, statusVersion)
		}
		statusMu.Unlock()
	}
	markStatusPending := func() {
		if !statusEnabled {
			return
		}
		statusMu.Lock()
		if active {
			pending = true
			statusVersion++
		}
		statusMu.Unlock()
	}
	renderStatusSnapshot := func(snapshot Status, version uint64, at time.Time) {
		shown, err := reporter.renderStatusAt(snapshot, at)
		statusMu.Lock()
		if version == statusVersion {
			pending = !shown && err == nil
		}
		statusMu.Unlock()
	}
	renderPendingStatus := func() {
		statusMu.Lock()
		if !active || !pending {
			statusMu.Unlock()
			return
		}
		snapshot, version := activeStatus, statusVersion
		statusMu.Unlock()
		renderStatusSnapshot(snapshot, version, reporter.currentTime())
	}
	reporterDone := make(chan struct{})
	var statusTicker *time.Ticker
	var statusTicks <-chan time.Time
	if statusEnabled {
		statusTicker = time.NewTicker(statusInterval)
		statusTicks = statusTicker.C
	}
	go func() {
		defer close(reporterDone)
		handleReport := func(event reportEvent) {
			switch event.kind {
			case reportPhase:
				_ = reporter.Phase(event.text)
			case reportWarning:
				_ = reporter.Warning("%s", event.text)
				markStatusPending()
				renderPendingStatus()
			case reportStatus:
				statusMu.Lock()
				if event.generation != statusGeneration {
					statusMu.Unlock()
					return
				}
				statusQueued = false
				statusMu.Unlock()
				renderStatusSnapshot(event.status, event.version, event.at)
				statusMu.Lock()
				if event.version != statusVersion && active && pending && !statusQueued {
					statusQueued = reports.enqueueStatus(statusGeneration, activeStatus, statusAt, statusVersion)
				}
				statusMu.Unlock()
			case reportPhaseStatus:
				renderStatusSnapshot(event.status, event.version, event.at)
			}
		}
		drainReports := func() {
			for {
				select {
				case event, ok := <-reports.records:
					if !ok {
						return
					}
					handleReport(event)
				default:
					return
				}
			}
		}
		diagnosticRecords := (<-chan session.Diagnostic)(diagnostics.records)
		reportRecords := (<-chan reportEvent)(reports.records)
		for diagnosticRecords != nil || reportRecords != nil {
			select {
			case diagnostic, ok := <-diagnosticRecords:
				if !ok {
					diagnosticRecords = nil
					continue
				}
				renderDiagnostic(reporter, diagnostic)
				if reporter.Level() == LogDebug {
					markStatusPending()
				}
			case event, ok := <-reportRecords:
				if !ok {
					reportRecords = nil
					continue
				}
				handleReport(event)
			case <-statusTicks:
				drainReports()
				renderPendingStatus()
			}
		}
		if dropped := reports.dropped.Load(); dropped != 0 {
			_ = reporter.Warning("CLI reporting queue dropped %d phase/warning records", dropped)
			markStatusPending()
		}
		if dropped := diagnostics.dropped.Load(); dropped != 0 {
			_ = reporter.Debug("diagnostics: dropped %d debug records because the queue was full", dropped)
			markStatusPending()
		}
		renderPendingStatus()
	}()
	stopReporter := func() {
		close(diagnostics.records)
		close(reports.records)
		if statusTicker != nil {
			statusTicker.Stop()
		}
		<-reporterDone
	}
	oldPhase, oldProgress := dependencies.OnPhase, dependencies.OnProgress
	oldPhaseStatus := dependencies.OnPhaseStatus
	oldStatus := dependencies.OnStatus
	oldWarning, oldSecondary := dependencies.OnWarning, dependencies.OnSecondary
	oldDiagnostic := dependencies.OnDiagnostic
	dependencies.OnPhase = func(phase string) {
		if oldPhase != nil {
			oldPhase(phase)
		}
		phase = SanitizeDiagnostic(phase)
		statusMu.Lock()
		active = false
		pending = false
		statusQueued = false
		statusGeneration++
		statusVersion++
		reports.enqueuePrepared(reportPhase, phase)
		statusMu.Unlock()
	}
	dependencies.OnPhaseStatus = func(phase string, progress session.RunProgress) {
		if oldPhaseStatus != nil {
			oldPhaseStatus(phase, progress)
		}
		setStatus(Status{Phase: phase, VerifiedSelectedBytes: uint64(maxInt64(0, progress.VerifiedSelectedBytes)), SelectedBytes: uint64(maxInt64(0, progress.SelectedBytes))}, true)
	}
	dependencies.OnProgress = func(progress session.RunProgress) {
		if oldProgress != nil {
			oldProgress(progress)
		}
		setStatus(transferStatus(progress), false)
	}
	if statusEnabled {
		dependencies.OnStatus = func(progress session.RunProgress) {
			if oldStatus != nil {
				oldStatus(progress)
			}
			setStatus(transferStatus(progress), false)
		}
	} else if oldStatus != nil {
		dependencies.OnStatus = oldStatus
	}
	dependencies.OnDiagnostic = func(diagnostic session.Diagnostic) {
		if oldDiagnostic != nil {
			oldDiagnostic(diagnostic)
		}
		diagnostics.enqueue(diagnostic)
	}
	dependencies.OnWarning = func(message string) {
		if oldWarning != nil {
			oldWarning(message)
		}
		message = SanitizeDiagnostic(message)
		statusMu.Lock()
		reports.enqueuePrepared(reportWarning, message)
		statusMu.Unlock()
	}
	dependencies.OnSecondary = func(shutdownErr error) {
		if oldSecondary != nil {
			oldSecondary(shutdownErr)
		}
		secondaries.add(shutdownErr)
	}
	result, err := session.Run(ctx, dependencies)
	stopReporter()
	for _, secondary := range secondaries.snapshot() {
		_ = reporter.secondaryFailureText(secondary)
	}
	if err != nil {
		_ = reporter.PrimaryFailure(err, result.HasVerifiedOutput)
		return err
	}
	if opts.ListFiles {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		for _, filePath := range result.Listed {
			if err := encoder.Encode(filePath); err != nil {
				failure := fmt.Errorf("cli: write file listing: %w", err)
				_ = reporter.PrimaryFailure(failure, false)
				return failure
			}
		}
		return nil
	}
	_ = reporter.ReportResult(Result{NoTransferNeeded: result.NoTransferNeeded,
		SelectionComplete: result.SelectionComplete, TorrentComplete: result.TorrentComplete})
	return nil
}

func transferStatus(progress session.RunProgress) Status {
	return Status{Phase: "transfer", VerifiedSelectedBytes: uint64(maxInt64(0, progress.VerifiedSelectedBytes)), SelectedBytes: uint64(maxInt64(0, progress.SelectedBytes)), ActivePeers: progress.ActivePeers, RecentRateBytesPerSec: progress.RecentRateBytesPerSec}
}

func maxInt64(value, floor int64) int64 {
	if value < floor {
		return floor
	}
	return value
}

// RunLocalListing is an explicit spelling for callers that only need the
// local .torrent listing path.
func RunLocalListing(opts Options, stdout io.Writer) error {
	if !opts.ListFiles {
		return fmt.Errorf("cli: --list-files is required")
	}
	return Run(opts, stdout)
}

// ListLocalFiles validates one local .torrent and writes every selectable
// regular path as one JSON string per line, in original torrent order. It does
// not inspect the destination, output files, or piece cache.
func ListLocalFiles(path string, stdout io.Writer) error {
	if path == "" {
		return fmt.Errorf("cli: torrent path is empty")
	}
	if stdout == nil {
		return fmt.Errorf("cli: nil listing writer")
	}
	meta, err := torrent.LoadMetainfo(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	for _, filePath := range torrent.SelectableFiles(meta) {
		if err := encoder.Encode(filePath); err != nil {
			return fmt.Errorf("cli: write file listing: %w", err)
		}
	}
	return nil
}

// ListFiles is an alias for ListLocalFiles.
func ListFiles(path string, stdout io.Writer) error { return ListLocalFiles(path, stdout) }
