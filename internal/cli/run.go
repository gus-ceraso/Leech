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
	var statusMu sync.Mutex
	var activeStatus Status
	active, pending := false, false
	renderStatus := func() {
		shown, err := reporter.renderStatus(activeStatus)
		pending = !shown && err == nil
	}
	stopStatus := func() {}
	if reporter.statusEnabled() {
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(statusInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					statusMu.Lock()
					if active && pending {
						renderStatus()
					}
					statusMu.Unlock()
				case <-stop:
					return
				}
			}
		}()
		stopStatus = func() { close(stop); <-done }
	}
	oldPhase, oldProgress := dependencies.OnPhase, dependencies.OnProgress
	oldPhaseStatus := dependencies.OnPhaseStatus
	oldWarning, oldSecondary := dependencies.OnWarning, dependencies.OnSecondary
	dependencies.OnPhase = func(phase string) {
		if oldPhase != nil {
			oldPhase(phase)
		}
		statusMu.Lock()
		active = false
		pending = false
		_ = reporter.Phase(phase)
		statusMu.Unlock()
	}
	dependencies.OnPhaseStatus = func(phase string, progress session.RunProgress) {
		if oldPhaseStatus != nil {
			oldPhaseStatus(phase, progress)
		}
		statusMu.Lock()
		activeStatus = Status{Phase: phase, VerifiedSelectedBytes: uint64(maxInt64(0, progress.VerifiedSelectedBytes)), SelectedBytes: uint64(maxInt64(0, progress.SelectedBytes))}
		active = true
		renderStatus()
		statusMu.Unlock()
	}
	dependencies.OnProgress = func(progress session.RunProgress) {
		if oldProgress != nil {
			oldProgress(progress)
		}
		statusMu.Lock()
		activeStatus = Status{Phase: "transfer", VerifiedSelectedBytes: uint64(maxInt64(0, progress.VerifiedSelectedBytes)), SelectedBytes: uint64(maxInt64(0, progress.SelectedBytes)), ActivePeers: progress.ActivePeers, RecentRateBytesPerSec: progress.RecentRateBytesPerSec}
		active = true
		renderStatus()
		statusMu.Unlock()
	}
	dependencies.OnWarning = func(message string) {
		if oldWarning != nil {
			oldWarning(message)
		}
		statusMu.Lock()
		_ = reporter.Warning("%s", message)
		pending = active
		statusMu.Unlock()
	}
	dependencies.OnSecondary = func(shutdownErr error) {
		if oldSecondary != nil {
			oldSecondary(shutdownErr)
		}
		statusMu.Lock()
		_ = reporter.SecondaryFailure(shutdownErr)
		pending = active
		statusMu.Unlock()
	}
	result, err := session.Run(ctx, dependencies)
	stopStatus()
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
