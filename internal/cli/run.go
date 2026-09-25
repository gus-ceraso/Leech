package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

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
	if stdout == nil {
		return fmt.Errorf("cli: nil output writer")
	}
	if stderr == nil {
		stderr = io.Discard
	}
	source, err := torrent.ParseSource(opts.Source)
	if err != nil {
		NewReporter(opts.LogLevel, stderr).PrimaryFailure(err, false)
		return err
	}
	reporter := NewReporter(opts.LogLevel, stderr)
	dependencies.Source = source
	dependencies.SourceRaw = opts.Source
	dependencies.OutputDir = opts.Output
	dependencies.Patterns = append([]string(nil), opts.Files...)
	dependencies.ListFiles = opts.ListFiles
	dependencies.Resume = opts.Resume
	dependencies.Streaming = opts.Stream
	dependencies.Timeout = opts.Timeout
	oldPhase, oldProgress := dependencies.OnPhase, dependencies.OnProgress
	oldWarning, oldSecondary := dependencies.OnWarning, dependencies.OnSecondary
	dependencies.OnPhase = func(phase string) {
		if oldPhase != nil {
			oldPhase(phase)
		}
		_ = reporter.Phase(phase)
	}
	dependencies.OnProgress = func(progress session.RunProgress) {
		if oldProgress != nil {
			oldProgress(progress)
		}
		_ = reporter.Status(Status{Phase: "transfer", VerifiedSelectedBytes: uint64(maxInt64(0, progress.VerifiedSelectedBytes)), SelectedBytes: uint64(maxInt64(0, progress.SelectedBytes)), ActivePeers: progress.ActivePeers, RecentRateBytesPerSec: progress.RecentRateBytesPerSec})
	}
	dependencies.OnWarning = func(message string) {
		if oldWarning != nil {
			oldWarning(message)
		}
		_ = reporter.Warning("%s", message)
	}
	dependencies.OnSecondary = func(shutdownErr error) {
		if oldSecondary != nil {
			oldSecondary(shutdownErr)
		}
		_ = reporter.SecondaryFailure(shutdownErr)
	}
	result, err := session.Run(ctx, dependencies)
	if err != nil {
		_ = reporter.PrimaryFailure(err, !opts.ListFiles)
		return err
	}
	if opts.ListFiles {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		for _, filePath := range result.Listed {
			if err := encoder.Encode(filePath); err != nil {
				return fmt.Errorf("cli: write file listing: %w", err)
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
