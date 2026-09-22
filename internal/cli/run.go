package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	// ErrDownloadUnavailable marks the temporary command boundary before the
	// transfer session is wired. Local listing remains fully implemented.
	ErrDownloadUnavailable = errors.New("download session is not available")
	// ErrRemoteListingUnavailable marks metadata-only discovery, which is added
	// with the tracker and peer session.
	ErrRemoteListingUnavailable = errors.New("remote metadata listing is not available")
)

// Run handles the initial side-effect-free CLI runtime path. A local torrent
// with --list-files is read, validated, and emitted to stdout. Other flows are
// deliberately left for the session coordinator; in particular, this function
// never validates --output or creates output and cache paths while listing.
func Run(opts Options, stdout io.Writer) error {
	if !opts.ListFiles {
		return ErrDownloadUnavailable
	}
	if stdout == nil {
		return fmt.Errorf("cli: nil listing writer")
	}
	source, err := torrent.ParseSource(opts.Source)
	if err != nil {
		return err
	}
	if source.Kind != torrent.SourcePath {
		return ErrRemoteListingUnavailable
	}
	return ListLocalFiles(source.Path, stdout)
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
