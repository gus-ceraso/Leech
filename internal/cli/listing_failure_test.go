package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/session"
)

type listingFailWriter struct {
	attempted bytes.Buffer
	err       error
}

func (w *listingFailWriter) Write(data []byte) (int, error) {
	w.attempted.Write(data)
	return 0, w.err
}

func TestReviewListingWriteFailureIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.torrent")
	if err := os.WriteFile(path, localListingTorrent(t), 0600); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("listing output failed\x1b[2J")
	stdout := &listingFailWriter{err: cause}
	var stderr bytes.Buffer
	err := RunWithSession(context.Background(), Options{Source: path, ListFiles: true, LogLevel: LogError}, stdout, &stderr, session.RunConfig{})
	if !errors.Is(err, cause) {
		t.Fatalf("listing error = %v, want original write cause", err)
	}
	if got, want := stdout.attempted.String(), "\"one.txt\"\n"; got != want {
		t.Fatalf("stdout = %q, want listing only %q", got, want)
	}
	if got, want := withoutLogTimes(stderr.String()), "error: failure: cli: write file listing: listing output failed\\x1b[2J\n"; got != want {
		t.Fatalf("stderr = %q, want primary error %q", got, want)
	}
}
