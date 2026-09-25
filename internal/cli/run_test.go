package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func testTorrentValue(kind bencode.Kind, bytesValue string) bencode.Value {
	if kind == bencode.Bytes {
		return bencode.Value{Type: kind, Bytes: []byte(bytesValue)}
	}
	return bencode.Value{}
}

func testTorrentEntry(key string, value bencode.Value) bencode.Entry {
	return bencode.Entry{Key: []byte(key), Value: value}
}

func testTorrentList(values ...bencode.Value) bencode.Value {
	return bencode.Value{Type: bencode.List, List: values}
}

func testTorrentDict(entries ...bencode.Entry) bencode.Value {
	return bencode.Value{Type: bencode.Dictionary, Dict: entries}
}

func testTorrentInt(value int64) bencode.Value {
	return bencode.Value{Type: bencode.Integer, Int: value}
}

func localListingTorrent(t *testing.T) []byte {
	t.Helper()
	info := testTorrentDict(
		testTorrentEntry("files", testTorrentList(
			testTorrentDict(
				testTorrentEntry("length", testTorrentInt(1)),
				testTorrentEntry("path", testTorrentList(testTorrentValue(bencode.Bytes, "one.txt"))),
			),
			testTorrentDict(
				testTorrentEntry("attr", testTorrentValue(bencode.Bytes, "p")),
				testTorrentEntry("length", testTorrentInt(1)),
			),
			testTorrentDict(
				testTorrentEntry("attr", testTorrentValue(bencode.Bytes, "l")),
				testTorrentEntry("path", testTorrentList(testTorrentValue(bencode.Bytes, "link"))),
				testTorrentEntry("symlink path", testTorrentList(testTorrentValue(bencode.Bytes, "one.txt"))),
			),
			testTorrentDict(
				testTorrentEntry("length", testTorrentInt(1)),
				testTorrentEntry("path", testTorrentList(testTorrentValue(bencode.Bytes, "dir"), testTorrentValue(bencode.Bytes, "two"))),
			),
		)),
		testTorrentEntry("name", testTorrentValue(bencode.Bytes, "release")),
		testTorrentEntry("piece length", testTorrentInt(4)),
		testTorrentEntry("pieces", testTorrentValue(bencode.Bytes, string(bytes.Repeat([]byte{0x42}, 20)))),
	)
	root := testTorrentDict(testTorrentEntry("info", info))
	encoded, err := bencode.Encode(root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestRunLocalListingValidatesAndWritesOnlySelectablePaths(t *testing.T) {
	torrentPath := filepath.Join(t.TempDir(), "fixture.torrent")
	if err := os.WriteFile(torrentPath, localListingTorrent(t), 0o600); err != nil {
		t.Fatal(err)
	}
	missingOutput := filepath.Join(t.TempDir(), "must-not-be-inspected")
	var output bytes.Buffer
	err := RunLocalListing(Options{Source: torrentPath, ListFiles: true, Output: missingOutput}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "\"one.txt\"\n\"dir/two\"\n"; got != want {
		t.Fatalf("listing = %q, want %q", got, want)
	}
	if _, err := os.Stat(missingOutput); !os.IsNotExist(err) {
		t.Fatalf("listing inspected or created output path: %v", err)
	}
}

func TestRunLocalListingDoesNotTouchOutputOnInvalidMetadata(t *testing.T) {
	torrentPath := filepath.Join(t.TempDir(), "bad.torrent")
	if err := os.WriteFile(torrentPath, []byte("not bencode"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingOutput := filepath.Join(t.TempDir(), "must-not-be-inspected")
	var output bytes.Buffer
	if err := Run(Options{Source: torrentPath, ListFiles: true, Output: missingOutput}, &output); err == nil {
		t.Fatal("invalid metadata unexpectedly succeeded")
	}
	if output.Len() != 0 {
		t.Fatalf("invalid metadata wrote output: %q", output.String())
	}
	if _, err := os.Stat(missingOutput); !os.IsNotExist(err) {
		t.Fatalf("invalid listing inspected output path: %v", err)
	}
}

func TestRunRemoteListingCancelsWithoutNetwork(t *testing.T) {
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := RunWithSession(ctx, Options{Source: "magnet:?xt=urn:btih:0123456789012345678901234567890123456789", ListFiles: true}, &output, io.Discard, session.RunConfig{
		HTTP:     noNetworkHTTP{},
		TCPDial:  noNetworkDial,
		UTPDial:  noNetworkDial,
		Resolver: noNetworkResolver{},
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(output.Bytes(), []byte(nil)) {
		t.Fatalf("remote listing wrote output: %q", output.String())
	}
}

func TestRunWithSessionDoesNotClaimInvalidTorrentIsResumable(t *testing.T) {
	torrentPath := filepath.Join(t.TempDir(), "bad.torrent")
	if err := os.WriteFile(torrentPath, []byte("not bencode"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	opts := Options{Source: torrentPath, Output: t.TempDir()}
	if err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &stderr, session.RunConfig{}); err == nil {
		t.Fatal("invalid torrent unexpectedly succeeded")
	}
	if strings.Contains(stderr.String(), "verified partial output remains resumable") {
		t.Fatalf("prevalidation failure claimed resumable output: %q", stderr.String())
	}
}

type noNetworkHTTP struct{}

func (noNetworkHTTP) Announce(ctx context.Context, _ string, _ tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	return tracker.HTTPAnnounceResult{}, ctx.Err()
}

type noNetworkResolver struct{}

func (noNetworkResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return nil, errors.New("resolver disabled in test")
}

func noNetworkDial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("dial disabled in test")
}

var _ peer.Resolver = noNetworkResolver{}
