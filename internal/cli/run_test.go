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

func TestRunWithSessionEmitsProductionDebugPhaseDiagnostic(t *testing.T) {
	info, _ := v1Info(t, []byte("x"))
	torrentPath := writeV1Torrent(t, v1Metainfo(t, info, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, level := range []LogLevel{LogDebug, LogInfo} {
		t.Run(string(level), func(t *testing.T) {
			var stderr bytes.Buffer
			var callerEvents []session.Diagnostic
			opts := Options{Source: torrentPath, Output: output, Resume: true, LogLevel: level}
			if err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &stderr, session.RunConfig{
				OnDiagnostic: func(event session.Diagnostic) { callerEvents = append(callerEvents, event) },
			}); err != nil {
				t.Fatal(err)
			}
			gotDebug := strings.Contains(stderr.String(), "debug: session phase=selection")
			if gotDebug != (level == LogDebug) {
				t.Fatalf("debug filtering at %s: %q", level, stderr.String())
			}
			if strings.Contains(stderr.String(), "status:") {
				t.Fatalf("noninteractive stderr emitted periodic status: %q", stderr.String())
			}
			if level == LogDebug && strings.Index(stderr.String(), "debug: session phase=selection") > strings.Index(stderr.String(), "result: output is already complete") {
				t.Fatalf("diagnostics were not drained before final output: %q", stderr.String())
			}
			if len(callerEvents) < 2 || callerEvents[0].Kind != session.DiagnosticPhaseTransition || callerEvents[0].Phase != "selection" {
				t.Fatalf("caller diagnostic callback was not composed: %#v", callerEvents)
			}
		})
	}
}

type failingDiagnosticTracker struct{ called chan struct{} }

func (f failingDiagnosticTracker) Announce(ctx context.Context, _ string, _ tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	select {
	case f.called <- struct{}{}:
	default:
	}
	return tracker.HTTPAnnounceResult{Transmitted: true}, errors.New("tracker failed at https://u:p@example.invalid/private?token=secret")
}

func TestRunWithSessionReportsRedactedProductionTrackerFailure(t *testing.T) {
	info, _ := v1Info(t, []byte("x"))
	trackerURL := "http://user:password@[2001:db8::1]:8080/private?token=secret"
	torrentPath := writeV1Torrent(t, v1Metainfo(t, info, trackerURL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stderr bytes.Buffer
	called := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunWithSession(ctx, Options{Source: torrentPath, Output: t.TempDir(), LogLevel: LogDebug}, &bytes.Buffer{}, &stderr, session.RunConfig{
			HTTP: failingDiagnosticTracker{called: called},
			OnDiagnostic: func(event session.Diagnostic) {
				if event.Kind == session.DiagnosticTrackerFailure && event.Endpoint.Host == "[2001:db8::1]:8080" {
					cancel()
				}
			},
			Resolver: noNetworkResolver{}, TCPDial: noNetworkDial, UTPDial: noNetworkDial,
		})
	}()
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("production tracker attempt did not run")
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("canceled tracker session unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not join after cancellation")
	}
	got := stderr.String()
	if !strings.Contains(got, "warning: tracker failure phase=transfer tracker=http://[2001:db8::1]:8080") {
		t.Fatalf("tracker warning missing safe endpoint: %q", got)
	}
	if !strings.Contains(got, "debug: tracker attempt phase=transfer") || !strings.Contains(got, "retrying: transaction failed") {
		t.Fatalf("tracker debug detail missing: %q", got)
	}
	for _, secret := range []string{"user", "password", "/private", "token=secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("tracker diagnostic leaked %q: %q", secret, got)
		}
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

func TestRunInvalidTorrentCreatesNoOutputOrCacheAndStartsNoNetwork(t *testing.T) {
	const unsafeTorrent = "d4:infod6:lengthi1e4:name4:../A12:piece lengthi1e6:pieces20:" +
		"\x6d\xcd\x4c\xe2\x3d\x88\xe2\xee\x95\x68\xba\x54\x6c\x00\x7c\x63\xd9\x13\x1c\x1bee"
	base := t.TempDir()
	torrentPath := filepath.Join(base, "unsafe.torrent")
	if err := os.WriteFile(torrentPath, []byte(unsafeTorrent), 0o600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(base, "output")
	cachePath := filepath.Join(base, "cache")
	var httpCalls, udpCalls, resolveCalls, dialCalls int
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := RunWithSession(ctx, Options{Source: torrentPath, Output: outputPath}, &stdout, &stderr, session.RunConfig{
		HTTP:     countHTTPAnnounce{calls: &httpCalls},
		UDP:      countUDPAnnounce{calls: &udpCalls},
		Resolver: countingResolver{calls: &resolveCalls},
		TCPDial: func(context.Context, string, string) (net.Conn, error) {
			dialCalls++
			return nil, errors.New("unexpected TCP dial")
		},
		UTPDial: func(context.Context, string, string) (net.Conn, error) {
			dialCalls++
			return nil, errors.New("unexpected uTP dial")
		},
		CacheRoot: cachePath,
	})
	if err == nil {
		t.Fatal("unsafe torrent unexpectedly succeeded")
	}
	for _, path := range []string{outputPath, cachePath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid torrent created or inspected %q: %v", path, err)
		}
	}
	if httpCalls != 0 || udpCalls != 0 || resolveCalls != 0 || dialCalls != 0 {
		t.Fatalf("invalid torrent started network work: HTTP=%d UDP=%d resolve=%d dial=%d", httpCalls, udpCalls, resolveCalls, dialCalls)
	}
}

type countHTTPAnnounce struct{ calls *int }

func (s countHTTPAnnounce) Announce(context.Context, string, tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	(*s.calls)++
	return tracker.HTTPAnnounceResult{}, errors.New("unexpected HTTP announce")
}

type countUDPAnnounce struct{ calls *int }

func (s countUDPAnnounce) Announce(context.Context, string, tracker.AnnounceRequest) (tracker.AnnounceResult, error) {
	(*s.calls)++
	return tracker.AnnounceResult{}, errors.New("unexpected UDP announce")
}

type countingResolver struct{ calls *int }

func (s countingResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	(*s.calls)++
	return nil, errors.New("unexpected resolve")
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
