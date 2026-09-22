package torrent

import (
	"encoding/base32"
	"strconv"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
)

const testHashHex = "0123456789abcdef0123456789abcdef01234567"

func TestParseSourceClassification(t *testing.T) {
	base32Hash := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
	tests := []struct {
		name string
		raw  string
		kind SourceKind
	}{
		{"path", "release.torrent", SourcePath},
		{"hash", testHashHex, SourceInfoHash},
		{"base32", strings.ToLower(base32Hash), SourceInfoHash},
		{"magnet", "MAGNET:?xt=urn:btih:" + testHashHex, SourceMagnet},
		{"hash-shaped-path", "./" + testHashHex, SourcePath},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := ParseSource(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			if source.Kind != test.kind {
				t.Fatalf("kind = %v, want %v", source.Kind, test.kind)
			}
			if len(source.Trackers) == 0 || source.Trackers[0] != DefaultTracker {
				t.Fatalf("trackers = %#v", source.Trackers)
			}
		})
	}
	for _, raw := range []string{"-", "http://example.test/a", "btmh:foo"} {
		if _, err := ParseSource(raw); err == nil {
			t.Errorf("ParseSource(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestParseMagnetFieldsAndBounds(t *testing.T) {
	raw := "magnet:?xt=urn%3Abtih%3A" + testHashHex +
		"&dn=The%20Name&tr=udp%3A%2F%2Ftracker.example%3A6969%2Fannounce" +
		"&tr=http%3A%2F%2Ftracker.example%2Fannounce&x.pe=%5B%3A%3A1%5D%3A6881" +
		"&so=0,2,4-6"
	magnet, err := ParseMagnet(raw)
	if err != nil {
		t.Fatal(err)
	}
	if magnet.DisplayName != "The Name" || len(magnet.Peers) != 1 || len(magnet.Selection) != 3 {
		t.Fatalf("unexpected magnet: %#v", magnet)
	}
	if magnet.Selection[2] != (IndexRange{Start: 4, End: 6}) {
		t.Fatalf("selection = %#v", magnet.Selection)
	}
	if len(magnet.Trackers) != 3 || magnet.Trackers[0] != DefaultTracker {
		t.Fatalf("trackers = %#v", magnet.Trackers)
	}
}

func TestParseMagnetRejectsV2ConflictAndMalformedFields(t *testing.T) {
	cases := []string{
		"magnet:?xt=urn%3Abtmh%3Adeadbeef&xt=urn%3Abtih%3A" + testHashHex,
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&xt=urn%3Abtih%3Aabcdefabcdefabcdefabcdefabcdefabcdefabcd",
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&so=4-2",
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&so=100000",
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&x.pe=127.0.0.1",
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&x.pe=0.0.0.0%3A1",
		"magnet:?xt=urn%3Abtih%3A" + testHashHex + "&tr=ftp%3A%2F%2Fexample.test%2Fannounce",
	}
	for _, raw := range cases {
		if _, err := ParseMagnet(raw); err == nil {
			t.Errorf("ParseMagnet(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestTrackerNormalization(t *testing.T) {
	trackers, err := MergeTrackers(
		[]string{"HTTP://Tracker.Example:80/announce", "http://tracker.example:80/announce"},
		[]string{"udp://tracker.example:6969/announce"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(trackers) != 3 || trackers[0] != DefaultTracker || trackers[1] != "http://tracker.example:80/announce" {
		t.Fatalf("trackers = %#v", trackers)
	}
}

func TestMagnetTrackerDuplicatesCollapseBeforeLimit(t *testing.T) {
	query := strings.Repeat("&tr=http%3A%2F%2Ftracker.example%2Fannounce", 65)
	magnet, err := ParseMagnet("magnet:?xt=urn%3Abtih%3A" + testHashHex + query)
	if err != nil {
		t.Fatal(err)
	}
	if len(magnet.Trackers) != 2 || magnet.Trackers[1] != "http://tracker.example/announce" {
		t.Fatalf("trackers = %#v", magnet.Trackers)
	}

	tooMany := make([]string, limits.Trackers)
	for i := range tooMany {
		tooMany[i] = "http://tracker-" + strconv.Itoa(i) + ".example/announce"
	}
	if _, err := TrackersWithDefault(tooMany); err == nil {
		t.Fatal("expected the default plus 64 unique trackers to exceed the limit")
	}
}

func TestTrackerErrorsDoNotExposeURL(t *testing.T) {
	raw := "http://user:secret@example.test:bad/announce?token=secret-query"
	_, err := NormalizeTrackers([]string{raw})
	if err == nil {
		t.Fatal("expected malformed tracker URL")
	}
	for _, secret := range []string{"user", "secret", "secret-query", "/announce"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("tracker error %q leaked %q", err, secret)
		}
	}
}

func TestParsePeerAddress(t *testing.T) {
	for _, test := range []struct {
		raw  string
		host string
		port uint16
	}{
		{"127.0.0.1:1", "127.0.0.1", 1},
		{"Example.test:6881", "example.test", 6881},
		{"[::1]:6881", "::1", 6881},
	} {
		peer, err := ParsePeerAddress(test.raw)
		if err != nil {
			t.Errorf("ParsePeerAddress(%q): %v", test.raw, err)
			continue
		}
		if peer.Host != test.host || peer.Port != test.port {
			t.Errorf("peer = %#v", peer)
		}
	}
}

func FuzzParseMagnet(f *testing.F) {
	f.Add("magnet:?xt=urn:btih:" + testHashHex)
	f.Add("magnet:?xt=urn%3Abtih%3A" + testHashHex + "&so=0-3")
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = ParseMagnet(raw)
	})
}

func FuzzParseSource(f *testing.F) {
	f.Add(testHashHex)
	f.Add("./" + testHashHex)
	f.Add("magnet:?xt=urn:btih:" + testHashHex)
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = ParseSource(raw)
	})
}
