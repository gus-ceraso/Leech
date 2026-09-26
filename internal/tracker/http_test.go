package tracker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestHTTPAnnounceSanitizesOriginalAndRedirectQueries(t *testing.T) {
	var info, peer [20]byte
	for i := range info {
		info[i] = byte(i)
		peer[i] = byte(0xf0 + i)
	}
	req := AnnounceRequest{
		InfoHash: info, PeerID: peer, Downloaded: 12, Left: 34, Uploaded: 0,
		Event: EventStarted, Key: 0x01020304, NumWant: -1, Port: 49152,
	}
	var mu sync.Mutex
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		requestNumber := len(queries)
		mu.Unlock()
		if requestNumber == 1 {
			http.Redirect(w, r, "/next?x=one&info_hash=redirected&x=two&peer_id=redirected&ip=127.0.0.1&ipv4=127.0.0.1&ipv6=%3A%3A1", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "d8:completei2e8:intervali60e8:leechersi3e5:peers6:\x7f\x00\x00\x01\x1a\xe1e")
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPConfig{Timeout: time.Second})
	got, err := client.Announce(context.Background(), server.URL+"/announce?x=zero&info_hash=old&x=last&peer_id=old&port=1&uploaded=7&downloaded=8&left=9&event=stopped&compact=0&key=1&numwant=2&ip=1.2.3.4&ipv4=1.2.3.4&ipv6=::1", req)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
	if !got.Transmitted || got.Interval != time.Minute || len(got.Peers) != 1 || got.Peers[0].Host != "127.0.0.1" || got.Peers[0].Port != 6881 {
		t.Fatalf("unexpected result: %+v", got)
	}

	mu.Lock()
	gotQueries := append([]string(nil), queries...)
	mu.Unlock()
	if len(gotQueries) != 2 {
		t.Fatalf("got %d requests, want 2", len(gotQueries))
	}
	for _, query := range gotQueries {
		for _, key := range []string{"info_hash", "peer_id", "port", "uploaded", "downloaded", "left", "event", "compact", "key", "numwant", "ip", "ipv4", "ipv6"} {
			if count := countQueryKey(query, key); count != map[bool]int{true: 1, false: 0}[key != "ip" && key != "ipv4" && key != "ipv6"] {
				t.Fatalf("query %q has %d occurrences of %q", query, count, key)
			}
		}
		if !strings.Contains(query, "x=zero") && !strings.Contains(query, "x=one") {
			t.Fatalf("unrelated query values were not retained: %q", query)
		}
		if !strings.Contains(query, "x=last") && !strings.Contains(query, "x=two") {
			t.Fatalf("redirect query values were not retained: %q", query)
		}
	}
	wantHash := "info_hash=" + escapeBinary(info[:])
	wantPeer := "peer_id=" + escapeBinary(peer[:])
	if !strings.Contains(gotQueries[0], wantHash) || !strings.Contains(gotQueries[0], wantPeer) {
		t.Fatalf("binary values were not encoded byte-for-byte: %q", gotQueries[0])
	}
	if strings.Contains(gotQueries[1], "redirected") || strings.Contains(gotQueries[1], "ip=") {
		t.Fatalf("redirect restored supplied announce parameters: %q", gotQueries[1])
	}
}

func TestHTTPAnnounceLimitsRedirects(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		http.Redirect(w, r, "/loop", http.StatusFound)
	}))
	defer server.Close()

	_, err := NewHTTPClient(HTTPConfig{Timeout: time.Second}).Announce(context.Background(), server.URL+"/loop", testRequest())
	if err == nil {
		t.Fatal("redirect loop was accepted")
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 10 {
		t.Fatalf("redirect loop sent %d requests, want 10", requests)
	}
}

func TestHTTPSanitizeQueryPreservesUnrelatedRawTokens(t *testing.T) {
	raw := "u=%2F%2f&u=+&%69nfo_hash=old&&i%70=bad&x=%ZZ&&"
	got := sanitizeHTTPURL(&url.URL{RawQuery: raw}, testRequest()).RawQuery
	if !strings.HasPrefix(got, "u=%2F%2f&u=+&&x=%ZZ&&&info_hash=") {
		t.Fatalf("unrelated raw query tokens changed: %q", got)
	}
	if countQueryKey(got, "info_hash") != 1 || countQueryKey(got, "ip") != 0 {
		t.Fatalf("owned query keys survived: %q", got)
	}
}

func TestHTTPSanitizeDelimiterHeavyQueryMemoryBound(t *testing.T) {
	raw := strings.Repeat("&", 1<<20)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := sanitizeHTTPURL(&url.URL{RawQuery: raw}, testRequest())
	runtime.ReadMemStats(&after)
	if !strings.HasPrefix(got.RawQuery, raw+"&info_hash=") {
		t.Fatal("delimiter-heavy query was not preserved")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4<<20 {
		t.Fatalf("sanitizing 1 MiB of delimiters allocated %d bytes, want at most 4 MiB", allocated)
	}
}

func countQueryKey(raw, want string) int {
	count := 0
	for _, part := range strings.Split(raw, "&") {
		key := part
		if i := strings.IndexByte(key, '='); i >= 0 {
			key = key[:i]
		}
		decoded, err := urlQueryUnescape(key)
		if err == nil && decoded == want {
			count++
		}
	}
	return count
}

// Kept as a tiny test helper so tests don't accidentally use URL encoding
// behavior that differs from the request sanitizer's raw query handling.
func urlQueryUnescape(value string) (string, error) {
	return url.QueryUnescape(value)
}

func TestHTTPAnnounceParsesCompactFamiliesAndDictionaryPeers(t *testing.T) {
	v4 := compactPeer(netip.MustParseAddrPort("127.0.0.1:6881"))
	v6 := compactPeer(netip.MustParseAddrPort("[2001:db8::1]:51413"))
	body := "d8:completei9e8:intervali60e8:leechersi4e5:peers" + strconv.Itoa(len(v4)) + ":" + string(v4) + "6:peers6" + strconv.Itoa(len(v6)) + ":" + string(v6) + "e"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	got, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
	if got.Interval != time.Minute || got.Leechers != 4 || got.Seeders != 9 || len(got.Peers) != 2 {
		t.Fatalf("unexpected compact result: %+v", got)
	}
	if got.Peers[0].Host != "127.0.0.1" || got.Peers[0].Port != 6881 || got.Peers[1].Host != "2001:db8::1" || got.Peers[1].Port != 51413 {
		t.Fatalf("peer families = %v", got.Peers)
	}

	dictionary := "d8:intervali60e5:peersl" +
		"d2:ip9:127.0.0.14:porti6881ee" +
		"d2:ip11:2001:db8::14:porti51413ee" +
		"ee"
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, dictionary)
	}))
	defer server2.Close()
	got, err = NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server2.URL, testRequest())
	if err != nil {
		t.Fatalf("dictionary announce: %v", err)
	}
	if len(got.Peers) != 2 {
		t.Fatalf("dictionary peers = %v", got.Peers)
	}

	dictionary = "d8:intervali60e5:peersl" +
		"d2:ip12:seed.example7:peer id20:aaaaaaaaaaaaaaaaaaaa4:porti6882ee" +
		"ee"
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, dictionary)
	}))
	defer server3.Close()
	got, err = NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server3.URL, testRequest())
	if err != nil || len(got.Peers) != 1 || got.Peers[0].Host != "seed.example" || !got.Peers[0].HasID {
		t.Fatalf("hostname dictionary peer = %+v, err=%v", got.Peers, err)
	}
}

func TestHTTPAnnounceParsesTwentyThousandDictionaryPeers(t *testing.T) {
	const peerCount = 20_000
	peerID := strings.Repeat("a", 20)
	body := []byte("d8:intervali60e5:peersl")
	for i := 0; i < peerCount; i++ {
		host := "peer-" + strconv.Itoa(i) + ".example"
		body = append(body, 'd')
		body = appendBencodeString(body, []byte("ip"))
		body = appendBencodeString(body, []byte(host))
		body = appendBencodeString(body, []byte("peer id"))
		body = appendBencodeString(body, []byte(peerID))
		body = appendBencodeString(body, []byte("port"))
		body = append(body, "i6881ee"...)
	}
	body = append(body, 'e', 'e')
	if len(body) >= 2<<20 {
		t.Fatalf("fixture unexpectedly large: %d bytes", len(body))
	}

	got, err := parseHTTPAnnounceResponse(body)
	if err != nil {
		t.Fatalf("parse maximum peer list: %v", err)
	}
	if len(got.Peers) != peerCount {
		t.Fatalf("parsed %d peers, want %d", len(got.Peers), peerCount)
	}
	if got.Peers[0].Host != "peer-0.example" || got.Peers[peerCount-1].Host != "peer-19999.example" {
		t.Fatalf("peer order = %q ... %q", got.Peers[0].Host, got.Peers[peerCount-1].Host)
	}
	var wantPeerID [20]byte
	copy(wantPeerID[:], peerID)
	if !got.Peers[0].HasID || got.Peers[0].PeerID != wantPeerID {
		t.Fatalf("peer ID was not retained: %+v", got.Peers[0])
	}
}

func TestHTTPAnnounceDeduplicatesPeerRepresentationsInOrder(t *testing.T) {
	v4a := netip.MustParseAddrPort("127.0.0.1:6881")
	v4b := netip.MustParseAddrPort("127.0.0.2:6882")
	v6 := netip.MustParseAddrPort("[2001:db8::1]:6883")
	compact := []byte("d8:intervali60e5:peers")
	compact4 := append(compactPeer(v4a), compactPeer(v4b)...)
	compact4 = append(compact4, compactPeer(v4a)...)
	compact = appendBencodeString(compact, compact4)
	compact = append(compact, "6:peers6"...)
	compact6 := append(compactPeer(v6), compactPeer(v6)...)
	compact = appendBencodeString(compact, compact6)
	compact = append(compact, 'e')

	got, err := parseHTTPAnnounceResponse(compact)
	if err != nil {
		t.Fatalf("parse compact peers: %v", err)
	}
	wantCompact := []string{"127.0.0.1", "127.0.0.2", "2001:db8::1"}
	if len(got.Peers) != len(wantCompact) {
		t.Fatalf("compact peers = %+v", got.Peers)
	}
	for i, host := range wantCompact {
		if got.Peers[i].Host != host {
			t.Fatalf("compact peer %d = %q, want %q", i, got.Peers[i].Host, host)
		}
	}

	var dictionary []byte
	dictionary = append(dictionary, "d8:intervali60e5:peersl"...)
	appendDictionaryPeer := func(host string, peerID string) {
		dictionary = append(dictionary, 'd')
		dictionary = appendBencodeString(dictionary, []byte("ip"))
		dictionary = appendBencodeString(dictionary, []byte(host))
		if peerID != "" {
			dictionary = appendBencodeString(dictionary, []byte("peer id"))
			dictionary = appendBencodeString(dictionary, []byte(peerID))
		}
		dictionary = appendBencodeString(dictionary, []byte("port"))
		dictionary = append(dictionary, "i6883ee"...)
	}
	appendDictionaryPeer("seed.example", "")
	appendDictionaryPeer("seed.example", strings.Repeat("a", 20))
	appendDictionaryPeer("seed.example", strings.Repeat("b", 20))
	appendDictionaryPeer("2001:db8::1", "")
	dictionary = append(dictionary, 'e')
	dictionary = append(dictionary, "6:peers6"...)
	dictionary = appendBencodeString(dictionary, compactPeer(v6))
	dictionary = append(dictionary, 'e')

	got, err = parseHTTPAnnounceResponse(dictionary)
	if err != nil {
		t.Fatalf("parse dictionary and compact duplicates: %v", err)
	}
	if len(got.Peers) != 2 || got.Peers[0].Host != "seed.example" || got.Peers[1].Host != "2001:db8::1" {
		t.Fatalf("dictionary peer order = %+v", got.Peers)
	}
	var wantPeerID [20]byte
	copy(wantPeerID[:], strings.Repeat("a", 20))
	if !got.Peers[0].HasID || got.Peers[0].PeerID != wantPeerID {
		t.Fatalf("duplicate changed optional peer ID semantics: %+v", got.Peers[0])
	}
}

func TestHTTPAnnounceRejectsExcessiveIgnoredTrees(t *testing.T) {
	tests := []struct {
		name  string
		count int
		item  string
	}{
		{name: "container entries", count: trackerBencodeContainerEntries + 1, item: "le"},
		{name: "decoded values", count: 20_000, item: "li0ei0ei0ei0ei0ee"},
		{name: "dictionary entries", count: 17_000, item: "d1:ai0e1:bi0e1:ci0e1:di0ee"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte("d8:intervali60e5:peers0:5:zzzzzl")
			for i := 0; i < test.count; i++ {
				body = append(body, test.item...)
			}
			body = append(body, 'e', 'e')
			if len(body) >= 1<<20 {
				t.Fatalf("fixture unexpectedly large: %d bytes", len(body))
			}
			_, err := parseHTTPAnnounceResponse(body)
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Code != HTTPErrorMalformed || !errors.Is(err, bencode.ErrLimit) {
				t.Fatalf("parse ignored tree error = %v, want malformed", err)
			}
		})
	}
}

func appendBencodeString(dst, value []byte) []byte {
	dst = strconv.AppendInt(dst, int64(len(value)), 10)
	dst = append(dst, ':')
	return append(dst, value...)
}

func TestHTTPAnnounceRejectsMalformedCompactStrideAndEndpoint(t *testing.T) {
	cases := []string{
		"d8:intervali60e5:peers1:xe",
		"d8:intervali60e5:peers6:\x00\x00\x00\x00\x00\x01e",
	}
	for _, body := range cases {
		body := body
		t.Run(strconv.Itoa(len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			_, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Code != HTTPErrorMalformed {
				t.Fatalf("error = %v, want malformed", err)
			}
		})
	}
}

func TestHTTPAnnounceBEP31RetryClasses(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		class  HTTPFailureClass
		delay  time.Duration
		code   HTTPErrorCode
		status int
	}{
		{name: "integer", body: "d14:failure reason3:bad8:retry ini5ee", class: HTTPFailureTransient, delay: 5 * time.Minute, code: HTTPErrorTracker},
		{name: "decimal string", body: "d14:failure reason3:bad8:retry in1:5e", class: HTTPFailureTransient, delay: 5 * time.Minute, code: HTTPErrorTracker},
		{name: "never", body: "d14:failure reason3:bad8:retry in5:nevere", class: HTTPFailureNever, code: HTTPErrorTracker},
		{name: "invalid", body: "d14:failure reason3:bad8:retry in1:0e", class: HTTPFailureInvalidDelay, code: HTTPErrorRetryDelay},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			_, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Class != test.class || httpErr.Code != test.code || httpErr.RetryAfter != test.delay {
				t.Fatalf("error = %+v, want class=%v delay=%v code=%v", httpErr, test.class, test.delay, test.code)
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "not a tracker")
	}))
	defer server.Close()
	_, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Class != HTTPFailureDefinitive || httpErr.StatusCode != http.StatusNotFound {
		t.Fatalf("404 error = %+v", httpErr)
	}
}

func TestHTTPAnnounce404BodyErrorsAreDefinitive(t *testing.T) {
	cases := []struct {
		name     string
		body     func(http.ResponseWriter)
		wantCode HTTPErrorCode
	}{
		{
			name: "oversized",
			body: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, strings.Repeat("x", limits.HTTPResponseBytes+1))
			},
			wantCode: HTTPErrorBodyLimit,
		},
		{
			name: "read error",
			body: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "4")
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, "x")
			},
			wantCode: HTTPErrorResponse,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				test.body(w)
			}))
			defer server.Close()
			result, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.Code != test.wantCode || httpErr.Class != HTTPFailureDefinitive || httpErr.StatusCode != http.StatusNotFound || !httpErr.Transmitted || !result.Transmitted {
				t.Fatalf("result=%+v error=%+v, want definitive 404 %s", result, httpErr, test.wantCode)
			}
		})
	}
}

func TestHTTPAnnounceTransmissionSurvivesResponseLoss(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("test server does not support hijacking")
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}))
	defer server.Close()
	result, err := NewHTTPClient(HTTPConfig{Timeout: time.Second}).Announce(context.Background(), server.URL, testRequest())
	if err == nil || !result.Transmitted {
		t.Fatalf("result=%+v err=%v, want transmitted response failure", result, err)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || !httpErr.Transmitted {
		t.Fatalf("error=%+v, want transmitted", httpErr)
	}
}

func TestHTTPAnnounceResponseSizeLimitAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("x", limits.HTTPResponseBytes+1)), int64(limits.HTTPResponseBytes+1))
	}))
	defer server.Close()
	_, err := NewHTTPClient(HTTPConfig{}).Announce(context.Background(), server.URL, testRequest())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != HTTPErrorBodyLimit {
		t.Fatalf("size error = %+v", httpErr)
	}

	blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer blocking.Close()
	started := time.Now()
	_, err = NewHTTPClient(HTTPConfig{Timeout: 20 * time.Millisecond}).Announce(context.Background(), blocking.URL, testRequest())
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	if !errors.As(err, &httpErr) || httpErr.Code != HTTPErrorTimeout {
		t.Fatalf("timeout error = %+v", httpErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started = time.Now()
	_, err = NewHTTPClient(HTTPConfig{Timeout: 20 * time.Millisecond}).Announce(ctx, blocking.URL, testRequest())
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded timeout with longer caller deadline took %v", elapsed)
	}
	if !errors.As(err, &httpErr) || httpErr.Code != HTTPErrorTimeout {
		t.Fatalf("bounded timeout error = %+v", httpErr)
	}
}

func TestHTTPAnnounceTLSUsesConfiguredVerifiedRoots(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "d8:intervali60e5:peers0:e")
	}))
	defer server.Close()
	if _, err := NewHTTPClient(HTTPConfig{Timeout: time.Second}).Announce(context.Background(), server.URL, testRequest()); err == nil {
		t.Fatal("default TLS verification unexpectedly trusted fixture certificate")
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
	client := NewHTTPClient(HTTPConfig{Client: &http.Client{Transport: transport}, Timeout: time.Second})
	if _, err := client.Announce(context.Background(), server.URL, testRequest()); err != nil {
		t.Fatalf("verified TLS announce: %v", err)
	}
}

func FuzzParseHTTPAnnounceResponse(f *testing.F) {
	f.Add([]byte("d8:intervali60e5:peers0:e"))
	f.Add([]byte("d14:failure reason3:bade"))
	f.Add([]byte("d8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe1e"))
	dictionaryPeer := []byte("d8:intervali60e5:peersld2:ip9:127.0.0.14:porti6881eeee")
	parsed, err := parseHTTPAnnounceResponse(dictionaryPeer)
	if err != nil || len(parsed.Peers) != 1 || parsed.Peers[0].Host != "127.0.0.1" || parsed.Peers[0].Port != 6881 {
		f.Fatalf("dictionary-peer fuzz seed = %+v, %v", parsed.Peers, err)
	}
	f.Add(dictionaryPeer)
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = parseHTTPAnnounceResponse(body)
	})
}

func compactPeer(peer netip.AddrPort) []byte {
	result := make([]byte, 6)
	if peer.Addr().Is4() {
		v4 := peer.Addr().As4()
		copy(result, v4[:])
	} else {
		result = make([]byte, 18)
		v6 := peer.Addr().As16()
		copy(result, v6[:])
	}
	binary.BigEndian.PutUint16(result[len(result)-2:], peer.Port())
	return result
}
