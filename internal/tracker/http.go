package tracker

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

const defaultHTTPTimeout = 30 * time.Second

// HTTPFailureClass tells the tracker lifecycle how to handle an HTTP
// transaction failure. A transient failure can be retried, while the other
// classes disable the tracker for the current run.
type HTTPFailureClass uint8

const (
	HTTPFailureTransient HTTPFailureClass = iota
	HTTPFailureDefinitive
	HTTPFailureNever
	HTTPFailureInvalidDelay
)

// HTTPErrorCode identifies the local reason an HTTP tracker transaction
// failed. The response body and URL are deliberately not included in errors.
type HTTPErrorCode string

const (
	HTTPErrorInvalidURL HTTPErrorCode = "invalid-url"
	HTTPErrorRequest    HTTPErrorCode = "request"
	HTTPErrorStatus     HTTPErrorCode = "status"
	HTTPErrorResponse   HTTPErrorCode = "response"
	HTTPErrorBodyLimit  HTTPErrorCode = "body-limit"
	HTTPErrorMalformed  HTTPErrorCode = "malformed"
	HTTPErrorTracker    HTTPErrorCode = "tracker"
	HTTPErrorInterval   HTTPErrorCode = "interval"
	HTTPErrorRetryDelay HTTPErrorCode = "retry-delay"
	HTTPErrorCanceled   HTTPErrorCode = "canceled"
	HTTPErrorTimeout    HTTPErrorCode = "timeout"
)

var (
	ErrHTTPInvalidURL = errors.New("invalid HTTP tracker URL")
	ErrHTTPTimeout    = errors.New("HTTP tracker transaction timed out")
)

// HTTPError reports a tracker-local HTTP transaction failure. Transmitted is
// true once net/http has written the complete request, even when the response
// is lost. RetryAfter is set for a finite BEP 31 delay.
type HTTPError struct {
	Code        HTTPErrorCode
	Class       HTTPFailureClass
	StatusCode  int
	RetryAfter  time.Duration
	Transmitted bool
	Err         error
}

func (e *HTTPError) Error() string {
	if e == nil {
		return ""
	}
	name := string(e.Code) + " HTTP tracker failure"
	if e.StatusCode != 0 {
		name += " (status " + strconv.Itoa(e.StatusCode) + ")"
	}
	if e.Err != nil {
		name += ": " + e.Err.Error()
	}
	return name
}

func (e *HTTPError) Unwrap() error { return e.Err }

// HTTPPeer is one dictionary or compact tracker peer. Host is either a
// canonical IP literal (compact peers) or the bounded hostname supplied by a
// dictionary response. Hostname resolution and candidate filtering belong to
// candidate admission, not the tracker transaction.
type HTTPPeer struct {
	Host   string
	Port   uint16
	PeerID [20]byte
	HasID  bool
}

// HTTPAnnounceResult is the successful data returned by one HTTP(S) announce.
type HTTPAnnounceResult struct {
	Interval    time.Duration
	Leechers    uint32
	Seeders     uint32
	Peers       []HTTPPeer
	Transmitted bool
}

// HTTPConfig supplies optional HTTP transport dependencies. A nil Client uses
// the standard library transport with normal TLS certificate verification.
// Timeout bounds the complete request and response exchange; zero selects the
// default bound.
type HTTPConfig struct {
	Client  *http.Client
	Timeout time.Duration
}

// HTTPClient performs bounded HTTP(S) tracker transactions. It is safe for
// independent tracker calls and does not retain tracker state between calls.
type HTTPClient struct {
	client  *http.Client
	timeout time.Duration
}

// NewHTTPClient constructs an HTTP tracker client.
func NewHTTPClient(cfg HTTPConfig) *HTTPClient {
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	return &HTTPClient{client: client, timeout: timeout}
}

// Announce sends one HTTP(S) tracker announce. The returned transmission bit
// remains observable on every error through HTTPError.Transmitted and the
// result value.
func (c *HTTPClient) Announce(ctx context.Context, trackerURL string, announce AnnounceRequest) (HTTPAnnounceResult, error) {
	var result HTTPAnnounceResult
	if c == nil {
		return result, &HTTPError{Code: HTTPErrorRequest, Class: HTTPFailureTransient, Err: errors.New("nil HTTP tracker client")}
	}
	if err := validateAnnounceRequest(announce); err != nil {
		return result, &HTTPError{Code: HTTPErrorInvalidURL, Class: HTTPFailureDefinitive, Err: err}
	}
	u, err := parseHTTPURL(trackerURL)
	if err != nil {
		return result, &HTTPError{Code: HTTPErrorInvalidURL, Class: HTTPFailureDefinitive, Err: err}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requestURL := sanitizeHTTPURL(u, announce)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return result, &HTTPError{Code: HTTPErrorRequest, Class: HTTPFailureDefinitive, Err: err}
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), c.timeout)
	request = request.WithContext(requestCtx)
	defer cancel()

	var transmitted atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			transmitted.Store(true)
		}
	}}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))

	client := *c.client
	previousRedirect := client.CheckRedirect
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		// Sanitize before and after an injected callback. This keeps both the
		// request sent to a custom callback and the final redirect authoritative.
		next.URL = sanitizeHTTPURL(next.URL, announce)
		next.RequestURI = ""
		if previousRedirect != nil {
			if err := previousRedirect(next, via); err != nil {
				return err
			}
		}
		next.URL = sanitizeHTTPURL(next.URL, announce)
		next.RequestURI = ""
		return nil
	}

	response, err := client.Do(request)
	if response != nil {
		// A response means that the request was accepted and sent even if the
		// transport did not provide a WroteRequest trace (for example, a test
		// RoundTripper).
		transmitted.Store(true)
	}
	result.Transmitted = transmitted.Load()
	if err != nil {
		return result, httpTransportError(err, result.Transmitted, request.Context())
	}
	if response == nil {
		return result, &HTTPError{Code: HTTPErrorResponse, Class: HTTPFailureTransient, Transmitted: result.Transmitted, Err: errors.New("HTTP tracker returned no response")}
	}
	defer response.Body.Close()
	body, readErr := readHTTPBody(response.Body)
	if readErr != nil {
		return result, &HTTPError{Code: bodyErrorCode(readErr), Class: HTTPFailureTransient, StatusCode: response.StatusCode, Transmitted: result.Transmitted, Err: readErr}
	}

	parsed, parseErr := parseHTTPAnnounceResponse(body)
	if parseErr != nil {
		if e, ok := parseErr.(*HTTPError); ok {
			e.StatusCode = response.StatusCode
			e.Transmitted = result.Transmitted
			if response.StatusCode >= 400 && response.StatusCode < 500 && e.Class == HTTPFailureTransient && (e.Code != HTTPErrorTracker || e.RetryAfter == 0) {
				// A non-BEP31 client error is definitive. A decoded tracker
				// failure remains governed by its retry-in value.
				e.Class = HTTPFailureDefinitive
			}
			return result, e
		}
		return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, StatusCode: response.StatusCode, Transmitted: result.Transmitted, Err: parseErr}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		class := HTTPFailureTransient
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			class = HTTPFailureDefinitive
		}
		return result, &HTTPError{Code: HTTPErrorStatus, Class: class, StatusCode: response.StatusCode, Transmitted: result.Transmitted, Err: errors.New("tracker HTTP status is not successful")}
	}
	parsed.Transmitted = result.Transmitted
	return parsed, nil
}

func parseHTTPURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" {
		return nil, ErrHTTPInvalidURL
	}
	return u, nil
}

// sanitizeHTTPURL removes all authoritative values from one URL query and
// appends exactly one value for each announce field. Unrelated raw query
// tokens, including duplicate values and their original escaping, survive
// unchanged. This function is reused for every redirect target.
func sanitizeHTTPURL(input *url.URL, announce AnnounceRequest) *url.URL {
	u := *input
	parts := make([]string, 0, strings.Count(input.RawQuery, "&")+11)
	if input.RawQuery != "" {
		for _, part := range strings.Split(input.RawQuery, "&") {
			key := part
			if index := strings.IndexByte(key, '='); index >= 0 {
				key = key[:index]
			}
			decoded, err := url.QueryUnescape(key)
			if err == nil && isOwnedHTTPParameter(decoded) {
				continue
			}
			parts = append(parts, part)
		}
	}
	parts = append(parts,
		"info_hash="+escapeBinary(announce.InfoHash[:]),
		"peer_id="+escapeBinary(announce.PeerID[:]),
		"port="+strconv.FormatUint(uint64(announce.Port), 10),
		"uploaded="+strconv.FormatInt(announce.Uploaded, 10),
		"downloaded="+strconv.FormatInt(announce.Downloaded, 10),
		"left="+strconv.FormatInt(announce.Left, 10),
		"compact=1",
		"key="+strconv.FormatUint(uint64(announce.Key), 10),
		"numwant="+strconv.FormatInt(int64(announce.NumWant), 10),
	)
	if announce.Event != EventNone {
		parts = append(parts, "event="+eventName(announce.Event))
	}
	u.RawQuery = strings.Join(parts, "&")
	return &u
}

func isOwnedHTTPParameter(key string) bool {
	switch key {
	case "info_hash", "peer_id", "port", "uploaded", "downloaded", "left", "event", "compact", "key", "numwant", "ip", "ipv4", "ipv6":
		return true
	default:
		return false
	}
}

func eventName(event Event) string {
	switch event {
	case EventStarted:
		return "started"
	case EventCompleted:
		return "completed"
	case EventStopped:
		return "stopped"
	default:
		return ""
	}
}

func escapeBinary(value []byte) string {
	const hex = "0123456789ABCDEF"
	var result strings.Builder
	result.Grow(len(value) * 3)
	for _, b := range value {
		result.WriteByte('%')
		result.WriteByte(hex[b>>4])
		result.WriteByte(hex[b&0x0f])
	}
	return result.String()
}

func readHTTPBody(body io.Reader) ([]byte, error) {
	limited := io.LimitReader(body, int64(limits.HTTPResponseBytes)+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, &httpBodyError{err: err}
	}
	if len(data) > limits.HTTPResponseBytes {
		return nil, &httpBodyError{limit: true}
	}
	return data, nil
}

type httpBodyError struct {
	err   error
	limit bool
}

func (e *httpBodyError) Error() string {
	if e.limit {
		return "HTTP tracker response exceeds size limit"
	}
	return "HTTP tracker response read failed: " + e.err.Error()
}

func (e *httpBodyError) Unwrap() error { return e.err }

func bodyErrorCode(err error) HTTPErrorCode {
	var bodyErr *httpBodyError
	if errors.As(err, &bodyErr) && bodyErr.limit {
		return HTTPErrorBodyLimit
	}
	return HTTPErrorResponse
}

func httpTransportError(err error, transmitted bool, ctx context.Context) *HTTPError {
	if ctxErr := ctx.Err(); ctxErr != nil {
		code := HTTPErrorCanceled
		wrapped := ctxErr
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			code = HTTPErrorTimeout
			wrapped = ErrHTTPTimeout
		}
		return &HTTPError{Code: code, Class: HTTPFailureTransient, Transmitted: transmitted, Err: wrapped}
	}
	return &HTTPError{Code: HTTPErrorRequest, Class: HTTPFailureTransient, Transmitted: transmitted, Err: err}
}

func parseHTTPAnnounceResponse(body []byte) (HTTPAnnounceResult, error) {
	var result HTTPAnnounceResult
	value, err := bencode.DecodeWithLimits(body, bencode.Limits{MaxBytes: limits.HTTPResponseBytes})
	if err != nil {
		return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: err}
	}
	if value.Type != bencode.Dictionary {
		return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker response is not a dictionary")}
	}
	if failure, ok := dictionaryValue(value, "failure reason"); ok {
		if failure.Type != bencode.Bytes {
			return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker failure reason is not a string")}
		}
		retry, retryErr := parseRetryIn(value)
		if retryErr != nil {
			return result, retryErr
		}
		e := &HTTPError{Code: HTTPErrorTracker, Class: HTTPFailureTransient, Err: errors.New("tracker reported failure")}
		if retry == retryNever {
			e.Class = HTTPFailureNever
		} else if retry > 0 {
			e.RetryAfter = retry
		}
		return result, e
	}

	intervalValue, ok := dictionaryValue(value, "interval")
	if !ok || intervalValue.Type != bencode.Integer || intervalValue.Int < limits.MinTrackerSeconds || intervalValue.Int > limits.MaxTrackerSeconds {
		return result, &HTTPError{Code: HTTPErrorInterval, Class: HTTPFailureInvalidDelay, Err: errors.New("tracker interval is outside supported range")}
	}
	peersValue, ok := dictionaryValue(value, "peers")
	if !ok {
		return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker response has no peers")}
	}
	peers, err := parseHTTPPeers(peersValue)
	if err != nil {
		return result, err
	}
	if peers6, ok := dictionaryValue(value, "peers6"); ok {
		if peers6.Type != bencode.Bytes {
			return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peers6 is not compact")}
		}
		more, parseErr := parseCompactPeers(peers6.Bytes, 16)
		if parseErr != nil {
			return result, parseErr
		}
		peers = appendUniqueHTTPPeers(peers, more...)
	}
	result.Interval = time.Duration(intervalValue.Int) * time.Second
	result.Peers = peers
	if leechers, ok := dictionaryValue(value, "leechers"); ok {
		result.Leechers, err = parseCounter(leechers)
		if err != nil {
			return result, err
		}
	}
	if seeders, ok := dictionaryValue(value, "complete"); ok {
		result.Seeders, err = parseCounter(seeders)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

const retryNever = time.Duration(-1)

func parseRetryIn(value bencode.Value) (time.Duration, error) {
	retry, ok := dictionaryValue(value, "retry in")
	if !ok {
		return 0, nil
	}
	if retry.Type == bencode.Bytes && string(retry.Bytes) == "never" {
		return retryNever, nil
	}
	var minutes int64
	switch retry.Type {
	case bencode.Integer:
		minutes = retry.Int
	case bencode.Bytes:
		if len(retry.Bytes) == 0 {
			return 0, invalidRetryDelay()
		}
		for _, c := range retry.Bytes {
			if c < '0' || c > '9' {
				return 0, invalidRetryDelay()
			}
			if minutes > (math.MaxInt64-int64(c-'0'))/10 {
				return 0, invalidRetryDelay()
			}
			minutes = minutes*10 + int64(c-'0')
		}
	default:
		return 0, invalidRetryDelay()
	}
	maxMinutes := int64(limits.MaxTrackerSeconds) / 60
	if minutes < 1 || minutes > maxMinutes {
		return 0, invalidRetryDelay()
	}
	return time.Duration(minutes) * time.Minute, nil
}

func invalidRetryDelay() error {
	return &HTTPError{Code: HTTPErrorRetryDelay, Class: HTTPFailureInvalidDelay, Err: errors.New("tracker retry delay is outside supported range")}
}

func parseCounter(value bencode.Value) (uint32, error) {
	if value.Type != bencode.Integer || value.Int < 0 || value.Int > math.MaxUint32 {
		return 0, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker counter is invalid")}
	}
	return uint32(value.Int), nil
}

func dictionaryValue(value bencode.Value, key string) (bencode.Value, bool) {
	for _, entry := range value.Dict {
		if string(entry.Key) == key {
			return entry.Value, true
		}
	}
	return bencode.Value{}, false
}

func parseHTTPPeers(value bencode.Value) ([]HTTPPeer, error) {
	switch value.Type {
	case bencode.Bytes:
		return parseCompactPeers(value.Bytes, 4)
	case bencode.List:
		peers := make([]HTTPPeer, 0, min(len(value.List), limits.Candidates))
		for _, item := range value.List {
			if len(peers) >= limits.Candidates {
				break
			}
			if item.Type != bencode.Dictionary {
				return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer is not a dictionary")}
			}
			ipValue, ok := dictionaryValue(item, "ip")
			if !ok || ipValue.Type != bencode.Bytes {
				return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has no IP")}
			}
			portValue, ok := dictionaryValue(item, "port")
			if !ok || portValue.Type != bencode.Integer || portValue.Int < 1 || portValue.Int > math.MaxUint16 {
				return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid port")}
			}
			host := string(ipValue.Bytes)
			if !validDictionaryPeerHost(host) {
				return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid IP")}
			}
			peer := HTTPPeer{Host: host, Port: uint16(portValue.Int)}
			if idValue, hasID := dictionaryValue(item, "peer id"); hasID {
				if idValue.Type != bencode.Bytes || len(idValue.Bytes) != len(peer.PeerID) {
					return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid peer ID")}
				}
				copy(peer.PeerID[:], idValue.Bytes)
				peer.HasID = true
			}
			peers = appendUniqueHTTPPeers(peers, peer)
		}
		return peers, nil
	default:
		return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peers has invalid type")}
	}
}

func parseCompactPeers(value []byte, addressBytes int) ([]HTTPPeer, error) {
	stride := addressBytes + 2
	if len(value)%stride != 0 {
		return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("compact peer list has incomplete endpoint")}
	}
	count := len(value) / stride
	if count > limits.Candidates {
		count = limits.Candidates
	}
	peers := make([]HTTPPeer, 0, count)
	for offset, retained := 0, 0; offset < len(value) && retained < count; offset, retained = offset+stride, retained+1 {
		var addr netip.Addr
		if addressBytes == 4 {
			var raw [4]byte
			copy(raw[:], value[offset:offset+4])
			addr = netip.AddrFrom4(raw)
		} else {
			var raw [16]byte
			copy(raw[:], value[offset:offset+16])
			addr = netip.AddrFrom16(raw)
		}
		port := int(value[offset+addressBytes])<<8 | int(value[offset+addressBytes+1])
		if port == 0 || addr.IsUnspecified() || addr.IsMulticast() {
			return nil, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("invalid compact peer endpoint")}
		}
		peers = appendUniqueHTTPPeers(peers, HTTPPeer{Host: addr.String(), Port: uint16(port)})
	}
	return peers, nil
}

func appendUniqueHTTPPeers(dst []HTTPPeer, src ...HTTPPeer) []HTTPPeer {
	seen := make(map[string]int, len(dst)+len(src))
	for i, peer := range dst {
		seen[peer.Host+"\x00"+strconv.Itoa(int(peer.Port))] = i
	}
	for _, peer := range src {
		key := peer.Host + "\x00" + strconv.Itoa(int(peer.Port))
		if index, ok := seen[key]; ok {
			// Preserve the first endpoint while retaining an ID learned from a
			// later dictionary record if the first record omitted it.
			if !dst[index].HasID && peer.HasID {
				dst[index].PeerID = peer.PeerID
				dst[index].HasID = true
			}
			continue
		}
		seen[key] = len(dst)
		dst = append(dst, peer)
	}
	return dst
}

func validDictionaryPeerHost(host string) bool {
	if len(host) == 0 || len(host) > 255 {
		return false
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.IsValid() && !addr.IsUnspecified() && !addr.IsMulticast()
	}
	for _, c := range host {
		if c <= ' ' || c == 127 || c > 126 || strings.ContainsRune("[]/\\%:", c) {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
