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

// These limits admit 20,000 dictionary peers with an optional peer ID:
// 80,000 peer values and 60,000 peer dictionary entries, plus response fields.
// Smaller tracker bounds keep ignored response trees from using the much
// larger metainfo decoding budget.
const (
	trackerBencodeValues            = 100_000
	trackerBencodeDictionaryEntries = 64_000
	trackerBencodeContainerEntries  = limits.Candidates
)

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
		class := HTTPFailureTransient
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			class = HTTPFailureDefinitive
		}
		return result, &HTTPError{Code: bodyErrorCode(readErr), Class: class, StatusCode: response.StatusCode, Transmitted: result.Transmitted, Err: readErr}
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
	var query strings.Builder
	query.Grow(len(input.RawQuery) + 512)
	first := true
	for start := 0; input.RawQuery != "" && start <= len(input.RawQuery); {
		end := strings.IndexByte(input.RawQuery[start:], '&')
		if end < 0 {
			end = len(input.RawQuery)
		} else {
			end += start
		}
		part := input.RawQuery[start:end]
		key := part
		if end := strings.IndexByte(key, '='); end >= 0 {
			key = key[:end]
		}
		if !isOwnedRawHTTPParameter(key) {
			if !first {
				query.WriteByte('&')
			}
			query.WriteString(part)
			first = false
		}
		if end == len(input.RawQuery) {
			break
		}
		start = end + 1
	}
	fields := [...]string{
		"info_hash=" + escapeBinary(announce.InfoHash[:]),
		"peer_id=" + escapeBinary(announce.PeerID[:]),
		"port=" + strconv.FormatUint(uint64(announce.Port), 10),
		"uploaded=" + strconv.FormatInt(announce.Uploaded, 10),
		"downloaded=" + strconv.FormatInt(announce.Downloaded, 10),
		"left=" + strconv.FormatInt(announce.Left, 10),
		"compact=1",
		"key=" + strconv.FormatUint(uint64(announce.Key), 10),
		"numwant=" + strconv.FormatInt(int64(announce.NumWant), 10),
	}
	for _, field := range fields {
		if !first {
			query.WriteByte('&')
		}
		query.WriteString(field)
		first = false
	}
	if announce.Event != EventNone {
		query.WriteString("&event=")
		query.WriteString(eventName(announce.Event))
	}
	u.RawQuery = query.String()
	return &u
}

func isOwnedRawHTTPParameter(key string) bool {
	if len(key) > 3*len("downloaded") {
		return false
	}
	if strings.IndexByte(key, '%') < 0 {
		return isOwnedHTTPParameter(key)
	}
	decoded, err := url.QueryUnescape(key)
	return err == nil && isOwnedHTTPParameter(decoded)
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
	value, err := bencode.DecodeWithLimits(body, bencode.Limits{
		MaxBytes:             limits.HTTPResponseBytes,
		MaxValues:            trackerBencodeValues,
		MaxDictionaryEntries: trackerBencodeDictionaryEntries,
		MaxContainerEntries:  trackerBencodeContainerEntries,
		MaxDepth:             limits.BencodeDepth,
	})
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
	peers := newHTTPPeerSet(httpPeerCapacity(peersValue))
	if err := parseHTTPPeers(peersValue, peers); err != nil {
		return result, err
	}
	if peers6, ok := dictionaryValue(value, "peers6"); ok {
		if peers6.Type != bencode.Bytes {
			return result, &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peers6 is not compact")}
		}
		if err := parseCompactPeers(peers6.Bytes, 16, peers); err != nil {
			return result, err
		}
	}
	result.Interval = time.Duration(intervalValue.Int) * time.Second
	result.Peers = peers.peers
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

type httpPeerEndpoint struct {
	host string
	port uint16
}

// httpPeerSet deduplicates every peer representation in one tracker response.
type httpPeerSet struct {
	peers []HTTPPeer
	seen  map[httpPeerEndpoint]int
}

func newHTTPPeerSet(capacity int) *httpPeerSet {
	if capacity > limits.Candidates {
		capacity = limits.Candidates
	}
	return &httpPeerSet{
		peers: make([]HTTPPeer, 0, capacity),
		seen:  make(map[httpPeerEndpoint]int, capacity),
	}
}

func (set *httpPeerSet) append(peer HTTPPeer) {
	key := httpPeerEndpoint{host: peer.Host, port: peer.Port}
	if index, ok := set.seen[key]; ok {
		// Preserve the first endpoint while retaining an ID learned from a
		// later dictionary record if the first record omitted it.
		if !set.peers[index].HasID && peer.HasID {
			set.peers[index].PeerID = peer.PeerID
			set.peers[index].HasID = true
		}
		return
	}
	if len(set.peers) >= limits.Candidates {
		return
	}
	set.seen[key] = len(set.peers)
	set.peers = append(set.peers, peer)
}

func httpPeerCapacity(value bencode.Value) int {
	var count int
	switch value.Type {
	case bencode.Bytes:
		count = len(value.Bytes) / 6
	case bencode.List:
		count = len(value.List)
	}
	return min(count, limits.Candidates)
}

func parseHTTPPeers(value bencode.Value, peers *httpPeerSet) error {
	switch value.Type {
	case bencode.Bytes:
		return parseCompactPeers(value.Bytes, 4, peers)
	case bencode.List:
		for _, item := range value.List {
			if len(peers.peers) >= limits.Candidates {
				break
			}
			if item.Type != bencode.Dictionary {
				return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer is not a dictionary")}
			}
			ipValue, ok := dictionaryValue(item, "ip")
			if !ok || ipValue.Type != bencode.Bytes {
				return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has no IP")}
			}
			portValue, ok := dictionaryValue(item, "port")
			if !ok || portValue.Type != bencode.Integer || portValue.Int < 1 || portValue.Int > math.MaxUint16 {
				return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid port")}
			}
			host := string(ipValue.Bytes)
			if !validDictionaryPeerHost(host) {
				return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid IP")}
			}
			peer := HTTPPeer{Host: host, Port: uint16(portValue.Int)}
			if idValue, hasID := dictionaryValue(item, "peer id"); hasID {
				if idValue.Type != bencode.Bytes || len(idValue.Bytes) != len(peer.PeerID) {
					return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peer has invalid peer ID")}
				}
				copy(peer.PeerID[:], idValue.Bytes)
				peer.HasID = true
			}
			peers.append(peer)
		}
		return nil
	default:
		return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("tracker peers has invalid type")}
	}
}

func parseCompactPeers(value []byte, addressBytes int, peers *httpPeerSet) error {
	stride := addressBytes + 2
	if len(value)%stride != 0 {
		return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("compact peer list has incomplete endpoint")}
	}
	count := len(value) / stride
	if count > limits.Candidates {
		count = limits.Candidates
	}
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
			return &HTTPError{Code: HTTPErrorMalformed, Class: HTTPFailureTransient, Err: errors.New("invalid compact peer endpoint")}
		}
		peers.append(HTTPPeer{Host: addr.String(), Port: uint16(port)})
	}
	return nil
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
