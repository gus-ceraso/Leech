package torrent

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// DefaultTracker is included in every source's tracker set.
const DefaultTracker = "http://tracker.opentrackr.org:1337/announce"

// SourceKind identifies the three source forms accepted by the CLI.
type SourceKind uint8

const (
	SourcePath SourceKind = iota
	SourceMagnet
	SourceInfoHash
)

// Source is a side-effect-free, classified CLI source. Exactly one of Path,
// InfoHash, or Magnet is meaningful according to Kind.
type Source struct {
	Kind     SourceKind
	Path     string
	InfoHash InfoHash
	Magnet   *Magnet
	Trackers []string
}

// Magnet contains the useful, bounded values from a v1 magnet URI. Trackers
// always includes DefaultTracker, and Selection keeps BEP 53 ranges compact.
type Magnet struct {
	InfoHash    InfoHash
	DisplayName string
	Trackers    []string
	Peers       []PeerAddress
	Selection   []IndexRange
}

// PeerAddress is a syntactically valid magnet x.pe endpoint. Host may be a
// DNS name or an IP literal; resolution is deliberately a later boundary.
type PeerAddress struct {
	Host string
	Port uint16
}

// IndexRange is an inclusive BEP 53 file-index range. Keeping ranges instead
// of expanding them prevents a small URI from causing unbounded work.
type IndexRange struct {
	Start int
	End   int
}

// FileIndexRange is a descriptive alias used by selection code.
type FileIndexRange = IndexRange

// ParseSource classifies raw according to the CLI source rules. It performs no
// filesystem or network operations.
func ParseSource(raw string) (Source, error) {
	if raw == "" {
		return Source{}, fmt.Errorf("source is empty")
	}
	if len(raw) >= len("magnet:") && strings.EqualFold(raw[:len("magnet:")], "magnet:") {
		magnet, err := ParseMagnet(raw)
		if err != nil {
			return Source{}, err
		}
		return Source{Kind: SourceMagnet, InfoHash: magnet.InfoHash, Magnet: &magnet, Trackers: append([]string(nil), magnet.Trackers...)}, nil
	}
	if hash, recognized, err := parseHashShape(raw); recognized {
		if err != nil {
			return Source{}, err
		}
		trackers, trackerErr := TrackersWithDefault(nil)
		if trackerErr != nil {
			return Source{}, trackerErr
		}
		return Source{Kind: SourceInfoHash, InfoHash: hash, Trackers: trackers}, nil
	}
	if looksLikeUnsupportedScheme(raw) {
		return Source{}, fmt.Errorf("unsupported source scheme")
	}
	if raw == "-" {
		return Source{}, fmt.Errorf("source '-' is unsupported")
	}
	trackers, err := TrackersWithDefault(nil)
	if err != nil {
		return Source{}, err
	}
	return Source{Kind: SourcePath, Path: raw, Trackers: trackers}, nil
}

// Parse is a short spelling for ParseSource.
func Parse(raw string) (Source, error) { return ParseSource(raw) }

// ParseInfoHash decodes a 40-character hexadecimal or 32-character unpadded
// Base32 v1 info hash.
func ParseInfoHash(raw string) (InfoHash, error) {
	hash, recognized, err := parseHashShape(raw)
	if err != nil {
		return InfoHash{}, err
	}
	if !recognized {
		return InfoHash{}, fmt.Errorf("info hash must be 40 hexadecimal or 32 Base32 characters")
	}
	return hash, nil
}

// DecodeInfoHash is an alias for ParseInfoHash.
func DecodeInfoHash(raw string) (InfoHash, error) { return ParseInfoHash(raw) }

func parseHashShape(raw string) (InfoHash, bool, error) {
	switch len(raw) {
	case hex.EncodedLen(len(InfoHash{})):
		if !allHex(raw) {
			return InfoHash{}, false, nil
		}
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			return InfoHash{}, true, fmt.Errorf("invalid hexadecimal info hash: %w", err)
		}
		var hash InfoHash
		copy(hash[:], decoded)
		return hash, true, nil
	case base32NoPaddingEncodedLen:
		if !allBase32(raw) {
			return InfoHash{}, false, nil
		}
		decoded, err := base32NoPadding.DecodeString(strings.ToUpper(raw))
		if err != nil || len(decoded) != len(InfoHash{}) {
			if err == nil {
				err = fmt.Errorf("decoded length is %d", len(decoded))
			}
			return InfoHash{}, true, fmt.Errorf("invalid Base32 info hash: %w", err)
		}
		var hash InfoHash
		copy(hash[:], decoded)
		return hash, true, nil
	default:
		return InfoHash{}, false, nil
	}
}

const base32NoPaddingEncodedLen = 32

var base32NoPadding = base32.StdEncoding.WithPadding(base32.NoPadding)

func allHex(value string) bool {
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func allBase32(value string) bool {
	for _, c := range value {
		upper := unicode.ToUpper(c)
		if !(upper >= 'A' && upper <= 'Z' || upper >= '2' && upper <= '7') {
			return false
		}
	}
	return true
}

// ParseMagnet parses the BEP 9 magnet form supported by Leech. It rejects v2
// btmh topics even when a v1 btih topic is also present.
func ParseMagnet(raw string) (Magnet, error) {
	if len(raw) > limits.MetainfoBytes {
		return Magnet{}, fmt.Errorf("magnet URI exceeds %d-byte limit", limits.MetainfoBytes)
	}
	if len(raw) < len("magnet:") || !strings.EqualFold(raw[:len("magnet:")], "magnet:") {
		return Magnet{}, fmt.Errorf("source is not a magnet URI")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Magnet{}, fmt.Errorf("invalid magnet URI: %w", err)
	}
	if !strings.EqualFold(u.Scheme, "magnet") || u.Host != "" || u.Opaque != "" || u.Fragment != "" {
		return Magnet{}, fmt.Errorf("invalid magnet URI")
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Magnet{}, fmt.Errorf("invalid magnet query: %w", err)
	}

	var hash InfoHash
	foundHash := false
	for _, topic := range values["xt"] {
		lower := strings.ToLower(topic)
		switch {
		case strings.HasPrefix(lower, "urn:btmh"):
			return Magnet{}, fmt.Errorf("btmh magnet topics are unsupported")
		case strings.HasPrefix(lower, "urn:btih"):
			if !strings.HasPrefix(lower, "urn:btih:") {
				return Magnet{}, fmt.Errorf("malformed btih topic")
			}
			candidate, err := ParseInfoHash(topic[len("urn:btih:"):])
			if err != nil {
				return Magnet{}, fmt.Errorf("invalid btih topic: %w", err)
			}
			if foundHash && candidate != hash {
				return Magnet{}, fmt.Errorf("conflicting btih topics")
			}
			hash, foundHash = candidate, true
		}
	}
	if !foundHash {
		return Magnet{}, fmt.Errorf("magnet URI must contain one v1 btih topic")
	}

	magnet := Magnet{InfoHash: hash}
	if names := values["dn"]; len(names) > 0 {
		if len(names) != 1 {
			return Magnet{}, fmt.Errorf("magnet dn must appear at most once")
		}
		if len(names[0]) > limits.PathBytes {
			return Magnet{}, fmt.Errorf("magnet display name exceeds %d-byte limit", limits.PathBytes)
		}
		magnet.DisplayName = names[0]
	}

	trackers := values["tr"]
	for _, tracker := range trackers {
		if tracker == "" {
			return Magnet{}, fmt.Errorf("magnet contains an empty tracker")
		}
	}
	magnet.Trackers, err = TrackersWithDefault(trackers)
	if err != nil {
		return Magnet{}, err
	}

	peers := values["x.pe"]
	if len(peers) > limits.MagnetPeers {
		return Magnet{}, fmt.Errorf("magnet contains too many embedded peers")
	}
	for _, endpoint := range peers {
		peer, err := ParsePeerAddress(endpoint)
		if err != nil {
			return Magnet{}, fmt.Errorf("invalid magnet peer %q: %w", endpoint, err)
		}
		key := peerKey(peer)
		duplicate := false
		for _, existing := range magnet.Peers {
			if peerKey(existing) == key {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if len(magnet.Peers) >= limits.MagnetPeers {
			return Magnet{}, fmt.Errorf("magnet contains too many embedded peers")
		}
		magnet.Peers = append(magnet.Peers, peer)
	}

	selections := values["so"]
	if len(selections) > limits.Files {
		return Magnet{}, fmt.Errorf("magnet contains too many selections")
	}
	for _, selection := range selections {
		ranges, err := parseSelection(selection)
		if err != nil {
			return Magnet{}, err
		}
		if len(magnet.Selection)+len(ranges) > limits.Files {
			return Magnet{}, fmt.Errorf("magnet selection contains too many ranges")
		}
		magnet.Selection = append(magnet.Selection, ranges...)
	}
	return magnet, nil
}

// ParsePeerAddress validates a BEP 9 x.pe endpoint without resolving it.
func ParsePeerAddress(raw string) (PeerAddress, error) {
	if raw == "" || len(raw) > limits.PathBytes || raw != strings.TrimSpace(raw) {
		return PeerAddress{}, fmt.Errorf("endpoint must be hostname:port, IPv4:port, or [IPv6]:port")
	}
	host, portText, err := net.SplitHostPort(raw)
	if err != nil || host == "" || !decimalPort(portText) {
		return PeerAddress{}, fmt.Errorf("endpoint must be hostname:port, IPv4:port, or [IPv6]:port")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return PeerAddress{}, fmt.Errorf("endpoint port must be between 1 and 65535")
	}
	host = strings.ToLower(host)
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() || ip.IsMulticast() {
			return PeerAddress{}, fmt.Errorf("endpoint address is unspecified or multicast")
		}
	} else if !validPeerHostname(host) {
		return PeerAddress{}, fmt.Errorf("endpoint host is malformed")
	}
	return PeerAddress{Host: host, Port: uint16(port)}, nil
}

func decimalPort(value string) bool {
	if value == "" {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func validPeerHostname(host string) bool {
	if host == "" || strings.Contains(host, ":") {
		return false
	}
	for _, c := range host {
		if unicode.IsSpace(c) || unicode.IsControl(c) || strings.ContainsRune("/?#\\[]%", c) {
			return false
		}
	}
	return true
}

func peerKey(peer PeerAddress) string {
	return peer.Host + ":" + strconv.FormatUint(uint64(peer.Port), 10)
}

func parseSelection(raw string) ([]IndexRange, error) {
	if raw == "" || len(raw) > limits.MetainfoBytes {
		return nil, fmt.Errorf("magnet so selection is empty or too large")
	}
	if strings.Count(raw, ",") >= limits.Files {
		return nil, fmt.Errorf("magnet selection contains too many ranges")
	}
	parts := strings.Split(raw, ",")
	ranges := make([]IndexRange, 0, len(parts))
	for _, part := range parts {
		if part == "" || strings.Count(part, "-") > 1 {
			return nil, fmt.Errorf("malformed magnet so selection")
		}
		bounds := strings.Split(part, "-")
		start, err := parseIndex(bounds[0])
		if err != nil {
			return nil, err
		}
		end := start
		if len(bounds) == 2 {
			end, err = parseIndex(bounds[1])
			if err != nil {
				return nil, err
			}
			if end < start {
				return nil, fmt.Errorf("magnet so range is reversed")
			}
		}
		ranges = append(ranges, IndexRange{Start: start, End: end})
	}
	return ranges, nil
}

func parseIndex(raw string) (int, error) {
	if raw == "" || (len(raw) > 1 && raw[0] == '+') {
		return 0, fmt.Errorf("malformed magnet so index")
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("malformed magnet so index")
		}
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value >= uint64(limits.Files) {
		return 0, fmt.Errorf("magnet so index exceeds supported file range")
	}
	return int(value), nil
}

// TrackersWithDefault validates, deduplicates, and prepends the mandatory
// default tracker. It preserves the first occurrence of each URL.
func TrackersWithDefault(trackers []string) ([]string, error) {
	all := make([]string, 0, len(trackers)+1)
	all = append(all, DefaultTracker)
	all = append(all, trackers...)
	return NormalizeTrackers(all)
}

// NormalizeTrackers validates and deduplicates tracker URLs while preserving
// order. It does not add the default tracker; use TrackersWithDefault when
// building a source's complete tracker set.
func NormalizeTrackers(trackers []string) ([]string, error) {
	capacity := len(trackers)
	if capacity > limits.Trackers {
		capacity = limits.Trackers
	}
	result := make([]string, 0, capacity)
	seen := make(map[string]struct{}, len(trackers))
	for _, tracker := range trackers {
		normalized, err := normalizeTrackerURL(tracker)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		if len(result) >= limits.Trackers {
			return nil, fmt.Errorf("too many unique trackers")
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

// MergeTrackers combines tracker groups, prepends the default, and applies the
// same normalization used for magnets and metainfo.
func MergeTrackers(groups ...[]string) ([]string, error) {
	var trackers []string
	for _, group := range groups {
		trackers = append(trackers, group...)
	}
	return TrackersWithDefault(trackers)
}

// DeduplicateTrackers is a descriptive alias for NormalizeTrackers.
func DeduplicateTrackers(trackers []string) ([]string, error) { return NormalizeTrackers(trackers) }

func normalizeTrackerURL(raw string) (string, error) {
	if raw == "" || len(raw) > limits.MetainfoBytes {
		return "", fmt.Errorf("tracker URL is empty or too large")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.Opaque != "" || u.Fragment != "" {
		return "", fmt.Errorf("malformed tracker URL")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	switch u.Scheme {
	case "http", "https", "udp":
	default:
		return "", fmt.Errorf("unsupported tracker URL scheme %q", u.Scheme)
	}
	if u.Hostname() == "" || strings.ContainsAny(u.Hostname(), "\x00\r\n") {
		return "", fmt.Errorf("malformed tracker URL")
	}
	if err := validateURLPort(u.Host); err != nil {
		return "", fmt.Errorf("malformed tracker URL: %w", err)
	}
	u.Host = strings.ToLower(u.Host)
	return u.String(), nil
}

func validateURLPort(host string) error {
	if strings.HasPrefix(host, "[") {
		close := strings.IndexByte(host, ']')
		if close < 0 {
			return fmt.Errorf("invalid bracketed host")
		}
		rest := host[close+1:]
		if rest == "" {
			return nil
		}
		if !strings.HasPrefix(rest, ":") || !decimalPort(rest[1:]) {
			return fmt.Errorf("invalid port")
		}
		return portInRange(rest[1:])
	}
	if strings.Count(host, ":") == 0 {
		return nil
	}
	if strings.Count(host, ":") > 1 {
		return fmt.Errorf("IPv6 hosts must be bracketed")
	}
	port := host[strings.LastIndexByte(host, ':')+1:]
	if !decimalPort(port) {
		return fmt.Errorf("invalid port")
	}
	return portInRange(port)
}

func portInRange(port string) error {
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
}

func looksLikeUnsupportedScheme(raw string) bool {
	colon := strings.IndexByte(raw, ':')
	if colon <= 0 {
		return false
	}
	for i, c := range raw[:colon] {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
				return false
			}
			continue
		}
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
