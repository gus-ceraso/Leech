package cli

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// DefaultDiagnosticBytes bounds one rendered diagnostic.  Diagnostics are
// deliberately short: they are useful to a person at the command line, and
// their inputs are not trusted to be small.
const DefaultDiagnosticBytes = 4096

// maxRedactableURLBytes matches the supported 64 MiB tracker-URL input bound.
const maxRedactableURLBytes = 64 << 20

const (
	maxRedactionScanBytes = maxRedactableURLBytes + DefaultDiagnosticBytes
	maxTrackerHostBytes   = 1024
	statusInterval        = time.Second
)

// ReporterOptions contains the seams needed by the CLI and its deterministic
// presentation tests. A nil IsTerminal uses the small os.File check below;
// no terminal package or terminal control protocol is required.
type ReporterOptions struct {
	Level              LogLevel
	Stderr             io.Writer
	IsTerminal         func(io.Writer) bool
	Now                func() time.Time
	MaxDiagnosticBytes int
}

// Reporter writes bounded, level-filtered diagnostics to standard error. It
// never writes to standard output, which remains reserved for file listings.
// Reporter is safe for concurrent session workers to use.
type Reporter struct {
	mu          sync.Mutex
	nowMu       sync.Mutex
	level       LogLevel
	stderr      io.Writer
	isTerminal  func(io.Writer) bool
	now         func() time.Time
	maxBytes    int
	interactive bool
	status      statusState
}

type statusState struct {
	shown    bool
	updated  bool
	lastTime time.Time
	lastLen  int
}

// NewReporter creates a reporter using the default terminal detector and
// diagnostic bound.
func NewReporter(level LogLevel, stderr io.Writer) *Reporter {
	return NewReporterWithOptions(ReporterOptions{Level: level, Stderr: stderr})
}

// NewReporterWithOptions creates a reporter with injectable clock and
// terminal state. An invalid or empty level uses warning, the CLI default.
func NewReporterWithOptions(options ReporterOptions) *Reporter {
	level := options.Level
	if !validLogLevel(level) {
		level = LogWarning
	}
	writer := options.Stderr
	if writer == nil {
		writer = io.Discard
	}
	terminal := options.IsTerminal
	if terminal == nil {
		terminal = isTerminalWriter
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	maxBytes := options.MaxDiagnosticBytes
	if maxBytes <= 0 || maxBytes > DefaultDiagnosticBytes {
		maxBytes = DefaultDiagnosticBytes
	}
	return &Reporter{
		level:       level,
		stderr:      writer,
		isTerminal:  terminal,
		now:         now,
		maxBytes:    maxBytes,
		interactive: terminal(writer),
	}
}

// Level returns the configured log level.
func (r *Reporter) Level() LogLevel {
	if r == nil {
		return LogWarning
	}
	return r.level
}

// Log writes one permanent line when level is enabled. Format arguments are
// rendered first and then made safe for a terminal; callers should use
// QuoteName for untrusted names when they need quotation as well as escaping.
func (r *Reporter) Log(level LogLevel, format string, args ...any) error {
	if r == nil || !r.enabled(level) {
		return nil
	}
	message := SanitizeDiagnostic(fmt.Sprintf(format, args...), r.maxBytes)
	return r.writePermanent(level, message)
}

// Debug writes a debug diagnostic.
func (r *Reporter) Debug(format string, args ...any) error {
	return r.Log(LogDebug, format, args...)
}

// Info writes an informational diagnostic.
func (r *Reporter) Info(format string, args ...any) error {
	return r.Log(LogInfo, format, args...)
}

// Warning writes a warning diagnostic.
func (r *Reporter) Warning(format string, args ...any) error {
	return r.Log(LogWarning, format, args...)
}

// Warn is a short spelling for Warning.
func (r *Reporter) Warn(format string, args ...any) error {
	return r.Warning(format, args...)
}

// Error writes an error diagnostic.
func (r *Reporter) Error(format string, args ...any) error {
	return r.Log(LogError, format, args...)
}

// Phase reports a permanent phase transition. Phase names are quoted because
// they are session-controlled input at this boundary.
func (r *Reporter) Phase(phase string) error {
	return r.Info("phase: %s", QuoteName(phase, r.maxBytes))
}

// Tracker reports tracker-local diagnostics without exposing credentials,
// paths, or query parameters from the supplied URL.
func (r *Reporter) Tracker(level LogLevel, trackerURL string, format string, args ...any) error {
	if r == nil || !r.enabled(level) {
		return nil
	}
	message := "tracker " + RedactTrackerURL(trackerURL)
	if format != "" {
		message += ": " + fmt.Sprintf(format, args...)
	}
	return r.Log(level, "%s", message)
}

// Status is the bounded snapshot rendered by an interactive status line.
// Byte fields are selected-content bytes, not total-torrent bytes.
type Status struct {
	Phase                 string
	VerifiedSelectedBytes uint64
	SelectedBytes         uint64
	ActivePeers           int
	RecentRateBytesPerSec uint64
}

// Status renders one replaceable line at info/debug level on an interactive
// stderr. Calls made less than one second apart do nothing.
func (r *Reporter) Status(snapshot Status) error {
	_, err := r.renderStatus(snapshot)
	return err
}

func (r *Reporter) currentTime() time.Time {
	r.nowMu.Lock()
	defer r.nowMu.Unlock()
	return r.now()
}

func (r *Reporter) renderStatus(snapshot Status) (bool, error) {
	if r == nil {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renderStatusLocked(snapshot, r.currentTime())
}

func (r *Reporter) renderStatusAt(snapshot Status, now time.Time) (bool, error) {
	if r == nil {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renderStatusLocked(snapshot, now)
}

func (r *Reporter) renderStatusLocked(snapshot Status, now time.Time) (bool, error) {
	if !r.statusEnabled() {
		return false, nil
	}
	if r.status.updated && now.Sub(r.status.lastTime) < statusInterval {
		return false, nil
	}
	if snapshot.ActivePeers < 0 {
		snapshot.ActivePeers = 0
	}
	line := "status: phase=" + QuoteName(snapshot.Phase, r.maxBytes)
	if snapshot.Phase == "resume" || snapshot.Phase == "transfer" {
		line += fmt.Sprintf(" verified=%d/%d bytes", snapshot.VerifiedSelectedBytes, snapshot.SelectedBytes)
	}
	if snapshot.Phase == "transfer" {
		line += fmt.Sprintf(" peers=%d rate=%d B/s", snapshot.ActivePeers, snapshot.RecentRateBytesPerSec)
	}
	line = truncateString(line, r.maxBytes)
	if r.status.shown {
		if _, err := io.WriteString(r.stderr, "\r"+line+strings.Repeat(" ", maxInt(0, r.status.lastLen-len(line)))+"\r"); err != nil {
			return false, err
		}
	} else if _, err := io.WriteString(r.stderr, "\r"+line+"\r"); err != nil {
		return false, err
	}
	r.status.shown = true
	r.status.updated = true
	r.status.lastTime = now
	r.status.lastLen = len(line)
	return true, nil
}

// Result describes a successful terminal result. SelectionComplete and
// TorrentComplete are separate because a selected download may intentionally
// leave the rest of the torrent absent.
type Result struct {
	NoTransferNeeded  bool
	SelectionComplete bool
	TorrentComplete   bool
}

// ReportResult writes the notable successful result required by the CLI
// contract: a resumed complete output, a selected-only completion, or a full
// torrent completion.
func (r *Reporter) ReportResult(result Result) error {
	switch {
	case result.NoTransferNeeded:
		return r.Info("result: output is already complete; no transfer was needed")
	case result.SelectionComplete && !result.TorrentComplete:
		return r.Info("result: selected content is complete; torrent remains incomplete")
	case result.TorrentComplete:
		return r.Info("result: torrent is complete")
	case result.SelectionComplete:
		return r.Info("result: selected content is complete")
	default:
		return r.Info("result: download finished")
	}
}

// PrimaryFailure reports the primary session failure. If resumable is true,
// the same permanent line records that verified partial output is available
// for a later --resume run.
func (r *Reporter) PrimaryFailure(err error, resumable bool) error {
	if r == nil || !r.enabled(LogError) {
		return nil
	}
	message := "failure: " + sanitizeError(err, r.maxBytes)
	if resumable {
		message += "; verified partial output remains resumable"
	}
	return r.writePermanent(LogError, truncateString(message, r.maxBytes))
}

// SecondaryFailure records a shutdown or final-event diagnostic without
// replacing the primary result.
func (r *Reporter) SecondaryFailure(err error) error {
	if r == nil || !r.enabled(LogError) {
		return nil
	}
	return r.secondaryFailureText(sanitizeError(err, r.maxBytes))
}

// secondaryFailureText writes a message already redacted and bounded before
// retention by the CLI reporting owner.
func (r *Reporter) secondaryFailureText(message string) error {
	if r == nil || !r.enabled(LogError) {
		return nil
	}
	return r.writePermanent(LogError, truncateString("shutdown: "+message, r.maxBytes))
}

// PrivateIgnored emits the required warning for the deliberate BEP 27
// override.
func (r *Reporter) PrivateIgnored() error {
	return r.Warning("torrent declares private=1; treating the torrent as public")
}

func (r *Reporter) enabled(level LogLevel) bool {
	if r == nil || !validLogLevel(level) {
		return false
	}
	return logRank(level) >= logRank(r.level)
}

func (r *Reporter) statusEnabled() bool {
	return r != nil && r.interactive && (r.level == LogDebug || r.level == LogInfo)
}

func (r *Reporter) writePermanent(level LogLevel, message string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status.shown {
		if _, err := io.WriteString(r.stderr, "\r"+strings.Repeat(" ", r.status.lastLen)+"\r"); err != nil {
			return err
		}
		r.status.shown = false
		r.status.lastLen = 0
	}
	_, err := fmt.Fprintf(r.stderr, "%s: %s\n", level, message)
	return err
}

func validLogLevel(level LogLevel) bool {
	switch level {
	case LogDebug, LogInfo, LogWarning, LogError:
		return true
	default:
		return false
	}
}

func logRank(level LogLevel) int {
	switch level {
	case LogDebug:
		return 0
	case LogInfo:
		return 1
	case LogWarning:
		return 2
	case LogError:
		return 3
	default:
		return 4
	}
}

func isTerminalWriter(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	return isTTYFile(file)
}

// QuoteName quotes and escapes untrusted text for a permanent diagnostic.
// The optional bound is for internal use; callers may omit it.
func QuoteName(value string, bounds ...int) string {
	maxBytes := DefaultDiagnosticBytes
	if len(bounds) > 0 && bounds[0] > 0 {
		maxBytes = bounds[0]
	}
	return strconv.Quote(truncateString(redactSensitive(value), maxBytes))
}

// EscapeUntrusted makes arbitrary text safe to include in one stderr line.
// It preserves ordinary Unicode while escaping controls, invalid UTF-8, and
// terminal escape bytes.
func EscapeUntrusted(value string, bounds ...int) string {
	maxBytes := DefaultDiagnosticBytes
	if len(bounds) > 0 && bounds[0] > 0 {
		maxBytes = bounds[0]
	}
	return truncateString(escapeControls(redactSensitive(value)), maxBytes)
}

// SanitizeDiagnostic redacts URLs and complete magnet tokens, escapes control
// characters, and bounds the rendered diagnostic.
func SanitizeDiagnostic(value string, bounds ...int) string {
	maxBytes := DefaultDiagnosticBytes
	if len(bounds) > 0 && bounds[0] > 0 {
		maxBytes = bounds[0]
	}
	return truncateString(escapeControls(redactSensitive(value)), maxBytes)
}

// RedactTrackerURL returns only a URL's scheme and host (including an explicit
// port). Malformed or hostless input is represented by a fixed placeholder.
func RedactTrackerURL(raw string) string {
	separator := strings.Index(raw, "://")
	if separator <= 0 || separator > 32 {
		return "<redacted-tracker>"
	}
	scheme := raw[:separator]
	authorityStart := separator + 3
	authorityEnd := len(raw)
	if offset := strings.IndexAny(raw[authorityStart:], "/?#"); offset >= 0 {
		authorityEnd = authorityStart + offset
	}
	authority := raw[authorityStart:authorityEnd]
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		authority = authority[at+1:]
	}
	if authority == "" || len(authority) > maxTrackerHostBytes || strings.IndexFunc(authority, unicode.IsControl) >= 0 {
		return "<redacted-tracker>"
	}
	u, err := url.Parse(scheme + "://" + authority)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return "<redacted-tracker>"
	}
	host := u.Hostname()
	if port := u.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return strings.ToLower(u.Scheme) + "://" + host
}

func sanitizeError(err error, maxBytes int) string {
	if err == nil {
		return "<nil>"
	}
	return SanitizeDiagnostic(err.Error(), maxBytes)
}

func redactSensitive(value string) string {
	const outputLimit = DefaultDiagnosticBytes - 3
	var out strings.Builder
	out.Grow(DefaultDiagnosticBytes)
	for i, scanned := 0, 0; i < len(value); {
		if out.Len() >= outputLimit || scanned >= maxRedactionScanBytes {
			return out.String() + "..."
		}
		if scheme, ok := sensitiveSchemeAt(value, i); ok {
			tokenLimit := maxRedactableURLBytes
			if remaining := maxRedactionScanBytes - scanned; remaining < tokenLimit {
				tokenLimit = remaining
			}
			end, complete := sensitiveTokenEnd(value, i, tokenLimit)
			token := value[i:end]
			replacement := "magnet:<redacted>"
			if scheme != "magnet" {
				replacement = RedactTrackerURL(token)
			}
			// net/url.Error quotes its URL. Keep a closing quote and the
			// following formatting punctuation without exposing URL text.
			replacement += redactedTokenSuffix(token)
			if len(replacement) > outputLimit-out.Len() {
				return out.String() + "..."
			}
			out.WriteString(replacement)
			scanned += end - i
			i = end
			if !complete {
				return out.String() + "..."
			}
			continue
		}
		_, size := utf8.DecodeRuneInString(value[i:])
		if size == 0 {
			size = 1
		}
		if scanned+size > maxRedactionScanBytes || out.Len()+size > outputLimit {
			return out.String() + "..."
		}
		out.WriteString(value[i : i+size])
		i += size
		scanned += size
	}
	return out.String()
}

func redactedTokenSuffix(token string) string {
	for i := len(token) - 1; i >= 0; i-- {
		switch token[i] {
		case '"':
			return token[i:]
		case ':', ',', ')', ']', '}':
			continue
		default:
			return ""
		}
	}
	return ""
}

func sensitiveTokenEnd(value string, start, limit int) (int, bool) {
	for i := start; i < len(value); {
		r, size := utf8.DecodeRuneInString(value[i:])
		if size == 0 {
			size = 1
		}
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return i, true
		}
		if i-start >= limit {
			return i, false
		}
		i += size
	}
	return len(value), true
}

func sensitiveSchemeAt(value string, offset int) (string, bool) {
	for _, scheme := range []string{"magnet:", "http://", "https://", "udp://"} {
		if len(value)-offset < len(scheme) || !strings.EqualFold(value[offset:offset+len(scheme)], scheme) {
			continue
		}
		if offset > 0 {
			previous := value[offset-1]
			if (previous >= 'a' && previous <= 'z') || (previous >= 'A' && previous <= 'Z') || (previous >= '0' && previous <= '9') || previous == '_' || previous == '-' {
				continue
			}
		}
		if strings.EqualFold(scheme, "magnet:") {
			return "magnet", true
		}
		return strings.TrimSuffix(strings.ToLower(scheme), "://"), true
	}
	return "", false
}

func escapeControls(value string) string {
	var out strings.Builder
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && size == 1 {
			out.WriteString(fmt.Sprintf("\\x%02x", value[i]))
			i++
			continue
		}
		if unicode.IsControl(r) {
			switch r {
			case '\n':
				out.WriteString(`\n`)
			case '\r':
				out.WriteString(`\r`)
			case '\t':
				out.WriteString(`\t`)
			case '\b':
				out.WriteString(`\b`)
			case '\f':
				out.WriteString(`\f`)
			case '\v':
				out.WriteString(`\v`)
			default:
				if r <= 0xff {
					out.WriteString(fmt.Sprintf("\\x%02x", r))
				} else {
					out.WriteString(fmt.Sprintf("\\u%04x", r))
				}
			}
		} else {
			out.WriteString(value[i : i+size])
		}
		i += size
	}
	return out.String()
}

func truncateString(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	if maxBytes <= 3 {
		limit := maxBytes
		for limit > 0 && !utf8.RuneStart(value[limit]) {
			limit--
		}
		return value[:limit]
	}
	limit := maxBytes - 3
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + "..."
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
