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

const statusInterval = time.Second

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
	if r == nil || !r.statusEnabled() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if r.status.shown && now.Sub(r.status.lastTime) < statusInterval {
		return nil
	}
	if snapshot.ActivePeers < 0 {
		snapshot.ActivePeers = 0
	}
	line := fmt.Sprintf("status: phase=%s verified=%d/%d bytes peers=%d rate=%d B/s",
		QuoteName(snapshot.Phase, r.maxBytes), snapshot.VerifiedSelectedBytes,
		snapshot.SelectedBytes, snapshot.ActivePeers, snapshot.RecentRateBytesPerSec)
	line = truncateString(line, r.maxBytes)
	if r.status.shown {
		if _, err := io.WriteString(r.stderr, "\r"+line+strings.Repeat(" ", maxInt(0, r.status.lastLen-len(line)))+"\r"); err != nil {
			return err
		}
	} else if _, err := io.WriteString(r.stderr, "\r"+line+"\r"); err != nil {
		return err
	}
	r.status.shown = true
	r.status.lastTime = now
	r.status.lastLen = len(line)
	return nil
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
	message := "failure: " + sanitizeError(err, r.maxBytes)
	if resumable {
		message += "; verified partial output remains resumable"
	}
	return r.Error("%s", message)
}

// SecondaryFailure records a shutdown or final-event diagnostic without
// replacing the primary result.
func (r *Reporter) SecondaryFailure(err error) error {
	return r.Error("shutdown: %s", sanitizeError(err, r.maxBytes))
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
		r.status = statusState{}
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
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return "<redacted-tracker>"
	}
	host := u.Hostname()
	if port := u.Port(); port != "" {
		host = net.JoinHostPort(host, port)
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
	var out strings.Builder
	// Replacements are always shorter than the input in the usual case, but
	// this cap prevents a hostile string from making the builder grow without
	// bound due to scanner mistakes.
	if len(value) > DefaultDiagnosticBytes*16 {
		value = value[:DefaultDiagnosticBytes*16]
	}
	for i := 0; i < len(value); {
		if scheme, ok := sensitiveSchemeAt(value, i); ok {
			end := tokenEnd(value, i)
			token := value[i:end]
			if scheme == "magnet" {
				out.WriteString("magnet:<redacted>")
			} else {
				out.WriteString(RedactTrackerURL(token))
			}
			i = end
			continue
		}
		_, size := utf8.DecodeRuneInString(value[i:])
		if size == 0 {
			size = 1
		}
		out.WriteString(value[i : i+size])
		i += size
	}
	return out.String()
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

func tokenEnd(value string, start int) int {
	for i := start; i < len(value); i++ {
		switch value[i] {
		case ' ', '\t', '\r', '\n', '\'', '"', '<', '>', '(', ')', '[', ']', '{', '}', ',', ';':
			return i
		}
	}
	return len(value)
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
