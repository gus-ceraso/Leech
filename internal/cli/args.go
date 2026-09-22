// Package cli contains the side-effect-free command-line boundary.
package cli

import (
	"fmt"
	"strings"
	"time"
)

// UsageText is the short synopsis used for command-line errors.
const UsageText = "Usage: leech [options] SOURCE"

// HelpText is the complete help output. It deliberately describes only the
// public command-line contract; runtime behavior belongs to the session layer.
const HelpText = `Usage:
  leech [options] SOURCE

Options:
  -o, --output DIR
        Use DIR as the destination directory (default: current directory).
  -f, --file PATTERN
        Download matching content. May be repeated.
  -l, --list-files
        List selectable files without downloading them.
  -L, --loglevel LEVEL
        Set logging to debug, info, warning, or error (default: warning).
  -r, --resume
        Verify and reuse existing selected output (default: overwrite).
  -s, --stream
        Prioritize earlier pieces for playback.
  -t, --timeout DURATION
        Fail after DURATION with no newly verified file piece; not a total timeout.
  -h, --help
        Show help and exit.
`

// LogLevel is the command-line logging level. Values are intentionally plain
// strings because they are also used by the reporter and its tests.
type LogLevel string

const (
	LogDebug   LogLevel = "debug"
	LogInfo    LogLevel = "info"
	LogWarning LogLevel = "warning"
	LogError   LogLevel = "error"
)

// Options contains parsed command-line values. Source is kept as the original
// argument so source classification can happen at the torrent boundary without
// any CLI parsing side effects.
type Options struct {
	Source    string
	Output    string
	Files     []string
	ListFiles bool
	LogLevel  LogLevel
	Resume    bool
	Stream    bool
	Timeout   time.Duration
	Help      bool

	// The Set fields preserve whether an option appeared. Listing mode has
	// conflicts even when an option's value equals its default.
	OutputSet    bool
	FilesSet     bool
	ListFilesSet bool
	LogLevelSet  bool
	ResumeSet    bool
	StreamSet    bool
	TimeoutSet   bool
}

// Arguments is retained as a descriptive alias for callers that prefer the
// command-line terminology.
type Arguments = Options

// UsageError marks an invalid command line. Callers can map it to exit status
// 2 while preserving the concise message.
type UsageError struct {
	Message string
}

func (e *UsageError) Error() string { return e.Message }

func usageError(format string, args ...any) error {
	return &UsageError{Message: fmt.Sprintf(format, args...)}
}

// ParseArgs parses arguments after argv[0]. It performs no filesystem,
// network, or cache operations. A help request does not require a source, but
// all supplied options still have to be syntactically valid.
func ParseArgs(argv []string) (Options, error) {
	opts := Options{
		Output:   ".",
		LogLevel: LogWarning,
	}

	optionMode := true
	seenSource := false
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		if seenSource {
			return Options{}, usageError("more than one source or an option follows SOURCE")
		}

		if optionMode {
			if arg == "--" {
				optionMode = false
				continue
			}
			if arg == "-" {
				return Options{}, usageError("SOURCE '-' is unsupported")
			}
			if strings.HasPrefix(arg, "--") {
				if err := parseLongOption(arg, argv, &i, &opts); err != nil {
					return Options{}, err
				}
				continue
			}
			if strings.HasPrefix(arg, "-") {
				if err := parseShortOption(arg, argv, &i, &opts); err != nil {
					return Options{}, err
				}
				continue
			}
			optionMode = false
		}

		if arg == "-" {
			return Options{}, usageError("SOURCE '-' is unsupported")
		}
		opts.Source = arg
		seenSource = true
	}

	if !opts.Help && opts.Source == "" {
		return Options{}, usageError("exactly one SOURCE is required")
	}
	if opts.ListFiles && (opts.OutputSet || opts.FilesSet || opts.ResumeSet || opts.StreamSet || opts.TimeoutSet) {
		return Options{}, usageError("--list-files cannot be combined with --output, --file, --resume, --stream, or --timeout")
	}
	return opts, nil
}

// Parse is a short compatibility spelling for ParseArgs.
func Parse(argv []string) (Options, error) { return ParseArgs(argv) }

func parseLongOption(arg string, argv []string, index *int, opts *Options) error {
	name, inline, hasInline := arg, "", false
	if equal := strings.IndexByte(arg, '='); equal >= 0 {
		name, inline, hasInline = arg[:equal], arg[equal+1:], true
	}

	switch name {
	case "--help":
		if hasInline {
			return usageError("option %s does not take a value", name)
		}
		opts.Help = true
	case "--list-files":
		if hasInline {
			return usageError("option %s does not take a value", name)
		}
		opts.ListFiles = true
		opts.ListFilesSet = true
	case "--resume":
		if hasInline {
			return usageError("option %s does not take a value", name)
		}
		opts.Resume = true
		opts.ResumeSet = true
	case "--stream":
		if hasInline {
			return usageError("option %s does not take a value", name)
		}
		opts.Stream = true
		opts.StreamSet = true
	case "--output":
		value, err := optionValue(name, inline, hasInline, argv, index)
		if err != nil {
			return err
		}
		if value == "" {
			return usageError("option %s requires a nonempty value", name)
		}
		opts.Output, opts.OutputSet = value, true
	case "--file":
		value, err := optionValue(name, inline, hasInline, argv, index)
		if err != nil {
			return err
		}
		if value == "" {
			return usageError("option %s requires a nonempty value", name)
		}
		opts.Files = append(opts.Files, value)
		opts.FilesSet = true
	case "--loglevel":
		value, err := optionValue(name, inline, hasInline, argv, index)
		if err != nil {
			return err
		}
		level, err := parseLogLevel(value)
		if err != nil {
			return err
		}
		opts.LogLevel, opts.LogLevelSet = level, true
	case "--timeout":
		value, err := optionValue(name, inline, hasInline, argv, index)
		if err != nil {
			return err
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return usageError("option %s requires a positive Go duration", name)
		}
		opts.Timeout, opts.TimeoutSet = duration, true
	default:
		return usageError("unknown option %s", name)
	}
	return nil
}

func parseShortOption(arg string, argv []string, index *int, opts *Options) error {
	switch arg {
	case "-h":
		opts.Help = true
	case "-l":
		opts.ListFiles, opts.ListFilesSet = true, true
	case "-r":
		opts.Resume, opts.ResumeSet = true, true
	case "-s":
		opts.Stream, opts.StreamSet = true, true
	case "-o":
		value, err := shortValue(arg, argv, index)
		if err != nil {
			return err
		}
		if value == "" {
			return usageError("option %s requires a nonempty value", arg)
		}
		opts.Output, opts.OutputSet = value, true
	case "-f":
		value, err := shortValue(arg, argv, index)
		if err != nil {
			return err
		}
		if value == "" {
			return usageError("option %s requires a nonempty value", arg)
		}
		opts.Files = append(opts.Files, value)
		opts.FilesSet = true
	case "-L":
		value, err := shortValue(arg, argv, index)
		if err != nil {
			return err
		}
		level, err := parseLogLevel(value)
		if err != nil {
			return err
		}
		opts.LogLevel, opts.LogLevelSet = level, true
	case "-t":
		value, err := shortValue(arg, argv, index)
		if err != nil {
			return err
		}
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return usageError("option %s requires a positive Go duration", arg)
		}
		opts.Timeout, opts.TimeoutSet = duration, true
	default:
		return usageError("unknown or combined short option %s", arg)
	}
	return nil
}

func optionValue(name, inline string, hasInline bool, argv []string, index *int) (string, error) {
	if hasInline {
		return inline, nil
	}
	if *index+1 >= len(argv) || strings.HasPrefix(argv[*index+1], "-") {
		return "", usageError("option %s requires a value", name)
	}
	*index = *index + 1
	return argv[*index], nil
}

func shortValue(name string, argv []string, index *int) (string, error) {
	if *index+1 >= len(argv) || strings.HasPrefix(argv[*index+1], "-") {
		return "", usageError("option %s requires a value", name)
	}
	*index = *index + 1
	return argv[*index], nil
}

func parseLogLevel(value string) (LogLevel, error) {
	switch LogLevel(value) {
	case LogDebug, LogInfo, LogWarning, LogError:
		return LogLevel(value), nil
	default:
		return "", usageError("invalid log level %q", value)
	}
}
