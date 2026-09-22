package cli

import (
	"testing"
	"time"
)

func TestParseArgsDefaults(t *testing.T) {
	opts, err := ParseArgs([]string{"source.torrent"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Source != "source.torrent" || opts.Output != "." || opts.LogLevel != LogWarning || opts.Resume || opts.Stream || opts.Timeout != 0 {
		t.Fatalf("unexpected defaults: %#v", opts)
	}
}

func TestParseArgsAllForms(t *testing.T) {
	opts, err := ParseArgs([]string{
		"--output=/srv/media", "-f", "one/*", "--file", "two?.mkv",
		"-L", "debug", "--resume", "-s", "--timeout=1h30m", "source.torrent",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Output != "/srv/media" || len(opts.Files) != 2 || opts.ListFiles || opts.LogLevel != LogDebug || !opts.Resume || !opts.Stream || opts.Timeout != 90*time.Minute {
		t.Fatalf("unexpected options: %#v", opts)
	}
	if !opts.OutputSet || !opts.FilesSet || opts.ListFilesSet || !opts.LogLevelSet || !opts.ResumeSet || !opts.StreamSet || !opts.TimeoutSet {
		t.Fatalf("presence tracking lost: %#v", opts)
	}
}

func TestParseArgsList(t *testing.T) {
	opts, err := ParseArgs([]string{"-l", "-L", "warning", "source.torrent"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.ListFiles || opts.LogLevel != LogWarning {
		t.Fatalf("unexpected listing options: %#v", opts)
	}
}

func TestParseArgsHelpNeedsNoSource(t *testing.T) {
	for _, argv := range [][]string{{"--help"}, {"-h"}, {"--help", "--loglevel=error"}} {
		opts, err := ParseArgs(argv)
		if err != nil {
			t.Errorf("ParseArgs(%q): %v", argv, err)
			continue
		}
		if !opts.Help {
			t.Errorf("ParseArgs(%q) did not set Help", argv)
		}
	}
}

func TestParseArgsListConflictsUsePresence(t *testing.T) {
	for _, arg := range []string{
		"--output=.", "--file=pattern", "--resume", "--stream", "--timeout=1s",
	} {
		if _, err := ParseArgs([]string{"--list-files", arg, "source.torrent"}); err == nil {
			t.Errorf("expected listing conflict for %s", arg)
		}
	}
	if _, err := ParseArgs([]string{"--list-files", "--loglevel=warning", "source.torrent"}); err != nil {
		t.Fatalf("loglevel should be allowed in listing mode: %v", err)
	}
}

func TestParseArgsRejectsMalformedForms(t *testing.T) {
	tests := [][]string{
		{},
		{"source", "other"},
		{"source", "--help"},
		{"--unknown", "source"},
		{"-lr", "source"},
		{"--output"},
		{"--output", "-dir", "source"},
		{"--file=", "source"},
		{"--loglevel=verbose", "source"},
		{"--timeout=0", "source"},
		{"--timeout=-1s", "source"},
		{"-", "source"},
	}
	for _, argv := range tests {
		if _, err := ParseArgs(argv); err == nil {
			t.Errorf("ParseArgs(%q) unexpectedly succeeded", argv)
		}
	}
}

func TestParseArgsDoubleDashPath(t *testing.T) {
	opts, err := ParseArgs([]string{"--", "-looks-like-an-option"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Source != "-looks-like-an-option" {
		t.Fatalf("source = %q", opts.Source)
	}
}

func FuzzParseArgs(f *testing.F) {
	f.Add("--help")
	f.Add("--timeout=1s source.torrent")
	f.Add("-- --hash-shaped-path")
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = ParseArgs([]string{raw})
	})
}
