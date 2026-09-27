package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/gus-ceraso/Leech/internal/cli"
)

func TestExecutableUsageDiagnostics(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "leech")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v\n%s", err, output)
	}

	for _, tc := range []struct {
		name     string
		args     []string
		contains string
	}{
		{"long escape", []string{"--\x1b[2J"}, `\x1b[2J`},
		{"long bounded", []string{"--" + strings.Repeat("x", 5000)}, "..."},
		{"short escape", []string{"-\x1b[2J"}, `\x1b[2J`},
		{"short bounded", []string{"-" + strings.Repeat("x", 5000)}, "..."},
		{"log level escape", []string{"--loglevel=\x1b[2J"}, `\x1b[2J`},
		{"short log level escape", []string{"-L", "\x1b[2J"}, `\x1b[2J`},
		{"log level bounded", []string{"--loglevel=" + strings.Repeat("x", 5000)}, "..."},
		{"invalid timeout", []string{"--timeout=\x1b[2J"}, "requires a positive Go duration"},
		{"invalid short timeout", []string{"-t", "\x1b[2J"}, "requires a positive Go duration"},
		{"missing value", []string{"--output"}, "requires a value"},
		{"listing conflict", []string{"--list-files", "--resume", "source"}, "cannot be combined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, tc.args...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 {
				t.Fatalf("exit = %v, want 2", err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("usage wrote stdout: %q", stdout.String())
			}
			text := stderr.String()
			parts := strings.Split(text, "\n")
			if len(parts) != 3 || !strings.HasPrefix(parts[0], "leech: ") || parts[1] != cli.UsageText || parts[2] != "" {
				t.Fatalf("usage layout = %q", text)
			}
			if len(strings.TrimPrefix(parts[0], "leech: ")) > cli.DefaultDiagnosticBytes {
				t.Fatalf("unbounded diagnostic: %d bytes", len(parts[0]))
			}
			if !strings.Contains(parts[0], tc.contains) {
				t.Fatalf("diagnostic %q does not contain %q", parts[0], tc.contains)
			}
			for _, r := range parts[0] {
				if unicode.IsControl(r) {
					t.Fatalf("literal control character in diagnostic: %q", parts[0])
				}
			}
		})
	}
	t.Run("help", func(t *testing.T) {
		cmd := exec.CommandContext(ctx, binary, "--help")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatal(err)
		}
		if stdout.String() != cli.HelpText || stderr.Len() != 0 {
			t.Fatalf("help stdout/stderr = %q/%q", stdout.String(), stderr.String())
		}
	})
	t.Run("listing write failure exits one", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("/dev/full requires Linux")
		}
		path := filepath.Join(t.TempDir(), "fixture.torrent")
		if err := os.WriteFile(path, []byte("d4:infod6:lengthi0e4:name1:x12:piece lengthi1e6:pieces0:ee"), 0600); err != nil {
			t.Fatal(err)
		}
		full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer full.Close()
		cmd := exec.CommandContext(ctx, binary, "--list-files", "--loglevel=error", path)
		var stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = full, &stderr
		err = cmd.Run()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("listing exit = %v, want 1", err)
		}
		timestamp, message, ok := strings.Cut(stderr.String(), " ")
		if _, err := time.Parse(time.RFC3339Nano, timestamp); !ok || err != nil || !strings.HasPrefix(message, "error: failure: cli: write file listing: ") {
			t.Fatalf("listing diagnostic = %q", stderr.String())
		}
	})
}
