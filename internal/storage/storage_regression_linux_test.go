package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestStorageFilesystemLimitsAreSeparateFromRelativeLimits(t *testing.T) {
	parts := make([]string, 17)
	for i := range parts {
		parts[i] = strings.Repeat("a", 240)
	}
	meta := storageParsedPath(t, parts)
	root := t.TempDir()
	_, err := Validate(root, meta, []int{0})
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("supported metainfo path must fail as a platform path error: %v", err)
	}
	// An unrepresentable later file must fail before overwrite can affect an
	// earlier existing file, even when the invalid path's parents are missing.
	first := filepath.Join(root, "bundle", "first")
	if err := os.MkdirAll(filepath.Dir(first), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta = testMeta("bundle", true, regular(0, "first", 0, 1), regular(1, "missing/"+strings.Repeat("b", 256), 1, 2))
	if _, err := Validate(root, meta, []int{0, 1}); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("unrepresentable component = %v", err)
	}
	storageContents(t, first, "keep")
	if _, err := os.Stat(filepath.Join(root, "bundle", "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation created missing parent: %v", err)
	}
}

func TestStorageManyFilesUnderDescriptorLimit(t *testing.T) {
	const childEnv = "LEECH_STORAGE_DESCRIPTOR_CHILD"
	if os.Getenv(childEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestStorageManyFilesUnderDescriptorLimit$", "-test.count=1")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("descriptor-limit child: %v\n%s", err, output)
		}
		return
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = 64
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	files := make([]torrent.File, 96)
	indices := make([]int, len(files))
	spans := make([]torrent.FileRange, len(files))
	for i := range files {
		files[i] = regular(i, fmt.Sprintf("file-%02d", i), int64(i), int64(i+1))
		indices[i] = i
		spans[i] = torrent.FileRange{Index: i, Range: files[i].Range}
	}
	plan, err := Validate(t.TempDir(), testMeta("bundle", true, files...), indices)
	if err != nil {
		t.Fatal(err)
	}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	countHandles := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	before := countHandles()
	for iteration := 0; iteration < 3; iteration++ {
		prepared, err := plan.Prepare(Overwrite)
		if err != nil {
			t.Fatalf("prepare 96 files: %v", err)
		}
		if countHandles() != before {
			t.Fatal("preparation retained output handles")
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte{'A' + byte(iteration)}, len(files))
		piece := s2StagePiece(iteration, 0, data)
		stage, err := stager.AdmitPiece(piece)
		if err != nil {
			t.Fatal(err)
		}
		if err := stage.WriteBlock(0, data); err != nil {
			t.Fatal(err)
		}
		mapping := torrent.PiecePlan{Piece: piece, Data: spans, Selected: spans}
		result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{{Begin: 0, Length: int64(len(data))}}), mapping, plan)
		if err != nil || !result.OutputCommitted || stager.StagedCount() != 0 {
			t.Fatalf("finalize 96 files = %+v, %v", result, err)
		}
		for _, entry := range plan.entries {
			storageContents(t, entry.Path, string(data[:1]))
		}
		if got := countHandles(); got != before {
			t.Fatalf("file handles leaked: before=%d, after=%d", before, got)
		}
	}
}
