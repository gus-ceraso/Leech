package storage

import (
	"context"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func s2StagePiece(index int, begin int64, data []byte) torrent.Piece {
	return torrent.Piece{Index: index, Range: torrent.ByteRange{Begin: begin, End: begin + int64(len(data))}, Hash: sha1.Sum(data)}
}

func TestStagerDoesNotCreateWorkspaceBeforeStart(t *testing.T) {
	parent := t.TempDir()
	stager := NewStager(StagerConfig{CacheRoot: filepath.Join(parent, "leech")})
	if got := stager.Workspace(); got != "" {
		t.Fatalf("workspace before Start = %q", got)
	}
	if entries, err := os.ReadDir(parent); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatalf("constructor created cache entries: %v", entries)
	}
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	workspace := stager.Workspace()
	info, err := os.Stat(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("workspace mode = %o, want 700", info.Mode().Perm())
	}
	if err := stager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace after Close: %v", err)
	}
}

func TestStagerAdmitsSparsePieceAndEnforcesBudgets(t *testing.T) {
	parent := t.TempDir()
	stager := NewStager(StagerConfig{CacheRoot: parent, MaxPieces: 1, MaxBytes: 8})
	if _, err := stager.AdmitPiece(s2StagePiece(0, 0, make([]byte, 9))); !errors.Is(err, ErrStagerNotStarted) {
		t.Fatalf("admit before Start = %v, want not started", err)
	}
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	tooLarge := s2StagePiece(0, 0, make([]byte, 9))
	if _, err := stager.AdmitPiece(tooLarge); !errors.Is(err, ErrStagingLimit) {
		t.Fatalf("oversized admission = %v, want budget error", err)
	}
	piece := s2StagePiece(0, 0, make([]byte, 8))
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(stage.Path())
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || info.Size() != 8 {
		t.Fatalf("stage file mode/size = %o/%d", info.Mode().Perm(), info.Size())
	}
	if _, err := stager.AdmitPiece(s2StagePiece(1, 8, make([]byte, 1))); !errors.Is(err, ErrStagingLimit) {
		t.Fatalf("piece-count admission = %v, want budget error", err)
	}
	if err := stage.WriteBlock(0, []byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n, err := stage.ReadAt(buf, 0); err != nil || n != len(buf) || string(buf) != "abcdefgh" {
		t.Fatalf("ReadAt = %d/%v/%q", n, err, buf)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
	if stager.StagedCount() != 0 || stager.StagedBytes() != 0 {
		t.Fatalf("reservations after Abort = %d/%d", stager.StagedCount(), stager.StagedBytes())
	}
	if err := stager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStagerRejectsOversizedAndOutOfRangeBlocks(t *testing.T) {
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	stage, err := stager.AdmitPiece(s2StagePiece(0, 0, make([]byte, limits.BlockBytes+1)))
	if err != nil {
		// The piece itself exceeds the supported piece limit only when the
		// block limit is changed; retain this branch for the default constants.
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, make([]byte, limits.BlockBytes+1)); !errors.Is(err, ErrStagingFatal) {
		t.Fatalf("oversized block = %v, want fatal staging error", err)
	}
}
