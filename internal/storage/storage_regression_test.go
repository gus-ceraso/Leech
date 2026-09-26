package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func storageParsedPath(t *testing.T, parts []string) torrent.Metainfo {
	t.Helper()
	var encoded strings.Builder
	encoded.WriteString("d4:infod5:filesld6:lengthi0e4:pathl")
	for _, part := range parts {
		fmt.Fprintf(&encoded, "%d:%s", len(part), part)
	}
	encoded.WriteString("eee4:name6:bundle12:piece lengthi1e6:pieces0:ee")
	meta, err := torrent.ParseMetainfo([]byte(encoded.String()))
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestStorageRelativePathLimitsExcludeTorrentName(t *testing.T) {
	parts := make([]string, limits.PathComponents)
	for i := range parts {
		parts[i] = "a"
	}
	meta := storageParsedPath(t, parts)
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Validate(t.TempDir(), meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := plan.Prepare(Overwrite)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(plan.entries[0].Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
		t.Fatalf("64-component selected output = %v, %v", info, err)
	}

	// The 4,096-byte metainfo bound is independent of any host path limit.
	parts = make([]string, 17)
	for i := range parts {
		parts[i] = strings.Repeat("b", 240)
	}
	meta = storageParsedPath(t, parts)
	if len(meta.Files[0].Path) != limits.PathBytes {
		t.Fatal("incorrect relative-byte boundary fixture")
	}
	if _, err := relativeParts(meta.Files[0].Path, "file path"); err != nil {
		t.Fatal(err)
	}
	if _, err := relativeParts(meta.Files[0].Path+"c", "file path"); err == nil {
		t.Fatal("accepted 4,097-byte relative path")
	}
}

func storageLink(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(from, to); err != nil {
		t.Fatal(err)
	}
}

func storageContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf("contents of %s = %q, %v; want %q", path, got, err, want)
	}
}

func TestStorageOverwriteDetachesOutsideHardlink(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(outside, []byte("untouched"), 0o640); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(root, "bundle", "selected")
	storageLink(t, outside, selected)
	plan, err := Validate(root, testMeta("bundle", true, regular(0, "selected", 0, 1)), []int{0})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := plan.Prepare(Overwrite)
	if err != nil {
		t.Fatal(err)
	}
	storageContents(t, selected, "")
	storageContents(t, outside, "untouched")
	if err := prepared.WriteRange(0, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	storageContents(t, selected, "A")
	storageContents(t, outside, "untouched")
}

func storageFinalizeByte(t *testing.T, stager *Stager, plan *Plan, index int, offset int64, value byte) {
	t.Helper()
	piece := s2StagePiece(int(offset), offset, []byte{value})
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, []byte{value}); err != nil {
		t.Fatal(err)
	}
	spans := []torrent.FileRange{{Index: index, Range: piece.Range}}
	mapping := torrent.PiecePlan{Piece: piece, Data: spans, Selected: spans}
	result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{{Begin: 0, Length: 1}}), mapping, plan)
	if err != nil || !result.OutputCommitted {
		t.Fatalf("finalize = %+v, %v", result, err)
	}
}

func TestStorageFinalizeDetachesSelectedAliases(t *testing.T) {
	root := t.TempDir()
	first, second := filepath.Join(root, "bundle", "a"), filepath.Join(root, "bundle", "b")
	if err := os.MkdirAll(filepath.Dir(first), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	storageLink(t, first, second)
	plan, err := Validate(root, testMeta("bundle", true, regular(0, "a", 0, 1), regular(1, "b", 1, 2)), []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	storageFinalizeByte(t, stager, plan, 0, 0, 'A')
	storageFinalizeByte(t, stager, plan, 1, 1, 'B')
	storageContents(t, first, "A")
	storageContents(t, second, "B")
}

func TestStorageResumeWritesAndTruncationPreserveAliases(t *testing.T) {
	files := []torrent.File{regular(0, "a", 0, 2), regular(1, "b", 2, 4), regular(2, "unselected", 4, 6)}
	pieces := []torrent.Piece{resumePiece(0, 0, []byte("R")), resumePiece(1, 1, []byte("A")), resumePiece(2, 2, []byte("R")), resumePiece(3, 3, []byte("B"))}
	selection, plan := resumeSelection(t, files, pieces, []string{"a", "b"})
	writeResumeFile(t, plan, 0, []byte("RxTAIL"))
	first, second := plan.entries[0].Path, plan.entries[1].Path
	unselected := filepath.Join(filepath.Dir(first), "unselected")
	outside := filepath.Join(t.TempDir(), "outside")
	storageLink(t, first, second)
	storageLink(t, first, unselected)
	storageLink(t, first, outside)
	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil || result.RetainedBytes != 2 || len(result.PendingTruncations) != 2 {
		t.Fatalf("resume = %+v, %v", result, err)
	}
	prepared, err := plan.Prepare(Resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	storageFinalizeByte(t, stager, plan, 0, 1, 'A')
	storageFinalizeByte(t, stager, plan, 1, 3, 'B')
	storageContents(t, first, "RATAIL")
	storageContents(t, second, "RBTAIL")
	for _, pending := range result.PendingTruncations {
		if err := plan.TruncateSelected(pending.Index, pending.Size); err != nil {
			t.Fatal(err)
		}
	}
	storageContents(t, first, "RA")
	storageContents(t, second, "RB")
	storageContents(t, unselected, "RxTAIL")
	storageContents(t, outside, "RxTAIL")
}

func TestStorageVerifiedResumeTruncationDetachesAliases(t *testing.T) {
	selection, plan := resumeSelection(t, []torrent.File{regular(0, "a", 0, 1), regular(1, "b", 1, 2)},
		[]torrent.Piece{resumePiece(0, 0, []byte("A")), resumePiece(1, 1, []byte("A"))}, nil)
	writeResumeFile(t, plan, 0, []byte("ATAIL"))
	storageLink(t, plan.entries[0].Path, plan.entries[1].Path)
	outside := filepath.Join(t.TempDir(), "outside")
	storageLink(t, plan.entries[0].Path, outside)
	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil || !result.NoTransferNeeded {
		t.Fatalf("resume = %+v, %v", result, err)
	}
	storageContents(t, plan.entries[0].Path, "A")
	storageContents(t, plan.entries[1].Path, "A")
	storageContents(t, outside, "ATAIL")
}

func TestStorageDetachFailureRetainsAliasesAndStage(t *testing.T) {
	for _, failure := range []string{"copy", "rename", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			piece, mapping, plan := s2PlanForPiece(t, []byte("A"))
			writeResumeFile(t, plan, 0, []byte("old"))
			outside := filepath.Join(t.TempDir(), "outside")
			storageLink(t, plan.entries[0].Path, outside)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("injected detach failure")
			oldCopy, oldRename := copyDetachedOutput, renameDetachedOutput
			t.Cleanup(func() { copyDetachedOutput, renameDetachedOutput = oldCopy, oldRename })
			switch failure {
			case "copy":
				copyDetachedOutput = func(context.Context, io.Writer, io.Reader, int64) error { return injected }
			case "rename":
				renameDetachedOutput = func(string, string) error { return injected }
			case "cancel":
				injected = context.Canceled
				copyDetachedOutput = func(ctx context.Context, dst io.Writer, src io.Reader, length int64) error {
					cancel()
					return copyOutputPrefix(ctx, dst, src, length)
				}
			}
			stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
			if err := stager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer stager.Close()
			stage, err := stager.AdmitPiece(piece)
			if err != nil {
				t.Fatal(err)
			}
			if err := stage.WriteBlock(0, []byte("A")); err != nil {
				t.Fatal(err)
			}
			result, err := stager.Finalize(ctx, NewPieceSnapshot(piece, []BlockCoverage{{Begin: 0, Length: 1}}), mapping, plan)
			if !errors.Is(err, injected) || result.OutputCommitted || stager.StagedCount() != 1 {
				t.Fatalf("detach failure = %+v, %v, stages=%d", result, err, stager.StagedCount())
			}
			if failure != "cancel" && !errors.Is(stager.Fatal(), ErrStagingFatal) {
				t.Fatalf("detach failure not fatal: %v", stager.Fatal())
			}
			if failure == "cancel" && stager.Fatal() != nil {
				t.Fatalf("cancellation was fatal: %v", stager.Fatal())
			}
			storageContents(t, plan.entries[0].Path, "old")
			storageContents(t, outside, "old")
			temps, err := filepath.Glob(filepath.Join(filepath.Dir(plan.entries[0].Path), ".leech-*"))
			if err != nil || len(temps) != 0 {
				t.Fatalf("detachment leaked temporary paths: %v, %v", temps, err)
			}
		})
	}
}

type storageShortWriter struct{}

func (storageShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestStorageCopyUsesOnlyObservedPrefixAndPropagatesShortIO(t *testing.T) {
	var dst bytes.Buffer
	if err := copyOutputPrefix(context.Background(), &dst, strings.NewReader("prefixsuffix"), 6); err != nil || dst.String() != "prefix" {
		t.Fatalf("prefix copy = %q, %v", dst.String(), err)
	}
	if err := copyOutputPrefix(context.Background(), storageShortWriter{}, strings.NewReader("abc"), 3); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short copy write = %v", err)
	}
	if err := copyOutputPrefix(context.Background(), io.Discard, strings.NewReader("a"), 2); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short copy read = %v", err)
	}
}

func TestStoragePrepareRevalidatesAllPathsBeforeOverwrite(t *testing.T) {
	root := t.TempDir()
	plan, err := Validate(root, testMeta("bundle", true, regular(0, "a", 0, 1), regular(1, "b", 1, 2)), []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	writeResumeFile(t, plan, 0, []byte("keep"))
	if err := os.Mkdir(plan.entries[1].Path, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Prepare(Overwrite); err == nil {
		t.Fatal("preparation accepted a directory target")
	}
	storageContents(t, plan.entries[0].Path, "keep")
}

type storageShortOutput struct{ outputFile }

func (storageShortOutput) WriteAt(data []byte, _ int64) (int, error) {
	return len(data) - 1, nil
}

func TestStorageFinalizeOpenAndShortWriteFailuresRetainStage(t *testing.T) {
	for _, failure := range []string{"open", "short write"} {
		t.Run(failure, func(t *testing.T) {
			piece, mapping, plan := s2PlanForPiece(t, []byte("data"))
			injected := errors.New("injected output open failure")
			if failure == "short write" {
				injected = io.ErrShortWrite
			}
			previous := openOutputFile
			openOutputFile = func(path string, flag int, mode os.FileMode) (outputFile, error) {
				if failure == "open" {
					return nil, injected
				}
				handle, err := os.OpenFile(path, flag, mode)
				if err != nil {
					return nil, err
				}
				return storageShortOutput{handle}, nil
			}
			t.Cleanup(func() { openOutputFile = previous })
			stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
			if err := stager.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer stager.Close()
			stage, err := stager.AdmitPiece(piece)
			if err != nil {
				t.Fatal(err)
			}
			if err := stage.WriteBlock(0, []byte("data")); err != nil {
				t.Fatal(err)
			}
			result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{{Begin: 0, Length: 4}}), mapping, plan)
			if !errors.Is(err, injected) || result.OutputCommitted || stager.StagedCount() != 1 || !errors.Is(stager.Fatal(), ErrStagingFatal) {
				t.Fatalf("output failure = %+v, %v; stage count=%d, fatal=%v", result, err, stager.StagedCount(), stager.Fatal())
			}
		})
	}
}
