package storage

import (
	"context"
	"crypto/sha1"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/torrent"
)

func finalizeEndpoint() Endpoint {
	return Endpoint{Addr: mustAddr("127.0.0.1"), Port: 6881}
}

func mustAddr(raw string) netip.Addr {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		panic(err)
	}
	return addr
}

func finalizeMeta(name string, files ...torrent.File) torrent.Metainfo {
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	return torrent.Metainfo{Name: name, MultiFile: true, TotalLength: total, Files: files}
}

func coverage(begin, length int64, endpoint Endpoint) BlockCoverage {
	return BlockCoverage{Begin: begin, Length: length, Contributors: []Endpoint{endpoint}}
}

func TestFinalizeVerifiesBeforeSelectedOutputAndCommitsOnlySelectedBytes(t *testing.T) {
	data := []byte("piece-data")
	piece := stagePiece(0, 0, data)
	meta := finalizeMeta("bundle", regular(0, "selected", 0, int64(len(data))), regular(1, "unselected", int64(len(data)), int64(len(data)+3)))
	root := t.TempDir()
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{
		Piece:    piece,
		Data:     []torrent.FileRange{{Index: 0, Range: piece.Range}},
		Selected: []torrent.FileRange{{Index: 0, Range: piece.Range}},
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
	if err := stage.WriteBlock(0, data); err != nil {
		t.Fatal(err)
	}
	snapshot := NewPieceSnapshot(piece, []BlockCoverage{coverage(0, int64(len(data)), finalizeEndpoint())})
	result, err := stager.Finalize(context.Background(), snapshot, selection, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contributors) != 1 || result.Contributors[0] != finalizeEndpoint() {
		t.Fatalf("contributors = %#v", result.Contributors)
	}
	got, err := os.ReadFile(filepath.Join(root, "bundle", "selected"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("selected output = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "bundle", "unselected")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected output exists: %v", err)
	}
	if stager.StagedCount() != 0 {
		t.Fatalf("staged count after finalization = %d", stager.StagedCount())
	}
}

func TestFinalizeSynthesizesPaddingAndSupportsCrossFileBlock(t *testing.T) {
	bytes := []byte{'a', 'b', 'c', 0, 0, 'd', 'e', 'f'}
	piece := torrent.Piece{Index: 0, Range: torrent.ByteRange{Begin: 0, End: int64(len(bytes))}, Hash: sha1.Sum(bytes)}
	meta := finalizeMeta("bundle", regular(0, "first", 0, 3), torrent.File{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 5}, Kind: torrent.PaddingFile}, regular(2, "second", 5, 8))
	root := t.TempDir()
	plan, err := Validate(root, meta, []int{2})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{
		Piece: piece,
		Data: []torrent.FileRange{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}},
			{Index: 2, Range: torrent.ByteRange{Begin: 5, End: 8}},
		},
		Selected: []torrent.FileRange{{Index: 2, Range: torrent.ByteRange{Begin: 5, End: 8}}},
		Padding:  []torrent.ByteRange{{Begin: 3, End: 5}},
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
	// One block crosses the boundary between two adjacent regular files. A
	// separate block supplies the regular bytes after synthetic padding.
	if err := stage.WriteBlock(0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(5, []byte("def")); err != nil {
		t.Fatal(err)
	}
	snapshot := NewPieceSnapshot(piece, []BlockCoverage{coverage(0, 3, finalizeEndpoint()), coverage(5, 3, finalizeEndpoint())})
	if _, err := stager.Finalize(context.Background(), snapshot, selection, plan); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "bundle", "second"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "def" {
		t.Fatalf("padding selected output = %q", got)
	}
}

func TestFinalizeAllowsOneBlockAcrossAdjacentRegularFiles(t *testing.T) {
	data := []byte("abcdef")
	piece := stagePiece(0, 0, data)
	meta := finalizeMeta("bundle", regular(0, "first", 0, 3), regular(1, "second", 3, 6))
	root := t.TempDir()
	plan, err := Validate(root, meta, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{
		Piece: piece,
		Data: []torrent.FileRange{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}},
			{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 6}},
		},
		Selected: []torrent.FileRange{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}},
			{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 6}},
		},
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
	if err := stage.WriteBlock(0, data); err != nil {
		t.Fatal(err)
	}
	if _, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{coverage(0, 6, finalizeEndpoint())}), selection, plan); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want string
	}{
		{filepath.Join(root, "bundle", "first"), "abc"},
		{filepath.Join(root, "bundle", "second"), "def"},
	} {
		got, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Fatalf("%s = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestFinalizeHashMismatchRemovesStageWithoutCreatingOutput(t *testing.T) {
	data := []byte("good")
	piece := stagePiece(0, 0, data)
	meta := finalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
	root := t.TempDir()
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{Piece: piece, Data: []torrent.FileRange{{Index: 0, Range: piece.Range}}, Selected: []torrent.FileRange{{Index: 0, Range: piece.Range}}}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, []byte("bad!")); err != nil {
		t.Fatal(err)
	}
	_, err = stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{coverage(0, 4, finalizeEndpoint())}), selection, plan)
	if !errors.Is(err, ErrPieceHashMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
	if stager.StagedCount() != 0 {
		t.Fatal("mismatching stage was retained")
	}
	if _, err := os.Stat(filepath.Join(root, "bundle", "file")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("output was created before verification: %v", err)
	}
}

func TestFinalizeRequiresCompleteCoverage(t *testing.T) {
	data := []byte("abcdef")
	piece := stagePiece(0, 0, data)
	meta := finalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
	root := t.TempDir()
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{Piece: piece, Data: []torrent.FileRange{{Index: 0, Range: piece.Range}}, Selected: []torrent.FileRange{{Index: 0, Range: piece.Range}}}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	if _, err := stager.AdmitPiece(piece); err != nil {
		t.Fatal(err)
	}
	_, err = stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{coverage(0, 3, finalizeEndpoint())}), selection, plan)
	if !errors.Is(err, ErrIncompletePiece) {
		t.Fatalf("incomplete coverage error = %v", err)
	}
	if stager.StagedCount() != 1 {
		t.Fatal("incomplete piece was removed")
	}
}

func TestFinalizePreservesStageWhenOutputCloseFails(t *testing.T) {
	data := []byte("data")
	piece := stagePiece(0, 0, data)
	meta := finalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
	plan, err := Validate(t.TempDir(), meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	selection := torrent.PiecePlan{Piece: piece, Data: []torrent.FileRange{{Index: 0, Range: piece.Range}}, Selected: []torrent.FileRange{{Index: 0, Range: piece.Range}}}
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, data); err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("injected output close failure")
	closes := 0
	previous := openOutputFile
	openOutputFile = func(string, int, os.FileMode) (outputFile, error) {
		return &closeFailureFile{closeErr: &closeErr, closes: &closes}, nil
	}
	defer func() { openOutputFile = previous }()
	_, err = stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{coverage(0, int64(len(data)), finalizeEndpoint())}), selection, plan)
	if !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want %v", err, closeErr)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("output close did not become fatal: %v", stager.Fatal())
	}
	if stager.StagedCount() != 1 {
		t.Fatal("stage removed before output close succeeded")
	}
}
