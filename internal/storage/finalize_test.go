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

func s2FinalizeEndpoint() Endpoint {
	return Endpoint{Addr: s2MustAddr("127.0.0.1"), Port: 6881}
}

func s2MustAddr(raw string) netip.Addr {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		panic(err)
	}
	return addr
}

func s2FinalizeMeta(name string, files ...torrent.File) torrent.Metainfo {
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	return torrent.Metainfo{Name: name, MultiFile: true, TotalLength: total, Files: files}
}

func s2Coverage(begin, length int64, endpoint Endpoint) BlockCoverage {
	return BlockCoverage{Begin: begin, Length: length, Contributors: []Endpoint{endpoint}}
}

func TestFinalizeVerifiesBeforeSelectedOutputAndCommitsOnlySelectedBytes(t *testing.T) {
	data := []byte("piece-data")
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "selected", 0, int64(len(data))), regular(1, "unselected", int64(len(data)), int64(len(data)+3)))
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
	snapshot := NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, int64(len(data)), s2FinalizeEndpoint())})
	result, err := stager.Finalize(context.Background(), snapshot, selection, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contributors) != 1 || result.Contributors[0] != s2FinalizeEndpoint() {
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
	meta := s2FinalizeMeta("bundle", regular(0, "first", 0, 3), torrent.File{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 5}, Kind: torrent.PaddingFile}, regular(2, "second", 5, 8))
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
	snapshot := NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 3, s2FinalizeEndpoint()), s2Coverage(5, 3, s2FinalizeEndpoint())})
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
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "first", 0, 3), regular(1, "second", 3, 6))
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
	if _, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 6, s2FinalizeEndpoint())}), selection, plan); err != nil {
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
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
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
	result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 4, s2FinalizeEndpoint())}), selection, plan)
	if !errors.Is(err, ErrPieceHashMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
	if result.OutputCommitted {
		t.Fatal("hash mismatch reported committed output")
	}
	if stager.StagedCount() != 0 {
		t.Fatal("mismatching stage was retained")
	}
	if stager.Fatal() != nil {
		t.Fatalf("hash mismatch became a cache failure: %v", stager.Fatal())
	}
	if _, err := os.Stat(filepath.Join(root, "bundle", "file")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("output was created before verification: %v", err)
	}
}

func TestFinalizeRequiresCompleteCoverage(t *testing.T) {
	data := []byte("abcdef")
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
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
	_, err = stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 3, s2FinalizeEndpoint())}), selection, plan)
	if !errors.Is(err, ErrIncompletePiece) {
		t.Fatalf("incomplete coverage error = %v", err)
	}
	if stager.StagedCount() != 1 {
		t.Fatal("incomplete piece was removed")
	}
}

func TestFinalizePreservesStageWhenOutputCloseFails(t *testing.T) {
	data := []byte("data")
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
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
	result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, int64(len(data)), s2FinalizeEndpoint())}), selection, plan)
	if !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v, want %v", err, closeErr)
	}
	if result.OutputCommitted {
		t.Fatal("output close failure reported committed output")
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("output close did not become fatal: %v", stager.Fatal())
	}
	if stager.StagedCount() != 1 {
		t.Fatal("stage removed before output close succeeded")
	}
}

func TestFinalizeWithNoSelectedBytesDoesNotReportOutputCommit(t *testing.T) {
	data := []byte("unselected")
	piece, mapping, plan := s2PlanForPiece(t, data)
	mapping.Selected = nil
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
	result, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, int64(len(data)), s2FinalizeEndpoint())}), mapping, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputCommitted {
		t.Fatal("piece with no selected bytes reported committed output")
	}
}

func TestFinalizeReportsCommittedOutputWhenCanceledAfterOutputClose(t *testing.T) {
	data := []byte("committed before cancel")
	piece, mapping, plan := s2PlanForPiece(t, data)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	previous := openOutputFile
	openOutputFile = func(path string, flag int, mode os.FileMode) (outputFile, error) {
		file, err := os.OpenFile(path, flag, mode)
		if err != nil {
			return nil, err
		}
		return &s2OutputEventFile{outputFile: file, onClose: cancel}, nil
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
	if err := stage.WriteBlock(0, data); err != nil {
		t.Fatal(err)
	}
	result, err := stager.Finalize(ctx, NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, int64(len(data)), s2FinalizeEndpoint())}), mapping, plan)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("finalize error = %v, want context cancellation", err)
	}
	if !result.OutputCommitted {
		t.Fatal("successful output close before cancellation was not reported committed")
	}
	got, readErr := os.ReadFile(filepath.Join(plan.Root(), "bundle", "file"))
	if readErr != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v; want committed payload", got, readErr)
	}
}

func TestFinalizeReportsCommittedOutputWhenStageRemovalFails(t *testing.T) {
	data := []byte("committed output")
	piece, mapping, plan := s2PlanForPiece(t, data)
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stage, err := stager.AdmitPiece(piece)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, data); err != nil {
		t.Fatal(err)
	}
	removeErr := errors.New("injected staged removal failure")
	previous := removeStagedFile
	removeStagedFile = func(string) error { return removeErr }
	t.Cleanup(func() { removeStagedFile = previous })
	result, finalizeErr := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, int64(len(data)), s2FinalizeEndpoint())}), mapping, plan)
	removeStagedFile = previous
	if !errors.Is(finalizeErr, removeErr) {
		t.Fatalf("finalize error = %v, want removal error", finalizeErr)
	}
	if !result.OutputCommitted {
		t.Fatal("finalize result did not report committed output")
	}
	got, err := os.ReadFile(filepath.Join(plan.Root(), "bundle", "file"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("output = %q, %v; want committed payload", got, err)
	}
	if err := os.Remove(stage.Path()); err != nil {
		t.Fatalf("remove leftover stage: %v", err)
	}
	if err := stager.Close(); !errors.Is(err, ErrStagingFatal) {
		t.Fatalf("stager close error = %v, want remembered removal failure", err)
	}
}
