package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

type s2InjectedStageFile struct {
	write    func([]byte, int64) (int, error)
	read     func([]byte, int64) (int, error)
	truncate error
	closeErr error
}

func (f *s2InjectedStageFile) WriteAt(data []byte, offset int64) (int, error) {
	if f.write != nil {
		return f.write(data, offset)
	}
	return len(data), nil
}

func (f *s2InjectedStageFile) ReadAt(data []byte, offset int64) (int, error) {
	if f.read != nil {
		return f.read(data, offset)
	}
	return len(data), nil
}

func (f *s2InjectedStageFile) Truncate(int64) error { return f.truncate }
func (f *s2InjectedStageFile) Close() error         { return f.closeErr }

func s2InstallStageFile(t *testing.T, file stagingFile) {
	t.Helper()
	previousOpen := openStagingFile
	previousRemove := removeStagedFile
	openStagingFile = func(string, int, os.FileMode) (stagingFile, error) { return file, nil }
	removeStagedFile = func(string) error { return nil }
	t.Cleanup(func() {
		openStagingFile = previousOpen
		removeStagedFile = previousRemove
	})
}

func s2PlanForPiece(t *testing.T, data []byte) (torrent.Piece, torrent.PiecePlan, *Plan) {
	t.Helper()
	piece := s2StagePiece(0, 0, data)
	meta := s2FinalizeMeta("bundle", regular(0, "file", 0, int64(len(data))))
	plan, err := Validate(t.TempDir(), meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	mapping := torrent.PiecePlan{
		Piece:    piece,
		Data:     []torrent.FileRange{{Index: 0, Range: piece.Range}},
		Selected: []torrent.FileRange{{Index: 0, Range: piece.Range}},
	}
	return piece, mapping, plan
}

func TestS2UnavailableCacheIsFatal(t *testing.T) {
	parent := t.TempDir()
	blocked := filepath.Join(parent, "cache-file")
	if err := os.WriteFile(blocked, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	stager := NewStager(StagerConfig{CacheRoot: filepath.Join(blocked, "child")})
	err := stager.Start(context.Background())
	if !errors.Is(err, ErrStagingFatal) || !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("unavailable cache error/fatal = %v/%v", err, stager.Fatal())
	}
	if workspace := stager.Workspace(); workspace != "" {
		t.Fatalf("unavailable cache workspace = %q", workspace)
	}
	_ = stager.Close()
}

func TestS2ShortStagedWriteIsFatal(t *testing.T) {
	file := &s2InjectedStageFile{write: func(data []byte, _ int64) (int, error) {
		return len(data) - 1, nil
	}}
	s2InstallStageFile(t, file)
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	stage, err := stager.AdmitPiece(s2StagePiece(0, 0, []byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.WriteBlock(0, []byte("data")); !errors.Is(err, ErrStagingFatal) {
		t.Fatalf("short write error = %v", err)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("short write did not poison stager: %v", stager.Fatal())
	}
}

func TestS2StagedReadFailureIsFatal(t *testing.T) {
	readErr := errors.New("injected staged read failure")
	file := &s2InjectedStageFile{read: func([]byte, int64) (int, error) { return 0, readErr }}
	s2InstallStageFile(t, file)
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	piece, mapping, _ := s2PlanForPiece(t, []byte("data"))
	if _, err := stager.AdmitPiece(piece); err != nil {
		t.Fatal(err)
	}
	_, err := stager.Verify(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 4, s2FinalizeEndpoint())}), mapping)
	if !errors.Is(err, ErrStagingFatal) || !errors.Is(err, readErr) {
		t.Fatalf("staged read error = %v", err)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("staged read did not poison stager: %v", stager.Fatal())
	}
}

func TestS2StagedCloseFailureIsFatal(t *testing.T) {
	closeErr := errors.New("injected staged close failure")
	s2InstallStageFile(t, &s2InjectedStageFile{closeErr: closeErr})
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stage, err := stager.AdmitPiece(s2StagePiece(0, 0, []byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Abort(); !errors.Is(err, closeErr) {
		t.Fatalf("staged close error = %v", err)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("staged close did not poison stager: %v", stager.Fatal())
	}
	_ = stager.Close()
}

func TestS2StagedRemovalFailureIsFatal(t *testing.T) {
	removeErr := errors.New("injected staged removal failure")
	previous := removeStagedFile
	removeStagedFile = func(string) error { return removeErr }
	t.Cleanup(func() { removeStagedFile = previous })
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stage, err := stager.AdmitPiece(s2StagePiece(0, 0, []byte("data")))
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Abort(); !errors.Is(err, removeErr) {
		t.Fatalf("staged removal error = %v", err)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("staged removal did not poison stager: %v", stager.Fatal())
	}
	_ = stager.Close()
}

func TestS2CleanupFailureIsReported(t *testing.T) {
	cleanupErr := errors.New("injected workspace removal failure")
	previous := removeStagingRoot
	removeStagingRoot = func(string) error { return cleanupErr }
	t.Cleanup(func() { removeStagingRoot = previous })
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stager.Close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestS2CancellationDuringHashLeavesStageAndIsNotFatal(t *testing.T) {
	data := make([]byte, limits.BlockBytes+1)
	piece, mapping, _ := s2PlanForPiece(t, data)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	file := &s2InjectedStageFile{read: func(data []byte, _ int64) (int, error) {
		cancel()
		return len(data), nil
	}}
	s2InstallStageFile(t, file)
	stager := NewStager(StagerConfig{CacheRoot: t.TempDir()})
	if err := stager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer stager.Close()
	if _, err := stager.AdmitPiece(piece); err != nil {
		t.Fatal(err)
	}
	blocks := []BlockCoverage{
		s2Coverage(0, limits.BlockBytes, s2FinalizeEndpoint()),
		s2Coverage(limits.BlockBytes, 1, s2FinalizeEndpoint()),
	}
	_, err := stager.Verify(ctx, NewPieceSnapshot(piece, blocks), mapping)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if stager.StagedCount() != 1 || stager.Fatal() != nil {
		t.Fatalf("cancellation state count/fatal = %d/%v", stager.StagedCount(), stager.Fatal())
	}
}

func TestS2MixedContributorsAreReturned(t *testing.T) {
	data := []byte("abcdefgh")
	piece, mapping, plan := s2PlanForPiece(t, data)
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
	first := s2FinalizeEndpoint()
	second := Endpoint{Addr: first.Addr, Port: first.Port + 1}
	snapshot := NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 4, first), s2Coverage(4, 4, second)})
	result, err := stager.Finalize(context.Background(), snapshot, mapping, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contributors) != 2 || result.Contributors[0] != first || result.Contributors[1] != second {
		t.Fatalf("mixed contributors = %#v", result.Contributors)
	}
}

type s2OutputEventFile struct {
	outputFile
	onClose func()
}

func (f *s2OutputEventFile) Close() error {
	err := f.outputFile.Close()
	if f.onClose != nil {
		f.onClose()
	}
	return err
}

func TestS2OutputClosesBeforeStageRemoval(t *testing.T) {
	data := []byte("data")
	piece, mapping, plan := s2PlanForPiece(t, data)
	closed := false
	previousOpen := openOutputFile
	previousRemove := removeStagedFile
	openOutputFile = func(path string, flag int, mode os.FileMode) (outputFile, error) {
		file, err := os.OpenFile(path, flag, mode)
		if err != nil {
			return nil, err
		}
		return &s2OutputEventFile{outputFile: file, onClose: func() { closed = true }}, nil
	}
	removeStagedFile = func(path string) error {
		if !closed {
			return errors.New("stage removed before output close")
		}
		return os.Remove(path)
	}
	t.Cleanup(func() {
		openOutputFile = previousOpen
		removeStagedFile = previousRemove
	})
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
	if _, err := stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 4, s2FinalizeEndpoint())}), mapping, plan); err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatal("output was not closed")
	}
}

type s2ShortOutputFile struct{ writeErr error }

func (f *s2ShortOutputFile) WriteAt([]byte, int64) (int, error) { return 0, f.writeErr }
func (f *s2ShortOutputFile) Truncate(int64) error               { return nil }
func (f *s2ShortOutputFile) Close() error                       { return nil }

func TestS2OutputWriteFailureIsFatalAndRetainsStage(t *testing.T) {
	data := []byte("data")
	piece, mapping, plan := s2PlanForPiece(t, data)
	writeErr := errors.New("injected output write failure")
	previous := openOutputFile
	openOutputFile = func(string, int, os.FileMode) (outputFile, error) {
		return &s2ShortOutputFile{writeErr: writeErr}, nil
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
	_, err = stager.Finalize(context.Background(), NewPieceSnapshot(piece, []BlockCoverage{s2Coverage(0, 4, s2FinalizeEndpoint())}), mapping, plan)
	if !errors.Is(err, writeErr) {
		t.Fatalf("output write error = %v", err)
	}
	if !errors.Is(stager.Fatal(), ErrStagingFatal) {
		t.Fatalf("output write did not poison stager: %v", stager.Fatal())
	}
	if stager.StagedCount() != 1 {
		t.Fatal("stage removed after output write failure")
	}
}
