package storage

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gus-ceraso/Leech/internal/torrent"
)

func makeResumeFile(index int, path string, begin, end int64, kind torrent.FileKind) torrent.File {
	return torrent.File{Index: index, Path: path, Range: torrent.ByteRange{Begin: begin, End: end}, Kind: kind}
}

func resumePiece(index int, begin int64, data []byte) torrent.Piece {
	return torrent.Piece{Index: index, Range: torrent.ByteRange{Begin: begin, End: begin + int64(len(data))}, Hash: sha1.Sum(data)}
}

func resumeSelection(t *testing.T, files []torrent.File, pieces []torrent.Piece, patterns []string) (*torrent.SelectionPlan, *Plan) {
	t.Helper()
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	meta := torrent.Metainfo{Name: "bundle", MultiFile: true, TotalLength: total, Files: files, Pieces: pieces}
	selection, err := torrent.Select(meta, patterns, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Validate(t.TempDir(), meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	return selection, plan
}

func writeResumeFile(t *testing.T, plan *Plan, index int, data []byte) {
	t.Helper()
	entry, ok := plan.Entry(index)
	if !ok {
		t.Fatalf("missing output entry %d", index)
	}
	if err := os.MkdirAll(filepath.Dir(entry.Path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry.Path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestScanResumeCompleteCrossFilePiece(t *testing.T) {
	data := []byte("abcdef")
	selection, plan := resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "a", 0, 3, torrent.RegularFile), makeResumeFile(1, "b", 3, 6, torrent.RegularFile)},
		[]torrent.Piece{resumePiece(0, 0, data)}, nil)
	writeResumeFile(t, plan, 0, data[:3])
	writeResumeFile(t, plan, 1, data[3:])

	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.VerifiedPieces, []int{0}) || result.RetainedBytes != 6 || !result.NoTransferNeeded {
		t.Fatalf("resume result = %+v", result)
	}
	want := []torrent.FileRange{{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}}, {Index: 1, Range: torrent.ByteRange{Begin: 3, End: 6}}}
	if !reflect.DeepEqual(result.VerifiedRanges, want) {
		t.Fatalf("verified ranges = %#v, want %#v", result.VerifiedRanges, want)
	}
}

func TestScanResumeReportsMissingSelectedZeroLengthFile(t *testing.T) {
	selection, plan := resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "empty", 0, 0, torrent.RegularFile)}, nil, nil)
	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !result.NoTransferNeeded || !reflect.DeepEqual(result.MissingZeroLength, []int{0}) {
		t.Fatalf("zero-length result=%+v", result)
	}
	entry, ok := plan.Entry(0)
	if !ok {
		t.Fatal("missing zero-length output entry")
	}
	if _, err := os.Stat(entry.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resume scan created zero-length output: err=%v", err)
	}
}

func TestScanResumeHashesSyntheticPadding(t *testing.T) {
	data := []byte{'a', 'b', 0, 0, 'c', 'd'}
	selection, plan := resumeSelection(t,
		[]torrent.File{
			makeResumeFile(0, "a", 0, 2, torrent.RegularFile),
			makeResumeFile(1, "", 2, 4, torrent.PaddingFile),
			makeResumeFile(2, "b", 4, 6, torrent.RegularFile),
		},
		[]torrent.Piece{resumePiece(0, 0, data)}, nil)
	writeResumeFile(t, plan, 0, data[:2])
	writeResumeFile(t, plan, 2, data[4:])

	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil || !result.NoTransferNeeded || result.RetainedBytes != 4 {
		t.Fatalf("padding resume result=%+v err=%v", result, err)
	}
}

func TestScanResumeRedownloadsPieceWithSkippedRegularRange(t *testing.T) {
	data := []byte{'a', 'b', 'X', 0}
	selection, plan := resumeSelection(t,
		[]torrent.File{
			makeResumeFile(0, "selected", 0, 2, torrent.RegularFile),
			makeResumeFile(1, "skipped", 2, 3, torrent.RegularFile),
			makeResumeFile(2, "", 3, 4, torrent.PaddingFile),
		},
		[]torrent.Piece{resumePiece(0, 0, data)}, []string{"selected"})
	writeResumeFile(t, plan, 0, data[:2])

	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.VerifiedPieces) != 0 || result.RetainedBytes != 0 || result.NoTransferNeeded {
		t.Fatalf("skipped-range result = %+v", result)
	}
}

func TestScanResumeMissingShortAndMismatchAreIncomplete(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "missing"},
		{name: "short", data: []byte("goo")},
		{name: "mismatch", data: []byte("bad!")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := []byte("good")
			selection, plan := resumeSelection(t,
				[]torrent.File{makeResumeFile(0, "a", 0, 4, torrent.RegularFile)},
				[]torrent.Piece{resumePiece(0, 0, want)}, nil)
			if test.data != nil {
				writeResumeFile(t, plan, 0, test.data)
			}
			result, err := ScanResume(context.Background(), selection, plan)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.VerifiedPieces) != 0 || result.NoTransferNeeded {
				t.Fatalf("incomplete result = %+v", result)
			}
		})
	}
}

func TestScanResumeOverlongTruncationAndPending(t *testing.T) {
	data := []byte("abc")
	selection, plan := resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "a", 0, 3, torrent.RegularFile)},
		[]torrent.Piece{resumePiece(0, 0, data)}, nil)
	writeResumeFile(t, plan, 0, append(append([]byte(nil), data...), []byte("extra")...))
	result, err := ScanResume(context.Background(), selection, plan)
	if err != nil || !result.NoTransferNeeded {
		t.Fatalf("overlong complete result=%+v err=%v", result, err)
	}
	entry, _ := plan.Entry(0)
	got, err := os.ReadFile(entry.Path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("truncated data=%q err=%v", got, err)
	}

	selection, plan = resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "a", 0, 3, torrent.RegularFile), makeResumeFile(1, "b", 3, 6, torrent.RegularFile)},
		[]torrent.Piece{resumePiece(0, 0, []byte("abcdef"))}, nil)
	writeResumeFile(t, plan, 0, append([]byte("abc"), []byte("extra")...))
	entry, _ = plan.Entry(0)
	result, err = ScanResume(context.Background(), selection, plan)
	if err != nil || len(result.PendingTruncations) != 1 || result.PendingTruncations[0] != (PendingTruncation{Index: 0, Size: 3}) {
		t.Fatalf("pending truncation result=%+v err=%v", result, err)
	}
	got, err = os.ReadFile(entry.Path)
	if err != nil || !bytes.Equal(got, []byte("abcextra")) {
		t.Fatalf("pending file changed=%q err=%v", got, err)
	}
	if err := plan.TruncateSelected(result.PendingTruncations[0].Index, result.PendingTruncations[0].Size); err != nil {
		t.Fatalf("apply pending truncation: %v", err)
	}
	got, err = os.ReadFile(entry.Path)
	if err != nil || !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("applied pending truncation data=%q err=%v", got, err)
	}
}

func TestScanResumeCancellationAndTruncateFailure(t *testing.T) {
	data := []byte("data")
	selection, plan := resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "a", 0, 4, torrent.RegularFile)},
		[]torrent.Piece{resumePiece(0, 0, data)}, nil)
	writeResumeFile(t, plan, 0, data)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ScanResume(ctx, selection, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan error=%v", err)
	}

	writeResumeFile(t, plan, 0, append(append([]byte(nil), data...), []byte("extra")...))
	previous := openOutputFile
	openOutputFile = func(string, int, os.FileMode) (outputFile, error) { return nil, errors.New("truncate open failure") }
	t.Cleanup(func() { openOutputFile = previous })
	if _, err := ScanResume(context.Background(), selection, plan); err == nil || !bytes.Contains([]byte(err.Error()), []byte("truncate open failure")) {
		t.Fatalf("truncate failure error=%v", err)
	}
}

func TestScanResumePropagatesReadFailure(t *testing.T) {
	selection, plan := resumeSelection(t,
		[]torrent.File{makeResumeFile(0, "a", 0, 4, torrent.RegularFile)},
		[]torrent.Piece{resumePiece(0, 0, []byte("data"))}, nil)
	writeResumeFile(t, plan, 0, []byte("data"))
	previous := readResumeAt
	readResumeAt = func(*Plan, int, int64, []byte) (int, error) {
		return 0, errors.New("resume read failure")
	}
	t.Cleanup(func() { readResumeAt = previous })
	if _, err := ScanResume(context.Background(), selection, plan); err == nil || !bytes.Contains([]byte(err.Error()), []byte("resume read failure")) {
		t.Fatalf("read failure error=%v", err)
	}
}
