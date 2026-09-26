package storage

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	ErrResumeInvalid = errors.New("storage: invalid resume plan")
)

// PendingTruncation identifies an overlong selected file that could not be
// truncated yet because one of its selected pieces still needs downloading.
type PendingTruncation struct {
	Index int
	Size  int64
}

// ResumeResult is the bounded result of one stateless resume scan.
type ResumeResult struct {
	VerifiedPieces     []int
	VerifiedRanges     []torrent.FileRange
	RetainedBytes      int64
	PendingTruncations []PendingTruncation
	MissingZeroLength  []int
	NoTransferNeeded   bool
}

type resumeFile struct {
	entry    Entry
	expected int64
	size     int64
	exists   bool
	complete bool
}

type resumeSpan struct {
	begin   int64
	end     int64
	padding bool
	index   int
}

// readResumeAt is a narrow test seam for propagating non-EOF read failures.
var readResumeAt = func(output *Plan, index int, offset int64, dst []byte) (int, error) {
	return output.ReadAt(index, offset, dst)
}

// ScanResume verifies wanted pieces using only selected regular output files
// and synthetic zero padding. It never creates files or a cache workspace.
// Hashing uses bounded block-sized reads. Existing files may be longer than
// their declared torrent range; only the expected prefix participates in the
// scan.
func ScanResume(ctx context.Context, selection *torrent.SelectionPlan, output *Plan) (ResumeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if selection == nil || output == nil {
		return ResumeResult{}, fmt.Errorf("%w: nil selection or output plan", ErrResumeInvalid)
	}
	if err := ctx.Err(); err != nil {
		return ResumeResult{}, err
	}
	files, err := resumeFiles(ctx, selection, output)
	if err != nil {
		return ResumeResult{}, err
	}
	result := ResumeResult{}
	wanted := selection.WantedPieces()
	zeroes := make([]byte, limits.BlockBytes)
	for _, index := range wanted {
		mapping, ok := selection.Piece(index)
		if !ok {
			return result, fmt.Errorf("%w: wanted piece %d is absent from selection plan", ErrResumeInvalid, index)
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		valid, err := hashResumePiece(ctx, mapping, files, output, zeroes)
		if err != nil {
			return result, err
		}
		if !valid {
			for _, span := range mapping.Selected {
				if file := files[span.Index]; file != nil {
					file.complete = false
				}
			}
			continue
		}
		result.VerifiedPieces = append(result.VerifiedPieces, mapping.Piece.Index)
		result.VerifiedRanges = append(result.VerifiedRanges, mapping.Selected...)
		for _, span := range mapping.Selected {
			if file := files[span.Index]; file != nil {
				amount := span.Range.End - span.Range.Begin
				if amount < 0 || result.RetainedBytes > int64(^uint64(0)>>1)-amount {
					return result, fmt.Errorf("%w: retained byte count overflow", ErrResumeInvalid)
				}
				result.RetainedBytes += amount
			}
		}
	}

	for index, file := range files {
		if !file.exists && file.expected == 0 {
			result.MissingZeroLength = append(result.MissingZeroLength, index)
		}
		if !file.exists || file.size <= file.expected {
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if file.complete {
			if err := output.truncateSelected(ctx, index, file.expected); err != nil {
				return result, err
			}
			continue
		}
		result.PendingTruncations = append(result.PendingTruncations, PendingTruncation{Index: index, Size: file.expected})
	}
	sort.Ints(result.MissingZeroLength)
	sort.Slice(result.PendingTruncations, func(i, j int) bool {
		return result.PendingTruncations[i].Index < result.PendingTruncations[j].Index
	})
	result.NoTransferNeeded = len(result.VerifiedPieces) == len(wanted)
	return result, nil
}

func resumeFiles(ctx context.Context, selection *torrent.SelectionPlan, output *Plan) (map[int]*resumeFile, error) {
	files := make(map[int]*resumeFile, len(selection.SelectedFiles()))
	for _, file := range selection.SelectedFiles() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, ok := output.Entry(file.Index)
		if !ok || entry.File != file {
			return nil, fmt.Errorf("%w: selected file index %d is absent from output plan", ErrResumeInvalid, file.Index)
		}
		state := &resumeFile{entry: entry, expected: file.Range.End - file.Range.Begin, complete: true}
		existing, err := output.Existing(file.Index)
		if err != nil {
			return nil, err
		}
		state.exists = existing.Exists
		state.size = existing.Size
		files[file.Index] = state
	}
	return files, nil
}

func hashResumePiece(ctx context.Context, mapping torrent.PiecePlan, files map[int]*resumeFile, output *Plan, zeroes []byte) (bool, error) {
	piece := mapping.Piece
	length := piece.Range.End - piece.Range.Begin
	if piece.Index < 0 || piece.Range.Begin < 0 || length <= 0 || length > limits.PieceBytes {
		return false, fmt.Errorf("%w: piece %d has invalid range", ErrResumeInvalid, piece.Index)
	}
	spans := make([]resumeSpan, 0, len(mapping.Data)+len(mapping.Padding))
	for _, data := range mapping.Data {
		if data.Range.Begin < piece.Range.Begin || data.Range.End > piece.Range.End || data.Range.End <= data.Range.Begin {
			return false, fmt.Errorf("%w: piece %d has invalid data range", ErrResumeInvalid, piece.Index)
		}
		if _, ok := files[data.Index]; !ok {
			return false, nil
		}
		spans = append(spans, resumeSpan{begin: data.Range.Begin, end: data.Range.End, index: data.Index})
	}
	for _, padding := range mapping.Padding {
		if padding.Begin < piece.Range.Begin || padding.End > piece.Range.End || padding.End <= padding.Begin {
			return false, fmt.Errorf("%w: piece %d has invalid padding range", ErrResumeInvalid, piece.Index)
		}
		spans = append(spans, resumeSpan{begin: padding.Begin, end: padding.End, padding: true})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].begin < spans[j].begin })
	hash := sha1.New()
	cursor := piece.Range.Begin
	buf := make([]byte, limits.BlockBytes)
	for _, span := range spans {
		if span.begin != cursor {
			return false, fmt.Errorf("%w: piece %d has a gap or overlap", ErrResumeInvalid, piece.Index)
		}
		cursor = span.end
		if span.padding {
			for remaining := span.end - span.begin; remaining > 0; {
				amount := int64(len(zeroes))
				if remaining < amount {
					amount = remaining
				}
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if _, err := hash.Write(zeroes[:int(amount)]); err != nil {
					return false, fmt.Errorf("storage: hash padding: %w", err)
				}
				remaining -= amount
			}
			continue
		}
		file := files[span.index]
		needed := span.end - file.entry.File.Range.Begin
		if !file.exists || needed < 0 || needed > file.size {
			return false, nil
		}
		for offset := span.begin; offset < span.end; {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			amount := int64(len(buf))
			if remaining := span.end - offset; remaining < amount {
				amount = remaining
			}
			n, err := readResumeAt(output, span.index, offset, buf[:int(amount)])
			if n > 0 {
				if _, writeErr := hash.Write(buf[:n]); writeErr != nil {
					return false, fmt.Errorf("storage: hash output: %w", writeErr)
				}
				offset += int64(n)
			}
			if err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
					return false, nil
				}
				return false, fmt.Errorf("storage: read resume output: %w", err)
			}
			if n != int(amount) {
				return false, nil
			}
		}
	}
	if cursor != piece.Range.End {
		return false, fmt.Errorf("%w: piece %d is not fully covered", ErrResumeInvalid, piece.Index)
	}
	var actual [20]byte
	copy(actual[:], hash.Sum(nil))
	return actual == piece.Hash, nil
}

// TruncateSelected applies the expected torrent length to one selected output
// file after its selected pieces have been verified and written. The size must
// equal the file's torrent length; the operation remains confined to the
// validated output path.
func (p *Plan) TruncateSelected(index int, size int64) error {
	return p.truncateSelected(context.Background(), index, size)
}

func (p *Plan) truncateSelected(ctx context.Context, index int, size int64) error {
	position, ok := p.byIndex[index]
	if !ok {
		return fmt.Errorf("%w: file index %d is absent from output plan", ErrResumeInvalid, index)
	}
	entry := p.entries[position]
	expected := entry.File.Range.End - entry.File.Range.Begin
	if size < 0 || size != expected {
		return fmt.Errorf("%w: file index %d has truncation size %d, want %d", ErrResumeInvalid, index, size, expected)
	}
	if err := p.checkPath(position); err != nil {
		return err
	}
	if err := detachOutput(ctx, entry.Path, size); err != nil {
		return err
	}
	handle, err := openOutputFile(entry.Path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("storage: open %q for resume truncation: %w", entry.Path, err)
	}
	if handle == nil {
		return fmt.Errorf("storage: open %q for resume truncation returned nil", entry.Path)
	}
	truncateErr := handle.Truncate(size)
	closeErr := handle.Close()
	if truncateErr != nil || closeErr != nil {
		var operationErr error
		if truncateErr != nil {
			operationErr = fmt.Errorf("storage: truncate %q: %w", entry.Path, truncateErr)
		}
		return errors.Join(operationErr, closeErr)
	}
	return nil
}
