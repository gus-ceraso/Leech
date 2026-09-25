package storage

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	// ErrIncompletePiece means that the coordinator snapshot does not cover
	// every non-padding byte required to verify the piece.
	ErrIncompletePiece = errors.New("storage: piece coverage is incomplete")
	// ErrPieceHashMismatch means that a complete staged piece failed its v1
	// SHA-1 check. The piece is removed and may be scheduled again.
	ErrPieceHashMismatch = errors.New("storage: piece hash mismatch")
	// ErrInvalidCoverage identifies overlapping, out-of-range, or oversized
	// coordinator coverage records.
	ErrInvalidCoverage = errors.New("storage: invalid piece coverage")
)

// Endpoint identifies a resolved peer address that contributed a block. It
// deliberately uses the same address/port identity for TCP and uTP. The
// coordinator owns the mutable strike table; finalization only returns a
// copied list of contributors.
type Endpoint struct {
	Addr netip.Addr
	Port uint16
}

// BlockCoverage is immutable coordinator data describing one piece-relative
// block accepted into a staged file. The data buffer itself belongs to the
// caller until WriteBlock returns; this value contains no payload bytes.
type BlockCoverage struct {
	Begin        int64
	Length       int64
	Contributors []Endpoint
}

// PieceSnapshot is the immutable handoff from the coordinator to the
// serialized finalizer. Blocks are piece-relative, while torrent.Piece and
// PiecePlan ranges are in concatenated torrent byte space.
type PieceSnapshot struct {
	Piece  torrent.Piece
	Blocks []BlockCoverage
}

// NewPieceSnapshot copies coverage and contributor slices so later coordinator
// mutations cannot affect a finalization already admitted by the caller.
func NewPieceSnapshot(piece torrent.Piece, blocks []BlockCoverage) PieceSnapshot {
	copyBlocks := make([]BlockCoverage, len(blocks))
	for i, block := range blocks {
		copyBlocks[i] = block
		copyBlocks[i].Contributors = append([]Endpoint(nil), block.Contributors...)
	}
	return PieceSnapshot{Piece: piece, Blocks: copyBlocks}
}

// FinalizeResult reports the verified piece and every unique endpoint that
// contributed to it. The caller applies any corruption strikes.
type FinalizeResult struct {
	Piece        torrent.Piece
	Contributors []Endpoint
	// OutputCommitted is true when a verified piece's selected bytes were
	// written and all output handles closed, even if later stage removal fails.
	OutputCommitted bool
}

// Finalize verifies one complete staged piece, writes only its selected
// intersections, closes all output handles, and then removes the stage. It
// serializes calls across a stager. A cache or output error leaves the stage
// in place for cleanup, while a hash mismatch removes it for rescheduling.
// The caller prepares the validated output plan once before transfer; that
// preparation creates selected zero-length files, which do not intersect any
// piece and therefore are not opened here.
func (s *Stager) Finalize(ctx context.Context, snapshot PieceSnapshot, mapping torrent.PiecePlan, plan *Plan) (FinalizeResult, error) {
	if s == nil {
		return FinalizeResult{}, ErrStagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.finalize.Lock()
	defer s.finalize.Unlock()

	stage, err := s.stageFor(snapshot.Piece)
	if err != nil {
		return FinalizeResult{}, err
	}
	if plan == nil {
		return FinalizeResult{}, errors.New("storage: nil output plan")
	}
	if err := validatePieceMapping(stage.Piece(), mapping); err != nil {
		return FinalizeResult{}, err
	}
	if err := validateCoverage(snapshot, mapping); err != nil {
		return FinalizeResult{}, err
	}
	contributors := snapshotContributors(snapshot)
	if err := ctx.Err(); err != nil {
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, err
	}
	actual, err := stageHash(ctx, stage)
	if err != nil {
		if ctx.Err() == nil {
			s.rememberFatal(err)
		}
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, err
	}
	if actual != snapshot.Piece.Hash {
		removeErr := stage.abort()
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, errors.Join(
			fmt.Errorf("%w: piece %d", ErrPieceHashMismatch, snapshot.Piece.Index), removeErr)
	}
	writeErr := writeSelected(ctx, stage, mapping.Selected, plan)
	if writeErr != nil {
		if ctx.Err() == nil {
			s.rememberFatal(writeErr)
		}
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, writeErr
	}
	outputCommitted := hasSelectedBytes(mapping.Selected)
	if err := ctx.Err(); err != nil {
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors, OutputCommitted: outputCommitted}, err
	}
	if err := stage.abort(); err != nil {
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors, OutputCommitted: outputCommitted}, err
	}
	return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors, OutputCommitted: outputCommitted}, nil
}

func hasSelectedBytes(selected []torrent.FileRange) bool {
	for _, span := range selected {
		if span.Range.End > span.Range.Begin {
			return true
		}
	}
	return false
}

// Verify checks a staged piece without touching output. It is useful for a
// coordinator that commits output through a different serialized operation.
// It still requires a complete coverage snapshot and removes mismatching
// stages, just like Finalize.
func (s *Stager) Verify(ctx context.Context, snapshot PieceSnapshot, mapping torrent.PiecePlan) (FinalizeResult, error) {
	if s == nil {
		return FinalizeResult{}, ErrStagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.finalize.Lock()
	defer s.finalize.Unlock()
	stage, err := s.stageFor(snapshot.Piece)
	if err != nil {
		return FinalizeResult{}, err
	}
	if err := validatePieceMapping(stage.Piece(), mapping); err != nil {
		return FinalizeResult{}, err
	}
	if err := validateCoverage(snapshot, mapping); err != nil {
		return FinalizeResult{}, err
	}
	contributors := snapshotContributors(snapshot)
	actual, err := stageHash(ctx, stage)
	if err != nil {
		if ctx.Err() == nil {
			s.rememberFatal(err)
		}
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, err
	}
	if actual != snapshot.Piece.Hash {
		return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, errors.Join(
			fmt.Errorf("%w: piece %d", ErrPieceHashMismatch, snapshot.Piece.Index), stage.abort())
	}
	return FinalizeResult{Piece: snapshot.Piece, Contributors: contributors}, nil
}

func (s *Stager) stageFor(piece torrent.Piece) (*PieceStage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(); err != nil {
		return nil, err
	}
	stage := s.pieces[piece.Index]
	if stage == nil {
		return nil, fmt.Errorf("storage: piece %d is not staged", piece.Index)
	}
	return stage, nil
}

func validatePieceMapping(staged torrent.Piece, mapping torrent.PiecePlan) error {
	if staged != mapping.Piece || staged.Hash != mapping.Piece.Hash {
		return fmt.Errorf("storage: finalizer piece mapping does not match staged piece")
	}
	return nil
}

func validateCoverage(snapshot PieceSnapshot, mapping torrent.PiecePlan) error {
	length := snapshot.Piece.Range.End - snapshot.Piece.Range.Begin
	blocks := make([]BlockCoverage, len(snapshot.Blocks))
	for i, block := range snapshot.Blocks {
		if block.Begin < 0 || block.Length <= 0 || block.Length > limits.BlockBytes || block.Begin > length-block.Length {
			return fmt.Errorf("%w: block %d is outside piece length %d", ErrInvalidCoverage, i, length)
		}
		blocks[i] = block
		blocks[i].Contributors = append([]Endpoint(nil), block.Contributors...)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Begin < blocks[j].Begin })
	for i := 1; i < len(blocks); i++ {
		if blocks[i-1].Begin+blocks[i-1].Length > blocks[i].Begin {
			return fmt.Errorf("%w: overlapping blocks", ErrInvalidCoverage)
		}
	}
	for _, block := range blocks {
		if !coveredByData(block.Begin, block.Begin+block.Length, snapshot.Piece.Range.Begin, mapping.Data) {
			return fmt.Errorf("%w: block [%d,%d) includes padding or unmapped bytes", ErrInvalidCoverage, block.Begin, block.Begin+block.Length)
		}
	}
	for _, data := range mapping.Data {
		begin := data.Range.Begin - snapshot.Piece.Range.Begin
		end := data.Range.End - snapshot.Piece.Range.Begin
		if !coveredByBlocks(begin, end, blocks) {
			return fmt.Errorf("%w: data range [%d,%d) is not covered", ErrIncompletePiece, begin, end)
		}
	}
	return nil
}

func coveredByData(begin, end, pieceBegin int64, data []torrent.FileRange) bool {
	globalBegin, globalEnd := begin+pieceBegin, end+pieceBegin
	cursor := globalBegin
	for _, span := range data {
		if span.Range.End <= cursor {
			continue
		}
		if span.Range.Begin > cursor {
			return false
		}
		if span.Range.End > cursor {
			cursor = span.Range.End
		}
		if cursor >= globalEnd {
			return true
		}
	}
	return cursor >= globalEnd
}

func coveredByBlocks(begin, end int64, blocks []BlockCoverage) bool {
	covered := begin
	for _, block := range blocks {
		if block.Begin > covered {
			break
		}
		if block.Begin+block.Length > covered {
			covered = block.Begin + block.Length
		}
		if covered >= end {
			return true
		}
	}
	return covered >= end
}

func snapshotContributors(snapshot PieceSnapshot) []Endpoint {
	result := make([]Endpoint, 0)
	seen := make(map[Endpoint]struct{})
	for _, block := range snapshot.Blocks {
		for _, endpoint := range block.Contributors {
			if _, ok := seen[endpoint]; ok {
				continue
			}
			seen[endpoint] = struct{}{}
			result = append(result, endpoint)
		}
	}
	return result
}

func stageHash(ctx context.Context, stage *PieceStage) ([20]byte, error) {
	hash := sha1.New()
	buf := make([]byte, limits.BlockBytes)
	var offset int64
	for offset < stage.length {
		if err := ctx.Err(); err != nil {
			return [20]byte{}, err
		}
		want := int64(len(buf))
		if remaining := stage.length - offset; remaining < want {
			want = remaining
		}
		n, err := stage.ReadAt(buf[:int(want)], offset)
		if n > 0 {
			if _, writeErr := hash.Write(buf[:n]); writeErr != nil {
				return [20]byte{}, fmt.Errorf("storage: hash staged piece: %w", writeErr)
			}
			offset += int64(n)
		}
		if err != nil && !(errors.Is(err, io.EOF) && offset == stage.length) {
			return [20]byte{}, fmt.Errorf("storage: read staged piece: %w", err)
		}
		if n == 0 {
			return [20]byte{}, io.ErrUnexpectedEOF
		}
	}
	var result [20]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

func writeSelected(ctx context.Context, stage *PieceStage, selected []torrent.FileRange, plan *Plan) error {
	if plan == nil {
		return errors.New("storage: nil output plan")
	}
	// Open only files intersected by this piece. This keeps finalization
	// bounded by one piece instead of reopening every selected file for every
	// piece, while still closing every handle before stage removal.
	indices := make(map[int]struct{}, len(selected))
	for _, span := range selected {
		if _, ok := plan.Entry(span.Index); !ok {
			return fmt.Errorf("storage: selected piece range references file %d outside output plan", span.Index)
		}
		indices[span.Index] = struct{}{}
	}
	for index := range indices {
		position := plan.byIndex[index]
		if err := plan.checkPath(position); err != nil {
			return err
		}
		entry := plan.entries[position]
		if err := os.MkdirAll(filepath.Dir(entry.Path), 0o755); err != nil {
			return fmt.Errorf("storage: create output directory for %q: %w", entry.Path, err)
		}
	}
	for index := range indices {
		if err := plan.checkPath(plan.byIndex[index]); err != nil {
			return err
		}
	}
	handles := make(map[int]outputFile, len(indices))
	closeHandles := func() error {
		var closeErr error
		for _, handle := range handles {
			closeErr = errors.Join(closeErr, handle.Close())
		}
		return closeErr
	}
	for index := range indices {
		entry, _ := plan.Entry(index)
		handle, err := openOutputFile(entry.Path, os.O_WRONLY|os.O_CREATE, 0o666)
		if err != nil {
			return errors.Join(fmt.Errorf("storage: open %q: %w", entry.Path, err), closeHandles())
		}
		if handle == nil {
			return errors.Join(fmt.Errorf("storage: open %q returned a nil handle", entry.Path), closeHandles())
		}
		handles[index] = handle
	}
	writeErr := error(nil)
	buf := make([]byte, limits.BlockBytes)
	for _, span := range selected {
		for offset := span.Range.Begin; offset < span.Range.End; {
			if err := ctx.Err(); err != nil {
				writeErr = err
				break
			}
			remaining := span.Range.End - offset
			amount := int64(len(buf))
			if remaining < amount {
				amount = remaining
			}
			n, err := stage.ReadAt(buf[:int(amount)], offset-stage.piece.Range.Begin)
			if n > 0 {
				if err := writeOutputRange(plan, handles[span.Index], span.Index, offset, buf[:n]); err != nil {
					writeErr = err
					break
				}
				offset += int64(n)
			}
			if err != nil && !(errors.Is(err, io.EOF) && offset == span.Range.End) {
				writeErr = fmt.Errorf("storage: read staged selected range: %w", err)
				break
			}
			if n == 0 {
				writeErr = io.ErrUnexpectedEOF
				break
			}
		}
		if writeErr != nil {
			break
		}
	}
	return errors.Join(writeErr, closeHandles())
}

func writeOutputRange(plan *Plan, handle outputFile, index int, globalOffset int64, data []byte) error {
	position, ok := plan.byIndex[index]
	if !ok {
		return fmt.Errorf("storage: file index %d is not selected", index)
	}
	entry := plan.entries[position]
	if err := validReadRange(entry.File.Range, globalOffset, len(data)); err != nil {
		return err
	}
	if handle == nil {
		return fmt.Errorf("storage: file index %d has no output handle", index)
	}
	offset := globalOffset - entry.File.Range.Begin
	n, err := handle.WriteAt(data, offset)
	if n < 0 || n > len(data) {
		return fmt.Errorf("storage: invalid short write count %d", n)
	}
	if err != nil {
		return fmt.Errorf("storage: write %q: %w", entry.Path, err)
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
