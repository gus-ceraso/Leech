package storage

// This file owns the on-disk piece workspace.  A Stager deliberately has no
// goroutines: the session coordinator owns coverage and endpoint provenance,
// and hands immutable snapshots to the serialized finalizer in finalize.go.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

var (
	// ErrStagerNotStarted is returned before Start creates the transfer
	// workspace.
	ErrStagerNotStarted = errors.New("storage: staging has not started")
	// ErrStagerClosed is returned after the current run has been cleaned up.
	ErrStagerClosed = errors.New("storage: staging is closed")
	// ErrPieceAlreadyStaged identifies a duplicate piece admission.
	ErrPieceAlreadyStaged = errors.New("storage: piece is already staged")
	// ErrStagingLimit identifies a piece or byte budget admission failure.
	ErrStagingLimit = errors.New("storage: staging budget exhausted")
	// ErrInvalidPiece identifies a piece whose declared range cannot be staged.
	ErrInvalidPiece = errors.New("storage: invalid piece")
	// ErrStagingFatal identifies a cache I/O failure remembered by the stager.
	ErrStagingFatal = errors.New("storage: staging failed")
)

// StagingFile is the narrow file contract used by a staged piece. RunConfig
// can provide an opener for deterministic session-level storage failures.
type StagingFile interface {
	WriteAt([]byte, int64) (int, error)
	ReadAt([]byte, int64) (int, error)
	Truncate(int64) error
	Close() error
}

type stagingFile = StagingFile

var (
	openStagingFile = func(path string, flag int, mode os.FileMode) (stagingFile, error) {
		return os.OpenFile(path, flag, mode)
	}
	removeStagedFile  = os.Remove
	removeStagingRoot = os.Remove
)

// StagerConfig contains the local cache limits. A zero field selects the
// supported default. CacheRoot is used as the parent of this run's random
// private directory; an empty value uses os.UserCacheDir()/leech.
type StagerConfig struct {
	CacheRoot string
	MaxPieces int
	MaxBytes  int64
	// OpenFile optionally opens staged piece files. A nil opener uses the
	// package's standard file opener.
	OpenFile func(path string, flag int, mode os.FileMode) (StagingFile, error)
}

func (c StagerConfig) normalized() (StagerConfig, error) {
	if c.MaxPieces == 0 {
		c.MaxPieces = limits.StagedPieces
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = limits.StagedBytes
	}
	if c.MaxPieces < 1 || c.MaxPieces > limits.StagedPieces {
		return StagerConfig{}, fmt.Errorf("storage: max staged pieces must be in 1..%d", limits.StagedPieces)
	}
	if c.MaxBytes < 1 || c.MaxBytes > limits.StagedBytes {
		return StagerConfig{}, fmt.Errorf("storage: max staged bytes must be in 1..%d", limits.StagedBytes)
	}
	return c, nil
}

// Stager owns the current run's random workspace and piece files. It is safe
// to call from one coordinator goroutine; its small mutex also makes cleanup
// and a finalizer call race-safe. Piece coverage and provenance are never
// stored here.
type Stager struct {
	mu        sync.Mutex
	finalize  sync.Mutex
	config    StagerConfig
	configErr error
	started   bool
	closed    bool
	root      string
	pieces    map[int]*PieceStage
	bytes     int64
	fatal     error
	closeErr  error
}

// NewStager constructs an inert stager. It performs no filesystem operation;
// call Start only after metadata, selection, and resume establish that
// transfer is needed.
func NewStager(config ...StagerConfig) *Stager {
	c := StagerConfig{}
	if len(config) > 1 {
		return &Stager{configErr: errors.New("storage: NewStager accepts at most one config")}
	}
	if len(config) == 1 {
		c = config[0]
	}
	normalized, err := c.normalized()
	if err != nil {
		return &Stager{config: c, configErr: err}
	}
	return &Stager{config: normalized, pieces: make(map[int]*PieceStage)}
}

// Start creates this run's private cache workspace. The operation is
// idempotent, and is the explicit transfer-needed boundary.
func (s *Stager) Start(ctx context.Context) error {
	if s == nil {
		return ErrStagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.configErr != nil {
		return s.configErr
	}
	if s.closed {
		return ErrStagerClosed
	}
	if s.started {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parent, err := s.cacheParent()
	if err != nil {
		return s.rememberFatalLocked(err)
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return s.rememberFatalLocked(fmt.Errorf("storage: create cache parent %q: %w", parent, err))
	}
	workspace, err := makeWorkspace(parent)
	if err != nil {
		return s.rememberFatalLocked(err)
	}
	s.root = workspace
	s.started = true
	s.pieces = make(map[int]*PieceStage)
	return nil
}

func (s *Stager) cacheParent() (string, error) {
	if s.config.CacheRoot != "" {
		return filepath.Clean(s.config.CacheRoot), nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("storage: user cache directory: %w", err)
	}
	if cache == "" {
		return "", errors.New("storage: user cache directory is empty")
	}
	return filepath.Join(cache, "leech"), nil
}

func makeWorkspace(parent string) (string, error) {
	// A random directory is used instead of a predictable torrent or info-hash
	// name. The retry count is finite, so a hostile or exhausted cache cannot
	// turn admission into unbounded work.
	for attempt := 0; attempt < 16; attempt++ {
		name, err := randomName()
		if err != nil {
			return "", fmt.Errorf("storage: random workspace name: %w", err)
		}
		path := filepath.Join(parent, name)
		err = os.Mkdir(path, 0o700)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("storage: create workspace: %w", err)
		}
		return path, nil
	}
	return "", errors.New("storage: unable to create a random workspace")
}

func randomName() (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	const hex = "0123456789abcdef"
	name := make([]byte, len(raw)*2)
	for i, b := range raw {
		name[2*i] = hex[b>>4]
		name[2*i+1] = hex[b&15]
	}
	return string(name), nil
}

// Workspace returns this run's workspace, or an empty string before Start.
func (s *Stager) Workspace() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.root
}

// Fatal returns the first fatal cache error, if any.
func (s *Stager) Fatal() error {
	if s == nil {
		return ErrStagerClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fatal
}

// AdmitPiece reserves the declared piece count and bytes before creating its
// file. A piece file is sparse and fixed to the declared length, so padding
// bytes read as synthetic zeroes without a whole-piece allocation.
func (s *Stager) AdmitPiece(piece torrent.Piece) (*PieceStage, error) {
	if s == nil {
		return nil, ErrStagerClosed
	}
	length, err := pieceLength(piece)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(); err != nil {
		return nil, err
	}
	if _, exists := s.pieces[piece.Index]; exists {
		return nil, ErrPieceAlreadyStaged
	}
	if len(s.pieces) >= s.config.MaxPieces || length > s.config.MaxBytes-s.bytes {
		return nil, fmt.Errorf("%w: pieces=%d/%d bytes=%d/%d", ErrStagingLimit, len(s.pieces), s.config.MaxPieces, s.bytes, s.config.MaxBytes)
	}
	name, err := randomName()
	if err != nil {
		return nil, s.rememberFatalLocked(fmt.Errorf("storage: random piece name: %w", err))
	}
	path := filepath.Join(s.root, name+".piece")
	open := s.config.OpenFile
	if open == nil {
		open = openStagingFile
	}
	f, err := open(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, s.rememberFatalLocked(fmt.Errorf("storage: create staged piece: %w", err))
	}
	if err := f.Truncate(length); err != nil {
		closeErr := f.Close()
		removeErr := removeStagedFile(path)
		return nil, s.rememberFatalLocked(errors.Join(fmt.Errorf("storage: size staged piece: %w", err), closeErr, removeErr))
	}
	stage := &PieceStage{owner: s, piece: piece, length: length, path: path, file: f}
	s.pieces[piece.Index] = stage
	s.bytes += length
	return stage, nil
}

func (s *Stager) readyLocked() error {
	if s.configErr != nil {
		return s.configErr
	}
	if s.closed {
		return ErrStagerClosed
	}
	if s.fatal != nil {
		return s.fatal
	}
	if !s.started {
		return ErrStagerNotStarted
	}
	return nil
}

func (s *Stager) rememberFatalLocked(err error) error {
	if err == nil {
		return nil
	}
	if s.fatal == nil {
		s.fatal = errors.Join(ErrStagingFatal, err)
	}
	return s.fatal
}

func (s *Stager) rememberFatal(err error) error {
	if s == nil || err == nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rememberFatalLocked(err)
}

func pieceLength(piece torrent.Piece) (int64, error) {
	if piece.Index < 0 || piece.Range.Begin < 0 || piece.Range.End < piece.Range.Begin {
		return 0, fmt.Errorf("%w: index or range is invalid", ErrInvalidPiece)
	}
	length := piece.Range.End - piece.Range.Begin
	if length <= 0 || length > limits.PieceBytes {
		return 0, fmt.Errorf("%w: length %d is outside 1..%d", ErrInvalidPiece, length, limits.PieceBytes)
	}
	return length, nil
}

// PieceStage owns one random-access staged piece file. A successful WriteBlock
// means the caller's buffer is no longer used and may be reused immediately.
// The coordinator retains block coverage and endpoint provenance separately.
type PieceStage struct {
	mu     sync.Mutex
	owner  *Stager
	piece  torrent.Piece
	length int64
	path   string
	file   stagingFile
	closed bool
}

// Piece returns the immutable piece declaration.
func (p *PieceStage) Piece() torrent.Piece {
	if p == nil {
		return torrent.Piece{}
	}
	return p.piece
}

// Path returns the private staged file path for diagnostics and local tests.
func (p *PieceStage) Path() string {
	if p == nil {
		return ""
	}
	return p.path
}

// Length returns the declared piece length.
func (p *PieceStage) Length() int64 {
	if p == nil {
		return 0
	}
	return p.length
}

// WriteBlock writes one bounded block at a piece-relative offset. A short
// write, invalid range, or I/O error is fatal to the current session.
func (p *PieceStage) WriteBlock(begin int64, data []byte) error {
	return p.WriteBlockContext(context.Background(), begin, data)
}

// WriteBlockContext is WriteBlock with cancellation checked before the disk
// operation and between retries for a short write.
func (p *PieceStage) WriteBlockContext(ctx context.Context, begin int64, data []byte) error {
	if p == nil {
		return ErrStagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(data) == 0 || len(data) > limits.BlockBytes {
		return p.fail(fmt.Errorf("storage: block length %d is outside 1..%d", len(data), limits.BlockBytes))
	}
	if begin < 0 || int64(len(data)) > p.length-begin {
		return p.fail(fmt.Errorf("storage: block range [%d,%d) is outside piece length %d", begin, begin+int64(len(data)), p.length))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep the owner-before-piece lock order used by cleanup. This keeps a
	// concurrent cancellation from deadlocking with a block write.
	p.owner.mu.Lock()
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.owner.mu.Unlock()
	if p.closed {
		return ErrStagerClosed
	}
	if p.owner.fatal != nil {
		return p.owner.fatal
	}
	var writeErr error
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := p.file.WriteAt(data, begin)
		if n < 0 || n > len(data) {
			return p.owner.rememberFatalLocked(fmt.Errorf("storage: staged piece returned invalid write count %d", n))
		}
		if n != len(data) {
			return p.owner.rememberFatalLocked(errors.Join(fmt.Errorf("storage: staged piece short write: wrote %d of %d bytes", n, len(data)), err))
		}
		begin += int64(n)
		data = data[n:]
		if err != nil {
			writeErr = fmt.Errorf("storage: write staged piece: %w", err)
			break
		}
		if n == 0 {
			writeErr = io.ErrShortWrite
			break
		}
	}
	if writeErr != nil {
		return p.owner.rememberFatalLocked(writeErr)
	}
	return nil
}

// ReadAt reads staged bytes for bounded verification or diagnostics. It does
// not expose a whole-piece read operation.
func (p *PieceStage) ReadAt(dst []byte, begin int64) (int, error) {
	if p == nil {
		return 0, ErrStagerClosed
	}
	if begin < 0 || int64(len(dst)) > p.length-begin {
		return 0, fmt.Errorf("storage: read range is outside staged piece")
	}
	p.owner.mu.Lock()
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.owner.mu.Unlock()
	if p.closed {
		return 0, ErrStagerClosed
	}
	if p.owner.fatal != nil {
		return 0, p.owner.fatal
	}
	n, err := p.file.ReadAt(dst, begin)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, p.owner.rememberFatalLocked(fmt.Errorf("storage: read staged piece: %w", err))
	}
	return n, err
}

func (p *PieceStage) fail(err error) error {
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	return p.owner.rememberFatalLocked(err)
}

// Abort removes this staged piece and releases its budget. It is idempotent.
func (p *PieceStage) Abort() error {
	if p == nil {
		return ErrStagerClosed
	}
	p.owner.finalize.Lock()
	defer p.owner.finalize.Unlock()
	return p.abort()
}

// abort is called while the owner's finalization lock is already held.
func (p *PieceStage) abort() error { return p.owner.removePiece(p, true) }

// StagedCount and StagedBytes report current reservations.
func (s *Stager) StagedCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pieces)
}

func (s *Stager) StagedBytes() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (s *Stager) removePiece(p *PieceStage, removeFile bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p == nil {
		return nil
	}
	current, ok := s.pieces[p.piece.Index]
	if !ok || current != p {
		return nil
	}
	p.mu.Lock()
	var operationErr error
	if !p.closed {
		closeErr := p.file.Close()
		p.closed = true
		if closeErr != nil {
			operationErr = errors.Join(operationErr, closeErr)
			s.rememberFatalLocked(fmt.Errorf("storage: close staged piece: %w", closeErr))
		}
	}
	var removeErr error
	if removeFile {
		removeErr = removeStagedFile(p.path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		if removeErr != nil {
			operationErr = errors.Join(operationErr, removeErr)
			s.rememberFatalLocked(fmt.Errorf("storage: remove staged piece: %w", removeErr))
		}
	}
	p.mu.Unlock()
	delete(s.pieces, p.piece.Index)
	s.bytes -= p.length
	if s.bytes < 0 {
		s.bytes = 0
	}
	return operationErr
}

// Close closes active stage files and removes only this run's workspace. It
// is idempotent and returns all cleanup failures. Cleanup never scans the
// cache parent or touches abandoned workspaces from previous runs.
func (s *Stager) Close() error {
	if s == nil {
		return nil
	}
	// Finalization owns this lock for its complete verify/write/close/remove
	// sequence. Waiting here keeps workspace removal from racing an active
	// output operation.
	s.finalize.Lock()
	defer s.finalize.Unlock()
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	pieces := make([]*PieceStage, 0, len(s.pieces))
	for _, piece := range s.pieces {
		pieces = append(pieces, piece)
	}
	root := s.root
	primary := s.fatal
	s.mu.Unlock()

	var cleanupErr error
	for _, piece := range pieces {
		cleanupErr = errors.Join(cleanupErr, s.removePieceForClose(piece))
	}
	if root != "" {
		if err := removeStagingRoot(root); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("storage: remove workspace: %w", err))
		}
	}
	s.mu.Lock()
	s.closeErr = errors.Join(primary, cleanupErr)
	s.mu.Unlock()
	return s.closeErr
}

// Cleanup is the primary-result aware form of Close. Cleanup errors are
// joined after the caller's primary error and therefore never hide it.
func (s *Stager) Cleanup(primary error) error {
	return errors.Join(primary, s.Close())
}

func (s *Stager) removePieceForClose(p *PieceStage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p == nil {
		return nil
	}
	p.mu.Lock()
	var err error
	if !p.closed {
		err = errors.Join(err, p.file.Close())
		p.closed = true
	}
	removeErr := removeStagedFile(p.path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	err = errors.Join(err, removeErr)
	p.mu.Unlock()
	delete(s.pieces, p.piece.Index)
	s.bytes -= p.length
	if s.bytes < 0 {
		s.bytes = 0
	}
	return err
}
