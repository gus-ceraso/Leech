// Package storage contains the confined final-output operations used by a
// download session.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

// PrepareMode controls what Prepare does with existing selected files.
type PrepareMode uint8

const (
	// Overwrite truncates every selected regular file before it is used.
	Overwrite PrepareMode = iota
	// Resume preserves the contents and length of existing selected files.
	Resume
)

var (
	// ErrClosed is returned when a prepared output is used after Close.
	ErrClosed = errors.New("storage: output is closed")
	// ErrNoOutput is returned for a plan that contains no selected regular file.
	ErrNoOutput = errors.New("storage: no selected output files")
)

// outputFile is deliberately small. The package-level opener is a narrow
// test seam for concrete open/close failures; production uses *os.File.
type outputFile interface {
	WriteAt([]byte, int64) (int, error)
	Truncate(int64) error
	Close() error
}

var openOutputFile = func(path string, flag int, mode os.FileMode) (outputFile, error) {
	return os.OpenFile(path, flag, mode)
}

// Entry is one selected regular torrent file and its confined output path.
// Index is the original index in Metainfo.Files. Path is absolute and rooted
// below Plan.Root.
type Entry struct {
	Index int
	File  torrent.File
	Path  string
}

// ExistingState describes one selected path without creating or modifying it.
type ExistingState struct {
	Exists bool
	Size   int64
}

// Plan is a validated, immutable selected output plan. Construct one with
// Validate; construction and all inspection methods are read-only.
type Plan struct {
	root    string
	entries []Entry
	parts   [][]string
	byIndex map[int]int
}

// Validate resolves root once and validates every selected output path. It
// never creates, truncates, or writes an output path.
func Validate(root string, meta torrent.Metainfo, selected []int) (*Plan, error) {
	resolved, err := resolveRoot(root)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, ErrNoOutput
	}
	name, err := oneComponent(meta.Name, "torrent name")
	if err != nil {
		return nil, err
	}
	if len(meta.Files) == 0 {
		return nil, fmt.Errorf("storage: metadata has no files")
	}
	if meta.TotalLength < 0 {
		return nil, fmt.Errorf("storage: metadata has a negative total length")
	}
	if !meta.MultiFile && len(selected) != 1 {
		return nil, fmt.Errorf("storage: single-file metadata requires one selected file")
	}

	entries := make([]Entry, 0, len(selected))
	parts := make([][]string, 0, len(selected))
	byIndex := make(map[int]int, len(selected))
	for _, index := range selected {
		if index < 0 || index >= len(meta.Files) {
			return nil, fmt.Errorf("storage: selected file index %d is out of bounds", index)
		}
		if _, duplicate := byIndex[index]; duplicate {
			return nil, fmt.Errorf("storage: selected file index %d is repeated", index)
		}
		file := meta.Files[index]
		if file.Index != index {
			return nil, fmt.Errorf("storage: file index %d does not match metadata index %d", file.Index, index)
		}
		if file.Kind != torrent.RegularFile {
			return nil, fmt.Errorf("storage: selected file index %d is not a regular file", index)
		}
		if err := validRange(file.Range, meta.TotalLength); err != nil {
			return nil, fmt.Errorf("storage: selected file index %d: %w", index, err)
		}

		pathParts := []string{name}
		if meta.MultiFile {
			fileParts, err := relativeParts(file.Path, "file path")
			if err != nil {
				return nil, fmt.Errorf("storage: selected file index %d: %w", index, err)
			}
			pathParts = append(pathParts, fileParts...)
		}
		if len(pathParts) > limits.PathComponents {
			return nil, fmt.Errorf("storage: selected file index %d exceeds path component limit", index)
		}
		if pathBytes(pathParts) > limits.PathBytes {
			return nil, fmt.Errorf("storage: selected file index %d exceeds path byte limit", index)
		}
		path := resolved
		for _, part := range pathParts {
			path = filepath.Join(path, part)
		}
		byIndex[index] = len(entries)
		entries = append(entries, Entry{Index: index, File: file, Path: path})
		parts = append(parts, pathParts)
	}

	if err := rejectCollisions(parts); err != nil {
		return nil, err
	}
	plan := &Plan{root: resolved, entries: entries, parts: parts, byIndex: byIndex}
	if err := plan.checkPaths(); err != nil {
		return nil, err
	}
	return plan, nil
}

// NewPlan is an explicit alias for Validate for callers that prefer a
// constructor-shaped name.
func NewPlan(root string, meta torrent.Metainfo, selected []int) (*Plan, error) {
	return Validate(root, meta, selected)
}

// Root returns the destination directory after one-time symlink resolution.
func (p *Plan) Root() string { return p.root }

// Entries returns a copy of the validated selected entries in caller order.
func (p *Plan) Entries() []Entry {
	entries := make([]Entry, len(p.entries))
	copy(entries, p.entries)
	return entries
}

// Entry returns a selected entry by its original metadata index.
func (p *Plan) Entry(index int) (Entry, bool) {
	position, ok := p.byIndex[index]
	if !ok {
		return Entry{}, false
	}
	return p.entries[position], true
}

// Existing inspects one selected output path without creating or changing it.
// A missing selected path is represented by Exists=false.
func (p *Plan) Existing(index int) (ExistingState, error) {
	position, ok := p.byIndex[index]
	if !ok {
		return ExistingState{}, fmt.Errorf("storage: file index %d is not selected", index)
	}
	if err := p.checkPath(position); err != nil {
		return ExistingState{}, err
	}
	info, err := os.Lstat(p.entries[position].Path)
	if errors.Is(err, os.ErrNotExist) {
		return ExistingState{}, nil
	}
	if err != nil {
		return ExistingState{}, fmt.Errorf("storage: inspect %q: %w", p.entries[position].Path, err)
	}
	if !info.Mode().IsRegular() {
		return ExistingState{}, fmt.Errorf("storage: selected output %q is not a regular file", p.entries[position].Path)
	}
	return ExistingState{Exists: true, Size: info.Size()}, nil
}

// ReadAt reads existing selected output without creating it. The global
// offset must stay inside the selected file's torrent byte range.
func (p *Plan) ReadAt(index int, globalOffset int64, dst []byte) (int, error) {
	position, ok := p.byIndex[index]
	if !ok {
		return 0, fmt.Errorf("storage: file index %d is not selected", index)
	}
	if err := validReadRange(p.entries[position].File.Range, globalOffset, len(dst)); err != nil {
		return 0, err
	}
	if err := p.checkPath(position); err != nil {
		return 0, err
	}
	f, err := os.Open(p.entries[position].Path)
	if err != nil {
		return 0, fmt.Errorf("storage: open %q for reading: %w", p.entries[position].Path, err)
	}
	n, readErr := f.ReadAt(dst, globalOffset-p.entries[position].File.Range.Begin)
	closeErr := f.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return n, errors.Join(readErr, closeErr)
	}
	if closeErr != nil {
		return n, closeErr
	}
	return n, readErr
}

// Prepare creates selected parent directories and opens only selected regular
// files. Overwrite truncation happens after every selected handle is opened;
// Resume leaves existing contents untouched. A failed operation closes every
// handle it already opened.
func (p *Plan) Prepare(mode PrepareMode) (*Prepared, error) {
	if mode != Overwrite && mode != Resume {
		return nil, fmt.Errorf("storage: unknown prepare mode %d", mode)
	}
	if err := p.checkPaths(); err != nil {
		return nil, err
	}
	for _, entry := range p.entries {
		if err := os.MkdirAll(filepath.Dir(entry.Path), 0o755); err != nil {
			return nil, fmt.Errorf("storage: create output directory for %q: %w", entry.Path, err)
		}
	}
	// Recheck after directory creation so an existing incompatible entry is
	// reported before any file is opened or truncated.
	if err := p.checkPaths(); err != nil {
		return nil, err
	}

	handles := make(map[int]outputFile, len(p.entries))
	closeOpened := func() error {
		var closeErr error
		for _, handle := range handles {
			closeErr = errors.Join(closeErr, handle.Close())
		}
		return closeErr
	}
	for _, entry := range p.entries {
		handle, err := openOutputFile(entry.Path, os.O_WRONLY|os.O_CREATE, 0o666)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("storage: open %q: %w", entry.Path, err), closeOpened())
		}
		if handle == nil {
			return nil, errors.Join(fmt.Errorf("storage: open %q returned a nil handle", entry.Path), closeOpened())
		}
		handles[entry.Index] = handle
	}
	if mode == Overwrite {
		for _, entry := range p.entries {
			if err := handles[entry.Index].Truncate(0); err != nil {
				return nil, errors.Join(fmt.Errorf("storage: truncate %q: %w", entry.Path, err), closeOpened())
			}
		}
	}
	return &Prepared{plan: p, handles: handles}, nil
}

// Prepared owns one output handle for every selected file until Close.
type Prepared struct {
	plan     *Plan
	handles  map[int]outputFile
	closed   bool
	closeErr error
}

// WriteRange writes verified data into a selected file. globalOffset is in
// the torrent's concatenated byte space, and may cover only this file's range.
// The write is confined to the selected path and may grow it sparsely.
func (p *Prepared) WriteRange(index int, globalOffset int64, data []byte) error {
	if p.closed {
		return ErrClosed
	}
	position, ok := p.plan.byIndex[index]
	if !ok {
		return fmt.Errorf("storage: file index %d is not selected", index)
	}
	entry := p.plan.entries[position]
	if err := validReadRange(entry.File.Range, globalOffset, len(data)); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	handle := p.handles[index]
	if handle == nil {
		return fmt.Errorf("storage: file index %d has no output handle", index)
	}
	offset := globalOffset - entry.File.Range.Begin
	for len(data) > 0 {
		n, err := handle.WriteAt(data, offset)
		if n < 0 || n > len(data) {
			return fmt.Errorf("storage: invalid short write count %d", n)
		}
		if n > 0 {
			data = data[n:]
			offset += int64(n)
		}
		if err != nil {
			return fmt.Errorf("storage: write %q: %w", entry.Path, err)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Close closes every selected output handle and returns any close failures.
// It is idempotent and repeats the first aggregate result on later calls.
func (p *Prepared) Close() error {
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	for _, handle := range p.handles {
		p.closeErr = errors.Join(p.closeErr, handle.Close())
	}
	return p.closeErr
}

func resolveRoot(root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("storage: output root is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("storage: resolve output root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("storage: resolve output root %q: %w", root, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("storage: stat output root %q: %w", root, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("storage: output root %q is not a directory", root)
	}
	return filepath.Clean(resolved), nil
}

func (p *Plan) checkPaths() error {
	for position := range p.entries {
		if err := p.checkPath(position); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plan) checkPath(position int) error {
	current := p.root
	for partPosition, part := range p.parts[position] {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("storage: inspect %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("storage: descendant symlink %q is not allowed", current)
		}
		last := partPosition == len(p.parts[position])-1
		if last {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("storage: selected output %q is not a regular file", current)
			}
			continue
		}
		if !info.IsDir() {
			return fmt.Errorf("storage: output path component %q is not a directory", current)
		}
	}
	return nil
}

func rejectCollisions(parts [][]string) error {
	order := make([]int, len(parts))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return compareParts(parts[order[i]], parts[order[j]]) < 0
	})
	for i := 1; i < len(order); i++ {
		previous := parts[order[i-1]]
		current := parts[order[i]]
		if hasPartsPrefix(current, previous) {
			return fmt.Errorf("storage: selected output paths collide")
		}
	}
	return nil
}

func compareParts(a, b []string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func hasPartsPrefix(value, prefix []string) bool {
	if len(prefix) > len(value) {
		return false
	}
	for i := range prefix {
		if value[i] != prefix[i] {
			return false
		}
	}
	return true
}

func oneComponent(value, label string) (string, error) {
	parts, err := relativeParts(value, label)
	if err != nil {
		return "", err
	}
	if len(parts) != 1 {
		return "", fmt.Errorf("storage: %s must be one path component", label)
	}
	return parts[0], nil
}

func relativeParts(value, label string) ([]string, error) {
	if value == "" {
		return nil, fmt.Errorf("%s is empty", label)
	}
	if strings.HasPrefix(value, "/") || strings.Contains(value, "\\") {
		return nil, fmt.Errorf("%s is absolute or uses an unsupported separator", label)
	}
	parts := strings.Split(value, "/")
	if len(parts) > limits.PathComponents {
		return nil, fmt.Errorf("%s has too many components", label)
	}
	for _, part := range parts {
		if _, err := oneName(part, label); err != nil {
			return nil, err
		}
	}
	return parts, nil
}

func oneName(value, label string) (string, error) {
	if value == "" || value == "." || value == ".." {
		return "", fmt.Errorf("%s contains an unsafe path component", label)
	}
	if filepath.IsAbs(value) || filepath.VolumeName(value) != "" || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 || strings.ContainsAny(value, "/\\") {
		return "", fmt.Errorf("%s contains an unrepresentable path component", label)
	}
	return value, nil
}

func pathBytes(parts []string) int {
	bytes := 0
	for i, part := range parts {
		if i > 0 {
			bytes++
		}
		bytes += len(part)
	}
	return bytes
}

func validRange(r torrent.ByteRange, total int64) error {
	if r.Begin < 0 || r.End < r.Begin {
		return fmt.Errorf("invalid byte range")
	}
	if total >= 0 && r.End > total {
		return fmt.Errorf("byte range exceeds torrent length")
	}
	return nil
}

func validReadRange(r torrent.ByteRange, globalOffset int64, dataLength int) error {
	if globalOffset < r.Begin || globalOffset > r.End {
		return fmt.Errorf("storage: write range starts outside selected file")
	}
	length := int64(dataLength)
	if length < 0 || length > r.End-globalOffset {
		return fmt.Errorf("storage: write range exceeds selected file")
	}
	return nil
}
