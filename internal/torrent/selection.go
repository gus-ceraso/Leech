package torrent

import (
	"errors"
	"fmt"
	pathpkg "path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	// ErrInvalidSelection identifies a selector or BEP 53 index that cannot be
	// applied to a normalized file table.
	ErrInvalidSelection = errors.New("invalid file selection")
	// ErrInvalidPattern identifies a malformed --file pattern.
	ErrInvalidPattern = errors.New("invalid file pattern")
	// ErrSelectionNoMatch means that a selection produced no regular output
	// file. Padding and symlink entries do not count as output files.
	ErrSelectionNoMatch = errors.New("file selection matches no regular files")
	// ErrSymlinkSelection means that a selector explicitly selected a symlink
	// entry. Leech never creates torrent-provided symlinks.
	ErrSymlinkSelection = errors.New("file selection includes a symlink")
)

// FileRange associates a half-open global torrent range with its original
// file-list index. Index is retained so storage and scheduling do not need to
// interpret paths or reconstruct the file table.
type FileRange struct {
	Index int
	Range ByteRange
}

// PiecePlan is the immutable mapping for one wanted piece. Data contains every
// non-padding span required to verify the complete piece, including unwanted
// regular-file bytes. Selected contains only bytes committed to output;
// Padding contains ranges supplied as synthetic zeroes and never requested
// from a peer.
type PiecePlan struct {
	Piece    Piece
	Data     []FileRange
	Selected []FileRange
	Padding  []ByteRange
}

// SelectionPlan is an immutable selection and storage mapping. All inspection
// methods return copies, so callers cannot change a plan after construction.
// Construct it with Select.
type SelectionPlan struct {
	selected []File
	wanted   []int
	pieces   []PiecePlan
}

// Select resolves explicit path patterns, a magnet's BEP 53 selection, or all
// regular files, in that order of precedence. Patterns are case-sensitive,
// use '/' separators, and support '*', '?', and bracket classes. A pattern
// that matches a directory selects its descendants.
//
// The returned plan retains original file-list indices, including padding and
// symlink entries, while selecting only regular files for output. A symlink
// selected by either patterns or BEP 53 is an error. A nil or empty explicit
// pattern list means that no --file option was supplied; a nonempty so list is
// then applied when present.
func Select(meta Metainfo, patterns []string, so []IndexRange) (*SelectionPlan, error) {
	selected := make([]bool, len(meta.Files))
	switch {
	case len(patterns) > 0:
		if err := selectPatterns(meta, patterns, selected); err != nil {
			return nil, err
		}
	case len(so) > 0:
		if err := selectRanges(meta, so, selected); err != nil {
			return nil, err
		}
	default:
		for i, file := range meta.Files {
			selected[i] = file.Kind == RegularFile
		}
	}

	selectedFiles := make([]File, 0, len(meta.Files))
	for i, file := range meta.Files {
		if !selected[i] {
			continue
		}
		if file.Kind == SymlinkFile {
			return nil, fmt.Errorf("%w: file index %d", ErrSymlinkSelection, i)
		}
		if file.Kind == RegularFile {
			selectedFiles = append(selectedFiles, file)
		}
	}
	if len(selectedFiles) == 0 {
		return nil, ErrSelectionNoMatch
	}

	plan, err := buildPlan(meta, selected)
	if err != nil {
		return nil, err
	}
	plan.selected = selectedFiles
	return plan, nil
}

// NewSelection is an alias for Select for callers that prefer a constructor
// name.
func NewSelection(meta Metainfo, patterns []string, so []IndexRange) (*SelectionPlan, error) {
	return Select(meta, patterns, so)
}

// BuildSelection is a descriptive alias used by session components.
func BuildSelection(meta Metainfo, patterns []string, so []IndexRange) (*SelectionPlan, error) {
	return Select(meta, patterns, so)
}

// SelectedIndices returns the selected regular file indices in original
// torrent order.
func (p *SelectionPlan) SelectedIndices() []int {
	if p == nil {
		return nil
	}
	indices := make([]int, len(p.selected))
	for i, file := range p.selected {
		indices[i] = file.Index
	}
	return indices
}

// SelectedFiles returns selected regular files in original torrent order.
func (p *SelectionPlan) SelectedFiles() []File {
	if p == nil {
		return nil
	}
	files := make([]File, len(p.selected))
	copy(files, p.selected)
	return files
}

// WantedPieces returns wanted piece indices in torrent order.
func (p *SelectionPlan) WantedPieces() []int {
	if p == nil {
		return nil
	}
	indices := make([]int, len(p.wanted))
	copy(indices, p.wanted)
	return indices
}

// WantedPieceIndices is a descriptive alias for WantedPieces.
func (p *SelectionPlan) WantedPieceIndices() []int { return p.WantedPieces() }

// Piece returns a copy of one wanted piece mapping. The bool is false when
// index is not wanted.
func (p *SelectionPlan) Piece(index int) (PiecePlan, bool) {
	if p == nil {
		return PiecePlan{}, false
	}
	position := sort.SearchInts(p.wanted, index)
	if position < len(p.wanted) && p.wanted[position] == index {
		return clonePiecePlan(p.pieces[position]), true
	}
	return PiecePlan{}, false
}

// PiecePlans returns all wanted piece mappings in torrent order.
func (p *SelectionPlan) PiecePlans() []PiecePlan {
	if p == nil {
		return nil
	}
	pieces := make([]PiecePlan, len(p.pieces))
	for i, piece := range p.pieces {
		pieces[i] = clonePiecePlan(piece)
	}
	return pieces
}

// Pieces is an alias for PiecePlans.
func (p *SelectionPlan) Pieces() []PiecePlan { return p.PiecePlans() }

// SelectableFiles returns every regular file path that --file accepts, in
// torrent order. Padding and symlink entries are omitted.
func SelectableFiles(meta Metainfo) []string {
	paths := make([]string, 0, len(meta.Files))
	for _, file := range meta.Files {
		if file.Kind == RegularFile {
			paths = append(paths, file.Path)
		}
	}
	return paths
}

// ListableFiles is an alias for SelectableFiles.
func ListableFiles(meta Metainfo) []string { return SelectableFiles(meta) }

func selectPatterns(meta Metainfo, patterns []string, selected []bool) error {
	if len(patterns) > limits.Files {
		return fmt.Errorf("%w: too many file patterns", ErrInvalidSelection)
	}
	for _, pattern := range patterns {
		if err := validatePattern(pattern); err != nil {
			return err
		}
	}
	for index, file := range meta.Files {
		if file.Path == "" {
			// Padding has no selectable path.
			continue
		}
		for _, pattern := range patterns {
			if pathPatternMatches(pattern, file.Path) {
				selected[index] = true
				break
			}
		}
	}
	return nil
}

func selectRanges(meta Metainfo, ranges []IndexRange, selected []bool) error {
	if len(ranges) > limits.Files {
		return fmt.Errorf("%w: too many BEP 53 ranges", ErrInvalidSelection)
	}
	for _, r := range ranges {
		if r.Start < 0 || r.End < r.Start || r.End >= len(meta.Files) {
			return fmt.Errorf("%w: BEP 53 range %d-%d is outside %d files", ErrInvalidSelection, r.Start, r.End, len(meta.Files))
		}
		for index := r.Start; index <= r.End; index++ {
			selected[index] = true
		}
	}
	return nil
}

func buildPlan(meta Metainfo, selected []bool) (*SelectionPlan, error) {
	if len(meta.Pieces) == 0 {
		return &SelectionPlan{}, nil
	}
	if len(meta.Pieces) > limits.Pieces {
		return nil, fmt.Errorf("%w: too many pieces", ErrInvalidSelection)
	}
	var previousEnd int64
	for i, file := range meta.Files {
		if file.Index != i || file.Range.Begin < 0 || file.Range.End < file.Range.Begin || file.Range.End > meta.TotalLength {
			return nil, fmt.Errorf("%w: invalid range for file index %d", ErrInvalidSelection, i)
		}
		if i > 0 && file.Range.Begin < previousEnd {
			return nil, fmt.Errorf("%w: overlapping file ranges at index %d", ErrInvalidSelection, i)
		}
		previousEnd = file.Range.End
	}
	plan := &SelectionPlan{}
	fileCursor := 0
	var previousPieceEnd int64
	for pieceIndex, piece := range meta.Pieces {
		if piece.Index != pieceIndex || piece.Range.Begin < 0 || piece.Range.End < piece.Range.Begin || piece.Range.End > meta.TotalLength {
			return nil, fmt.Errorf("%w: invalid range for piece index %d", ErrInvalidSelection, pieceIndex)
		}
		if pieceIndex > 0 && piece.Range.Begin < previousPieceEnd {
			return nil, fmt.Errorf("%w: overlapping piece ranges at index %d", ErrInvalidSelection, pieceIndex)
		}
		previousPieceEnd = piece.Range.End
		wanted := false
		mapping := PiecePlan{Piece: piece}
		for fileCursor < len(meta.Files) && meta.Files[fileCursor].Range.End <= piece.Range.Begin {
			fileCursor++
		}
		for index := fileCursor; index < len(meta.Files) && meta.Files[index].Range.Begin < piece.Range.End; index++ {
			file := meta.Files[index]
			span, ok := intersect(piece.Range, file.Range)
			if !ok || span.Begin == span.End {
				continue
			}
			switch file.Kind {
			case PaddingFile:
				mapping.Padding = append(mapping.Padding, span)
			case RegularFile:
				mapping.Data = append(mapping.Data, FileRange{Index: index, Range: span})
				if selected[index] {
					mapping.Selected = append(mapping.Selected, FileRange{Index: index, Range: span})
					wanted = true
				}
			case SymlinkFile:
				// Normalized symlinks have no length. Keep this branch explicit so
				// malformed caller-supplied metadata cannot become payload data.
			default:
				return nil, fmt.Errorf("%w: unknown file kind %d", ErrInvalidSelection, file.Kind)
			}
		}
		if wanted {
			plan.wanted = append(plan.wanted, pieceIndex)
			plan.pieces = append(plan.pieces, mapping)
		}
	}
	return plan, nil
}

func clonePiecePlan(piece PiecePlan) PiecePlan {
	clone := piece
	clone.Data = append([]FileRange(nil), piece.Data...)
	clone.Selected = append([]FileRange(nil), piece.Selected...)
	clone.Padding = append([]ByteRange(nil), piece.Padding...)
	return clone
}

func intersect(a, b ByteRange) (ByteRange, bool) {
	begin, end := a.Begin, a.End
	if b.Begin > begin {
		begin = b.Begin
	}
	if b.End < end {
		end = b.End
	}
	return ByteRange{Begin: begin, End: end}, begin < end
}

func validatePattern(pattern string) error {
	if pattern == "" || len(pattern) > limits.PathBytes || !utf8.ValidString(pattern) {
		return fmt.Errorf("%w: pattern is empty, too long, or invalid UTF-8", ErrInvalidPattern)
	}
	if strings.Contains(pattern, "**") {
		return fmt.Errorf("%w: ** is unsupported", ErrInvalidPattern)
	}
	if strings.ContainsAny(pattern, "\\\x00") || strings.HasPrefix(pattern, "/") {
		return fmt.Errorf("%w: pattern must be relative and use '/' separators", ErrInvalidPattern)
	}
	parts := strings.Split(pattern, "/")
	if len(parts) > limits.PathComponents {
		return fmt.Errorf("%w: pattern has too many components", ErrInvalidPattern)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("%w: pattern has an unsafe path component", ErrInvalidPattern)
		}
	}
	if _, err := pathpkg.Match(pattern, ""); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPattern, err)
	}
	return nil
}

func pathPatternMatches(pattern, candidate string) bool {
	parts := strings.Split(candidate, "/")
	for end := 1; end <= len(parts); end++ {
		prefix := strings.Join(parts[:end], "/")
		matched, err := pathpkg.Match(pattern, prefix)
		if err == nil && matched {
			return true
		}
	}
	return false
}
