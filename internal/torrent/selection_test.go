package torrent

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func selectionMeta(files []File, pieces []Piece) Metainfo {
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	return Metainfo{Name: "release", MultiFile: true, TotalLength: total, Files: files, Pieces: pieces}
}

func selectionFile(index int, path string, begin, end int64, kind FileKind) File {
	return File{Index: index, Path: path, Range: ByteRange{Begin: begin, End: end}, Kind: kind}
}

func selectionPiece(index int, begin, end int64) Piece {
	return Piece{Index: index, Range: ByteRange{Begin: begin, End: end}}
}

func TestSelectPatternsAndDirectoryDescendants(t *testing.T) {
	meta := selectionMeta(
		[]File{
			selectionFile(0, "dir/a.txt", 0, 2, RegularFile),
			selectionFile(1, "dir/sub/b.bin", 2, 4, RegularFile),
			selectionFile(2, "root.dat", 4, 5, RegularFile),
		},
		[]Piece{selectionPiece(0, 0, 4), selectionPiece(1, 4, 5)},
	)

	tests := []struct {
		name     string
		patterns []string
		want     []int
	}{
		{name: "directory", patterns: []string{"dir"}, want: []int{0, 1}},
		{name: "star directory", patterns: []string{"dir/*"}, want: []int{0, 1}},
		{name: "question", patterns: []string{"dir/?.txt"}, want: []int{0}},
		{name: "class", patterns: []string{"dir/[a]\\.txt"}, want: nil},
		{name: "class match", patterns: []string{"dir/[a].txt"}, want: []int{0}},
		{name: "union overlap", patterns: []string{"dir", "dir/a.txt", "root.dat"}, want: []int{0, 1, 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if strings.Contains(test.patterns[0], `\`) {
				if _, err := Select(meta, test.patterns, nil); !errors.Is(err, ErrInvalidPattern) {
					t.Fatalf("error = %v, want ErrInvalidPattern", err)
				}
				return
			}
			plan, err := Select(meta, test.patterns, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := plan.SelectedIndices(); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("selected = %v, want %v", got, test.want)
			}
		})
	}
	for _, pattern := range []string{"dir/**", "dir/[", "../dir", "/dir", "dir//a"} {
		if _, err := Select(meta, []string{pattern}, nil); !errors.Is(err, ErrInvalidPattern) {
			t.Errorf("pattern %q error = %v, want ErrInvalidPattern", pattern, err)
		}
	}
}

func TestGlobUnionPreservesPathMatchSemantics(t *testing.T) {
	paths := []string{
		"a",
		"a/b",
		"a/c/d",
		"a/z",
		"b",
		"c",
		"unicode/éclair",
	}
	files := make([]File, len(paths))
	pieces := make([]Piece, len(paths))
	for index, path := range paths {
		files[index] = selectionFile(index, path, int64(index), int64(index+1), RegularFile)
		pieces[index] = selectionPiece(index, int64(index), int64(index+1))
	}
	meta := selectionMeta(files, pieces)
	patterns := []string{
		"a/*/d",
		"a/[b-c]",
		"[a-bd]",
		"a/[^b]",
		"a[^x]b",
		"[c-0]",
		"unicode/?clair",
		"unicode/é*",
		"missing/[a-z]",
	}
	for _, pattern := range patterns {
		plan, err := Select(meta, []string{pattern}, nil)
		want := make([]int, 0)
		for index, file := range meta.Files {
			if pathPatternMatches(pattern, file.Path) {
				want = append(want, index)
			}
		}
		if len(want) == 0 {
			if !errors.Is(err, ErrSelectionNoMatch) {
				t.Errorf("pattern %q error = %v, want ErrSelectionNoMatch", pattern, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("pattern %q: %v", pattern, err)
		}
		if got := plan.SelectedIndices(); !reflect.DeepEqual(got, want) {
			t.Errorf("pattern %q selected = %v, want %v", pattern, got, want)
		}
	}
}

func TestSelectBEP53KeepsOriginalIndicesAndRejectsSpecialFiles(t *testing.T) {
	meta := selectionMeta(
		[]File{
			selectionFile(0, "a", 0, 2, RegularFile),
			selectionFile(1, "", 2, 3, PaddingFile),
			selectionFile(2, "link", 3, 3, SymlinkFile),
			selectionFile(3, "b", 3, 5, RegularFile),
		},
		[]Piece{selectionPiece(0, 0, 4), selectionPiece(1, 4, 5)},
	)

	plan, err := Select(meta, nil, []IndexRange{{Start: 0, End: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.SelectedIndices(), []int{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected = %v, want %v", got, want)
	}
	if got, want := plan.WantedPieces(), []int{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wanted = %v, want %v", got, want)
	}
	if _, err := Select(meta, nil, []IndexRange{{Start: 2, End: 2}}); !errors.Is(err, ErrSymlinkSelection) {
		t.Fatalf("symlink selection error = %v", err)
	}
	if _, err := Select(meta, nil, []IndexRange{{Start: 1, End: 1}}); !errors.Is(err, ErrSelectionNoMatch) {
		t.Fatalf("padding-only selection error = %v", err)
	}
	if _, err := Select(meta, nil, []IndexRange{{Start: 0, End: 4}}); !errors.Is(err, ErrInvalidSelection) {
		t.Fatalf("out-of-bounds selection error = %v", err)
	}
}

func TestSelectionPiecePlanIncludesSkippedDataAndSyntheticPadding(t *testing.T) {
	meta := selectionMeta(
		[]File{
			selectionFile(0, "selected", 0, 2, RegularFile),
			selectionFile(1, "skipped", 2, 3, RegularFile),
			selectionFile(2, "", 3, 4, PaddingFile),
			selectionFile(3, "tail", 4, 6, RegularFile),
		},
		[]Piece{selectionPiece(0, 0, 4), selectionPiece(1, 4, 6)},
	)
	plan, err := Select(meta, []string{"selected"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	piece, ok := plan.Piece(0)
	if !ok {
		t.Fatal("piece 0 is not wanted")
	}
	wantData := []FileRange{{Index: 0, Range: ByteRange{Begin: 0, End: 2}}, {Index: 1, Range: ByteRange{Begin: 2, End: 3}}}
	wantSelected := []FileRange{{Index: 0, Range: ByteRange{Begin: 0, End: 2}}}
	wantPadding := []ByteRange{{Begin: 3, End: 4}}
	if !reflect.DeepEqual(piece.Data, wantData) || !reflect.DeepEqual(piece.Selected, wantSelected) || !reflect.DeepEqual(piece.Padding, wantPadding) {
		t.Fatalf("piece plan = %#v, want data=%v selected=%v padding=%v", piece, wantData, wantSelected, wantPadding)
	}
	if _, ok := plan.Piece(1); ok {
		t.Fatal("unselected piece unexpectedly wanted")
	}
	// Returned slices are copies; mutating one inspection result cannot mutate
	// the immutable plan used by a later scheduler lookup.
	piece.Data[0].Range.Begin = 99
	again, ok := plan.Piece(0)
	if !ok || again.Data[0].Range.Begin != 0 {
		t.Fatalf("plan changed through Piece result: %#v", again)
	}
}

func TestSelectZeroLengthFileIsValidWithoutWantedPieces(t *testing.T) {
	meta := selectionMeta([]File{selectionFile(0, "empty", 0, 0, RegularFile)}, nil)
	plan, err := Select(meta, []string{"empty"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.SelectedIndices(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("selected = %v", got)
	}
	if got := plan.WantedPieces(); len(got) != 0 {
		t.Fatalf("wanted = %v, want empty", got)
	}
}

func TestSelectableFilesOmitsPaddingAndSymlinks(t *testing.T) {
	meta := selectionMeta([]File{
		selectionFile(0, "a", 0, 1, RegularFile),
		selectionFile(1, "", 1, 2, PaddingFile),
		selectionFile(2, "link", 2, 2, SymlinkFile),
		selectionFile(3, "b", 2, 2, RegularFile),
	}, nil)
	if got, want := SelectableFiles(meta), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %v, want %v", got, want)
	}
}

func TestSelectionScalesAcrossManyFilesAndPieces(t *testing.T) {
	const count = 8192
	files := make([]File, count)
	pieces := make([]Piece, count)
	for index := 0; index < count; index++ {
		files[index] = selectionFile(index, "f-"+strings.Repeat("x", 1)+formatSelectionIndex(index), int64(index), int64(index+1), RegularFile)
		pieces[index] = selectionPiece(index, int64(index), int64(index+1))
	}
	meta := selectionMeta(files, pieces)
	plan, err := Select(meta, []string{"f-x4096"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.WantedPieces(), []int{4096}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wanted = %v, want %v", got, want)
	}
}

func TestSelectionScalesAcrossManyDistinctWildcardPatterns(t *testing.T) {
	const count = 100_000
	files := make([]File, count)
	pieces := make([]Piece, count)
	patterns := make([]string, count)
	for index := 0; index < count; index++ {
		files[index] = selectionFile(index, "file-"+fmt.Sprintf("%05d", index), int64(index), int64(index+1), RegularFile)
		pieces[index] = selectionPiece(index, int64(index), int64(index+1))
		// Every selector starts with '?', so there is no usable literal
		// prefix. None matches the file paths; this exercises the worst-case
		// nonmatching set without relying on repeated-pattern deduplication.
		patterns[index] = "?missing-" + fmt.Sprintf("%05d", index)
	}
	meta := selectionMeta(files, pieces)
	if _, err := Select(meta, patterns, nil); !errors.Is(err, ErrSelectionNoMatch) {
		t.Fatalf("selection error = %v, want ErrSelectionNoMatch", err)
	}
}

func formatSelectionIndex(index int) string {
	return fmt.Sprintf("%04d", index)
}

func FuzzSelectPattern(f *testing.F) {
	f.Add("dir/*")
	f.Add("dir/[a-z]?.bin")
	f.Add("dir/**")
	meta := selectionMeta([]File{selectionFile(0, "dir/a.bin", 0, 1, RegularFile)}, []Piece{selectionPiece(0, 0, 1)})
	f.Fuzz(func(t *testing.T, pattern string) {
		_, _ = Select(meta, []string{pattern}, nil)
	})
}

func FuzzGlobRegexMatchesPathMatch(f *testing.F) {
	for _, seed := range []string{"*", "a/?", "a/[b-d]", "a/[^x]b", "unicode/é*", "[a-bd]"} {
		f.Add(seed)
	}
	candidates := []string{"", "a", "a/b", "a/c", "a/c/d", "a/bb", "a/bx", "unicode/éclair", "x/y"}
	f.Fuzz(func(t *testing.T, pattern string) {
		if err := validatePattern(pattern); err != nil {
			return
		}
		matcher, err := compileGlobPatterns([]string{pattern})
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range candidates {
			want := pathPatternMatches(pattern, candidate)
			if got := matcher.MatchString(candidate); got != want {
				t.Fatalf("pattern %q candidate %q = %v, want %v", pattern, candidate, got, want)
			}
		}
	})
}
