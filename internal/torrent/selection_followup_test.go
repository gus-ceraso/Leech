package torrent

import (
	"errors"
	"path"
	"reflect"
	"strings"
	"testing"
)

func TestSelectEscapedPatterns(t *testing.T) {
	paths := []string{"*", "-", "]", "file*.txt", "file", "dir/[x]/leaf", "dir/x", "dir/a.txt", "dir/-.txt", "a/b", "?", "*wild", "é"}
	files := make([]File, len(paths))
	pieces := make([]Piece, len(paths))
	for i, name := range paths {
		files[i] = selectionFile(i, name, int64(i), int64(i+1), RegularFile)
		pieces[i] = selectionPiece(i, int64(i), int64(i+1))
	}
	meta := selectionMeta(files, pieces)
	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{`\*`, []string{"*"}},
		{`[\-]`, []string{"-"}},
		{`[\]]`, []string{"]"}},
		{`file\*.txt`, []string{"file*.txt"}},
		{`\f\i\l\e`, []string{"file"}},
		{`dir/\[x\]`, []string{"dir/[x]/leaf"}},
		{`dir\/x`, []string{"dir/x"}},
		{`dir/a\.txt`, []string{"dir/a.txt"}},
		{`dir/[a\-].txt`, []string{"dir/a.txt", "dir/-.txt"}},
		{`a[//]b`, []string{"a/b"}},
		{`\?`, []string{"?"}},
		{`\**`, []string{"*", "*wild"}},
		{`[**]`, []string{"*"}},
		{`\é`, []string{"é"}},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			var expected []string
			for _, name := range paths {
				prefix := name
				for {
					matched, err := path.Match(tc.pattern, prefix)
					if err != nil {
						t.Fatal(err)
					}
					if matched {
						expected = append(expected, name)
						break
					}
					last := strings.LastIndexByte(prefix, '/')
					if last < 0 {
						break
					}
					prefix = prefix[:last]
				}
			}
			if !reflect.DeepEqual(expected, tc.want) {
				t.Fatalf("path.Match fixture = %q, want %q", expected, tc.want)
			}
			plan, err := Select(meta, []string{tc.pattern}, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(plan.SelectedFiles()))
			for _, file := range plan.SelectedFiles() {
				got = append(got, file.Path)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("selected paths = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEscapedPatternValidation(t *testing.T) {
	for _, pattern := range []string{`\*`, `[\-]`, `[\]]`, `dir/\[x\]`, `dir\/x`, `\**`, `[**]`, `a[//]b`, `\f\i\l\e`} {
		if err := validatePattern(pattern); err != nil {
			t.Errorf("valid pattern %q: %v", pattern, err)
		}
	}
	for _, pattern := range []string{`**`, `dir/**`, `\***`, `dir/\`, `[\]`, `dir/[a-\]`, `dir//x`, `../x`, `/x`, `\/x`} {
		if err := validatePattern(pattern); !errors.Is(err, ErrInvalidPattern) {
			t.Errorf("invalid pattern %q: %v", pattern, err)
		}
	}
	if err := validatePattern(strings.Repeat(`a\/`, 63) + "a"); err != nil {
		t.Fatalf("64 escaped-slash components: %v", err)
	}
	if err := validatePattern(strings.Repeat(`a\/`, 64) + "a"); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("65 escaped-slash components: %v", err)
	}
}

func TestSelectRangesUnionsOriginalIndicesWithoutMutatingInput(t *testing.T) {
	meta := selectionMeta([]File{
		selectionFile(0, "a", 0, 1, RegularFile),
		selectionFile(1, "", 1, 2, PaddingFile),
		selectionFile(2, "b", 2, 3, RegularFile),
		selectionFile(3, "link", 3, 3, SymlinkFile),
		selectionFile(4, "c", 3, 4, RegularFile),
	}, []Piece{
		selectionPiece(0, 0, 1), selectionPiece(1, 1, 2),
		selectionPiece(2, 2, 3), selectionPiece(3, 3, 4),
	})
	ranges := []IndexRange{{0, 2}, {1, 2}, {4, 4}, {0, 0}}
	before := append([]IndexRange(nil), ranges...)
	plan, err := Select(meta, nil, ranges)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.SelectedIndices(), []int{0, 2, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selected original indices = %v, want %v", got, want)
	}
	if got, want := plan.WantedPieces(), []int{0, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wanted pieces = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(ranges, before) {
		t.Fatalf("caller ranges changed: %v", ranges)
	}
	if _, err := Select(meta, nil, []IndexRange{{3, 3}}); !errors.Is(err, ErrSymlinkSelection) {
		t.Fatalf("symlink index error = %v", err)
	}
	if _, err := Select(meta, nil, []IndexRange{{1, 1}}); !errors.Is(err, ErrSelectionNoMatch) {
		t.Fatalf("padding-only index error = %v", err)
	}
	if explicit, err := Select(meta, []string{"a"}, []IndexRange{{3, 3}}); err != nil || !reflect.DeepEqual(explicit.SelectedIndices(), []int{0}) {
		t.Fatalf("explicit override = %v, %v", explicit, err)
	}
	selected := make([]bool, len(meta.Files))
	if err := selectRanges(meta, []IndexRange{{0, 2}, {4, 5}}, selected); !errors.Is(err, ErrInvalidSelection) {
		t.Fatalf("invalid range error = %v", err)
	}
	for _, marked := range selected {
		if marked {
			t.Fatal("invalid range partially marked selection")
		}
	}
}
