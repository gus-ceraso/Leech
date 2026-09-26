package torrent

import (
	"errors"
	"path"
	"reflect"
	"strings"
	"testing"
)

func TestSelectDescendingClasses(t *testing.T) {
	filesAtRoot := []string{"a", "b", "0", "^", "[", "é"}
	filesInDirectories := []string{"a/b", "a/c/child", "d/child"}
	for _, tc := range []struct {
		pattern string
		paths   []string
		want    []string
	}{
		{"[z-a]", filesAtRoot, nil},
		{"[^z-a]", filesAtRoot, filesAtRoot},
		{"[z-a0]", filesAtRoot, []string{"0"}},
		{"[^z-a0]", filesAtRoot, []string{"a", "b", "^", "[", "é"}},
		{"[z-a^]", filesAtRoot, []string{"^"}},
		{"[z-a[]", filesAtRoot, []string{"["}},
		{"[^z-a]", filesInDirectories, filesInDirectories},
		{"a[^z-a]b", filesInDirectories, []string{"a/b"}},
		{"a[^z-a]c", filesInDirectories, []string{"a/c/child"}},
		{"a[/]b", filesInDirectories, []string{"a/b"}},
		{"a?b", filesInDirectories, nil},
		{"a*b", filesInDirectories, nil},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			files := make([]File, len(tc.paths))
			pieces := make([]Piece, len(tc.paths))
			for i, name := range tc.paths {
				files[i] = selectionFile(i, name, int64(i), int64(i+1), RegularFile)
				pieces[i] = selectionPiece(i, int64(i), int64(i+1))
			}
			meta := selectionMeta(files, pieces)
			// Derive directory matches independently from path.Match, and
			// retain explicit expected paths so the fixture checks both APIs.
			var expected []string
			for _, name := range tc.paths {
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
					separator := strings.LastIndexByte(prefix, '/')
					if separator < 0 {
						break
					}
					prefix = prefix[:separator]
				}
			}
			if !reflect.DeepEqual(expected, tc.want) {
				t.Fatalf("path.Match fixture = %q, want %q", expected, tc.want)
			}
			plan, err := Select(meta, []string{tc.pattern}, nil)
			if len(tc.want) == 0 {
				if !errors.Is(err, ErrSelectionNoMatch) {
					t.Fatalf("Select = %v, want no match", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, file := range plan.SelectedFiles() {
				got = append(got, file.Path)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("selected paths = %q, want %q", got, tc.want)
			}
		})
	}
}
