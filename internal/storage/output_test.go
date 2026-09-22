package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/torrent"
)

func testMeta(name string, multi bool, files ...torrent.File) torrent.Metainfo {
	var total int64
	for _, file := range files {
		if file.Range.End > total {
			total = file.Range.End
		}
	}
	return torrent.Metainfo{Name: name, MultiFile: multi, TotalLength: total, Files: files}
}

func regular(index int, path string, begin, end int64) torrent.File {
	return torrent.File{Index: index, Path: path, Range: torrent.ByteRange{Begin: begin, End: end}, Kind: torrent.RegularFile}
}

func TestValidateResolvesRootSymlinkAndInspectsWithoutMutation(t *testing.T) {
	root := t.TempDir()
	realRoot := filepath.Join(root, "real")
	if err := os.Mkdir(realRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatal(err)
	}
	meta := testMeta("bundle", true, regular(0, "dir/file", 0, 4))
	plan, err := Validate(link, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Root() != realRoot {
		t.Fatalf("resolved root = %q, want %q", plan.Root(), realRoot)
	}
	state, err := plan.Existing(0)
	if err != nil {
		t.Fatal(err)
	}
	if state.Exists {
		t.Fatal("missing output reported as existing")
	}
	if _, err := os.Stat(filepath.Join(realRoot, "bundle")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation touched output: stat error = %v", err)
	}
}

func TestValidateRejectsUnsafePathsSymlinksAndCollisions(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name string
		meta torrent.Metainfo
	}{
		{name: "traversal", meta: testMeta("bundle", true, regular(0, "../escape", 0, 1))},
		{name: "separator", meta: testMeta("bundle", true, regular(0, "a\\b", 0, 1))},
		{name: "collision", meta: testMeta("bundle", true,
			regular(0, "a", 0, 1), regular(1, "a/b", 1, 2))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Validate(root, tc.meta, []int{0, 1}[:len(tc.meta.Files)]); err == nil {
				t.Fatal("Validate succeeded for unsafe plan")
			}
		})
	}

	outside := filepath.Join(root, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "bundle")); err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(root, testMeta("bundle", true, regular(0, "file", 0, 1)), []int{0}); err == nil {
		t.Fatal("Validate accepted a descendant symlink")
	}
}

func TestValidateChecksAllSelectedPathsBeforePreparation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bundle"), 0o755); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(root, "bundle", "first")
	if err := os.WriteFile(first, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "bundle", "second"), 0o755); err != nil {
		t.Fatal(err)
	}
	meta := testMeta("bundle", true, regular(0, "first", 0, 1), regular(1, "second", 1, 2))
	if _, err := Validate(root, meta, []int{0, 1}); err == nil {
		t.Fatal("Validate accepted a directory target")
	}
	contents, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "keep me" {
		t.Fatalf("validation changed an existing file: %q", contents)
	}
}

func TestPrepareOverwriteLeavesUnselectedFilesUntouched(t *testing.T) {
	root := t.TempDir()
	bundle := filepath.Join(root, "bundle")
	if err := os.Mkdir(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(bundle, "selected")
	unselected := filepath.Join(bundle, "unselected")
	if err := os.WriteFile(selected, []byte("old selected"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unselected, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta := testMeta("bundle", true, regular(0, "selected", 0, 3), regular(1, "unselected", 3, 7))
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := plan.Prepare(Overwrite)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.WriteRange(0, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(selected)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("selected output = %q, want new", got)
	}
	got, err = os.ReadFile(unselected)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "keep" {
		t.Fatalf("unselected output = %q, want keep", got)
	}
}

func TestPrepareResumePreservesExistingAndCreatesZeroLength(t *testing.T) {
	root := t.TempDir()
	bundle := filepath.Join(root, "bundle")
	if err := os.Mkdir(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(bundle, "existing")
	if err := os.WriteFile(existing, []byte("resume"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta := testMeta("bundle", true, regular(0, "existing", 0, 6), regular(1, "empty", 6, 6))
	plan, err := Validate(root, meta, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	state, err := plan.Existing(1)
	if err != nil {
		t.Fatal(err)
	}
	if state.Exists {
		t.Fatal("missing zero-length output reported as existing")
	}
	prepared, err := plan.Prepare(Resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "resume" {
		t.Fatalf("resume output = %q, want resume", got)
	}
	info, err := os.Stat(filepath.Join(bundle, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("zero-length output size = %d", info.Size())
	}
}

func TestWriteRangeGrowsSelectedFileAndRejectsUnselected(t *testing.T) {
	root := t.TempDir()
	meta := testMeta("bundle", true, regular(0, "selected", 100, 200))
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := plan.Prepare(Resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.WriteRange(0, 150, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := prepared.WriteRange(0, 199, []byte("too long")); err == nil {
		t.Fatal("WriteRange accepted data beyond selected range")
	}
	if err := prepared.WriteRange(1, 100, []byte("x")); err == nil {
		t.Fatal("WriteRange accepted an unselected file")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(root, "bundle", "selected"))
	if err != nil {
		t.Fatal(err)
	}
	if len(contents) != 53 || string(contents[50:]) != "abc" {
		t.Fatalf("sparse output length/content = %d/%q", len(contents), contents[50:])
	}
}

type closeFailureFile struct {
	closeErr *error
	closes   *int
}

func (f *closeFailureFile) WriteAt(data []byte, _ int64) (int, error) { return len(data), nil }
func (f *closeFailureFile) Truncate(int64) error                      { return nil }
func (f *closeFailureFile) Close() error {
	*f.closes++
	return *f.closeErr
}

func TestPrepareReportsEveryCloseFailure(t *testing.T) {
	root := t.TempDir()
	meta := testMeta("bundle", true, regular(0, "a", 0, 1), regular(1, "b", 1, 2))
	plan, err := Validate(root, meta, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("close failed")
	closes := 0
	previous := openOutputFile
	openOutputFile = func(string, int, os.FileMode) (outputFile, error) {
		return &closeFailureFile{closeErr: &closeErr, closes: &closes}, nil
	}
	defer func() { openOutputFile = previous }()
	prepared, err := plan.Prepare(Resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want %v", err, closeErr)
	}
	if closes != 2 {
		t.Fatalf("closed handles = %d, want 2", closes)
	}
	if err := prepared.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("second Close error = %v, want %v", err, closeErr)
	}
}

func TestReadAtReportsShortExistingData(t *testing.T) {
	root := t.TempDir()
	meta := testMeta("bundle", true, regular(0, "file", 0, 4))
	plan, err := Validate(root, meta, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := plan.Prepare(Resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bundle", "file"), []byte("ab"), 0o600); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	n, err := plan.ReadAt(0, 0, buf)
	if !errors.Is(err, io.EOF) || n != 2 {
		t.Fatalf("ReadAt = (%d, %v), want (2, EOF)", n, err)
	}
	if !strings.HasPrefix(string(buf), "ab") {
		t.Fatalf("ReadAt data = %q", buf)
	}
}
