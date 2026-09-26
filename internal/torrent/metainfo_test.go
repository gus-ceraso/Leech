package torrent

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestParseMetainfoSingleFileExactHashAndRanges(t *testing.T) {
	info := metaDict(
		metaEntry("length", metaInteger(5)),
		metaEntry("name", metaStringValue("hello.bin")),
		metaEntry("piece length", metaInteger(4)),
		metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{0x11}, 40))),
		metaEntry("private", metaInteger(1)),
		metaEntry("unknown", metaStringValue("kept in the hash")),
	)
	infoRaw := metaEncode(info)
	torrent := metaEncode(metaDict(
		metaEntry("announce", metaStringValue("HTTP://tracker.example/announce")),
		metaEntry("info", info),
	))
	got, err := ParseMetainfo(torrent)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha1.Sum(infoRaw)
	if !bytes.Equal(got.InfoHash[:], wantHash[:]) {
		t.Fatalf("info hash = %x, want %x", got.InfoHash, wantHash)
	}
	if got.Name != "hello.bin" || got.MultiFile || got.TotalLength != 5 || got.PieceLength != 4 || !got.Private {
		t.Fatalf("metadata = %#v", got)
	}
	if len(got.Files) != 1 || got.Files[0] != (File{Index: 0, Path: "hello.bin", Range: ByteRange{End: 5}, Kind: RegularFile}) {
		t.Fatalf("files = %#v", got.Files)
	}
	if len(got.Pieces) != 2 || got.Pieces[0].Range != (ByteRange{End: 4}) || got.Pieces[1].Range != (ByteRange{Begin: 4, End: 5}) {
		t.Fatalf("pieces = %#v", got.Pieces)
	}
	if len(got.Trackers) != 2 || got.Trackers[0] != DefaultTracker || got.Trackers[1] != "http://tracker.example/announce" {
		t.Fatalf("trackers = %#v", got.Trackers)
	}
}

func TestIndependentOneByteInfoGoldenAndMutations(t *testing.T) {
	// This wire value is intentionally literal: the info hash is over these
	// exact canonical bytes, including the binary SHA-1 digest in pieces.
	const canonicalInfo = "d6:lengthi1e4:name1:A12:piece lengthi1e6:pieces20:" +
		"\x6d\xcd\x4c\xe2\x3d\x88\xe2\xee\x95\x68\xba\x54\x6c\x00\x7c\x63\xd9\x13\x1c\x1be"
	wantInfoHash := [20]byte{0x1d, 0xb2, 0xe0, 0xa5, 0xd9, 0x6e, 0x3e, 0xf5, 0x2f, 0x80, 0x4b, 0x92, 0x8b, 0x28, 0xa1, 0x9f, 0x90, 0xe3, 0xf9, 0x2e}
	if got := sha1.Sum([]byte(canonicalInfo)); got != wantInfoHash {
		t.Fatalf("literal info hash = %x, want %x", got, wantInfoHash)
	}
	metainfo := append([]byte("d4:info"), []byte(canonicalInfo)...)
	metainfo = append(metainfo, 'e')
	parsed, err := ParseMetainfo(metainfo)
	if err != nil {
		t.Fatalf("ParseMetainfo(canonical vector): %v", err)
	}
	if parsed.InfoHash != wantInfoHash || parsed.TotalLength != 1 || len(parsed.Pieces) != 1 || parsed.Pieces[0].Hash != [20]byte{0x6d, 0xcd, 0x4c, 0xe2, 0x3d, 0x88, 0xe2, 0xee, 0x95, 0x68, 0xba, 0x54, 0x6c, 0x00, 0x7c, 0x63, 0xd9, 0x13, 0x1c, 0x1b} {
		t.Fatalf("parsed vector = %#v", parsed)
	}

	mutations := map[string]string{
		"duplicate key":     "d6:lengthi1e4:name1:A4:name1:A12:piece lengthi1e6:pieces20:" + strings.TrimPrefix(canonicalInfo, "d6:lengthi1e4:name1:A12:piece lengthi1e6:pieces20:"),
		"unordered keys":    "d4:name1:A6:lengthi1e12:piece lengthi1e6:pieces20:" + strings.TrimPrefix(canonicalInfo, "d6:lengthi1e4:name1:A12:piece lengthi1e6:pieces20:"),
		"wrong piece count": "d6:lengthi2e4:name1:A12:piece lengthi1e6:pieces20:" + strings.TrimPrefix(canonicalInfo, "d6:lengthi1e4:name1:A12:piece lengthi1e6:pieces20:"),
		"unsafe path":       "d6:lengthi1e4:name4:../A12:piece lengthi1e6:pieces20:" + strings.TrimPrefix(canonicalInfo, "d6:lengthi1e4:name1:A12:piece lengthi1e6:pieces20:"),
	}
	for name, info := range mutations {
		t.Run(name, func(t *testing.T) {
			data := append([]byte("d4:info"), []byte(info)...)
			data = append(data, 'e')
			if _, err := ParseMetainfo(data); err == nil {
				t.Fatal("invalid metainfo succeeded")
			}
		})
	}
}

func TestParseMetainfoMultiFilePaddingSymlinkAndFlattenedTrackers(t *testing.T) {
	files := metaList(
		metaDict(metaEntry("length", metaInteger(3)), metaEntry("path", metaPath("dir", "a"))),
		metaDict(metaEntry("attr", metaStringValue("p")), metaEntry("length", metaInteger(2))),
		metaDict(metaEntry("attr", metaStringValue("l")), metaEntry("path", metaPath("link")), metaEntry("symlink path", metaPath("dir", "a"))),
		metaDict(metaEntry("attr", metaStringValue("xq")), metaEntry("length", metaInteger(1)), metaEntry("path", metaPath("z"))),
	)
	info := metaDict(
		metaEntry("files", files),
		metaEntry("name", metaStringValue("release")),
		metaEntry("piece length", metaInteger(4)),
		metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{0x22}, 40))),
	)
	root := metaDict(
		metaEntry("announce", metaStringValue("http://ignored.example/announce")),
		metaEntry("announce-list", metaList(
			metaList(metaStringValue("udp://tracker.example:6969/announce"), metaStringValue("http://tracker.example/announce")),
			metaList(metaStringValue("http://tracker.example/announce")),
		)),
		metaEntry("info", info),
	)
	got, err := ParseMetainfo(metaEncode(root))
	if err != nil {
		t.Fatal(err)
	}
	if !got.MultiFile || got.TotalLength != 6 || len(got.Files) != 4 {
		t.Fatalf("metadata = %#v", got)
	}
	want := []File{
		{Index: 0, Path: "dir/a", Range: ByteRange{End: 3}, Kind: RegularFile},
		{Index: 1, Range: ByteRange{Begin: 3, End: 5}, Kind: PaddingFile},
		{Index: 2, Path: "link", Range: ByteRange{Begin: 5, End: 5}, Kind: SymlinkFile},
		{Index: 3, Path: "z", Range: ByteRange{Begin: 5, End: 6}, Kind: RegularFile},
	}
	for i := range want {
		if got.Files[i] != want[i] {
			t.Fatalf("file %d = %#v, want %#v", i, got.Files[i], want[i])
		}
	}
	if len(got.Trackers) != 3 || got.Trackers[0] != DefaultTracker || got.Trackers[1] != "udp://tracker.example:6969/announce" || got.Trackers[2] != "http://tracker.example/announce" {
		t.Fatalf("trackers = %#v", got.Trackers)
	}
}

func TestParseInfoDictionaryChecksExpectedHashAndTrackers(t *testing.T) {
	info := metaEncode(metaDict(
		metaEntry("length", metaInteger(0)),
		metaEntry("name", metaStringValue("empty")),
		metaEntry("piece length", metaInteger(16<<10)),
		metaEntry("pieces", metaStringBytes(nil)),
	))
	sum := sha1.Sum(info)
	var expected InfoHash
	copy(expected[:], sum[:])
	got, err := ParseInfoDictionary(info, expected, []string{"http://tracker.example/a", "HTTP://TRACKER.EXAMPLE/a"})
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalLength != 0 || len(got.Pieces) != 0 || len(got.Trackers) != 2 {
		t.Fatalf("metadata = %#v", got)
	}
	expected[0]++
	if _, err := ParseInfoDictionary(info, expected, nil); !errors.Is(err, ErrInfoHashMismatch) {
		t.Fatalf("hash error = %v, want ErrInfoHashMismatch", err)
	}
}

func TestParseMetainfoRejectsInvalidStructure(t *testing.T) {
	base := func(extra ...bencode.Entry) []byte {
		entries := []bencode.Entry{
			metaEntry("length", metaInteger(1)),
			metaEntry("name", metaStringValue("x")),
			metaEntry("piece length", metaInteger(1)),
			metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{1}, 20))),
		}
		return metaEncode(metaDict(append(entries, extra...)...))
	}
	for name, data := range map[string][]byte{
		"v2":                base(metaEntry("meta version", metaInteger(2))),
		"both length files": base(metaEntry("files", metaList())),
		"bad piece count":   metaEncode(metaDict(metaEntry("info", metaDict(metaEntry("length", metaInteger(2)), metaEntry("name", metaStringValue("x")), metaEntry("piece length", metaInteger(1)), metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{1}, 20))))))),
		"unsafe name":       metaEncode(metaDict(metaEntry("info", metaDict(metaEntry("length", metaInteger(1)), metaEntry("name", metaStringValue("../x")), metaEntry("piece length", metaInteger(1)), metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{1}, 20))))))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseMetainfo(data); err == nil {
				t.Fatal("invalid metadata succeeded")
			}
		})
	}
}

func TestParseMetainfoRejectsPathCollisionsAndMalformedSymlinks(t *testing.T) {
	makeInfo := func(files bencode.Value) []byte {
		return metaEncode(metaDict(metaEntry("info", metaDict(
			metaEntry("files", files), metaEntry("name", metaStringValue("root")), metaEntry("piece length", metaInteger(1)), metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{1}, 20*len(files.List)))),
		))))
	}
	for _, files := range []bencode.Value{
		metaList(metaDict(metaEntry("length", metaInteger(1)), metaEntry("path", metaPath("a"))), metaDict(metaEntry("length", metaInteger(1)), metaEntry("path", metaPath("a", "b")))),
		metaList(metaDict(metaEntry("length", metaInteger(1)), metaEntry("path", metaPath("a"))), metaDict(metaEntry("length", metaInteger(1)), metaEntry("path", metaPath("a")))),
		metaList(metaDict(metaEntry("attr", metaStringValue("p")), metaEntry("length", metaInteger(1)), metaEntry("path", metaList()))),
		metaList(metaDict(metaEntry("attr", metaStringValue("l")), metaEntry("path", metaPath("a")))),
	} {
		if _, err := ParseMetainfo(makeInfo(files)); err == nil {
			t.Fatalf("invalid files %#v succeeded", files)
		}
	}
}

func TestParseMetainfoRejectsInvalidUTF8TrackerURLs(t *testing.T) {
	info := metaDict(
		metaEntry("length", metaInteger(0)),
		metaEntry("name", metaStringValue("empty")),
		metaEntry("piece length", metaInteger(16<<10)),
		metaEntry("pieces", metaStringBytes(nil)),
	)
	for _, root := range []bencode.Value{
		metaDict(metaEntry("announce", metaStringBytes([]byte{'h', 't', 't', 'p', ':', '/', '/', 0xff})), metaEntry("info", info)),
		metaDict(metaEntry("announce-list", metaList(metaList(metaStringBytes([]byte{'u', 'd', 'p', ':', '/', '/', 0xff})))), metaEntry("info", info)),
	} {
		if _, err := ParseMetainfo(metaEncode(root)); err == nil {
			t.Fatalf("invalid UTF-8 tracker URL succeeded: %#v", root)
		}
	}
}

func TestParseMetainfoRejectsPathCollisionWithInterveningName(t *testing.T) {
	makeFile := func(parts ...string) bencode.Value {
		return metaDict(metaEntry("length", metaInteger(1)), metaEntry("path", metaPath(parts...)))
	}
	for _, files := range []bencode.Value{
		metaList(makeFile("a"), makeFile("a-"), makeFile("a", "x")),
		metaList(makeFile("a", "x"), makeFile("a-"), makeFile("a")),
	} {
		info := metaDict(
			metaEntry("files", files),
			metaEntry("name", metaStringValue("root")),
			metaEntry("piece length", metaInteger(1)),
			metaEntry("pieces", metaStringBytes(bytes.Repeat([]byte{1}, 60))),
		)
		if _, err := ParseMetainfo(metaEncode(metaDict(metaEntry("info", info)))); err == nil {
			t.Fatalf("path collision with intervening name succeeded: %#v", files)
		}
	}
}

func TestReadMetainfoBoundsInput(t *testing.T) {
	reader := io.LimitReader(strings.NewReader("x"), 1)
	if _, err := ReadMetainfo(reader); err == nil {
		t.Fatal("malformed input succeeded")
	}
	if _, err := ReadMetainfo(nil); err == nil {
		t.Fatal("nil reader succeeded")
	}
	if limits.MetainfoBytes <= 0 {
		t.Fatal("invalid metainfo limit")
	}
}

func FuzzParseMetainfoBounded(f *testing.F) {
	f.Add([]byte("d6:lengthi0e4:name1:x12:piece lengthi1e6:pieces0:e"))
	f.Add([]byte("d4:infod6:lengthi0e4:name1:x12:piece lengthi1e6:pieces0:1:ud0:2:\x00\xffeee"))
	f.Add([]byte("d4:infod6:lengthi274877906945e4:name1:x12:piece lengthi1e6:pieces0:ee"))
	f.Add([]byte("d4:infod6:lengthi0e4:name5:empty12:piece lengthi16384e6:pieces0:ee"))
	f.Add([]byte("d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"))
	f.Fuzz(func(t *testing.T, input []byte) {
		if meta, err := ParseMetainfo(input); err == nil {
			assertInputReviewMetainfo(t, meta)
		}
		// Match the supplied bytes so valid fetched dictionaries reach full
		// normalization instead of always stopping at the info-hash check.
		if meta, err := ParseInfoDictionary(input, InfoHash(sha1.Sum(input)), nil); err == nil {
			assertInputReviewMetainfo(t, meta)
		}
	})
}

func metaDict(entries ...bencode.Entry) bencode.Value {
	return bencode.Value{Type: bencode.Dictionary, Dict: entries}
}

func metaList(values ...bencode.Value) bencode.Value {
	return bencode.Value{Type: bencode.List, List: values}
}

func metaInteger(value int64) bencode.Value { return bencode.Value{Type: bencode.Integer, Int: value} }

func metaStringValue(value string) bencode.Value { return metaStringBytes([]byte(value)) }

func metaStringBytes(value []byte) bencode.Value {
	return bencode.Value{Type: bencode.Bytes, Bytes: value}
}

func metaPath(parts ...string) bencode.Value {
	values := make([]bencode.Value, len(parts))
	for i, part := range parts {
		values[i] = metaStringValue(part)
	}
	return metaList(values...)
}

func metaEntry(key string, value bencode.Value) bencode.Entry {
	return bencode.Entry{Key: []byte(key), Value: value}
}

func metaEncode(value bencode.Value) []byte {
	encoded, err := bencode.Encode(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
