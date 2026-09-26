package torrent

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestInputReviewUnknownBinaryInfoKeysStayInExactHash(t *testing.T) {
	const info = "d6:lengthi0e4:name1:x12:piece lengthi1e6:pieces0:1:ud0:2:\x00\xffee"
	const hash = "beff6b94f2072cd4ec73ae6724b05f06d9cdc3c8"
	metainfo, err := ParseMetainfo([]byte("d4:info" + info + "e"))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(metainfo.InfoHash[:]) != hash {
		t.Fatalf("exact info hash = %x, want %s", metainfo.InfoHash, hash)
	}
	fetched, err := ParseInfoDictionary([]byte(info), metainfo.InfoHash, nil)
	if err != nil || fetched.InfoHash != metainfo.InfoHash {
		t.Fatalf("fetched info = %x, %v", fetched.InfoHash, err)
	}
	wire := []byte("d4:info" + info + "e")
	for end := range len(wire) {
		if _, err := ParseMetainfo(wire[:end]); err == nil {
			t.Fatalf("truncated metadata accepted at byte %d", end)
		}
	}
}

func TestInputReviewLiteralAttributesPreserveBEP53Indices(t *testing.T) {
	const info = "d5:filesl" +
		"d6:lengthi1e4:pathl1:aee" +
		"d4:attr1:p6:lengthi2ee" +
		"d4:attr1:l4:pathl4:linke12:symlink pathl1:aee" +
		"d4:attr3:xhq6:lengthi1e4:pathl1:bee" +
		"e4:name4:root12:piece lengthi4e6:pieces20:aaaaaaaaaaaaaaaaaaaae"
	meta, err := ParseMetainfo([]byte("d4:info" + info + "e"))
	if err != nil {
		t.Fatal(err)
	}
	want := []File{
		{Index: 0, Path: "a", Range: ByteRange{Begin: 0, End: 1}, Kind: RegularFile},
		{Index: 1, Range: ByteRange{Begin: 1, End: 3}, Kind: PaddingFile},
		{Index: 2, Path: "link", Range: ByteRange{Begin: 3, End: 3}, Kind: SymlinkFile},
		{Index: 3, Path: "b", Range: ByteRange{Begin: 3, End: 4}, Kind: RegularFile},
	}
	if !reflect.DeepEqual(meta.Files, want) || meta.TotalLength != 4 {
		t.Fatalf("normalized files = %+v, total=%d", meta.Files, meta.TotalLength)
	}
	magnet, err := ParseMagnet("magnet:?xt=urn:btih:" + testHashHex + "&so=1,3&so=3")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Select(meta, nil, magnet.Selection)
	if err != nil || !reflect.DeepEqual(plan.SelectedIndices(), []int{3}) {
		t.Fatalf("BEP 53 selection = %v, %v", plan, err)
	}
	plan, err = Select(meta, []string{"a"}, []IndexRange{{Start: 2, End: 2}})
	if err != nil || !reflect.DeepEqual(plan.SelectedIndices(), []int{0}) {
		t.Fatalf("explicit override of symlink index = %v, %v", plan, err)
	}
}

func TestInputReviewMetainfoArithmeticBoundaries(t *testing.T) {
	for _, total := range []int64{0, 1, limits.TorrentBytes} {
		count, err := pieceCount(total, limits.PieceBytes)
		if err != nil {
			t.Fatal(err)
		}
		wire := fmt.Sprintf("d4:infod6:lengthi%de4:name1:x12:piece lengthi%de6:pieces%d:", total, limits.PieceBytes, count*20)
		wire += strings.Repeat("x", count*20) + "ee"
		meta, err := ParseMetainfo([]byte(wire))
		if err != nil || meta.TotalLength != total || len(meta.Pieces) != count {
			t.Fatalf("total %d: %+v, %v", total, meta, err)
		}
		if count > 0 && meta.Pieces[count-1].Range.End != total {
			t.Fatal("last piece exceeded declared length")
		}
	}
	for _, length := range []int64{-1, limits.TorrentBytes + 1, 9223372036854775807} {
		wire := fmt.Sprintf("d4:infod6:lengthi%de4:name1:x12:piece lengthi1e6:pieces0:ee", length)
		if _, err := ParseMetainfo([]byte(wire)); err == nil {
			t.Fatalf("unsupported length %d accepted", length)
		}
	}
	if count, err := pieceCount(limits.Pieces, 1); err != nil || count != limits.Pieces {
		t.Fatalf("piece-count boundary = %d, %v", count, err)
	}
	if _, err := pieceCount(limits.Pieces+1, 1); err == nil {
		t.Fatal("too many pieces accepted")
	}
	wire := fmt.Sprintf("d4:infod5:filesld6:lengthi%de4:pathl1:aeed6:lengthi1e4:pathl1:bee", limits.TorrentBytes)
	wire += "e4:name1:x12:piece lengthi1e6:pieces0:ee"
	if _, err := ParseMetainfo([]byte(wire)); err == nil {
		t.Fatal("multi-file total beyond 256 GiB accepted")
	}
}

func TestInputReviewSourcePrecedenceAndMagnetTopics(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		kind SourceKind
	}{
		{strings.Repeat("g", 40), SourcePath},
		{"./magnet:?not-a-query", SourcePath},
		{"./" + testHashHex, SourcePath},
		{strings.ToUpper(testHashHex), SourceInfoHash},
	} {
		source, err := ParseSource(tc.raw)
		if err != nil || source.Kind != tc.kind {
			t.Fatalf("source %q = %+v, %v", tc.raw, source, err)
		}
	}
	magnet, err := ParseMagnet("magnet:?xt=urn:btih:" + testHashHex + "&xt=urn:btih:" + strings.ToUpper(testHashHex) + "&dn=..%2Fdisplay-only&x.pe=EXAMPLE.test:1&x.pe=example.test:1")
	if err != nil || magnet.DisplayName != "../display-only" || len(magnet.Peers) != 1 {
		t.Fatalf("effective duplicate topic/display/peers = %+v, %v", magnet, err)
	}
	if _, err := ParseMagnet("magnet:?xt=urn:btih:" + testHashHex + "&xt=urn:btmh:1220deadbeef"); err == nil {
		t.Fatal("hybrid magnet accepted")
	}
	// A Base32 topic and hexadecimal topic for the same bytes are one topic.
	base32 := base32NoPadding.EncodeToString(bytes.Repeat([]byte{0}, 20))
	if _, err := ParseMagnet("magnet:?xt=urn:btih:" + strings.Repeat("0", 40) + "&xt=urn:btih:" + base32); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkInputReviewBEP53RepeatedRanges(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			files := make([]File, count)
			ranges := make([]IndexRange, count)
			for i := range files {
				files[i] = selectionFile(i, fmt.Sprintf("file-%d", i), 0, 0, RegularFile)
				ranges[i] = IndexRange{Start: 0, End: count - 1}
			}
			meta := selectionMeta(files, nil)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				plan, err := Select(meta, nil, ranges)
				if err != nil || len(plan.SelectedFiles()) != count {
					b.Fatalf("selection: %v", err)
				}
			}
		})
	}
}

func assertInputReviewMetainfo(t *testing.T, meta Metainfo) {
	t.Helper()
	if meta.TotalLength < 0 || meta.TotalLength > limits.TorrentBytes || meta.PieceLength <= 0 || meta.PieceLength > limits.PieceBytes || len(meta.Files) > limits.Files || len(meta.Pieces) > limits.Pieces {
		t.Fatal("normalized metadata exceeds its supported bounds")
	}
	var end int64
	for index, file := range meta.Files {
		if file.Index != index || file.Range.Begin != end || file.Range.End < end || file.Range.End > meta.TotalLength {
			t.Fatalf("invalid normalized file %d: %+v", index, file)
		}
		if file.Kind == SymlinkFile && file.Range.End != end || file.Kind == PaddingFile && file.Path != "" {
			t.Fatalf("invalid special-file normalization: %+v", file)
		}
		end = file.Range.End
	}
	if end != meta.TotalLength {
		t.Fatal("file ranges do not cover declared torrent length")
	}
	end = 0
	for index, piece := range meta.Pieces {
		if piece.Index != index || piece.Range.Begin != end || piece.Range.End <= end || piece.Range.End > meta.TotalLength || piece.Range.End-end > meta.PieceLength {
			t.Fatalf("invalid normalized piece %d: %+v", index, piece)
		}
		if index+1 < len(meta.Pieces) && piece.Range.End-end != meta.PieceLength {
			t.Fatal("non-final piece is shorter than piece length")
		}
		end = piece.Range.End
	}
	if end != meta.TotalLength {
		t.Fatal("piece ranges do not cover declared torrent length")
	}
}

func TestInputReviewRejectsHybridMarkersInsideValidV1Metainfo(t *testing.T) {
	const prefix = "d6:lengthi0e4:name1:x12:piece lengthi1e6:pieces0:"
	for _, info := range []string{
		"d9:file treede" + prefix[1:] + "e",
		"d6:lengthi0e12:meta versioni2e4:name1:x12:piece lengthi1e6:pieces0:e",
		"d6:lengthi0e4:name1:x12:piece layersde12:piece lengthi1e6:pieces0:e",
		prefix + "11:pieces root32:" + strings.Repeat("x", 32) + "e",
	} {
		if _, err := bencode.Decode([]byte(info)); err != nil {
			t.Fatalf("hybrid fixture is not canonical: %v", err)
		}
		if _, err := ParseMetainfo([]byte("d4:info" + info + "e")); err == nil {
			t.Fatalf("recognized hybrid info marker accepted: %q", info)
		}
		if _, err := ParseInfoDictionary([]byte(info), InfoHash(sha1.Sum([]byte(info))), nil); err == nil {
			t.Fatalf("recognized fetched hybrid marker accepted: %q", info)
		}
	}
	if _, err := ParseMetainfo([]byte("d4:info" + prefix + "e12:piece layersdee")); err == nil {
		t.Fatal("top-level piece layers accepted")
	}
	if _, err := ParseMetainfo([]byte("d4:info" + prefix + "ee")); err != nil {
		t.Fatalf("v1 positive control rejected: %v", err)
	}
}

func TestInputReviewMetainfoRelativePathBounds(t *testing.T) {
	for _, tc := range []struct {
		components, width int
		valid             bool
	}{
		{64, 1, true}, {65, 1, false}, {17, 240, true}, {17, 241, false},
	} {
		var wire strings.Builder
		wire.WriteString("d4:infod5:filesld6:lengthi0e4:pathl")
		for i := 0; i < tc.components; i++ {
			fmt.Fprintf(&wire, "%d:%s", tc.width, strings.Repeat("a", tc.width))
		}
		wire.WriteString("eee4:name1:x12:piece lengthi1e6:pieces0:ee")
		_, err := ParseMetainfo([]byte(wire.String()))
		if (err == nil) != tc.valid {
			t.Fatalf("components=%d width=%d: %v; valid=%v", tc.components, tc.width, err, tc.valid)
		}
	}
}
