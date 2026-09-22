package session

import (
	"context"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/torrent"
)

func TestRunKnownResumeCompletesBeforeTrackerActivity(t *testing.T) {
	data := []byte("resume me")
	pieces := sha1.Sum(data)
	info := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieces[:]}},
	}}
	torrentBytes, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("info"), Value: info}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	torrentPath := filepath.Join(root, "payload.torrent")
	if err := os.WriteFile(torrentPath, torrentBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "payload"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := torrent.LoadMetainfo(torrentPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), RunConfig{Source: torrent.Source{Kind: torrent.SourcePath, Path: torrentPath}, OutputDir: root, Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NoTransferNeeded || !result.SelectionComplete || result.Metainfo.InfoHash != meta.InfoHash {
		t.Fatalf("unexpected run result: %#v", result)
	}
	got, err := os.ReadFile(filepath.Join(root, "payload"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("resume output = %q, %v", got, err)
	}
}

func TestRetainedAccountingExcludesUnselectedMixedPieceBytes(t *testing.T) {
	mapping := torrent.PiecePlan{
		Data: []torrent.FileRange{
			{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}},
			{Index: 1, Range: torrent.ByteRange{Begin: 3, End: 6}},
		},
		Selected: []torrent.FileRange{{Index: 0, Range: torrent.ByteRange{Begin: 0, End: 3}}},
	}
	if got := realPieceBytes(mapping); got != 3 {
		t.Fatalf("retained mixed-piece bytes = %d, want 3", got)
	}
}
