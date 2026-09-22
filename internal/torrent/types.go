package torrent

// InfoHash is the SHA-1 digest of the exact encoded v1 info dictionary.
type InfoHash [20]byte

type FileKind uint8

const (
	RegularFile FileKind = iota
	PaddingFile
	SymlinkFile
)

// ByteRange uses half-open offsets in the concatenated v1 byte space.
type ByteRange struct {
	Begin int64
	End   int64
}

// File.Index is its original position in the v1 file list, including padding
// and symlinks. Path is relative to the torrent root; padding has no path.
type File struct {
	Index int
	Path  string
	Range ByteRange
	Kind  FileKind
}

type Piece struct {
	Index int
	Range ByteRange
	Hash  [20]byte
}

// Metainfo is normalized, validated v1 metadata. Callers treat its tables as
// immutable after construction.
type Metainfo struct {
	InfoHash    InfoHash
	Name        string
	MultiFile   bool
	PieceLength int64
	TotalLength int64
	Files       []File
	Pieces      []Piece
	Trackers    []string
	Private     bool
}
