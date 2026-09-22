package torrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

var (
	ErrInvalidMetainfo  = errors.New("invalid metainfo")
	ErrInfoHashMismatch = errors.New("info hash does not match")
)

// ParseMetainfo validates one complete .torrent file and returns its
// normalized v1 tables. The returned InfoHash is the SHA-1 of the exact raw
// bencoded info dictionary in data.
func ParseMetainfo(data []byte) (Metainfo, error) {
	if len(data) > limits.MetainfoBytes {
		return Metainfo{}, fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidMetainfo, limits.MetainfoBytes)
	}
	root, err := bencode.Decode(data)
	if err != nil {
		return Metainfo{}, fmt.Errorf("%w: %v", ErrInvalidMetainfo, err)
	}
	if root.Type != bencode.Dictionary {
		return Metainfo{}, fmt.Errorf("%w: top level is not a dictionary", ErrInvalidMetainfo)
	}
	if _, ok := root.Lookup("piece layers"); ok {
		return Metainfo{}, fmt.Errorf("%w: v2 or hybrid metadata is unsupported", ErrInvalidMetainfo)
	}
	info, ok := root.Lookup("info")
	if !ok {
		return Metainfo{}, fmt.Errorf("%w: missing info dictionary", ErrInvalidMetainfo)
	}
	trackers, err := metainfoTrackers(root)
	if err != nil {
		return Metainfo{}, err
	}
	return normalizeInfo(info, trackers, nil)
}

// ParseInfoDictionary validates a fetched, complete v1 info dictionary. Its
// exact raw bytes must hash to expected. Trackers
// are supplied by the source boundary and are normalized with the mandatory
// default tracker.
func ParseInfoDictionary(data []byte, expected InfoHash, trackers []string) (Metainfo, error) {
	if len(data) > limits.MetainfoBytes {
		return Metainfo{}, fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidMetainfo, limits.MetainfoBytes)
	}
	info, err := bencode.Decode(data)
	if err != nil {
		return Metainfo{}, fmt.Errorf("%w: %v", ErrInvalidMetainfo, err)
	}
	if info.Type != bencode.Dictionary {
		return Metainfo{}, fmt.Errorf("%w: fetched metadata is not an info dictionary", ErrInvalidMetainfo)
	}
	return normalizeInfo(info, trackers, &expected)
}

// ParseFetchedInfo is a descriptive alias for ParseInfoDictionary.
func ParseFetchedInfo(data []byte, expected InfoHash, trackers []string) (Metainfo, error) {
	return ParseInfoDictionary(data, expected, trackers)
}

// ReadMetainfo reads one bounded .torrent stream before validating it.
func ReadMetainfo(r io.Reader) (Metainfo, error) {
	if r == nil {
		return Metainfo{}, fmt.Errorf("%w: nil metadata reader", ErrInvalidMetainfo)
	}
	data, err := io.ReadAll(io.LimitReader(r, int64(limits.MetainfoBytes)+1))
	if err != nil {
		return Metainfo{}, fmt.Errorf("%w: reading metadata: %v", ErrInvalidMetainfo, err)
	}
	return ParseMetainfo(data)
}

// LoadMetainfo opens and reads a local .torrent without creating or changing
// any output or cache paths.
func LoadMetainfo(path string) (Metainfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return Metainfo{}, err
	}
	defer f.Close()
	return ReadMetainfo(f)
}

// ParseMetainfoFile is an alias for LoadMetainfo.
func ParseMetainfoFile(path string) (Metainfo, error) { return LoadMetainfo(path) }

func normalizeInfo(info bencode.Value, trackers []string, expected *InfoHash) (Metainfo, error) {
	if info.Type != bencode.Dictionary {
		return Metainfo{}, fmt.Errorf("%w: info is not a dictionary", ErrInvalidMetainfo)
	}
	hash := sha1.Sum(info.Raw)
	if expected != nil && !equalHash(hash, *expected) {
		return Metainfo{}, ErrInfoHashMismatch
	}
	var infoHash InfoHash
	copy(infoHash[:], hash[:])

	name, err := infoName(info)
	if err != nil {
		return Metainfo{}, err
	}
	pieceLength, err := integerField(info, "piece length", true)
	if err != nil || pieceLength <= 0 || pieceLength > limits.PieceBytes {
		return Metainfo{}, fieldError("piece length", err, "must be positive and at most %d", limits.PieceBytes)
	}
	piecesValue, ok := info.Lookup("pieces")
	if !ok || piecesValue.Type != bencode.Bytes || len(piecesValue.Bytes)%sha1.Size != 0 {
		return Metainfo{}, fmt.Errorf("%w: pieces must be a byte string with 20-byte hashes", ErrInvalidMetainfo)
	}
	if _, ok := info.Lookup("meta version"); ok {
		return Metainfo{}, fmt.Errorf("%w: v2 or hybrid metadata is unsupported", ErrInvalidMetainfo)
	}
	for _, key := range []string{"file tree", "piece layers", "pieces root"} {
		if _, ok := info.Lookup(key); ok {
			return Metainfo{}, fmt.Errorf("%w: v2 or hybrid metadata is unsupported", ErrInvalidMetainfo)
		}
	}
	private, err := privateField(info)
	if err != nil {
		return Metainfo{}, err
	}
	if _, hasLength := info.Lookup("length"); hasLength {
		if _, hasFiles := info.Lookup("files"); hasFiles {
			return Metainfo{}, fmt.Errorf("%w: info has both length and files", ErrInvalidMetainfo)
		}
		length, err := integerField(info, "length", false)
		if err != nil || length < 0 || length > limits.TorrentBytes {
			return Metainfo{}, fieldError("length", err, "must be nonnegative and at most %d", limits.TorrentBytes)
		}
		pieceCount, err := pieceCount(length, pieceLength)
		if err != nil {
			return Metainfo{}, err
		}
		if len(piecesValue.Bytes) != pieceCount*sha1.Size {
			return Metainfo{}, fmt.Errorf("%w: pieces count does not match length", ErrInvalidMetainfo)
		}
		files := []File{{Index: 0, Path: name, Range: ByteRange{Begin: 0, End: length}, Kind: RegularFile}}
		pieces := makePieces(piecesValue.Bytes, pieceCount, pieceLength, length)
		normalizedTrackers, err := TrackersWithDefault(trackers)
		if err != nil {
			return Metainfo{}, fmt.Errorf("%w: trackers: %v", ErrInvalidMetainfo, err)
		}
		return Metainfo{InfoHash: infoHash, Name: name, PieceLength: pieceLength, TotalLength: length, Files: files, Pieces: pieces, Trackers: normalizedTrackers, Private: private}, nil
	}
	filesValue, ok := info.Lookup("files")
	if !ok || filesValue.Type != bencode.List {
		return Metainfo{}, fmt.Errorf("%w: info must contain exactly one of length or files", ErrInvalidMetainfo)
	}
	files, total, err := normalizeFiles(filesValue)
	if err != nil {
		return Metainfo{}, err
	}
	pieceCount, err := pieceCount(total, pieceLength)
	if err != nil {
		return Metainfo{}, err
	}
	if len(piecesValue.Bytes) != pieceCount*sha1.Size {
		return Metainfo{}, fmt.Errorf("%w: pieces count does not match total length", ErrInvalidMetainfo)
	}
	normalizedTrackers, err := TrackersWithDefault(trackers)
	if err != nil {
		return Metainfo{}, fmt.Errorf("%w: trackers: %v", ErrInvalidMetainfo, err)
	}
	return Metainfo{InfoHash: infoHash, Name: name, MultiFile: true, PieceLength: pieceLength, TotalLength: total, Files: files, Pieces: makePieces(piecesValue.Bytes, pieceCount, pieceLength, total), Trackers: normalizedTrackers, Private: private}, nil
}

func metainfoTrackers(root bencode.Value) ([]string, error) {
	var raw []string
	if announceList, present := root.Lookup("announce-list"); present {
		if announceList.Type != bencode.List {
			return nil, fmt.Errorf("%w: announce-list is not a list", ErrInvalidMetainfo)
		}
		for _, tier := range announceList.List {
			if tier.Type != bencode.List {
				return nil, fmt.Errorf("%w: announce-list tier is not a list", ErrInvalidMetainfo)
			}
			for _, value := range tier.List {
				if value.Type != bencode.Bytes {
					return nil, fmt.Errorf("%w: announce-list URL is not a string", ErrInvalidMetainfo)
				}
				raw = append(raw, string(value.Bytes))
			}
		}
	} else if announce, present := root.Lookup("announce"); present {
		if announce.Type != bencode.Bytes {
			return nil, fmt.Errorf("%w: announce is not a string", ErrInvalidMetainfo)
		}
		raw = append(raw, string(announce.Bytes))
	}
	trackers, err := TrackersWithDefault(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: trackers: %v", ErrInvalidMetainfo, err)
	}
	return trackers, nil
}

func infoName(info bencode.Value) (string, error) {
	value, ok := info.Lookup("name")
	if !ok || value.Type != bencode.Bytes || !utf8.Valid(value.Bytes) {
		return "", fmt.Errorf("%w: name must be valid UTF-8", ErrInvalidMetainfo)
	}
	name := string(value.Bytes)
	if err := validateComponent(name); err != nil {
		return "", fmt.Errorf("%w: invalid name: %v", ErrInvalidMetainfo, err)
	}
	return name, nil
}

func privateField(info bencode.Value) (bool, error) {
	value, ok := info.Lookup("private")
	if !ok {
		return false, nil
	}
	if value.Type != bencode.Integer || (value.Int != 0 && value.Int != 1) {
		return false, fmt.Errorf("%w: private must be 0 or 1", ErrInvalidMetainfo)
	}
	return value.Int == 1, nil
}

func integerField(dict bencode.Value, key string, required bool) (int64, error) {
	value, ok := dict.Lookup(key)
	if !ok {
		if required {
			return 0, errors.New("missing")
		}
		return 0, errors.New("missing")
	}
	if value.Type != bencode.Integer {
		return 0, fmt.Errorf("must be an integer")
	}
	return value.Int, nil
}

func fieldError(name string, cause error, requirement string, args ...any) error {
	if cause != nil && cause.Error() != "missing" {
		return fmt.Errorf("%w: %s %v", ErrInvalidMetainfo, name, cause)
	}
	return fmt.Errorf("%w: %s %s", ErrInvalidMetainfo, name, fmt.Sprintf(requirement, args...))
}

func pieceCount(total, pieceLength int64) (int, error) {
	if total < 0 || pieceLength <= 0 {
		return 0, fmt.Errorf("%w: invalid piece range", ErrInvalidMetainfo)
	}
	count64 := int64(0)
	if total != 0 {
		count64 = (total-1)/pieceLength + 1
	}
	if count64 > limits.Pieces || count64 > int64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("%w: too many pieces", ErrInvalidMetainfo)
	}
	return int(count64), nil
}

func makePieces(encoded []byte, count int, pieceLength, total int64) []Piece {
	pieces := make([]Piece, count)
	for i := range pieces {
		begin := int64(i) * pieceLength
		end := begin + pieceLength
		if end > total {
			end = total
		}
		pieces[i] = Piece{Index: i, Range: ByteRange{Begin: begin, End: end}}
		copy(pieces[i].Hash[:], encoded[i*sha1.Size:(i+1)*sha1.Size])
	}
	return pieces
}

func normalizeFiles(value bencode.Value) ([]File, int64, error) {
	if len(value.List) > limits.Files {
		return nil, 0, fmt.Errorf("%w: too many files", ErrInvalidMetainfo)
	}
	files := make([]File, 0, len(value.List))
	paths := make(map[string]int, len(value.List))
	named := make([]struct {
		path       string
		components []string
		index      int
	}, 0, len(value.List))
	var total int64
	for index, item := range value.List {
		if item.Type != bencode.Dictionary {
			return nil, 0, fmt.Errorf("%w: file %d is not a dictionary", ErrInvalidMetainfo, index)
		}
		attr, err := fileAttributes(item)
		if err != nil {
			return nil, 0, err
		}
		path, components, err := filePath(item, attr)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: file %d: %v", ErrInvalidMetainfo, index, err)
		}
		length, err := fileLength(item, attr)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: file %d: %v", ErrInvalidMetainfo, index, err)
		}
		kind := RegularFile
		if attr.padding {
			kind = PaddingFile
		} else if attr.symlink {
			kind = SymlinkFile
		}
		if path != "" {
			if prior, exists := paths[path]; exists {
				return nil, 0, fmt.Errorf("%w: duplicate file path at indices %d and %d", ErrInvalidMetainfo, prior, index)
			}
			paths[path] = index
			named = append(named, struct {
				path       string
				components []string
				index      int
			}{path: path, components: components, index: index})
		}
		end, ok := limits.Add(total, length)
		if !ok || end > limits.TorrentBytes {
			return nil, 0, fmt.Errorf("%w: total length exceeds supported limit", ErrInvalidMetainfo)
		}
		files = append(files, File{Index: index, Path: path, Range: ByteRange{Begin: total, End: end}, Kind: kind})
		total = end
	}
	for _, current := range named {
		prefix := ""
		for i := 0; i+1 < len(current.components); i++ {
			if i > 0 {
				prefix += "/"
			}
			prefix += current.components[i]
			if prior, exists := paths[prefix]; exists {
				return nil, 0, fmt.Errorf("%w: file path %q collides with directory at index %d", ErrInvalidMetainfo, current.path, prior)
			}
		}
	}
	return files, total, nil
}

type fileAttrs struct{ padding, symlink bool }

func fileAttributes(dict bencode.Value) (fileAttrs, error) {
	value, ok := dict.Lookup("attr")
	if !ok {
		return fileAttrs{}, nil
	}
	if value.Type != bencode.Bytes {
		return fileAttrs{}, fmt.Errorf("%w: attr is not a string", ErrInvalidMetainfo)
	}
	var attr fileAttrs
	for _, c := range value.Bytes {
		switch c {
		case 'p':
			attr.padding = true
		case 'l':
			attr.symlink = true
		}
	}
	if attr.padding && attr.symlink {
		return fileAttrs{}, fmt.Errorf("%w: padding and symlink attributes are incompatible", ErrInvalidMetainfo)
	}
	return attr, nil
}

func fileLength(dict bencode.Value, attr fileAttrs) (int64, error) {
	value, ok := dict.Lookup("length")
	if !ok {
		if attr.symlink {
			return 0, nil
		}
		return 0, fmt.Errorf("missing length")
	}
	if value.Type != bencode.Integer || value.Int < 0 || value.Int > limits.TorrentBytes {
		return 0, fmt.Errorf("length must be nonnegative and at most %d", limits.TorrentBytes)
	}
	if attr.symlink && value.Int != 0 {
		return 0, fmt.Errorf("symlink length must be zero")
	}
	return value.Int, nil
}

func filePath(dict bencode.Value, attr fileAttrs) (string, []string, error) {
	value, ok := dict.Lookup("path")
	if !ok {
		if attr.padding {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("missing path")
	}
	if value.Type != bencode.List {
		return "", nil, fmt.Errorf("path is not a list")
	}
	if len(value.List) == 0 && !attr.padding {
		return "", nil, fmt.Errorf("path is empty")
	}
	if len(value.List) > limits.PathComponents {
		return "", nil, fmt.Errorf("path has more than %d components", limits.PathComponents)
	}
	components := make([]string, len(value.List))
	pathBytes := 0
	for i, part := range value.List {
		if part.Type != bencode.Bytes || !utf8.Valid(part.Bytes) {
			return "", nil, fmt.Errorf("path component is not valid UTF-8")
		}
		component := string(part.Bytes)
		if err := validateComponent(component); err != nil {
			return "", nil, err
		}
		pathBytes += len(part.Bytes)
		if i != 0 {
			pathBytes++
		}
		components[i] = component
	}
	if pathBytes > limits.PathBytes {
		return "", nil, fmt.Errorf("path exceeds %d bytes", limits.PathBytes)
	}
	if attr.symlink {
		target, ok := dict.Lookup("symlink path")
		if !ok || target.Type != bencode.List || len(target.List) == 0 || len(target.List) > limits.PathComponents {
			return "", nil, fmt.Errorf("symlink path must be a nonempty component list")
		}
		targetBytes := 0
		for i, part := range target.List {
			if part.Type != bencode.Bytes || !utf8.Valid(part.Bytes) {
				return "", nil, fmt.Errorf("symlink path component is not valid UTF-8")
			}
			component := string(part.Bytes)
			if err := validateComponent(component); err != nil {
				return "", nil, fmt.Errorf("invalid symlink path component")
			}
			targetBytes += len(part.Bytes)
			if i != 0 {
				targetBytes++
			}
		}
		if targetBytes > limits.PathBytes {
			return "", nil, fmt.Errorf("symlink path exceeds %d bytes", limits.PathBytes)
		}
	}
	if attr.padding {
		return "", components, nil
	}
	return strings.Join(components, "/"), components, nil
}

func validateComponent(component string) error {
	if component == "" || len(component) > limits.PathBytes || component == "." || component == ".." || strings.ContainsAny(component, "/\\\x00") {
		return fmt.Errorf("invalid path component")
	}
	return nil
}

func equalHash(sum [20]byte, expected InfoHash) bool {
	for i := range sum {
		if sum[i] != expected[i] {
			return false
		}
	}
	return true
}
