// Package limits contains the supported-domain bounds shared by Leech components.
package limits

import "math"

const (
	MetainfoBytes           = 64 << 20
	BencodeValues           = 1_000_000
	DictionaryEntries       = 1_000_000
	ContainerEntries        = 200_000
	BencodeDepth            = 64
	TorrentBytes      int64 = 256 << 30
	Files                   = 100_000
	PathComponents          = 64
	PathBytes               = 4_096
	Pieces                  = 2_000_000
	PieceBytes        int64 = 64 << 20
	Trackers                = 64
	EndpointAttempts        = 100_000
	DNSAnswers              = 64
	MagnetPeers             = 1_024
	Candidates              = 20_000
	ActivePeers             = 64
	EndpointRaces           = 32
	StagedPieces            = 64
	StagedBytes       int64 = 512 << 20
	PeerFrameBytes          = 1 << 20
	PeerRequests            = 128
	GlobalRequests          = 4_096
	RequestTombstones       = 256
	MetadataRequests        = 32
	SessionEvents           = 4_096
	PeerCommands            = 256
	HTTPResponseBytes       = 8 << 20
	DatagramBytes           = 64 << 10
	UTPUnackedPackets       = 1_024
	UTPReorderPackets       = 2_048
	UTPBufferBytes          = 4 << 20
	MinTrackerSeconds       = 1
	MaxTrackerSeconds       = 7 * 24 * 60 * 60
	BlockBytes              = 16 << 10
)

// Add and Mul report overflow before an untrusted size can reach an allocation or offset.
func Add(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func Mul(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a == math.MinInt64 && b == -1 || b == math.MinInt64 && a == -1 {
		return 0, false
	}
	result := a * b
	return result, result/b == a
}
