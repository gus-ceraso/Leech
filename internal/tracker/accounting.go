package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	minAnnouncedPort  uint16 = 49152
	announcedPortSpan uint32 = 65535 - uint32(minAnnouncedPort) + 1
)

// Identity is the session-wide identity sent to every tracker. It is created
// once and can safely be reused by metadata and transfer phases.
type Identity struct {
	PeerID [20]byte
	Key    uint32
	Port   uint16
}

// GenerateIdentity creates an opaque peer ID, a tracker key, and an announced
// port in the dynamic range. The port is only an announce value: this package
// never probes, binds, or reserves it.
func GenerateIdentity(random io.Reader) (Identity, error) {
	if random == nil {
		random = rand.Reader
	}
	var identity Identity
	var raw [26]byte
	if _, err := io.ReadFull(random, raw[:]); err != nil {
		return Identity{}, err
	}
	copy(identity.PeerID[:], raw[:20])
	identity.Key = binary.BigEndian.Uint32(raw[20:24])
	// 65536 is exactly divisible by the dynamic range size (16384), so
	// reducing a uint16 does not introduce modulo bias here.
	identity.Port = minAnnouncedPort + uint16(binary.BigEndian.Uint16(raw[24:26])%uint16(announcedPortSpan))
	return identity, nil
}

// Snapshot is the immutable accounting view consumed immediately before an
// announce. Downloaded counts received file payload, including bytes later
// discarded or redownloaded. Retained and Total count real torrent bytes;
// synthetic padding is excluded by the caller's total.
type Snapshot struct {
	Downloaded int64
	Retained   int64
	Total      int64
	Metadata   bool
}

// Announce builds a request from this snapshot. Metadata discovery uses the
// approved left=1 sentinel until exact torrent accounting is available.
func (s Snapshot) Announce(identity Identity, infoHash [20]byte, event Event, numWant int32) (AnnounceRequest, error) {
	if s.Downloaded < 0 || s.Retained < 0 || s.Total < 0 || s.Retained > s.Total {
		return AnnounceRequest{}, errors.New("invalid tracker accounting snapshot")
	}
	left := int64(1)
	if !s.Metadata {
		left = s.Total - s.Retained
	}
	return AnnounceRequest{
		InfoHash:   infoHash,
		PeerID:     identity.PeerID,
		Downloaded: s.Downloaded,
		Left:       left,
		Uploaded:   0,
		Event:      event,
		Key:        identity.Key,
		NumWant:    numWant,
		Port:       identity.Port,
	}, nil
}

// Accounting is a small synchronized counter source suitable for a session
// coordinator. AddReceived records payload as soon as it is received, even
// when verification later rejects it. AddRetained records real bytes that are
// verified and committed; it is bounded by Total.
type Accounting struct {
	mu         sync.Mutex
	downloaded int64
	retained   int64
	total      int64
}

// NewAccounting creates counters for a known torrent. Metadata-only phases
// can use Snapshot directly because their total is not known yet.
func NewAccounting(total int64) (*Accounting, error) {
	if total < 0 {
		return nil, errors.New("negative torrent length")
	}
	return &Accounting{total: total}, nil
}

// AddReceived increments downloaded payload bytes. It rejects negative values
// and overflow instead of wrapping counters sent to an untrusted tracker.
func (a *Accounting) AddReceived(bytes int64) error {
	if bytes < 0 {
		return errors.New("negative received payload")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if bytes > 0 && a.downloaded > int64(^uint64(0)>>1)-bytes {
		return errors.New("received payload counter overflow")
	}
	a.downloaded += bytes
	return nil
}

// AddRetained increments verified real torrent bytes. The caller must count a
// byte once when it becomes retained, even if it was selected after resume.
func (a *Accounting) AddRetained(bytes int64) error {
	if bytes < 0 {
		return errors.New("negative retained payload")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if bytes > a.total-a.retained {
		return errors.New("retained payload exceeds torrent length")
	}
	a.retained += bytes
	return nil
}

// Snapshot returns a consistent announce view. The context is accepted so a
// coordinator can use the same seam as other bounded snapshot providers; this
// operation itself is nonblocking.
func (a *Accounting) Snapshot(ctx context.Context, metadata bool) (Snapshot, error) {
	if err := ctxErr(ctx); err != nil {
		return Snapshot{}, err
	}
	if a == nil {
		return Snapshot{Metadata: metadata}, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return Snapshot{Downloaded: a.downloaded, Retained: a.retained, Total: a.total, Metadata: metadata}, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
