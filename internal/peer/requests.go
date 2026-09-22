package peer

import (
	"errors"
	"fmt"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// Block identifies one requested block. The tuple, rather than a peer ID or
// piece index alone, is the identity used to match terminal responses.
type Block struct {
	Index  uint32
	Begin  uint32
	Length uint32
}

// Terminal identifies the one terminal response which completed a request.
type Terminal uint8

const (
	TerminalPiece Terminal = iota + 1
	TerminalReject
	TerminalLatePiece
	TerminalLateReject
)

var (
	ErrRequestExists        = errors.New("peer request already outstanding")
	ErrRequestLimit         = errors.New("peer request limit reached")
	ErrRequestMissing       = errors.New("peer request is not outstanding")
	ErrUnexpectedTerminal   = errors.New("unexpected peer request terminal")
	ErrTombstoneLimit       = errors.New("peer request tombstone limit reached")
	ErrInvalidBlock         = errors.New("invalid peer block")
	ErrInvalidRequestConfig = errors.New("invalid peer request configuration")
)

// RequestTable tracks a single connection's request obligations. It is
// deliberately not concurrent-safe: the session coordinator owns it, while
// connection workers only report decoded messages.
type RequestTable struct {
	fast           bool
	maxOutstanding int
	maxTombstones  int
	outstanding    map[Block]struct{}
	tombstones     map[Block]struct{}
}

// NewRequestTable creates a request table using the supported per-peer caps.
func NewRequestTable(fast bool) *RequestTable {
	return &RequestTable{
		fast:           fast,
		maxOutstanding: limits.PeerRequests,
		maxTombstones:  limits.RequestTombstones,
		outstanding:    make(map[Block]struct{}),
		tombstones:     make(map[Block]struct{}),
	}
}

// NewRequestTableWithCaps is useful for deterministic tests and for a
// coordinator that has already applied a smaller local pipeline limit.
func NewRequestTableWithCaps(fast bool, maxOutstanding, maxTombstones int) (*RequestTable, error) {
	if maxOutstanding < 1 || maxOutstanding > limits.PeerRequests || maxTombstones < 1 || maxTombstones > limits.RequestTombstones {
		return nil, ErrInvalidRequestConfig
	}
	return &RequestTable{
		fast:           fast,
		maxOutstanding: maxOutstanding,
		maxTombstones:  maxTombstones,
		outstanding:    make(map[Block]struct{}, maxOutstanding),
		tombstones:     make(map[Block]struct{}, maxTombstones),
	}, nil
}

// Fast reports whether the table applies BEP 6 choke semantics.
func (r *RequestTable) Fast() bool { return r != nil && r.fast }

func (r *RequestTable) OutstandingCount() int {
	if r == nil {
		return 0
	}
	return len(r.outstanding)
}

func (r *RequestTable) TombstoneCount() int {
	if r == nil {
		return 0
	}
	return len(r.tombstones)
}

func (r *RequestTable) Outstanding(block Block) bool {
	return r != nil && hasBlock(r.outstanding, block)
}

func (r *RequestTable) Tombstoned(block Block) bool {
	return r != nil && hasBlock(r.tombstones, block)
}

// Add records an outbound request after its command has been admitted by the
// coordinator. A tombstoned tuple cannot be reused until its late terminal is
// consumed, which prevents a late response from being attributed to a newer
// request with identical fields.
func (r *RequestTable) Add(block Block) error {
	if err := validateBlockTuple(block); err != nil {
		return err
	}
	if hasBlock(r.outstanding, block) {
		return ErrRequestExists
	}
	if hasBlock(r.tombstones, block) {
		return fmt.Errorf("%w: tuple is tombstoned", ErrRequestExists)
	}
	if len(r.outstanding) >= r.maxOutstanding {
		return ErrRequestLimit
	}
	r.outstanding[block] = struct{}{}
	return nil
}

// Cancel moves a request to the tombstone set. The caller should send the
// wire Cancel separately; this method only changes coordinator state.
func (r *RequestTable) Cancel(block Block) error { return r.tombstone(block) }

// Timeout has the same obligation semantics as Cancel.
func (r *RequestTable) Timeout(block Block) error { return r.tombstone(block) }

func (r *RequestTable) tombstone(block Block) error {
	if err := validateBlockTuple(block); err != nil {
		return err
	}
	if !hasBlock(r.outstanding, block) {
		return ErrRequestMissing
	}
	if len(r.tombstones) >= r.maxTombstones {
		// Do not delete an older tombstone to make room. The caller must close
		// the connection before any obligation is forgotten.
		return ErrTombstoneLimit
	}
	delete(r.outstanding, block)
	r.tombstones[block] = struct{}{}
	return nil
}

// Choke applies BEP 6 or ordinary BEP 3 semantics. Fast requests remain
// outstanding across a choke. Ordinary requests are released but retain a
// bounded late-response tombstone.
func (r *RequestTable) Choke() error {
	if r.fast {
		return nil
	}
	if len(r.outstanding) > r.maxTombstones-len(r.tombstones) {
		return ErrTombstoneLimit
	}
	for block := range r.outstanding {
		delete(r.outstanding, block)
		r.tombstones[block] = struct{}{}
	}
	return nil
}

// Terminal consumes one exact Piece or Reject Request terminal. The caller
// supplies the same tuple carried by the wire message; payload bytes are
// checked by the connection-state layer before this method is called.
func (r *RequestTable) Terminal(block Block, reject bool) (Terminal, error) {
	if err := validateBlockTuple(block); err != nil {
		return 0, err
	}
	if hasBlock(r.outstanding, block) {
		delete(r.outstanding, block)
		if reject {
			return TerminalReject, nil
		}
		return TerminalPiece, nil
	}
	if hasBlock(r.tombstones, block) {
		delete(r.tombstones, block)
		if reject {
			return TerminalLateReject, nil
		}
		return TerminalLatePiece, nil
	}
	return 0, ErrUnexpectedTerminal
}

// Release closes all normal outstanding requests without creating a new
// obligation. It is only for connection teardown, after no more messages can
// arrive; cancel and timeout must use their tombstone-preserving methods.
func (r *RequestTable) Release() {
	if r == nil {
		return
	}
	clear(r.outstanding)
	clear(r.tombstones)
}

// ClampReqQ applies the local request pipeline cap to a remote reqq hint.
// A zero hint means that no additional requests may be pipelined.
func ClampReqQ(reqq uint32) int {
	if reqq > limits.PeerRequests {
		return limits.PeerRequests
	}
	return int(reqq)
}

func validateBlockTuple(block Block) error {
	if block.Length == 0 || block.Length > limits.BlockBytes {
		return fmt.Errorf("%w: length %d is outside 1..%d", ErrInvalidBlock, block.Length, limits.BlockBytes)
	}
	if block.Begin > ^uint32(0)-block.Length {
		return fmt.Errorf("%w: offset overflows uint32", ErrInvalidBlock)
	}
	return nil
}

func hasBlock(set map[Block]struct{}, block Block) bool {
	_, ok := set[block]
	return ok
}
