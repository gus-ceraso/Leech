package peer

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestRequestTableFastChokeKeepsTerminalObligation(t *testing.T) {
	table := NewRequestTable(true)
	block := Block{Index: 2, Begin: 0, Length: 1024}
	if err := table.Add(block); err != nil {
		t.Fatal(err)
	}
	if err := table.Choke(); err != nil {
		t.Fatal(err)
	}
	if !table.Outstanding(block) || table.TombstoneCount() != 0 {
		t.Fatalf("Fast choke changed request state: outstanding=%v tombstones=%d", table.Outstanding(block), table.TombstoneCount())
	}
	terminal, err := table.Terminal(block, false)
	if err != nil || terminal != TerminalPiece {
		t.Fatalf("piece terminal = %v, %v", terminal, err)
	}
	if _, err := table.Terminal(block, false); !errors.Is(err, ErrUnexpectedTerminal) {
		t.Fatalf("duplicate terminal error = %v", err)
	}
}

func TestRequestTableCancelAndTombstoneCap(t *testing.T) {
	table, err := NewRequestTableWithCaps(false, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	a := Block{Index: 0, Begin: 0, Length: 1}
	b := Block{Index: 1, Begin: 0, Length: 1}
	if err := table.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(a); err != nil {
		t.Fatal(err)
	}
	if terminal, err := table.Terminal(a, true); err != nil || terminal != TerminalLateReject {
		t.Fatalf("late reject = %v, %v", terminal, err)
	}
	if err := table.Add(b); err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(b); err != nil {
		t.Fatal(err)
	}
	if err := table.Add(a); err != nil {
		t.Fatal(err)
	}
	if err := table.Cancel(a); !errors.Is(err, ErrTombstoneLimit) {
		t.Fatalf("full tombstone cap error = %v", err)
	}
	if !table.Outstanding(a) {
		t.Fatal("request was forgotten when tombstone cap was full")
	}
}

func TestPeerStateAvailabilityAndAllowedFastAreIndependent(t *testing.T) {
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 4, PieceLength: 16 << 10, Fast: true})
	if err != nil {
		t.Fatal(err)
	}
	if changed, interested, err := state.SetWanted(2, true); err != nil || changed || interested {
		t.Fatalf("initial wanted state = %v, %v, %v", changed, interested, err)
	}
	have := Message{ID: HaveID, Payload: uint32Payload(2)}
	effect, err := state.ApplyMessage(have)
	if err != nil || !effect.InterestChanged || !effect.Interested {
		t.Fatalf("Have effect = %#v, %v", effect, err)
	}
	if _, err := state.ApplyMessage(Message{ID: AllowedFastID, Payload: uint32Payload(2)}); err != nil {
		t.Fatal(err)
	}
	if !state.AllowedFast(2) || !state.Availability(2) {
		t.Fatal("availability and Allowed Fast were not retained")
	}
	effect, err = state.ApplyMessage(Message{ID: HaveNoneID})
	if err != nil || !effect.InterestChanged || effect.Interested || state.Availability(2) || !state.AllowedFast(2) {
		t.Fatalf("Have None state = %#v, avail=%v allowed=%v err=%v", effect, state.Availability(2), state.AllowedFast(2), err)
	}
	if state.CanRequest(2) {
		t.Fatal("Allowed Fast alone made an unavailable piece requestable")
	}
	if _, err := state.ApplyMessage(have); err != nil {
		t.Fatal(err)
	}
	if !state.CanRequest(2) {
		t.Fatal("availability plus Allowed Fast did not permit choked request")
	}
}

func TestPeerStateInitialAvailabilityOrdering(t *testing.T) {
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 8, Fast: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: BitfieldID, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: AllowedFastID, Payload: uint32Payload(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: HaveNoneID}); err != nil {
		t.Fatal(err)
	}
	if state.Availability(0) || !state.AllowedFast(3) {
		t.Fatalf("Have None did not clear ordinary availability: ordinary=%v allowed=%v", state.Availability(0), state.AllowedFast(3))
	}
	// A stale initial Bitfield or Have All is structurally valid, but cannot
	// restore ordinary availability after the first availability frame.
	if _, err := state.ApplyMessage(Message{ID: BitfieldID, Payload: []byte{0x80}}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: HaveAllID}); err != nil {
		t.Fatal(err)
	}
	if state.Availability(0) || state.Availability(7) || !state.AllowedFast(3) {
		t.Fatalf("late initial availability restored stale state: ordinary0=%v ordinary7=%v allowed3=%v", state.Availability(0), state.Availability(7), state.AllowedFast(3))
	}
	// Have None remains an idempotent clear and does not clear Allowed Fast.
	if _, err := state.ApplyMessage(Message{ID: HaveNoneID}); err != nil {
		t.Fatal(err)
	}
	if state.Availability(0) || !state.AllowedFast(3) {
		t.Fatal("repeated Have None changed the independent Allowed Fast state")
	}
	if _, err := state.ApplyMessage(Message{ID: BitfieldID, Payload: nil}); !IsProtocolViolation(err) {
		t.Fatalf("malformed late Bitfield error = %v", err)
	}
}

func TestPeerStateRepeatedInitialAvailabilityDoesNotRestoreState(t *testing.T) {
	for _, first := range []Message{
		{ID: BitfieldID, Payload: []byte{0x80}},
		{ID: HaveAllID},
		{ID: HaveNoneID},
	} {
		state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 8, Fast: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.ApplyMessage(first); err != nil {
			t.Fatalf("first %#v: %v", first, err)
		}
		if _, err := state.ApplyMessage(Message{ID: HaveNoneID}); err != nil {
			t.Fatalf("clear after %#v: %v", first, err)
		}
		if _, err := state.ApplyMessage(Message{ID: HaveAllID}); err != nil {
			t.Fatalf("late Have All after %#v: %v", first, err)
		}
		if _, err := state.ApplyMessage(Message{ID: BitfieldID, Payload: []byte{0xff}}); err != nil {
			t.Fatalf("late Bitfield after %#v: %v", first, err)
		}
		for index := uint32(0); index < 8; index++ {
			if state.Availability(index) {
				t.Fatalf("first %#v restored stale piece %d", first, index)
			}
		}
	}
}

func TestPeerStateLargeWantedAvailabilityUsesChangedBits(t *testing.T) {
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: limits.Pieces, Fast: true})
	if err != nil {
		t.Fatal(err)
	}
	wanted := make([]int, int(limits.Pieces))
	for index := range wanted {
		wanted[index] = index
	}
	if err := state.SetWantedPieces(wanted); err != nil {
		t.Fatal(err)
	}
	if state.RequestableCount() != 0 {
		t.Fatalf("initial requestable count = %d, want zero", state.RequestableCount())
	}

	last := uint32(limits.Pieces - 1)
	effect, err := state.ApplyMessage(Message{ID: HaveID, Payload: uint32Payload(last)})
	if err != nil || !effect.InterestChanged || !effect.Interested || len(effect.AvailabilityAdded) != 0 {
		t.Fatalf("first choked Have = %#v, %v", effect, err)
	}
	if state.RequestableCount() != 0 {
		t.Fatalf("ordinary choked Have requestable count = %d, want zero", state.RequestableCount())
	}
	for i := 0; i < 64; i++ {
		effect, err = state.ApplyMessage(Message{ID: HaveID, Payload: uint32Payload(last)})
		if err != nil || effect.InterestChanged || !effect.Interested || len(effect.AvailabilityAdded) != 0 || len(effect.AvailabilityRemoved) != 0 {
			t.Fatalf("repeated Have %d = %#v, %v", i, effect, err)
		}
	}

	effect, err = state.ApplyMessage(Message{ID: AllowedFastID, Payload: uint32Payload(last)})
	if err != nil || len(effect.AvailabilityAdded) != 1 || effect.AvailabilityAdded[0] != last {
		t.Fatalf("Allowed Fast availability delta = %#v, %v", effect, err)
	}
	if state.RequestableCount() != 1 {
		t.Fatalf("Allowed Fast requestable count = %d, want one", state.RequestableCount())
	}
	effect, err = state.ApplyMessage(Message{ID: HaveNoneID})
	if err != nil || !effect.InterestChanged || effect.Interested || len(effect.AvailabilityRemoved) != 1 || effect.AvailabilityRemoved[0] != last || !state.AllowedFast(last) {
		t.Fatalf("Have None delta = %#v, allowed=%v, err=%v", effect, state.AllowedFast(last), err)
	}
	if state.RequestableCount() != 0 {
		t.Fatalf("empty Have None requestable count = %d, want zero", state.RequestableCount())
	}
	for i := 0; i < 64; i++ {
		effect, err = state.ApplyMessage(Message{ID: HaveNoneID})
		if err != nil || effect.InterestChanged || effect.Interested || len(effect.AvailabilityAdded) != 0 || len(effect.AvailabilityRemoved) != 0 {
			t.Fatalf("repeated empty Have None %d = %#v, %v", i, effect, err)
		}
	}
}

func TestPeerStateRequestableCountAcrossBulkAndChokeTransitions(t *testing.T) {
	state, err := NewPeerState(4, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SetWantedPieces([]int{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: AllowedFastID, Payload: uint32Payload(2)}); err != nil {
		t.Fatal(err)
	}
	effect, err := state.ApplyMessage(Message{ID: HaveAllID})
	if err != nil || state.RequestableCount() != 1 || len(effect.AvailabilityAdded) != 1 || effect.AvailabilityAdded[0] != 2 {
		t.Fatalf("Have All requestable state = %#v, count %d, err %v", effect, state.RequestableCount(), err)
	}
	effect, err = state.ApplyMessage(Message{ID: UnchokeID})
	if err != nil || state.RequestableCount() != 3 || len(effect.AvailabilityAdded) != 2 {
		t.Fatalf("Unchoke requestable state = %#v, count %d, err %v", effect, state.RequestableCount(), err)
	}
	effect, err = state.ApplyMessage(Message{ID: ChokeID})
	if err != nil || state.RequestableCount() != 1 || len(effect.AvailabilityRemoved) != 2 {
		t.Fatalf("Choke requestable state = %#v, count %d, err %v", effect, state.RequestableCount(), err)
	}
	effect, err = state.ApplyMessage(Message{ID: HaveNoneID})
	if err != nil || state.RequestableCount() != 0 || len(effect.AvailabilityRemoved) != 1 || effect.AvailabilityRemoved[0] != 2 || !state.AllowedFast(2) {
		t.Fatalf("Have None requestable state = %#v, count %d, err %v", effect, state.RequestableCount(), err)
	}

	bitfield, err := NewPeerState(16, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := bitfield.SetWantedPieces([]int{0, 3, 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := bitfield.ApplyMessage(Message{ID: UnchokeID}); err != nil {
		t.Fatal(err)
	}
	effect, err = bitfield.ApplyMessage(Message{ID: BitfieldID, Payload: []byte{0x90, 0x80}})
	if err != nil || bitfield.RequestableCount() != 3 || len(effect.AvailabilityAdded) != 3 {
		t.Fatalf("Bitfield requestable state = %#v, count %d, err %v", effect, bitfield.RequestableCount(), err)
	}
}

func TestPeerStateIncomingRequestsNeverProducePayload(t *testing.T) {
	state, err := NewPeerState(3, true)
	if err != nil {
		t.Fatal(err)
	}
	block := Block{Index: 1, Begin: 0, Length: 1024}
	payload := blockPayload(block.Index, block.Begin, block.Length)
	effect, err := state.ApplyMessage(Message{ID: RequestID, Payload: payload})
	if err != nil || effect.Response == nil || effect.Response.ID != RejectRequestID {
		t.Fatalf("incoming Fast request effect = %#v, %v", effect, err)
	}
	if len(effect.Response.Payload) != 12 {
		t.Fatal("reject response does not identify the request")
	}
	if _, err := state.ApplyMessage(Message{ID: RequestID, Payload: payload}); !errors.Is(err, ErrIncomingRequest) || !IsProtocolViolation(err) || ClassOf(err) != ClassProtocolViolation {
		t.Fatalf("repeated incoming request error = %v", err)
	}
	nonFast, err := NewPeerState(3, false)
	if err != nil {
		t.Fatal(err)
	}
	effect, err = nonFast.ApplyMessage(Message{ID: RequestID, Payload: payload})
	if err != nil || effect.Response != nil {
		t.Fatalf("non-Fast incoming request = %#v, %v", effect, err)
	}
}

func TestPeerStateUnsolicitedTerminalIsProtocolViolation(t *testing.T) {
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 1, PieceLength: 1024, Fast: true})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 9)
	if _, err := state.ApplyMessage(Message{ID: PieceID, Payload: payload}); !errors.Is(err, ErrUnexpectedTerminal) || !IsProtocolViolation(err) || ClassOf(err) != ClassProtocolViolation {
		t.Fatalf("unsolicited piece error = %v, class=%v", err, ClassOf(err))
	}
	reject := blockPayload(0, 0, 1)
	if _, err := state.ApplyMessage(Message{ID: RejectRequestID, Payload: reject}); !errors.Is(err, ErrUnexpectedTerminal) || !IsProtocolViolation(err) {
		t.Fatalf("unsolicited reject error = %v", err)
	}
}

func TestPeerStateReqQPresence(t *testing.T) {
	implicit, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if implicit.ReqQ() != 128 {
		t.Fatalf("missing reqq = %d", implicit.ReqQ())
	}
	explicit, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 1, ReqQSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if explicit.ReqQ() != 0 {
		t.Fatalf("explicit zero reqq = %d", explicit.ReqQ())
	}
	if got := explicit.SetReqQHint(0, false); got != 128 {
		t.Fatalf("missing later reqq = %d", got)
	}
	if got := explicit.SetReqQ(129); got != 128 {
		t.Fatalf("clamped later reqq = %d", got)
	}
}

func TestPeerStateLatePieceIsConsumedOnce(t *testing.T) {
	state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 1, PieceLength: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.SetWanted(0, true); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: HaveID, Payload: uint32Payload(0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyMessage(Message{ID: UnchokeID}); err != nil {
		t.Fatal(err)
	}
	block := Block{Index: 0, Begin: 0, Length: 512}
	if err := state.AddRequest(block); err != nil {
		t.Fatal(err)
	}
	if err := state.TimeoutRequest(block); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8+block.Length)
	binary.BigEndian.PutUint32(payload[:4], block.Index)
	binary.BigEndian.PutUint32(payload[4:8], block.Begin)
	effect, err := state.ApplyMessage(Message{ID: PieceID, Payload: payload})
	if err != nil || effect.Terminal != TerminalLatePiece {
		t.Fatalf("late piece = %#v, %v", effect, err)
	}
	if _, err := state.ApplyMessage(Message{ID: PieceID, Payload: payload}); !errors.Is(err, ErrUnexpectedTerminal) {
		t.Fatalf("duplicate late piece error = %v", err)
	}
}

func uint32Payload(value uint32) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, value)
	return payload
}

func FuzzRequestTableTransitions(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3}, true)
	f.Add([]byte{255, 128, 7}, false)
	f.Fuzz(func(t *testing.T, input []byte, fast bool) {
		table, err := NewRequestTableWithCaps(fast, 4, 4)
		if err != nil {
			t.Fatal(err)
		}
		for i, value := range input {
			block := Block{Index: uint32(value % 4), Begin: uint32(i * 32), Length: 1}
			switch value % 5 {
			case 0:
				_ = table.Add(block)
			case 1:
				_ = table.Cancel(block)
			case 2:
				_ = table.Timeout(block)
			case 3:
				_, _ = table.Terminal(block, value&1 != 0)
			default:
				_ = table.Choke()
			}
		}
	})
}

func FuzzPeerStateMessages(f *testing.F) {
	f.Add(byte(HaveID), []byte{0, 0, 0, 0})
	f.Add(byte(PieceID), []byte{0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, id byte, payload []byte) {
		state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 8, PieceLength: 16 << 10, Fast: true})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = state.ApplyMessage(Message{ID: id, Payload: payload})
	})
}

func FuzzPeerInitialAvailabilitySequences(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 2, 1})
	f.Add([]byte{2, 2, 0, 3, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		state, err := NewPeerStateWithConfig(PeerStateConfig{PieceCount: 8, Fast: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range input {
			var message Message
			switch value % 4 {
			case 0:
				message = Message{ID: BitfieldID, Payload: []byte{0x80}}
			case 1:
				message = Message{ID: HaveAllID}
			case 2:
				message = Message{ID: HaveNoneID}
			default:
				message = Message{ID: AllowedFastID, Payload: uint32Payload(uint32(value % 8))}
			}
			_, _ = state.ApplyMessage(message)
		}
	})
}
