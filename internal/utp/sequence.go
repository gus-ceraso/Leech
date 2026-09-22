package utp

// Sequence is a uTP packet sequence number. Sequence numbers are compared on
// a 16-bit ring. A distance of exactly half the ring is ambiguous and is
// rejected by the checked helpers below.
type Sequence uint16

// Timestamp is the low 32 bits of a uTP microsecond timestamp.
//
// uTP timestamps are deliberately not wall-clock values. They are compared
// only by modular distance, just like packet sequence numbers.
type Timestamp uint32

const (
	sequenceHalfRange  = uint32(1) << 15
	timestampHalfRange = uint64(1) << 31
)

// Add advances a sequence number modulo 2^16. Use Advance when the amount is
// derived from untrusted or bounded protocol state and must be checked.
func (s Sequence) Add(delta uint16) Sequence {
	return Sequence(uint16(s) + delta)
}

// Advance advances a sequence number when delta is within the unambiguous
// forward half of the sequence ring. The returned bool is false for a delta
// that cannot be safely used to order sequence numbers.
func (s Sequence) Advance(delta uint32) (Sequence, bool) {
	if delta >= sequenceHalfRange {
		return 0, false
	}
	return s.Add(uint16(delta)), true
}

// Distance returns the forward modular distance from from to to. The bool is
// false when the result is exactly half the sequence ring, where direction is
// ambiguous.
func SequenceDistance(from, to Sequence) (uint16, bool) {
	distance := uint16(to - from)
	if uint32(distance) == sequenceHalfRange {
		return distance, false
	}
	return distance, true
}

// CompareSequence reports -1, 0, or 1 according to modular sequence order.
// The bool is false only for the exactly-half-ring ambiguous case.
func CompareSequence(a, b Sequence) (int, bool) {
	distance, ok := SequenceDistance(a, b)
	if !ok {
		return 0, false
	}
	if distance == 0 {
		return 0, true
	}
	if uint32(distance) < sequenceHalfRange {
		return -1, true
	}
	return 1, true
}

// Before reports whether s precedes other in modular sequence order. It
// returns false when the order is equal or ambiguous; CompareSequence exposes
// the ambiguity to callers that need to distinguish those cases.
func (s Sequence) Before(other Sequence) bool {
	comparison, ok := CompareSequence(s, other)
	return ok && comparison < 0
}

// After reports whether s follows other in modular sequence order.
func (s Sequence) After(other Sequence) bool {
	comparison, ok := CompareSequence(s, other)
	return ok && comparison > 0
}

// InForwardRange reports whether value is at most count packets after start.
// count must be in the unambiguous half of the sequence ring. A zero count
// includes only start.
func InForwardRange(start, value Sequence, count uint32) bool {
	if count >= sequenceHalfRange {
		return false
	}
	distance, ok := SequenceDistance(start, value)
	return ok && uint32(distance) <= count
}

// Add advances a timestamp modulo 2^32.
func (t Timestamp) Add(delta uint32) Timestamp {
	return Timestamp(uint32(t) + delta)
}

// TimestampDistance returns the forward modular distance from from to to. The
// bool is false when the result is exactly half the timestamp ring.
func TimestampDistance(from, to Timestamp) (uint32, bool) {
	distance := uint32(to - from)
	if uint64(distance) == timestampHalfRange {
		return distance, false
	}
	return distance, true
}

// CompareTimestamp reports -1, 0, or 1 according to modular timestamp order.
// The bool is false only for the exactly-half-ring ambiguous case.
func CompareTimestamp(a, b Timestamp) (int, bool) {
	distance, ok := TimestampDistance(a, b)
	if !ok {
		return 0, false
	}
	if distance == 0 {
		return 0, true
	}
	if uint64(distance) < timestampHalfRange {
		return -1, true
	}
	return 1, true
}

// Before reports whether t precedes other in modular timestamp order.
func (t Timestamp) Before(other Timestamp) bool {
	comparison, ok := CompareTimestamp(t, other)
	return ok && comparison < 0
}

// After reports whether t follows other in modular timestamp order.
func (t Timestamp) After(other Timestamp) bool {
	comparison, ok := CompareTimestamp(t, other)
	return ok && comparison > 0
}
