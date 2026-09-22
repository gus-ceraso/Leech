package utp

import "testing"

func TestSequenceWrapAndAmbiguity(t *testing.T) {
	if got := Sequence(0xffff).Add(1); got != 0 {
		t.Fatalf("wrapped sequence = %d", got)
	}
	distance, ok := SequenceDistance(Sequence(0xffff), Sequence(0))
	if !ok || distance != 1 {
		t.Fatalf("distance across wrap = %d, %v", distance, ok)
	}
	comparison, ok := CompareSequence(Sequence(0xffff), Sequence(0))
	if !ok || comparison >= 0 {
		t.Fatalf("comparison across wrap = %d, %v", comparison, ok)
	}
	if _, ok := Sequence(0).Advance(1 << 15); ok {
		t.Fatal("half-ring advance was accepted")
	}
	if _, ok := SequenceDistance(0, Sequence(1<<15)); ok {
		t.Fatal("half-ring distance was accepted")
	}
	if Sequence(0).Before(Sequence(1<<15)) || Sequence(1<<15).After(0) {
		t.Fatal("ambiguous order was reported as ordered")
	}
	if !InForwardRange(Sequence(0xffff), Sequence(1), 2) || InForwardRange(0, 3, 2) {
		t.Fatal("forward range check is incorrect")
	}
}

func TestTimestampWrapAndAmbiguity(t *testing.T) {
	if got := Timestamp(0xffffffff).Add(1); got != 0 {
		t.Fatalf("wrapped timestamp = %d", got)
	}
	distance, ok := TimestampDistance(Timestamp(0xffffffff), Timestamp(0))
	if !ok || distance != 1 {
		t.Fatalf("timestamp distance across wrap = %d, %v", distance, ok)
	}
	if _, ok := TimestampDistance(0, Timestamp(1<<31)); ok {
		t.Fatal("timestamp half-ring distance was accepted")
	}
}

func FuzzSequenceDistance(f *testing.F) {
	f.Add(uint16(0), uint16(0))
	f.Add(uint16(0xffff), uint16(0))
	f.Add(uint16(1234), uint16(5678))
	f.Fuzz(func(t *testing.T, start, delta uint16) {
		if uint32(delta) >= sequenceHalfRange {
			return
		}
		end := Sequence(start).Add(delta)
		got, ok := SequenceDistance(Sequence(start), end)
		if !ok || got != delta {
			t.Fatalf("distance(%#x,%#x) = %d, %v; want %d", start, end, got, ok, delta)
		}
		if !InForwardRange(Sequence(start), end, uint32(delta)) {
			t.Fatal("distance was not in its own forward range")
		}
	})
}
