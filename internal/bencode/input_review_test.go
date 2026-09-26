package bencode

import (
	"errors"
	"strings"
	"testing"
)

func TestInputReviewCanonicalIntegerLengthAndEmptyKeyBoundaries(t *testing.T) {
	for _, wire := range []string{"i9223372036854775807e", "i-9223372036854775808e", "d0:i0e1:ai1ee"} {
		if _, err := Decode([]byte(wire)); err != nil {
			t.Fatalf("canonical %q: %v", wire, err)
		}
	}
	for _, wire := range []string{
		"d0:i0e0:i1ee", "d1:ai0e0:i1ee", "i18446744073709551616e",
		"9223372036854775808:", "18446744073709551616:", "00:", "-1:a",
	} {
		if _, err := Decode([]byte(wire)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed %q: %v", wire, err)
		}
	}
}

func TestInputReviewIndependentDecodeBudgets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		wire  string
		bound Limits
	}{
		{"bytes", "1:a", Limits{MaxBytes: 3}},
		{"values", "ld1:ai0e1:bleee", Limits{MaxValues: 4}},
		{"dictionary entries across containers", "ld1:ai0eed1:bi0eee", Limits{MaxDictionaryEntries: 2}},
		{"dictionary container", "d1:ai0e1:bi0ee", Limits{MaxContainerEntries: 2}},
		{"depth", "ll0:ee", Limits{MaxDepth: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeWithLimits([]byte(tc.wire), tc.bound); err != nil {
				t.Fatalf("exact boundary rejected: %v", err)
			}
			switch tc.name {
			case "bytes":
				tc.bound.MaxBytes--
			case "values":
				tc.bound.MaxValues--
			case "dictionary entries across containers":
				tc.bound.MaxDictionaryEntries--
			case "dictionary container":
				tc.bound.MaxContainerEntries--
			case "depth":
				tc.bound.MaxDepth--
			}
			if _, err := DecodeWithLimits([]byte(tc.wire), tc.bound); !errors.Is(err, ErrLimit) {
				t.Fatalf("budget exhaustion = %v", err)
			}
		})
	}
	// Opaque trailing bytes still count toward the caller's wire-byte budget.
	if _, _, err := DecodePrefixWithLimits([]byte("i0e"+strings.Repeat("x", 30)), Limits{MaxBytes: 32}); !errors.Is(err, ErrLimit) {
		t.Fatalf("prefix input budget = %v", err)
	}
}
