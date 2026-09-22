package bencode

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalGoldenValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{name: "empty string", wire: "0:"},
		{name: "string", wire: "4:spam"},
		{name: "zero", wire: "i0e"},
		{name: "minimum int64", wire: "i-9223372036854775808e"},
		{name: "list", wire: "l4:spam4:eggse"},
		{name: "dictionary", wire: "d3:cow3:moo4:spam4:eggse"},
		{name: "nested", wire: "d4:spaml1:a1:bee"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := Decode([]byte(tc.wire))
			if err != nil {
				t.Fatalf("Decode(%q): %v", tc.wire, err)
			}
			if string(value.Raw) != tc.wire {
				t.Fatalf("raw span = %q, want %q", value.Raw, tc.wire)
			}
			encoded, err := Encode(value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if string(encoded) != tc.wire {
				t.Fatalf("encoded = %q, want %q", encoded, tc.wire)
			}
		})
	}
}

func TestExactNestedRawSpan(t *testing.T) {
	input := []byte("d4:infod3:bar3:baz4:name4:testee")
	root, err := Decode(input)
	if err != nil {
		t.Fatal(err)
	}
	info, ok := root.Lookup("info")
	if !ok {
		t.Fatal("missing info dictionary")
	}
	want := []byte("d3:bar3:baz4:name4:teste")
	if !bytes.Equal(info.Raw, want) {
		t.Fatalf("info raw = %q, want %q", info.Raw, want)
	}
	if !bytes.Equal(info.LookupMust(t, "name").Raw, []byte("4:test")) {
		t.Fatal("nested value did not retain its exact span")
	}
}

func TestPrefixLeavesBinaryPayload(t *testing.T) {
	header := []byte("d8:msg_typei1e5:piecei2ee")
	input := append(append([]byte(nil), header...), 0, 1, 2, 3)
	value, consumed, err := DecodePrefix(input)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(header) || !bytes.Equal(value.Raw, header) {
		t.Fatalf("consumed=%d raw=%q, want %d %q", consumed, value.Raw, len(header), header)
	}
	if _, err := Decode(input); !errors.Is(err, ErrTrailing) {
		t.Fatalf("full Decode error = %v, want ErrTrailing", err)
	}
}

func TestRejectNonCanonicalOrUnboundedValues(t *testing.T) {
	for _, wire := range []string{
		"d1:b1:21:a1:1e", // unsorted keys
		"d1:a1:11:a1:1e", // duplicate keys
		"i-0e", "i01e", "+1e", "i9223372036854775808e",
		"i-9223372036854775809e", "i1", "3abc", "01:a", "1:aX",
		"l1:a", "d1:a1:b", "1:a0:",
	} {
		if _, err := Decode([]byte(wire)); err == nil {
			t.Errorf("Decode(%q) succeeded", wire)
		}
	}
	if _, err := Decode([]byte("0:trailing")); !errors.Is(err, ErrTrailing) {
		t.Fatalf("trailing error = %v, want ErrTrailing", err)
	}

	bound := DefaultLimits()
	bound.MaxValues = 2
	if _, err := DecodeWithLimits([]byte("l1:a1:be"), bound); !errors.Is(err, ErrLimit) {
		t.Fatalf("value limit error = %v, want ErrLimit", err)
	}
	bound = DefaultLimits()
	bound.MaxContainerEntries = 1
	if _, err := DecodeWithLimits([]byte("l1:a1:be"), bound); !errors.Is(err, ErrLimit) {
		t.Fatalf("container limit error = %v, want ErrLimit", err)
	}
	bound = DefaultLimits()
	bound.MaxDepth = 2
	deep := strings.Repeat("l", 4) + "e" + strings.Repeat("e", 3)
	if _, err := DecodeWithLimits([]byte(deep), bound); !errors.Is(err, ErrLimit) {
		t.Fatalf("depth limit error = %v, want ErrLimit", err)
	}

	// Dictionary keys are counted by MaxDictionaryEntries, not MaxValues. The
	// root dictionary and its integer value fit this two-value budget.
	bound = DefaultLimits()
	bound.MaxValues = 2
	bound.MaxDictionaryEntries = 1
	if _, err := DecodeWithLimits([]byte("d1:ai1ee"), bound); err != nil {
		t.Fatalf("dictionary key consumed value budget: %v", err)
	}
	if _, err := DecodeWithLimits([]byte("d1:ai1e1:bi2ee"), bound); !errors.Is(err, ErrLimit) {
		t.Fatalf("dictionary entry limit error = %v, want ErrLimit", err)
	}

	defaults := DefaultLimits()
	for name, larger := range map[string]Limits{
		"bytes":              func() Limits { v := defaults; v.MaxBytes++; return v }(),
		"values":             func() Limits { v := defaults; v.MaxValues++; return v }(),
		"dictionary entries": func() Limits { v := defaults; v.MaxDictionaryEntries++; return v }(),
		"container entries":  func() Limits { v := defaults; v.MaxContainerEntries++; return v }(),
		"depth":              func() Limits { v := defaults; v.MaxDepth++; return v }(),
	} {
		t.Run("reject oversized "+name, func(t *testing.T) {
			if _, err := DecodeWithLimits([]byte("i0e"), larger); !errors.Is(err, ErrLimit) {
				t.Fatalf("DecodeWithLimits error = %v, want ErrLimit", err)
			}
			if _, err := EncodeWithLimits(Value{Type: Integer, Int: 0}, larger); !errors.Is(err, ErrLimit) {
				t.Fatalf("EncodeWithLimits error = %v, want ErrLimit", err)
			}
		})
	}
}

func TestEncodeSortsAndRejectsDuplicateKeys(t *testing.T) {
	value := Value{Type: Dictionary, Dict: []Entry{
		{Key: []byte("z"), Value: Value{Type: Bytes, Bytes: []byte("last")}},
		{Key: []byte("a"), Value: Value{Type: Integer, Int: 1}},
	}}
	got, err := Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "d1:ai1e1:z4:laste" {
		t.Fatalf("encoded = %q", got)
	}
	value.Dict = append(value.Dict, Entry{Key: []byte("a"), Value: Value{Type: Bytes}})
	if _, err := Encode(value); !errors.Is(err, ErrMalformed) {
		t.Fatalf("duplicate encode error = %v, want ErrMalformed", err)
	}
	value.Dict = []Entry{
		{Key: nil, Value: Value{Type: Integer, Int: 1}},
		{Key: []byte{}, Value: Value{Type: Integer, Int: 2}},
	}
	if _, err := Encode(value); !errors.Is(err, ErrMalformed) {
		t.Fatalf("duplicate empty-key encode error = %v, want ErrMalformed", err)
	}
}

// LookupMust is test-only shorthand that keeps the golden test focused on the
// span being asserted.
func (v Value) LookupMust(t *testing.T, key string) Value {
	t.Helper()
	got, ok := v.Lookup(key)
	if !ok {
		t.Fatalf("missing dictionary key %q", key)
	}
	return got
}

func FuzzDecodeBounded(f *testing.F) {
	for _, seed := range []string{
		"0:", "i0e", "l1:a1:be", "d3:cow3:moo4:spam4:eggse",
		"d8:msg_typei1e5:piecei2ee0000",
		"d1:a1:1e", "i-9223372036854775808e",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = Decode(input)
		value, consumed, err := DecodePrefix(input)
		if err != nil {
			return
		}
		if consumed <= 0 || consumed > len(input) || !bytes.Equal(value.Raw, input[:consumed]) {
			t.Fatalf("invalid prefix span: consumed=%d len=%d raw=%q", consumed, len(input), value.Raw)
		}
		if _, err := Decode(input[:consumed]); err != nil {
			t.Fatalf("prefix did not form complete value: %v", err)
		}
	})
}
