// Package bencode implements the bounded, canonical bencoding used by
// BitTorrent. Decoded byte strings and Raw fields refer to the input passed to
// Decode; callers that need the bytes after Decode returns must retain that
// input.
package bencode

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// Kind identifies a bencoded value.
type Kind uint8

const (
	Invalid Kind = iota
	Integer
	Bytes
	List
	Dictionary

	// Short aliases match the terminology used in the wire format.
	Int    = Integer
	String = Bytes
	Dict   = Dictionary
)

// Value is a decoded bencoded value.
//
// Type selects the field containing the decoded value: Integer for Int,
// Bytes for String, List for List, and Dict for Dictionary. Raw is the exact
// encoded span of this value, including its prefix and delimiters. Raw and
// Bytes point into the input supplied to Decode or DecodePrefix.
type Value struct {
	Type Type

	Int   int64
	Bytes []byte
	List  []Value
	Dict  []Entry
	Raw   []byte
}

// Type is kept as an alias so callers can use either Value.Type or Kind in
// declarations without introducing another representation for decoded values.
type Type = Kind

// Entry is one dictionary key and value. Dictionary keys are byte strings,
// and decoded dictionaries are always in strictly increasing raw-byte order.
type Entry struct {
	Key   []byte
	Value Value
}

var (
	// ErrMalformed indicates a non-canonical or otherwise invalid bencoded
	// value.
	ErrMalformed = errors.New("bencode: malformed value")
	// ErrLimit indicates that a supported decoder or encoder bound was hit.
	ErrLimit = errors.New("bencode: limit exceeded")
	// ErrTrailing indicates that Decode found bytes after its one complete
	// value. DecodePrefix intentionally permits those bytes.
	ErrTrailing = errors.New("bencode: trailing bytes")
)

// Limits controls resource bounds for one decode or encode operation. A zero
// field is replaced by the corresponding DefaultLimits value. Values above
// DefaultLimits are rejected because the DESIGN §16 bounds are fixed supported
// domain limits; callers can use smaller values for local tests or profiles.
type Limits struct {
	MaxBytes             int
	MaxValues            int
	MaxDictionaryEntries int
	MaxContainerEntries  int
	MaxDepth             int
}

// DefaultLimits returns the supported bencoding bounds from internal/limits.
func DefaultLimits() Limits {
	return Limits{
		MaxBytes:             limits.MetainfoBytes,
		MaxValues:            limits.BencodeValues,
		MaxDictionaryEntries: limits.DictionaryEntries,
		MaxContainerEntries:  limits.ContainerEntries,
		MaxDepth:             limits.BencodeDepth,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxBytes == 0 {
		l.MaxBytes = d.MaxBytes
	}
	if l.MaxValues == 0 {
		l.MaxValues = d.MaxValues
	}
	if l.MaxDictionaryEntries == 0 {
		l.MaxDictionaryEntries = d.MaxDictionaryEntries
	}
	if l.MaxContainerEntries == 0 {
		l.MaxContainerEntries = d.MaxContainerEntries
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = d.MaxDepth
	}
	return l
}

// Decode decodes exactly one complete value. The input is bounded before any
// recursive work or allocation is attempted.
func Decode(input []byte) (Value, error) {
	return DecodeWithLimits(input, DefaultLimits())
}

// DecodeWithLimits is Decode with explicit supported-domain bounds.
func DecodeWithLimits(input []byte, bound Limits) (Value, error) {
	bound = bound.withDefaults()
	if err := validateLimits(bound); err != nil {
		return Value{}, err
	}
	if len(input) > bound.MaxBytes {
		return Value{}, ErrLimit
	}
	p := parser{input: input, bound: bound}
	v, end, err := p.value(0, 0)
	if err != nil {
		return Value{}, err
	}
	if end != len(input) {
		return Value{}, ErrTrailing
	}
	return v, nil
}

// DecodePrefix decodes the first complete value and returns the number of
// bytes consumed. It is intended for extension messages whose bencoded
// header is followed by an opaque binary payload. The returned Value.Raw ends
// at consumed, and all remaining bytes are left to the caller.
func DecodePrefix(input []byte) (Value, int, error) {
	return DecodePrefixWithLimits(input, DefaultLimits())
}

// DecodePrefixWithLimits is DecodePrefix with explicit supported-domain bounds.
func DecodePrefixWithLimits(input []byte, bound Limits) (Value, int, error) {
	bound = bound.withDefaults()
	if err := validateLimits(bound); err != nil {
		return Value{}, 0, err
	}
	if len(input) > bound.MaxBytes {
		return Value{}, 0, ErrLimit
	}
	p := parser{input: input, bound: bound}
	v, end, err := p.value(0, 0)
	if err != nil {
		return Value{}, 0, err
	}
	return v, end, nil
}

func validateLimits(l Limits) error {
	if l.MaxBytes < 0 || l.MaxValues < 0 || l.MaxDictionaryEntries < 0 ||
		l.MaxContainerEntries < 0 || l.MaxDepth < 0 {
		return ErrLimit
	}
	d := DefaultLimits()
	if l.MaxBytes > d.MaxBytes || l.MaxValues > d.MaxValues ||
		l.MaxDictionaryEntries > d.MaxDictionaryEntries ||
		l.MaxContainerEntries > d.MaxContainerEntries || l.MaxDepth > d.MaxDepth {
		return ErrLimit
	}
	return nil
}

type parser struct {
	input []byte
	bound Limits
	// values counts bencoded nodes other than dictionary keys. Keys are
	// accounted for separately by entries, which prevents one dictionary
	// entry from consuming the value budget twice.
	values  int
	entries int
}

func (p *parser) value(start, depth int) (Value, int, error) {
	if start >= len(p.input) {
		return Value{}, 0, ErrMalformed
	}
	if depth > p.bound.MaxDepth {
		return Value{}, 0, ErrLimit
	}
	if p.values >= p.bound.MaxValues {
		return Value{}, 0, ErrLimit
	}
	p.values++

	switch p.input[start] {
	case 'i':
		return p.integer(start)
	case 'l':
		return p.list(start, depth)
	case 'd':
		return p.dictionary(start, depth)
	default:
		if p.input[start] >= '0' && p.input[start] <= '9' {
			return p.string(start)
		}
		return Value{}, 0, ErrMalformed
	}
}

func (p *parser) integer(start int) (Value, int, error) {
	i := start + 1
	if i >= len(p.input) {
		return Value{}, 0, ErrMalformed
	}

	negative := p.input[i] == '-'
	if negative {
		i++
		if i >= len(p.input) || p.input[i] == '0' {
			return Value{}, 0, ErrMalformed
		}
	}
	if i >= len(p.input) || p.input[i] < '0' || p.input[i] > '9' {
		return Value{}, 0, ErrMalformed
	}
	if p.input[i] == '0' && i+1 < len(p.input) && p.input[i+1] != 'e' {
		return Value{}, 0, ErrMalformed
	}

	var n uint64
	for ; i < len(p.input) && p.input[i] != 'e'; i++ {
		c := p.input[i]
		if c < '0' || c > '9' {
			return Value{}, 0, ErrMalformed
		}
		digit := uint64(c - '0')
		if n > (math.MaxUint64-digit)/10 {
			return Value{}, 0, ErrMalformed
		}
		n = n*10 + digit
	}
	if i >= len(p.input) || p.input[i] != 'e' {
		return Value{}, 0, ErrMalformed
	}

	var number int64
	if negative {
		if n > uint64(math.MaxInt64)+1 {
			return Value{}, 0, ErrMalformed
		}
		if n == uint64(math.MaxInt64)+1 {
			number = math.MinInt64
		} else {
			number = -int64(n)
		}
	} else {
		if n > uint64(math.MaxInt64) {
			return Value{}, 0, ErrMalformed
		}
		number = int64(n)
	}
	end := i + 1
	return Value{Type: Integer, Int: number, Raw: p.input[start:end]}, end, nil
}

func (p *parser) string(start int) (Value, int, error) {
	i := start
	if p.input[i] == '0' {
		if i+1 >= len(p.input) || p.input[i+1] != ':' {
			return Value{}, 0, ErrMalformed
		}
		i++
	} else {
		for i < len(p.input) && p.input[i] >= '0' && p.input[i] <= '9' {
			i++
		}
		if i >= len(p.input) || p.input[i] != ':' {
			return Value{}, 0, ErrMalformed
		}
	}

	colon := i
	var length int64
	for j := start; j < colon; j++ {
		digit := uint64(p.input[j] - '0')
		if length > (math.MaxInt64-int64(digit))/10 {
			return Value{}, 0, ErrMalformed
		}
		length = length*10 + int64(digit)
	}
	if length > int64(len(p.input)-colon-1) {
		return Value{}, 0, ErrMalformed
	}
	if length > int64(p.bound.MaxBytes) {
		return Value{}, 0, ErrLimit
	}
	begin := colon + 1
	end64 := int64(begin) + length
	if end64 < int64(begin) || end64 > int64(len(p.input)) {
		return Value{}, 0, ErrMalformed
	}
	end := int(end64)
	return Value{Type: Bytes, Bytes: p.input[begin:end], Raw: p.input[start:end]}, end, nil
}

func (p *parser) list(start, depth int) (Value, int, error) {
	values := make([]Value, 0)
	i := start + 1
	for {
		if i >= len(p.input) {
			return Value{}, 0, ErrMalformed
		}
		if p.input[i] == 'e' {
			end := i + 1
			return Value{Type: List, List: values, Raw: p.input[start:end]}, end, nil
		}
		if len(values) >= p.bound.MaxContainerEntries {
			return Value{}, 0, ErrLimit
		}
		v, end, err := p.value(i, depth+1)
		if err != nil {
			return Value{}, 0, err
		}
		values = append(values, v)
		i = end
	}
}

func (p *parser) dictionary(start, depth int) (Value, int, error) {
	entries := make([]Entry, 0)
	i := start + 1
	var previous []byte
	for {
		if i >= len(p.input) {
			return Value{}, 0, ErrMalformed
		}
		if p.input[i] == 'e' {
			end := i + 1
			return Value{Type: Dictionary, Dict: entries, Raw: p.input[start:end]}, end, nil
		}
		if len(entries) >= p.bound.MaxContainerEntries {
			return Value{}, 0, ErrLimit
		}
		if p.input[i] < '0' || p.input[i] > '9' {
			return Value{}, 0, ErrMalformed
		}
		key, end, err := p.string(i)
		if err != nil {
			return Value{}, 0, err
		}
		if previous != nil && bytes.Compare(previous, key.Bytes) >= 0 {
			return Value{}, 0, ErrMalformed
		}
		previous = key.Bytes
		p.entries++
		if p.entries > p.bound.MaxDictionaryEntries {
			return Value{}, 0, ErrLimit
		}
		value, valueEnd, err := p.value(end, depth+1)
		if err != nil {
			return Value{}, 0, err
		}
		entries = append(entries, Entry{Key: key.Bytes, Value: value})
		i = valueEnd
	}
}

// Lookup returns the value associated with key in a dictionary.
func (v Value) Lookup(key string) (Value, bool) {
	if v.Type != Dictionary {
		return Value{}, false
	}
	needle := []byte(key)
	i := sort.Search(len(v.Dict), func(i int) bool {
		return bytes.Compare(v.Dict[i].Key, needle) >= 0
	})
	if i < len(v.Dict) && bytes.Equal(v.Dict[i].Key, needle) {
		return v.Dict[i].Value, true
	}
	return Value{}, false
}

// Encode returns the canonical bencoding of v. Decoded Raw spans are ignored;
// encoding always serializes the structured fields. Dictionary entries are
// sorted by raw key bytes, and duplicate keys are rejected.
func Encode(v Value) ([]byte, error) {
	return EncodeWithLimits(v, DefaultLimits())
}

// EncodeWithLimits is Encode with explicit output and structure bounds.
func EncodeWithLimits(v Value, bound Limits) ([]byte, error) {
	bound = bound.withDefaults()
	if err := validateLimits(bound); err != nil {
		return nil, err
	}
	var out []byte
	e := encoder{bound: bound}
	if err := e.value(&out, v, 0); err != nil {
		return nil, err
	}
	return out, nil
}

type encoder struct {
	bound   Limits
	values  int
	entries int
}

func (e *encoder) value(out *[]byte, v Value, depth int) error {
	if depth > e.bound.MaxDepth {
		return ErrLimit
	}
	if e.values >= e.bound.MaxValues {
		return ErrLimit
	}
	e.values++
	switch v.Type {
	case Integer:
		*out = append(*out, 'i')
		*out = strconv.AppendInt(*out, v.Int, 10)
		*out = append(*out, 'e')
	case Bytes:
		if len(v.Bytes) > e.bound.MaxBytes || len(*out) > e.bound.MaxBytes-len(v.Bytes) {
			return ErrLimit
		}
		*out = strconv.AppendInt(*out, int64(len(v.Bytes)), 10)
		*out = append(*out, ':')
		*out = append(*out, v.Bytes...)
	case List:
		if len(v.List) > e.bound.MaxContainerEntries {
			return ErrLimit
		}
		*out = append(*out, 'l')
		for _, item := range v.List {
			if err := e.value(out, item, depth+1); err != nil {
				return err
			}
		}
		*out = append(*out, 'e')
	case Dictionary:
		if len(v.Dict) > e.bound.MaxContainerEntries {
			return ErrLimit
		}
		entries := append([]Entry(nil), v.Dict...)
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].Key, entries[j].Key) < 0
		})
		*out = append(*out, 'd')
		var previous []byte
		havePrevious := false
		for _, item := range entries {
			if havePrevious && bytes.Equal(previous, item.Key) {
				return ErrMalformed
			}
			previous = item.Key
			havePrevious = true
			e.entries++
			if e.entries > e.bound.MaxDictionaryEntries {
				return ErrLimit
			}
			if len(item.Key) > e.bound.MaxBytes {
				return ErrLimit
			}
			*out = strconv.AppendInt(*out, int64(len(item.Key)), 10)
			*out = append(*out, ':')
			*out = append(*out, item.Key...)
			if err := e.value(out, item.Value, depth+1); err != nil {
				return err
			}
		}
		*out = append(*out, 'e')
	default:
		return fmt.Errorf("%w: invalid type", ErrMalformed)
	}
	if len(*out) > e.bound.MaxBytes {
		return ErrLimit
	}
	return nil
}
