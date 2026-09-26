package peer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestMetadataDataUsesExactBEP9BlockGeometry(t *testing.T) {
	first := bytes.Repeat([]byte{0xA1}, 16<<10)
	last := []byte{0xB2}
	message, err := ParseMetadataData(metadataBody(t, MetadataData, 0, 16<<10|1, first), 0)
	if err != nil {
		t.Fatal(err)
	}
	if message.Type != MetadataData || message.Piece != 0 || message.TotalSize != 16<<10|1 || !bytes.Equal(message.Block, first) {
		t.Fatalf("first block = %#v", message)
	}
	message, err = ParseMetadataData(metadataBody(t, MetadataData, 1, 16<<10|1, last), 16<<10|1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Block, last) {
		t.Fatalf("last block = %x", message.Block)
	}

	for _, bad := range [][]byte{
		metadataBody(t, MetadataData, 0, 16<<10|1, first[:len(first)-1]),
		metadataBody(t, MetadataData, 1, 16<<10|1, append(last, 0)),
		metadataBody(t, MetadataData, 2, 16<<10|1, nil),
	} {
		if _, err := ParseMetadataData(bad, 0); err == nil || !IsProtocolViolation(err) {
			t.Errorf("bad metadata block succeeded: %v", err)
		}
	}
}

func TestMetadataAssemblerRequiresStableSizeAndAllBlocks(t *testing.T) {
	a, err := NewMetadataAssemblerWithSize(16<<10 | 1)
	if err != nil {
		t.Fatal(err)
	}
	first := bytes.Repeat([]byte{1}, 16<<10)
	last := []byte{2}
	if err := a.Add(metadataBody(t, MetadataData, 0, 16<<10|1, first)); err != nil {
		t.Fatal(err)
	}
	if a.Complete() {
		t.Fatal("assembler completed before final block")
	}
	if err := a.Add(metadataBody(t, MetadataData, 1, 16<<10|1, last)); err != nil {
		t.Fatal(err)
	}
	if !a.Complete() {
		t.Fatal("assembler did not complete")
	}
	got, err := a.Metadata()
	if err != nil || len(got) != 16<<10|1 || !bytes.Equal(got[:1], first[:1]) || got[len(got)-1] != 2 {
		t.Fatalf("assembled metadata len=%d err=%v", len(got), err)
	}
	if err := a.Add(metadataBody(t, MetadataData, 1, 16<<10|2, []byte{2})); err == nil || !IsProtocolViolation(err) {
		t.Fatalf("changed total_size error = %v", err)
	}
	if err := a.Add(metadataBody(t, MetadataData, 1, 16<<10|1, []byte{3})); err == nil || !IsProtocolViolation(err) {
		t.Fatalf("conflicting duplicate error = %v", err)
	}
}

func TestMetadataControlEncodingAndNoUploadBoundary(t *testing.T) {
	for _, typ := range []MetadataMessageType{MetadataRequest, MetadataReject} {
		var wire []byte
		var err error
		if typ == MetadataRequest {
			wire, err = EncodeMetadataRequest(13, 7)
		} else {
			wire, err = EncodeMetadataReject(13, 7)
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) < 7 || wire[4] != ExtendedID || wire[5] != 13 {
			t.Fatalf("metadata control prefix = %v", wire)
		}
		message, err := ParseMetadataControl(wire[6:])
		if err != nil || message.Type != typ || message.Piece != 7 {
			t.Fatalf("control = %#v, %v", message, err)
		}
	}
	if _, err := EncodeMetadataRequest(0, 0); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("zero extension ID error = %v", err)
	}
}

func TestUnknownMetadataMessageCanBeIgnored(t *testing.T) {
	for _, typ := range []string{"-9223372036854775808", "-1", "3", "99", "255", "256", "257", "258", "9223372036854775807"} {
		t.Run(typ, func(t *testing.T) {
			message, err := ParseMetadataMessage([]byte("d8:msg_typei" + typ + "ee"))
			if err != nil || message.Type != MetadataUnknown || len(message.Block) != 0 {
				t.Fatalf("unknown metadata message = %#v, %v", message, err)
			}
		})
	}
}

func TestMalformedKnownMetadataMessagesRemainViolations(t *testing.T) {
	for _, body := range []string{
		"d8:msg_typei0ee", "d8:msg_typei1ee", "d8:msg_typei2ee",
		"d8:msg_typei1e5:piecei0e10:total_sizei1ee",
		"d8:msg_typei2e5:piecei-1ee", "d8:msg_type3:256e",
		"d8:msg_typei0256ee", "d8:msg_typei9223372036854775808ee",
	} {
		if _, err := ParseMetadataMessage([]byte(body)); !IsProtocolViolation(err) {
			t.Fatalf("%q: %v, want protocol violation", body, err)
		}
	}
}

func TestOverLimitMetadataSizeIsUnsupported(t *testing.T) {
	body := metadataBody(t, MetadataData, 0, int64(64<<20)+1, nil)
	if _, err := ParseMetadataData(body, 0); err == nil || !errors.Is(err, ErrUnsupported) || IsProtocolViolation(err) {
		t.Fatalf("over-limit total_size = %v, want peer-local unsupported", err)
	}
}

func FuzzMetadataMessages(f *testing.F) {
	f.Add([]byte("d8:msg_typei1e5:piecei0e10:total_sizei1ee"), []byte{0})
	f.Add([]byte("d8:msg_typei0e5:piecei0ee"), []byte{})
	for _, typ := range []string{"-9223372036854775808", "-1", "256", "257", "258", "9223372036854775807"} {
		f.Add([]byte("d8:msg_typei"+typ+"ee"), []byte{})
	}
	f.Fuzz(func(t *testing.T, header, block []byte) {
		body := append(append([]byte(nil), header...), block...)
		message, err := ParseMetadataMessage(body)
		if err == nil {
			switch message.Type {
			case MetadataUnknown, MetadataRequest, MetadataReject:
				if len(message.Block) != 0 {
					t.Fatal("control or unknown type retained a block")
				}
			case MetadataData:
				if len(message.Block) == 0 || len(message.Block) > limits.BlockBytes {
					t.Fatal("unbounded data block")
				}
			default:
				t.Fatalf("unclassified type %d", message.Type)
			}
		}
		assembler := NewMetadataAssembler()
		_ = assembler.Add(body)
	})
}

func metadataBody(t *testing.T, typ MetadataMessageType, piece uint32, totalSize int64, block []byte) []byte {
	t.Helper()
	entries := []bencode.Entry{
		{Key: []byte("msg_type"), Value: bencode.Value{Type: bencode.Integer, Int: int64(typ)}},
		{Key: []byte("piece"), Value: bencode.Value{Type: bencode.Integer, Int: int64(piece)}},
	}
	if typ == MetadataData {
		entries = append(entries, bencode.Entry{Key: []byte("total_size"), Value: bencode.Value{Type: bencode.Integer, Int: totalSize}})
	}
	header, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: entries})
	if err != nil {
		t.Fatal(err)
	}
	return append(header, block...)
}
