package peer

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestExtensionIDsAreDirectionalAndRepeatedHandshakesAreAdditive(t *testing.T) {
	state := NewExtensionStateWithLocalID(7)
	if got, ok := state.LocalExtensionID(UtMetadataExtension); !ok || got != 7 {
		t.Fatalf("local ID = %d, %v", got, ok)
	}
	if _, ok := state.RemoteExtensionID(UtMetadataExtension); ok {
		t.Fatal("remote extension unexpectedly enabled before handshake")
	}
	if _, err := state.EncodeMetadataRequest(0); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("request without remote mapping = %v", err)
	}

	body := extensionHandshakeBody(t, map[string]int64{"ut_metadata": 9, "ut_pex": 3}, 32769)
	if err := state.ApplyHandshake(body); err != nil {
		t.Fatal(err)
	}
	if got, ok := state.RemoteExtensionID(UtMetadataExtension); !ok || got != 9 {
		t.Fatalf("remote ID = %d, %v", got, ok)
	}
	if got, ok := state.RemoteMetadataSize(); !ok || got != 32769 {
		t.Fatalf("remote size = %d, %v", got, ok)
	}
	if wire, err := EncodeMetadataRequest(9, 4); err != nil {
		t.Fatal(err)
	} else if wire[4] != ExtendedID || wire[5] != 9 {
		t.Fatalf("request used the wrong directional ID: %v", wire[:6])
	}
	if wire, err := state.EncodeMetadataRequest(4); err != nil {
		t.Fatal(err)
	} else if wire[5] != 9 {
		t.Fatalf("state request used the wrong directional ID: %v", wire[:6])
	}

	// An omitted entry remains enabled; a zero entry disables only that name.
	if err := state.ApplyHandshake(extensionHandshakeBody(t, map[string]int64{"ut_metadata": 0}, 0)); err != nil {
		t.Fatal(err)
	}
	if _, ok := state.RemoteExtensionID(UtMetadataExtension); ok {
		t.Fatal("remote extension remained enabled after additive disable")
	}
	if got, ok := state.RemoteMetadataSize(); !ok || got != 32769 {
		t.Fatalf("metadata size was changed by omitted field: %d, %v", got, ok)
	}

	// The local ID is used for dispatch, independently of the remote ID.
	request := metadataBody(t, MetadataRequest, 2, 0, nil)
	if err := state.ApplyHandshake(extensionHandshakeBody(t, map[string]int64{"ut_metadata": 9}, 0)); err != nil {
		t.Fatal(err)
	}
	event, err := state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{7}, request...)})
	if err != nil {
		t.Fatal(err)
	}
	if event.Ignored || event.Metadata == nil || event.Metadata.Piece != 2 {
		t.Fatalf("local dispatch = %#v", event)
	}
	if len(event.Response) == 0 || event.Response[4] != ExtendedID || event.Response[5] != 9 {
		t.Fatalf("reject did not use remote ID: %v", event.Response)
	}
	ignored, err := state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{9}, request...)})
	if err != nil {
		t.Fatal(err)
	}
	if !ignored.Ignored || ignored.Response != nil {
		t.Fatalf("remote ID was incorrectly used for receive dispatch: %#v", ignored)
	}
}

func TestExtensionRequestRejectDependsOnCurrentRemoteMapping(t *testing.T) {
	state := NewExtensionState()
	request := metadataBody(t, MetadataRequest, 0, 0, nil)

	if err := state.ApplyHandshake(extensionHandshakeBody(t, map[string]int64{"ut_metadata": 11}, 0)); err != nil {
		t.Fatal(err)
	}
	event, err := state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{1}, request...)})
	if err != nil || len(event.Response) == 0 {
		t.Fatalf("first metadata request = %#v, %v", event, err)
	}
	if _, err := ParseMetadataControl(event.Response[6:]); err != nil {
		t.Fatalf("reject body = %v", err)
	}

	if err := state.ApplyHandshake(extensionHandshakeBody(t, map[string]int64{"ut_metadata": 0}, 0)); err != nil {
		t.Fatal(err)
	}
	event, err = state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{1}, request...)})
	if err != nil {
		t.Fatal(err)
	}
	if event.Response != nil {
		t.Fatalf("request after remote disable received a reject: %v", event.Response)
	}
}

func TestExtensionHandshakeEncodingAndAllowlist(t *testing.T) {
	state := NewExtensionStateWithLocalID(6)
	wire, err := state.EncodeHandshake()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) < 7 || wire[4] != ExtendedID || wire[5] != ExtensionHandshakeID {
		t.Fatalf("handshake prefix = %v", wire)
	}
	if !bytes.Equal(wire[6:], []byte("d1:md11:ut_metadatai6eee")) {
		t.Fatalf("handshake body = %q", wire[6:])
	}
	if _, err := EncodeOutbound(Message{ID: ExtendedID, Payload: []byte{6, 'd'}}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("generic extended encoder error = %v", err)
	}
}

func TestParseExtensionHandshakeRejectsMalformedAndBoundsUnknown(t *testing.T) {
	for _, body := range [][]byte{
		[]byte("i1e"),
		[]byte("d1:mi1ee"),
		[]byte("d1:md11:ut_metadatai256eee"),
		[]byte("d1:md11:ut_metadatai1ee"), // missing final dictionary terminator
	} {
		if _, err := ParseExtensionHandshake(body); err == nil || !IsProtocolViolation(err) {
			t.Errorf("ParseExtensionHandshake(%q) = %v, want protocol violation", body, err)
		}
	}
	unknown := extensionHandshakeBody(t, map[string]int64{"ut_pex": 7}, 0)
	parsed, err := ParseExtensionHandshake(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Extensions) != 0 {
		t.Fatalf("unknown extension was retained: %#v", parsed.Extensions)
	}
	tooLarge := extensionHandshakeBody(t, map[string]int64{}, int64(64<<20)+1)
	if _, err := ParseExtensionHandshake(tooLarge); err == nil || !errors.Is(err, ErrUnsupported) || IsProtocolViolation(err) {
		t.Fatalf("over-limit metadata_size = %v, want peer-local unsupported", err)
	}
}

func TestExtensionReqQPresenceAndRepeatedUpdates(t *testing.T) {
	state := NewExtensionState()
	if got, present := state.RemoteReqQ(); got != 0 || present {
		t.Fatalf("initial reqq = %d, %t", got, present)
	}
	without := extensionHandshakeBody(t, nil, 0)
	parsed, err := ParseExtensionHandshake(without)
	if err != nil || parsed.HasReqQ {
		t.Fatalf("absent reqq = %#v, %v", parsed, err)
	}
	if err := state.ApplyHandshake(without); err != nil {
		t.Fatal(err)
	}
	if _, present := state.RemoteReqQ(); present {
		t.Fatal("omitted reqq became present")
	}

	for _, test := range []struct {
		name string
		wire int64
		want uint32
	}{
		{"zero", 0, 0},
		{"one", 1, 1},
		{"local cap", limits.PeerRequests, limits.PeerRequests},
		{"above uint32", int64(^uint32(0)) + 1, limits.PeerRequests},
		{"maximum int64", math.MaxInt64, limits.PeerRequests},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := extensionHandshakeReqQBody(t, bencode.Value{Type: bencode.Integer, Int: test.wire})
			parsed, err := ParseExtensionHandshake(body)
			if err != nil || !parsed.HasReqQ || parsed.ReqQ != test.want {
				t.Fatalf("parsed reqq = %#v, %v; want %d", parsed, err, test.want)
			}
			if err := state.ApplyHandshake(body); err != nil {
				t.Fatal(err)
			}
			if got, present := state.RemoteReqQ(); !present || got != test.want {
				t.Fatalf("remote reqq = %d, %t; want %d", got, present, test.want)
			}
			if err := state.ApplyHandshake(without); err != nil {
				t.Fatal(err)
			}
			if got, present := state.RemoteReqQ(); !present || got != test.want {
				t.Fatalf("omitted update changed reqq to %d, %t", got, present)
			}
		})
	}
}

func TestExtensionReqQRejectsInvalidValues(t *testing.T) {
	for _, body := range [][]byte{
		extensionHandshakeReqQBody(t, bencode.Value{Type: bencode.Integer, Int: -1}),
		extensionHandshakeReqQBody(t, bencode.Value{Type: bencode.Bytes, Bytes: []byte("1")}),
		[]byte("d4:reqqi9223372036854775808ee"),
	} {
		if _, err := ParseExtensionHandshake(body); err == nil || !IsProtocolViolation(err) {
			t.Errorf("invalid reqq %q = %v, want protocol violation", body, err)
		}
	}
}

func FuzzExtensionTransitions(f *testing.F) {
	for _, typ := range []string{"-1", "256", "257", "258", "9223372036854775807"} {
		f.Add(byte(1), []byte("d1:md11:ut_metadatai9eee"), []byte("d8:msg_typei"+typ+"ee"))
	}
	f.Add(byte(1), []byte("d1:md11:ut_metadatai9eee"), []byte("d8:msg_typei0e5:piecei0ee"))
	f.Add(byte(255), []byte("d1:md11:ut_metadatai0eee"), []byte("garbage"))
	f.Add(byte(7), []byte("d4:reqqi0ee"), []byte("d8:msg_typei0e5:piecei0ee"))
	f.Add(byte(7), []byte("d4:reqqi9223372036854775807ee"), []byte("garbage"))
	f.Fuzz(func(t *testing.T, extensionID byte, handshake, body []byte) {
		state := NewExtensionStateWithLocalID(extensionID)
		parsed, parseErr := ParseExtensionHandshake(handshake)
		if err := state.ApplyHandshake(handshake); (err == nil) != (parseErr == nil) {
			t.Fatalf("parse/apply mismatch: %v, %v", parseErr, err)
		}
		if parseErr == nil {
			if parsed.HasReqQ && parsed.ReqQ > limits.PeerRequests {
				t.Fatalf("reqq %d exceeds local cap", parsed.ReqQ)
			}
			if got, present := state.RemoteReqQ(); got != parsed.ReqQ || present != parsed.HasReqQ {
				t.Fatalf("remote reqq = %d, %t; parsed %d, %t", got, present, parsed.ReqQ, parsed.HasReqQ)
			}
		}
		event, err := state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{extensionID}, body...)})
		if err == nil && event.Metadata != nil && event.Metadata.Type == MetadataUnknown && len(event.Response) != 0 {
			t.Fatal("unknown metadata type produced a response")
		}
	})
}

func extensionHandshakeBody(t *testing.T, ids map[string]int64, metadataSize int64) []byte {
	t.Helper()
	entries := make([]bencode.Entry, 0, len(ids))
	for name, id := range ids {
		entries = append(entries, bencode.Entry{Key: []byte(name), Value: bencode.Value{Type: bencode.Integer, Int: id}})
	}
	dict := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{
		Key: []byte("m"), Value: bencode.Value{Type: bencode.Dictionary, Dict: entries},
	}}}
	if metadataSize != 0 {
		dict.Dict = append(dict.Dict, bencode.Entry{Key: []byte("metadata_size"), Value: bencode.Value{Type: bencode.Integer, Int: metadataSize}})
	}
	body, err := bencode.Encode(dict)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func extensionHandshakeReqQBody(t *testing.T, reqQ bencode.Value) []byte {
	t.Helper()
	body, err := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("reqq"), Value: reqQ},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
