package peer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gus-ceraso/Leech/internal/bencode"
)

func TestExtensionIDsAreDirectionalAndRepeatedHandshakesAreAdditive(t *testing.T) {
	state := NewExtensionStateWithLocalID(7)
	if got, ok := state.LocalExtensionID(UtMetadataExtension); !ok || got != 7 {
		t.Fatalf("local ID = %d, %v", got, ok)
	}
	if _, ok := state.RemoteExtensionID(UtMetadataExtension); ok {
		t.Fatal("remote extension unexpectedly enabled before handshake")
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
}

func FuzzExtensionTransitions(f *testing.F) {
	f.Add(byte(1), []byte("d1:md11:ut_metadatai9eee"), []byte("d8:msg_typei0e5:piecei0ee"))
	f.Add(byte(255), []byte("d1:md11:ut_metadatai0eee"), []byte("garbage"))
	f.Fuzz(func(t *testing.T, extensionID byte, handshake, body []byte) {
		state := NewExtensionStateWithLocalID(extensionID)
		_ = state.ApplyHandshake(handshake)
		_, _ = state.ApplyMessage(Message{ID: ExtendedID, Payload: append([]byte{extensionID}, body...)})
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
