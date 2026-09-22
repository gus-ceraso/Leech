package tracker

import (
	"context"
	"testing"
)

func TestGenerateIdentityAndSnapshotAccounting(t *testing.T) {
	raw := make([]byte, 26)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	identity, err := GenerateIdentity(bytesReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if identity.PeerID[0] != 1 || identity.PeerID[19] != 20 || identity.Key != 0x15161718 {
		t.Fatalf("identity = %+v", identity)
	}
	if identity.Port != 0x191a%16384+49152 {
		t.Fatalf("port = %d", identity.Port)
	}

	metadata, err := (Snapshot{Downloaded: 9, Metadata: true}).Announce(identity, [20]byte{1}, EventStarted, -1)
	if err != nil || metadata.Left != 1 || metadata.Downloaded != 9 || metadata.Uploaded != 0 {
		t.Fatalf("metadata announce = %+v, err=%v", metadata, err)
	}
	transfer, err := (Snapshot{Downloaded: 12, Retained: 4, Total: 10}).Announce(identity, [20]byte{1}, EventNone, -1)
	if err != nil || transfer.Left != 6 {
		t.Fatalf("transfer announce = %+v, err=%v", transfer, err)
	}
	if _, err := (Snapshot{Retained: 11, Total: 10}).Announce(identity, [20]byte{1}, EventNone, -1); err == nil {
		t.Fatal("expected invalid accounting snapshot")
	}

	accounting, err := NewAccounting(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := accounting.AddReceived(8); err != nil {
		t.Fatal(err)
	}
	if err := accounting.AddReceived(3); err != nil {
		t.Fatal(err)
	}
	if err := accounting.AddRetained(4); err != nil {
		t.Fatal(err)
	}
	got, err := accounting.Snapshot(context.Background(), false)
	if err != nil || got.Downloaded != 11 || got.Retained != 4 || got.Total != 10 {
		t.Fatalf("accounting snapshot = %+v, err=%v", got, err)
	}
}
