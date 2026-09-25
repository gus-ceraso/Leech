package peer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func FuzzPeerRegistryPeerIDTransitions(f *testing.F) {
	f.Add([]byte{1, 2, 1, 3, 2, 1, 4})
	f.Add([]byte{0, 0, 0, 4, 8, 12})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 128 {
			operations = operations[:128]
		}
		registry, err := NewPeerRegistry(4)
		if err != nil {
			t.Fatal(err)
		}
		retained := make(map[[20]byte]*LivePeer)
		var extraEnds []net.Conn
		defer func() {
			for _, live := range retained {
				_ = live.Conn.Close()
			}
			for _, conn := range extraEnds {
				_ = conn.Close()
			}
		}()

		for index, operation := range operations {
			var id [20]byte
			id[0] = operation & 3
			id[1] = (operation >> 2) & 3
			if operation&0x80 != 0 {
				old := retained[id]
				client, server := net.Pipe()
				accepted := registry.Release(id, client)
				_ = client.Close()
				_ = server.Close()
				if old == nil && accepted {
					t.Fatalf("operation %d released unknown peer ID %x", index, id[:2])
				}
				if old != nil {
					if accepted {
						t.Fatalf("operation %d released a peer through the wrong connection", index)
					}
					if actual, ok := registry.Lookup(id); !ok || actual != old {
						t.Fatalf("operation %d stale release removed the retained connection", index)
					}
					if registry.Release(id, old.Conn) != true {
						t.Fatalf("operation %d exact release failed", index)
					}
					delete(retained, id)
					_ = old.Conn.Close()
				}
			} else {
				client, server := net.Pipe()
				endpoint := Endpoint{Addr: netip.AddrFrom4([4]byte{127, 0, 0, byte(index%250 + 1)}), Port: uint16(index%65534 + 1)}
				result := HandshakeResult{Endpoint: endpoint, Transport: TransportTCP, Conn: client, Handshake: Handshake{PeerID: id}}
				live, admitErr := registry.Admit(result)
				if old := retained[id]; old != nil {
					_ = server.Close()
					if live != nil || !errors.Is(admitErr, ErrPeerIDCollision) {
						t.Fatalf("operation %d duplicate ID result = %v, %v", index, live, admitErr)
					}
					if current, ok := registry.Lookup(id); !ok || current != old {
						t.Fatalf("operation %d collision replaced older connection", index)
					}
				} else if admitErr == nil {
					if live == nil || live.Conn != client || live.ID != id {
						t.Fatalf("operation %d admission = %#v", index, live)
					}
					retained[id] = live
					extraEnds = append(extraEnds, server)
				} else if !errors.Is(admitErr, ErrLivePeerLimit) {
					t.Fatalf("operation %d unexpected admission error: %v", index, admitErr)
				} else {
					_ = server.Close()
				}
			}
			if registry.Len() != len(retained) {
				t.Fatalf("operation %d registry length=%d retained=%d", index, registry.Len(), len(retained))
			}
			for id, live := range retained {
				if current, ok := registry.Lookup(id); !ok || current != live {
					t.Fatalf("operation %d lost retained ID %x", index, id[:2])
				}
			}
		}
	})
}

func FuzzRaceEndpointHandshakeAndCancellation(f *testing.F) {
	f.Add(byte(0))
	f.Add(byte(1))
	f.Add(byte(2))
	f.Fuzz(func(t *testing.T, mode byte) {
		local := Handshake{InfoHash: [20]byte{1, 2, 3}, PeerID: [20]byte{4, 5, 6}}
		endpoint := Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 51413}
		clock := &fakeRaceClock{created: make(chan struct{})}
		var tcpCalls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct {
			result HandshakeResult
			err    error
		}, 1)
		go func() {
			result, err := RaceEndpoint(ctx, endpoint, RaceConfig{
				LocalHandshake: local,
				UTPDial: func(context.Context, string, string) (net.Conn, error) {
					return nil, errors.New("scripted uTP setup failure")
				},
				TCPDial: func(context.Context, string, string) (net.Conn, error) {
					tcpCalls.Add(1)
					client, server := net.Pipe()
					go func() {
						defer server.Close()
						if _, err := ReadHandshake(server, nil, nil); err != nil {
							return
						}
						remote := Handshake{InfoHash: local.InfoHash, PeerID: [20]byte{9}}
						if mode&1 != 0 {
							remote.InfoHash[0] ^= 0x80
						}
						_ = WriteHandshake(server, remote.InfoHash, remote.PeerID, remote.Reserved)
					}()
					return client, nil
				},
				Clock: clock, UTPHeadStart: time.Second,
			})
			done <- struct {
				result HandshakeResult
				err    error
			}{result, err}
		}()
		select {
		case <-clock.created:
		case <-time.After(time.Second):
			t.Fatal("race did not create the controlled head-start timer")
		}
		if mode&2 != 0 {
			cancel()
		} else {
			clock.Fire()
		}
		select {
		case outcome := <-done:
			if mode&2 != 0 {
				if !errors.Is(outcome.err, context.Canceled) || outcome.result.Conn != nil || tcpCalls.Load() != 0 {
					t.Fatalf("canceled race = %v, conn=%v tcpCalls=%d", outcome.err, outcome.result.Conn, tcpCalls.Load())
				}
			} else if mode&1 != 0 {
				if outcome.err == nil || outcome.result.Conn != nil || !IsProtocolViolation(outcome.err) {
					t.Fatalf("invalid-handshake race = %v, conn=%v", outcome.err, outcome.result.Conn)
				}
			} else {
				if outcome.err != nil || outcome.result.Conn == nil || outcome.result.Transport != TransportTCP {
					t.Fatalf("valid-handshake race = %+v, %v", outcome.result, outcome.err)
				}
				_ = outcome.result.Conn.Close()
			}
		case <-time.After(time.Second):
			t.Fatal("race did not join after scripted outcome")
		}
	})
}
