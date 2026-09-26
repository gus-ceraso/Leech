package session

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

// The script begins after Leech's extension handshake. Closing the client and
// joining the script keeps even a rejected or canceled exchange owned by the test.
func metadataExchange(t *testing.T, ctx context.Context, timeout time.Duration, script func(net.Conn) error) ([]byte, error) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		if _, err := peer.ReadMessage(server); err != nil {
			done <- err
			return
		}
		done <- script(server)
	}()
	data, err := fetchMetadata(ctx, client, timeout)
	client.Close()
	if scriptErr := <-done; scriptErr != nil {
		t.Fatalf("metadata supplier: %v", scriptErr)
	}
	return data, err
}

func expectMetadataRequest(conn net.Conn, id byte, piece uint32) error {
	message, err := readMetadataPeerMessage(conn, id)
	if err != nil {
		return err
	}
	want := metadataControlFrame(id, peer.MetadataRequest, piece)
	if message.ID != peer.ExtendedID || !bytes.Equal(message.Payload, want[5:]) {
		return fmt.Errorf("metadata request = %#v, want ID %d piece %d", message, id, piece)
	}
	return nil
}

func TestReviewSerialMetadataRequestsExhaustCandidateDeadline(t *testing.T) {
	info := largeTestInfo(t)
	got, err := metadataExchange(t, context.Background(), 150*time.Millisecond, func(conn net.Conn) error {
		if err := writeTestFrame(conn, extensionHandshakeFrame(7, int64(len(info)))); err != nil {
			return err
		}
		for piece, begin := uint32(0), 0; begin < len(info); piece, begin = piece+1, begin+limits.BlockBytes {
			if err := expectMetadataRequest(conn, 7, piece); err != nil {
				return err
			}
			time.Sleep(90 * time.Millisecond)
			end := min(begin+limits.BlockBytes, len(info))
			if err := writeTestFrame(conn, metadataDataFrame(1, piece, int64(len(info)), info[begin:end])); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || !bytes.Equal(got, info) {
		t.Fatalf("productive metadata = %d bytes, %v; want %d bytes", len(got), err, len(info))
	}
	if err := validateMetadataCandidate(got, torrent.InfoHash(sha1.Sum(info))); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataRequestPipelineIsBounded(t *testing.T) {
	info := bytes.Repeat([]byte{'x'}, (limits.MetadataRequests+1)*limits.BlockBytes)
	got, err := metadataExchange(t, context.Background(), time.Second, func(conn net.Conn) error {
		if err := writeTestFrame(conn, extensionHandshakeFrame(7, int64(len(info)))); err != nil {
			return err
		}
		for piece, begin := uint32(0), 0; begin < len(info); piece, begin = piece+1, begin+limits.BlockBytes {
			if err := expectMetadataRequest(conn, 7, piece); err != nil {
				return err
			}
			if piece == 0 {
				// This implementation deliberately permits only one outstanding
				// request, even when the candidate has more than 32 blocks.
				conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
				_, err := peer.ReadMessage(conn)
				conn.SetReadDeadline(time.Time{})
				var timedOut net.Error
				if !errors.As(err, &timedOut) || !timedOut.Timeout() {
					return fmt.Errorf("extra request before response: %v", err)
				}
			}
			if err := writeTestFrame(conn, metadataDataFrame(1, piece, int64(len(info)), info[begin:begin+limits.BlockBytes])); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || !bytes.Equal(got, info) {
		t.Fatalf("bounded metadata exchange: %v", err)
	}
}

func TestMetadataTerminalResponsesMatchOutstandingRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		beforeSize bool
		firstBlock bool
		response   []byte
		protocol   bool
	}{
		{"unsolicited reject before handshake", true, false, metadataControlFrame(1, peer.MetadataReject, 0), true},
		{"unsolicited data before handshake", true, false, metadataDataFrame(1, 0, 1, []byte{'x'}), true},
		{"wrong piece reject", false, false, metadataControlFrame(1, peer.MetadataReject, 1), true},
		{"wrong piece data", false, false, metadataDataFrame(1, 1, limits.BlockBytes+1, []byte{'x'}), true},
		{"duplicate data", false, true, metadataDataFrame(1, 0, limits.BlockBytes+1, make([]byte, limits.BlockBytes)), true},
		{"reject for completed piece", false, true, metadataControlFrame(1, peer.MetadataReject, 0), true},
		{"matching reject", false, false, metadataControlFrame(1, peer.MetadataReject, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := metadataExchange(t, context.Background(), time.Second, func(conn net.Conn) error {
				if !tc.beforeSize {
					if err := writeTestFrame(conn, extensionHandshakeFrame(7, limits.BlockBytes+1)); err != nil {
						return err
					}
					if err := expectMetadataRequest(conn, 7, 0); err != nil {
						return err
					}
				}
				if tc.firstBlock {
					if err := writeTestFrame(conn, metadataDataFrame(1, 0, limits.BlockBytes+1, make([]byte, limits.BlockBytes))); err != nil {
						return err
					}
					if err := expectMetadataRequest(conn, 7, 1); err != nil {
						return err
					}
				}
				return writeTestFrame(conn, tc.response)
			})
			if errors.Is(err, peer.ErrProtocolViolation) != tc.protocol || errors.Is(err, ErrMetadataRejected) == tc.protocol {
				t.Fatalf("terminal classification = %v; protocol violation = %t", err, tc.protocol)
			}
		})
	}
}

func TestMetadataUnproductiveSupplierIsBounded(t *testing.T) {
	for _, beforeSize := range []bool{false, true} {
		for _, behavior := range []string{"idle", "irrelevant flood", "extension updates"} {
			t.Run(fmt.Sprintf("before size %t/%s", beforeSize, behavior), func(t *testing.T) {
				_, err := metadataExchange(t, context.Background(), 80*time.Millisecond, func(conn net.Conn) error {
					if !beforeSize {
						if err := writeTestFrame(conn, extensionHandshakeFrame(7, 1)); err != nil {
							return err
						}
						if err := expectMetadataRequest(conn, 7, 0); err != nil {
							return err
						}
					}
					if behavior == "idle" {
						_, err := io.Copy(io.Discard, conn)
						return err
					}
					wire := extensionFrame(1, []byte("d8:msg_typei256ee"))
					if behavior == "extension updates" {
						wire = extensionFrame(0, []byte("d1:md11:ut_metadatai7eee"))
					}
					for i := 0; i <= metadataMessageLimit; i++ {
						if behavior == "extension updates" {
							time.Sleep(10 * time.Millisecond)
						}
						if err := writeTestFrame(conn, wire); err != nil {
							return nil
						}
					}
					return errors.New("supplier exhausted response-work bound without disconnect")
				})
				if err == nil || errors.Is(err, peer.ErrProtocolViolation) {
					t.Fatalf("unproductive supplier = %v, want ordinary failure", err)
				}
			})
		}
	}
}

func TestMetadataCancellationWithOutstandingRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := metadataExchange(t, ctx, time.Minute, func(conn net.Conn) error {
		if err := writeTestFrame(conn, extensionHandshakeFrame(7, 1)); err != nil {
			return err
		}
		if err := expectMetadataRequest(conn, 7, 0); err != nil {
			return err
		}
		cancel()
		_, err := io.Copy(io.Discard, conn)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestMetadataMappingUpdatesKeepOriginalGeometry(t *testing.T) {
	info := largeTestInfo(t)
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled %t", disabled), func(t *testing.T) {
			got, err := metadataExchange(t, context.Background(), time.Second, func(conn net.Conn) error {
				total := int64(len(info))
				if err := writeTestFrame(conn, extensionHandshakeFrame(7, total)); err != nil {
					return err
				}
				if err := expectMetadataRequest(conn, 7, 0); err != nil {
					return err
				}
				id := byte(9)
				if disabled {
					id = 0
				}
				if err := writeTestFrame(conn, extensionHandshakeFrame(id, 1)); err != nil {
					return err
				}
				if err := writeTestFrame(conn, metadataDataFrame(1, 0, total, info[:limits.BlockBytes])); err != nil {
					return err
				}
				if disabled {
					_, err := peer.ReadMessage(conn)
					if err == nil {
						return errors.New("requested metadata after disablement")
					}
					return nil
				}
				if err := expectMetadataRequest(conn, 9, 1); err != nil {
					return err
				}
				return writeTestFrame(conn, metadataDataFrame(1, 1, total, info[limits.BlockBytes:]))
			})
			if disabled {
				if !errors.Is(err, ErrMetadataRejected) || errors.Is(err, peer.ErrProtocolViolation) {
					t.Fatalf("disable = %v", err)
				}
			} else if err != nil || !bytes.Equal(got, info) {
				t.Fatalf("updated mapping changed candidate: %v", err)
			}
		})
	}
}

func TestMetadataDiscoveryIgnoresUnknownIntegerTypes(t *testing.T) {
	info := testInfo(t)
	expected := torrent.InfoHash(sha1.Sum(info))
	endpoint := peer.Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 51435}
	backoff := peer.NewEndpointBackoff()
	done := make(chan error, 1)
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			err := func() error {
				if _, err := peer.ReadHandshake(server, nil, nil); err != nil {
					return err
				}
				if err := peer.WriteHandshake(server, [20]byte(expected), [20]byte{9}, [8]byte{5: 0x10}); err != nil {
					return err
				}
				if _, err := peer.ReadMessage(server); err != nil {
					return err
				}
				// This canonical unknown integer must remain ignorable before
				// the supplier has advertised its size or received any request.
				if err := writeTestFrame(server, extensionFrame(1, []byte("d8:msg_typei256ee"))); err != nil {
					return err
				}
				if err := writeTestFrame(server, extensionHandshakeFrame(7, int64(len(info)))); err != nil {
					return err
				}
				if err := expectMetadataRequest(server, 7, 0); err != nil {
					return err
				}
				for _, typ := range []string{"-1", "257", "258", "9223372036854775807", "-9223372036854775808"} {
					if err := writeTestFrame(server, extensionFrame(1, []byte("d8:msg_typei"+typ+"ee"))); err != nil {
						return err
					}
				}
				return writeTestFrame(server, metadataDataFrame(1, 0, int64(len(info)), info))
			}()
			done <- err
		}()
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := DiscoverMetadata(ctx, MetadataConfig{
		InfoHash: expected, HTTP: &metadataFixtureTracker{ports: []uint16{endpoint.Port}}, TCPDial: dial,
		Backoff: backoff, Identity: tracker.Identity{PeerID: [20]byte{3}, Port: 49154}, PeerTimeout: time.Second,
	})
	if scriptErr := <-done; scriptErr != nil {
		t.Fatal(scriptErr)
	}
	if err != nil || result.Metainfo.InfoHash != expected {
		t.Fatalf("discovery after unknown type: %v", err)
	}
	if backoff.IsBlacklisted(endpoint) || len(result.Strikes) != 0 {
		t.Fatalf("unknown types penalized supplier: %v", result.Strikes)
	}
}

func TestMetadataTerminalEndpointClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		beforeSize bool
		piece      uint32
		blacklist  bool
	}{
		{"unsolicited reject", true, 0, true},
		{"wrong piece reject", false, 1, true},
		{"matching reject", false, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := peer.Endpoint{Addr: netip.MustParseAddr("127.0.0.1"), Port: 51436}
			local := peer.Handshake{PeerID: [20]byte{3}, Reserved: [8]byte{5: 0x10}}
			backoff := peer.NewEndpointBackoff()
			done := make(chan error, 1)
			dial := func(context.Context, string, string) (net.Conn, error) {
				client, server := net.Pipe()
				go func() {
					defer server.Close()
					done <- func() error {
						if _, err := peer.ReadHandshake(server, nil, nil); err != nil {
							return err
						}
						if err := peer.WriteHandshake(server, local.InfoHash, [20]byte{9}, local.Reserved); err != nil {
							return err
						}
						if _, err := peer.ReadMessage(server); err != nil {
							return err
						}
						if !tc.beforeSize {
							if err := writeTestFrame(server, extensionHandshakeFrame(7, limits.BlockBytes+1)); err != nil {
								return err
							}
							if err := expectMetadataRequest(server, 7, 0); err != nil {
								return err
							}
						}
						return writeTestFrame(server, metadataControlFrame(1, peer.MetadataReject, tc.piece))
					}()
				}()
				return client, nil
			}
			manager, err := peer.NewDialManager(peer.DialManagerConfig{
				Race: peer.RaceConfig{LocalHandshake: local, TCPDial: dial}, Backoff: backoff,
			})
			if err != nil {
				t.Fatal(err)
			}
			discovery := &MetadataDiscovery{config: MetadataConfig{PeerTimeout: time.Second}}
			strikes := make(map[peer.Endpoint]uint8)
			_, err = discovery.tryCandidate(context.Background(), manager, peer.ResolvedCandidate{Endpoint: endpoint}, strikes, backoff)
			if scriptErr := <-done; scriptErr != nil {
				t.Fatal(scriptErr)
			}
			if err == nil || backoff.IsBlacklisted(endpoint) != tc.blacklist || len(strikes) != 0 {
				t.Fatalf("classification: %v, blacklist %t, strikes %v", err, backoff.IsBlacklisted(endpoint), strikes)
			}
			if !tc.blacklist && backoff.Ready(endpoint, time.Now()) {
				t.Fatal("matching refusal did not apply ordinary backoff")
			}
		})
	}
}
