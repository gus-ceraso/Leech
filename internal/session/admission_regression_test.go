package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

func admissionFixture(t *testing.T) (*coordinator, torrent.Metainfo, *torrent.SelectionPlan, *storage.Plan, *h2Tracker) {
	t.Helper()
	root := t.TempDir()
	_, path := h2Torrent(t, root, []byte("piece"))
	meta, err := torrent.LoadMetainfo(path)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := torrent.Select(meta, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "out")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	plan, err := storage.Validate(output, meta, selection.SelectedIndices())
	if err != nil {
		t.Fatal(err)
	}
	fixture := &h2Tracker{}
	c := &coordinator{
		config: RunConfig{HTTP: fixture, CacheRoot: filepath.Join(root, "cache"), UTPHeadStart: time.Microsecond,
			UTPDial: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("fixture has no uTP transport")
			}},
		identity:       tracker.Identity{PeerID: [20]byte{99}, Port: 49152},
		verifiedOutput: new(atomic.Bool), backoff: peer.NewEndpointBackoff(),
		updateQueue: newTrackerPeerUpdateQueue(), ownSet: true,
	}
	t.Cleanup(func() {
		if err := c.closeSet(); err != nil {
			t.Error(err)
		}
	})
	return c, meta, selection, plan, fixture
}

func assertAdmissionCacheRemoved(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cache entries = %v, %v; want an empty cache root", entries, err)
	}
}

func TestAdmissionAdvancesExaminedCandidates(t *testing.T) {
	for _, mode := range []string{"idle", "dial-failure", "live-ID-collision", "backoff"} {
		t.Run(mode, func(t *testing.T) {
			c, meta, selection, plan, trackerFixture := admissionFixture(t)
			good := endpoint(102)
			first := endpoint(101)
			order := []peer.Endpoint{first, good}
			if mode == "live-ID-collision" {
				order = []peer.Endpoint{first, endpoint(103), good}
			}
			// Candidate snapshots are newest first; seed the complete pool before
			// acquisition so tracker timing cannot determine the dial order.
			for i := len(order) - 1; i >= 0; i-- {
				c.metadataEndpoints = append(c.metadataEndpoints, peer.ResolvedCandidate{Endpoint: order[i]})
			}
			if mode == "backoff" {
				c.backoff.RecordFailure(first, time.Now().Add(time.Hour))
			}
			var mu sync.Mutex
			var attempts []string
			var workers sync.WaitGroup
			c.config.TCPDial = func(_ context.Context, _, address string) (net.Conn, error) {
				mu.Lock()
				attempts = append(attempts, address)
				mu.Unlock()
				if mode == "dial-failure" && address == first.String() {
					return nil, errors.New("fixture dial failure")
				}
				client, server := net.Pipe()
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer server.Close()
					if _, err := peer.ReadHandshake(server, (*[20]byte)(&meta.InfoHash), nil); err != nil {
						return
					}
					id := [20]byte{1}
					if address == good.String() {
						id[0] = 2
					}
					if err := peer.WriteHandshake(server, [20]byte(meta.InfoHash), id, [8]byte{}); err != nil {
						return
					}
					if address == good.String() {
						if err := writeFixtureFrame(server, peer.BitfieldID, []byte{0x80}); err != nil {
							return
						}
						if err := writeFixtureFrame(server, peer.UnchokeID, nil); err != nil {
							return
						}
						block, err := h2Request(server)
						if err != nil {
							return
						}
						if err := h2Reply(server, []byte("piece"), block); err != nil {
							return
						}
					}
					_, _ = io.Copy(io.Discard, server)
				}()
				return client, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := c.startTransferPhase(ctx, torrent.Source{}, meta, selection, plan, storage.ResumeResult{})
			workers.Wait()
			if err != nil {
				t.Fatalf("transfer did not reach the useful candidate: %v", err)
			}
			mu.Lock()
			got := append([]string(nil), attempts...)
			mu.Unlock()
			if mode == "backoff" {
				order = order[1:]
			}
			want := make([]string, len(order))
			for i, e := range order {
				want[i] = e.String()
			}
			if len(got) < len(want) || !reflect.DeepEqual(got[:len(want)], want) {
				t.Fatalf("attempt order = %v, want prefix %v", got, want)
			}
			if data, err := os.ReadFile(filepath.Join(plan.Root(), "payload")); err != nil || string(data) != "piece" {
				t.Fatalf("verified output = %q, %v", data, err)
			}
			assertAdmissionCacheRemoved(t, c.config.CacheRoot)
			assertH2Final(t, trackerFixture.snapshot())
		})
	}
}

func TestAdmissionQueueOverflowStopsTransfer(t *testing.T) {
	for _, mode := range []string{"callback", "direct"} {
		t.Run(mode, func(t *testing.T) {
			c, meta, selection, plan, fixture := admissionFixture(t)
			// A zero-capacity private status queue makes overflow deterministic:
			// the real enqueue callback/branch must handle its first update.
			c.updateQueue.events = make(chan tracker.Update)
			if mode == "direct" {
				updates := make(chan tracker.Update, 16)
				set, err := tracker.NewTrackerSet(tracker.TrackerSetConfig{
					Identity: c.identity, InfoHash: [20]byte(meta.InfoHash),
					Trackers: []string{torrent.DefaultTracker}, HTTP: fixture,
					Snapshot: func(context.Context) (tracker.Snapshot, error) {
						return tracker.Snapshot{Total: 5}, nil
					},
					OnUpdate: func(update tracker.Update) { updates <- update },
				})
				if err != nil {
					t.Fatal(err)
				}
				c.set, c.updates = set, updates
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := c.startTransferPhase(ctx, torrent.Source{}, meta, selection, plan, storage.ResumeResult{})
			if !errors.Is(err, ErrTrackerUpdateQueue) {
				t.Fatalf("transfer error = %v, want queue overflow", err)
			}
			if !c.overflow.Load() {
				t.Fatal("queue overflow was not latched")
			}
			assertAdmissionCacheRemoved(t, c.config.CacheRoot)
			for url, requests := range fixture.snapshot() {
				if len(requests) != 2 || requests[0].Event != tracker.EventStarted || requests[1].Event != tracker.EventStopped {
					t.Errorf("tracker %s events = %v, want started then stopped", url, requests)
				}
			}
		})
	}
}

type admissionTemporaryError struct{}

func (admissionTemporaryError) Error() string   { return "temporary fixture failure" }
func (admissionTemporaryError) Temporary() bool { return true }

func TestAdmissionRetriesOnlyExplicitTemporaryErrors(t *testing.T) {
	for _, retryErr := range []error{nil, ErrNoPeer, fmt.Errorf("wrapped: %w", admissionTemporaryError{})} {
		t.Run(fmt.Sprint(retryErr), func(t *testing.T) {
			c, _, selection, plan, _ := admissionFixture(t)
			fatal := errors.New("permanent acquisition failure")
			calls := 0
			shutdown := false
			client, server := net.Pipe()
			t.Cleanup(func() { client.Close(); server.Close() })
			ready, joined := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(joined)
				defer server.Close()
				_, _ = server.Write([]byte{0, 0, 0, 0})
				close(ready)
				_, _ = io.Copy(io.Discard, server)
			}()
			transfer, err := NewTransfer(TransferConfig{
				Selection: selection, Output: plan,
				PieceCount: 1, PieceLength: 5, LastPieceLength: 5,
				Stager: storage.NewStager(storage.StagerConfig{CacheRoot: c.config.CacheRoot}),
				Peers:  []ConnectedPeer{{ID: "idle", Endpoint: endpoint(104), Conn: client}},
				AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
					select {
					case <-ready:
					case <-ctx.Done():
						return ConnectedPeer{}, ctx.Err()
					}
					calls++
					if calls == 1 && retryErr != nil {
						return ConnectedPeer{}, retryErr
					}
					return ConnectedPeer{}, fatal
				},
				BeforePeerShutdown: func() error {
					shutdown = true
					return errors.New("secondary shutdown failure")
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := transfer.Run(ctx); !errors.Is(err, fatal) {
				t.Fatalf("transfer error = %v, want original permanent failure", err)
			}
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatal("peer fixture did not join after fatal acquisition")
			}
			wantCalls := 1
			if retryErr != nil {
				wantCalls++
			}
			if calls != wantCalls || !shutdown {
				t.Fatalf("calls = %d, shutdown = %v", calls, shutdown)
			}
			assertAdmissionCacheRemoved(t, c.config.CacheRoot)
		})
	}
}

func TestAdmissionTemporaryFailuresRecoverWithUsefulPeer(t *testing.T) {
	c, _, selection, plan, _ := admissionFixture(t)
	conn, joined := startFixturePeer(t, [20]byte{42}, []fixturePiece{{index: 0, data: []byte("piece")}}, false, false)
	t.Cleanup(func() { conn.Close() })
	calls := 0
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: plan,
		PieceCount: 1, PieceLength: 5, LastPieceLength: 5,
		Stager: storage.NewStager(storage.StagerConfig{CacheRoot: c.config.CacheRoot}),
		AcquirePeer: func(ctx context.Context) (ConnectedPeer, error) {
			calls++
			switch calls {
			case 1:
				return ConnectedPeer{}, ErrNoPeer
			case 2:
				return ConnectedPeer{}, admissionTemporaryError{}
			case 3:
				return ConnectedPeer{ID: "useful", Endpoint: endpoint(105), Conn: conn}, nil
			default:
				<-ctx.Done()
				return ConnectedPeer{}, ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transfer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-joined:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("useful peer fixture did not join")
	}
	if calls < 3 {
		t.Fatalf("acquisition calls = %d, want recovery on third call", calls)
	}
	if data, err := os.ReadFile(filepath.Join(plan.Root(), "payload")); err != nil || string(data) != "piece" {
		t.Fatalf("verified output = %q, %v", data, err)
	}
	assertAdmissionCacheRemoved(t, c.config.CacheRoot)
}
