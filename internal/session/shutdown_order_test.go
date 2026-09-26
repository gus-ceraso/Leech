package session

import (
	"context"
	"crypto/sha1"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

type shutdownOrderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *shutdownOrderLog) record(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *shutdownOrderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// Cancellation is observable before the resolver returns. The test releases
// that return separately, so cancellation alone cannot satisfy the join check.
type shutdownBarrierResolver struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	done     chan struct{}
	log      *shutdownOrderLog
}

func newShutdownBarrierResolver(t *testing.T, log *shutdownOrderLog) *shutdownBarrierResolver {
	r := &shutdownBarrierResolver{
		started: make(chan struct{}), canceled: make(chan struct{}),
		release: make(chan struct{}), done: make(chan struct{}), log: log,
	}
	t.Cleanup(func() {
		select {
		case <-r.release:
		default:
			close(r.release)
		}
	})
	return r
}

func (r *shutdownBarrierResolver) LookupIPAddr(ctx context.Context, _ string) ([]net.IPAddr, error) {
	close(r.started)
	<-ctx.Done()
	close(r.canceled)
	<-r.release
	r.log.record("resolver done")
	close(r.done)
	return nil, ctx.Err()
}

type shutdownOrderTracker struct {
	log            *shutdownOrderLog
	finalStarted   chan struct{}
	metadataPeer   bool
	queue          *trackerPeerUpdateQueue
	pendingAtFinal int
	urls           []string
	finalErr       error
}

func (f *shutdownOrderTracker) Announce(_ context.Context, url string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	f.urls = append(f.urls, url)
	if request.Event == tracker.EventStopped {
		f.log.record("stopped")
		if f.queue != nil {
			f.pendingAtFinal = f.queue.pendingPeers()
		}
		close(f.finalStarted)
		return tracker.HTTPAnnounceResult{Transmitted: true}, f.finalErr
	}
	f.log.record("started")
	peers := []tracker.HTTPPeer{{Host: "held-resolver.test", Port: 51437}}
	if f.metadataPeer {
		peers = append(peers, tracker.HTTPPeer{Host: "127.0.0.1", Port: 51438})
	}
	return tracker.HTTPAnnounceResult{Transmitted: true, Interval: time.Minute, Peers: peers}, nil
}

func awaitShutdownBarrier(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("did not reach %s", name)
	}
}

func releaseShutdownResolver(t *testing.T, r *shutdownBarrierResolver, finalStarted <-chan struct{}) {
	t.Helper()
	awaitShutdownBarrier(t, r.canceled, "resolver cancellation")
	// The barrier, not a slow DNS response, holds the resolver. Give the phase
	// a scheduling turn to expose any final event sent before the join.
	select {
	case <-finalStarted:
		t.Error("final tracker event preceded resolver completion")
	case <-time.After(25 * time.Millisecond):
	}
	close(r.release)
}

func assertShutdownOrder(t *testing.T, log *shutdownOrderLog, fixture *shutdownOrderTracker, phases ...string) {
	t.Helper()
	want := append([]string{"started", "resolver done", "stopped"}, phases...)
	want = append(want, "return")
	if got := log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("shutdown order = %v, want %v", got, want)
	}
	if fixture.pendingAtFinal != 0 {
		t.Errorf("pending peers at final event = %d", fixture.pendingAtFinal)
	}
	if !reflect.DeepEqual(fixture.urls, []string{torrent.DefaultTracker, torrent.DefaultTracker}) {
		t.Errorf("tracker routes = %v, want the default tracker started/stopped locally", fixture.urls)
	}
}

func TestReviewMetadataStoppedWhileResolverStillRunning(t *testing.T) {
	for _, mode := range []string{"success", "normalization error", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			log := new(shutdownOrderLog)
			resolver := newShutdownBarrierResolver(t, log)
			fixture := &shutdownOrderTracker{log: log, finalStarted: make(chan struct{}), metadataPeer: mode != "cancellation", finalErr: errors.New("secondary stopped failure")}
			info := testInfo(t)
			if mode == "normalization error" {
				info = []byte("de")
			}
			hash := torrent.InfoHash(sha1.Sum(info))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			peerDone := make(chan struct{})
			secondary := make(chan error, 1)
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				select {
				case <-resolver.started:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				client, server := net.Pipe()
				go serveMetadataPeer(t, server, hash, info, peerDone)
				return client, nil
			}
			done := make(chan error, 1)
			go func() {
				_, err := Run(ctx, RunConfig{
					Source: torrent.Source{Kind: torrent.SourceInfoHash, InfoHash: hash}, OutputDir: t.TempDir(),
					HTTP: fixture, Resolver: resolver, TCPDial: dial,
					UTPDial:      func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("no fixture uTP") },
					UTPHeadStart: time.Microsecond,
					OnPhase: func(phase string) {
						if phase == "selection" {
							log.record("selection")
						}
					},
					OnSecondary: func(err error) { secondary <- err },
				})
				log.record("return")
				done <- err
			}()
			awaitShutdownBarrier(t, resolver.started, "metadata resolver start")
			if mode == "cancellation" {
				cancel()
			}
			releaseShutdownResolver(t, resolver, fixture.finalStarted)
			select {
			case err := <-done:
				switch mode {
				case "success":
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
				case "normalization error":
					if !errors.Is(err, torrent.ErrInvalidMetainfo) {
						t.Fatalf("normalization error = %v", err)
					}
				case "cancellation":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation = %v", err)
					}
				}
			case <-time.After(3 * time.Second):
				t.Fatal("metadata phase did not return after resolver release")
			}
			awaitShutdownBarrier(t, resolver.done, "metadata resolver join")
			if mode != "cancellation" {
				awaitShutdownBarrier(t, peerDone, "metadata supplier join")
			}
			if mode == "success" {
				assertShutdownOrder(t, log, fixture, "selection")
			} else {
				assertShutdownOrder(t, log, fixture)
			}
			select {
			case err := <-secondary:
				if !errors.Is(err, fixture.finalErr) {
					t.Fatalf("secondary = %v", err)
				}
			default:
				t.Fatal("stopped failure was not secondary")
			}
		})
	}
}

func TestReviewTransferFinalEventsPrecedeResolverJoin(t *testing.T) {
	for _, mode := range []string{"timeout", "cancellation", "admission error"} {
		t.Run(mode, func(t *testing.T) {
			c, meta, selection, plan, _ := admissionFixture(t)
			meta.Trackers = []string{torrent.DefaultTracker}
			log := new(shutdownOrderLog)
			resolver := newShutdownBarrierResolver(t, log)
			fixture := &shutdownOrderTracker{log: log, finalStarted: make(chan struct{}), queue: c.updateQueue, finalErr: errors.New("secondary stopped failure")}
			c.config.HTTP, c.config.Resolver = fixture, resolver
			secondary := make(chan error, 1)
			c.config.OnSecondary = func(err error) { secondary <- err }
			if mode == "timeout" {
				c.config.Timeout = 100 * time.Millisecond
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				err := c.startTransferPhase(ctx, torrent.Source{}, meta, selection, plan, storage.ResumeResult{})
				log.record("return")
				done <- err
			}()
			awaitShutdownBarrier(t, resolver.started, "transfer resolver start")
			want := ErrNoProgressTimeout
			switch mode {
			case "cancellation":
				want = context.Canceled
				cancel()
			case "admission error":
				want = ErrTrackerUpdateQueue
				c.overflow.Store(true)
				c.updateQueue.signal()
			}
			releaseShutdownResolver(t, resolver, fixture.finalStarted)
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("transfer error = %v, want %v", err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("transfer did not return after resolver release")
			}
			awaitShutdownBarrier(t, resolver.done, "transfer resolver join")
			assertShutdownOrder(t, log, fixture)
			if c.updateQueue.pendingPeers() != 0 || len(c.updateQueue.events) != 0 {
				t.Fatal("phase retained pending updates after return")
			}
			assertAdmissionCacheRemoved(t, c.config.CacheRoot)
			select {
			case err := <-secondary:
				if !errors.Is(err, fixture.finalErr) {
					t.Fatalf("secondary = %v", err)
				}
			default:
				t.Fatal("stopped failure was not secondary")
			}
		})
	}
}
