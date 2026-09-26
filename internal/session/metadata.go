package session

// Metadata discovery is the short network phase used by magnets and bare
// info hashes. It owns neither normalized torrent state nor any storage. A
// caller may inject an already-created TrackerSet when it needs the same
// session identity and disabled-tracker state for the later transfer phase.

import (
	"container/list"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/limits"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

const (
	// BEP 10 assigns bit 20 from the right, which is reserved[5]'s 0x10 bit.
	metadataExtensionReservedBit byte = 0x10
	magnetPeerSource                  = "magnet"
	metadataEventQueueSize            = limits.SessionEvents
	metadataMessageLimit              = limits.MetadataRequests * 4
	metadataPeerTimeout               = 30 * time.Second
	metadataRetryPoll                 = 100 * time.Millisecond
	trackerPeerQueueLimit             = limits.Candidates
	trackerPeerHostLimit              = limits.PathBytes
	trackerPeerHostBytesLimit         = limits.HTTPResponseBytes
	trackerResolverWorkers            = 8
	trackerResolverTimeout            = 2 * time.Second
	trackerEndpointRaceTimeout        = 2 * time.Second
	trackerAdmissionBatch             = 64
	trackerEventBatch                 = 8
	trackerResolverQueue              = trackerResolverWorkers
	peerSourceSlots                   = limits.Trackers + 2
)

var (
	ErrMetadataConfig       = errors.New("invalid metadata discovery configuration")
	ErrMetadataUnavailable  = errors.New("metadata is unavailable from the current peers")
	ErrMetadataRejected     = errors.New("metadata peer rejected the requested metadata")
	ErrMetadataInvalid      = errors.New("peer supplied invalid complete metadata")
	ErrMetadataEventQueue   = errors.New("metadata tracker event queue is full")
	ErrMetadataAlreadyPhase = errors.New("metadata tracker phase is already active")
	ErrTrackerUpdateQueue   = errors.New("session tracker event queue is full")
	ErrTrackerSourceLimit   = errors.New("session tracker source limit reached")
)

// trackerPeerUpdateQueue keeps status events and announced peers in separate
// bounded queues. IP literals get priority in each admission pump. At the peer
// or hostname-byte cap, admission evicts the oldest eligible peer from an
// overrepresented source. Peers already being resolved are never evicted.
//
// The event channel retains the original accounting/error fields but never a
// tracker-owned peer slice. Peer entries are copied into one queue node each,
// so a large input slice cannot keep an oversized backing array reachable.
type trackerPeerUpdateQueue struct {
	mu          sync.Mutex
	sourceMu    sync.Mutex
	events      chan tracker.Update
	notify      chan struct{}
	ipPeers     list.List
	hosts       list.List
	total       int // queued and currently resolving peers
	hostBytes   int // queued and currently resolving hostname bytes
	nextID      uint64
	sourceCount map[peer.CandidateSource]int
	sourceOrder map[peer.CandidateSource]*queuedPeerSource
	sources     map[string]peer.CandidateSource
}

type queuedTrackerPeer struct {
	peer            tracker.TrackerPeer
	phase           tracker.Phase
	source          peer.CandidateSource
	ip              bool
	id              uint64
	queueElement    *list.Element
	sourceElement   *list.Element
	hostSourceEntry *list.Element
}

type queuedPeerSource struct {
	all   list.List
	hosts list.List
}

func newTrackerPeerUpdateQueue() *trackerPeerUpdateQueue {
	return &trackerPeerUpdateQueue{
		events:      make(chan tracker.Update, metadataEventQueueSize),
		notify:      make(chan struct{}, 1),
		sourceCount: make(map[peer.CandidateSource]int),
		sourceOrder: make(map[peer.CandidateSource]*queuedPeerSource),
		sources:     make(map[string]peer.CandidateSource),
	}
}

// enqueue is nonblocking with respect to consumers and network operations.
// The caller owns handling ErrTrackerUpdateQueue, preserving the existing
// tracker-event overflow behavior.
func (q *trackerPeerUpdateQueue) enqueue(update tracker.Update) error {
	if q == nil {
		return ErrTrackerUpdateQueue
	}
	source, err := q.sourceID(update.Tracker)
	if err != nil {
		return err
	}
	status := update
	status.Peers = nil
	select {
	case q.events <- status:
	default:
		return ErrTrackerUpdateQueue
	}
	q.enqueuePeersFromID(source, update.Phase, update.Peers)
	return nil
}

func (q *trackerPeerUpdateQueue) enqueuePeers(phase tracker.Phase, peers []tracker.TrackerPeer) {
	q.enqueuePeersFromID(peer.DefaultCandidateSource, phase, peers)
}

func (q *trackerPeerUpdateQueue) enqueuePeersFrom(source string, phase tracker.Phase, peers []tracker.TrackerPeer) error {
	if q == nil {
		return ErrTrackerUpdateQueue
	}
	sourceID, err := q.sourceID(source)
	if err != nil {
		return err
	}
	q.enqueuePeersFromID(sourceID, phase, peers)
	return nil
}

// sourceID maps each exact tracker URL once per configured tracker. Peer queue
// entries and CandidatePool keys carry only this uint16 ID.
func (q *trackerPeerUpdateQueue) sourceID(source string) (peer.CandidateSource, error) {
	if source == "" {
		return peer.DefaultCandidateSource, nil
	}
	if source == magnetPeerSource {
		return peer.MagnetCandidateSource, nil
	}
	q.sourceMu.Lock()
	defer q.sourceMu.Unlock()
	if existing, ok := q.sources[source]; ok {
		return existing, nil
	}
	if len(q.sources) >= limits.Trackers {
		return peer.DefaultCandidateSource, ErrTrackerSourceLimit
	}
	id := peer.FirstTrackerCandidateSource + peer.CandidateSource(len(q.sources))
	q.sources[source] = id
	return id, nil
}

func (q *trackerPeerUpdateQueue) enqueuePeersFromID(source peer.CandidateSource, phase tracker.Phase, peers []tracker.TrackerPeer) {
	if q == nil {
		return
	}
	limit := len(peers)
	if limit > trackerPeerQueueLimit {
		limit = trackerPeerQueueLimit
	}
	if limit == 0 {
		q.signal()
		return
	}
	q.mu.Lock()
	// Admit IP literals first, so a hostname-heavy update cannot place its
	// usable literal peers behind any resolver work, including in later updates.
	// Append IPs from the end so takePeer's back-pop preserves parser order
	// within this update while still putting newer updates ahead of older ones.
	for i := limit - 1; i >= 0; i-- {
		announced := peers[i]
		if !usableQueuedPeer(announced) || !isTrackerPeerIP(announced.Host) {
			continue
		}
		if !q.makeRoomForIPLocked(source) {
			continue
		}
		q.pushLocked(source, phase, announced, true)
	}
	for i := 0; i < limit; i++ {
		announced := peers[i]
		if !usableQueuedPeer(announced) || isTrackerPeerIP(announced.Host) {
			continue
		}
		victims, ok := q.planEvictionsLocked(source, len(announced.Host))
		if !ok {
			continue
		}
		for _, victim := range victims {
			q.removeQueuedPeerLocked(victim)
		}
		q.pushLocked(source, phase, announced, false)
	}
	q.mu.Unlock()
	q.signal()
}

func usableQueuedPeer(announced tracker.TrackerPeer) bool {
	return announced.Port != 0 && announced.Host != "" && len(announced.Host) <= trackerPeerHostLimit
}

func isTrackerPeerIP(host string) bool {
	if len(host) == 0 || len(host) > trackerPeerHostLimit {
		return false
	}
	_, err := netip.ParseAddr(host)
	return err == nil
}

func (q *trackerPeerUpdateQueue) pushLocked(source peer.CandidateSource, phase tracker.Phase, announced tracker.TrackerPeer, isIP bool) {
	q.nextID++
	entry := &queuedTrackerPeer{
		peer: tracker.TrackerPeer{
			Host: strings.Clone(announced.Host), Port: announced.Port,
			PeerID: announced.PeerID, HasID: announced.HasID,
		},
		phase: phase, source: source, ip: isIP, id: q.nextID,
	}
	if isIP {
		entry.queueElement = q.ipPeers.PushBack(entry)
	} else {
		entry.queueElement = q.hosts.PushBack(entry)
		q.hostBytes += len(entry.peer.Host)
	}
	perSource := q.sourceOrder[source]
	if perSource == nil {
		perSource = &queuedPeerSource{}
		q.sourceOrder[source] = perSource
	}
	entry.sourceElement = perSource.all.PushBack(entry)
	if !isIP {
		entry.hostSourceEntry = perSource.hosts.PushBack(entry)
	}
	q.total++
	q.sourceCount[source]++
}

func (q *trackerPeerUpdateQueue) makeRoomForIPLocked(source peer.CandidateSource) bool {
	if q.total < trackerPeerQueueLimit {
		return true
	}
	return q.evictQueuedPeerLocked(source)
}

func (q *trackerPeerUpdateQueue) planEvictionsLocked(source peer.CandidateSource, newHostBytes int) ([]*queuedTrackerPeer, bool) {
	if q.total < trackerPeerQueueLimit && (newHostBytes == 0 || q.hostBytes+newHostBytes <= trackerPeerHostBytesLimit) {
		return nil, true
	}
	var counts [peerSourceSlots]int
	var allFronts [peerSourceSlots]*list.Element
	var hostFronts [peerSourceSlots]*list.Element
	for source, count := range q.sourceCount {
		counts[int(source)] = count
		if order := q.sourceOrder[source]; order != nil {
			allFronts[int(source)] = order.all.Front()
			hostFronts[int(source)] = order.hosts.Front()
		}
	}
	total, bytes := q.total, q.hostBytes
	var victims []*queuedTrackerPeer
	for total >= trackerPeerQueueLimit || newHostBytes > 0 && bytes+newHostBytes > trackerPeerHostBytesLimit {
		hostOnly := newHostBytes > 0 && bytes+newHostBytes > trackerPeerHostBytesLimit
		fronts := allFronts
		if hostOnly {
			fronts = hostFronts
		}
		victimSource, ok := chooseQueuedSource(source, &counts, &fronts)
		if !ok {
			return nil, false
		}
		victim := fronts[int(victimSource)].Value.(*queuedTrackerPeer)
		victims = append(victims, victim)
		if allFronts[int(victimSource)] != nil && allFronts[int(victimSource)].Value.(*queuedTrackerPeer) == victim {
			allFronts[int(victimSource)] = allFronts[int(victimSource)].Next()
		}
		if victim.hostSourceEntry != nil {
			if hostFronts[int(victimSource)] != nil && hostFronts[int(victimSource)].Value.(*queuedTrackerPeer) == victim {
				hostFronts[int(victimSource)] = hostFronts[int(victimSource)].Next()
			}
			bytes -= len(victim.peer.Host)
		}
		counts[int(victimSource)]--
		total--
	}
	return victims, true
}

func (q *trackerPeerUpdateQueue) evictQueuedPeerLocked(source peer.CandidateSource) bool {
	target, found := q.chooseQueuedSourceLocked(source)
	if !found {
		return false
	}
	q.removeQueuedPeerLocked(q.sourceOrder[target].all.Front().Value.(*queuedTrackerPeer))
	return true
}

func (q *trackerPeerUpdateQueue) chooseQueuedSourceLocked(incoming peer.CandidateSource) (peer.CandidateSource, bool) {
	ownCount := q.sourceCount[incoming]
	var maxOther peer.CandidateSource
	maxOtherCount, hasOther := 0, false
	for source, count := range q.sourceCount {
		order := q.sourceOrder[source]
		if source == incoming || order == nil || order.all.Front() == nil {
			continue
		}
		if !hasOther || count > maxOtherCount || count == maxOtherCount && source < maxOther {
			maxOther, maxOtherCount, hasOther = source, count, true
		}
	}
	if hasOther && (ownCount == 0 || maxOtherCount > ownCount) {
		return maxOther, true
	}
	if own := q.sourceOrder[incoming]; own != nil && own.all.Front() != nil {
		return incoming, true
	}
	var target peer.CandidateSource
	maxCount, found := 0, false
	for source, count := range q.sourceCount {
		order := q.sourceOrder[source]
		if order == nil || order.all.Front() == nil {
			continue
		}
		if !found || count > maxCount || count == maxCount && source < target {
			target, maxCount, found = source, count, true
		}
	}
	return target, found
}

func chooseQueuedSource(incoming peer.CandidateSource, counts *[peerSourceSlots]int, fronts *[peerSourceSlots]*list.Element) (peer.CandidateSource, bool) {
	ownCount := counts[int(incoming)]
	var maxOther peer.CandidateSource
	maxOtherCount, hasOther := 0, false
	for index, count := range counts {
		source := peer.CandidateSource(index)
		if source == incoming || fronts[index] == nil {
			continue
		}
		if !hasOther || count > maxOtherCount || count == maxOtherCount && source < maxOther {
			maxOther, maxOtherCount, hasOther = source, count, true
		}
	}
	if hasOther && (ownCount == 0 || maxOtherCount > ownCount) {
		return maxOther, true
	}
	if fronts[int(incoming)] != nil {
		return incoming, true
	}
	var target peer.CandidateSource
	maxCount, found := 0, false
	for index, count := range counts {
		source := peer.CandidateSource(index)
		if fronts[index] == nil {
			continue
		}
		if !found || count > maxCount || count == maxCount && source < target {
			target, maxCount, found = source, count, true
		}
	}
	return target, found
}

func (q *trackerPeerUpdateQueue) detachSourceOrderLocked(item *queuedTrackerPeer) {
	if item.sourceElement != nil {
		perSource := q.sourceOrder[item.source]
		perSource.all.Remove(item.sourceElement)
		item.sourceElement = nil
	}
	if item.hostSourceEntry != nil {
		perSource := q.sourceOrder[item.source]
		perSource.hosts.Remove(item.hostSourceEntry)
		item.hostSourceEntry = nil
	}
}

func (q *trackerPeerUpdateQueue) removeQueuedPeerLocked(item *queuedTrackerPeer) {
	if item.ip {
		q.ipPeers.Remove(item.queueElement)
	} else {
		q.hosts.Remove(item.queueElement)
		q.hostBytes -= len(item.peer.Host)
	}
	q.detachSourceOrderLocked(item)
	q.releasePeerLocked(item)
}

func (q *trackerPeerUpdateQueue) releasePeerLocked(item *queuedTrackerPeer) {
	q.total--
	q.sourceCount[item.source]--
	if q.sourceCount[item.source] == 0 {
		delete(q.sourceCount, item.source)
		if order := q.sourceOrder[item.source]; order != nil && order.all.Len() == 0 && order.hosts.Len() == 0 {
			delete(q.sourceOrder, item.source)
		}
	}
}

func (q *trackerPeerUpdateQueue) takePeer(allowHostname bool) *queuedTrackerPeer {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if newest := q.ipPeers.Back(); newest != nil {
		item := newest.Value.(*queuedTrackerPeer)
		q.ipPeers.Remove(newest)
		item.queueElement = nil
		q.detachSourceOrderLocked(item)
		return item
	}
	if allowHostname {
		if front := q.hosts.Front(); front != nil {
			item := front.Value.(*queuedTrackerPeer)
			q.hosts.Remove(front)
			item.queueElement = nil
			q.detachSourceOrderLocked(item)
			return item
		}
	}
	return nil
}

func (q *trackerPeerUpdateQueue) releasePeer(item *queuedTrackerPeer) {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.total > 0 {
		if item != nil {
			if item.sourceElement != nil {
				q.detachSourceOrderLocked(item)
			}
			if !item.ip {
				q.hostBytes -= len(item.peer.Host)
			}
			q.releasePeerLocked(item)
		}
	}
	q.mu.Unlock()
}

func (q *trackerPeerUpdateQueue) pendingPeers() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	n := q.total
	q.mu.Unlock()
	return n
}

func (q *trackerPeerUpdateQueue) clear() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.ipPeers.Init()
	q.hosts.Init()
	q.total = 0
	q.hostBytes = 0
	q.sourceCount = make(map[peer.CandidateSource]int)
	q.sourceOrder = make(map[peer.CandidateSource]*queuedPeerSource)
	q.mu.Unlock()
	for {
		select {
		case <-q.events:
		default:
			q.signal()
			return
		}
	}
}

func (q *trackerPeerUpdateQueue) signal() {
	if q == nil {
		return
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

type trackerResolveResult struct {
	peer       *queuedTrackerPeer
	candidates []peer.ResolvedCandidate
	err        error
}

// trackerPeerResolver owns a fixed worker set. Its job queue is deliberately
// small; the remaining announced peers stay in trackerPeerUpdateQueue, where
// IP literals can continue to pass hostname work.
type trackerPeerResolver struct {
	ctx          context.Context
	cancel       context.CancelFunc
	resolver     peer.Resolver
	onDiagnostic func(Diagnostic)
	onPump       func()
	queue        *trackerPeerUpdateQueue
	jobs         chan *queuedTrackerPeer
	results      chan trackerResolveResult
	wg           sync.WaitGroup
	mu           sync.Mutex
}

func newTrackerPeerResolver(ctx context.Context, resolver peer.Resolver, queue *trackerPeerUpdateQueue, onDiagnostic ...func(Diagnostic)) *trackerPeerResolver {
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	var observe func(Diagnostic)
	if len(onDiagnostic) != 0 {
		observe = onDiagnostic[0]
	}
	a := &trackerPeerResolver{
		ctx: workerCtx, cancel: cancel, resolver: resolver, onDiagnostic: observe, queue: queue,
		jobs:    make(chan *queuedTrackerPeer, trackerResolverQueue),
		results: make(chan trackerResolveResult, trackerResolverWorkers),
	}
	for i := 0; i < trackerResolverWorkers; i++ {
		a.wg.Add(1)
		go a.worker()
	}
	return a
}

func (a *trackerPeerResolver) worker() {
	defer a.wg.Done()
	for {
		if a.ctx.Err() != nil {
			return
		}
		select {
		case <-a.ctx.Done():
			return
		case item := <-a.jobs:
			if item == nil {
				continue
			}
			if a.ctx.Err() != nil {
				a.queue.releasePeer(item)
				continue
			}
			lookupCtx, cancel := context.WithTimeout(a.ctx, trackerResolverTimeout)
			candidates, err := peer.ResolveCandidate(lookupCtx, a.resolver, peer.Candidate{
				Host: item.peer.Host, Port: item.peer.Port,
				ExpectedPeerID: item.peer.PeerID, HasExpectedID: item.peer.HasID,
			})
			cancel()
			// A full result buffer can hold at most one result per worker. This
			// blocking send is always canceled during phase shutdown, and keeps
			// ownership explicit so Close can release every peer reservation.
			select {
			case a.results <- trackerResolveResult{peer: item, candidates: candidates, err: err}:
				a.queue.signal()
			case <-a.ctx.Done():
				a.queue.releasePeer(item)
				return
			}
		}
	}
}

func (a *trackerPeerResolver) pump(ctx context.Context, phase tracker.Phase, pool *peer.CandidatePool) int {
	if a == nil || pool == nil {
		return 0
	}
	a.mu.Lock()
	defer func() {
		a.mu.Unlock()
		if a.onPump != nil {
			a.onPump()
		}
	}()
	work := 0
	for work < trackerResolverWorkers {
		select {
		case result := <-a.results:
			if result.err == nil {
				for _, candidate := range result.candidates {
					before := pool.Len()
					added, addErr := pool.AddFrom(result.peer.source, candidate)
					observePeerSelection(a.onDiagnostic, trackerPhaseName(result.peer.phase), candidate.Endpoint, added, added && before >= limits.Candidates, addErr)
				}
			}
			a.queue.releasePeer(result.peer)
			work++
		default:
			goto resultsDrained
		}
	}
resultsDrained:
	// Give one queued hostname a resolver turn before processing the IP stream.
	// takeHostname is FIFO, and this one-job allowance keeps IPs preferred while
	// preventing a continuing stream of literal IPs from starving hostnames.
	if len(a.jobs) < cap(a.jobs) && work < trackerAdmissionBatch-trackerEventBatch-trackerResolverWorkers {
		if item := a.queue.takeHostname(); item != nil {
			if item.phase != phase {
				a.queue.releasePeer(item)
				work++
			} else {
				select {
				case a.jobs <- item:
					work++
				default:
					a.queue.requeuePeer(item)
					return work
				}
			}
		}
	}
	for work < trackerAdmissionBatch-trackerEventBatch-trackerResolverWorkers {
		allowHostname := len(a.jobs) < cap(a.jobs)
		item := a.queue.takePeer(allowHostname)
		if item == nil {
			break
		}
		if item.phase != phase {
			a.queue.releasePeer(item)
			work++
			continue
		}
		if item.ip {
			candidates, err := peer.ResolveCandidate(ctx, nil, peer.Candidate{
				Host: item.peer.Host, Port: item.peer.Port,
				ExpectedPeerID: item.peer.PeerID, HasExpectedID: item.peer.HasID,
			})
			if err == nil {
				for _, candidate := range candidates {
					before := pool.Len()
					added, addErr := pool.AddFrom(item.source, candidate)
					observePeerSelection(a.onDiagnostic, trackerPhaseName(item.phase), candidate.Endpoint, added, added && before >= limits.Candidates, addErr)
				}
			}
			a.queue.releasePeer(item)
			work++
			continue
		}
		select {
		case a.jobs <- item:
			work++
		default:
			// Only the coordinator submits jobs, and workers only receive, so
			// this is defensive. Return the item to the front without changing
			// the retained-peer count.
			a.queue.requeuePeer(item)
			return work
		}
	}
	for i := 0; i < trackerEventBatch; i++ {
		select {
		case <-a.queue.events:
		default:
			i = trackerEventBatch
		}
	}
	return work
}

func observePeerSelection(callback func(Diagnostic), phase string, endpoint peer.Endpoint, added, replaced bool, err error) {
	if callback == nil {
		return
	}
	detail := "candidate duplicate"
	if added {
		detail = "candidate admitted"
	}
	if replaced {
		detail = "candidate replaced at capacity"
	}
	if err != nil {
		detail = "candidate rejected"
	}
	callback(Diagnostic{Kind: DiagnosticPeerSelection, Phase: phase, Peer: endpoint, Detail: detail})
}

func (q *trackerPeerUpdateQueue) takeHostname() *queuedTrackerPeer {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	front := q.hosts.Front()
	if front == nil {
		return nil
	}
	item := front.Value.(*queuedTrackerPeer)
	q.hosts.Remove(front)
	item.queueElement = nil
	q.detachSourceOrderLocked(item)
	return item
}

func (q *trackerPeerUpdateQueue) requeuePeer(item *queuedTrackerPeer) {
	if q == nil || item == nil {
		return
	}
	q.mu.Lock()
	perSource := q.sourceOrder[item.source]
	if perSource == nil {
		perSource = &queuedPeerSource{}
		q.sourceOrder[item.source] = perSource
	}
	if item.ip {
		item.queueElement = q.ipPeers.PushFront(item)
	} else {
		item.queueElement = q.hosts.PushFront(item)
		item.hostSourceEntry = perSource.hosts.PushFront(item)
	}
	item.sourceElement = perSource.all.PushFront(item)
	q.mu.Unlock()
	q.signal()
}

func (a *trackerPeerResolver) close() {
	if a == nil {
		return
	}
	a.cancel()
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	for {
		select {
		case result := <-a.results:
			a.queue.releasePeer(result.peer)
		case <-done:
			for {
				select {
				case result := <-a.results:
					a.queue.releasePeer(result.peer)
				default:
					goto resultsDrained
				}
			}
		resultsDrained:
			for {
				select {
				case item := <-a.jobs:
					a.queue.releasePeer(item)
				default:
					return
				}
			}
		}
	}
}

// MetadataConfig contains only phase-local dependencies. TrackerSet and
// Updates are both optional together: when TrackerSet is nil, discovery
// creates one with an internal bounded update queue. To reuse a session-wide
// TrackerSet, inject that set and the channel used by its OnUpdate callback.
type MetadataConfig struct {
	InfoHash torrent.InfoHash
	Trackers []string
	Peers    []torrent.PeerAddress

	TrackerSet    *tracker.TrackerSet
	Updates       <-chan tracker.Update
	updateQueue   *trackerPeerUpdateQueue
	candidatePool *peer.CandidatePool
	Identity      tracker.Identity
	HTTP          tracker.TrackerHTTP
	UDP           tracker.TrackerUDP
	TrackerClock  tracker.Clock
	Random        io.Reader

	Resolver       peer.Resolver
	TCPDial        peer.DialFunc
	UTPDial        peer.DialFunc
	Clock          peer.RaceClock
	UTPHeadStart   time.Duration
	PeerTimeout    time.Duration
	Backoff        *peer.EndpointBackoff
	LocalHandshake peer.Handshake
	Now            func() time.Time
	OnSecondary    func(error)
	OnDiagnostic   func(Diagnostic)
	onTrackerPump  func()

	// OnStrike observes a completed, hash-invalid candidate. It is called once
	// per invalid candidate, after the endpoint strike count is incremented.
	OnStrike func(peer.Endpoint, uint8)
}

// EndpointStrike carries the bounded corruption state that must survive the
// metadata-to-transfer phase boundary. Endpoint identity is resolved IP and
// port, independent of transport and peer ID.
type EndpointStrike struct {
	Endpoint peer.Endpoint
	Strikes  uint8
}

// MetadataResult is immutable session-owned output. The slices are detached
// copies, and callers must treat them as read-only. No network worker or
// staging workspace is retained.
type MetadataResult struct {
	Metainfo  torrent.Metainfo
	Endpoints []peer.ResolvedCandidate
	Strikes   []EndpointStrike
}

// MetadataDiscovery runs one metadata-only phase.
type MetadataDiscovery struct {
	config MetadataConfig
}

type metadataCandidate struct {
	data []byte
}

// NewMetadataDiscovery validates static inputs without touching the network,
// filesystem, or cache.
func NewMetadataDiscovery(config MetadataConfig) (*MetadataDiscovery, error) {
	if config.TrackerSet == nil {
		trackers, err := torrent.TrackersWithDefault(config.Trackers)
		if err != nil {
			return nil, fmt.Errorf("%w: trackers: %v", ErrMetadataConfig, err)
		}
		config.Trackers = trackers
		if config.Updates != nil {
			return nil, fmt.Errorf("%w: Updates requires an injected TrackerSet", ErrMetadataConfig)
		}
	} else if config.Updates == nil {
		if config.updateQueue == nil {
			return nil, fmt.Errorf("%w: injected TrackerSet requires Updates", ErrMetadataConfig)
		}
	}
	for _, candidate := range config.Peers {
		if candidate.Port == 0 || candidate.Host == "" {
			return nil, fmt.Errorf("%w: embedded peer has an invalid endpoint", ErrMetadataConfig)
		}
	}
	return &MetadataDiscovery{config: config}, nil
}

// DiscoverMetadata is the convenient one-shot form of NewMetadataDiscovery
// and Run.
func DiscoverMetadata(ctx context.Context, config MetadataConfig) (MetadataResult, error) {
	discovery, err := NewMetadataDiscovery(config)
	if err != nil {
		return MetadataResult{}, err
	}
	return discovery.Run(ctx)
}

// Run starts all metadata trackers, admits embedded and tracker peers through
// CandidatePool, and tries candidates until one supplies valid metadata or the
// caller cancels. Tracker loops and the active peer are always joined before
// Run returns. Final stopped announcements are best effort and never replace
// the primary result.
func (d *MetadataDiscovery) Run(ctx context.Context) (result MetadataResult, primary error) {
	if d == nil {
		return MetadataResult{}, ErrMetadataConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return MetadataResult{}, err
	}

	config := d.config
	updates := config.Updates
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	updateQueue := config.updateQueue
	if updateQueue == nil {
		updateQueue = newTrackerPeerUpdateQueue()
	}
	defer updateQueue.clear()

	var callbackErr error
	var callbackMu sync.Mutex
	setCallbackErr := func(err error) {
		callbackMu.Lock()
		if callbackErr == nil {
			callbackErr = err
		}
		callbackMu.Unlock()
		cancel()
	}
	callback := func(update tracker.Update) {
		if update.Phase != tracker.MetadataPhase {
			return
		}
		if enqueueErr := updateQueue.enqueue(update); enqueueErr != nil {
			setCallbackErr(ErrMetadataEventQueue)
		}
	}

	set := config.TrackerSet
	ownSet := false
	pool := config.candidatePool
	var err error
	if set == nil {
		ownSet = true
		identity := config.Identity
		if identity.PeerID == ([20]byte{}) || identity.Port == 0 {
			random := config.Random
			if random == nil {
				random = cryptorand.Reader
			}
			var err error
			identity, err = tracker.GenerateIdentity(random)
			if err != nil {
				return MetadataResult{}, fmt.Errorf("%w: generate identity: %v", ErrMetadataConfig, err)
			}
		}
		var err error
		set, err = tracker.NewTrackerSet(tracker.TrackerSetConfig{
			InfoHash:  [20]byte(config.InfoHash),
			Trackers:  config.Trackers,
			Identity:  identity,
			HTTP:      config.HTTP,
			UDP:       config.UDP,
			Clock:     config.TrackerClock,
			Random:    config.Random,
			OnUpdate:  callback,
			NeedPeers: func() bool { return pool == nil || pool.Len() == 0 },
		})
		if err != nil {
			return MetadataResult{}, fmt.Errorf("%w: tracker set: %v", ErrMetadataConfig, err)
		}
	}

	identity := set.Identity()
	local := peer.Handshake{InfoHash: [20]byte(config.InfoHash), PeerID: identity.PeerID}
	if config.Identity.PeerID != ([20]byte{}) && config.Identity.PeerID != identity.PeerID {
		return MetadataResult{}, fmt.Errorf("%w: local peer ID does not match TrackerSet identity", ErrMetadataConfig)
	}
	if config.LocalHandshake != (peer.Handshake{}) {
		local = config.LocalHandshake
		if local.InfoHash != [20]byte(config.InfoHash) || local.PeerID != identity.PeerID {
			return MetadataResult{}, fmt.Errorf("%w: LocalHandshake does not match session identity", ErrMetadataConfig)
		}
	}
	local.Reserved[5] |= metadataExtensionReservedBit

	if pool == nil {
		pool, err = peer.NewCandidatePool(peer.CandidatePoolConfig{Resolver: config.Resolver})
		if err != nil {
			return MetadataResult{}, fmt.Errorf("%w: candidate pool: %v", ErrMetadataConfig, err)
		}
	}
	for _, embedded := range config.Peers {
		updateQueue.enqueuePeersFrom(magnetPeerSource, tracker.MetadataPhase, []tracker.TrackerPeer{{Host: embedded.Host, Port: embedded.Port}})
	}

	backoff := config.Backoff
	if backoff == nil {
		backoff = peer.NewEndpointBackoff()
	}
	manager, err := peer.NewDialManager(peer.DialManagerConfig{
		Race: peer.RaceConfig{
			LocalHandshake: local,
			TCPDial:        config.TCPDial,
			UTPDial:        config.UTPDial,
			Clock:          config.Clock,
			UTPHeadStart:   config.UTPHeadStart,
		},
		Backoff: backoff,
		Now:     config.Now,
	})
	if err != nil {
		return MetadataResult{}, fmt.Errorf("%w: dial manager: %v", ErrMetadataConfig, err)
	}

	var run *tracker.PhaseRun
	var resolver *trackerPeerResolver
	finalize := func() {
		if run == nil {
			return
		}
		cancel()
		run.Wait()
		resolver.close()
		updateQueue.clear()
		if config.onTrackerPump != nil {
			config.onTrackerPump()
		}
		finalErr := run.Finalize(context.Background(), false)
		run = nil
		if finalErr != nil && config.OnSecondary != nil {
			config.OnSecondary(finalErr)
		}
	}
	defer func() {
		cancel()
		finalize()
		if ownSet {
			_ = set.Close(context.Background())
		}
	}()
	run, err = set.Start(phaseCtx, tracker.MetadataPhase)
	if err != nil {
		return MetadataResult{}, err
	}
	resolver = newTrackerPeerResolver(phaseCtx, config.Resolver, updateQueue, config.OnDiagnostic)
	resolver.onPump = config.onTrackerPump

	strikes := make(map[peer.Endpoint]uint8)
	for {
		if err := ctx.Err(); err != nil {
			primary = err
			break
		}
		callbackMu.Lock()
		queuedErr := callbackErr
		callbackMu.Unlock()
		if queuedErr != nil {
			primary = queuedErr
			break
		}

		// Admit a bounded amount of peer work before dialing. The peer queue
		// prioritizes IP literals, while names go to the fixed resolver pool.
		resolver.pump(phaseCtx, tracker.MetadataPhase, pool)
		for i := 0; updates != nil && i < trackerEventBatch; i++ {
			select {
			case update, ok := <-updates:
				if !ok {
					updates = nil
					i = trackerEventBatch
					continue
				}
				if err := updateQueue.enqueue(update); err != nil {
					setCallbackErr(ErrMetadataEventQueue)
				}
			default:
				i = trackerEventBatch
			}
		}

		var candidate peer.ResolvedCandidate
		found := false
		now := d.now()
		for _, next := range pool.Snapshot() {
			if backoff.Ready(next.Endpoint, now) {
				candidate, found = next, true
				break
			}
		}
		if found {
			candidateData, err := d.tryCandidate(phaseCtx, manager, candidate, strikes, backoff)
			if err == nil {
				// Hash and canonical bencoding have been checked while network
				// workers were active. Full v1 normalization starts only after
				// every metadata worker and tracker loop has been joined.
				cancel()
				finalize()
				result.Endpoints = pool.Snapshot()
				result.Strikes = strikeSnapshot(strikes)
				result.Metainfo, err = torrent.ParseInfoDictionary(candidateData.data, config.InfoHash, set.Trackers())
				if err != nil {
					primary = err
					return result, err
				}
				return result, nil
			}
			var budgetErr *peer.EndpointBudgetError
			if errors.As(err, &budgetErr) {
				primary = budgetErr
				break
			}
			if errors.Is(err, ErrMetadataRejected) && config.OnDiagnostic != nil {
				config.OnDiagnostic(Diagnostic{Kind: DiagnosticMetadataRefusal, Phase: "metadata", Peer: candidate.Endpoint, Detail: "peer refused metadata or disabled BEP 10"})
			}
			if errors.Is(err, ErrMetadataInvalid) {
				count := strikes[candidate.Endpoint]
				if count >= 3 {
					backoff.Blacklist(candidate.Endpoint)
				}
				if config.OnStrike != nil {
					config.OnStrike(candidate.Endpoint, count)
				}
			}
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				primary = ctx.Err()
				break
			}
			continue
		}

		select {
		case <-updateQueue.notify:
		case update, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			if err := updateQueue.enqueue(update); err != nil {
				setCallbackErr(ErrMetadataEventQueue)
			}
		case <-ctx.Done():
			primary = ctx.Err()
		case <-retryTimer(pool.Len() > 0, metadataRetryPoll):
		}
		if primary != nil {
			break
		}
	}

	result.Endpoints = pool.Snapshot()
	result.Strikes = strikeSnapshot(strikes)
	return result, primary
}

func (d *MetadataDiscovery) tryCandidate(ctx context.Context, manager *peer.DialManager, candidate peer.ResolvedCandidate, strikes map[peer.Endpoint]uint8, backoff *peer.EndpointBackoff) (metadataCandidate, error) {
	raceCtx, cancel := context.WithTimeout(ctx, trackerEndpointRaceTimeout)
	connected, err := manager.Race(raceCtx, candidate)
	cancel()
	if err != nil {
		if errors.Is(err, peer.ErrProtocolViolation) {
			backoff.Blacklist(candidate.Endpoint)
		}
		return metadataCandidate{}, err
	}
	defer connected.Conn.Close()
	if connected.Handshake.Reserved[5]&metadataExtensionReservedBit == 0 {
		backoff.RecordFailure(candidate.Endpoint, d.now())
		return metadataCandidate{}, fmt.Errorf("%w: peer did not negotiate BEP 10", ErrMetadataRejected)
	}
	data, err := fetchMetadata(ctx, connected.Conn, d.peerTimeout())
	if err != nil {
		if errors.Is(err, peer.ErrProtocolViolation) {
			backoff.Blacklist(candidate.Endpoint)
		} else if !errors.Is(err, context.Canceled) {
			backoff.RecordFailure(candidate.Endpoint, d.now())
		}
		return metadataCandidate{}, err
	}
	if err := validateMetadataCandidate(data, torrent.InfoHash(connected.Handshake.InfoHash)); err != nil {
		strikes[candidate.Endpoint]++
		backoff.RecordFailure(candidate.Endpoint, d.now())
		return metadataCandidate{}, fmt.Errorf("%w: %v", ErrMetadataInvalid, err)
	}
	return metadataCandidate{data: data}, nil
}

func validateMetadataCandidate(data []byte, expected torrent.InfoHash) error {
	if len(data) == 0 || len(data) > limits.MetainfoBytes {
		return fmt.Errorf("metadata length is outside the supported bound")
	}
	digest := sha1.Sum(data)
	if digest != [20]byte(expected) {
		return torrent.ErrInfoHashMismatch
	}
	value, err := bencode.Decode(data)
	if err != nil {
		return fmt.Errorf("canonical metadata bencoding: %v", err)
	}
	if value.Type != bencode.Dictionary {
		return errors.New("metadata is not an info dictionary")
	}
	return nil
}

func retryTimer(enabled bool, delay time.Duration) <-chan time.Time {
	if !enabled {
		return nil
	}
	return time.After(delay)
}

func (d *MetadataDiscovery) peerTimeout() time.Duration {
	if d != nil && d.config.PeerTimeout > 0 {
		return d.config.PeerTimeout
	}
	return metadataPeerTimeout
}

func (d *MetadataDiscovery) now() time.Time {
	if d != nil && d.config.Now != nil {
		return d.config.Now()
	}
	return time.Now()
}

func fetchMetadata(ctx context.Context, conn net.Conn, timeout time.Duration) ([]byte, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	state := peer.NewExtensionState()
	handshake, err := state.EncodeHandshake()
	if err != nil {
		return nil, err
	}
	if err := writeWithContext(ctx, conn, handshake, deadline); err != nil {
		return nil, err
	}

	// Take the first bounded size advertised by this connection. A later
	// repeated handshake may update the peer's extension mapping, but it cannot
	// replace the candidate geometry already chosen for this supplier.
	var size int64
	for messageCount := 0; messageCount < metadataMessageLimit; messageCount++ {
		message, err := readPeerMessage(ctx, conn, deadline)
		if err != nil {
			return nil, err
		}
		if message.KeepAlive || message.ID != peer.ExtendedID {
			continue
		}
		event, err := state.ApplyMessage(message)
		if err != nil {
			return nil, err
		}
		if len(event.Response) != 0 {
			if err := writeWithContext(ctx, conn, event.Response, deadline); err != nil {
				return nil, err
			}
		}
		if event.Metadata != nil && (event.Metadata.Type == peer.MetadataReject || event.Metadata.Type == peer.MetadataData) {
			return nil, fmt.Errorf("%w: metadata response before any request", peer.ErrProtocolViolation)
		}
		if advertised, ok := state.RemoteMetadataSize(); ok {
			size = advertised
			break
		}
	}
	if size == 0 {
		return nil, fmt.Errorf("%w: peer did not advertise metadata_size", ErrMetadataRejected)
	}
	assembler, err := peer.NewMetadataAssemblerWithSize(size)
	if err != nil {
		return nil, err
	}
	blocks := (size + int64(limits.BlockBytes) - 1) / int64(limits.BlockBytes)
	// Keep exactly one request outstanding. Only its valid data response
	// renews the idle deadline; other messages cannot prolong a stalled block.
	for piece := int64(0); piece < blocks; piece++ {
		if err := requestMetadataPiece(ctx, conn, state, uint32(piece), deadline); err != nil {
			return nil, err
		}
		received := false
		for messageCount := 0; messageCount < metadataMessageLimit; messageCount++ {
			message, err := readPeerMessage(ctx, conn, deadline)
			if err != nil {
				return nil, err
			}
			if message.KeepAlive || message.ID != peer.ExtendedID {
				continue
			}
			event, err := state.ApplyMessage(message)
			if err != nil {
				return nil, err
			}
			if len(event.Response) != 0 {
				if err := writeWithContext(ctx, conn, event.Response, deadline); err != nil {
					return nil, err
				}
			}
			if event.Metadata == nil {
				continue
			}
			switch event.Metadata.Type {
			case peer.MetadataUnknown:
				continue
			case peer.MetadataReject, peer.MetadataData:
				if int64(event.Metadata.Piece) != piece {
					return nil, fmt.Errorf("%w: unsolicited metadata response for piece %d", peer.ErrProtocolViolation, event.Metadata.Piece)
				}
				if event.Metadata.Type == peer.MetadataReject {
					return nil, ErrMetadataRejected
				}
				if err := assembler.AddMessage(*event.Metadata); err != nil {
					return nil, err
				}
				received = true
				if timeout > 0 {
					deadline = time.Now().Add(timeout)
				}
			}
			if received {
				break
			}
		}
		if !received {
			return nil, fmt.Errorf("%w: metadata response limit reached", ErrMetadataRejected)
		}
	}
	return assembler.Metadata()
}

func requestMetadataPiece(ctx context.Context, conn net.Conn, state *peer.ExtensionState, piece uint32, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, ok := state.RemoteExtensionID(peer.UtMetadataExtension)
	if !ok {
		return fmt.Errorf("%w: peer disabled ut_metadata", ErrMetadataRejected)
	}
	wire, err := peer.EncodeMetadataRequest(id, piece)
	if err != nil {
		return err
	}
	return writeWithContext(ctx, conn, wire, deadline)
}

func readPeerMessage(ctx context.Context, conn net.Conn, deadline time.Time) (peer.Message, error) {
	if !deadline.IsZero() {
		_ = conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}
	var timeout <-chan time.Time
	var timer *time.Timer
	if !deadline.IsZero() {
		timer = time.NewTimer(time.Until(deadline))
		timeout = timer.C
		defer timer.Stop()
	}
	type response struct {
		message peer.Message
		err     error
	}
	done := make(chan response, 1)
	go func() {
		message, err := peer.ReadMessage(conn)
		done <- response{message: message, err: err}
	}()
	select {
	case result := <-done:
		return result.message, result.err
	case <-ctx.Done():
		_ = conn.Close()
		<-done
		return peer.Message{}, ctx.Err()
	case <-timeout:
		_ = conn.Close()
		<-done
		return peer.Message{}, context.DeadlineExceeded
	}
}

// writeWithContext makes a bounded write joinable even when a test or custom
// transport does not unblock its Write method on context cancellation. The
// ordinary net.Conn deadline bounds each wait for useful metadata progress.
func writeWithContext(ctx context.Context, conn net.Conn, data []byte, deadline time.Time) error {
	if !deadline.IsZero() {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}
	var timeout <-chan time.Time
	var timer *time.Timer
	if !deadline.IsZero() {
		timer = time.NewTimer(time.Until(deadline))
		timeout = timer.C
		defer timer.Stop()
	}
	done := make(chan error, 1)
	go func() { done <- writeAll(conn, data) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = conn.Close()
		err := <-done
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return err
	case <-timeout:
		_ = conn.Close()
		<-done
		return context.DeadlineExceeded
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return errors.New("invalid network write count")
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func strikeSnapshot(strikes map[peer.Endpoint]uint8) []EndpointStrike {
	result := make([]EndpointStrike, 0, len(strikes))
	for endpoint, count := range strikes {
		result = append(result, EndpointStrike{Endpoint: endpoint, Strikes: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if order := result[i].Endpoint.Addr.Compare(result[j].Endpoint.Addr); order != 0 {
			return order < 0
		}
		return result[i].Endpoint.Port < result[j].Endpoint.Port
	})
	return result
}
