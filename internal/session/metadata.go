package session

// Metadata discovery is the short network phase used by magnets and bare
// info hashes. It owns neither normalized torrent state nor any storage. A
// caller may inject an already-created TrackerSet when it needs the same
// session identity and disabled-tracker state for the later transfer phase.

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
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
	metadataEventQueueSize            = limits.SessionEvents
	metadataMessageLimit              = limits.MetadataRequests * 4
	metadataPeerTimeout               = 30 * time.Second
	metadataRetryPoll                 = 100 * time.Millisecond
)

var (
	ErrMetadataConfig       = errors.New("invalid metadata discovery configuration")
	ErrMetadataUnavailable  = errors.New("metadata is unavailable from the current peers")
	ErrMetadataRejected     = errors.New("metadata peer rejected the requested metadata")
	ErrMetadataInvalid      = errors.New("peer supplied invalid complete metadata")
	ErrMetadataEventQueue   = errors.New("metadata tracker event queue is full")
	ErrMetadataAlreadyPhase = errors.New("metadata tracker phase is already active")
)

// MetadataConfig contains only phase-local dependencies. TrackerSet and
// Updates are both optional together: when TrackerSet is nil, discovery
// creates one with an internal bounded update queue. To reuse a session-wide
// TrackerSet, inject that set and the channel used by its OnUpdate callback.
type MetadataConfig struct {
	InfoHash torrent.InfoHash
	Trackers []string
	Peers    []torrent.PeerAddress

	TrackerSet *tracker.TrackerSet
	Updates    <-chan tracker.Update
	Identity   tracker.Identity
	HTTP       tracker.TrackerHTTP
	UDP        tracker.TrackerUDP
	Random     io.Reader

	Resolver       peer.Resolver
	TCPDial        peer.DialFunc
	UTPDial        peer.DialFunc
	Clock          peer.RaceClock
	UTPHeadStart   time.Duration
	PeerTimeout    time.Duration
	Backoff        *peer.EndpointBackoff
	LocalHandshake peer.Handshake
	Now            func() time.Time

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
		return nil, fmt.Errorf("%w: injected TrackerSet requires Updates", ErrMetadataConfig)
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

	var updateQueue chan tracker.Update
	if config.TrackerSet == nil {
		updateQueue = make(chan tracker.Update, metadataEventQueueSize)
		updates = updateQueue
	}

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
		select {
		case updateQueue <- update:
		default:
			setCallbackErr(ErrMetadataEventQueue)
		}
	}

	set := config.TrackerSet
	ownSet := false
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
			Random:    config.Random,
			OnUpdate:  callback,
			NeedPeers: func() bool { return true },
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

	resolver := config.Resolver
	pool, err := peer.NewCandidatePool(peer.CandidatePoolConfig{Resolver: resolver})
	if err != nil {
		return MetadataResult{}, fmt.Errorf("%w: candidate pool: %v", ErrMetadataConfig, err)
	}
	for _, embedded := range config.Peers {
		_, admitErr := pool.Admit(phaseCtx, peer.Candidate{Host: embedded.Host, Port: embedded.Port})
		if admitErr != nil {
			return MetadataResult{}, fmt.Errorf("%w: embedded peer: %v", ErrMetadataConfig, admitErr)
		}
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
	defer func() {
		cancel()
		if run != nil {
			if finalErr := run.Finalize(context.Background(), false); primary == nil && finalErr != nil {
				// Final event errors are deliberately secondary. They are not returned
				// when discovery has a primary result, including cancellation.
			}
		}
		if ownSet {
			_ = set.Close(context.Background())
		}
	}()
	run, err = set.Start(phaseCtx, tracker.MetadataPhase)
	if err != nil {
		return MetadataResult{}, err
	}

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

		// Consume all immediately available tracker updates before looking for
		// another dial. The callback itself remains nonblocking.
		for {
			select {
			case update, ok := <-updates:
				if !ok {
					updates = nil
					continue
				}
				for _, announced := range update.Peers {
					_, _ = pool.Admit(phaseCtx, peer.Candidate{Host: announced.Host, Port: announced.Port, ExpectedPeerID: announced.PeerID, HasExpectedID: announced.HasID})
				}
			default:
				goto updatesDrained
			}
		}
	updatesDrained:

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
				_ = run.Finalize(context.Background(), false)
				result.Endpoints = pool.Snapshot()
				result.Strikes = strikeSnapshot(strikes)
				result.Metainfo, err = torrent.ParseInfoDictionary(candidateData.data, config.InfoHash, set.Trackers())
				if err != nil {
					primary = err
					return result, err
				}
				return result, nil
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
		case update, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			for _, announced := range update.Peers {
				_, _ = pool.Admit(phaseCtx, peer.Candidate{Host: announced.Host, Port: announced.Port, ExpectedPeerID: announced.PeerID, HasExpectedID: announced.HasID})
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
	connected, err := manager.Race(ctx, candidate)
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
	state := peer.NewExtensionState()
	if err := state.WriteHandshake(conn); err != nil {
		return nil, err
	}

	// Take the first bounded size advertised by this connection. A later
	// repeated handshake may update the peer's extension mapping, but it cannot
	// replace the candidate geometry already chosen for this supplier.
	var size int64
	for messageCount := 0; messageCount < metadataMessageLimit; messageCount++ {
		message, err := readPeerMessage(ctx, conn, timeout)
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
			if err := writeAll(conn, event.Response); err != nil {
				return nil, err
			}
		}
		if event.Metadata != nil && event.Metadata.Type == peer.MetadataReject {
			return nil, ErrMetadataRejected
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
	for piece := int64(0); piece < blocks; piece++ {
		if err := requestMetadataPiece(ctx, conn, state, uint32(piece)); err != nil {
			return nil, err
		}
		received := false
		for messageCount := 0; messageCount < metadataMessageLimit; messageCount++ {
			message, err := readPeerMessage(ctx, conn, timeout)
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
				if err := writeAll(conn, event.Response); err != nil {
					return nil, err
				}
			}
			if event.Metadata == nil {
				continue
			}
			switch event.Metadata.Type {
			case peer.MetadataUnknown:
				continue
			case peer.MetadataReject:
				return nil, ErrMetadataRejected
			case peer.MetadataData:
				if int64(event.Metadata.Piece) != piece {
					return nil, fmt.Errorf("%w: unsolicited metadata piece %d", ErrMetadataRejected, event.Metadata.Piece)
				}
				if err := assembler.AddMessage(*event.Metadata); err != nil {
					return nil, err
				}
				received = true
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

func requestMetadataPiece(ctx context.Context, conn net.Conn, state *peer.ExtensionState, piece uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, ok := state.RemoteExtensionID(peer.UtMetadataExtension)
	if !ok {
		return fmt.Errorf("%w: peer disabled ut_metadata", ErrMetadataRejected)
	}
	return peer.WriteMetadataRequest(conn, id, piece)
}

func readPeerMessage(ctx context.Context, conn net.Conn, timeout time.Duration) (peer.Message, error) {
	if timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		defer conn.SetReadDeadline(time.Time{})
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
