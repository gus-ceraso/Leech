package session

// run.go owns the lifetime of one command.  The lower level packages expose
// deliberately small seams, but they do not know when metadata, output, and
// transfer workers may overlap.  Run is that phase boundary.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
	"github.com/gus-ceraso/Leech/internal/utp"
)

var (
	ErrRunConfig         = errors.New("invalid session configuration")
	ErrNoProgressTimeout = errors.New("no verified file pieces before timeout")
)

// RunConfig contains command-independent session options and test seams.  A
// zero dependency uses the production standard-library implementation.  The
// source is normally filled by torrent.ParseSource; RunSource is a convenient
// boundary for callers that still have the original command-line string.
type RunConfig struct {
	Source    torrent.Source
	SourceRaw string
	OutputDir string
	// Output is a compatibility spelling for OutputDir.
	Output    string
	Patterns  []string
	Files     []string
	ListFiles bool
	Resume    bool
	Streaming bool
	Timeout   time.Duration

	Identity tracker.Identity
	Random   io.Reader

	HTTP            tracker.TrackerHTTP
	UDP             tracker.TrackerUDP
	TrackerClock    tracker.Clock
	Resolver        peer.Resolver
	TCPDial         peer.DialFunc
	UTPDial         peer.DialFunc
	RaceClock       peer.RaceClock
	UTPHeadStart    time.Duration
	CacheRoot       string
	StageFileOpener func(path string, flag int, mode os.FileMode) (storage.StagingFile, error)

	// TrackerSet and Updates are an optional complete tracker seam.  When they
	// are nil Run builds the production set and bounded update queue itself.
	TrackerSet *tracker.TrackerSet
	Updates    <-chan tracker.Update

	OnPhase     func(string)
	OnProgress  func(RunProgress)
	OnWarning   func(string)
	OnSecondary func(error)
}

// RunResult describes the validated session result.  Metainfo and Selection
// are retained for callers that need to render a list or final status; they
// are immutable after Run returns.
type RunResult struct {
	Metainfo          torrent.Metainfo
	Selection         *torrent.SelectionPlan
	Listed            []string
	NoTransferNeeded  bool
	SelectionComplete bool
	TorrentComplete   bool
	// HasVerifiedOutput reports selected payload retained in final output,
	// including pieces verified during resume.
	HasVerifiedOutput bool
}

// Progress is the small callback snapshot emitted after a verified piece.
type RunProgress struct {
	VerifiedSelectedBytes int64
	SelectedBytes         int64
	ActivePeers           int
	RecentRateBytesPerSec uint64
}

// RunSource parses raw source syntax and runs one session.
func RunSource(ctx context.Context, raw string, config RunConfig) (RunResult, error) {
	source, err := torrent.ParseSource(raw)
	if err != nil {
		return RunResult{}, err
	}
	config.Source = source
	config.SourceRaw = raw
	return Run(ctx, config)
}

// Run executes one complete source lifecycle.  It never starts transfer
// discovery before selection and resume validation, and it returns only after
// every owned network and storage worker has stopped.
func Run(ctx context.Context, config RunConfig) (result RunResult, err error) {
	var verifiedOutput atomic.Bool
	defer func() { result.HasVerifiedOutput = verifiedOutput.Load() }()

	if ctx == nil {
		ctx = context.Background()
	}
	if config.OutputDir == "" {
		config.OutputDir = config.Output
		if config.OutputDir == "" {
			config.OutputDir = "."
		}
	}
	if len(config.Patterns) == 0 {
		config.Patterns = append([]string(nil), config.Files...)
	}
	source := config.Source
	if source.Path == "" && source.Kind == torrent.SourcePath && config.SourceRaw != "" {
		var err error
		source, err = torrent.ParseSource(config.SourceRaw)
		if err != nil {
			return RunResult{}, err
		}
	}
	if source.Kind == torrent.SourcePath && source.Path == "" {
		return RunResult{}, fmt.Errorf("%w: source is empty", ErrRunConfig)
	}
	if config.Timeout < 0 {
		return RunResult{}, fmt.Errorf("%w: timeout must be positive", ErrRunConfig)
	}

	identity := config.Identity
	if identity.PeerID == ([20]byte{}) || identity.Port == 0 {
		random := config.Random
		if random == nil {
			random = rand.Reader
		}
		var err error
		identity, err = tracker.GenerateIdentity(random)
		if err != nil {
			return RunResult{}, fmt.Errorf("%w: generate session identity: %v", ErrRunConfig, err)
		}
	}

	run := &coordinator{config: config, identity: identity, backoff: peer.NewEndpointBackoff(), verifiedOutput: &verifiedOutput}
	run.updateQueue = newTrackerPeerUpdateQueue()
	run.ownSet = config.TrackerSet == nil
	defer func() {
		if closeErr := run.closeSet(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else if run.config.OnSecondary != nil {
				run.config.OnSecondary(closeErr)
			}
		}
	}()
	if config.TrackerSet != nil {
		run.set = config.TrackerSet
		run.updates = config.Updates
		if run.updates == nil {
			return RunResult{}, fmt.Errorf("%w: injected tracker set requires updates", ErrRunConfig)
		}
		run.identity = run.set.Identity()
	} else {
		run.updates = nil
		if source.Kind != torrent.SourcePath {
			if err := run.makeSet(source.Trackers, source.InfoHash, true); err != nil {
				return RunResult{}, err
			}
		}
	}

	var meta torrent.Metainfo
	switch source.Kind {
	case torrent.SourcePath:
		meta, err = torrent.LoadMetainfo(source.Path)
		if err != nil {
			return RunResult{}, err
		}
		if config.TrackerSet == nil && !config.ListFiles {
			// A local .torrent is validated completely before any network owner is
			// constructed.  This preserves offline validation and lets the
			// metainfo's tracker list replace the source's default-only list.
			if err := run.makeSet(meta.Trackers, meta.InfoHash, false); err != nil {
				return RunResult{}, err
			}
		}
	case torrent.SourceMagnet, torrent.SourceInfoHash:
		if config.TrackerSet == nil {
			run.phase("metadata")
			metadata, discoverErr := run.discover(ctx, source)
			if discoverErr != nil {
				return RunResult{}, discoverErr
			}
			meta = metadata
			run.metadataEndpoints = append([]peer.ResolvedCandidate(nil), run.metadataEndpoints...)
			if err := run.switchAccounting(meta); err != nil {
				return RunResult{}, err
			}
		} else {
			// An injected set still needs the ordinary metadata phase for a
			// magnet/hash source; its caller owns set construction and callbacks.
			run.phase("metadata")
			metadata, discoverErr := run.discover(ctx, source)
			if discoverErr != nil {
				return RunResult{}, discoverErr
			}
			meta = metadata
		}
	default:
		return RunResult{}, fmt.Errorf("%w: unknown source kind", ErrRunConfig)
	}

	if meta.Private {
		if run.config.OnWarning != nil {
			run.config.OnWarning("torrent declares private=1; treating the torrent as public")
		}
	}
	if config.ListFiles {
		return RunResult{Metainfo: meta, Listed: torrent.SelectableFiles(meta)}, nil
	}

	selection, err := torrent.Select(meta, config.Patterns, magnetSelection(source))
	if err != nil {
		return RunResult{}, err
	}
	run.phase("selection")
	plan, err := storage.Validate(config.OutputDir, meta, selection.SelectedIndices())
	if err != nil {
		return RunResult{}, err
	}

	resumeResult := storage.ResumeResult{}
	if config.Resume {
		run.phase("resume")
		resumeResult, err = storage.ScanResume(ctx, selection, plan)
		if len(resumeResult.VerifiedPieces) > 0 {
			verifiedOutput.Store(true)
		}
		if err != nil {
			return RunResult{}, err
		}
	}

	account, err := tracker.NewAccounting(realTorrentBytes(meta))
	if err != nil {
		return RunResult{}, err
	}
	run.account = account
	run.metadataMode.Store(false)
	for _, index := range resumeResult.VerifiedPieces {
		mapping, ok := selection.Piece(index)
		if !ok {
			return RunResult{}, fmt.Errorf("%w: resume piece %d is absent", ErrRunConfig, index)
		}
		if err := account.AddRetained(realPieceBytes(mapping)); err != nil {
			return RunResult{}, err
		}
	}

	// A complete resume still has to materialize selected zero-length files.
	// It is the only output operation before transfer, and it occurs after all
	// metadata, selection, and resume checks have succeeded.
	if resumeResult.NoTransferNeeded {
		prepared, prepareErr := plan.Prepare(storage.Resume)
		if prepareErr == nil {
			prepareErr = prepared.Close()
		}
		if prepareErr != nil {
			return RunResult{}, prepareErr
		}
		for _, pending := range resumeResult.PendingTruncations {
			if truncateErr := plan.TruncateSelected(pending.Index, pending.Size); truncateErr != nil {
				return RunResult{}, truncateErr
			}
		}
		return RunResult{Metainfo: meta, Selection: selection, NoTransferNeeded: true, SelectionComplete: true, TorrentComplete: fullSelection(meta, selection)}, nil
	}

	// No wanted pieces can occur for an empty selection of zero-length regular
	// files.  Prepare once so those files still exist on disk.
	if len(selection.WantedPieces()) == 0 {
		prepared, prepareErr := plan.Prepare(storage.Overwrite)
		if prepareErr == nil {
			prepareErr = prepared.Close()
		}
		if prepareErr != nil {
			return RunResult{}, prepareErr
		}
		return RunResult{Metainfo: meta, Selection: selection, SelectionComplete: true, TorrentComplete: fullSelection(meta, selection)}, nil
	}

	run.phase("transfer")
	if err := run.startTransferPhase(ctx, source, meta, selection, plan, resumeResult); err != nil {
		return RunResult{}, err
	}
	return RunResult{Metainfo: meta, Selection: selection, SelectionComplete: true, TorrentComplete: fullSelection(meta, selection)}, nil
}

func magnetSelection(source torrent.Source) []torrent.IndexRange {
	if source.Magnet == nil {
		return nil
	}
	return append([]torrent.IndexRange(nil), source.Magnet.Selection...)
}

func fullSelection(meta torrent.Metainfo, selection *torrent.SelectionPlan) bool {
	if selection == nil {
		return false
	}
	selected := make(map[int]struct{}, len(selection.SelectedFiles()))
	for _, file := range selection.SelectedFiles() {
		selected[file.Index] = struct{}{}
	}
	for _, file := range meta.Files {
		if file.Kind != torrent.RegularFile {
			continue
		}
		if _, ok := selected[file.Index]; !ok {
			return false
		}
	}
	return true
}

func realTorrentBytes(meta torrent.Metainfo) int64 {
	var total int64
	for _, file := range meta.Files {
		if file.Kind == torrent.RegularFile {
			total += file.Range.End - file.Range.Begin
		}
	}
	return total
}

func realPieceBytes(mapping torrent.PiecePlan) int64 {
	var total int64
	// Only selected ranges are retained in final output. A mixed piece may
	// contain unwanted regular bytes in Data, but those bytes remain left after
	// the staged piece is removed.
	for _, span := range mapping.Selected {
		total += span.Range.End - span.Range.Begin
	}
	return total
}

type coordinator struct {
	config         RunConfig
	identity       tracker.Identity
	verifiedOutput *atomic.Bool

	set         *tracker.TrackerSet
	ownSet      bool
	updates     <-chan tracker.Update
	updateQueue *trackerPeerUpdateQueue
	account     *tracker.Accounting

	metadataMode      atomic.Bool
	backoff           *peer.EndpointBackoff
	poolMu            sync.Mutex
	pool              *peer.CandidatePool
	metadataEndpoints []peer.ResolvedCandidate
	strikes           map[peer.Endpoint]int
	overflow          atomic.Bool
}

func (c *coordinator) phase(name string) {
	if c != nil && c.config.OnPhase != nil {
		c.config.OnPhase(name)
	}
}

func (c *coordinator) makeSet(trackers []string, infoHash torrent.InfoHash, metadata bool) error {
	normalized, err := torrent.TrackersWithDefault(trackers)
	if err != nil {
		return fmt.Errorf("%w: trackers: %v", ErrRunConfig, err)
	}
	c.metadataMode.Store(metadata)
	set, err := tracker.NewTrackerSet(tracker.TrackerSetConfig{
		InfoHash: [20]byte(infoHash),
		Trackers: normalized,
		Identity: c.identity,
		Random:   c.config.Random,
		HTTP:     c.config.HTTP,
		UDP:      c.config.UDP,
		Clock:    c.config.TrackerClock,
		Snapshot: func(ctx context.Context) (tracker.Snapshot, error) {
			if c.metadataMode.Load() || c.account == nil {
				return tracker.Snapshot{Metadata: true}, nil
			}
			return c.account.Snapshot(ctx, false)
		},
		NeedPeers: func() bool {
			c.poolMu.Lock()
			pool := c.pool
			c.poolMu.Unlock()
			return pool == nil || pool.Len() == 0
		},
		OnUpdate: c.enqueueUpdate,
	})
	if err != nil {
		return fmt.Errorf("%w: tracker set: %v", ErrRunConfig, err)
	}
	c.set = set
	return nil
}

func (c *coordinator) enqueueUpdate(update tracker.Update) {
	if c == nil || c.updateQueue == nil {
		return
	}
	if err := c.updateQueue.enqueue(update); err != nil {
		c.overflow.Store(true)
	}
}

func (c *coordinator) closeSet() error {
	if c == nil || c.set == nil || !c.ownSet {
		return nil
	}
	return c.set.CloseResources()
}

func (c *coordinator) switchAccounting(meta torrent.Metainfo) error {
	account, err := tracker.NewAccounting(realTorrentBytes(meta))
	if err != nil {
		return err
	}
	c.account = account
	c.metadataMode.Store(false)
	return nil
}

func (c *coordinator) discover(ctx context.Context, source torrent.Source) (torrent.Metainfo, error) {
	if c.set == nil {
		if err := c.makeSet(source.Trackers, source.InfoHash, true); err != nil {
			return torrent.Metainfo{}, err
		}
	}
	p, err := peer.NewCandidatePool(peer.CandidatePoolConfig{Resolver: c.config.Resolver})
	if err != nil {
		return torrent.Metainfo{}, err
	}
	c.poolMu.Lock()
	c.pool = p
	c.poolMu.Unlock()
	local := peer.Handshake{InfoHash: [20]byte(source.InfoHash), PeerID: c.identity.PeerID}
	local.Reserved[5] |= 0x10
	local.Reserved[7] |= peer.FastExtensionBit
	strikes := make(map[peer.Endpoint]int)
	metadata, err := DiscoverMetadata(ctx, MetadataConfig{
		InfoHash: source.InfoHash, Trackers: source.Trackers,
		Peers: peersForSource(source), TrackerSet: c.set, Updates: c.updates,
		updateQueue: c.updateQueue, candidatePool: p,
		Identity: c.identity, HTTP: c.config.HTTP, UDP: c.config.UDP,
		TrackerClock: c.config.TrackerClock, Resolver: c.config.Resolver,
		TCPDial: c.tcpDial(), UTPDial: c.utpDial(), Clock: c.config.RaceClock,
		UTPHeadStart: c.config.UTPHeadStart, Backoff: c.backoff,
		LocalHandshake: local, OnSecondary: c.config.OnSecondary, OnStrike: func(endpoint peer.Endpoint, count uint8) {
			strikes[endpoint] = int(count)
		},
	})
	if err != nil {
		return torrent.Metainfo{}, err
	}
	for _, strike := range metadata.Strikes {
		strikes[strike.Endpoint] = int(strike.Strikes)
	}
	c.strikes = strikes
	c.metadataEndpoints = metadata.Endpoints
	c.poolMu.Lock()
	c.pool = p
	c.poolMu.Unlock()
	return metadata.Metainfo, nil
}

func peersForSource(source torrent.Source) []torrent.PeerAddress {
	if source.Magnet == nil {
		return nil
	}
	return append([]torrent.PeerAddress(nil), source.Magnet.Peers...)
}

func (c *coordinator) tcpDial() peer.DialFunc {
	if c.config.TCPDial != nil {
		return c.config.TCPDial
	}
	return (&net.Dialer{}).DialContext
}

func (c *coordinator) utpDial() peer.DialFunc {
	if c.config.UTPDial != nil {
		return c.config.UTPDial
	}
	return utp.DialContext
}

func (c *coordinator) startTransferPhase(ctx context.Context, source torrent.Source, meta torrent.Metainfo, selection *torrent.SelectionPlan, output *storage.Plan, resume storage.ResumeResult) error {
	if c.set == nil {
		if err := c.makeSet(meta.Trackers, meta.InfoHash, false); err != nil {
			return err
		}
	}
	if c.account == nil {
		if err := c.switchAccounting(meta); err != nil {
			return err
		}
	}
	c.metadataMode.Store(false)
	pool, err := peer.NewCandidatePool(peer.CandidatePoolConfig{Resolver: c.config.Resolver})
	if err != nil {
		return err
	}
	for _, candidate := range c.metadataEndpoints {
		_, _ = pool.Add(candidate)
	}
	for _, endpoint := range peersForSource(source) {
		c.updateQueue.enqueuePeers(tracker.TransferPhase, []tracker.TrackerPeer{{Host: endpoint.Host, Port: endpoint.Port}})
	}
	c.poolMu.Lock()
	c.pool = pool
	c.poolMu.Unlock()
	local := peer.Handshake{InfoHash: [20]byte(meta.InfoHash), PeerID: c.identity.PeerID}
	local.Reserved[5] |= 0x10
	local.Reserved[7] |= peer.FastExtensionBit
	manager, err := peer.NewDialManager(peer.DialManagerConfig{Race: peer.RaceConfig{
		LocalHandshake: local,
		TCPDial:        c.tcpDial(), UTPDial: c.utpDial(), Clock: c.config.RaceClock,
		UTPHeadStart: c.config.UTPHeadStart,
	}, Backoff: c.backoff})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	trackerRun, err := c.set.Start(runCtx, tracker.TransferPhase)
	if err != nil {
		return err
	}
	admission := newTrackerPeerResolver(runCtx, c.config.Resolver, c.updateQueue)
	defer func() {
		admission.close()
		c.updateQueue.clear()
	}()
	var liveMu sync.Mutex
	live := make(map[net.Conn]*peer.LivePeer)
	var candidateCursorMu sync.Mutex
	candidateCursor := 0
	acquire := func(acquireCtx context.Context) (ConnectedPeer, error) {
		for {
			if c.overflow.Load() {
				return ConnectedPeer{}, errors.New("session tracker event queue is full")
			}
			admission.pump(acquireCtx, tracker.TransferPhase, pool)
			select {
			case <-c.updateQueue.notify:
				candidateCursorMu.Lock()
				candidateCursor = 0
				candidateCursorMu.Unlock()
			default:
			}
			snapshot := pool.Snapshot()
			candidateCursorMu.Lock()
			start := 0
			if len(snapshot) > 0 {
				start = candidateCursor % len(snapshot)
				candidateCursor = (start + trackerAdmissionBatch) % len(snapshot)
			}
			candidateCursorMu.Unlock()
			checked := len(snapshot)
			if checked > trackerAdmissionBatch {
				checked = trackerAdmissionBatch
			}
			for i := 0; i < checked; i++ {
				candidate := snapshot[(start+i)%len(snapshot)]
				if !c.backoff.Ready(candidate.Endpoint, time.Now()) {
					continue
				}
				dialCtx, dialCancel := context.WithTimeout(acquireCtx, trackerEndpointRaceTimeout)
				admitted, dialErr := manager.Dial(dialCtx, candidate)
				dialCancel()
				if dialErr != nil {
					break
				}
				liveMu.Lock()
				live[admitted.Conn] = admitted
				liveMu.Unlock()
				return ConnectedPeer{ID: string(admitted.ID[:]), Endpoint: admitted.Endpoint, Conn: admitted.Conn, Handshake: admitted.Handshake}, nil
			}
			select {
			case <-acquireCtx.Done():
				return ConnectedPeer{}, acquireCtx.Err()
			case update, ok := <-c.updates:
				if !ok {
					return ConnectedPeer{}, ErrNoPeer
				}
				if enqueueErr := c.updateQueue.enqueue(update); enqueueErr != nil {
					c.overflow.Store(true)
					return ConnectedPeer{}, errors.New("session tracker event queue is full")
				}
				candidateCursorMu.Lock()
				candidateCursor = 0
				candidateCursorMu.Unlock()
			case <-c.updateQueue.notify:
				candidateCursorMu.Lock()
				candidateCursor = 0
				candidateCursorMu.Unlock()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	release := func(input ConnectedPeer) {
		liveMu.Lock()
		admitted := live[input.Conn]
		delete(live, input.Conn)
		liveMu.Unlock()
		if admitted != nil {
			_ = manager.Release(admitted)
		} else if input.Conn != nil {
			_ = input.Conn.Close()
		}
	}
	initialStrikes := make(map[peer.Endpoint]int, len(c.strikes))
	for endpoint, count := range c.strikes {
		initialStrikes[endpoint] = count
		if count >= 3 {
			c.backoff.Blacklist(endpoint)
		}
	}
	verifiedSelected := int64(0)
	rate := newPayloadRate()
	prepareMode := storage.Overwrite
	if c.config.Resume {
		prepareMode = storage.Resume
	}
	for _, span := range resume.VerifiedRanges {
		verifiedSelected += span.Range.End - span.Range.Begin
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: output, Stager: storage.NewStager(storage.StagerConfig{
			CacheRoot: c.config.CacheRoot,
			OpenFile:  c.config.StageFileOpener,
		}),
		PrepareMode:     prepareMode,
		SchedulerConfig: Config{Streaming: c.config.Streaming}, LocalHandshake: local,
		PieceCount: uint32(len(meta.Pieces)), PieceLength: uint32(meta.PieceLength),
		LastPieceLength: uint32(meta.Pieces[len(meta.Pieces)-1].Range.End - meta.Pieces[len(meta.Pieces)-1].Range.Begin),
		AcquirePeer:     acquire, ReleasePeer: release, InitialStrikes: initialStrikes,
		OnEndpointBlacklisted: c.backoff.Blacklist,
		ResumeComplete:        resume.VerifiedPieces,
		OnPieceVerified: func(piece PieceVerified) {
			if piece.SelectedBytes > 0 {
				c.verifiedOutput.Store(true)
			}
			if mapping, ok := selection.Piece(piece.PieceIndex); ok {
				_ = c.account.AddRetained(realPieceBytes(mapping))
			}
			verifiedSelected += piece.SelectedBytes
			if c.config.OnProgress != nil {
				liveMu.Lock()
				activePeers := len(live)
				liveMu.Unlock()
				progress := RunProgress{VerifiedSelectedBytes: verifiedSelected, SelectedBytes: runSelectedBytes(selection), ActivePeers: activePeers, RecentRateBytesPerSec: rate.perSecond(time.Now())}
				c.config.OnProgress(progress)
			}
		},
		OnPayloadReceived: func(n int64) error {
			rate.add(time.Now(), n)
			return c.account.AddReceived(n)
		},
		BeforePeerShutdown: func() error {
			// Transfer invokes this after scheduling and admission stop, but
			// before closing peer workers. Join regular tracker loops here so
			// terminal events cannot race peer cleanup or begin another announce.
			trackerRun.Wait()
			return nil
		},
	})
	if err != nil {
		_ = trackerRun.Finalize(context.Background(), false)
		return err
	}
	transferCtx := runCtx
	var stopTimeout func()
	var timeoutFired atomic.Bool
	if c.config.Timeout > 0 {
		var timerMu sync.Mutex
		timer := time.NewTimer(c.config.Timeout)
		timeoutDone := make(chan struct{})
		timeoutCtx, timeoutCancel := context.WithCancel(runCtx)
		transferCtx = timeoutCtx
		stopTimeout = func() {
			timeoutCancel()
			close(timeoutDone)
			timerMu.Lock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerMu.Unlock()
		}
		go func() {
			select {
			case <-timer.C:
				timeoutFired.Store(true)
				timeoutCancel()
			case <-timeoutDone:
			case <-runCtx.Done():
			}
		}()
		oldProgress := c.config.OnProgress
		c.config.OnProgress = func(progress RunProgress) {
			timerMu.Lock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(c.config.Timeout)
			timerMu.Unlock()
			if oldProgress != nil {
				oldProgress(progress)
			}
		}
	}
	transferDone := make(chan error, 1)
	go func() { transferDone <- transfer.Run(transferCtx) }()
	var transferErr error
	select {
	case transferErr = <-transferDone:
	case <-ctx.Done():
		trackerRun.Wait()
		transferErr = <-transferDone
	case <-transferCtx.Done():
		trackerRun.Wait()
		transferErr = <-transferDone
	}
	if stopTimeout != nil {
		stopTimeout()
	}
	err = transferErr
	if errors.Is(err, context.Canceled) && ctx.Err() == nil && timeoutFired.Load() {
		err = fmt.Errorf("%w: %s", ErrNoProgressTimeout, c.config.Timeout)
	}
	if err == nil {
		for _, pending := range resume.PendingTruncations {
			if truncateErr := output.TruncateSelected(pending.Index, pending.Size); truncateErr != nil {
				err = truncateErr
				break
			}
		}
	}
	full := err == nil && fullSelection(meta, selection)
	// Transfer.Run has joined every peer and finalizer worker.  Cancel and join
	// regular tracker loops before entering the one-shot terminal sequence so a
	// normal announce cannot race a stopped event.
	trackerRun.Wait()
	if finalErr := trackerRun.Finalize(context.Background(), full); finalErr != nil && c.config.OnSecondary != nil {
		c.config.OnSecondary(finalErr)
	}
	if err != nil {
		return err
	}
	return nil
}

type payloadRateBucket struct {
	second int64
	bytes  uint64
}

// payloadRate keeps a bounded rolling window for interactive status.
type payloadRate struct {
	buckets [5]payloadRateBucket
}

func newPayloadRate() *payloadRate { return &payloadRate{} }

func (r *payloadRate) add(now time.Time, n int64) {
	if r == nil || n <= 0 {
		return
	}
	second := now.Unix()
	index := second % int64(len(r.buckets))
	if index < 0 {
		index += int64(len(r.buckets))
	}
	bucket := &r.buckets[index]
	if bucket.second != second {
		bucket.second = second
		bucket.bytes = 0
	}
	if uint64(n) <= ^uint64(0)-bucket.bytes {
		bucket.bytes += uint64(n)
	}
}

func (r *payloadRate) perSecond(now time.Time) uint64 {
	if r == nil {
		return 0
	}
	current := now.Unix()
	var total uint64
	for i := range r.buckets {
		bucket := r.buckets[i]
		if bucket.second <= current && current-bucket.second < int64(len(r.buckets)) {
			if bucket.bytes > ^uint64(0)-total {
				return ^uint64(0) / uint64(len(r.buckets))
			}
			total += bucket.bytes
		}
	}
	return total / uint64(len(r.buckets))
}

func runSelectedBytes(selection *torrent.SelectionPlan) int64 {
	var total int64
	for _, file := range selection.SelectedFiles() {
		total += file.Range.End - file.Range.Begin
	}
	return total
}
