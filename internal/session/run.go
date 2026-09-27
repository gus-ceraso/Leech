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
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
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

type timeoutTimer interface {
	channel() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type systemTimeoutTimer struct{ timer *time.Timer }

func (t systemTimeoutTimer) channel() <-chan time.Time  { return t.timer.C }
func (t systemTimeoutTimer) Stop() bool                 { return t.timer.Stop() }
func (t systemTimeoutTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }

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
	backoff    *peer.EndpointBackoff

	OnPhase func(string)
	// OnPhaseStatus follows OnPhase for metadata, resume, and transfer entry.
	// It is separate from OnProgress, which reports committed pieces only.
	OnPhaseStatus func(string, RunProgress)
	OnProgress    func(RunProgress)
	OnStatus      func(RunProgress)
	// Now controls transfer and payload-rate timestamps for deterministic callers.
	Now             func() time.Time
	newTimeoutTimer func(time.Duration) timeoutTimer
	OnWarning       func(string)
	OnSecondary     func(error)
	// OnDiagnostic receives optional bounded read-only observations. Callbacks
	// must return promptly; they run synchronously at session ownership points.
	OnDiagnostic func(Diagnostic)
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

// RunProgress is the immutable snapshot used by commit and live-status callbacks.
type RunProgress struct {
	VerifiedSelectedBytes int64
	SelectedBytes         int64
	ActivePeers           int
	RecentRateBytesPerSec uint64
}

// DiagnosticKind identifies a bounded, read-only observation. Diagnostics are
// separate from progress, warnings, and secondary failures.
type DiagnosticKind uint8

const (
	DiagnosticPhaseTransition DiagnosticKind = iota + 1
	DiagnosticTrackerAttempt
	DiagnosticMetadataRefusal
	DiagnosticPeerSelection
	DiagnosticLifecycle
	DiagnosticTransfer
	DiagnosticTransportRace
)

// DiagnosticEndpoint contains only tracker scheme and host. Producers must not
// place userinfo, path, query, or fragment data here.
type DiagnosticEndpoint struct {
	Scheme string
	Host   string
}

// Diagnostic is a fixed-field observation. Peer is a resolved numeric endpoint
// (netip.Addr and uint16 port); it is zero when not peer-specific or when the
// address has a zone.
// Text fields retained by a CLI observer are bounded to 4096 aggregate bytes
// per record; queues are bounded independently by the observer. Counts and
// durations remain typed values.
type Diagnostic struct {
	Kind      DiagnosticKind
	Phase     string
	Endpoint  DiagnosticEndpoint
	Peer      peer.Endpoint
	Count     uint64
	IPv4Count uint64
	IPv6Count uint64
	Duration  time.Duration
	Detail    string
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

	backoff := config.backoff
	if backoff == nil {
		backoff = peer.NewEndpointBackoff()
	}
	run := &coordinator{config: config, identity: identity, backoff: backoff, verifiedOutput: &verifiedOutput}
	run.updateQueue = newTrackerPeerUpdateQueue()
	run.ownSet = config.TrackerSet == nil
	defer func() {
		run.diagnostic(Diagnostic{Kind: DiagnosticLifecycle, Detail: "shutdown finalization"})
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
			run.activePhase("metadata", RunProgress{})
			metadata, discoverErr := run.discover(ctx, source)
			if discoverErr != nil {
				run.phaseExit("metadata", discoverErr)
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
			run.activePhase("metadata", RunProgress{})
			metadata, discoverErr := run.discover(ctx, source)
			if discoverErr != nil {
				run.phaseExit("metadata", discoverErr)
				return RunResult{}, discoverErr
			}
			meta = metadata
		}
	default:
		return RunResult{}, fmt.Errorf("%w: unknown source kind", ErrRunConfig)
	}
	if source.Kind == torrent.SourceMagnet || source.Kind == torrent.SourceInfoHash {
		run.phaseExit("metadata", nil)
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
		run.phaseExit("selection", err)
		return RunResult{}, err
	}
	run.phaseExit("selection", nil)

	resumeResult := storage.ResumeResult{}
	if config.Resume {
		run.activePhase("resume", RunProgress{SelectedBytes: runSelectedBytes(selection)})
		resumeResult, err = storage.ScanResume(ctx, selection, plan)
		if len(resumeResult.VerifiedPieces) > 0 {
			verifiedOutput.Store(true)
		}
		if err != nil {
			run.phaseExit("resume", err)
			return RunResult{}, err
		}
		run.phaseExit("resume", nil)
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

	verifiedSelected := int64(0)
	for _, span := range resumeResult.VerifiedRanges {
		verifiedSelected += span.Range.End - span.Range.Begin
	}
	run.activePhase("transfer", RunProgress{VerifiedSelectedBytes: verifiedSelected, SelectedBytes: runSelectedBytes(selection)})
	if err := run.startTransferPhase(ctx, source, meta, selection, plan, resumeResult); err != nil {
		run.phaseExit("transfer", err)
		return RunResult{}, err
	}
	run.phaseExit("transfer", nil)
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
	trackerDiagMu     sync.Mutex
	trackerFailed     map[string]bool
	pendingWarnings   []trackerWarningEvent
	overflow          atomic.Bool
}

func (c *coordinator) phase(name string) {
	if c == nil {
		return
	}
	c.diagnostic(Diagnostic{Kind: DiagnosticPhaseTransition, Phase: name})
	if c.config.OnPhase != nil {
		c.config.OnPhase(name)
	}
}

func (c *coordinator) phaseExit(name string, err error) {
	detail := "phase exited successfully"
	if err != nil {
		detail = "phase exited with failure"
	}
	c.diagnostic(Diagnostic{Kind: DiagnosticLifecycle, Phase: name, Detail: detail})
}

func (c *coordinator) diagnostic(event Diagnostic) {
	if c != nil && c.config.OnDiagnostic != nil {
		c.config.OnDiagnostic(event)
	}
}

func (c *coordinator) activePhase(name string, progress RunProgress) {
	c.phase(name)
	if c.config.OnPhaseStatus != nil {
		c.config.OnPhaseStatus(name, progress)
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
	if c == nil {
		return
	}
	c.observeTracker(update)
	if c.updateQueue == nil {
		return
	}
	if err := c.updateQueue.enqueue(update); err != nil {
		c.overflow.Store(true)
		c.updateQueue.signal()
	}
}

func (c *coordinator) observeTracker(update tracker.Update) {
	endpoint := diagnosticTrackerEndpoint(update.Tracker)
	event := Diagnostic{Kind: DiagnosticTrackerAttempt, Phase: trackerPhaseName(update.Phase), Endpoint: endpoint,
		Count: uint64(len(update.Peers)), IPv4Count: update.IPv4Compact, IPv6Count: update.IPv6Compact}
	finalEvent := update.Request.Event == tracker.EventCompleted || update.Request.Event == tracker.EventStopped
	if update.Attempted {
		event.Detail = trackerEventName(update.Request.Event) + " attempted"
		if update.Transmitted {
			event.Detail += ", transmitted"
		} else {
			event.Detail += ", not transmitted"
		}
		if update.Activated {
			event.Detail += ", response accepted"
		} else if update.Err != nil {
			event.Detail += ", response failed"
			if finalEvent {
				event.Detail += ", final event"
			} else if errors.Is(update.Err, context.Canceled) {
				event.Detail += ", canceled"
			} else if update.Disabled {
				event.Detail += ", disabled"
			} else {
				event.Detail += ", retrying"
			}
			event.Detail += ": " + trackerFailureDetail(update.Err)
		}
		c.diagnostic(event)
	}
	if finalEvent || !update.Attempted || c.config.OnWarning == nil || errors.Is(update.Err, context.Canceled) {
		return
	}
	c.trackerDiagMu.Lock()
	if c.trackerFailed == nil {
		c.trackerFailed = make(map[string]bool)
	}
	wasFailed := c.trackerFailed[update.Tracker]
	if update.Err == nil {
		if wasFailed {
			delete(c.trackerFailed, update.Tracker)
			c.queueTrackerWarningLocked(update.Tracker, false, false)
		}
	} else if !wasFailed {
		c.trackerFailed[update.Tracker] = true
		c.queueTrackerWarningLocked(update.Tracker, true, update.Disabled)
	}
	c.trackerDiagMu.Unlock()
}

type trackerWarningEvent struct {
	tracker  string
	failure  bool
	disabled bool
}

func (c *coordinator) queueTrackerWarningLocked(trackerURL string, failure, disabled bool) {
	count, last := 0, -1
	for i := range c.pendingWarnings {
		if c.pendingWarnings[i].tracker == trackerURL {
			count++
			last = i
		}
	}
	if last >= 0 && c.pendingWarnings[last].failure == failure {
		if failure {
			c.pendingWarnings[last].disabled = disabled
		}
		return
	}
	if count >= 3 {
		c.pendingWarnings[last] = trackerWarningEvent{tracker: trackerURL, failure: failure, disabled: disabled}
		return
	}
	const maxPendingWarnings = 3 * limits.Trackers
	if len(c.pendingWarnings) < maxPendingWarnings {
		c.pendingWarnings = append(c.pendingWarnings, trackerWarningEvent{tracker: trackerURL, failure: failure, disabled: disabled})
	}
}

func (c *coordinator) drainTrackerWarnings() {
	if c == nil || c.config.OnWarning == nil {
		return
	}
	for {
		c.trackerDiagMu.Lock()
		if len(c.pendingWarnings) == 0 {
			c.trackerDiagMu.Unlock()
			return
		}
		event := c.pendingWarnings[0]
		copy(c.pendingWarnings, c.pendingWarnings[1:])
		c.pendingWarnings[len(c.pendingWarnings)-1] = trackerWarningEvent{}
		c.pendingWarnings = c.pendingWarnings[:len(c.pendingWarnings)-1]
		c.trackerDiagMu.Unlock()
		endpoint := diagnosticTrackerEndpoint(event.tracker)
		detail := "recovered"
		if event.failure {
			detail = "failure; retrying"
			if event.disabled {
				detail = "failure; disabled"
			}
		}
		c.config.OnWarning(boundedTrackerWarning(endpoint, detail))
	}
}

func boundedTrackerWarning(endpoint DiagnosticEndpoint, detail string) string {
	const maxBytes = 4096
	prefix, schemeSep, suffix := "tracker ", "://", " "+detail
	remaining := maxBytes - len(prefix) - len(schemeSep) - len(suffix)
	if remaining < 0 {
		remaining = 0
	}
	scheme := endpoint.Scheme
	if len(scheme) > remaining {
		scheme = scheme[:remaining]
	}
	remaining -= len(scheme)
	host := endpoint.Host
	if len(host) > remaining {
		host = host[:remaining]
	}
	return prefix + scheme + schemeSep + host + suffix
}

func trackerFailureDetail(err error) string {
	var httpErr *tracker.HTTPError
	if errors.As(err, &httpErr) {
		return "HTTP tracker transaction failed"
	}
	var udpErr *tracker.Error
	if errors.As(err, &udpErr) {
		return "UDP tracker transaction failed"
	}
	return "transaction failed"
}

func transportRaceDiagnostic(phase string, endpoint peer.Endpoint, result peer.HandshakeResult, err error) Diagnostic {
	detail := "failed"
	if errors.Is(err, peer.ErrPeerIDCollision) {
		detail = "peer ID collision; older connection retained; winner=" + result.Transport.String()
	} else if result.Conn != nil {
		detail = "winner=" + result.Transport.String()
		if err != nil {
			detail = "peer admission failed; " + detail
		}
	}
	return Diagnostic{Kind: DiagnosticTransportRace, Phase: phase, Peer: endpoint, Detail: detail}
}

func diagnosticTrackerEndpoint(raw string) DiagnosticEndpoint {
	u, err := url.Parse(raw)
	if err != nil {
		return DiagnosticEndpoint{}
	}
	return DiagnosticEndpoint{Scheme: u.Scheme, Host: u.Host}
}

func trackerPhaseName(phase tracker.Phase) string {
	if phase == tracker.MetadataPhase {
		return "metadata"
	}
	return "transfer"
}

func trackerEventName(event tracker.Event) string {
	switch event {
	case tracker.EventStarted:
		return "started"
	case tracker.EventCompleted:
		return "completed"
	case tracker.EventStopped:
		return "stopped"
	default:
		return "regular"
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
		LocalHandshake: local, OnSecondary: c.config.OnSecondary, OnDiagnostic: c.config.OnDiagnostic, onTrackerPump: c.drainTrackerWarnings, OnStrike: func(endpoint peer.Endpoint, count uint8) {
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
		_, _ = pool.AddFrom(candidate.Source, candidate)
	}
	for _, endpoint := range peersForSource(source) {
		c.updateQueue.enqueuePeersFrom(magnetPeerSource, tracker.TransferPhase, []tracker.TrackerPeer{{Host: endpoint.Host, Port: endpoint.Port}})
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
	admission := newTrackerPeerResolver(runCtx, c.config.Resolver, c.updateQueue, c.config.OnDiagnostic)
	admission.onPump = c.drainTrackerWarnings
	defer c.updateQueue.clear()
	quiesceAdmission := func() {
		cancel()
		trackerRun.Wait()
		admission.close()
		c.updateQueue.clear()
		c.drainTrackerWarnings()
	}
	var liveMu sync.Mutex
	live := make(map[net.Conn]*peer.LivePeer)
	candidateCursor := 0
	acquire := func(acquireCtx context.Context) (ConnectedPeer, error) {
		for {
			if c.overflow.Load() {
				return ConnectedPeer{}, ErrTrackerUpdateQueue
			}
			admission.pump(acquireCtx, tracker.TransferPhase, pool)
			snapshot := pool.Snapshot()
			checked := len(snapshot)
			if checked > trackerAdmissionBatch {
				checked = trackerAdmissionBatch
			}
			for i := 0; i < checked; i++ {
				if err := acquireCtx.Err(); err != nil {
					return ConnectedPeer{}, err
				}
				index := candidateCursor % len(snapshot)
				candidate := snapshot[index]
				candidateCursor = (index + 1) % len(snapshot)
				if !c.backoff.Ready(candidate.Endpoint, time.Now()) {
					c.diagnostic(Diagnostic{Kind: DiagnosticPeerSelection, Phase: "transfer", Peer: candidate.Endpoint, Detail: "candidate delayed by endpoint backoff"})
					continue
				}
				c.diagnostic(Diagnostic{Kind: DiagnosticPeerSelection, Phase: "transfer", Peer: candidate.Endpoint, Detail: "candidate selected for dial"})
				dialCtx, dialCancel := context.WithTimeout(acquireCtx, trackerEndpointRaceTimeout)
				admitted, raceResult, dialErr := manager.DialWithResult(dialCtx, candidate)
				dialCancel()
				if dialErr != nil {
					c.diagnostic(transportRaceDiagnostic("transfer", candidate.Endpoint, raceResult, dialErr))
					c.diagnostic(Diagnostic{Kind: DiagnosticPeerSelection, Phase: "transfer", Peer: candidate.Endpoint, Detail: "candidate dial failed"})
					var budgetErr *peer.EndpointBudgetError
					if errors.As(dialErr, &budgetErr) {
						return ConnectedPeer{}, budgetErr
					}
					continue
				}
				c.diagnostic(transportRaceDiagnostic("transfer", admitted.Endpoint, raceResult, nil))
				c.diagnostic(Diagnostic{Kind: DiagnosticPeerSelection, Phase: "transfer", Peer: admitted.Endpoint, Detail: "candidate connected"})
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
					return ConnectedPeer{}, ErrTrackerUpdateQueue
				}
			case <-c.updateQueue.notify:
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
	now := c.config.Now
	if now == nil {
		now = time.Now
	}
	statusSnapshot := func(activePeers int) RunProgress {
		return RunProgress{
			VerifiedSelectedBytes: verifiedSelected,
			SelectedBytes:         runSelectedBytes(selection),
			ActivePeers:           activePeers,
			RecentRateBytesPerSec: rate.perSecond(now()),
		}
	}
	prepareMode := storage.Overwrite
	if c.config.Resume {
		prepareMode = storage.Resume
	}
	for _, span := range resume.VerifiedRanges {
		verifiedSelected += span.Range.End - span.Range.Begin
	}
	stager := storage.NewStager(storage.StagerConfig{
		CacheRoot: c.config.CacheRoot,
		OpenFile:  c.config.StageFileOpener,
	})
	var onStatus func(int)
	if c.config.OnStatus != nil {
		onStatus = func(activePeers int) {
			c.config.OnStatus(statusSnapshot(activePeers))
		}
	}
	transfer, err := NewTransfer(TransferConfig{
		Selection: selection, Output: output, Stager: stager,
		PrepareMode:     prepareMode,
		SchedulerConfig: Config{Streaming: c.config.Streaming}, LocalHandshake: local,
		PieceCount: uint32(len(meta.Pieces)), PieceLength: uint32(meta.PieceLength),
		LastPieceLength: uint32(meta.Pieces[len(meta.Pieces)-1].Range.End - meta.Pieces[len(meta.Pieces)-1].Range.Begin),
		AcquirePeer:     acquire, ReleasePeer: release, InitialStrikes: initialStrikes,
		OnEndpointBlacklisted: c.backoff.Blacklist,
		ResumeComplete:        resume.VerifiedPieces,
		Now:                   now,
		OnStatus:              onStatus,
		OnDiagnostic:          c.config.OnDiagnostic,
		OnPieceVerified: func(piece PieceVerified) {
			if piece.SelectedBytes > 0 {
				c.verifiedOutput.Store(true)
			}
			if mapping, ok := selection.Piece(piece.PieceIndex); ok {
				_ = c.account.AddRetained(realPieceBytes(mapping))
			}
			verifiedSelected += piece.SelectedBytes
			if c.config.OnProgress != nil {
				progress := statusSnapshot(piece.ActivePeers)
				c.config.OnProgress(progress)
			}
		},
		OnPayloadReceived: func(n int64) error {
			rate.add(now(), n)
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
		quiesceAdmission()
		_ = trackerRun.Finalize(context.Background(), false)
		return err
	}
	transferCtx := runCtx
	var stopTimeout func()
	var timeoutFired atomic.Bool
	if c.config.Timeout > 0 {
		var timerMu sync.Mutex
		newTimer := c.config.newTimeoutTimer
		if newTimer == nil {
			newTimer = func(duration time.Duration) timeoutTimer {
				return systemTimeoutTimer{timer: time.NewTimer(duration)}
			}
		}
		timer := newTimer(c.config.Timeout)
		timeoutDone := make(chan struct{})
		timeoutCtx, timeoutCancel := context.WithCancel(runCtx)
		transferCtx = timeoutCtx
		stopTimeout = func() {
			timeoutCancel()
			close(timeoutDone)
			timerMu.Lock()
			if !timer.Stop() {
				select {
				case <-timer.channel():
				default:
				}
			}
			timerMu.Unlock()
		}
		go func() {
			select {
			case <-timer.channel():
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
				case <-timer.channel():
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
		// Transfer.Run has joined its cleanup; idempotent Close retrieves its recorded result.
		if cleanupErr := stager.Close(); cleanupErr != nil && c.config.OnSecondary != nil {
			c.config.OnSecondary(cleanupErr)
		}
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
	// Transfer.Run has joined every peer and finalizer worker. Join admission
	// too, and discard queued peers before the one-shot terminal sequence.
	quiesceAdmission()
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
