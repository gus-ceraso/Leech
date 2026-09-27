# Session guidance

Read [DESIGN §6](../../DESIGN.md#6-session-phases-and-ownership),
[§13](../../DESIGN.md#13-piece-scheduling-and-verification), and
[§15](../../DESIGN.md#15-concurrency-and-lifecycle). `run.go` owns phase order,
`metadata.go` owns metadata discovery/admission, `scheduler.go` owns pure
piece/request state, and `transfer.go` coordinates peer I/O and finalization.

## Ownership and handoffs

- Carry one run identity, endpoint-attempt budget, strikes, and blacklists across
  phases. Share endpoint state with the dial manager and propagate transfer
  blacklists back to it; see [peer guidance](../peer/AGENTS.md).
- No network worker crosses the metadata-to-resume boundary. Admission/DNS
  workers also belong to phase shutdown and must join before final tracker
  events and normalization. Follow §15's complete shutdown order, not merely
  eventual joining before return.
- During transfer shutdown, join peers and the finalizer, close cache resources,
  and remove this run's workspace before final tracker events. Announcements do
  not need the workspace; early cleanup avoids retaining it through that wait.
- A metadata candidate has one supplier. Verify its complete hash and canonical
  encoding before normalization; retain no network worker in the result. Keep
  invalid complete metadata, severe messages, and ordinary refusal/timeouts
  distinct when assigning penalties.
- Metadata keeps one request outstanding. Only matching valid data renews its
  inactivity deadline; other messages remain bounded without extending the wait.
  Match terminal replies before classifying a reject as ordinary refusal.
- Metadata discovery uses the approved Fast exception in
  [DESIGN §7.4](../../DESIGN.md#74-metadata-acquisition): extension handshake first,
  no initial availability or file-request replies, and structural framing without
  Fast-negotiation enforcement. Do not apply transfer-phase Fast state here.
- `TransferConfig.PrepareMode` defaults to `storage.Overwrite`. A partial resume
  must explicitly pass `storage.Resume` or verified output will be truncated.
  Preserve verified pieces and pending truncations from
  [storage](../storage/AGENTS.md); see [DESIGN §9](../../DESIGN.md#9-resume-behavior).
- Tracker-update queues retain peer data, not just events. Bound their entries
  and bytes and preserve source fairness before candidate admission. Keep DNS
  work off the coordinator, with bounded workers and per-lookup deadlines, so
  already-admitted peers remain schedulable.
- Acquisition retries only `ErrNoPeer` or errors reporting `Temporary() == true`;
  other errors terminate transfer. Advance the candidate cursor only for examined
  candidates, preserving it across tracker notifications.
- `RunConfig.OnDiagnostic` is optional and separate from progress, warnings, and
  secondary failures. Its exact value shape is
  `Diagnostic{Kind DiagnosticKind, Phase string, Endpoint DiagnosticEndpoint,
  Peer peer.Endpoint, Count, IPv4Count, IPv6Count uint64,
  Duration time.Duration, Detail string}`;
  `DiagnosticEndpoint` is `{Scheme, Host string}` and peer endpoint is the fixed
  `netip.Addr` plus `uint16` port (`peer.Endpoint`), zero when not applicable
  or when the address has a zone.
  Kinds are constants; phase, tracker, metadata, peer-selection, lifecycle,
  transfer, and transport-race observations are emitted by their owning state
  transitions. CLI-retained Phase/Detail/tracker text totals at most 4096
  bytes per record; a numeric peer renders to at most 47 bytes. CLI queue holds
  128 records. Producers must return promptly. Tracker endpoints contain
  scheme/host only; never put URLs, errors, payload, or magnet sources in
  diagnostics. Tracker updates carry attempt/transmission/activation results and
  compact IPv4/IPv6 counts from response decoding; final attempts use the same
  callback after normal admission stops. The coordinator coalesces unchanged
  regular failures and reports recovery through `OnWarning`; pending warning
  transitions retain at most three per tracker (192 total) and drain from the
  session admission pump, including after normal loops join. Final one-shot
  failures remain secondary and never claim a retry. Tracker callbacks only
  enqueue bounded warning state and debug observations; they run after protocol
  transactions and outside coordinator/protocol locks. The CLI drops debug
  records on pressure. Transfer emits `DiagnosticTransfer` transitions from the
  coordinator at choke, availability, Allowed Fast, useful-block, tombstone,
  scheduling, and finalizer state effects. Initial empty availability is observed
  at admission; unchanged availability is silent. Request assignments and newly
  received Allowed Fast grants accumulate in saturating per-peer counters and are
  emitted at most once per peer per existing replacement tick (also flushed on
  retirement or shutdown); duplicate grants do not increment them. Keep event
  details summarized, with no per-block logging. `DiagnosticTransportRace`
  records the actual handshake winner or a failed race, including metadata phase,
  without retaining raw errors. `RaceWithResult` distinguishes a started race
  from errors during admission, budget checks, or slot waits. `DialWithResult`
  carries the winner through collision rejection while its closed connection
  remains unowned by session.
- The coordinator alone mutates rarity, request ownership, provenance, strikes,
  and completion. Give the finalizer immutable coverage snapshots. Worker
  callbacks and command enqueueing must not stall coordination indefinitely.
  Transfer status snapshots run on the transfer coordinator's existing
  replacement tick; it supplies admitted live-peer counts. The session owner
  composes verified bytes and its five-second payload-rate window there. Keep
  `OnProgress` commit-only: status observations never reset the no-progress
  timer. Callbacks must return promptly.
- Retire disconnected peers after a drive pass, once their workers have joined
  and no event index or iteration is in use. Clear removed slice references;
  keep endpoint penalties separately.

## Scheduling and results

- Apply request, queue, staged-count, and staged-byte budgets together. Bound
  scheduler work as well as peer-state updates: small unchanged events must not
  force full wanted-piece scans. Request assignment visits admitted stages;
  reservation eligibility caches must survive unrelated peers' sparse changes.
  Re-rank fitting reservations by current rarity after stage credits change.
- Reserve stages only with request capacity. Under staging pressure, abort
  stages that current peers cannot advance before releasing their credits.
  Preserve useful partial stages, active requests, and pending verification.
- Coalesce contiguous data across file boundaries, stopping at padding and the
  block limit. Keep pending positions, active block lookup, and remaining-work
  counters consistent when requests settle, peers leave, or pieces reset.
- Apply queued extension handshakes before assigning more blocks, including
  repeated `reqq` changes and zero capacity. Keep endgame winner/cancel handling
  consistent with peer terminal-response and tombstone obligations.
- Request expiry and peer replacement use recent useful activity, not a lifetime
  productive flag. Ordinary stalls do not earn strikes. A failed piece gives
  one strike per distinct contributor; three strikes blacklist. Invalid complete
  metadata strikes its sole supplier; severe violations blacklist immediately.
  After hash-mismatch accounting, propagate any recorded `Stager.Fatal()` with
  the scheduler result so storage failure cannot be treated as retryable peer
  corruption.
- Count every received file-payload byte for tracker accounting, including
  discarded data. Draining queued events can complete the download; recheck
  completion before scheduling again. Advance completion and the no-progress
  timer only after a newly verified file piece commits. Whole-torrent `left`
  differs from selected progress; see [tracker guidance](../tracker/AGENTS.md).
  When classifying the session-owned no-progress timeout, retrieve the stager's
  recorded close result and report it through optional `OnSecondary`; leave
  ordinary cancellation and final tracker errors on their existing paths.
- Preserve primary errors. Cleanup failure replaces success, while final tracker
  event failures remain secondary. Honor committed-output reporting even when
  later staged-file removal fails.

Integration checks should assert wire behavior, tracker order, filesystem state,
and joined workers. Reuse injected dependencies and event barriers rather than
increasing sleeps to hide fixture races.
