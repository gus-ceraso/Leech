# Leech unresolved work

[DESIGN.md](DESIGN.md) is the behavioral specification; this file is the execution
backlog. Read the applicable `AGENTS.md` before changing a package. Preserve the
supported input domain and resource limits while fixing the defects below.

The baseline is commit `0914fee9750d60a828a828493a6bc1af1d826e6b`, reconciled on
2026-09-26. Finding IDs and severities are retained from that review. R-24 is
partially fixed; its remaining scope is explicit below. Fixture recipes here are
self-contained and do not depend on temporary review directories.

## Execution order and ownership

Each task owns its implementation and focused regressions. Dependencies below
include ordering needed for shared production files. Tasks with disjoint files
can proceed independently; a dependency is not a reason to block unrelated work.
Use separate task-specific test files when concurrent tasks share a package.
Expand a task's file scope only after resolving ownership with any affected task.

| Task | Findings or remaining scope | Depends on |
| --- | --- | --- |
| UTP | R-11, R-12 | — |
| SELECT | R-13 | — |
| STORAGE | R-14, R-15, R-16 | — |
| ENDPOINT | R-17 | — |
| ADMISSION | R-18, unresolved part of R-24 | — |
| METADATA | R-19, R-20, R-21 | — |
| CLI | R-27, R-28 | — |
| PEERS | R-23 | ADMISSION |
| SHUTDOWN | R-22 | ADMISSION, METADATA |
| SCHEDULER | R-25, R-26, F-01/SEC4 | PEERS |
| STATUS | R-29 | CLI, SHUTDOWN |
| INPUT-REVIEW | Unfinished offline-input review coverage | SELECT |
| VALIDATE | Integrated acceptance of this backlog | All tasks above |

`ADMISSION → PEERS → SCHEDULER` serializes edits to `transfer.go`.
`ADMISSION + METADATA → SHUTDOWN → STATUS` serializes edits to `run.go` and
`metadata.go`. SHUTDOWN and PEERS can proceed together, as can STATUS and
SCHEDULER. STORAGE owns output operations; SCHEDULER uses the existing staging
abort API and does not own storage production files.

Validation is local and deterministic, using the current stable Go toolchain.
Commands below can run through `bash -ic 'go ...'` in this environment. Run focused
checks with each change, and run the full gates once on the final integrated
tree. Use existing private test seams; route the mandatory tracker locally.
Never test against live trackers or existing BitTorrent clients. Regression tests
must assert corrected behavior, including when an earlier proof asserted the bug.

## UTP — Bound packet work and honor write deadlines

**Findings:** R-11 (P2), R-12 (P2). **Depends on:** none.
**Owns:** `internal/utp/packet.go`, `conn.go`, and focused uTP tests.
**Guidance:** [uTP](internal/utp/AGENTS.md).
**References:** DESIGN §§14, 16, 19; BEP 29.

R-11: `ParsePacket` retains every unknown extension, and `Packet.Validate`
visits them again. A 65,534-byte datagram containing 32,757 empty unknown
extensions allocates megabytes; `Conn.handleDatagram` parses it before checking
the connection ID. Repeated packets amplify CPU and memory without stream
progress. R-12: `Conn.Write` queues data whenever buffer space is available even
when its write deadline has already expired, violating its `net.Conn` contract.

- [ ] Walk and validate unknown extension framing without materializing one
  object per ignored header. Preserve supported datagrams, payload ownership,
  selective-ACK validation, and malformed-chain rejection. Do not introduce a
  smaller extension-count or datagram limit to hide the amplification.
- [ ] Check the connection identity early enough to avoid extension/state work
  for unrelated packets, without weakening validation for the active connection.
- [ ] Check the effective write deadline before each newly accepted prefix.
  Expired writes return a timeout without enqueueing more bytes; writes that
  accepted an earlier prefix retain correct partial-write results. Preserve
  deadline updates, close/reset wakeups, and concurrent `net.Conn` behavior.

**Fixtures and validation:** Construct a valid STATE header whose first extension
type is 2, followed by 32,757 two-byte headers with zero-length bodies; each
header names type 2 next except the last, which names 0. Compare allocations for
short and maximum chains and assert that ignored headers create no retained
per-header state. Include truncated chains, mixed unknown/SACK extensions, and
an unrelated connection ID. On a local connected uTP fixture with free send
capacity, set a past deadline and write 12 bytes: require zero accepted bytes
and a timeout. Also cover expiry after a partial write and extension/clearing of
the deadline while blocked. Run `go test ./internal/utp` and
`go test -race ./internal/utp`; seed and run bounded
`FuzzParsePacketBounded` and `FuzzConnectedTransportTransitions` campaigns.

**Acceptance:** valid ignored extensions require only bounded framing work and
no per-header retained allocation; active connection state ignores unrelated
IDs; write results reflect the deadline in force when bytes are accepted.

## SELECT — Preserve negated descending glob classes

**Finding:** R-13 (P2). **Depends on:** none.
**Owns:** `internal/torrent/selection.go` and selection tests/fuzz seeds.
**Guidance:** [torrent](internal/torrent/AGENTS.md).
**References:** DESIGN §§4.3, 8, 19.

`globClassToRegexp` turns an empty descending range into a NUL-only match even
when negated. `path.Match("[^z-a]", "a")` is true, but the accelerated selector
excludes the file, causing missing selections or a false no-match error.

- [ ] Preserve negation for classes whose ranges contribute no characters, or
  use the existing `path.Match` semantics for that case. Keep supported patterns
  and the existing accelerated selection bound.
- [ ] Add the exact pattern/path pair to the differential corpus. Cover positive
  and negated descending-only classes, descending ranges mixed with ordinary
  members, escaped class characters, and directory selection. Retain the rule
  that a bracket class can consume `/`, while `*` and `?` cannot.

**Validation:** Run `go test ./internal/torrent` and a bounded
`FuzzGlobRegexMatchesPathMatch` campaign, with the named edge cases replayed
deterministically. Check selected paths, not just compiled regular expressions.

**Acceptance:** accelerated selection agrees with `path.Match` over the supported
domain, including `[^z-a]`, without losing existing large-selection behavior.

## STORAGE — Preserve valid paths, isolate output inodes, and bound handles

**Findings:** R-14 (P2), R-15 (P1), R-16 (P2). **Depends on:** none.
**Owns:** `internal/storage/output.go`, `finalize.go`, `resume.go`, any necessary
storage platform helper, and focused storage tests. Metainfo parsing is a
read-only reference for this task.
**Guidance:** [storage](internal/storage/AGENTS.md).
**References:** DESIGN §§4.7, 8–9, 13.1, 16–19.

R-14: `Validate` prepends `info.name` before checking the relative file's
64-component/4,096-byte limits, rejecting valid metadata. R-15: `Prepare`,
`writeSelected`, and `TruncateSelected` mutate existing hardlinked inodes. An
outside file can be truncated, and two selected paths can both finish with the
second path's bytes despite successful piece verification. No concurrent
filesystem race is needed. R-16: preparation and finalization open every affected
file at once; valid many-file torrents fail with `EMFILE` under ordinary limits.

- [ ] Apply metainfo path limits to the relative file path, excluding the
  torrent root name. Check the combined path for actual filesystem
  representability separately; keep unsafe-path and collision rejection.
- [ ] Ensure selected output paths have distinct writable inodes before
  destructive preparation, verified writes, or resume truncation. Detach
  existing hardlinks as needed without modifying their other names. Preserve
  existing regular-file support and verified resume bytes; choose the smallest
  implementation that meets these conditions rather than rejecting every
  hardlinked regular file. Bound any copying and propagate its failures.
- [ ] Keep simultaneous output handles bounded independently of file count in
  both `Prepare` and `writeSelected`. Complete plan validation before destructive
  preparation, and preserve fatal open/write/short-write/close error handling.
- [ ] Preserve verification-before-output and staged-piece lifetime: do not
  delete a stage until all selected writes and closes succeed. Keep unselected
  paths untouched and zero-length selected files supported.

**Fixtures and validation:** Parse and select canonical metainfo with 64
one-character relative components, then validate and prepare its output. Check
the relative-byte boundary separately from genuine platform path failures.
Create a selected hardlink to an outside sentinel and verify overwrite leaves
the sentinel intact. Link two selected paths to one inode, finalize independently
hashed `A` and `B` pieces, and require final contents `A` and `B`. Repeat the
alias cases for resume-preserving writes and truncation of a verified overlong
file, including an unselected alias. In isolated child processes with a soft
descriptor limit of 64, exercise 96-file preparation and one piece spanning 96
selected files; both must succeed without leaking handles. Keep failure
injection for writes, closes, detachment/copying, and cancellation. Run
`go test ./internal/storage` and `go test -race ./internal/storage` plus affected
session resume/finalization regressions.

**Acceptance:** supported relative paths are not rejected merely because the
root was prepended; selected mutations cannot corrupt another pathname; file
count does not dictate simultaneous open handles; I/O failure remains fatal.

## ENDPOINT — Give routed IPv6 aliases one identity

**Finding:** R-17 (P2). **Depends on:** none.
**Owns:** `internal/peer/candidates.go` and focused endpoint/dial tests in
`internal/peer/`. Inspect tracker dictionary parsing and session admission as
callers without changing their files in this task.
**Guidance:** [peer](internal/peer/AGENTS.md).
**References:** DESIGN §§11, 16–17, 19; BEP 7.

`ResolveCandidate`, `NormalizeEndpoint`, and `normalizedEndpoint` retain
irrelevant IPv6 zones. An HTTP dictionary peer can announce `::1%anything` and
`::1` as separate identities even though Linux connects both to the same
listener. Changing the zone bypasses deduplication, backoff, strikes, and the
run-long blacklist; it also undermines distinct-endpoint accounting.

- [ ] Canonicalize at endpoint admission: remove zones with no routing meaning
  and normalize meaningful interface aliases consistently. Preserve distinct
  link-local routes and accepted IPv4/IPv6, private, and loopback endpoints.
- [ ] Use the same canonical identity for pool membership, dial attempts,
  backoff, corruption penalties, and blacklists across reconnects/transports.
  Keep tracker-supplied peer IDs out of identity.
- [ ] Cover direct parsed addresses and resolver-returned addresses, including
  name/numeric aliases for a meaningful scope. Keep ordinary invalid-interface
  failures distinct from a peer-origin protocol violation.

**Fixtures and validation:** Recreate
`TestAuditZonedLoopbackAliasesBypassEndpointBlacklist` with a loopback endpoint
and variants `::1`, `::1%anything`, and another arbitrary zone: require one
candidate identity and one shared blacklist/backoff state. A local IPv6 listener
can confirm routing equivalence on Linux. Controlled interface/resolver fixtures
must also prove that meaningful different scopes stay distinct and equivalent
interface aliases share strikes and attempted-endpoint accounting. Run
`go test ./internal/peer` and `go test -race ./internal/peer`, retaining existing
candidate-source fairness and endpoint-budget tests.

**Acceptance:** addresses that route to the same scoped endpoint cannot create
fresh penalty identities; genuinely distinct scoped endpoints remain usable.

## ADMISSION — Advance candidates fairly and propagate fatal failures

**Findings:** R-18 (P1), unresolved part of R-24 (P2).
**Depends on:** none. **Owns:** `internal/session/run.go`, `transfer.go`, and
dedicated admission regressions. Leave `metadata.go` to METADATA/SHUTDOWN.
**Guidance:** [session](internal/session/AGENTS.md).
**References:** DESIGN §§11, 15, 18–19.

R-18: the acquisition closure in `coordinator.startTransferPhase` advances
`candidateCursor` by the full 64-entry batch before examining candidates, then
returns on success or breaks on an ordinary dial error. A two-entry pool can
retry its first live peer forever while never trying a useful second peer.
R-24: `Transfer.acquireLoop` now propagates `peer.EndpointBudgetError`, but still
retries other permanent errors, including the coordinator's latched tracker-event
queue overflow. The real failure is eventually hidden by cancellation/timeout.

- [ ] Advance the cursor for candidates actually examined, including the one
  ending an acquisition. Continue within the bounded batch after an ordinary
  dial failure or live-peer-ID collision, observing endpoint backoff and
  cancellation without busy retrying.
- [ ] Define the acquisition boundary's retryable outcomes explicitly. Retry
  `ErrNoPeer` and explicitly temporary failures; deliver permanent errors to
  `Transfer.Run` and preserve them through normal shutdown. Keep existing fatal
  endpoint-budget propagation and ordinary indefinite discovery retries.
- [ ] Make both direct and latched queue-overflow paths reach the same fatal
  outcome without building a generic error-classification framework.

**Fixtures and validation:** Recreate
`TestAuditTwoCandidatesStarveSecondAfterFirstConnects`: use two controlled peers,
one that handshakes and idles, and another that supplies the piece. Assert the
second is attempted and makes progress with capacity available. Include first
candidate dial failure, live-ID collision, and backoff; assert attempt order with
barriers rather than the original 900 ms sleep. Recreate
`TestReviewFatalAcquireErrorMustStopTransfer` by injecting a permanent acquisition
error: require that error before a safety deadline, joined workers, normal
`stopped`/cache cleanup, and no endless retries. Cover the actual queue-overflow
callback and temporary/no-peer recovery. Run `go test ./internal/session` and
`go test -race ./internal/session` with the focused acquisition tests.

**Acceptance:** an eligible candidate is not starved by batch-cursor arithmetic
or a live collision; permanent acquisition errors terminate with their original
cause, while ordinary lack of peers remains retryable.

## METADATA — Permit progress and classify replies correctly

**Findings:** R-19 (P2), R-20 (P2), R-21 (P2). **Depends on:** none.
**Owns:** `internal/session/metadata.go`, `internal/peer/metadata.go`, and focused
metadata tests. Keep phase-shutdown edits in SHUTDOWN after this task.
**Guidance:** [session](internal/session/AGENTS.md), [peer](internal/peer/AGENTS.md).
**References:** DESIGN §§7.4, 12.3, 16, 19; BEPs 9 and 10.

R-19: `fetchMetadata` sends one 16 KiB request at a time under one fixed
30-second candidate deadline. A healthy supplier whose cumulative round trips
exceed that cutoff is discarded despite progress, repeatedly restarting large
supported metadata. R-20: the pre-size and response loops accept unsolicited or
wrong-piece rejects as ordinary `ErrMetadataRejected`. R-21:
`peer.ParseMetadataMessage` rejects canonical unknown integer `msg_type=256`
before unknown-type handling, and `tryCandidate` blacklists the endpoint.

- [ ] Implement bounded metadata request scheduling and a timeout policy that
  allows steady progress through supported metadata sizes while rotating stalled
  or unproductive suppliers. Keep at most the design's 32 metadata requests in
  flight, bounded response work, one supplier per candidate, and fixed geometry
  from the first accepted advertised size. Choose pipeline width and timeout
  details as internal implementation decisions, not new user-facing limits.
- [ ] Track outstanding pieces and accept each terminal response only for an
  outstanding request. Treat unsolicited/mismatched data or rejects as protocol
  violations; a matching reject remains ordinary refusal/backoff. Handle out-of-
  order valid responses if the chosen pipeline permits them.
- [ ] Compare the full decoded `msg_type` integer with known types before any
  narrowing. Ignore other well-formed integer types within existing bencode and
  frame bounds; keep malformed known messages rejectable.
- [ ] Preserve per-connection directional extension IDs, repeated mapping
  updates/disablement, cancellation, exact candidate hash validation, sole-
  supplier strikes for complete invalid metadata, and the no-metadata-upload API.

**Fixtures and validation:** Recreate
`TestReviewSerialMetadataRequestsExhaustCandidateDeadline` with at least two
valid metadata blocks, each returned after 90 ms, and a scaled old total budget
of 150 ms; the corrected policy must finish while each response is productive.
Use controlled time/barriers where practical and also check a permanently idle
supplier and endless irrelevant messages. Recreate
`TestReviewUnsolicitedMetadataRejectBeforeHandshake` and
`TestReviewWrongPieceRejectAbortsCandidate` over `net.Pipe`: a reject before any
request and a reject for piece 1 while only piece 0 is outstanding must be
protocol violations; a correct reject must not earn a strike. Feed the literal
canonical body `d8:msg_typei256ee` through parsing and discovery, then a valid
reply, and require ignored-message behavior with no blacklist. Include other
well-formed unknown integers and malformed known types. Exercise request-cap,
duplicate-terminal, extension-update, and cancellation cases. Run
`go test ./internal/peer ./internal/session` and their `-race` equivalents;
seed/run bounded `FuzzMetadataMessages` and `FuzzExtensionTransitions` campaigns.

**Acceptance:** productive supported metadata can complete without a fixed
cumulative-round-trip failure; stalled work stays bounded; request matching and
unknown-message handling produce the required endpoint classification.

## PEERS — Retire disconnected transfer state

**Finding:** R-23 (P1). **Depends on:** ADMISSION, for `transfer.go` ownership.
**Owns:** `internal/session/transfer.go` and dedicated transfer-retirement tests.
**Guidance:** [session](internal/session/AGENTS.md).
**References:** DESIGN §§11–13, 15–16, 19.

`admitCandidate` appends every `transferPeer`, while `disconnectPeer` marks it
done without removing it. Default indefinite retry retains closed connections,
workers, and bitsets and repeatedly scans their history. At two million pieces,
the three bitsets alone retain about 750,000 bytes per old connection; reconnects
to one endpoint evade the distinct-endpoint budget as a bound on this growth.

- [ ] Remove a disconnected peer from the active event set after its worker
  joins, releasing its connection and request ownership exactly once.
- [ ] Update event indices/selection safely when removing entries. Preserve
  processing of live peers, endgame cancellation, live peer-ID release, and
  later reconnection. Keep run-long endpoint strikes/blacklists in their
  separate state.

**Fixtures and validation:** Recreate
`TestReviewDisconnectedPeersDoNotAccumulate`: admit and disconnect 200 peers
during an incomplete transfer, including repeated connections to one endpoint.
After joins, require retained peer state to track live peers instead of history
(zero live peers must not leave 200 dead entries). Keep a useful peer active
while others leave; deliver events around removal, reuse a released peer ID,
and verify no lost events, double releases, or reset penalties. Run
`go test ./internal/session` and `go test -race ./internal/session` with these
regressions and the existing closed-worker/endgame tests.

**Acceptance:** reconnect history does not grow the active peer slice or retain
per-connection state; active event handling and run-long penalties remain correct.

## SHUTDOWN — Join admission before final tracker events

**Finding:** R-22 (P2). **Depends on:** ADMISSION and METADATA, for shared files.
**Owns:** `internal/session/metadata.go`, `run.go`, and dedicated shutdown tests.
**Guidance:** [session](internal/session/AGENTS.md).
**References:** DESIGN §§6, 7.4, 15, 18–19.

`MetadataDiscovery.Run` finalizes trackers before joining `trackerPeerResolver`;
`coordinator.startTransferPhase` defers resolver close until after final events.
An active lookup therefore overlaps `stopped`; metadata also begins full
normalization before resolver completion. Existing proofs show an ordering
defect, not a permanent goroutine leak or DNS work entering resume.

- [ ] Cancel and join admission/resolver workers before final tracker events,
  and before full metadata normalization. Keep pending-queue cleanup within the
  same ownership boundary and make success, failure, cancellation, and timeout
  exits follow it.
- [ ] Preserve the rest of the phase shutdown order, bounded final-event
  attempts, primary-error precedence, and ordinary worker joining before return.

**Fixtures and validation:** Recreate
`TestReviewMetadataStoppedWhileResolverStillRunning` and
`TestReviewTransferFinalEventsPrecedeResolverJoin` with a controlled resolver
that observes cancellation but completes only after a barrier. Record resolver
completion, tracker events, and normalization/phase entry. Require resolver join
before the first final event and before normalization, plus joined workers on
outer return. Exercise successful metadata acquisition and transfer timeout,
then representative cancellation/error exits. Run `go test ./internal/session`,
`go test -race ./internal/session`, and a bounded
`FuzzRunMetadataCancellationShutdown` campaign.

**Acceptance:** no admission/DNS worker remains active during final tracker
events or crosses the metadata-normalization boundary; shutdown remains bounded.

## SCHEDULER — Keep requestable work moving within resource bounds

**Findings:** R-25 (P2), R-26 (P2), remaining F-01 (high)/SEC4 work.
**Depends on:** PEERS, for `transfer.go` ownership.
**Owns:** `internal/session/scheduler.go`, `transfer.go`, and dedicated scheduler
and transfer regressions/benchmarks. Use `storage.PieceStage.Abort` for stage
cleanup; storage production files remain with STORAGE.
**Guidance:** [session](internal/session/AGENTS.md).
**References:** DESIGN §§8, 12–13, 16, 19.

R-25: `RemovePeer` releases requests but leaves all stages. Sixty-four unavailable
stages prevent `ReservePiece` from admitting another peer's available piece.
The transfer `drive` loop also reserves stages before checking a peer's zero
request capacity, so an explicit `reqq=0` can fill the stage budget without
issuing a request. This blocks available progress; it does not prove that the
whole torrent could finish without the unavailable pieces.

R-26: `makeBlocks` splits every non-padding file span separately; repeated
`choosePiece`, `firstAssignableBlock`, `findActive`, and `allDone` scans then make
one piece spanning many tiny files quadratic to schedule. The baseline benchmark
with one selected file took about 4.22 ms for 1,000 spans and 350.68 ms for 10,000
on the review host. This is independent of output-handle limits.

F-01/SEC4: `Transfer.Run`/`drive` still reaches `choosePiece` after keepalives or
duplicate availability messages, scanning the full wanted order even when
availability is unchanged. Prior state-delta fixes do not remove this scheduler
scan. Small repeated messages can monopolize coordinator CPU at large piece
counts. This is a separate dimension from R-26's blocks within one piece.

- [ ] Skip new stage reservation for peers with no request capacity. When full
  staging prevents requestable work, reclaim stages that current peers cannot
  advance, together with their cache files and scheduler/cache credits. Preserve
  useful partial stages when capacity permits. Keep stage removal failures fatal.
- [ ] Reconcile stage eligibility after disconnect, availability/choke changes,
  and request-limit updates without losing active requests, Fast tombstones,
  accepted-block provenance, or verification state. Do not increase staging
  budgets or discard useful in-flight work merely to evade the defect.
- [ ] Coalesce contiguous non-padding file spans into requests no larger than
  the existing block limit. Preserve piece boundaries and synthetic padding.
  Use direct block lookup and remaining-work state where needed so unavoidable
  fragmentation does not cause a full block-list scan per request/response.
- [ ] Schedule from eligible work instead of rescanning every wanted piece after
  unchanged events or for each pipeline slot. Preserve rarest-first randomized
  ties, streaming fallback, Allowed Fast eligibility, per-peer/global caps,
  endgame duplicates, and exact late-terminal handling. Use the smallest state
  needed to maintain these properties.

**Fixtures and validation:**

- Recreate `TestReviewOrphanedStagesBlockAvailableWork` with 65 wanted 16 KiB
  pieces. Admit pieces 0–63 for a departing peer, remove it, then offer piece 64
  from another peer. Require a request for 64, correct stage/file reclamation,
  and bounded count/bytes. Retain the positive control
  `TestReviewHealthyReplacementCanUseRetainedStage`: a replacement offering an
  existing partial piece can resume it. Cover pressure from both stage count
  and declared bytes, plus injected abort/removal failure.
- Recreate `TestReviewZeroReqQStagesWithoutRequests` with one stage slot, a peer
  advertising `reqq=0`, and a useful second peer. Require zero reservation for
  the zero-capacity peer and progress from the second. Cover repeated extension
  handshakes changing capacity to and from zero.
- Recreate `BenchmarkReviewTinyFilesOnePiece` with 1,000 and 10,000 one-byte
  regular files inside one piece, selecting only the first file. Check full-piece
  request coverage, bounded request count for contiguous bytes, verified output,
  and scaling. Add padding-separated spans to exercise unavoidable fragmentation.
- Recreate `BenchmarkReviewNoAssignableBlock` with 10,000 and 100,000 wanted
  pieces, a peer offering only piece 0, and its block already assigned. Repeated
  scheduling must find no work without traversing the full wanted order. Also
  recreate `BenchmarkReviewPipelineFill` with 64 available staged pieces while
  varying total wanted pieces. Drive duplicate `Have`, empty `Have None`,
  keepalive, and sparse choke/unchoke sequences through transfer, including a
  peer with other requestable work, and check that scheduling honors the same
  bound. Use deterministic work assertions where practical; report benchmark
  scaling instead of requiring a host-specific millisecond threshold.

Run `go test ./internal/session`, `go test -race ./internal/session`, the focused
benchmarks with allocation reporting, and bounded `FuzzSchedulerEvents`,
`FuzzSchedulerEndgameEvents`, and `FuzzSchedulerStrikeAccounting` campaigns.

**Acceptance:** unusable stages cannot indefinitely block requestable pieces;
zero capacity does not reserve work; tiny-file fragmentation does not produce
quadratic block handling; unchanged small messages do not force full-torrent
scans. Existing scheduling, Fast, verification, and cache bounds still hold.

## CLI — Report listing failures and sanitize usage errors

**Findings:** R-27 (P3), R-28 (P3). **Depends on:** none.
**Owns:** `internal/cli/run.go`, `args.go`, `report.go`, `cmd/leech/main.go`, and
focused CLI/command tests. STATUS takes CLI files after this task.
**Guidance:** [CLI](internal/cli/AGENTS.md).
**References:** DESIGN §§4.4–4.5, 4.9–4.10, 19.

R-27: `RunWithSession` returns a wrapped `--list-files` stdout write failure
without calling `Reporter.PrimaryFailure`, leaving stderr empty at error level.
R-28: `parseLongOption`/`parseShortOption` interpolate raw unknown options, and
`main` prints the resulting usage error without the reporter's escaping/bound.
Command-line control bytes can alter the terminal, and long options exceed the
diagnostic limit; no remote injection path was demonstrated.

- [ ] Report the wrapped listing-write error as the primary error before
  returning it. Preserve its cause, stdout/stderr separation, and failure exit.
- [ ] Escape and bound untrusted usage text at the argument/printing boundary,
  using the existing diagnostic policy. Preserve useful usage context, exit 2,
  accepted argument forms, and help behavior.

**Fixtures and validation:** Recreate
`TestReviewListingWriteFailureIsReported` with valid local metainfo and a stdout
writer that fails; require the original error and an error-level stderr
diagnostic. Run the executable with an unknown option containing literal ESC
(`--` followed by byte `0x1b` and `[2J`) and with a 5,000-byte unknown option.
Require escaped control text, a bounded diagnostic, and exit 2; the baseline
emitted literal ESC and 5,055 stderr bytes. Include short-option and invalid-value
paths that reach the same boundary. Run `go test ./internal/cli ./cmd/leech` and
the focused subprocess checks. These cases require no network.

**Acceptance:** listing failures explain their cause at error level, and every
usage-error path prints bounded terminal-safe text with the correct exit status.

## STATUS — Show active phases before the first committed piece

**Finding:** R-29 (P3). **Depends on:** CLI and SHUTDOWN, for shared files.
**Owns:** `internal/cli/run.go`, `report.go`, `internal/session/run.go`, and
dedicated status tests. `transfer.go` is a read-only progress-callback reference.
**Guidance:** [CLI](internal/cli/AGENTS.md), [session](internal/session/AGENTS.md).
**References:** DESIGN §§4.9, 6, 19.

The CLI's `OnPhase` callback only prints permanent messages. `Reporter.Status`
is called from `OnProgress`, which first fires after a committed piece.
Metadata discovery, resume checking, and a pre-commit stalled transfer therefore
lack the promised replaceable interactive status line.

- [ ] Emit an initial status at active metadata, resume, and transfer phase
  entry, using available progress values accurately. Keep subsequent transfer
  progress connected to the existing callback.
- [ ] Preserve terminal detection, info/debug filtering, status throttling,
  permanent phase/result lines, and stdout's listing-only role. Check quick
  phase transitions against the existing one-second update bound rather than
  resetting the throttle to force every rapid transition into a new update.

**Fixtures and validation:** Use injected terminal/time state and complete CLI
fixtures. For resume, supply a one-byte torrent with matching output and run
`--resume --loglevel info`; require a resume status before the already-complete
result even though no transfer occurs. Hold metadata acquisition and transfer
before any commit behind controlled barriers and require status in both phases.
Include non-TTY stderr, warning/error levels, and fast consecutive transitions.
Run `go test ./internal/cli ./internal/session` and their `-race` equivalents;
use a local pseudo-terminal check where needed to cover the executable boundary.

**Acceptance:** metadata, resume, and transfer status no longer depend on a
committed piece; existing terminal, logging, and rate rules remain intact.

## INPUT-REVIEW — Finish the bounded offline-input review

**Scope:** unresolved review coverage, not a confirmed new defect.
**Depends on:** SELECT so the selection fix is included.
**Owns:** review of `internal/bencode/` and `internal/torrent/{source,metainfo,selection}.go`,
with dedicated test/fuzz additions in those packages. Read CLI/session validation
call sites and storage boundaries; do not edit their production files here.
**Guidance:** [bencode](internal/bencode/AGENTS.md),
[torrent](internal/torrent/AGENTS.md).
**References:** DESIGN §§4.1–4.4, 7.1–7.3, 8, 16–19; relevant local BEPs.

The full-application review left bencoding, source parsing, metainfo normalization,
and file selection marked in progress. Completed fixes and passing general tests
do not close that specific coverage gap.

- [ ] Complete a focused review of these input paths against their documented
  domain: canonical decoding and exact `info` bytes; byte/node/container bounds
  and checked arithmetic; source precedence and magnet fields; normalized file
  ranges/attributes/paths; selection and original BEP 53 indices.
- [ ] Check that representative invalid local inputs fail before output/cache
  mutation and network activity. Reuse existing lifecycle evidence and independent
  vectors rather than rebuilding the complete integration matrix.
- [ ] Replay existing fuzz corpora and run bounded campaigns for
  `FuzzDecodeBounded`, `FuzzParseSource`, `FuzzParseMagnet`,
  `FuzzParseMetainfoBounded`, `FuzzSelectPattern`, and
  `FuzzGlobRegexMatchesPathMatch`. Add concrete missing edge cases and minimized
  regressions with their expected contract.
- [ ] Record any confirmed new defect as an actionable task in this backlog
  with file scope, evidence, acceptance, and dependencies. Do not turn hypotheses
  or unrelated refactoring into mandatory work.

**Validation:** Run `go test ./internal/bencode ./internal/torrent` plus the
affected existing offline CLI tests. Report reviewed boundaries and checks in
the completion handoff; keep durable lessons in applicable guidance.

**Acceptance:** the unfinished review scope has been examined on the integrated
input code, and every confirmed remaining gap has an explicit task. Completion
of this review does not imply that any newly reported defect is fixed.

## VALIDATE — Verify the integrated corrections

**Depends on:** every task above and any contract defects found by INPUT-REVIEW.
**Owns:** final review of the integrated changes, necessary cross-component
regression tests, and backlog reconciliation. It owns no speculative production
cleanup; return a concrete failure to its implementation task.
**Guidance:** root [AGENTS.md](AGENTS.md) and the affected package guidance.
**References:** DESIGN §19 and each task's acceptance criteria.

- [ ] Confirm every original ID in the task table has corrected-behavior
  evidence, including R-24's non-budget permanent errors, R-25's zero-capacity
  trigger, and F-01/SEC4's scheduler scans. Check that tests exercise the
  production path and would detect the original trigger.
- [ ] Recheck interactions changed by these tasks: endpoint identity through
  penalties; candidate/peer/stage replacement; metadata replies and shutdown;
  verified output across linked/many-file paths and resume; CLI primary errors
  and phase status. Reuse existing no-upload, local-tracker, and cleanup fixtures.
  Add only missing cross-boundary assertions.
- [ ] Verify formatting and run `go test ./...`, `go test -race ./...`,
  `go vet ./...`, and `CGO_ENABLED=0 go build ./cmd/leech` on the integrated tree.
  Replay all fuzz seeds and run the bounded campaigns required by changed
  boundaries. Retain minimized failures as regressions.
- [ ] Reconcile acceptance with the actual results. Remove resolved tasks from
  this backlog after their checks pass; retain or split unresolved work with its
  original IDs and update affected dependencies. Update durable guidance only
  when the implementation establishes a new useful boundary or pitfall.

**Acceptance:** integrated tests, race checks, vet, and the pure-Go build pass;
each implemented fix meets its specific acceptance criteria; any remaining work
is visible. Completion reports identify local deterministic validation and do
not imply interoperability testing against live trackers or existing clients.
