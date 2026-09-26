# Approved implementation tasks

The six changes below are approved and unimplemented. The IDs retain the audit's
finding numbers. [DESIGN.md](DESIGN.md) is the behavior contract; this document
defines implementation scope, ownership, and acceptance checks. Source pointers
describe commit `bd7417f`; locate functions by name after earlier tasks merge.

## Roles and execution rules

| Role | Model | Reasoning effort | Responsibility |
| --- | --- | --- | --- |
| Orchestrator | GPT-6 Sol (`gpt-6-sol`) | High | Assign bounded tasks, resolve interfaces, integrate commits, and verify acceptance. |
| Worker | GPT-6 Luna (`gpt-6-luna`) | High | Implement only the assigned task or diagnostics subtask in its worktree. |
| Reviewer | GPT-6 Luna (`gpt-6-luna`) | Max | Independently inspect the exact task diff and its tests; report findings without editing it. |

- Read root [AGENTS.md](AGENTS.md), the task's cited DESIGN sections, and the
  guidance in every package the task owns. Do not load unrelated subtrees.
- Workers and reviewers do not spawn agents. The orchestrator assigns each
  worker and a separate reviewer explicitly with the models and efforts above.
  Use fresh agent context with a complete task prompt when setting model overrides.
- Use at most three child agents concurrently, including reviewers. A reviewer
  uses a slot released by a finished worker. Never let two agents edit one worktree.
- Do not broaden the task to fix adjacent issues, rename existing APIs, reorganize
  packages, tune performance, or improve unrelated tests. Report unrelated defects
  to the orchestrator and continue the assigned scope when possible.
- Reuse current types, callbacks, clocks, and fixtures. Add a small local helper
  or field when needed, not a generic framework, event bus, cache library, or
  error-classification subsystem. Production remains standard-library-only Go.
- Do not weaken DESIGN or tests to make implementation easier. Routine internal
  choices belong to the orchestrator. A proposed change to approved behavior
  requires a user decision before dependent work proceeds.
- Reviewers must identify a concrete contract violation, defect, race, unbounded
  resource, or missing acceptance check. No required finding count and no requests
  for speculative abstractions, stylistic rewrites, or broader coverage targets.

## Decisions that must remain unchanged

1. Metadata discovery follows the Fast exception in [§7.4](DESIGN.md#74-metadata-acquisition):
   extension handshake first, no initial availability, ignored file requests,
   and structural framing without Fast-negotiation enforcement. File transfer
   retains its separate Fast rules. Do not copy tests asserting the old metadata
   `Have None` or file-request rejection requirements.
2. [§12.4](DESIGN.md#124-requests-and-framing) accepts incoming keepalives but
   does not schedule outgoing ones. Add no keepalive timer.
3. Cache cleanup precedes final tracker events under [§15](DESIGN.md#15-concurrency-and-lifecycle).
   Cleanup failure suppresses `completed`, even after output verifies; otherwise
   applicable `stopped` attempts remain enabled. Do not reorder shutdown, remove
   announcements, or change their existing 15-second final-event budget.

Preserve the no-upload boundary, verified-before-write rule, file selection,
resume behavior, signal behavior, and all existing supported input limits.
WebSocket trackers and the original live-torrent test are outside these tasks.
All verification uses local fixtures; never contact live trackers or run existing
BitTorrent clients against Leech.

## Worktrees and dependencies

The independent tracker and transport work can run alongside the session work.
The session tasks deliberately run in order because they share transfer,
callback, and shutdown code. Diagnostics comes last so it instruments the final
behavior instead of producing competing edits to those files.

| Lane | Order | Branches |
| --- | --- | --- |
| Session/CLI | F6 → F9 → F4 | `codex/f6-cache-errors`, `codex/f9-timeout-errors`, `codex/f4-live-status` |
| UDP trackers | F7 | `codex/f7-udp-retention` |
| uTP | F8 | `codex/f8-utp-deadlines` |
| Diagnostics | F1, after all three lanes merge | `codex/f1-diagnostics` |

Orchestrator procedure:

1. Preserve unrelated working-tree changes. Create or select an integration
   branch, normally `codex/design-fixes`, and commit the approved planning files
   and guidance before dispatch. Record that commit as the shared baseline.
   Worktrees do not inherit uncommitted edits; do not branch workers from the old
   design accidentally. Do not commit unrelated files.
2. Create separate worktrees for F6, F7, and F8 from that baseline. Use a fresh
   path for each and the branch names above. For example,
   `git worktree add -b codex/f7-udp-retention <fresh-path> <baseline-sha>`;
   replace both placeholders with verified values. Never reset an existing branch
   or reuse a dirty worktree to force this command to succeed.
3. Give the worker its absolute worktree path, base commit, task text, allowed
   files, acceptance checks, and handoff format. Every command must run in that
   worktree. One task per branch; diagnostics has the subtask sequence below.
4. After the worker stops editing and commits, give a fresh reviewer the base
   and resulting commit, task contract, and test commands. The reviewer must read
   the code and relevant tests, not rely on the worker's summary. If fixes are
   needed, return only those findings to the worker, then review the new diff.
5. Only the orchestrator integrates reviewed commits. Integrate F6 before
   creating F9's worktree from the updated integration branch; integrate F9 before
   creating F4's. F7 and F8 need not wait for that chain. Keep diagnostics out of
   these branches. Workers do not merge other workers' branches.
6. If a conflict affects behavior, have the owning worker repair it on the updated
   base and review that repair. Do not resolve semantic conflicts by blindly
   choosing either side. Re-run affected checks after conflict resolution.
7. After F4, F7, and F8 are integrated, start F1 from the combined tree. Finish
   its four bounded subtasks in order, using separate worker assignments if needed.
8. Run the final integrated checks below, update task status, and report results.
   Keep task worktrees until their commits are integrated and verified. Do not
   push, publish, or create a PR unless requested separately.

Workers may update only their package's guidance when implementation changes
durable knowledge. Only the orchestrator updates this checklist and shared
DESIGN/root guidance. A necessary file outside the assigned scope must be agreed
with the orchestrator before editing; it is not an invitation to expand behavior.

## F6 — Propagate fatal cache errors during corrupt-piece handling

- [ ] Implemented, independently reviewed, integrated, and verified.

**Contract:** [§13.1](DESIGN.md#131-cache-staging), [§18](DESIGN.md#18-failure-semantics).
Read [session guidance](internal/session/AGENTS.md) and
[storage guidance](internal/storage/AGENTS.md).

**Own:** `internal/session/transfer.go`, focused session tests, and necessary
session guidance. Read storage code; its existing error contract is sufficient.

**Current defect:** `Stager.Finalize` joins a hash mismatch with a failed staged
file close/removal and records `Stager.Fatal()`. `Transfer.finalizePiece` sees
`ErrPieceHashMismatch`, applies corruption accounting, and returns only the
scheduler result. A local test with expected bytes `good`, staged bytes `evil`,
and an injected close failure returned nil despite a recorded fatal error.

**Implementation instructions:**

1. Keep the existing hash-mismatch reset, contributor strikes, and blacklist
   notification path. Each contributor still receives exactly one strike.
2. After that accounting, propagate the stager's recorded fatal error, joined
   with any scheduler error. Use `Stager.Fatal()` and existing `errors.Join`;
   do not parse error text or introduce a new classification system.
3. A plain hash mismatch with no storage failure remains retryable. Do not
   return every hash mismatch as a fatal session error. Preserve
   `ErrStagingFatal` so peer-local recovery cannot swallow the storage failure.
4. Do not change successful finalization, including `OutputCommitted` handling
   when output succeeded but later stage removal failed.

**Required checks:**

- Add the corrupt-bytes plus injected-close-failure regression at the actual
  `finalizePiece` boundary. Assert the returned error contains the injected
  failure and `ErrStagingFatal`, one strike, and no corrupt output commit.
- Keep plain-corruption retry and mixed-contributor strike tests passing.
- Add or extend a local transfer fixture to show the fatal failure reaches
  graceful shutdown without waiting for another storage operation or timeout.
- Preserve `TestCommittedFinalizeErrorSettlesPieceAndCallsVerifiedOnce`.

**Starting points:** [transfer tests](internal/session/transfer_test.go),
[storage finalizer](internal/storage/finalize.go),
[storage failure fixtures](internal/storage/s2_failure_test.go).
**Package checks:** `./internal/session ./internal/storage`.

## F9 — Report cleanup failures alongside a no-progress timeout

- [ ] Implemented, independently reviewed, integrated, and verified.

**Depends on:** F6. **Contract:** [§4.10](DESIGN.md#410-exit-status-and-signals),
[§13.1](DESIGN.md#131-cache-staging). Read session, storage, and
[CLI guidance](internal/cli/AGENTS.md).

**Own:** `internal/session/run.go`, focused session/CLI tests, and necessary
session guidance. Do not change storage's primary-error-aware cleanup API.

**Current defect:** `startTransferPhase` replaces the entire joined transfer error
with `ErrNoProgressTimeout`. A local session with a 300 ms timeout and an injected
staged-file close failure reported only the timeout; `OnSecondary` saw nothing.

**Implementation instructions:**

1. Keep a local reference to the existing transfer stager instead of constructing
   it only inside the `TransferConfig` literal. After `Transfer.Run` has returned,
   its idempotent `Close()` exposes the stored storage/cleanup outcome separately
   from the cancellation error. Reuse that existing result; do not add error-tree
   traversal, string matching, or a general result hierarchy.
2. In the existing no-progress-timeout classification branch, send a non-nil
   cleanup outcome through `RunConfig.OnSecondary` once, then return timeout as
   the primary failure. The callback remains optional; do not add a fallback
   logger or duplicate that secondary failure in the primary CLI line.
3. Retrieving the recorded result must not repeat file closure, workspace removal,
   or tracker finalization. Keep ordinary cancellation and non-timeout errors on
   their existing paths. Keep tracker final-event failures secondary too.
4. Preserve cleanup-before-announcements and the accepted `completed` suppression
   rule. Do not change timeout duration, reset conditions, or exit statuses.

**Required checks:**

- Use `RunConfig.StageFileOpener` to inject a close error after a local peer has
  caused a stage to open and then withheld the requested data. Wait for those
  fixture events before relying on the timeout. Assert `errors.Is(result,
  ErrNoProgressTimeout)`, the close failure in `OnSecondary`, and one actual close.
- Through the CLI, assert one primary timeout diagnostic and a separate cleanup
  diagnostic at `error` level, with the existing nonzero exit classification.
- Cover a clean timeout, timeout plus final-tracker failure, normal cancellation,
  and a nil secondary callback. Preserve verified-partial-output reporting.
- A failure injected after a successful OS close proves error reporting, not
  leftover files; do not claim it demonstrates failed removal.

**Starting points:** [session timeout tests](internal/session/run_test.go),
[CLI integration fixtures](internal/cli/v1_test.go),
[`Stager.Close` and `Cleanup`](internal/storage/staging.go).
**Package checks:** `./internal/session ./internal/cli ./internal/storage`.

## F4 — Refresh interactive activity independently of piece commits

- [ ] Implemented, independently reviewed, integrated, and verified.

**Depends on:** F9. **Contract:** [§4.8](DESIGN.md#48-no-progress-timeout),
[§4.9](DESIGN.md#49-output-and-terminal-behavior). Read session and CLI guidance.

**Own:** `internal/session/run.go`, `internal/session/transfer.go`,
`internal/cli/run.go`, focused tests, and corresponding guidance. Change
`internal/cli/report.go` only if existing rendering cannot consume the new data.

**Current defect:** `OnProgress` supplies peer count and recent rate only after
a piece commits. The CLI timer retries that snapshot without refreshing it.
A connected, choked peer still displayed `peers=0`; a nonzero rate cannot decay
during a stall without another commit.

**Implementation instructions:**

1. Keep `OnProgress` commit-only. Add one separate optional session status callback
   carrying a `RunProgress` value. Status must never invoke the callback that
   resets the no-progress timer.
2. Produce fresh snapshots from the transfer coordinator's existing one-second
   replacement tick. Add a narrow transfer-to-session snapshot hook if needed.
   Count admitted, live peers with the coordinator's state, not candidates or
   queued dial results. No new polling goroutine, status service, or event history.
3. Compose verified bytes and rate in the same serialized owner as
   `OnPieceVerified` and `OnPayloadReceived`. Retain the existing five-second
   `payloadRate` calculation and its received-payload semantics. Do not read the
   scheduler or its mutable counters concurrently from the CLI worker.
4. Wire the fresh callback to the existing CLI status snapshot and renderer.
   Preserve phase-entry status, the one-second render throttle, permanent-line
   handling, and joining the existing CLI status worker before final output.
5. Install CLI activity observation only when status is enabled, while preserving
   any caller-supplied callback. Noninteractive stderr and warning/error levels
   still have no periodic progress output. Do not change stdout or add flags.

**Required checks:**

- Connect a local peer and keep it choked before any piece commits. The next
  eligible status refresh must show one active peer and unchanged verified bytes.
  Disconnect it and show the count falling without requiring a commit.
- Receive payload without completing a piece: rate becomes nonzero while verified
  bytes stay unchanged. Advance the existing rate window with no further payload:
  rate becomes zero without a commit. Use injected times and event barriers.
- Status ticks, new connections, and received blocks do not reset the no-progress
  timer; a newly verified piece still does. Resume bytes remain correct.
- Preserve TTY/level filtering, at-most-once-per-second rendering, deferred
  phase status, warnings followed by status, and no output after final shutdown.

**Starting points:** [phase status fixtures](internal/cli/phase_status_test.go),
[reporter tests](internal/cli/report_test.go),
[`payloadRate` and callbacks](internal/session/run.go),
[`Transfer.Run` tick and `countLive`](internal/session/transfer.go).
**Package checks:** `./internal/session ./internal/cli`.

## F7 — Bound UDP tracker endpoint retention

- [ ] Implemented, independently reviewed, integrated, and verified.

**Contract:** [§10.3](DESIGN.md#103-udp), [§16](DESIGN.md#16-supported-bounds),
[§17](DESIGN.md#17-security-and-trust-boundaries). Read
[tracker guidance](internal/tracker/AGENTS.md) and [BEP 15](beps/bep_0015.rst).

**Own:** `internal/tracker/udp.go`, focused tracker tests, and tracker guidance.
Do not change tracker URL validation, peer admission, session callbacks, or uTP.

**Current defect:** `UDPClient.sessions` retains every historical resolved
endpoint. A local probe using one tracker and 128 successive addresses retained
128 entries and open simulated sockets despite expired connection IDs. All were
released only by `UDPClient.Close`; OS descriptor exhaustion was not tested.

**Implementation instructions:**

1. Use a private cache capacity of `2 * limits.Trackers` (currently 128): one
   IPv4 and one IPv6 endpoint per supported tracker can be active concurrently.
   This is an internal retention choice, not a new input limit or CLI option.
   Keep endpoint identity and existing same-endpoint transaction serialization.
2. Reuse an existing entry when present. At capacity, retire any unused entry
   and close its socket before opening a replacement. An arbitrary unused entry
   is sufficient: no LRU list, expiry heap, background sweeper, or timer per entry.
3. Under the client mutex, track users of an entry from acquisition through
   transaction completion, including callers waiting for its transaction lock.
   Never evict such an entry. Release ownership on every error/cancellation path.
4. If every entry is in use, wait cancellably for capacity; do not grow the map,
   dial an untracked overflow socket, busy-loop, or reject a valid tracker URL.
   Shutdown must wake capacity waiters. Count retiring sockets against capacity
   until closed, so replacing entries cannot temporarily accumulate another cache.
5. Preserve independent IPv4/IPv6 transactions, BEP 15 ID expiry and retransmission,
   transmitted-request accounting, and BEP 41 URL data. Do not hold the client
   mutex across dialing, exchanges, or waiting for an entry's transaction lock.
6. Preserve idempotent `Close`, interruption of active exchanges, and the existing
   rule that a dial finishing after client closure cannot install a socket.

**Required checks:**

- Extend the rotating-DNS fixture beyond twice the capacity using independent
  connect/announce response bytes and a controlled clock. Assert both retained
  entries and owned open sockets stay bounded and retired sockets close.
- Revisit a retained address and reuse its valid ID; revisit an evicted address
  and successfully reconnect. Expired IDs still refresh before announcement.
- Use barriers to hold an entry active during eviction pressure. Verify it is
  not closed, waiting users are protected, and another unused entry can retire.
- Cover all-entries-busy cancellation, client close waking capacity waiters,
  close racing dial/eviction, both address families, and cleanup on failure.
  Test the actual production bound, not a test-only configurable capacity.

**Starting points:** [`UDPClient`, `announceFamily`, `ensureConnection`, `Close`](internal/tracker/udp.go),
[existing UDP fixtures and close-race tests](internal/tracker/udp_test.go),
[`limits.Trackers`](internal/limits/limits.go).
**Package checks:** `./internal/tracker`.

## F8 — Honor expired uTP read deadlines with buffered data

- [ ] Implemented, independently reviewed, integrated, and verified.

**Contract:** [§14](DESIGN.md#14-utp), [Go `net.Conn`](https://pkg.go.dev/net#Conn).
Read [uTP guidance](internal/utp/AGENTS.md).

**Own:** `internal/utp/conn.go`, focused uTP tests, and uTP guidance. Do not change
packet formats, receive ordering, congestion control, peer logic, or write policy.

**Current defect:** `Conn.Read` consumes `ReceiveState` bytes before checking its
deadline. The loopback reproduction acknowledged a one-byte DATA packet, set a
deadline in the past, and then read `x` with no error.

**Implementation instructions:**

1. For a nonempty read, check the current read deadline under `c.mu` before
   consuming buffered bytes. An expired deadline returns zero bytes and an error
   matching `os.ErrDeadlineExceeded`, without discarding the buffered payload.
2. After a wake or timer result, recheck the current deadline under the same
   mutex. An old timer must not override a deadline another caller has extended
   or cleared. The existing `Write` loop illustrates this pattern.
3. Preserve zero-length-read behavior and existing EOF/RESET/close behavior when
   no expired deadline intervenes. Preserve bytes before FIN. Do not change the
   common wait helper's other callers just to fix this read loop.

**Required checks:**

- Send independently specified DATA bytes through the existing loopback fixture
  and observe their ACK before setting a past deadline. Assert zero bytes and
  `errors.Is(err, os.ErrDeadlineExceeded)` on the next read.
- Clear and, separately, extend the deadline; the exact same buffered bytes must
  then be returned once, in order. No reconnect or retransmission is required.
- Cover empty-buffer timeout, shortening a blocked read's deadline, extending or
  clearing it while blocked, concurrent deadline changes, and close/RESET wakeup.
- Keep existing write-deadline, dial-context-lifetime, and FIN-ordering tests green.

**Starting points:** [`Conn.Read`, `Write`, and deadline setters](internal/utp/conn.go),
[`newWriteFixture` and deadline regressions](internal/utp/conn_regression_test.go),
[read/close fixtures](internal/utp/conn_test.go).
**Package checks:** `./internal/utp`.

## F1 — Implement bounded diagnostics and the required session observations

- [ ] All four subtasks implemented, independently reviewed, integrated, and verified.

**Depends on:** F4, F6, F7, F8, and F9 integrated. **Contract:**
[§4.5](DESIGN.md#45-logging), [§4.9](DESIGN.md#49-output-and-terminal-behavior),
[§19.4](DESIGN.md#194-no-upload-and-terminal-response-invariants).
Read CLI, session, tracker, and [peer guidance](internal/peer/AGENTS.md).
Read storage guidance if an observation requires touching storage.

**Current defect:** `Reporter.Debug` exists but has no production callers.
Session callbacks lack a debug channel; `trackerPeerResolver.pump` discards
tracker status events. A failing local tracker produced only phase lines and a
timeout. Several §19.4 observations are also missing. Merely logging that timeout
or adding synthetic reporter tests does not satisfy this task.

### F1a — Establish the narrow observation and rendering path

- [ ] Reviewed checkpoint.

Own session diagnostic definitions/wiring and CLI reporting integration. Before
dispatching producer work, the orchestrator must record the exact callback/event
shape and its byte/count bounds in the owning code comments or guidance.

- Use one optional session diagnostic callback and a small fixed-field value
  type. Use constants for event kinds and typed counts/durations/endpoints;
  no `map[string]any`, serialized event protocol, subscriber registry, or generic
  observability package. Lower-level packages must not import `session` or `cli`.
- Keep ordinary progress/status separate from diagnostics. Use existing
  `OnWarning`, `OnSecondary`, and reporter methods for their current purposes;
  preserve caller-provided callbacks when the CLI composes its own.
- The session-facing CLI callback must return promptly. Use bounded delivery to
  a CLI-owned consumer; never perform stderr writes while holding coordinator or
  protocol-state locks. Reuse the existing CLI reporting worker where practical.
  Start diagnostic consumption even when stderr is noninteractive; retain the
  separate TTY/level rule for status. Do not create a goroutine per event. Stop
  and join reporting before final output.
- Bound retained diagnostic bytes before enqueueing, not just rendered line size.
  Use a fixed capacity of 128 records and `DefaultDiagnosticBytes` (4096) as
  the per-record retained text ceiling, plus fixed-size fields. Document those
  internal bounds. A full debug queue drops debug detail and increments a fixed
  saturating dropped counter. It must not drop primary or secondary failure
  reporting or turn diagnostic pressure into transfer failure.
- Normal debug output covers state transitions and bounded summaries, not a log
  line for every packet, block, or duplicate announcement. Use fixed counters for
  repeated activity. No persistent metrics, files, exporter, tracing SDK, or new
  command-line/environment controls.
- Redact tracker identifiers to scheme and host before retaining them. Diagnostic
  records must contain no URL userinfo/path/query, magnet URI, raw payload, or
  unbounded error string. Continue terminal escaping and final output bounds
  through the existing reporter.

### F1b — Tracker, metadata, and lifecycle diagnostics

- [ ] Reviewed checkpoint after F1a.

Own the necessary producers in `internal/session/run.go`,
`internal/session/metadata.go`, and `internal/tracker`, plus their tests.

- Observe tracker started/regular/final attempts, transmission versus response
  success, failures, retry/disable outcomes, and peer counts/families. Use existing
  `tracker.Update` information and narrow result fields where it lacks a required
  observation. Capture final-event attempts even after normal admission stops;
  do not make observability depend solely on `pump` continuing during shutdown.
- Report a recoverable tracker failure at warning level while the session keeps
  trying other sources. Coalesce repeated unchanged failures and report recovery;
  keep detailed attempt/retry information at debug. Keep `private=1` warning.
- Observe metadata refusal, peer selection/replacement reasons, phase entry/exit,
  and shutdown/finalization. Preserve all current retry classifications and the
  approved metadata Fast exception. Do not add discovery or change wire behavior.
- Reuse existing safe tracker labels and errors. A tracker error may embed a URL
  independently of its label; sanitize the entire retained/rendered diagnostic.

### F1c — Peer, scheduling, and verification observations

- [ ] Reviewed checkpoint after F1b.

Own producers in `internal/session/transfer.go`, peer/dial integration, and their
tests. Small result fields in `internal/peer` are allowed when the session cannot
observe a required transition otherwise. Keep the scheduler free of logging I/O.

- Observe actual transition/results at their existing owners. Reuse the state
  effects, dial results/errors, request assignments, finalizer results, and
  accepted-block path. Do not reparse frames or duplicate protocol state machines.
- Keep additional per-peer observation state inside currently owned peer objects;
  release it with those objects. Use fixed per-session counters for retired peers,
  not a new unbounded map keyed by every historical endpoint.
- Report hash failure/retry and fatal staging failure without changing strikes,
  blacklists, scheduling, request/tombstone lifetimes, or error propagation from F6.
- Scheduling diagnostics should identify piece assignment/completion/retry,
  outstanding work, endgame entry, and existing reasons that prevent assignment
  such as choking, zero `reqq`, or staging pressure. Emit transitions and summaries
  from existing decisions; do not run another scheduler, scan all pieces for log
  output, or print a line for each block request.

The completed F1 implementation must expose all these observations to local tests:

| Required observation | Meaning and minimum test |
| --- | --- |
| Ordinary choke duration | Measure the remote's ordinary choked state from connection/admission or a choke transition through unchoke/disconnect. Allowed Fast does not clear that state. Use controlled time; no per-tick log spam. |
| Empty availability | Observe an empty initial set and transitions to/from empty; duplicate messages do not create repeated transition events. |
| Allowed Fast receipt and usefulness | Observe valid grants and whether they enable requests for wanted, advertised pieces while choked; distinguish request use from successfully received useful data. No request based on a grant alone. |
| First useful block | The first block accepted for outstanding wanted work and written to staging for that peer; keepalives, duplicate/late blocks, and rejected data do not qualify. |
| Tombstone consumption | An exact late piece/reject consumes one tombstone once, without renewing useful progress or adding a strike. |
| Peer-ID collision | Report the rejected collision while retaining the older live peer and unchanged endpoint penalties. |
| Transport-race outcome | Report the actual winning transport or failed race; preserve loser cancellation/join and handshake-based selection. |
| Metadata refusal | Observe a metadata reject/disabled extension at the existing refusal boundary; preserve its retry classification. |
| Compact peer family | Observe IPv4/IPv6 at compact response decoding/family results. If provenance is absent, add bounded result counts; do not guess compact encoding from an unrelated resolved hostname. |
| Final tracker-event attempts | Observe applicable `completed`/`stopped` attempts, including failure or a missing response to transmitted `started`; never report an unattempted event as sent. |
| Staging or hash failure | Distinguish retryable corruption from fatal storage failure, including the simultaneous case fixed by F6. |

### F1d — Session and CLI acceptance checks

- [ ] Reviewed checkpoint after F1c.

Own focused integration tests and only the corrections needed by those tests.

- A local tracker failure must produce a redacted warning, and debug must add
  tracker detail. Demonstrate real peer, scheduling, and lifecycle debug output
  from a local session; direct calls to `Reporter.Debug` alone are insufficient.
- Use a matrix mapping every observation above to named tests. Prefer extending
  existing narrowly relevant fixtures; do not build a general swarm simulator.
- Verify all four log levels, noninteractive output, stdout separation, status
  restoration after diagnostics, and primary versus secondary failures from F9.
- Inject credentials in URL userinfo/path/query, IPv6 URL punctuation, a complete
  magnet URI, terminal controls, and long errors. None may escape the existing
  redaction/escaping/size contract. Check retained record/queue bounds too.
- Exercise repeated tracker failures and duplicate peer messages under diagnostic
  pressure. Prove bounded retained state, prompt producers, continued transfer,
  and joined reporting. With no observer or with debug disabled, download behavior
  must remain unchanged.
- Keep the no-upload and phase-specific rejection assertions in revised §19.4.
  Observations must come from production paths and never cause storage reads in
  response to incoming payload/metadata requests.

**Starting points:** [reporter](internal/cli/report.go),
[CLI callback wiring](internal/cli/run.go),
[tracker updates](internal/tracker/loop.go),
[metadata admission](internal/session/metadata.go),
[transfer state effects](internal/session/transfer.go),
[peer dialing](internal/peer/dial.go).
**Package checks:** `./internal/cli ./internal/session ./internal/peer ./internal/tracker`;
include `./internal/storage` if changed.

## Validation, handoff, and completion

For each task, first add a meaningful regression that demonstrates the specified
old behavior. Implement the fix, then run the task's package set with:

```sh
go test -count=1 <package-set>
go test -race -count=1 <package-set>
```

Replace `<package-set>` with the packages listed for the task. If Go is absent
from the ordinary PATH, use `bash -ic 'go ...'` as root guidance describes.
Use `gofmt` on changed Go files and `git diff --check`. After relevant tests pass,
do not repeatedly broaden testing without a new change, failure, or concern.

Tests must own and join fixture goroutines, use disposable temporary directories,
and route the mandatory default tracker to an injected local fixture while
asserting its inclusion. Use independently specified protocol bytes and outcomes,
controlled time, and synchronization barriers. Bounded waits detect fixture
failure; arbitrary sleeps must not establish correctness. Do not depend on the
original audit's `/tmp` files, logs, or overlay tests.

Each worker handoff must contain:

- task/subtask ID, base commit, resulting commit(s), and absolute worktree path;
- concise behavior change and files touched;
- named regression checks and exact commands/results;
- new ownership, capacity, or callback rules and their guidance location;
- unresolved defects or scope questions, explicitly separated from completed work.

The reviewer checks the assigned diff against the task and current DESIGN,
verifies tests exercise production behavior, and returns either concrete findings
with file/line evidence or approval with remaining limitations. The orchestrator
marks a task complete only after the reviewed change is integrated and its
acceptance checks pass. Passing historical tests alone is insufficient.

Final integration checks:

- [ ] All six tasks and all F1 observations have reviewed implementations and tests.
- [ ] The three kept decisions and no-upload rules remain unchanged.
- [ ] Applicable nested guidance matches the implemented ownership and behavior;
      root guidance's context map remains accurate.
- [ ] Run `bash -ic 'make check'` on the combined tree: tests, race tests, vet,
      and the pure-Go production build. Resolve failures within their owning task.
- [ ] Verify the diff contains no unrelated edits, temporary audit artifacts,
      new external dependencies, live-network fixtures, or new user-facing controls.

Stop when these checks pass. Further conformance audits, protocol features, and
refactoring are separate work.
