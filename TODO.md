# Leech implementation plan

Implement the behavior in [DESIGN.md](DESIGN.md). That document and the local
[BEPs](beps/) remain the specifications; this file organizes the work. Development
milestones deliberately cover subsets of the design. They do not narrow the
finished CLI's promised behavior.

## Working arrangement

- **KISS:** Choose the simplest implementation that satisfies the accepted design.
  Favor short, working iterations; add abstractions, machinery, or polish only
  for a concrete need. Preserve correctness and the required robustness.
- **Adapt the plan:** Update `TODO.md` as implementation and review reveal new
  facts. Add, split, combine, or reorder tasks and revise dependencies when useful.
  If a breaking change to user-visible behavior or an approved design contract
  is required, explain why, its impact, and the smallest proposed change, then
  prompt the user for explicit approval before implementing it. Continue
  unaffected work. Routine planning and internal implementation changes do not
  require approval.
- **Orchestrator:** GPT-5.6 Sol, `high`. Dispatch ready tasks, settle interface
  questions, integrate changes, keep this checklist current, and run integration
  checks. Keep implementation work with the workers when practical.
- **Workers:** GPT-5.6 Luna, `xhigh`, one worker per task ID. A task includes its
  implementation, focused tests, and fixes. Its checkboxes are subtasks for that
  worker, not separate agent assignments.
- **Reviewers:** GPT-5.6 Luna, `xhigh`, one separate reviewer per group, R1–R5.
  Review the integrated group once; return fixes to the responsible workers and
  recheck affected areas. No reviewer for every small task or additional review
  hierarchy. The orchestrator accepts the small A0 bootstrap directly.
- Give each worker its task, dependencies, owned files, relevant design sections,
  and acceptance checks. Workers should report consequential findings and
  interface changes as they discover them, then finish with changed files,
  checks run, and remaining issues.
- Use isolated worktrees when workers share a repository. Keep one active owner
  per production file, including shared types. The paths below are initial
  ownership boundaries; A0 may simplify them before dispatch. The orchestrator
  owns shared-file integration and updates to this plan and root guidance.
- Integrate small, compiling changes as soon as their focused checks pass.
  Dependents need their named prerequisites, not every task in an earlier group.
  Review checkpoints mark group completion; they are not global scheduling
  barriers. A known contract or safety defect blocks affected dependent work.
- Keep handoffs in task messages and this checklist. Do not create a parallel
  reporting system, a generic framework, or a new design document per task.

### Keep iteration short

Establish real bounds, hash verification, output confinement, no-upload behavior,
and cancellation ownership when each boundary first appears. These are cheaper
to preserve than to retrofit. Defer streaming, endgame, complete terminal output,
and the full integration matrix until the basic download path works.

Start uTP early and develop it alongside the TCP path. Do not make the first
local download wait for uTP. Early transfer tests may inject connected TCP peers;
the finished CLI must use the designed uTP/TCP race. Use private test seams, not
temporary user-facing switches, environment settings, or fallback contracts.

Use concrete types, ordinary functions, and small interfaces only where two
components or deterministic tests actually need them. Add fixtures beside the
component that needs them. Extract shared test helpers only after actual reuse.
Avoid speculative optimizations, generalized simulators, plugin architectures,
and arbitrary coverage targets. Required design behavior and regression tests
remain required even when implemented in a later iteration.

## Dispatch and milestones

Dependencies below are prerequisites for dispatch. Tasks without a dependency on
one another can run concurrently if their files do not overlap. Fill available
worker slots from this table; do not launch workers merely to wait. Prioritize
the next runnable milestone and the uTP path when slots are limited.

| Task | Deliverable | Depends on |
| --- | --- | --- |
| A0 | Buildable skeleton, shared contracts, limits | — |
| I1 | Strict bencoding | A0 |
| I2 | CLI arguments and source parsing | A0 |
| I3 | Validated, normalized v1 metadata | I1 |
| I4 | Selection and offline file listing | I2, I3 |
| S1 | Confined output operations | A0 |
| S2 | Disk staging, verification, and commit | I4, S1 |
| S3 | Stateless resume | I4, S1 |
| P1 | Peer framing, handshake, safe outbound API | A0 |
| P2 | Peer request and Fast state | P1 |
| P3 | Candidate admission and transport racing | P1 |
| D1 | Basic rarest-first scheduler | I4, P2 |
| D2 | First local TCP download | D1, S2 |
| D3 | Streaming, endgame, and peer replacement | D2 |
| T1 | HTTP(S) tracker transactions | I1 |
| T2 | UDP tracker transactions | A0 |
| T3 | Tracker lifecycle and accounting | T1, T2 |
| M1 | BEP 10 and metadata messages | I1, P1 |
| M2 | Metadata-only discovery | I2, I3, P2, P3, T3, M1 |
| U1 | uTP packets, sequence arithmetic, test link | A0 |
| U2 | uTP receive state | U1 |
| U3 | uTP send state and congestion control | U1 |
| U4 | Outgoing uTP `net.Conn` | U2, U3 |
| L1 | Logs, terminal status, and signal adapter | I2, D2 |
| L2 | Complete session and CLI wiring | D2, S3, P3, T3, M2, U4, L1 |
| V1 | Complete local integration coverage | L2, D3 |

The first parallel set after A0 is **I1, I2, S1, P1, T2, and U1**, subject to
available slots. Then start T1 and I3 after I1; P2, P3, and M1 after their
prerequisites; and U2/U3 together after U1. Storage, discovery, and uTP continue
while the first TCP transfer is assembled.

| Milestone | Demonstrable result | Required tasks |
| --- | --- | --- |
| 0: Command | Buildable command with help and argument errors | A0, I2 |
| 1: Offline listing | Real `.torrent --list-files`, entirely offline | I1–I4 |
| 2: TCP transfer | Verified selected bytes from a local TCP fixture reach final files | S1, S2, P1, P2, D1, D2, plus their dependencies |
| 3: Metadata | Local tracker-driven metadata acquisition stops cleanly before returning metadata | T1–T3, P2, P3, M1, M2, plus their dependencies |
| 4: uTP | Outgoing uTP stream survives deterministic loss and reordering | U1–U4 |
| 5: Complete CLI | Complete CLI passes the local acceptance suite | All tasks and R1–R5 |

Milestone 2 is an integration test of the transfer path, not a claim that the
public CLI already supports discovery. Milestone 3 uses injected dialers until
U4 is ready. L2 binds the real transports. Keep these limitations explicit in
development documentation until milestone 5 passes.

## A0. Bootstrap and shared contracts

**Owner:** one worker. **Owns:** `go.mod`, `cmd/leech/main.go`, initial shared
types in their owning packages, and `internal/limits/`.
**References:** DESIGN §§6, 15–16, 19.

- [x] Verify the installed stable Go toolchain and create a standard-library-only
  module and buildable command. Keep production pure Go. Use the existing
  `bash -ic 'go ...'` environment when needed.
- [x] Establish only the shared types needed to unblock the table: immutable
  info hash/file/piece metadata, original file indices and byte ranges, selection
  ranges, resolved endpoint identity, and the small worker event/command shapes.
  Keep types with their owning component; avoid a catch-all model package.
- [x] Keep buffer, block, connection, and completion-signal ownership with the
  first component that uses each resource. The session coordinator owns mutable
  torrent state; I/O workers report bounded events. P2, D2, and T3 must specify
  how a canceled producer or full queue unblocks when they add those queues.
- [x] Put every fixed supported-domain limit from DESIGN §16 in one place.
  Validate arithmetic before conversion, allocation, seeking, or duration use.
  Add unspecified operational timings as named constants in the component that
  introduces them; use reasonable initial values without creating tuning flags.
- [x] Establish private dependency injection for clocks, dialers/resolvers, and
  failing I/O only where the first consumers need it. Ensure test construction
  can route the mandatory default tracker locally without changing its inclusion
  in production. Do not build all fake peers and trackers up front.

**Acceptance:** the command builds with `CGO_ENABLED=0`; the initial tests pass;
each ready worker knows its files and the concrete contracts it consumes. The
bootstrap should unblock implementation, not attempt to design every method.

## Group 1: Offline input and storage

### I1. Strict bencoding

**Depends on:** A0. **Owns:** `internal/bencode/`.
**References:** DESIGN §§7.2, 16; BEP 3.

- [x] Implement bounded decoding with exact byte spans, including the raw `info`
  span. Support decoding a bounded prefix where an extension header precedes
  binary payload; full-value callers must reject trailing bytes.
- [x] Reject unsorted or duplicate dictionary keys, invalid lengths/integers,
  negative zero, leading zeros, signed-64-bit overflow, truncation, and excessive
  bytes, depth, values, or container entries before excessive work or allocation.
- [x] Add only the encoding needed for Leech's permitted protocol messages.
  Keep exact received bytes available; never derive an info hash by re-encoding.
- [x] Add independently specified golden bytes and a decoder fuzz target covering
  malformed, truncated, canonical, and near-limit inputs.

**Acceptance:** canonical values decode predictably, invalid inputs fail within
bounds, and exact-span tests distinguish hashing the original bytes from encoding
the decoded structure again.

### I2. CLI arguments and source parsing

**Depends on:** A0. **Owns:** `internal/cli/args.go`, help text and parser tests,
and `internal/torrent/source.go`.
**References:** DESIGN §§4.1, 4.4–4.8, 7.1; BEPs 9, 53.

- [x] Implement every documented option, long-option value form, `--`, one-source
  arity, uncombined short options, and options-before-source rule. Preserve the
  exact defaults and reject list-mode conflicts even when an explicitly supplied
  option equals its default. Validate log levels and positive Go durations.
- [x] Classify case-insensitive `magnet:` first, exact-length hex/Base32 hashes
  second, and paths otherwise. Reject literal `-`; allow `./` to disambiguate a
  hash-shaped filename. Parse and validate without output/cache mutations.
- [x] Parse one effective v1 `btih`, reject conflicts and any `btmh`, and preserve
  `tr`, `x.pe`, display-only `dn`, and bounded `so` indices/ranges. Reject malformed
  endpoints, selections, and unsupported schemes without unbounded expansion.
- [x] Provide tracker URL deduplication and mandatory default-tracker inclusion.
  Keep hostname resolution separate from parsing. Use the same normalization
  when I3 extracts metainfo trackers.
- [x] Add parser tables and fuzz cases for option errors, escaped names, malformed
  magnets, numeric overflow, source precedence, and all accepted hash forms.

**Acceptance:** help exits 0, usage errors exit 2, parsing causes no network or
storage side effects, and the documented CLI examples parse as specified.

### I3. Metainfo validation and normalization

**Depends on:** I1. **Owns:** `internal/torrent/metainfo.go` and normalization
tests. **References:** DESIGN §§7.2–7.3, 8, 16; BEPs 3, 12, 47, 52.

- [x] Bound file reads and metadata bytes, hash the exact `info` span, and accept
  both a full metainfo file and a fetched info dictionary at the appropriate
  boundary. Preserve unknown keys in the hash while ignoring their semantics.
- [x] Validate single-file versus multi-file exclusivity, piece length/count,
  SHA-1 string length, UTF-8 where required, checked total length, and all metadata
  limits. Reject v2 and hybrid structures.
- [x] Build immutable file and piece tables with half-open ranges and every
  original file-list position. Normalize padding without a path, symlinks without
  a length, and ignored attributes correctly. Validate names, path structure,
  duplicates, and file/directory collisions; S1 adds filesystem-specific checks.
- [x] Retain the conventional `info.name` output root. Recognize but ignore
  `private=1` and expose it for the required warning. Extract flattened
  `announce-list` trackers, or `announce` when the list is absent; I2's helper is
  connected during I4 integration, so I3 need not wait for I2.
- [x] Add normalization/range properties, golden info hashes, and metainfo fuzzing,
  including zero-length content, omitted fields, overflow, and malformed tables.

**Acceptance:** invalid metadata fails before output/cache creation; valid
metadata exposes consistent ranges and original indices without storing payload.

### I4. Selection and offline file listing

**Depends on:** I2, I3. **Owns:** `internal/torrent/selection.go` and the initial
local listing path in `internal/cli/run.go`.
**References:** DESIGN §§4.2–4.4, 8; BEPs 47, 53.

- [x] Implement case-sensitive exact and glob selection with `/`, `*`, `?`, and
  `[]`; reject `**` and malformed patterns. A matching directory selects its
  descendants. Union repeated matches without duplicate work.
- [x] Apply explicit selectors instead of magnet `so`. Interpret `so` against
  original file positions, including padding and symlinks; reject selected
  symlinks and selections without regular files. Handle single-file index zero.
- [x] Map selected ranges to wanted pieces, selected output intersections, and
  synthetic zero padding. Preserve unwanted non-padding ranges that a full piece
  still needs for verification. Include selected zero-length files.
- [x] Wire local `.torrent --list-files`: validate metadata, emit JSON-quoted
  selectable paths in torrent order, and omit padding/symlinks. Do not validate
  the destination or touch output/cache paths. Merge tracker sources for later
  download use through I2's helper.
- [x] Cover directory/glob semantics, overlap, no matches, index bounds, padding,
  symlinks, and exact listing output. Prove local listing makes no network calls.

**Acceptance:** offline listing works, and the same immutable selection plan can
drive storage and scheduling without each component interpreting paths again.

### S1. Confined output operations

**Depends on:** A0. **Owns:** `internal/storage/output.go` and path checks.
**References:** DESIGN §§4.2, 4.7, 8, 17–18.

- [x] Resolve the existing destination once, allowing its root to be a symlink.
  Refuse descendant symlinks, unsafe/unrepresentable names, path collisions, and
  incompatible existing entries. Check the complete selected output plan before
  destructive preparation. Keep hostile concurrent filesystem races out of scope.
- [x] Expose separate validation and preparation operations so listing and early
  validation never mutate output. Default preparation truncates only selected
  regular files; resume preserves their contents. Never open unselected paths
  for writing or create torrent-provided symlinks/padding files.
- [x] Create selected zero-length files; otherwise allow verified writes to grow
  files naturally. Confine all writes below the resolved root and close every
  handle. Do not add preallocation or `fsync` requirements.
- [x] Test traversal, separators, root/descendant symlinks, file/directory
  conflicts, target-filesystem representability, sparse growth, and protection
  of existing unselected/unrelated files. Provide narrowly scoped I/O failure
  injection for subsequent storage tasks.

**Acceptance:** unsafe plans fail before truncation; valid selected writes stay
confined; output preparation can be tested independently with A0's file ranges.

### S2. Disk staging, verification, and commit

**Depends on:** I4, S1. **Owns:** `internal/storage/staging.go`,
`internal/storage/finalize.go`, and their tests.
**References:** DESIGN §§13.1, 15–18.

- [x] Create a random private workspace under `os.UserCacheDir()/leech` only on
  an explicit transfer-needed call. Use directory `0700` and file `0600` where
  supported. Enforce staged-piece count and total declared-length budgets before
  admission; never use whole-piece memory or a memory fallback.
- [x] Stage block-sized writes in per-piece random-access files and synthesize
  padding. Keep block coverage and endpoint provenance coordinator-owned; pass
  immutable snapshots to the finalizer when needed. Define block-buffer ownership
  through the write completion event.
- [x] Serialize finalization: hash the whole staged piece using bounded buffers,
  then write only selected intersections. Report mismatch and contributors to
  the coordinator. Remove the piece after mismatch, or after every successful
  selected write and close; never commit corrupt data.
- [x] Make open/read/write/short-write/close/removal errors fatal. Preserve the
  primary error and report secondary cleanup failures. Close/join staging work
  before removing only this run's workspace; ignore abandoned workspaces.
- [x] Test cross-file and skipped-file boundaries, padding, mixed contributors,
  hash failures, budget exhaustion, unavailable/full cache, output failures,
  cleanup failures, and cancellation during finalization.

**Acceptance:** verification precedes output writes; no whole piece is retained
in memory; resource credits and handles return on every tested success/failure
path; deleting staged data never precedes successful output close.

### S3. Stateless resume

**Depends on:** I4, S1. **Owns:** `internal/storage/resume.go` and tests. It may
run alongside S2. **References:** DESIGN §§4.7, 9, 16.

- [x] Reconstruct and hash complete pieces from selected existing regular files
  and synthetic padding, using bounded buffers and no cache workspace/index.
  Mark missing, short, or mismatching content as needing download.
- [x] Require redownload of an entire piece when skipped non-padding ranges
  prevent reconstruction. Do not infer validity from file length, previous
  progress, or only the selected part of a piece.
- [x] Hash only the expected prefix of an overlong selected file; truncate its
  excess only after that file's selected content validates successfully. Return
  any necessary pending truncation to the transfer path for later completion.
- [x] Return verified selected ranges and whole-torrent retained-byte accounting,
  plus an explicit no-transfer-needed result. Preserve unselected output and
  support selected zero-length files and canceled scans.
- [x] Test missing/short/overlong files, piece boundaries across files, padding,
  partial selection, mismatches, read/truncate failures, and a fully valid resume.

**Acceptance:** scan results derive only from SHA-1 verification. L2 can run the
scan with no network workers and can exit without creating a workspace or
starting transfer discovery when selection is already complete.

- [x] **R1 — Offline/storage review:** one reviewer checks I1–I4 and S1–S3
  together. Focus on validation before mutation, original indices, exact hashes,
  selected-range confinement, bounded memory, and error/cleanup behavior. Verify
  offline listing and the storage regression tests; do not wait for networking
  to review this group.

  R1 fixes to recheck:

  - [x] I1: reject duplicate empty dictionary keys (`6061e62`).
  - [x] I3: reject an explicitly empty padding `path` list and invalid UTF-8
    metainfo announce URLs (`8fdb1d0`).
  - [x] I4: bound selection work for up to 100,000 files and patterns without
    narrowing the supported selection domain (`6eaef77`, corrected class
    handling in `e08df8f`).
  - [x] S3: scan resume mappings one at a time to avoid cloning millions of
    piece descriptors and range slices (`c272bb2`).

## Group 2: Peer protocol and the first download

### P1. Peer framing and the no-upload API

**Depends on:** A0. **Owns:** `internal/peer/wire.go`, `handshake.go`, and golden
fixtures. **References:** DESIGN §§11–12, 16–17; BEPs 3, 4, 6.

- [x] Parse and validate the BEP 3 handshake, info hash, optional expected peer ID,
  and reserved bits. Read bounded frames and keepalives from any `net.Conn`;
  reject oversized/truncated/invalid frames before payload-sized allocation.
- [x] Encode only permitted local control, request, cancel, and reject messages.
  Provide no outbound file `piece` or metadata `data` encoder and no storage
  access from incoming-request handling.
- [x] With Fast, emit immediate `Have None` as the sole local availability
  message; otherwise omit the bitfield. Never emit `Have`, `Bitfield`, `Have All`,
  or `Unchoke`. Keep extension-specific encoding with M1.
- [x] Distinguish severe protocol violations from ordinary disconnects and
  unsupported cooperation. Ignore bounded unknown core IDs; validate known
  messages against negotiated bits and valid indices.
- [x] Add independent wire vectors, fragmented-I/O tests, handshake/framing fuzz
  targets, and an outbound-message allowlist assertion.

**Acceptance:** captured outbound traffic preserves no-upload/no-availability
behavior, and malformed handshakes/frames return actionable classifications to
the coordinator without allocating beyond the supported bounds.

### P2. Requests, Fast, and per-peer state

**Depends on:** P1. **Owns:** `internal/peer/state.go`, `requests.go`, and bounded
connection I/O workers. **References:** DESIGN §12; BEPs 3, 6.

- [x] Represent ordinary availability and Allowed Fast separately. Incoming
  `Have None` clears only availability; choked requests require both availability
  and Allowed Fast. Parse Suggest Piece without needing a scheduling policy.
- [x] Bound requests and clamp `reqq`; require exact piece/begin/length response
  matching. Fast choke retains outstanding terminal obligations. Cancel and
  local timeout create bounded tombstones, not forgotten requests.
- [x] Consume one exact late piece/reject per tombstone. Close without a strike
  before the cap would require forgetting one. Release non-Fast requests on
  choke while retaining bounded protection for allowed late piece races.
- [x] Reject each admissible incoming Fast payload request exactly once; ignore
  non-Fast requests. Handle abusive repetition as specified. Incoming requests
  must have no path to storage reads. Update interested/not-interested from
  useful advertised availability and keep otherwise useful idle peers alive.
- [x] Keep protocol state under coordinator ownership and connection I/O in
  bounded workers. Test choke/cancel/reject/timeout permutations, unknown frames,
  stale availability, terminal-response duplication, and blocked-queue shutdown;
  fuzz transitions and run race tests for the I/O boundary.

**Acceptance:** local peer fixtures cover Fast seeds, ordinary peers, Allowed
Fast-only cooperation, and uncooperative peers. No unavailable Allowed Fast piece
is requested, and compatibility failure never becomes a corruption strike.

### P3. Candidate admission and handshake racing

**Depends on:** P1. **Owns:** `internal/peer/candidates.go`, `dial.go`, and race
tests. **References:** DESIGN §§11, 15–16.

- [x] Resolve and normalize bounded endpoint candidates keyed by IP and port.
  Permit loopback/private unicast addresses; reject invalid ports, unspecified,
  and multicast addresses. Bound DNS results, candidates, races, and live peers
  before spawning work; deduplicate across sources and transports.
- [x] Race injected uTP and TCP dial functions against the exact same resolved
  endpoint, with uTP's short head start. Win only after a valid BEP 3 handshake;
  then cancel, close, and join the loser. Inspect phase capabilities afterward
  without reviving the loser. Bind the actual uTP dialer in L2.
- [x] Do not deduplicate on tracker-supplied peer IDs. Apply optional expected-ID
  validation and retain the older established connection on a live peer-ID
  collision. Release the ID when it closes, permitting later connections.
- [x] Keep ordinary failure backoff and endpoint blacklist state independent of
  transport and peer-ID spoofing. Expose bounded outcomes to the coordinator,
  which owns admission/penalty decisions.
- [x] Use controlled dialers/clocks to test handshake races, a connected socket
  with a stalled handshake, both failures, cancellation, duplicate IDs,
  reconnects, DNS changes, and IPv4/IPv6. Fuzz event ordering and run race tests.

**Acceptance:** one endpoint consumes one race slot; no loser or canceled dial
survives completion; ordinary failure does not poison a peer ID or incur a
corruption strike. These tests need no working uTP implementation.

### D1. Basic piece and block scheduling

**Depends on:** I4, P2. **Owns:** `internal/session/scheduler.go` and pure-state
tests. **References:** DESIGN §§8, 12–13, 16.

- [x] Track wanted pieces, connected-peer rarity, block state, selected-byte
  progress, and contributor endpoints under one coordinator. Choose rarest-first
  with cryptographically randomized ties; accept controlled randomness in tests.
- [x] Split requests into at most 16 KiB without crossing piece boundaries or
  requesting padding. Stage whole wanted pieces while committing only selected
  intersections. Initially keep at most one active request per block.
- [x] Enforce per-peer/global request caps, queue capacity, staged-piece count,
  and staged-byte admission together. Return work after disconnect or rejection;
  avoid spinning when no useful work is available.
- [x] Turn successful verification/commit results into completion/progress. On a
  hash mismatch, reschedule and add one strike per distinct contributing endpoint.
  Blacklist at three strikes across reconnects/transports; severe violations blacklist
  immediately. Keep compatibility and ordinary timeouts out of strike accounting.
- [x] Add properties for coverage, rarity changes, padding, request budgets,
  progress, mixed contributors, and strike deduplication; fuzz scheduler events.

**Acceptance:** a deterministic sequence of peer events produces bounded valid
requests and correct completion/penalties. Streaming and endgame remain D3 work.

### D2. First local TCP transfer

**Depends on:** D1, S2. **Owns:** `internal/session/transfer.go` and its local TCP
integration fixtures. **References:** DESIGN §§6, 13, 15, 18–19.

- [x] Connect the coordinator, peer I/O, scheduler, staged writes, and serialized
  finalizer. Accept test-supplied handshaken TCP connections so this milestone
  does not depend on trackers, metadata acquisition, or uTP.
- [x] Define explicit transfer completion, cancellation, and storage-failure
  paths. Stop scheduling, unblock and join workers, settle the current bounded
  output operation, close handles, and remove the current workspace.
- [x] Download a spec-derived single-file fixture, then selected multi-file
  ranges with padding and a skipped-file boundary. Verify final bytes and prove
  unselected files are absent. Exercise incoming payload requests throughout.
- [x] Add corruption/retry, disconnect/reassignment, cancellation, and fatal
  storage-failure scenarios. Capture bounded read-only observations needed for
  assertions; do not build a metrics subsystem.

**Acceptance:** the TCP milestone passes with wire and storage no-upload assertions,
verified output, no leaked workers, and race-detector coverage. The test peer
uses independent expected wire bytes rather than trusting Leech's own encoder.

### D3. Streaming, endgame, and replacement

**Depends on:** D2. **Owns:** subsequent changes to `internal/session/scheduler.go`
and peer-replacement policy. **References:** DESIGN §§4.6, 12–13.

- [ ] Add sequential priority for streaming while using later available pieces
  when earlier ones would leave a useful connection idle.
- [ ] Enter endgame only after every remaining block has an assignment. Duplicate
  within existing budgets, accept the first response once, cancel the others,
  and preserve P2's terminal/tombstone obligations for late responses.
- [ ] Rotate persistently choked/unproductive peers while retaining useful data
  suppliers and useful Allowed Fast peers. Use fixed initial timings and ordinary
  endpoint backoff, not corruption penalties or extra upload behavior.
- [ ] Test changing availability, scarce pieces, winner/late-response races,
  duplicate payload accounting, tombstone pressure, replacement, and reconnects.
  Add event-sequence fuzz coverage for the new transitions.

**Acceptance:** bulk behavior still passes, streaming progresses without needless
idle connections, and endgame cannot double-commit a block or leak request slots.

- [ ] **R2 — Peer/transfer review:** one reviewer checks P1–P3 and D1–D3. Inspect
  outbound API reachability, independent availability/Allowed Fast, terminal
  obligations, exact-endpoint races, coordinator ownership, and corruption
  attribution. Run the first-download and concurrent-state regressions.

## Group 3: Trackers and metadata discovery

### T1. HTTP(S) tracker transactions

**Depends on:** I1. **Owns:** `internal/tracker/http.go` and HTTP fixtures.
**References:** DESIGN §§10.2, 10.4, 16; BEPs 3, 7, 23, 31.

- [x] Build announces with exact binary info-hash/peer-ID encoding. Remove every
  existing Leech-owned parameter plus `ip`, `ipv4`, and `ipv6`, then add one
  authoritative value for each applicable parameter. Preserve unrelated tracker
  data and apply the same sanitization to every redirect target.
- [x] Use standard TLS verification, bounded bodies, cancellation/deadlines, and
  normal redirect limits. Report whether the complete announce was transmitted
  independently of whether any response was received or parsed successfully.
- [x] Parse dictionary peers, compact IPv4, and compact IPv6. Validate a complete
  compact string's stride before dropping excess whole records. A successful
  HTTP status with a bencoded failure is still a tracker failure.
- [x] Parse intervals and BEP 31 integer/decimal-string retry minutes with checked
  conversions. Distinguish transient failure, definitive HTTP client failure,
  `never`, and invalid delays so T3 can apply the correct policy.
- [x] Test local HTTP/TLS servers, redirects, duplicate-query first-value/last-value
  parsers, malformed bodies, response loss after transmission, family variants,
  credentials in URLs, and response-size limits; fuzz response parsing.

**Acceptance:** each emitted request has authoritative counters/identity,
redirects cannot restore supplied announce values, and failures cannot become
successful tracker activations. Diagnostic data is safe for L1 to render.

### T2. UDP tracker transactions

**Depends on:** A0. **Owns:** `internal/tracker/udp.go` and UDP fixtures.
**References:** DESIGN §§10.3–10.4, 16; BEPs 7, 15, 41.

- [x] Implement connect/announce messages, connection-ID validity, transaction
  and action checks, tracker errors, and bounded datagrams. A mismatched
  transaction ID is a tracker-local failure, never an accepted response.
- [x] Follow BEP 15's `15 × 2^n` transaction retransmission schedule and reconnect
  when the connection ID expires. Keep transaction retries distinct from T3's
  tracker-loop backoff and interruptible by the final-event deadline.
- [x] Encode BEP 41 URL data and parse complete IPv4/IPv6 peer strides. For a
  dual-stack hostname, announce to one resolved endpoint per available family
  with the same session identity; handle unequal family support.
- [x] Report transmission before awaiting a response. Test exact packet bytes,
  loss/retry schedules using controlled time, ID expiry, malformed/mismatched
  replies, URL data, family handling, and cancellation; fuzz packet decoding.

**Acceptance:** the local model observes BEP 15 transactions and cancellation
without real-time retry sleeps; no malformed datagram activates a tracker.

### T3. Independent tracker lifecycle and accounting

**Depends on:** T1, T2. **Owns:** `internal/tracker/loop.go`, session identity and
announce snapshot helpers, and lifecycle tests.
**References:** DESIGN §§6, 10, 15–16; BEP 31.

- [x] Generate one opaque cryptographically random 20-byte peer ID, tracker key,
  and dynamic-range announced port per run. Reuse them across phases, trackers,
  and families; never probe, reserve, bind, or listen on the announced port.
- [x] Run each unique tracker independently, including the mandatory default.
  Start every phase with `started`; track transmitted-started separately from
  activation. Isolate tracker failures, honor capped exponential backoff/jitter,
  and disable permanent failures or invalid intervals/retry delays for the run.
- [x] Never shorten a not-before time. Allow HTTP peer-depletion rerequests;
  enforce UDP intervals except for defined events. Retry indefinitely by default
  and remain interruptible during every wait/transaction.
- [x] Consume coordinator snapshots: `uploaded=0`, received file payload for
  `downloaded` including duplicates/corruption, metadata-phase `left=1`, then
  whole-torrent retained-byte `left` excluding synthetic padding. Do not mistake
  selected-byte completion for full-torrent completion.
- [x] Stop and join regular loops before separate bounded final-event operations.
  Full completion sends `completed` then `stopped`; other exits send `stopped`
  only. Attempt stopped for every eligible transmitted-started tracker even
  without a response. Final failures are secondary; no regular announce follows.
- [x] Test/fuzz phase/event ordering, empty candidate pools, failed activation,
  no-response started, interval/delay limits, cancellation, disabled trackers,
  partial/full completion, and no normal announce after finalization. Run race
  tests around loop termination and final operations.

**Acceptance:** recorded request traces satisfy ordering and counters for each
tracker independently, and final announcements cannot keep the process alive
beyond their bounded shutdown budget.

### M1. Extension protocol and metadata messages

**Depends on:** I1, P1. **Owns:** `internal/peer/extensions.go`, `metadata.go`,
and codec/state tests. **References:** DESIGN §§7.4, 12.3; BEPs 9, 10.

- [x] Maintain per-connection extension maps: local IDs dispatch received
  messages; remote IDs encode outgoing messages. Apply repeated handshakes as
  additive enable/disable updates and ignore bounded unknown extensions.
- [x] Encode metadata requests/rejects only. Parse received metadata data with
  a bounded bencoded header and exact block bytes; validate message fields,
  indices, block lengths, advertised size, and repeated `total_size`.
- [x] Reject an incoming metadata request exactly once when the peer currently
  provides a usable remote `ut_metadata` ID; otherwise ignore it. Expose no
  metadata-data encoder and perform no storage read for a request.
- [x] Test differing local/remote IDs, ID changes and disablement, repeated
  handshakes, unknown extensions, malformed blocks, and rejection counts.
  Fuzz extension and metadata transitions, not just decoding.

**Acceptance:** a peer can use a different ID in each direction and change its
mapping without misdispatch or upload. Invalid messages remain bounded and
produce the intended peer-local error classification.

### M2. Metadata-only acquisition

**Depends on:** I2, I3, P2, P3, T3, M1. **Owns:** `internal/session/metadata.go` and
local metadata-discovery tests. **References:** DESIGN §§6, 7.4, 15.

- [ ] Start independent metadata trackers with `left=1`, accept embedded/tracker
  candidates, and keep only handshake winners that support metadata. Use P3's
  dialer seams; real uTP wiring is L2's responsibility.
- [ ] Try the first bounded advertised size without a consensus wait. Have one
  endpoint supply the complete candidate with the metadata request cap; do not
  combine suppliers. Keep candidate data only in bounded run memory.
- [ ] Validate blocks, complete canonical bencoding, and exact info hash. Give
  the sole supplier one strike for a complete invalid candidate, retaining
  endpoint penalties across retries and later phases. Rotate peers or advertised
  sizes after failure; treat ordinary rejection/timeouts as ordinary failures.
- [ ] Once the candidate passes hash/bencoding validation, cancel and join
  tracker loops, dials, and metadata peers, then attempt bounded stopped events
  before full normalization. Return immutable metadata and bounded retained
  endpoint values, never a live network worker or cache workspace.
- [ ] Test magnet/bare hashes, wrong sizes/hashes, refusing peers, ID changes,
  corruption strikes, repeated retry/cancellation, tracker loss, and the absence
  of file-payload requests. Assert the phase boundary under the race detector.

**Acceptance:** the metadata milestone obtains valid metainfo from local fixtures
and returns only after workers stop. It creates no piece workspace/output,
serves no metadata, and is not subject to the file-transfer no-progress timeout.

- [ ] **R3 — Discovery review:** one reviewer checks T1–T3 and M1–M2 together.
  Focus on HTTP sanitization, UDP timing, transmitted-versus-successful started,
  final events, whole-torrent accounting, directional extension IDs, single-source
  metadata, and phase quiescence. Use captured local request traces as evidence.

## Group 4: uTP, developed alongside TCP

### U1. Packets, sequence arithmetic, and a deterministic test link

**Depends on:** A0. **Owns:** `internal/utp/packet.go`, `sequence.go`, and the
small test-only datagram link. **References:** DESIGN §§14, 16; BEP 29.

- [x] Implement v1 headers, packet types, extension chains, selective-ACK bits,
  connection-ID representation, and checked timestamp/sequence wraparound.
  Reject malformed packets/extensions and enforce datagram bounds.
- [x] Define the narrow packet/action contracts shared by U2 and U3 so their
  state logic can develop independently. Keep socket ownership for U4.
- [x] Build a deterministic test link supporting clock advancement and scripted
  packet loss, delay, reordering, and duplication. Keep it specific to uTP tests;
  do not build a generic network simulation service.
- [x] Add independent BEP-derived packet vectors, sequence/SACK properties, and
  packet fuzzing. Supply reusable wraparound and adversarial packet cases.

**Acceptance:** U2/U3 can test state changes without sockets or wall-clock sleeps,
and malformed packet input cannot drive unbounded parsing or allocation.

### U2. Receive windows and ordered reassembly

**Depends on:** U1. **Owns:** `internal/utp/receive.go` and receive-state tests.
**References:** DESIGN §§14, 16; BEP 29.

- [x] Reassemble an ordered byte stream, handling duplicates, missing packets,
  out-of-order data, sequence wraparound, and consumption by partial reads.
- [x] Generate ACK/selective-ACK state and advertise receive capacity from bounded
  storage. Enforce both packet-count and byte limits without treating a valid
  peer's window pressure as permission to allocate more.
- [x] Track receive-side FIN/RESET state for U4; preserve bytes preceding FIN and
  define when reads return EOF or an error. Reject invalid connection/state input
  according to the transport contract.
- [x] Test/fuzz loss/reordering/duplication, gaps across wraparound, receive-window
  exhaustion/reopening, FIN before missing data, reset, and cancellation actions.

**Acceptance:** the received stream has neither gaps nor duplicate bytes; ACKs
describe actual retained packets; receive memory remains within the fixed caps.

### U3. Sending, recovery, and congestion control

**Depends on:** U1. **Owns:** `internal/utp/send.go`, `congestion.go`, and send-state
tests. May run alongside U2. **References:** DESIGN §§14, 16; BEP 29.

- [x] Bound queued bytes and unacknowledged packets, segment writes, and respect
  the remote receive window. Process cumulative and selective ACKs without
  releasing bytes twice or advancing from invalid acknowledgments.
- [x] Implement RTT/RTO estimation, retransmission, duplicate-ACK loss detection,
  timeout backoff, and recovery from window pressure per BEP 29. Use controlled
  time for tests; no independent unowned retry goroutines.
- [x] Implement BEP 29's delay-based congestion control and packet sizing. Start
  with the specified algorithm and constants; defer performance tuning rather
  than substituting an unrestricted sender or omitting congestion control.
- [x] Account for timestamp/sequence wraparound and define send-side SYN/FIN
  retransmission actions for U4. Keep queued transport bytes distinct from
  torrent-payload upload, which the peer API already forbids.
- [x] Test/fuzz ACK/SACK combinations, reordering, retransmission ambiguity,
  timeout/backoff, changing windows, delay samples, send limits, and wraparound.

**Acceptance:** deterministic traces demonstrate recovery without duplicate
delivery or exceeding local/remote windows, and congestion response follows the
local BEP rather than an invented approximation.

### U4. Outgoing `net.Conn` and transport integration

**Depends on:** U2, U3. **Owns:** `internal/utp/conn.go`, `dial.go`, and full
transport tests. **References:** DESIGN §§11, 14–16; BEP 29.

- [ ] Combine the state logic behind one outgoing connection using a connected
  UDP socket per attempt. Implement SYN setup, connection-ID rules, IPv4/IPv6,
  FIN/RESET, and protocol teardown. Expose no listener or inbound-SYN/server API.
- [x] Implement `Read`, `Write`, addresses, deadlines, context-aware dialing, and
  idempotent `Close` with `net.Conn` concurrency semantics. Unblock all pending
  I/O on deadline, cancellation, reset, or close and join owned workers.
- [ ] Exercise complete streams over the deterministic link with loss, delay,
  reordering, duplication, SACKs, window pressure, timeout, and wraparound. Add
  real loopback UDP tests for socket/address/deadline behavior.
- [ ] Test P3-style cancellation while dialing/handshaking and a peer handshake
  over the stream. Verify concurrent read/write/deadline/close behavior under
  the race detector and fuzz transport transitions.

**Acceptance:** the transport milestone passes with bounded packets/bytes and
joined workers. The peer layer sees an ordinary `net.Conn`; it needs no uTP
branches. Correctness is demonstrated by independent fixtures, not just two
copies of this implementation successfully talking to each other.

- [ ] **R4 — uTP review:** one reviewer checks U1–U4 as a major task group.
  Compare packet/state behavior directly with local BEP 29, especially sequence
  arithmetic, SACKs, retransmission, congestion control, window bounds, and
  cancellation. Keep tuning suggestions separate from correctness fixes.

  Initial R4 review fixes are integrated: outgoing ACK/window/delay fields and
  STATE ACK headers, handshake RESET rejection, post-FIN delivery, and bounded
  congestion arithmetic. Recheck those fixes and full-stream coverage before
  marking R4 complete.

## Group 5: Complete CLI and lifecycle

### L1. Logs, status, and signal adapter

**Depends on:** I2, D2. **Owns:** `internal/cli/report.go`, `signals*.go`, and
presentation/signal tests. This can run while uTP and metadata work continue.
**References:** DESIGN §§4.5, 4.9–4.10, 15.

- [x] Implement the exact log levels/default, permanent phase/result lines, and
  stdout/stderr separation. Quote/escape untrusted names; redact tracker URL
  userinfo/path/query and never echo a full magnet, including through wrapped
  errors. Keep diagnostics bounded.
- [x] At info/debug on an interactive stderr, show one replaceable line updated
  at most once per second, with the defined phase/progress fields. Noninteractive
  stderr gets enabled permanent lines only. Use no color or terminal dependency;
  isolate the minimal platform-specific terminal detection if needed.
- [x] Report no-transfer-needed resume, selection-versus-torrent completion,
  resumable verified partial output, and ignored `private=1` accurately. Keep a
  primary error distinct from secondary shutdown diagnostics.
- [x] Implement the signal adapter: first SIGINT/SIGTERM requests graceful
  cancellation with the right exit reason; a second may terminate immediately.
  Keep process exit out of the reusable session code.
- [x] Add golden log/status tests using injected terminal/time state and helper
  process tests for signal behavior. Include escaped control characters and
  credential-bearing URLs in error cases.

**Acceptance:** user-visible output matches the CLI contract without leaking
untrusted terminal controls or tracker credentials; signal tests cannot terminate
the test runner. L2 receives a small reporter/cancellation adapter.

### L2. Complete session orchestration and executable wiring

**Depends on:** D2, S3, P3, T3, M2, U4, L1.
**Owns:** `internal/session/run.go`, lifecycle integration, and final changes to
`internal/cli/run.go` and `cmd/leech/main.go`.
**References:** DESIGN §§4, 6, 9–11, 15, 18.

- [ ] Wire `.torrent`, magnet, and bare-hash flows through one explicit lifecycle.
  Keep run identity/strikes across phases. Bind real TCP/uTP dialers, independent
  trackers, the scheduler, storage, and reporter without production test switches.
- [ ] For known metadata, validate/select and finish resume before tracker/peer
  activity. For fetched metadata, join metadata discovery and finish its stopped
  sequence before normalization/selection/resume. Start transfer with fresh
  started events and real `left` only if selected content is missing.
- [ ] Finish listing immediately after validated metadata, including remote
  metadata-phase cleanup, without selection/resume/destination access. Finish a
  complete resume without transfer discovery or piece-cache creation. Prepare
  default overwrite only after metadata, selection, and path validation.
- [ ] Start the optional no-progress timer only at file transfer; reset it only
  for a newly verified file piece. Metadata, resume, duplicates, mere received
  bytes, and tracker responses must not extend it. With no option, keep retrying.
- [ ] Implement the complete ordered shutdown from DESIGN §15: stop admission,
  join regular trackers, join dials/peers, settle/join finalization, attempt
  applicable completed/stopped, close resources, and remove this workspace.
  Preserve the primary result; cleanup failure changes an otherwise successful
  result, while final tracker-event failures remain secondary.
- [ ] Map help/list/download outcomes, usage failures, runtime failures, and
  supported signals to 0/2/1/130/143 as specified. Apply pending overlong-file
  truncation only when that file has validated. Cover empty/zero-length selected
  content and final output-close failures.

**Acceptance:** the actual command runs complete local-fixture sessions in both
transports, with correct listing/resume/overwrite/timeout/signal behavior. No
worker crosses a phase boundary or outlives orderly return.

### V1. Remaining integration and failure coverage

**Depends on:** L2, D3. **Owns:** cross-component integration tests, missing
regressions, and final usage/build documentation. Reuse the earlier local fixtures.
**References:** DESIGN §19 and the coverage map below.

- [ ] Close gaps across `.torrent`/magnet/bare-hash input; HTTP/UDP discovery;
  IPv4/IPv6; TCP/uTP winners; full/selective output; bulk/streaming; overwrite/
  resume; and local/remote listings. Use representative combinations, not a
  mechanically exhaustive Cartesian product.
- [ ] Assert mandatory default-tracker inclusion while routing every resolver,
  tracker transport, and peer dial to controlled local fixtures. Test helpers may
  provide dependencies to the same CLI/session entry points. Do not contact live
  trackers/clients, add test-only user flags, or silently skip protocol families.
- [ ] Cover cancellation and first-signal cleanup during discovery, dialing,
  resume, transfer, and finalization; second-signal immediate termination;
  blocked queues; no-progress expiry; mixed-source corruption; and injected
  cache/output/close/removal failures. Assert bounded final-event deadlines.
- [ ] Capture outbound peer traffic and instrument storage boundaries throughout
  complete sessions. Prove payload/metadata requests cause no payload/cache/
  output reads, no payload responses, no availability/unchoke messages, and the
  specified rejection counts. Assert `uploaded=0` on every announce.
- [ ] Verify fuzz targets and seed cases exist for each parser/state boundary
  required by DESIGN §19, including racing, deduplication, strikes, and shutdown.
  Run bounded fuzz sessions and the relevant race tests; retain discovered
  regressions. Reuse earlier evidence when the implementation has not changed.
- [ ] Update README with build/run examples, supported behavior, and the actual
  implementation status. Update durable AGENTS guidance only where implementation
  reveals a useful command, boundary, or pitfall. Keep temporary task history out
  of guidance and do not claim external interoperability evidence.

**Acceptance:** every promised design behavior has implementation and local
evidence. Remaining follow-ups concern measured tuning or out-of-scope ideas,
not silently omitted requirements.

- [ ] **R5 — Final integration review:** one reviewer checks L1, L2, and V1,
  plus the interfaces between already reviewed groups. Verify phase ordering,
  CLI side effects, shutdown/error precedence, default tracker routing in tests,
  and the coverage map. Revisit earlier internals only when integration changes
  or new evidence warrant it.

## Coverage and completion

This map assigns the cross-cutting requirements to concrete tasks. Workers add
tests with the behavior; V1 fills integration gaps instead of becoming a deferred
testing phase.

| Design area | Implementation owners | Main evidence |
| --- | --- | --- |
| CLI parsing, selection, listing (§4) | I2, I4, L1, L2 | Parser/golden cases, offline and remote-listing side effects |
| Protocol profile and overrides (§5) | I2, I3, P1, T3, M2, L2 | Default tracker, public private-marker behavior, no availability/listener |
| Source, hashes, metainfo (§7) | I1–I3, M1, M2 | Exact-byte vectors, normalization properties, fuzzing |
| Output mapping and resume (§§8–9) | I4, S1–S3, L2 | Filesystem boundaries, skipped bytes, overlong/zero-length files |
| Tracker transactions/lifecycle (§10) | T1–T3 | Independent request traces, retries, sanitization, event ordering |
| Candidate identity/racing (§11) | P3, L2 | Controlled races, ID collisions, reconnect and loser join |
| No upload and peer state (§12) | P1, P2, M1, D2, V1 | Wire allowlist, rejection counts, storage-read assertions, fuzzing |
| Scheduling, corruption, staging (§13) | S2, D1–D3 | Coverage/provenance properties, endgame, hash and disk failures |
| Outgoing uTP (§14) | U1–U4 | Independent packets, simulated loss/windows/wraparound, race tests |
| Phases, timeout, shutdown (§§6, 15, 18) | T3, M2, D2, L1, L2, V1 | No overlapping phases, joined workers, signal/failure traces |
| Limits and trust boundaries (§§16–17) | A0 and each boundary owner | Limit/overflow cases, bounded queues/buffers, confinement |

For each group, the reviewer blocks completion on contract violations, incorrect
data, unbounded resource use, ownership/shutdown bugs, and missing evidence for a
required invariant. Style preferences and speculative performance improvements
are advisory. The orchestrator resolves findings; do not add another reviewer
merely because there is a disagreement.

Before marking the implementation complete:

- [ ] All task checkboxes and R1–R5 are complete; any breaking change has explicit
  user approval and a matching design update. Development subsets are not
  described as the complete supported client.
- [ ] Formatting, `go vet ./...`, and `go test ./...` pass on the integrated tree.
- [ ] `go test -race ./...` passes with the toolchain support the race detector
  needs. This does not relax the pure-Go production rule.
- [ ] `CGO_ENABLED=0 go build ./cmd/leech` succeeds with no third-party modules.
- [ ] Required bounded fuzz runs and deterministic network/filesystem regressions
  have passed. Run focused tests during work and these integrated checks once on
  the final tree; repeat only after relevant changes or failures.
- [ ] Validation has used no existing BitTorrent clients or live trackers. State
  that limit in the completion report rather than implying tested compatibility.
