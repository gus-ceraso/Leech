# Outgoing uTP interoperability audit

## Scope and evidence

This incremental audit compares Leech's promised outgoing BEP 29 stream with
[bittorrent/libutp](https://github.com/bittorrent/libutp). Work starts at Leech
`527958d`, on `agent/libutp-audit`. The reference is pinned to
[`2b364cbb0650bdab64a5de2abb4518f9f228ec44`](https://github.com/bittorrent/libutp/tree/2b364cbb0650bdab64a5de2abb4518f9f228ec44)
(2018-05-15, master HEAD fetched for this audit), cloned to
`/tmp/leech-libutp-audit-reference`. Reference line numbers below refer to
[`utp_internal.cpp` at that commit](https://github.com/bittorrent/libutp/blob/2b364cbb0650bdab64a5de2abb4518f9f228ec44/utp_internal.cpp).
The reference-audit agent used no live tracker, swarm, or existing download;
its experiments were local loopback only. The parent's separate integrated live
check is recorded below. Reference C/C++ is an external oracle, not a production
dependency.

All five confirmed baseline defects below are fixed in this change. The report
was created before investigation and updated after material findings and checks.

## Findings

### 1. SYN-ACK STATE incorrectly consumes a receive sequence number — confirmed

- Evidence supplied by the parent: independent wire fixture at
  `/tmp/leech-live.hKOrZe/utp-sequence-check/main.go` sends STATE sequence 500,
  DATA 500 (`A`), DATA 501 (`B`); Leech delivers only `B` (`result.txt`). Its
  temporary module targets the main checkout and must not be reused unchanged.
- Baseline Leech source: `Conn.handleDatagram` initialized reception with
  `NewReceiveState(packet.SeqNr.Add(1))`.
- Reference evidence supplied: `utp_internal.cpp`, `send_ack` does not advance
  `seq_nr`; incoming SYN-ACK handling initializes `ack_nr` to `pk_seq_nr - 1`.
  BEP 29's setup diagram conflicts with its ST_STATE description.
- Impact: the first peer DATA packet is discarded and falsely acknowledged,
  corrupting the reliable byte stream and potentially preventing the BitTorrent
  handshake. Existing STATE N / DATA N+1 fixtures mirror the defect.
- Independent check: new `TestConnStateDoesNotConsumeSequence` failed before
  the fix for STATE sequences 500, 0, and 65535: initial outgoing ACK was N,
  not N−1. Its raw headers do not use Leech's packet encoder; expected stream
  is `AB` then EOF, with duplicate STATE/DATA and wraparound coverage.
- Verified pinned reference: `utp_internal.cpp:771–832` (`send_ack`) and
  `:1870–1875` (SYN-ACK initialization) confirm the parent evidence.
- Status: fixed receive initialization and corrected uTP/CLI peer fixtures,
  including their initial ACK and STATE sequence expectations.
  `go test ./internal/utp -count=1` and focused CLI uTP/IPv6 tests pass.

### 2. Ordinary incoming DATA counts as duplicate-ACK loss evidence — confirmed

- Baseline `SendState.applyACK` incremented `duplicateAcks` for any packet carrying
  the unchanged cumulative ACK. Three DATA packets can spuriously retransmit
  a just-sent request and halve the congestion window, even without loss.
- Pinned libutp `utp_internal.cpp:1910–1943` explicitly counts only ST_STATE
  for this heuristic: streaming DATA is not a response to the local packet.
  SACK evidence remains meaningful on DATA.
- Impact: avoidable retransmission and congestion collapse during downloads.
- Regression check: `TestSendDataIsNotDuplicateACKEvidence` fails before the
  fix on its third incoming DATA packet, reporting one lost packet and
  retransmitting sequence 10. Fixed by counting only STATE duplicates while
  preserving SACK processing on DATA. Full uTP package tests pass.

### 3. Traffic without ACK progress postpones recovery — confirmed

- Baseline `SendState.Handle` refreshed the timeout's `lastActivity` on every
  packet. libutp refreshes its retransmission timer in `ack_packet` when an
  outstanding packet is acknowledged (`utp_internal.cpp:1388–1400`).
- `TestSendTimeoutRequiresACKProgress` fails before the fix for incoming DATA,
  stale STATE, newly sent DATA, and repeated zero-window updates: all postpone
  the expected one-second retry/probe without acknowledging anything. This
  affects both lost requests and persist recovery while downloading.
- BEP 29's prose says to reset on every packet, but that permits indefinite
  starvation; use libutp's ACK-progress timer semantics to preserve reliable
  bidirectional delivery. Fixed timer restarts at new flights, genuine
  cumulative/SACK progress, or timeout, not unrelated traffic or later sends.
  Full uTP package tests pass; DESIGN §14 now records the clarification.

### 4. RESET echoing the send connection ID is discarded — confirmed

- Baseline `Conn.handleDatagram` admitted only `recvID`, even for RESET. libutp
  `utp_internal.cpp:2850–2875` accepts either connection ID; its unknown-socket
  response at `:2947` echoes the offending packet's ID. After SYN, that ID is
  Leech's `sendID` (SYN ID + 1).
- Impact: a restarted peer's valid RESET is ignored, leaving reads/writes to
  wait for unrelated application deadlines instead of reporting reset.
- Regression check: `TestConnResetAcceptsBothConnectionIDs` fails before the
  fix for send IDs 1235 and 0 (wrapping receive ID 65535). The ID exception
  must apply only to validated RESET, not DATA/STATE or arbitrary IDs.
- Status: fixed with a RESET-only send-ID exception before full parsing; full
  uTP package tests pass, including wraparound and malformed RESET rejection.

### 5. Missing delay samples and non-ACK traffic drive congestion control — confirmed

- Baseline `SendState.Handle` passed every packet's delay field and all bytes in flight
  into `ObserveDelay`, before checking what was acknowledged. A zero field
  (BEP 29's explicit “no sample yet”) becomes the minimum baseline; a later
  valid, unsynchronized clock offset can then look like enormous queue delay.
  Incoming payload/duplicate ACKs can also grow or collapse the send window
  without acknowledging any new bytes.
- libutp `utp_internal.cpp:2017–2024,2136–2141` ignores missing delay samples
  and applies congestion gain only to actual newly acknowledged bytes.
- Regression check: `TestSendCongestionRequiresMeasuredDelayAndACKProgress`
  fails before the fix: both a missing sample and unacknowledged incoming DATA
  double a four-byte window to eight. Fixed: ignore missing samples, retain
  real delay history, and apply gain only to newly ACKed payload bytes. The
  existing zero-window restart test now uses a real nonzero baseline sample,
  not the absent-sample sentinel. Full uTP tests pass; libutp's complete
  congestion controller is not imported.
- Additional ACK checks pass: independently encoded DATA SACK still triggers
  fast retransmission; first SACK progress resets backoff, whereas the same
  repeated SACK neither releases bytes twice nor postpones the retry timer.

## Reference tests and licensing

- The pinned tree contains no test directory or golden packet-vector corpus.
  README's `cd utp_test && make` example points to a directory absent from this
  revision. `ucat.c` is a sample application, not a vector suite.
- Historical tests were removed by
  [`dc149387f38de2726e3fe309fce717ab58831eee`](https://github.com/bittorrent/libutp/commit/dc149387f38de2726e3fe309fce717ab58831eee)
  (2013-05-22). Inspected its parent
  `7c4f19abdfe0f781311cdc68e147084961d67424`:
  [`tests/test_transfer.cpp`](https://github.com/bittorrent/libutp/blob/7c4f19abdfe0f781311cdc68e147084961d67424/tests/test_transfer.cpp) has
  simulated loss/reordering, a generated 160 KiB byte pattern, connection/EOF
  checks, and seven wrap-comparison assertions—not golden wire packets. Its
  receive callback explicitly leaves payload equality as a TODO, so byte-count
  success alone would miss corruption. Existing Leech tests are a better home
  for independent invariants than porting this old API's harness.
- Historical `utp_test/README` describes a configurable two-node logging tool;
  `pyutp/utp/tests/test_utp.py` contains Twisted integration tests, not packet
  vectors, and carries a separate Twisted Matrix Laboratories copyright
  (2001–2008). No historical code is copied or executed in Leech.
- The pinned [LICENSE](https://github.com/bittorrent/libutp/blob/2b364cbb0650bdab64a5de2abb4518f9f228ec44/LICENSE)
  grants the MIT license, copyright 2010–2013 BitTorrent, Inc.; the inspected
  historical root LICENSE is MIT, copyright 2010 BitTorrent, Inc. Any copied
  substantive source/tests must retain their applicable notices. No reference
  code or vectors have been copied into Leech.
- Transferable original regressions are in `libutp_regression_test.go` and
  `send_regression_test.go`: STATE N / DATA N with N=500, 0, 65535; both RESET
  IDs including wrap; unchanged DATA ACK versus repeated STATE ACK; one-second
  progress-based retries/probes; absent delay versus measured ACK progress;
  and raw SACK bytes `00 04 07 00 00 00` with ACK 9, selecting 11, 12, 13.
  Expected bytes/actions come from BEP 29 and the pinned source behavior, not
  captured upstream golden vectors. Historical transfer scenarios and wrapping
  assertions are reusable ideas, not an upstream wire-vector corpus.

## Local reference-library experiments

- Built unmodified pinned libutp's `ucat-static` in the temporary clone.
  First IPv4 loopback check completed Leech→libutp setup and delivered the
  exact 48-byte reverse stream, but timed out receiving payload. `ucat` filled
  its stdin buffer before accept, tried writing while still SYN_RECV, and did
  not retry after the first client DATA. Its log contains no transmitted DATA.
  This is an oracle application scheduling limitation, not yet evidence of a
  Leech receive defect. Initial evidence is retained as
  `/tmp/leech-libutp-audit-interop-prefeed.txt` and
  `/tmp/leech-libutp-audit-ucat-prefeed.log`.
- Corrected scheduling: feed `ucat` stdin only after its stdout contains the
  exact reverse stream. The **unmodified library and unmodified application**
  then transferred all 8,388,608 generated bytes to Leech with exact byte
  equality, SHA-256
  `60606d40d8865c0642feb0ef74d2d8793d27b52df9db9d1a60d3c7a1b3bdaa94`;
  the 48-byte reverse stream also matched. `ucat` exits 1 on Leech's deliberate
  local Close RESET. Logs: `/tmp/leech-libutp-audit-interop.txt` and
  `/tmp/leech-libutp-audit-ucat.log`. This check uses ReadFull, not remote EOF;
  no injected loss, IPv6 reference peer, or live swarm is claimed.
- The disposable driver and its worktree-specific module replacement are
  preserved at `/tmp/leech-libutp-audit-oracle/{main.go,go.mod}`, outside the
  commit. Reproduce locally with:
  `make -C /tmp/leech-libutp-audit-reference ucat-static`, then
  `go -C /tmp/leech-libutp-audit-oracle run .`. It generates its own payload
  (`byte(i*31 + i/251)`), uses only loopback, and touches no torrent/download.

## Reviewed boundaries

| Boundary | Evidence and disposition |
| --- | --- |
| Handshake and IDs | SYN retains the receive ID on retry; later packets use ID+1; ACK must match SYN. Fixed initial receive sequence and RESET's two-ID rule. |
| Ordering, duplicates, wrap | Independent first-DATA/wrap/duplicate regression plus existing reassembly, packet, sequence, and scripted-link tests. No further confirmed defect. |
| ACK/SACK and retransmission | Checked ACK bounds, bit numbering, immutable retransmitted payload, refreshed headers, one-time credit, and RTT exclusion for retransmitted packets. Fixed duplicate evidence and progress timers; DATA SACK still works. |
| Windows and persist | Existing bounded queue/reorder tests and lost-reopen/probe-ACK test pass. New regression prevents continuous zero-window updates from starving probes. Limits are unchanged. |
| Delay control | Fixed absent-sample handling and byte-based ACK credit. Retained BEP's RTO floor and existing bounded controller, not libutp feature/tuning parity. |
| FIN/RESET | Existing FIN gap, payload-before-EOF, duplicate FIN, post-FIN, and reset tests pass; new raw first-DATA test also reaches EOF. Local Close remains abortive RESET, not a newly added graceful-drain API. |
| Ownership/deadlines | Reviewed `dial.go` and `conn.go`; existing cancellation, socket-rebind, deadline-change, concurrent I/O, and joined-close tests pass under race. Successful streams still outlive the dial context. |

## Validation

- After all five fixes, these checks pass with Go 1.27.1 linux/amd64:
  - `go test ./... -count=1` (also passed after fixes 1–4);
  - `go test -race ./internal/utp -count=1`;
  - `go test -race ./internal/cli -run '^TestV1UTP' -count=1`;
  - `go vet ./...`;
  - `CGO_ENABLED=0 go build -o /tmp/leech-libutp-audit-leech ./cmd/leech`;
  - `git diff --check`.
- Repeated the unmodified libutp 8 MiB/48-byte loopback check after the delay
  correction: exact byte equality passes again. The mixed-source corruption
  test was not changed or weakened.

- The new focused regressions also pass 20 repeated runs and five repeated
  race-instrumented runs. The preserved temporary driver's module replacement
  was checked with another successful 8 MiB reference transfer. No new fuzzing
  campaign or external-library dependency was added.
- Baseline `go test ./internal/utp` passed before any fix, demonstrating why
  independently specified packet outcomes matter. Each defect has a regression
  observed failing before its fix; source evidence is distinguished above from
  reference-executable results.
- README now removes the known sequence-defect caveat without claiming live
  uTP validation; DESIGN §14 and uTP guidance record the corrected invariants.

## Parent integration and live check

The parent integrated the audit as `b186d86`, logging fixes as `e0cd004`, and the
separate valid-peer-backlog fix as `d8276d7`. A clean build at `86f44e8` passed
`make check` (tests, full race suite, vet, and pure-Go build), then downloaded a
fresh Big Buck Bunny copy under `/home/user/Downloads/leech-integrated.at5rlY`.

- Exit 0; all 1,055 SHA-1 piece hashes independently verified (276,445,467 bytes).
- Transfer 194.544 seconds; session 209.550 seconds, including final announces.
- 19 uTP and 9 TCP handshake winners. Correlating connected peers with first
  accepted/staged block events identifies 10 useful uTP and 4 useful TCP
  connections. This is a mixed-transport live download, not a uTP-only test.
- No hash mismatch, staging failure, or dropped diagnostic was observed. The
  cache workspace was removed and the owned processes joined.
- `debug.log`, `result.json`, `analysis.json`, `verification.json`, binary hash,
  and exact command/build revision are retained in that artifact directory.

This extends validation beyond the reference loopback experiment but does not
establish universal client interoperability or isolate uTP performance. See
[the performance report](../../LIVE_PERFORMANCE.md) for the combined measurements
and their limits.

## Unresolved limits

- The live check used mixed TCP/uTP peers; it did not force a uTP-only download
  or identify and validate every remote client's implementation.
- Actual libutp interoperability was exercised on IPv4 loopback without injected
  loss. Differential loss/reordering, remote zero-window pressure, FIN, and IPv6
  experiments remain future work; existing Go fixtures cover those behaviors
  but cannot prove interoperability with every client or libutp fork.
- The delay controller is still simpler than libutp's. Clock-drift compensation,
  congestion fairness, loss-epoch tuning, MTU behavior, and Internet performance
  were not comprehensively evaluated. This audit does not claim full parity.
- Integrated tests passed, including the known intermittent mixed-source
  corruption test. That unrelated intermittent behavior remains unresolved;
  neither its implementation nor its assertions were weakened.
