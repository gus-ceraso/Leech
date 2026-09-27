# Live download performance investigation

## Findings and integration

**Completed, 2026-09-27. No investigation-owned processes remain active.**
Worktree: `/home/user/Desktop/Leech-live-performance`; branch:
`agent/live-performance`; baseline: `527958dc5ec49606b7f333721da0cd243442c83a`.
No push, other-worktree changes, host/network changes, or subagents.

1. **Confirmed Leech defect: valid response bursts disconnect productive TCP
   peers.** The baseline disconnects a peer when its bounded event queue remains
   nonempty after processing 16 messages. Ordinary requested Piece replies can
   satisfy this condition. A deterministic local fixture reproduces it; a live
   observation recorded **17 such retirements among 19 useful connections**.
2. **Confirmed constraint: endpoint races are serial in the CLI.** Only one runs
   at a time, not 32. Dead endpoints occupy that admission path for up to two
   seconds each. This slows finding/replacing useful peers, but its independent
   effect on completion time has not been measured.
3. **Swarm/profile limitations remain.** Tracker-only discovery, no upload,
   choking, unreachable endpoints, and the separately owned uTP defect all matter.
   They do not explain Leech explicitly discarding unchoked TCP contributors.
4. **No evidence that bulk disk throughput, memory, or CPU saturation explains
   the slow baseline.** Measured staging/finalization wall time was small.
   The event-drain policy turns ordinary bursts and scheduling delays into
   disconnections even when aggregate resource utilization is low.

**Integrated:** the parent reviewed proposed patch
`5dceb5e47658bf16f70a6671a3f78966ddea9660` and applied it to main as `d8276d7`.
It yields instead of disconnecting,
retaining the 16-message work bound, four-slot event queue, deferred assignment
while events remain, supported request/staging bounds, no-upload boundary, and
verification-before-output. It includes a regression and owning guidance.
All temporary observation changes were removed. No dial-concurrency, timeout,
request-limit, tracker-policy, or uTP changes are proposed here.

The patched downloads were much faster, but **the ratios are not causal speedup
estimates**: different seeds and conditions contributed to each run. The causal
case for the patch rests on the local reproduction and explicit live retirement
observations, not merely on faster runs.

## Safety, sources, and artifacts

The user explicitly authorized live tests of content linked from
<https://webtorrent.io/free-torrents>. The page was read as untrusted data. It lists
Big Buck Bunny, Cosmos Laundromat, Sintel, Tears of Steel, and The WIRED CD as
public-domain/Creative Commons testing content. Only Big Buck Bunny was used.

The page's actual link is
<https://webtorrent.io/torrents/big-buck-bunny.torrent>. The text extraction service
omitted hrefs, so the exact HTML was also fetched with
`curl --fail --location --max-time 30` and parsed for links. The fetched torrent
was byte-identical to the original, which was never modified:

- Original: `/home/user/Downloads/big-buck-bunny.torrent`.
- SHA-256: `13b4241c2fc4c2be3806287895566c0f596b9716643f2e679fd1482bcc7ed449`;
  checked again after all runs.
- Exact-info SHA-1: `dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c`.
- Three files, **276,445,467 bytes**, **1,055 pieces**, **262,144-byte piece length**.

**Artifact root:** `/home/user/Downloads/leech-perf.KvICZl`, a unique private
root. All live output, logs, and caches are beneath it; no existing output was
reused or resumed. Only one live transfer ran at a time. The HTTP reference ran
between torrent runs. No upload-capable client or public listener was used.
Initial free disk was 78 GiB; final free disk was 77 GiB; retained artifacts occupy
about 980 MiB. The destination is Btrfs on `/dev/sda2`, not the prior runs' tmpfs.

Each run directory contains `output/`, `cache/`, `command.txt`, `ownership.json`,
`runner.pid`, `leech.pid`, `runner.log`, `stdout.log`, `debug.log`, `process.jsonl`,
`result.json`, and `analysis.json`. Successful complete runs also have
`verification.json`; observed runs have `observed-stats.json`.

The artifact root retains the exact binaries, `runner.py`, `analyze.py`,
`observed_stats.py`, `verify.py`, source HTML/metainfo, temporary observation
patches (`observation.patch`, `observation-yield.patch`, `perf_probe.go`), the
original local reproduction, and validation logs. None is added to git.

### Process ownership and final state

`runner.py` owned and waited for each Leech child. It applied the total deadline
with SIGINT, allowed 45 seconds for cleanup, and could kill only its own child if
that grace period expired. No hard kill was needed. All runs also used a 120-second
verified-piece no-progress timeout. Wrapper polling adds up to about one second
to recorded wrapper elapsed time; CLI durations below are used for comparisons.

| Run | Runner / Leech PID | Start UTC | Total limit | Joined UTC | Exit |
| --- | --- | --- | ---: | --- | ---: |
| `baseline-1` | 109272 / 109274 | 11:33:28.908 | 600 s | 11:41:51.444 | 0 |
| `observed-2` | 111780 / 111782 | 11:42:30.822 | 180 s | 11:45:46.028 | 130 |
| `yield-3` | 113119 / 113121 | 11:46:56.910 | 480 s | 11:48:07.984 | 0 |
| `yield-4` | 114013 / 114015 | 11:48:51.587 | 240 s | 11:50:06.671 | 0 |

All eight PIDs were absent from `/proc` at the final check. Every cache workspace
was removed; only empty `cache/leech` parents remain. `observed-2` is deliberately
**incomplete**: its scheduled SIGINT initiated normal cleanup after 180 seconds.
Its partial output is retained, not reused. No unattended run remains.

## Build identity and exact settings

Go **1.27.1**, Linux/amd64, `GOAMD64=v1`, kernel
`6.12.107+deb13-amd64`, four available CPUs. Production builds used CGO=0.
`GOMAXPROCS`, `GOGC`, `GOMEMLIMIT`, `GODEBUG`, `GOFLAGS`, `GOWORK`, and
`GOTOOLCHAIN` overrides were unset. No runtime or network tuning.

Build commands ran in the exclusive worktree:

```sh
cd /home/user/Desktop/Leech-live-performance
CGO_ENABLED=0 go build -o /home/user/Downloads/leech-perf.KvICZl/leech-baseline ./cmd/leech
# After adding only the temporary observations:
CGO_ENABLED=0 go build -o /home/user/Downloads/leech-perf.KvICZl/leech-observed ./cmd/leech
# After additionally applying the yield change:
CGO_ENABLED=0 go build -o /home/user/Downloads/leech-perf.KvICZl/leech-observed-yield ./cmd/leech
# After removing observations, retaining only the proposed fix/regression:
make check
cp leech /home/user/Downloads/leech-perf.KvICZl/leech-yield
```

All four binaries embed revision `527958d` and `vcs.modified=true`: the baseline
had only the untracked report; the other builds had the explicitly described
changes. The clean yield source was subsequently committed as `5dceb5e`.

| Binary | SHA-256 |
| --- | --- |
| `leech-baseline` | `78d8b1b1ee83a76e77129dcf65ed6e513a9e4ac986ec00df05c688c23d1f8a79` |
| `leech-observed` | `9e7a5d60061c5a02d3b2b377b745b50fb6deb969f4c7c5f2cd799aa024761b55` |
| `leech-observed-yield` | `c8ddf0c3615abaaef46f9ea37991cdc146f651a2309d0d9639e99befb6fcfce6` |
| `leech-yield` | `a29dbdd35145fce79cc5da95059c166aff28ff8b7b97143e6f118d070448b179` |

All runs used the following command shape, expanded literally in their
`command.txt`. `R` is the artifact root, and `RUN`/`BIN` are paired below.

```sh
XDG_CACHE_HOME="$R/$RUN/cache" "$R/$BIN" \
  --loglevel debug --timeout 120s --output "$R/$RUN/output" \
  /home/user/Downloads/big-buck-bunny.torrent
```

| RUN | BIN | Wrapper total argument |
| --- | --- | ---: |
| `baseline-1` | `leech-baseline` | 600 |
| `observed-2` | `leech-observed` | 180 |
| `yield-3` | `leech-observed-yield` | 480 |
| `yield-4` | `leech-yield` | 240 |

Owner invocation: `nohup python3 -u "$R/runner.py" "$R/$RUN" "$R/$BIN" TOTAL`,
stdin `/dev/null`, stdout/stderr redirected to `$R/$RUN/runner.log`. Default bulk
scheduling, full selection, default trackers, impaired baseline uTP, no resume.
The observations added fixed numeric per-peer counters and cumulative wall times
to the existing one-second diagnostic tick—not a production metrics interface.

## Measurements

### End-to-end results

Rates are **received payload**, including late/duplicate bytes, not useful-output
rate. Complete outputs independently passed every SHA-1 piece hash from the
original metainfo using `verify.py`, with piece-at-a-time buffering.

| Run | Verified bytes | Transfer / session seconds | Payload bytes | Average B/s | Useful connections |
| --- | ---: | ---: | ---: | ---: | ---: |
| Prior diagnostic run, read-only | 276,445,467 | 355.858 / 370.864 | 290,388,251 | 816,022 | 19 |
| `baseline-1` | 276,445,467 | 487.294 / 502.366 | 288,127,259 | 591,280 | 24 |
| `observed-2`, interrupted | 113,508,352 | 180.183 / 195.193 | 122,175,488 | 678,063 | 19 |
| `yield-3`, observed fix | 276,445,467 | 56.033 / 71.039 | 282,392,859 | 5,039,784 | 11 |
| `yield-4`, clean fix | 276,445,467 | 59.254 / 74.261 | 276,609,307 | 4,668,168 | 2 |

The final ~15 seconds of each session were bounded final tracker work, not payload
download. The known stale final progress snapshot was not interpreted as ongoing
transfer. Each complete run completed all 1,055 pieces once; no hash mismatch,
staging/finalization failure, or diagnostic drops appeared. Log sizes were
639,323 / 306,429 / 525,809 / 202,248 bytes for runs 1–4.

The prior first successful run at `/tmp/leech-live.vcGERk` was reported as ~493.8 s
with an old 12.9 MB log; it was not rerun or modified. Prior diagnostic evidence
at `/tmp/leech-live.hKOrZe` was analyzed read-only. The new unchanged baseline's
487 s versus that prior run's 356 s already demonstrates substantial variability.

### Discovery and admission

Times below are relative to transfer-phase entry, not first tracker request.

| Run | First TCP handshake | First accepted block | First verified piece | Races / TCP wins |
| --- | ---: | ---: | ---: | ---: |
| `baseline-1` | 3.240 s | 3.798 s | 4.467 s | 310 / 29 |
| `observed-2` | 1.116 s | 1.775 s | 2.640 s | 121 / 22 |
| `yield-3` | 1.225 s | 1.764 s | 2.453 s | 39 / 12 |
| `yield-4` | 13.057 s | 13.677 s | 14.401 s | 35 / 2 |

Every run had **zero uTP handshakes**. This is not new uTP interoperability
validation; another agent owns the known sequence defect. Canceled uTP losers
are not evidence of unsupported uTP.

Source: `Transfer.Run` starts one `acquireLoop` (`transfer.go:346–354`);
that loop calls `AcquirePeer` serially. `run.go:927–966` waits synchronously for
`manager.DialWithResult` before selecting another endpoint. The manager's 32-race
bound is only a ceiling. Pairing explicit candidate-start/race-outcome records
confirmed **maximum one selected, unsettled endpoint** in every new run and the
prior diagnostic run. Rounded race durations alone can falsely suggest tiny
sub-millisecond overlaps, so they were not used to infer concurrency.

The race deadline is **2 s** (`metadata.go:43`); TCP starts after a **100 ms** uTP
head start (`peer/dial.go:42`). In baseline run 1, unsuccessful/canceled races
occupied ~466.9 s of that serial path; successful races occupied ~19.7 s.
Existing peers download concurrently, so **466.9 s is not additive download
latency**. It measures how continuously admission was occupied by unusable
candidates. TCP failures included 180 dial timeouts, 49 refusals, 28 handshake
EOF/truncations, 11 network-unreachable results, 10 handshake resets, and only
2 handshake timeouts. Do not attribute these all to the uTP sequence bug.

No concurrency tuning was attempted. Parallelizing the current callback directly
would violate its single-owner candidate cursor/admission assumptions. Any later
change needs bounded dispatch, endpoint deduplication, backoff/identity admission,
and cancellation/join tests—not just more `acquireLoop` goroutines.

### Productive-peer retention: the confirmed bottleneck

Baseline `transfer.go:494–501` closed a peer if its queue remained nonempty after
16 drained events. The queue holds four events plus one backpressured reader
item (`peer/worker.go`). Those are memory/work bounds, not abuse evidence.

In `observed-2`, **17 backlog retirements** discarded useful, unchoked connections:
median connection age **2.835 s**. Every such retirement followed accepted data
within 0–2 ms. Most had 111 outstanding requests, zero rejects, and zero timeouts.
These connections had supplied **119,586,816 accepted bytes** in total. Examples:

- 54,820,864 bytes in 6.406 s of connection lifetime, then locally disconnected;
- 27,459,584 bytes in 10.197 s, then locally disconnected;
- 14,417,920 bytes over 81.039 s, then locally disconnected.

Two other retirements were ordinary ~60-second inactivity rotations: one choked
Allowed Fast contributor, and one choked peer that delivered nothing after its
128 requests expired. These were not attributed to the backlog defect.

The local regression creates 32 outstanding requests, supplies independent valid
Piece frames through `net.Pipe`, and uses `synctest.Wait` barriers to expose a
legal continuously replenished queue. Baseline: **disconnect after 16/32 accepted
blocks**. Proposed fix: first pass handles exactly 16, second pass finishes and
verifies exact output. It is a deterministic scheduling test, not a link-speed
simulation. The live observation supplies the missing evidence that this path
occurs with real peers.

Run 3 retained its main contributors: two supplied ~107 MB and ~98 MB. Run 4
completed with only two useful TCP connections, despite unchanged serial dialing
and broken uTP. This demonstrates that those other limitations did not prevent
much faster downloads in these samples; it does not establish a universal rate.

### Pipelines, staging, timeouts, and duplicate work

- Local per-peer cap: 128 × 16 KiB = **2 MiB**; global cap: 4,096 requests.
  `reqq=0` was never observed in the temporary samples.
- In observed baseline run 2, **212/214 eligible unchoked sample rows had a full
  pipeline**. These are application assignments, not a TCP-flight measurement.
  Maximum sampled global outstanding requests: **384**. The ordinary request
  window was not generally starved while these peers survived. Choked
  states accounted for 191/405 peer-sample rows; these are observations, not
  independent peers or exact time-weighted population estimates.
- Staging reached **64 pieces / 16 MiB** in both observed runs. The 512 MiB byte
  bound was nowhere near binding for this torrent. The unchanged baseline spent
  ~417.5 s at 64 staged pieces; run 3 spent ~51.3 s there. A full stage table does
  not mean existing pieces cannot receive blocks. Runs 1, 3, and 4 reclaimed no
  stages; the interrupted observed baseline reclaimed **17** under staging
  pressure. The logs do not quantify how many accepted bytes those stages held.
- Run 3's maximum sampled outstanding count was **1,408**, including endgame
  duplicates, below 4,096. Many "staged piece limit" diagnostics remained after
  the fix. Stage-count pressure may limit utilization with many fast peers and
  small pieces, but these observations do not justify raising supported bounds.
- Requests expire after **30 s**; ordinary inactivity rotates peers after **60 s**.
  Observed baseline run 2 had **456 request expirations**, no rejects, and
  **1,245,184 late bytes**. It accepted 120,930,304 bytes, of which 113,508,352
  had verified by interruption. The difference includes unfinished staging and
  any accepted bytes discarded by reclamation; it cannot all be classified as
  duplicate-download overhead. Shutdown also discarded unfinished staging.
- Complete-run payload excess was **4.23%** in baseline run 1, **2.15%** in run 3,
  and **0.059%** in run 4. Run 3 had **zero request expirations**, 118 rejects,
  and 5,947,392 late bytes, exactly accounting for its excess. Endgame began
  ~2.2 s before completion. No evidence of repeated hash-failure downloads.

### Local costs and serialized work

| Run | Child CPU seconds, user + system | Peak RSS KiB | Major faults |
| --- | ---: | ---: | ---: |
| `baseline-1` | 45.217 | 22,436 | 0 |
| `observed-2` | 13.214 | 23,316 | 0 |
| `yield-3` | 24.597 | 25,452 | 0 |
| `yield-4` | 27.005 | 21,220 | 0 |

These are joined-child resource usage, supplemented by one-second `/proc`
samples. Baseline CPU was ~9% of one CPU over its transfer. Even the fast runs
used under half of one CPU on average; averages do not exclude short bursts.

The coordinator synchronously writes accepted blocks and calls the serialized
finalizer (`transfer.go` → `Stager.Finalize`). Temporary monotonic wall timers:

| Observed run | Accepted-block writes | Finalization | Scheduling/drive |
| --- | ---: | ---: | ---: |
| Baseline behavior, 180 s | 0.423 s | 1.058 s | 2.941 s |
| Yield behavior, 56 s | 0.848 s | 2.031 s | 12.554 s |

Drive includes stage admission and request scheduling. Finalization includes
hashing, selected output writes/closes, and stage removal. These timers do not
cover all coordinator work or physical persistence; Leech intentionally does not
fsync. Warm-cache independent whole-output verification took 0.48–0.90 s.
There is no basis here for storage redesign, asynchronous finalization, larger
buffers, GC tuning, or a broad profiling/metrics system. At higher retained-peer
rates, scheduling deserves measurement before any further optimization.

### HTTP reference: not an apples-to-apples comparison

The torrent's `url-list` names `https://webtorrent.io/torrents/`. Between runs 1
and 2, one bounded range of the corresponding MP4 was fetched:

```sh
curl --fail --location --max-time 45 --speed-time 15 --speed-limit 1024 \
  --range 0-33554431 --max-filesize 33554432 \
  --dump-header "$R/http-reference/headers.txt" \
  --output "$R/http-reference/prefix.mp4" \
  --write-out 'http=%{http_code} bytes=%{size_download} total=%{time_total} start=%{time_starttransfer} speed=%{speed_download}\n' \
  'https://webtorrent.io/torrents/Big%20Buck%20Bunny/Big%20Buck%20Bunny.mp4'
```

HTTP **206**, **33,554,432 bytes**, **9.690 s** total, **0.635 s** to first byte,
**3,462,710 B/s**. The range matched the independently verified MP4 prefix.
It demonstrates throughput above the slow baseline average, not an internet-link
ceiling or an achievable swarm rate. Different servers, paths, and protocols
make a direct performance ratio invalid; the patched swarm runs were faster.

## Validation, limits, and next steps

Validation logs are in the artifact root:

- Original reproduction failed as expected: `backlog-baseline-test.txt`.
  An initial shell status print accidentally followed a successful `cd`; the Go
  log explicitly records failure. No claimed test pass depends on that print.
- Initial experimental fix: 20 regression runs, 10 race runs, then `go test ./...`.
- Clean proposed patch: `go test ./internal/session -run
  '^TestTransferValidPieceBacklogYieldsWithoutDisconnect$' -count=100`, and
  `go test -race ./internal/session -run
  '^TestTransferValidPieceBacklogYieldsWithoutDisconnect$' -count=20`, all passed.
- `make check` passed: all tests, all race tests, vet, and CGO=0 build.
  Logs: `proposed-patch-regression.txt`, `proposed-patch-check.txt`.
- Three complete live outputs independently verified; cache cleanup and joined
  process state checked. The interrupted run's full output is not claimed valid.

Limits: one torrent, one host, a short time window, changing tracker samples,
random peer identity/piece ties, no fixed-seed or reciprocal-client comparison,
and known impaired uTP. Observation timing can change queue scheduling.
Baseline run 1 overlapped some local compilation/tests; run 3/4 did not. The host
was shared with other agents, so system-wide load was not controlled. No peer's
upload policy or remote bandwidth was measured. No statistical speedup estimate
is warranted from these four sequential runs.

## Parent integration and live verification

The parent integrated the productive-peer fix as `d8276d7`, the logging fixes as
`e0cd004`, and the uTP audit fixes as `b186d86`. `make check` passed on their
combined tree: tests, the full race suite, vet, and a pure-Go build. No temporary
observation code was included. Validation log: `/tmp/leech-combined-check.log`.

A fresh, non-resume Big Buck Bunny download used the clean binary at `86f44e8`,
with the same bounded runner and a 600-second total limit. Artifacts are in
`/home/user/Downloads/leech-integrated.at5rlY`, including command, revision,
binary hash, process samples, log, result, analysis, and independent verification.

| Combined run | Result |
| --- | ---: |
| Transfer / session seconds | 194.544 / 209.550 |
| Independently verified output | 276,445,467 bytes; all 1,055 piece hashes |
| Received payload / average rate | 282,933,531 bytes / 1,454,338 B/s |
| Handshake winners | 19 uTP; 9 TCP |
| Useful connections, correlated from log events | 10 uTP; 4 TCP |
| Exit / timeout | 0 / no interruption |

The run observed useful live uTP data for the first time. Its timing was between
the unchanged baseline and the two TCP-only patch runs; it does **not** establish
that enabling uTP sped up or slowed down the transfer. Peer samples, transports,
and swarm conditions changed. The separate regressions establish the fixed
correctness defects; these live runs establish successful real-world operation,
not a universal speedup.

The log reported specific invalid-compact-endpoint and deadline-exceeded causes.
Transfer reporting switched to `shutdown` before final announces, with no stale
progress afterward. Three final announcements failed nonfatally; final tracker
work still took about 15 seconds. No hash mismatch, staging failure, or diagnostic
drop was observed. All output hashes passed, the cache workspace was removed,
and both owned processes exited. Original downloads were untouched.

Remaining performance work, if needed:

1. Investigate a bounded asynchronous dial dispatcher with deterministic
   ownership/cancellation tests if startup or replacement remains slow. Do not
   merely multiply callbacks or extend deadlines without evidence.
2. Profile scheduling and stage-count pressure if throughput still falls short
   with retained useful peers. Do not tune supported limits from blocker-message
   counts alone or relax compact-peer validation for benchmarks.
