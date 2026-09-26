# Leech: Download-Only BitTorrent Client Design

- **Status:** Approved
- **Design date:** 2026-09-19
- **Implementation status:** Unresolved work is tracked in [TODO.md](TODO.md).
- **Primary specifications:** [`beps/`](beps/), especially BEP 3

This document defines Leech's required behavior. Present-tense descriptions are
contracts, not claims that every requirement is implemented. [TODO.md](TODO.md)
records known gaps and their acceptance checks; task sequencing does not relax
these contracts. [README.md](README.md) provides build and usage instructions.

| Topic | Sections |
| --- | --- |
| Product scope and protocol choices | [Summary](#1-decision-summary), [goals](#2-goals), [non-goals](#3-non-goals), [protocol profile](#5-protocol-profile) |
| User interface | [CLI](#4-command-line-interface) |
| Session and files | [Phases](#6-session-phases-and-ownership), [metadata](#7-input-and-metadata), [selection and storage](#8-selection-and-storage-mapping), [resume](#9-resume-behavior) |
| Networking | [Trackers](#10-tracker-subsystem), [candidates and dialing](#11-candidate-peers-and-dialing), [peer wire](#12-peer-wire-behavior), [uTP](#14-utp) |
| Transfer and shutdown | [Scheduling and verification](#13-piece-scheduling-and-verification), [lifecycle](#15-concurrency-and-lifecycle) |
| Limits and acceptance | [Bounds](#16-supported-bounds), [trust boundaries](#17-security-and-trust-boundaries), [failures](#18-failure-semantics), [validation](#19-validation), [tradeoffs](#20-key-tradeoffs) |

## 1. Decision summary

Leech is a stateless, single-torrent, download-only BitTorrent v1 CLI written in Go. It obtains peers only from trackers and magnet-embedded endpoints, connects only outbound over uTP or TCP, downloads through the peer protocol, and never uploads file payload or torrent metadata.

The design favors a small, explicit state machine over broad protocol coverage:

- one active torrent per process;
- BitTorrent v1 only;
- `.torrent`, magnet URI, and bare info-hash inputs;
- HTTP(S) and UDP trackers, plus magnet `x.pe` peers;
- outbound IPv4 and IPv6 connections;
- uTP preferred, with TCP fallback;
- selective files, sequential-priority streaming, and stateless resume;
- complete pieces staged on disk and SHA-1 verified before output writes;
- no third-party Go packages.

The cost is reduced reachability and download performance. Leech does not listen, participate in decentralized discovery, advertise acquired pieces, or reciprocate. It depends on seeds, optimistic unchokes, and BEP 6 Allowed Fast behavior.

Fast is the principal recovery path for a client that advertises no availability, not a compatibility guarantee. A peer that lacks BEP 6, rejects `Have None`, requires peer-wire encryption or a nonempty bitfield, or refuses nonreciprocating peers may be unusable. Leech treats that as a peer-local compatibility failure: it closes or rotates the peer without a corruption strike and never weakens the no-upload boundary to retain it.

## 2. Goals

1. Download selected content correctly from BitTorrent v1 swarms.
2. Preserve a hard no-upload invariant for file payload and metadata.
3. Reject malformed or unsupported metadata before output or cache writes.
4. Bound memory, disk staging, network concurrency, and parser work.
5. Recover from interruption by validating output files rather than loading application state.
6. Keep all concurrent work owned, cancellable, and joined.
7. Remain portable Go, with Linux as the first supported environment. Target only
   the current stable Go release; backward toolchain compatibility is not a goal.

## 3. Non-goals

Leech does not support:

- BitTorrent v2 or hybrid torrents;
- BEP 27 private-torrent isolation or access-control semantics;
- seeding or payload upload while downloading;
- inbound peer connections;
- DHT, peer exchange, local peer discovery, or tracker exchange;
- NAT traversal, port mapping, or hole punching;
- web seeds;
- proxies, anonymity mode, or peer-wire encryption;
- bandwidth limiting;
- persistent configuration, session state, resume databases, cache indexes, or reusable cached data;
- tracker scrape, torrent creation, feeds, signing, or mutable torrents;
- multiple torrents in one process;
- automated or manual interoperability tests against existing clients or live trackers.

## 4. Command-line interface

Leech exposes one command with download and file-listing modes:

```text
leech [options] SOURCE
```

One invocation handles one torrent. Download is the default; `--list-files` stops
after reading and validating metadata. There are no subcommands, configuration
files, environment-variable settings, prompts, or stdin input. Options precede
`SOURCE`. This keeps the command aligned with the single-session design and makes
every run self-contained.

### 4.1 Source and argument parsing

`SOURCE` is exactly one of:

- a path to a v1 metainfo file, conventionally ending in `.torrent`;
- a magnet URI; or
- a 40-character hexadecimal or 32-character Base32 v1 info hash.

The parser recognizes a case-insensitive `magnet:` scheme first, an exact-length
info hash second, and otherwise treats the value as a file path. Prefix a
hash-shaped filename with `./` or another directory component to force path
interpretation. The literal `-` is unsupported. `--` ends option parsing, which
permits a path beginning with `-`.

Long options accept either `--name value` or `--name=value`. Short options are not
combined. Unknown options, missing values, and more or fewer than one source are
usage errors.

```text
Usage:
  leech [options] SOURCE

Options:
  -o, --output DIR
        Use DIR as the destination directory (default: current directory).
  -f, --file PATTERN
        Download matching content. May be repeated.
  -l, --list-files
        List selectable files without downloading them.
  -L, --loglevel LEVEL
        Set logging to debug, info, warning, or error (default: warning).
  -r, --resume
        Verify and reuse existing selected output (default: overwrite).
  -s, --stream
        Prioritize earlier pieces for playback.
  -t, --timeout DURATION
        Fail after DURATION with no newly verified file piece; not a total timeout.
  -h, --help
        Show help and exit.
```

### 4.2 Destination

`--output` names a directory, never an output filename. A relative directory is
interpreted from the process's initial working directory. The directory must
already exist. Leech resolves it once if it is a symlink and applies the storage
rules in §8 below that resolved root.

Leech retains the torrent's name:

- a single-file torrent writes `<output>/<torrent name>`;
- a multi-file torrent writes `<output>/<torrent name>/<file path>`.

It does not offer output renaming.

### 4.3 File selection

Each `--file` adds a pattern to one union. Matching is case-sensitive and uses
`/` regardless of the host operating system. `*`, `?`, and `[]` are supported;
matching follows Go's `path.Match`: `*` and `?` do not cross `/`, but a bracket
class can consume `/`. `**` is rejected. A directory match includes all of its
descendants. Shell metacharacters should be quoted.

For a multi-file torrent, patterns are relative to the torrent root and omit the
root name. For a single-file torrent, the selectable path is its torrent name.
The combined selection must match at least one regular file. A selection that
resolves only to padding, or any selection of a symlink entry, is an error.
Repeated or overlapping matches do not duplicate work.

If no `--file` is present, Leech uses a magnet's BEP 53 `so` selection when one
exists; otherwise, it selects all regular files. One or more explicit selectors
replace, rather than extend, magnet `so`.

### 4.4 File listing

`--list-files` validates metadata and writes every selectable regular-file path
to standard output, one JSON-quoted path per line in torrent order, with no
header. Each string contains the path accepted by `--file`: a multi-file path is
relative to the torrent root, while a single-file path is the torrent name.
Padding and symlink entries are omitted because Leech cannot select them.

For a local metainfo file, listing performs no network activity. For a magnet or
bare hash, Leech runs the metadata-only discovery phase, stops and joins its
workers, sends applicable `stopped` announces, prints the validated file list,
and exits. Listing never validates, creates, truncates, or writes output or piece
cache paths.

`--list-files` may be combined only with `--loglevel`. Explicit `--output`,
`--file`, `--resume`, `--stream`, or `--timeout` options in listing mode are usage
errors.

### 4.5 Logging

`--loglevel` accepts exactly `debug`, `info`, `warning`, or `error`, ordered from
most to least detail. The default is `warning`. The selected level includes
messages at that level and every less detailed level:

- `debug` adds bounded tracker, peer, scheduling, and lifecycle diagnostics;
- `info` adds phase changes, transfer progress, and successful completion;
- `warning` reports recoverable or important behavior, including ignored
  `private=1`;
- `error` reports only the primary failure and secondary shutdown failures.

Help, usage errors, and `--list-files` output are not filtered by the log level.
All log messages follow the redaction and escaping rules in §4.9.

### 4.6 Transfer order

`--stream` selects the sequential-priority scheduler described in §13. Leech
still writes verified ranges to their final files and may request later available
pieces to keep connections productive. The option does not expose a byte stream,
choose a media file, or promise uninterrupted playback.

Without `--stream`, Leech uses bulk rarest-first scheduling.

### 4.7 Existing output

Overwrite is the default. After metadata and selection validation, Leech
truncates existing selected regular files and downloads their wanted pieces from
scratch. It never truncates unselected or unrelated files, and it cannot bypass
path, collision, or symlink validation.

`--resume` verifies and reuses existing selected output before transfer discovery,
as defined in §9. Missing, short, or mismatching data remains eligible for
download.

### 4.8 No-progress timeout

`--timeout` is only a file-transfer no-progress timeout. It accepts a positive Go
duration such as `30m`, `2h`, or `1h30m`. A missing option means no timeout; zero
and negative durations are usage errors.

The timer begins only when file transfer begins and resets only when a new file
piece passes hash verification. It does not limit metadata discovery, selection,
or resume hashing. Expiry initiates the ordinary graceful failure path, including
best-effort `stopped` announces and cache cleanup.

### 4.9 Output and terminal behavior

Standard output contains only `--list-files` results. Logs and interactive status
go to standard error.

At `info` or `debug` level on an interactive terminal, Leech shows one replaceable
status line for metadata discovery, optional resume checking, and transfer
progress. It refreshes no more than once per second. Transfer status includes
verified selected bytes, selected bytes, active peers, and recent payload rate.
Phase changes, warnings, and the final result receive permanent lines when their
levels are enabled.

When standard error is not an interactive terminal, Leech emits enabled log lines
but no periodic progress. It never uses color or requires terminal capabilities.

Names and other untrusted text are quoted and escaped before display. Tracker
diagnostics identify a tracker by scheme and host only; they do not print URL
userinfo, path, or query data that may contain credentials. Leech never echoes a
complete magnet URI.

Notable results are explicit:

- resumed output that is already valid reports that no transfer was needed;
- a partial selection reports completion of the selection, not the torrent;
- failure reports that verified partial output remains resumable when applicable;
- `private=1` produces a warning that Leech treats the torrent as public.

### 4.10 Exit status and signals

| Status | Meaning |
| ---: | --- |
| 0 | The selected output is complete and verified, or the validated file list was written. |
| 1 | Source, metadata, selection, network, verification, storage, timeout, or cleanup failure. |
| 2 | Invalid command-line usage. |
| 130 | Graceful exit after `SIGINT`, where supported. |
| 143 | Graceful exit after `SIGTERM`, where supported. |

`--help` writes help to standard output and exits 0. Usage errors write a concise
error and usage synopsis to standard error. Runtime errors write one primary
error; secondary shutdown failures are diagnostics and do not replace it.

The first termination signal starts the graceful shutdown defined in §15. A
second may terminate immediately, so cleanup is no longer guaranteed.

### 4.11 Examples

```sh
leech release.torrent

leech --output /srv/media 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567'

leech -l series.torrent

leech -L info -l 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567'

leech --file 'Season 1' --file 'extras/*.srt' series.torrent

leech -r -s -f 'movie.mkv' -t 20m \
  0123456789abcdef0123456789abcdef01234567

leech -- -release.torrent
```

The CLI deliberately uses one action flag instead of subcommands because listing
and downloading share source parsing and metadata acquisition. A destination is
an option rather than a second positional value. Repeated file options preserve
commas in names, and one log-level option replaces overlapping verbose and quiet
switches. JSON status and user-tunable protocol settings remain out of scope.

## 5. Protocol profile

Leech implements only the required parts of the local specifications:

| BEP | Use |
| --- | --- |
| [3](beps/bep_0003.rst) | Bencoding, v1 metainfo, trackers, and peer wire protocol |
| [4](beps/bep_0004.rst) | Reserved bits and message IDs |
| [6](beps/bep_0006.rst) | Fast Extension |
| [7](beps/bep_0007.rst) | IPv6 tracker peers |
| [9](beps/bep_0009.rst) | Magnet URIs and peer metadata transfer |
| [10](beps/bep_0010.rst) | Extension transport for metadata transfer |
| [12](beps/bep_0012.rst) | Tracker-list syntax |
| [15](beps/bep_0015.rst) | UDP tracker protocol |
| [23](beps/bep_0023.rst) | Compact tracker peer lists |
| [27](beps/bep_0027.rst) | Recognition only; semantics intentionally overridden |
| [29](beps/bep_0029.rst) | uTP v1 |
| [31](beps/bep_0031.rst) | Tracker retry hints |
| [41](beps/bep_0041.rst) | UDP tracker URL data |
| [47](beps/bep_0047.rst) | Receive/storage-side padding and file attributes |
| [52](beps/bep_0052.rst) | Detection and rejection of v2/hybrid metadata |
| [53](beps/bep_0053.rst) | Magnet file selection |

### 5.1 Explicit profile overrides

These are product decisions, not claims of full BEP compliance:

- **Premetadata announces:** Before magnet or bare-hash metadata reveals the true size, `left=1` is used as a nonstandard “unknown but incomplete” sentinel. Exact whole-torrent accounting begins after metadata and resume validation.
- **Private marker:** `private=1` is ignored. The torrent is treated as public, the mandatory default tracker is added, and magnet-embedded peers remain usable. This intentionally violates BEP 27.
- **Tracker tiers:** BEP 12 tiers are flattened; every unique tracker runs independently.
- **Announced port:** Leech announces an unbound random nonzero port despite BEP 3 and BEP 7 describing a listening endpoint.
- **Availability:** Leech advertises no pieces and never sends `Have`, despite possessing verified output.
- **Padding upload:** Leech understands BEP 47 padding for downloading and verification but never services padding or other payload requests.

Unknown metainfo keys remain part of the exact info-hash bytes but are otherwise ignored. Out-of-scope protocols are not represented as dormant extension points.

## 6. Session phases and ownership

One process owns one `Session`. It is the lifetime boundary for metadata, trackers, peer candidates, connections, scheduling, cache staging, output, counters, strikes, and shutdown.

A session moves through explicit phases:

1. **Source parsing:** Parse the `.torrent`, magnet, or bare hash without filesystem mutation.
2. **Metadata-only discovery, if needed:** Announce to trackers and contact embedded/tracker peers only to obtain BEP 9 metadata.
3. **Metadata validation and normalization:** Validate v1 metadata, normalize files, and reject v2/hybrid data.
4. **Selection and resume:** Resolve selected files and verify existing output. No file-payload requests occur.
5. **Transfer, if needed:** Start normal tracker loops, peer dialing, piece scheduling, cache staging, and output commits.
6. **Shutdown:** Quiesce workers, send final tracker events, clean the current workspace, and return.

At the end of metadata-only discovery, Leech cancels and joins regular tracker loops, dials, and metadata peer connections, then sends bounded `stopped` announcements. Resume scanning therefore runs without concurrent tracker or peer activity. A later transfer phase starts fresh `started` announces with the correct `left`. Bounded endpoint values may remain in memory, but no network worker crosses the phase boundary.

CLI file-listing mode exits after metadata validation and normalization. A local
`.torrent` listing performs no network activity. A magnet or bare-hash listing
uses the ordinary metadata-only phase and its `stopped` sequence, then exits
without selection, resume, output access, or piece-cache creation.

```text
 source
   |
   +--> known metadata ----------------------+
   |                                         |
   +--> metadata-only discovery --> metadata |
                                             v
                                  validate + normalize
                                             |
                                  select + resume scan
                                             |
                                  missing selected data?
                                      |             |
                                     no            yes
                                      |             v
                                      |      transfer discovery
                                      |             |
                                      |       peer block requests
                                      |             v
                                      |       per-piece cache
                                      |             v
                                      |      verify + commit
                                      +-----------> shutdown
```

The session coordinator solely owns mutable torrent state. Tracker workers, dial attempts, peer workers, and the piece finalizer exchange bounded events with it; they do not mutate rarity, request, strike, or completion state directly.

A root context requests cancellation. Every goroutine has an owner, bounded work, an unblock mechanism, and a completion path joined before ordinary exit.

## 7. Input and metadata

### 7.1 Accepted sources

- A `.torrent` supplies the encoded info dictionary and tracker metadata.
- A magnet must contain exactly one effective v1 `btih` topic. Hexadecimal and Base32 forms are accepted. `tr`, `x.pe`, `dn`, and `so` are honored.
- A bare info hash accepts the same hexadecimal and Base32 forms.

Any magnet containing `btmh` is rejected, even if it also contains `btih`, because such links identify v2 or hybrid content. Conflicting `btih` values, malformed endpoints or selections, and unsupported schemes are errors. Magnet `dn` is display-only and never defines an output path.

The default tracker `http://tracker.opentrackr.org:1337/announce` is always added. Duplicate URLs are removed. For `.torrent` input, `announce-list` URLs are used when present; otherwise `announce` is used. Tier grouping is discarded.

### 7.2 Strict bencoding and info hashes

The bounded decoder rejects:

- unsorted or duplicate dictionary keys;
- malformed lengths or integers;
- negative zero and leading-zero integers;
- integers outside signed 64-bit range;
- truncated values or unexpected trailing bytes;
- excessive nesting, values, or container entries.

For `.torrent` input, Leech records and hashes the exact encoded byte span of `info`; it never computes an info hash by re-encoding a decoded object. BEP 9 metadata is the exact encoded info dictionary and must hash to the requested SHA-1.

### 7.3 v1 validation and normalization

Leech validates:

- the `info` dictionary contains exactly one of single-file `length` and multi-file `files`;
- positive bounded piece length;
- nonnegative lengths and an overflow-safe total;
- a `pieces` byte string whose length is exactly 20 times the logical piece count;
- valid UTF-8 for BEP-defined human-readable strings;
- no v2 `meta version`, `file tree`, or other recognized hybrid structure;
- bounded file count, path depth, path bytes, and decoded structure.

The `private` key is parsed but ignored.

File entries normalize as follows:

- A regular multi-file entry requires nonnegative `length` and a nonempty relative `path`.
- A padding entry requires its length but may omit `path`; it receives an internal identity and no output path.
- A symlink entry may omit `length`, which normalizes to zero. Its own path is validated, but Leech never creates it.
- Unknown attribute characters are ignored. Executable, hidden, and per-file SHA-1 hints do not affect output.
- `.` and `..`, absolute paths, duplicate paths, separators inside a component, target-filesystem collisions, and unrepresentable names are rejected.

After validation, `info.name` is one safe path component used as the conventional output name. A single-file torrent maps to `<destination>/<info.name>`. A multi-file torrent maps to `<destination>/<info.name>/<file path>`. Magnet `dn` never overrides `info.name`.

### 7.4 Metadata acquisition

A magnet or bare-hash session uses this metadata-only profile:

1. Start all configured trackers with `event=started`, `uploaded=0`, and the explicitly nonstandard `left=1` sentinel.
2. Dial embedded and tracker-returned endpoints.
3. Keep only peers that negotiate BEP 10 and advertise `ut_metadata`.
4. Accept the first advertised metadata size within bounds without waiting for consensus.
5. Have one peer supply the complete candidate.
6. Validate message fields, block sizes, repeated `total_size`, canonical bencoding, and final SHA-1.
7. Give the sole endpoint one corruption strike after a complete invalid candidate, then try another peer or advertised size.
8. After candidate hash and bencoding validation, quiesce and join metadata-discovery workers and send bounded `stopped` announces before full v1 normalization, selection, and resume.

Leech advertises its own local `ut_metadata` ID so the peer can send extension messages to it. Incoming extension dispatch uses Leech's advertised ID; outbound requests and rejects use the remote peer's advertised ID. IDs and enable/disable state are per connection. Repeated BEP 10 handshakes update that connection's mapping according to BEP 10.

Leech never sends metadata data blocks. An incoming metadata request receives a reject if the peer still advertises a usable remote `ut_metadata` ID; otherwise it is ignored.

Fetched metadata remains in bounded memory for the run and is discarded at exit. Parsed file and piece tables are active in-memory session state, not persistent state.

Treating `private=1` as public is deliberate. A private torrent may be disclosed to the default tracker and embedded peers and may fail because its tracker expects authentication. Leech does not attempt to preserve BEP 27 isolation.

## 8. Selection and storage mapping

The normalized metadata becomes an immutable file table. Every original file-list position is retained for BEP 53 indexing, while each entry separately records whether it has an output path. Entries also carry length, attributes, and their half-open range in the v1 concatenated byte space.

Selections follow the exact-path and glob rules in [§4.3](#43-file-selection).
No match is an error. Explicit user selection replaces magnet `so`.

When `so` applies, indices refer to original file-list positions before padding or symlink filtering. Padding indices contribute no output selection. Selecting a symlink is an error. A selection containing only padding or otherwise producing no output files is an error. A single-file torrent has index zero.

A piece is wanted if it intersects a selected non-padding regular file. Leech downloads the complete piece required by its SHA-1, including skipped-file ranges. Padding ranges are synthesized as zeros and are not requested. Only intersections with selected regular files are committed.

The destination directory is resolved once if it is a symlink. No descendant traversed or created by Leech may be a symlink. Unsafe or colliding paths are rejected rather than renamed. Races from a hostile concurrent local process are outside scope.

Selected zero-length files are created. Other selected files grow as verified
ranges arrive and may be sparse while incomplete. No output file is created for
unselected content. Final paths remain partial until their selected content has
passed piece verification and the download completes.

## 9. Resume behavior

Leech persists no resume state. After metadata and selection, resume mode scans output before transfer discovery:

1. Reconstruct every complete piece available solely from selected regular files and synthetic padding.
2. Hash it and mark matching selected ranges valid.
3. Treat missing, short, or mismatching data as absent.
4. Redownload a whole piece if skipped non-padding ranges prevent reconstruction.
5. For an overlong selected file, hash only its declared prefix and truncate excess after that selected file validates successfully.

With `.torrent` input, the scan occurs before any network activity. Magnet and bare-hash inputs first complete and stop their metadata-only discovery phase; no network worker remains active during scanning.

If selected output is already complete, Leech exits without starting transfer discovery. Without resume, it truncates existing selected files and treats every wanted piece as missing. Unselected and unrelated files remain untouched.

If resume finds missing data, transfer preparation stays in resume mode and
preserves verified output. It must not switch to overwrite mode after the scan.

The optional no-progress timeout starts only when transfer begins. It resets only after a newly completed file piece verifies.

## 10. Tracker subsystem

Each unique tracker owns an independent state machine. Source tiers do not suppress one another, and the default tracker always participates.

Every run generates one cryptographically random:

- unbranded 20-byte peer ID;
- 32-bit tracker key;
- announced port in `49152–65535`.

The same values are used across trackers, phases, and address families. The
announced port is not probed, bound, or reserved. `uploaded` is always zero.

### 10.1 Accounting

After metadata is known, `left` is the number of real torrent bytes not retained, not merely the selected amount. Skipped non-padding bytes remain left; padding is locally available as synthetic zeros. A partial selection never sends `completed`. Leech sends `completed` only when all regular-file bytes are retained and verified, then sends `stopped` because it exits instead of seeding.

During metadata-only discovery, approved profile exception `left=1` replaces exact accounting. This value has no BEP-defined sentinel meaning and can be false for an empty torrent.

`downloaded` counts received file-payload bytes, including data later discarded or redownloaded. Metadata and transport overhead are excluded.

### 10.2 HTTP(S)

Leech owns the authoritative announce parameters: `info_hash`, `peer_id`, `port`, `uploaded`, `downloaded`, `left`, `event`, `compact`, `key`, and `numwant`. It also removes `ip`, `ipv4`, and `ipv6` and never generates them. Before each request, all existing occurrences of these keys are removed from the tracker URL and exactly one Leech-owned value is added as applicable. Other tracker-specific query data is preserved. Redirect targets are sanitized by the same rule.

HTTP(S) uses bounded bodies, normal redirect limits, standard TLS verification, and compact mode. Responses may contain dictionary peers, compact IPv4 `peers`, and compact IPv6 `peers6`.

Peer-list deduplication uses linear work and preserves the first occurrence's
order. Tracker-specific decoded-node and container bounds admit the supported
20,000-peer dictionary response while limiting allocation from ignored fields.
Query sanitization uses memory proportional to URL bytes and retains the
supported 64 MiB tracker-URL input limit.

### 10.3 UDP

UDP trackers implement BEP 15 connection IDs and lifetimes, transaction matching, the specified `15 × 2^n` transaction retransmission schedule, IPv4 and IPv6 response strides, and BEP 41 URL data. This transaction schedule is separate from tracker-loop backoff after a transaction fails. For a dual-stack hostname, one resolved endpoint per available family receives announces with the same session identity.

The two address-family transactions run independently, so a silent family does
not delay the working one. Both are canceled and joined on shutdown.

### 10.4 Tracker state machine

For each phase and tracker:

1. The first announce uses `event=started`.
2. The worker records a `started` request as transmitted only after the protocol transport accepts the complete request for transmission and before waiting for or parsing its response; transmission and response success are separate states.
3. A valid response makes the tracker active and supplies a positive interval.
4. An HTTP success response containing `failure reason`, an invalid interval, a malformed compact peer list, or a UDP response with the wrong transaction ID is a tracker-local failure and does not activate the tracker.
5. HTTP trackers may rerequest early when the candidate pool is depleted, as permitted by BEP 3.
6. UDP trackers never rerequest before their interval unless sending a defined event, as required by BEP 15.
7. Transient failures retry indefinitely with capped exponential backoff and jitter.
8. BEP 31 `retry in` is a not-before duration in minutes; Leech accepts the specified integer form and the deployed decimal-string form, with checked conversion. `never` and definitive HTTP client errors disable only that tracker for the run.

Definitive HTTP client-error classification survives a body-read or body-size
failure unless a parsed applicable retry hint changes it.

On a phase transition or final shutdown, the session first cancels and joins every regular announce loop. It then uses a separate bounded context to send at most one announce for each applicable final event per tracker: full completion sends `completed` and then `stopped`; every other exit sends only `stopped`. `stopped` is attempted for every nonpermanently-disabled tracker to which a `started` request was transmitted, whether or not a response arrived. No regular announce may begin after the final-event sequence starts. Final announce failure is secondary and never changes an existing primary result.

`stopped` has its own bounded transmission opportunity if `completed` stalls;
the total final-event sequence remains bounded.

Peer endpoints from trackers or magnets may be public, private, or loopback. Reject peer endpoints with invalid ports, unspecified addresses, or multicast addresses. Tracker-server destinations retain unrestricted address resolution, including loopback and private addresses. This intentionally permits untrusted inputs to induce connections to local services.

## 11. Candidate peers and dialing

### 11.1 Admission and bounds

Candidate endpoints enter one bounded set keyed by resolved IP and port. TCP and
uTP are two attempts for one endpoint, not separate candidates. At capacity,
repeated announcements from one tracker cannot evict every candidate supplied by
another. Stale or repeatedly failed candidates can be replaced; ordinary
endpoint-backoff state remains bounded as candidates churn.

Pending tracker updates are bounded by their total retained peer data, not just
their event count, and preserve later tracker sources under queue pressure.
Hostname resolution uses bounded results, a small worker limit, and a deadline
per lookup. Admission interleaves with dialing so a large hostname batch cannot
block already available peers or phase shutdown.

Across metadata and transfer, one run may attempt at most 100,000 distinct
resolved IP/port endpoints. Retrying an endpoint already attempted does not spend
this budget. Exhaustion fails the download with a resource error after normal
shutdown; it never forgets a blacklist to admit more endpoints.

### 11.2 Transport race

For each admitted endpoint:

1. Start outgoing uTP to the exact resolved IP and port.
2. After a short fixed head start, start TCP to that same IP and port if uTP has not won.
3. Accept the first transport that completes a valid BEP 3 BitTorrent handshake.
4. Cancel, close, and join the losing attempt.
5. Evaluate BEP 10 or other phase-specific capabilities only after the race. If the winner is unsuitable, close it through normal peer replacement; never retroactively choose the loser.

uTP uses one connected UDP socket per attempt. TCP uses `net.Dialer.DialContext` with the literal resolved address, so DNS cannot silently change endpoint identity.

### 11.3 Identity and penalties

Tracker-supplied peer IDs are not used for candidate deduplication. If supplied,
they are only expected handshake values under BEP 3. A mismatch with that
unauthenticated expectation is not a peer-origin protocol violation and cannot
blacklist the endpoint. A later unpoisoned announcement can retry it.

If two live connections claim the same peer ID, the older established connection
remains and the newcomer closes. The ID is not blacklisted, and another endpoint
may be tried after the retained connection closes.

Blacklisted endpoints are not retried in the run. Ordinary failures and timeouts use per-endpoint backoff rather than strikes.

## 12. Peer-wire behavior

The peer layer consumes a reliable `net.Conn` stream from TCP or uTP and runs one framing/state machine.

### 12.1 Local state and no-upload boundary

Leech always keeps the remote choked and advertises no availability:

- with Fast negotiated, send `Have None` as the sole Fast availability message immediately after the handshake;
- otherwise omit the initial bitfield;
- never send `Have`, `Bitfield`, `Have All`, or `Unchoke`.

Leech sends `interested` only while the remote advertises a wanted piece and `not interested` otherwise. Incoming file requests cannot read payload, cache, or output storage. With Fast they receive `Reject Request`; without Fast they are ignored. Repeated abusive requests are a protocol violation.

The outbound peer-message API contains no file `piece` encoder and no metadata `data` encoder. Transport ACKs, tracker requests, peer control messages, metadata requests/rejects, and block requests are permitted; torrent payload responses are impossible through the API.

### 12.2 Fast Extension

Allowed Fast and availability are independent. Leech may request a choked piece only when the remote has both advertised that piece as available and included it in Leech's Allowed Fast set. Allowed Fast alone never implies availability. Suggest Piece is parsed and may be ignored.

When Fast is negotiated, receiving `Have None` immediately replaces the remote peer's ordinary availability set with the empty set. The separate bounded Allowed Fast set remains intact. Future choked request selection still requires membership in both sets.

With Fast negotiated, every request remains outstanding across a choke until exactly one matching piece or reject arrives or the connection closes. Sending cancel or reaching the local request timeout does not erase the expected terminal response; it moves the request to a bounded tombstone so a late matching piece or reject is consumed safely. If adding a required tombstone would exceed the cap, Leech closes the connection without a strike before forgetting any request. Without Fast, choke implicitly releases pending requests, while the same bounded tombstone rule permits race-delayed matching pieces described by BEP 3.

### 12.3 Extension protocol

Extension IDs are directional and per connection:

- Leech's advertised ID dispatches messages received by Leech.
- The remote's advertised ID is used for messages sent to that remote.
- Repeated handshakes apply additive enable/disable updates.
- Unknown extension names and bounded unknown extension messages are ignored.

### 12.4 Requests and framing

- Request blocks are at most 16 KiB and never cross a piece boundary.
- A peer pipeline is bounded by the local cap and the peer's latest advertised
  `reqq`, including zero and repeated updates. Already queued extension
  handshakes apply before more blocks are assigned.
- Piece messages must match an outstanding or tombstoned request exactly.
- Duplicate endgame responses after one winner are consumed and discarded safely.
- Keepalives preserve otherwise useful idle connections.
- Bounded, well-framed unknown core IDs are ignored.
- Invalid handshakes, impossible indices, malformed bitfields, invalid reserved-bit-dependent messages, and oversized frames immediately blacklist the endpoint.

Active block requests have bounded lifetimes; expired blocks are released or
reassigned while preserving Fast terminal-response obligations. Peer replacement
uses recent useful activity, not a lifetime productivity flag. Persistently
choked peers that neither deliver data nor offer useful Allowed Fast pieces are
rotated out. Ordinary stalls do not cause corruption strikes.

Peer-command enqueue time is bounded so sending Fast or metadata rejects to a
nonreading peer cannot stall the coordinator.

## 13. Piece scheduling and verification

The coordinator tracks remote availability, wanted pieces, block state, outstanding requests, and endpoint provenance.

Wanted-piece initialization takes one pass. Interest and scheduler availability
updates follow changed bits; duplicate `Have`, already-empty `Have None`, and
other unchanged small events must not cause repeated whole-torrent scans.
Sparse availability and choke changes remain proportional to occupied words
without repeatedly sorting a full-torrent delta.

Bulk mode chooses the rarest wanted piece among connected peers, with randomized ties. Streaming mode ranks earlier wanted pieces first but may fetch later available pieces rather than idle a useful connection.

A block normally has one active request. Once every remaining block has been assigned, endgame may duplicate outstanding requests across productive peers. The first accepted response wins and triggers cancels for redundant requests.

### 13.1 Cache staging

Leech creates no cache workspace until validated metadata, selection, and resume establish that network piece transfer is required. It then creates a random private workspace under Go's platform user-cache directory, normally `$XDG_CACHE_HOME/leech` or `~/.cache/leech` on Linux. Directories use `0700` and files `0600` where supported.

Each active piece has a random-access cache file and an in-memory block bitmap plus endpoint provenance. Network reads use bounded block buffers; no whole piece is held in memory. Multiple endpoints may contribute. Cache failure is fatal, with no memory fallback.

Every completed piece must pass SHA-1 verification before it is marked complete
or written to output, whether its bytes came from peers, staging, or existing
files. The finalizer reads a completed staged piece sequentially and verifies it:

- On mismatch, each contributing endpoint receives one strike; the staged piece is removed and rescheduled.
- Strikes are keyed by resolved IP and port across transports and reconnects for this run.
- Three strikes disconnect and blacklist the endpoint.
- An invalid complete metadata candidate gives its sole endpoint one strike.

On success, one output committer opens each affected selected file without following descendant symlinks, writes selected intersections, closes every handle, and only then removes the staged piece. Successful `Write` and `Close` are sufficient; no `fsync` is required.

A cache or output error fails the session. Verified output remains available for future resume. On orderly exit, the finalizer and all cache handles are joined and closed before workspace removal. If the primary operation succeeded, final close or removal failure becomes the returned error; if a primary failure already exists, cleanup failures are secondary diagnostics. Crashes and immediate second signals may leave ignored workspaces.

Later runs do not scan, recover, reuse, or clean abandoned workspaces. Any
power-loss inconsistency in final output is found by rehashing during resume.

## 14. uTP

The in-tree uTP implementation provides an outgoing-only `net.Conn`-compatible stream over one connected UDP socket. It implements BEP 29, not a general transport framework.

It covers:

- v1 headers, packet types, extension chains, and connection-ID rules;
- outgoing SYN setup and sequence-number wraparound;
- ordered byte-stream reassembly;
- receive windows and bounded send state;
- ACK and selective-ACK generation and processing;
- RTT/RTO estimation, retransmission, duplicate-ACK loss detection, and timeout backoff;
- delay-based congestion control and packet sizing;
- FIN and RESET handling;
- context cancellation, deadlines, and idempotent close;
- strict datagram, packet-count, and byte bounds;
- IPv4 and IPv6 connected UDP sockets.

It does not accept unsolicited SYN packets, share a listener, perform hole punching, or expose server APIs. The peer layer depends only on `net.Conn` and does not branch by transport after dialing.

Duplicate packets reuse unchanged selective-ACK state rather than repeatedly
scanning the reorder window. A zero congestion window has bounded timeout
recovery with a one-packet restart, including when no packet remains
unacknowledged. A persistently closed remote receive window is probed at a bounded
rate so a lost reopen update cannot stall queued writes indefinitely.

## 15. Concurrency and lifecycle

Ownership boundaries are:

- **Session coordinator:** torrent, peer, rarity, request, strike, and completion state.
- **Tracker workers:** protocol transactions and regular announce timers.
- **Dial manager:** bounded endpoint attempts and uTP/TCP races.
- **Peer workers:** connection I/O and bounded event/command queues.
- **Piece finalizer/output committer:** serialized verification and output commits.
- **Signal owner:** graceful cancellation; a second signal terminates immediately.

No goroutine starts without observed completion. Admission is bounded before goroutine creation. Raw network I/O is unblocked with deadlines or owned connection closure.

Ordinary transfer shutdown is ordered:

1. Stop scheduling and candidate admission.
2. Cancel and join regular tracker loops so no normal announce can follow `stopped`.
3. Cancel and join dials and peers.
4. Finish or abort the bounded current output operation and join the finalizer.
5. Send `completed` when applicable, then bounded `stopped` announces through separate one-shot operations.
6. Close all remaining owned cache resources.
7. Remove the current workspace.
8. Return the primary result, applying the cleanup-error rule from §13.1.

Metadata-only phase transition uses the same quiescence rule but sends only `stopped` and creates no cache workspace. A second termination signal may bypass cleanup.

## 16. Supported bounds

These are fixed supported-domain limits, not tuning promises:

| Resource | Limit |
| --- | ---: |
| Total `.torrent` or info-dictionary bytes | 64 MiB |
| Tracker URL bytes | 64 MiB |
| Decoded bencode values and dictionary entries | 1,000,000 |
| Entries in one bencode list or dictionary | 200,000 |
| Bencode nesting depth | 64 |
| Total torrent length | 256 GiB |
| Files | 100,000 |
| Path components per file | 64 |
| Encoded bytes in one relative path | 4,096 |
| Pieces | 2,000,000 |
| Piece length | 64 MiB |
| Trackers | 64 unique URLs |
| DNS answers retained per hostname | 64 |
| Magnet-embedded peers | 1,024 |
| Candidate endpoints retained | 20,000 |
| Distinct peer endpoints attempted per run | 100,000 |
| Active peer connections | 64 |
| Concurrent endpoint races | 32 |
| Concurrent staged pieces | 64 |
| Sum of staged declared piece lengths | 512 MiB |
| Peer-wire frame | 1 MiB |
| Outstanding requests per peer | 128 |
| Outstanding requests globally | 4,096 |
| Recently canceled request tombstones per peer | 256 |
| Metadata requests in flight | 32 |
| Session event queue | 4,096 |
| Per-peer outbound command queue | 256 |
| HTTP tracker response body | 8 MiB |
| UDP/uTP datagram | 64 KiB |
| uTP unacknowledged outbound packets | 1,024 |
| uTP out-of-order packets | 2,048 |
| uTP buffered bytes per direction | 4 MiB |
| Tracker interval or finite BEP 31 delay | 1 second to 7 days |

All arithmetic is checked in 64 bits before conversion to `int`, allocation, seeking, duration conversion, or protocol fields. `reqq` and remote windows may be clamped to local capacity. Nonpositive or over-limit tracker intervals and finite BEP 31 delays are rejected and disable that tracker; Leech never shortens a tracker's not-before time.

Malformed encoded values are never byte-truncated. A complete compact peer string must have a valid stride. Once valid, endpoint records beyond retention limits may be dropped whole and reported diagnostically.

## 17. Security and trust boundaries

Torrent files, metadata peers, trackers, peer messages, paths, cache payload, and existing output are untrusted.

Controls are deliberately local:

- exact info-hash and piece-hash verification;
- strict byte, structure, queue, and state bounds;
- path confinement and static symlink refusal;
- private cache permissions;
- endpoint-scoped strikes and blacklists;
- standard TLS certificate verification;
- cryptographic randomness for identities, keys, ports, and randomized ties;
- an outbound API with no payload-producing message.

Accepted residual risks are:

- SHA-1 weaknesses inherent to v1;
- plaintext HTTP default-tracker traffic;
- disclosure and public treatment of `private=1` torrents, contrary to BEP 27;
- tracker- or magnet-induced connections to private and loopback services;
- an unbound announced port that may coincide with another service;
- peer-ID spoofing affecting live deduplication;
- abandoned cache data after crashes;
- hostile concurrent local filesystem races;
- poor or failed downloads when peers refuse to serve a nonreciprocating client;
- nonstandard `left=1` accounting before metadata.

## 18. Failure semantics

- Malformed, v2, hybrid, or out-of-bounds metadata fails before output or cache creation.
- `private=1` does not fail and has no special behavior.
- Tracker failures are isolated; remaining trackers continue.
- Peer failures return unfinished blocks to scheduling unless the endpoint is blacklisted.
- Corrupt pieces never reach output.
- Cache and output failures are fatal.
- Exhausting the per-run distinct endpoint budget is fatal after graceful cleanup.
- A no-progress timeout is fatal after graceful cleanup.
- Partial verified output remains in final paths for later resume.
- Final tracker-event failure never replaces the primary result.

## 19. Validation

Validation is local and deterministic; it never uses existing clients or live trackers.

The checks below are required evidence, not a record of completed validation.
Outstanding work belongs in [TODO.md](TODO.md).

### 19.1 Command-line boundary

- CLI parser tests covering every option form, `--`, hash-shaped paths, repeated
  file selection, list-mode conflicts, log levels, bad arity, malformed durations,
  and unsupported stdin;
- CLI golden tests covering help text, usage failures, quoted untrusted names,
  redacted tracker diagnostics, log filtering, TTY status replacement, non-TTY
  line output, and JSON-quoted file lists;
- CLI selection tests covering exact paths, each supported metacharacter,
  directories, multiple patterns, `**`, no matches, padding, symlinks,
  single-file torrents, and explicit selection overriding magnet `so`;
- CLI lifecycle tests proving that command-line and pre-network validation
  failures create no output or cache files and send no network traffic, that local
  file listings stay offline, and that remote listings stop after metadata
  acquisition without touching output or piece-cache paths;
- CLI integration tests covering output mapping, default overwrite, optional
  resume, bulk and streaming scheduling, no-progress timeout behavior, exit
  statuses, partial-output messages, and first-signal cleanup.

### 19.2 Parsers and protocol state

- golden vectors for bencoding, exact info hashes, compact endpoints, peer frames, Fast messages, BEP 10 directionality, metadata messages, UDP trackers, and uTP packets;
- fuzzing of parsers and state-machine transitions for bencoding, metainfo, magnets, trackers, peer wire, BEP 6, BEP 10, metadata transfer, uTP, transport racing, peer-ID deduplication, strike accounting, and session shutdown;
- properties for checked ranges, file normalization, BEP 53 indexing, selection, block coverage, rarity, tracker accounting, and sequence wraparound;
- differential checks of accelerated selection against Go's `path.Match`,
  including bracket classes that consume `/` while `*` and `?` cannot;
- deterministic tracker models covering premetadata `left=1`, actual `left`, started/regular/completed/stopped sequences, HTTP early rerequests, UDP interval enforcement, BEP 31, phase quiescence, stopped ordering, first-value-wins and last-value-wins duplicate query parsing, HTTP success responses with failure bodies, malformed compact IPv4 and IPv6 lists, a transmitted `started` with no response followed by `stopped`, and unequal IPv4/IPv6 support;
- deterministic peer models covering a Fast seed using `Have None`, a reciprocal leecher, an Allowed Fast peer, a peer without Fast, a peer that requires ordinary availability before honoring Allowed Fast, and a peer that would retain stale availability unless `Have None` clears it;
- peer-state tests covering Fast choke/reject/cancel, repeated extension handshakes, local/remote extension IDs, request tombstones, metadata rejection, and independent bounded availability and Allowed Fast sets;
- deterministic dial tests proving exact-endpoint use, first-handshake wins, loser cancellation/close/join, duplicate peer-ID handling, and reconnect eligibility;
- strike tests across transports and reconnects, including mixed-source corrupt pieces and sole-source invalid metadata.

### 19.3 Transport, storage, and lifecycle

- uTP simulation with loss, delay, duplication, reordering, selective ACKs, window pressure, timeout, wraparound, teardown, and cancellation;
- race-detector tests for tracker transitions, peer replacement, event queues, cache finalization, and both signal paths;
- filesystem tests for traversal, collisions, omitted padding paths, omitted symlink lengths, descendant symlinks, padding, selected boundaries, sparse growth, overlong files, interruption, and resume;
- local fake HTTP/UDP trackers and TCP/uTP peers for complete `.torrent`, magnet, and bare-hash sessions.

### 19.4 No-upload and terminal-response invariants

- wire and storage assertions that an incoming payload or metadata request causes no payload, cache, or output disk read and never emits file `piece` or metadata `data`;
- terminal-response assertions that each admissible Fast payload request and each rejectable metadata request produces exactly one rejection, while ignored cases produce none;
- tombstone assertions that an exact late `Piece` or `Reject Request` consumes its tombstone once, or the connection closes before the bounded tombstone is forgotten;
- wire assertions that Leech never emits `Unchoke`, `Have`, `Bitfield`, or `Have All`, never requests an unavailable Allowed Fast piece, and reports tracker `uploaded=0` on every announce.

Tests observe bounded, read-only per-session debug events or counters for ordinary choke duration, empty availability, Allowed Fast receipt and usefulness, first useful block, tombstone consumption, peer-ID collision, transport-race outcome, metadata refusal, compact peer family, tracker final-event attempts, and staging or hash failure. These observations do not enable upload, additional discovery, or persistent session state.

Production builds are pure Go. Race tooling may enable cgo or require a C toolchain where the current Go release requires it.

## 20. Key tradeoffs

- **No upload:** preserves product identity but substantially reduces swarm cooperation.
- **Premetadata `left=1`:** enables tracker-based metadata discovery without knowing size, but is not BEP-defined accounting.
- **Ignoring `private=1`:** keeps one public-torrent behavior but discards BEP 27 isolation and may disclose private swarm identifiers.
- **No listener with a fake port:** improves tracker acceptance while advertising an unreachable endpoint.
- **All trackers independently:** maximizes discovery and simplicity at the cost of tracker traffic and tier semantics.
- **Disk-staged pieces:** bounds Go memory and handles skipped boundary bytes simply, at the cost of extra I/O and crash residue.
- **Stateless resume:** avoids a database at the cost of startup hashing and boundary redownloads.
- **One UDP socket per uTP connection:** simplifies ownership at the cost of file descriptors.
- **Standard library only:** removes dependency risk but makes uTP correctness the project's responsibility.
- **No external interoperability testing:** keeps validation deterministic but raises reliance on precise specifications and independent wire fixtures.
