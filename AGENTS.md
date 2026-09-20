# Leech project guidance

## Context map

- Nested `AGENTS.md` files: none.
- `beps/` contains the project's authoritative, up-to-date BitTorrent specifications. Treat BEP 3 as the base protocol and consult the relevant extension BEPs directly.

## Product boundary

Leech is a KISS, robust, download-only BitTorrent v1 client written in Go and exposed as a CLI.

- Accept `.torrent` files, magnet URIs, and bare v1 info hashes in 40-character hexadecimal or 32-character Base32.
- Always include `http://tracker.opentrackr.org:1337/announce`, in addition to trackers supplied by the input. Run every configured tracker independently rather than treating trackers as failover tiers. Each phase starts with `started`; HTTP may rerequest early for peers, while UDP uses BEP 15's transaction schedule and must wait for its interval or an event between announces. Stop and join regular loops before final events; full completion sends `completed` then `stopped`. Disable a tracker after permanent failure or an invalid interval/retry delay; never shorten a tracker not-before time. Retry transient failures with bounded exponential backoff.
- Honor magnet `tr`, `x.pe`, `dn`, and BEP 53 `so` parameters.
- Discover peers only through HTTP(S) trackers, UDP trackers, and magnet-embedded peers. Permit loopback and private-network peer endpoints, but reject invalid ports, unspecified addresses, and multicast addresses. Do not implement DHT, PEX, local peer discovery, or other decentralized discovery.
- Announce one random port from the dynamic range `49152–65535` to every tracker for the run even though the client does not listen on it. Do not probe, bind, or reserve the port.
- Support outgoing TCP and uTP peer connections over IPv4 and IPv6. Dial the same resolved endpoint with both transports: give uTP a short head start, then start TCP and keep the first connection that completes a valid BEP 3 handshake. Evaluate extension capabilities only after the race. Cancel, close, and join the losing attempt. Use one connected UDP socket per active uTP connection. Before dialing, deduplicate candidates by resolved IP and port and treat both transports as one race. Do not trust tracker-supplied peer IDs for deduplication. If two live handshakes claim the same peer ID, retain the older established connection and close the newcomer; do not blacklist that ID or prevent later connections after the retained one closes. Do not listen for inbound peer connections or implement NAT traversal, port mapping, or hole punching.
- Never upload torrent file payload data or serve torrent metadata. Advertise no piece availability, even after verifying pieces. The client is not a seeder.
- Support the Fast Extension (BEP 6), including immediate `Have None`, independent availability/Allowed Fast state, and exactly one terminal response per request. Canceled or locally timed-out Fast requests remain bounded tombstones; close without a strike before forgetting one if the tombstone cap is full.
- Ignore `private=1` and treat the torrent as public, intentionally overriding BEP 27. Do not support web seeds.
- Support multi-file torrents and selective downloads chosen by exact paths or glob patterns. Match case-sensitively relative to the torrent root using `/`; support `*`, `?`, and `[]`, but not `**`. A matched directory includes its descendants, and no matches is an error. An explicit user selection overrides magnet `so`.
- Report tracker `left` against the entire torrent. Completing only a selection sends `stopped`, not `completed`; send `completed` only when the entire torrent is retained and verified. On completion, cancellation, timeout, or ordinary failure, make a best-effort `stopped` announcement to every functioning tracker under a short bounded deadline, then exit regardless of announce failures.
- Download one torrent at a time. Bulk mode uses rarest-first piece selection with randomized ties. Optional streaming mode prioritizes pieces sequentially but may fetch later available pieces rather than leave connections idle. Use endgame duplicate requests and cancel redundant requests after one succeeds.
- Remain stateless across runs: keep no configuration, session file, resume index, cache index, or reusable cached data. With known metadata, finish resume hashing before tracker or peer activity. Magnet and bare-hash inputs use a separate metadata-only discovery phase, then stop and join its network workers before selection and resume. Exit without starting transfer discovery if selected content is already complete. Redownload an entire piece when skipped ranges prevent verification from existing selected output. If an existing selected file is too long, hash only its expected prefix and truncate the excess after successful verification. Without resume, truncate and overwrite existing selected output.
- Before metadata is known, announce the approved nonstandard `left=1` sentinel. Keep fetched metainfo in bounded memory for the current run and discard it on exit. Try the first bounded advertised metadata size without waiting for consensus and have one peer supply the complete candidate. Verify it against the info hash, and rotate to another peer or advertised size after failure. BEP 10 extension IDs are directional and per connection: local IDs receive; remote IDs send.
- Create a randomly named private piece workspace under Go's platform user-cache directory (normally `$XDG_CACHE_HOME/leech` or `~/.cache/leech` on Linux) only after validation, selection, and resume establish that network transfer is needed; never retain whole pieces in memory. Use directory mode `0700` and file mode `0600` where supported. Bounded block-sized network and I/O buffers are allowed. Blocks for one piece may come from multiple peers. Fail if the cache is unavailable or full; never fall back to whole-piece memory storage.
- Remove a staged piece after verification and successful output writes. Remove the current run's incomplete workspace on orderly cancellation, timeout, or error. Do not scan, recover, or clean abandoned workspaces on later runs.
- A piece overlapping selected and unwanted files may be downloaded and cached in full, but only selected byte ranges may be written to output.
- Let selected output files grow as verified pieces arrive, and create selected zero-length files. Write selected data directly to final paths after piece verification. Accept omitted BEP 47 padding paths and omitted symlink lengths, recognize padding without writing it, never create torrent-provided symlinks, ignore executable and hidden attributes, and reject selected symlink entries.
- Resolve the chosen output root once if it is a symlink, then refuse symlinks below it. Reject unsafe, unrepresentable, duplicate, or colliding paths instead of renaming them. A concurrent hostile local process racing filesystem operations is out of scope.
- Use an opaque, cryptographically random 20-byte peer ID per run, without client or version branding.
- Retry indefinitely by default. An optional no-progress timeout starts only after metadata acquisition, selection, and resume verification finish; it measures time without a newly verified file piece, not total runtime.
- Allow tracker URLs to resolve to any address, including loopback and private networks, without special filtering. Strip all Leech-owned announce parameters plus `ip`, `ipv4`, and `ipv6` from HTTP tracker URLs and redirects before adding one authoritative value for each emitted parameter.
- Design for torrents measured in gigabytes, not terabytes, with explicit conservative resource limits.
- Target Linux first while keeping the design portable where practical.
- Proxying, anonymity features, peer-wire encryption, and bandwidth limiting are out of scope.
- BitTorrent v2 and hybrid torrents are out of scope.

## Design principles and invariants

- Choose the smallest design that meets the stated interoperability requirements. Do not add extension points for out-of-scope protocols.
- Treat metainfo, tracker responses, peer messages, magnet parameters, filenames, and cached payload data as untrusted input.
- Compute a v1 info hash from the exact bencoded `info` byte sequence and reject malformed or non-canonical bencoding as required by BEP 3.
- Verify every completed piece against its SHA-1 hash before marking it complete or writing it to output, regardless of whether bytes came from peers, the cache, or existing files.
- Keep output writes confined to the selected destination and staging writes confined to a dedicated user-cache location. Reject absolute paths, traversal components, invalid path structures, collisions, unsafe symlinks, and names that cannot be represented safely on the target filesystem.
- Apply the explicit byte, decoded-node, container-entry, queue, request, packet, timer, disk, and concurrency limits in `DESIGN.md`; do not let peer- or metainfo-controlled values cause unbounded work or overflow.
- Track corruption strikes by resolved IP and port, independent of transport and across reconnects for the current run. Give every endpoint that contributed to a failed piece one strike; give the sole supplier of invalid metadata one strike. Disconnect and blacklist an endpoint after three strikes. Do not key penalties to spoofable peer IDs.
- Immediately blacklist endpoints that send severe protocol violations such as an invalid handshake, impossible indices, invalid bitfields, or oversized messages. Ignore bounded, well-framed unknown peer message IDs for forward compatibility. Ordinary disconnects and timeouts are not violations.
- Periodically replace persistently choked, unproductive connections while retaining peers that deliver data or offer useful Allowed Fast pieces.
- Preserve the no-upload invariant independently of peer behavior: no peer request may cause `Unchoke`, piece availability, torrent file payload, or metadata data transmission.
- Never create output files for unselected content. Full in-progress pieces may exist only in the internal cache.
- Treat final output paths as partial until their selected content has passed piece verification and the download completes.
- A successful output `Write` and `Close` is sufficient before deleting a staged piece; do not require `fsync`, and recover from any resulting power-loss inconsistency by rehashing in a later resume run.
- Fail the whole download after any cache or output error, including disk-full, permission, short-write, and close failures.
- Keep the single-active-torrent lifecycle explicit so cancellation, cleanup, and completion are deterministic. On `SIGINT` or `SIGTERM`, stop scheduling work, cancel and join connections, attempt bounded `stopped` announces, remove the current cache workspace, close output files, and exit; a second signal may terminate immediately.

## Go and dependencies

- Use idiomatic Go with explicit ownership, cancellation, I/O, and goroutine-lifecycle contracts.
- Keep platform-independent logic portable; isolate any Linux-specific filesystem or networking behavior.
- Use only the Go standard library and implement all BitTorrent protocol components in-tree, including bencoding and uTP. Reversing this rule requires an explicit project decision.
- Target only the current stable Go release; backward toolchain compatibility is not a goal. The current environment provides Go 1.27.1 through an interactive shell (`bash -ic 'go ...'`).
- Keep production builds pure Go and free of cgo.

## Validation

- Fuzz all untrusted parsers and state-machine boundaries with crisp safety and protocol invariants, including Fast, BEP 10 directionality, tracker event ordering, transport racing, deduplication, strikes, and shutdown.
- Run race-detector tests for concurrent components and deterministic simulated-network tests for uTP timing, loss, reordering, duplication, and wraparound. Tooling may enable cgo or require a C compiler when the Go race detector requires it; this does not relax the pure-Go production constraint.
- Do not perform automated or manual interoperability tests against existing BitTorrent clients or live trackers. Use specification-derived fixtures plus local deterministic peers and trackers for integration coverage.
