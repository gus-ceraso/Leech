# Leech

Leech is a download-only BitTorrent v1 CLI written in Go. It handles one torrent
at a time, stages pieces on disk, verifies them before writing selected data to
final paths, and never uploads file payload or torrent metadata. Linux is the
first supported environment.

[DESIGN.md](DESIGN.md) defines the required behavior.

## Build

Use the current stable Go release. The implementation uses only the standard
library.

```sh
make build
make check
```

`make build` produces `./leech` without cgo. `make check` runs tests, the race
detector, vet, and a build. Race-detector tooling may require a C compiler.

## Use

Put options before the source. `--output` names an existing directory and defaults
to the current directory. Leech preserves the torrent's name under that directory.

```sh
# Download a local torrent.
./leech example.torrent

# Download matching paths, relative to the torrent root.
./leech --output Downloads --file 'docs/*.pdf' example.torrent

# Verify and reuse existing selected output.
./leech --resume --output Downloads example.torrent

# List selectable paths, one JSON-quoted path per line.
./leech --list-files example.torrent

# Download from a magnet URI or a hexadecimal v1 info hash.
./leech 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567'
./leech 0123456789abcdef0123456789abcdef01234567
```

Bare hashes also accept 32-character Base32. Run `./leech --help` for all options
or see the [CLI contract](DESIGN.md#4-command-line-interface).

- **Selection:** Repeat `--file` to combine case-sensitive paths and patterns.
  Use `/`, `*`, `?`, and `[]`; `**` is unsupported. A directory match includes its
  descendants. Quote patterns to keep the shell from expanding them.
- **Existing output:** Selected files are overwritten unless `--resume` is set.
  Local `.torrent` resume hashing precedes network activity. Magnet and bare-hash
  inputs first acquire metadata, stop that discovery phase, then scan output
  before starting transfer discovery.
- **Transfer order:** `--stream` prioritizes earlier pieces but can fetch later
  available pieces. It does not promise uninterrupted playback.
- **Timeout:** `--timeout 20m` limits time without a newly verified file piece.
  It starts at file transfer, not during metadata discovery or resume hashing.
  Without it, retrying has no time limit.
- **Output:** `--list-files` uses standard output. Diagnostics go to standard
  error with UTC timestamps. `--loglevel info` enables progress, a final summary,
  and completion messages. Redirected transfer progress appears at most every
  30 seconds; terminal status refreshes at most once per second.
- **Diagnostics:** `--loglevel debug` adds state changes, batched request/late-reply
  counts, and per-transport race outcomes with safe failure categories. Final
  totals distinguish TCP/uTP wins, failures, and cancellations. Skipped tracker
  URLs are counted by reason without printing them. Final tracker-announcement
  failures are explicitly nonfatal warnings and do not change a successful exit.
  Summary payload rates include discarded bytes; `useful-connections` counts
  connections that staged data, with reconnects counted separately.

## Limits

Leech discovers peers through HTTP(S) and UDP trackers and magnet `x.pe` entries.
It always adds [`http://tracker.opentrackr.org:1337/announce`](http://tracker.opentrackr.org:1337/announce)
and skips unusable tracker URLs, including WebSocket URLs. It races outgoing
uTP and TCP connections over IPv4 and IPv6.

There is no DHT, PEX, local peer discovery, web-seed support, inbound peer
listener, upload, BitTorrent v2, or hybrid-torrent support. Leech ignores
`private=1` and treats those torrents as public, including use of the default
tracker. Peers that require reciprocation may refuse to serve it.

Automated validation uses local deterministic tracker and peer fixtures. Live
Big Buck Bunny downloads have completed with HTTP/UDP tracker discovery and TCP
peers. A known uTP setup sequencing defect drops the first incoming data packet
with libutp-style sequence numbering; live uTP transfers remain unverified.
See the design's [supported bounds](DESIGN.md#16-supported-bounds) and
[trust boundaries](DESIGN.md#17-security-and-trust-boundaries) for the complete
limits and accepted risks.
