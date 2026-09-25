# Leech

Leech is a download-only BitTorrent v1 client with a command-line interface. It
downloads one torrent at a time, verifies every piece before writing selected
bytes to their final paths, and never uploads torrent data.

## Build

```sh
make build
make check
```

`make build` produces `./leech` without cgo. `make check` runs the tests,
race detector, vet, and build in sequence. The implementation uses only the Go
standard library.

## Use

```sh
# Download a torrent into the current directory.
./leech example.torrent

# Download only matching paths, relative to the torrent root.
./leech --output Downloads --file 'docs/*.pdf' example.torrent

# Verify and reuse existing selected output.
./leech --resume --output Downloads example.torrent

# Prioritize earlier pieces for streaming playback.
./leech --stream --output Downloads example.torrent

# List selectable files without starting a transfer.
./leech --list-files example.torrent

# Download from a magnet URI or a v1 info hash in hexadecimal or Base32.
./leech 'magnet:?xt=urn:btih:…'
./leech 0123456789012345678901234567890123456789
```

Run `./leech --help` for all options. Without `--resume`, selected output files
are overwritten. With resume enabled, Leech hashes existing selected content
before contacting trackers. Magnet and hash inputs first acquire and verify
metainfo, then begin a separate transfer phase. A no-progress timeout can be set
with `--timeout`; it measures time since the last newly verified file piece.

Leech accepts HTTP(S) and UDP trackers from the input and always adds its
configured default tracker. It discovers peers from those trackers and magnet
peer entries, then races outgoing TCP and uTP connections over IPv4 and IPv6.
File selection supports exact paths and `*`, `?`, and `[]` patterns. Selected
data is staged in the user cache until its complete piece hash verifies.

Leech intentionally does not implement DHT, PEX, local peer discovery, web seeds,
inbound peer connections, uploads, BitTorrent v2, or hybrid torrents. It treats
`private=1` torrents as public.

Automated validation uses deterministic local tracker and peer fixtures. The
project has not tested interoperability against live trackers or existing
BitTorrent clients.
