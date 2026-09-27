# Leech project guidance

1. **KISS.** Choose the simplest solution that satisfies the request and preserves
   approved contracts.
2. **Keep guidance current.** Update the relevant root or nested `AGENTS.md` when
   something changes or you learn something worth remembering for future work.
   Record durable knowledge in its owning scope and remove stale guidance.

Leech is a stateless, single-torrent, download-only BitTorrent v1 CLI in Go.

## Read first

- [DESIGN.md](DESIGN.md) is the authoritative behavior contract. Read relevant
  sections before changing a boundary. Passing tests or completed historical
  tasks do not establish full conformance.
- [beps/](beps/) contains the authoritative local protocol specifications. Start
  with BEP 3 and the relevant extensions; DESIGN records intentional overrides.
- [README.md](README.md) is the user-facing build and usage guide.
- Read applicable descendant guidance before working in a subtree; it is not
  loaded recursively. For changes spanning packages, read both guides. Changes
  to approved behavior require explicit user approval; routine internal changes
  do not.

## Context map

```text
internal/
├── bencode/AGENTS.md  — exact byte spans and bounded decoding
├── cli/AGENTS.md      — arguments, presentation, and signals
├── peer/AGENTS.md     — peer state, endpoint identity, and transport races
├── session/AGENTS.md  — phases, scheduling, admission, and shutdown
├── storage/AGENTS.md  — output confinement, staging, and resume
├── torrent/AGENTS.md  — sources, immutable metadata, and selection
├── tracker/AGENTS.md  — transactions, accounting, and final events
└── utp/AGENTS.md      — outgoing stream state and socket ownership
```

`cmd/leech` is the process adapter; read the CLI guide when changing it.
`internal/limits` holds shared supported-domain limits from DESIGN §16.
Keep this map complete. Record durable decisions and pitfalls in their owning
guide. Omit task history from working guidance.

## Shared guardrails

- Preserve the no-upload boundary: never serve torrent payload or metadata or
  advertise acquired pieces. Incoming peer requests must not reach storage.
- Treat encoded input, messages, paths, cached payload, and existing output as
  untrusted. Check bounds and 64-bit arithmetic before allocation, conversion,
  seeking, or duration use. Bound retained bytes and work as well as item counts;
  keep operational timings named in their owning component.
- Verify complete pieces before writing selected output. Stage pieces on disk
  in the current run's private cache workspace; never retain whole pieces in
  memory or add reusable cache or resume state.
- The session coordinator owns mutable torrent state. Workers own I/O and send
  bounded events. Every goroutine needs an owner, cancellation/unblock path, and
  joined completion. Keep types with their owning package and interfaces small.
- Use only the Go standard library, including the in-tree protocols. Production
  must remain pure Go. Target the current stable Go release and Linux first;
  isolate platform-specific code where practical.

## Build and validation

The environment supplies Go 1.27.1 through an interactive shell. If `go` is not
on the ordinary PATH, use `bash -ic 'make check'` or `bash -ic 'go ...'`.

```sh
make build  # CGO_ENABLED=0; produces ./leech
make test   # go test ./...
make race   # go test -race ./...
make vet    # go vet ./...
make check  # test, race, vet, then build
```

Run focused checks while changing code, then applicable integrated checks. Race
tooling may need cgo or a C compiler; production builds still must not. Fuzz
untrusted parsers and state transitions with useful valid seeds, explicit
invariants, and retained regressions. Use independently specified wire bytes and
expected outcomes rather than Leech's own encoders alone.

Never test against live trackers or existing BitTorrent clients without the user's
explict permission.
