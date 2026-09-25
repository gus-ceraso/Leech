# Security review: malicious peers and trackers

Scope: static review of production code. This review follows untrusted tracker responses and peer traffic through parsing, scheduling, metadata acquisition, and output. Tests were neither examined nor run.

## Findings

### F-01 — Piece-count scans make peer admission and small availability messages costly (high)

A peer can send repeated `Have` or `Have None` messages after the handshake. Each message calls `refreshInterestChange`, which can scan every piece in the torrent even when availability did not change. The transfer coordinator then rebuilds the scheduler's availability from every wanted piece. The supported limit is two million pieces, so a nine-byte `Have` wire frame can trigger several full scans; multiple connected peers can sustain this work without sending payload data. Before reading any availability, peer admission calls `SetWanted` once per wanted piece, and each call scans every piece while availability is empty. An all-selected, two-million-piece torrent therefore makes one peer's startup quadratic in piece count. See `internal/peer/state.go:157-165,237-262,402-418` and `internal/session/transfer.go:441-458,808-812,880-888`.

Initialize wanted pieces in one batch. Update interest and scheduler availability from changed bits, and treat duplicate `Have` and already-empty `Have None` as no-ops.

### F-02 — A tracker can blacklist an honest peer by lying about its peer ID (medium)

Dictionary tracker peers can include a `peer id`. The candidate keeps that unauthenticated value as `ExpectedPeerID`; the dialer passes it to `ReadHandshake`, which classifies a mismatch as a peer protocol violation. If the race has no successful transport, one such mismatch makes `DialManager.Race` blacklist the endpoint for the run. A malicious tracker can pair an honest peer's address with a false ID and make Leech reject that peer even if its handshake has the correct info hash. It can also add a false ID to an endpoint previously supplied without one by another tracker. See `internal/tracker/http.go:510-517`, `internal/peer/candidates.go:245-249`, `internal/session/run.go:582`, `internal/peer/dial.go:174-188`, and `internal/peer/handshake.go:52-57`.

Keep tracker-provided ID mismatches distinct from peer-origin protocol violations, and do not blacklist the endpoint on that basis. If connecting despite a mismatch is desired, that choice also needs an explicit update to the documented expected-ID contract.

### F-03 — Tracker peer lists can stall discovery and fill a large event queue (high)

An HTTP tracker may return up to 20,000 dictionary peers with distinct hostnames in one response. The metadata and transfer coordinators call `pool.Admit` for every entry on their scheduling goroutine; each admission performs a synchronous DNS lookup under the phase context, which has no per-lookup or batch deadline. A tracker can make the coordinator spend a long time resolving names before it tries usable peers. During that time, tracker workers can enqueue up to 4,096 updates, each retaining another large peer slice, so the queue's event count does not provide a practical memory bound. See `internal/tracker/http.go:485-520`, `internal/peer/candidates.go:110-145`, `internal/session/metadata.go:301-317`, `internal/session/run.go:553-585`, and `internal/session/run.go:418-426`.

Bound the work and retained bytes per tracker update before enqueueing it. Resolve peer hostnames with a small concurrent worker limit and per-lookup deadline, allowing already-admitted endpoints to remain schedulable.

### F-04 — A peer that sends one block can hold requests and a connection indefinitely (high)

The transfer path has no active block-request timeout. After one accepted block, it sets `productive = true`; `findUnproductive` then skips that connection forever, regardless of later inactivity or choke state. A malicious peer can answer one request and stop responding while keeping its connection open. Its outstanding blocks remain assigned, and at the 64-peer cap such connections also prevent new admission. Without the optional global no-progress timeout, the download can stall indefinitely. See `internal/session/transfer.go:541-567`, `internal/session/transfer.go:667-679`, and `internal/session/transfer.go:829-855`.

Expire or reassign individual block requests and rotate peers based on recent useful activity. Keep Fast request tombstones when a local request times out, as the peer protocol state already supports.

### F-05 — Compact tracker peer parsing has quadratic cost (high)

Each compact peer entry calls `appendUniqueHTTPPeers`, which creates a new map and copies all previously retained peers before appending one entry. A tracker can return 20,000 unique IPv4 peers in about 117 KiB, causing roughly 200 million map insertions, plus repeated allocations, on the tracker worker before those peers are offered for dialing. The same pattern affects dictionary peers. See `internal/tracker/http.go:485-520`, `internal/tracker/http.go:525-575`, and `internal/limits/limits.go:21`.

Build one deduplication map per response and reuse it while appending both IPv4 and IPv6 peers.

### F-06 — Invalid bitfield spare bits bypass peer rejection (low)

Both bitfield validators test only one spare bit with `1 << (spare - 1)`. When the piece count leaves two or more spare bits, a peer can set any of the other spare bits and still pass validation. The availability parser ignores those bits, so this does not grant piece availability, but the malformed bitfield avoids the required protocol-violation blacklist. See `internal/peer/wire.go:442-450` and `internal/peer/state.go:382-399`.

Reject the last byte when any of its spare bits are set, using a mask of `(1 << spare) - 1` in the shared validation path.

### F-07 — One tracker can exhaust the candidate pool and block honest peers (high)

The shared candidate pool holds at most 20,000 endpoints and never evicts one. A malicious tracker can fill it in one compact peer response with unreachable, syntactically valid addresses. Later endpoints from other trackers fail admission with `ErrCandidateLimit`; both coordinators ignore that error, so honest peers never enter the dial set. The existing candidates remain in the pool even after dial failures and backoff. See `internal/limits/limits.go:21`, `internal/peer/candidates.go:234-255`, `internal/session/metadata.go:301-317`, and `internal/session/run.go:574-583`.

Reserve admission capacity across tracker sources or replace stale, repeatedly failed candidates so later responses can supply dialable peers.

### F-08 — Duplicate uTP packets repeatedly rebuild a large selective ACK (medium)

A uTP peer can leave the next expected packet missing while filling the reorder map with about 2,000 one-byte packets. Each duplicate out-of-order packet then calls `AckPacket`, which scans that map twice to rebuild the same selective-ACK mask. The socket adapter ignores that prebuilt ACK and calls `AckPacket` again, for four full map scans and an outgoing ACK per small incoming packet. The peer can sustain this work without advancing the stream. See `internal/utp/receive.go:149-207`, `internal/utp/receive.go:274-324`, and `internal/utp/conn.go:444-475`.

Send the ACK already returned by the receive state, and reuse its selective-ACK mask while the receive window is unchanged.

### F-09 — Small tracker responses can expand into very large decoded trees (high)

The HTTP tracker parser caps body bytes at 8 MiB but inherits the metainfo decoder's one-million-value and 200,000-entry limits. A tracker can put hundreds of thousands of tiny values in an unused response field. Each decoded value occupies a `Value` struct and list backing storage, so a response of only a few MiB can consume more than 100 MiB while parsing. Independent tracker workers can do this concurrently. See `internal/tracker/http.go:363-367`, `internal/bencode/bencode.go:34-48`, `internal/bencode/bencode.go:84-92`, `internal/bencode/bencode.go:313-333`, and `internal/limits/limits.go:7-10,33`.

Use tracker-specific decoded-node and container limits sized for the supported peer response, and bound decoded memory separately from wire bytes.
