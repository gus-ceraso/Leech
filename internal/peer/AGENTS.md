# Peer guidance

Read [DESIGN §§11–12](../../DESIGN.md#11-candidate-peers-and-dialing) and
BEPs [3](../../beps/bep_0003.rst), [6](../../beps/bep_0006.rst),
[9](../../beps/bep_0009.rst), and [10](../../beps/bep_0010.rst).

## State and I/O

- The coordinator owns `PeerState` and request state. `ConnectionWorker` owns
  I/O; queued events own their payload until consumed. Bound aggregate retained
  payload, use cancellation-safe backpressure, and preserve the first terminal
  error even when its notification cannot enqueue.
- The outbound API must contain no file `piece` or metadata `data` encoder.
  Keep incoming requests independent of storage. Fast `Have None` and request
  rejection are control behavior, not permission to expose payload access.
- Availability and Allowed Fast are separate sets; choked requests require both.
  `Have None` clears only availability. Preserve initial availability-message
  ordering and use one validator for every spare bit in an initial bitfield.
- Initialize wanted state in a batch and propagate changed bits. Preserve sparse
  update order so small messages need neither whole-torrent scans nor full-set
  sorting. Scheduler work is a separate concern owned by session.
- Match terminal responses exactly. Cancel and local timeout do not erase Fast
  obligations: retain bounded tombstones and consume an exact late terminal once,
  or close without a strike before forgetting one. Apply §12.2's separate
  non-Fast choke behavior.
- BEP 10 mappings are directional and per connection: local IDs receive, remote
  IDs send. Repeated handshakes update state. Preserve the distinction between
  absent `reqq` and explicit zero; metadata rejects use the current remote ID.
- Compare metadata message types at their full decoded integer width before
  narrowing. Unknown integer types remain ignorable within parser/frame bounds.

## Identity and racing

- Use the same normalized resolved IP/port for candidates, races, backoff,
  strikes, and blacklists. Tracker peer IDs are only expected handshake values;
  their mismatch is not a peer-origin violation. Live-ID collisions retain the
  older connection and release its ID when it closes.
  Discard zones outside link-local IPv6. Resolve meaningful name/numeric scope
  aliases to one interface name using Go's name-first socket semantics; invalid
  interfaces are ordinary candidate failures.
- `CandidatePool` owns source-fair retention with numeric source keys.
  `EndpointBackoff` owns the 100,000 distinct attempted-endpoint budget shared
  across phases. Retrying known endpoints costs no new entry; candidate churn
  must not discard run-long blacklists or attempted-endpoint history.
- Race both transports to the same literal endpoint. Preserve the uTP head start
  even after early failure; a valid BEP 3 handshake wins. Cancel, close, and join
  the loser before evaluating capabilities. Never revive it to obtain an
  extension. See the [uTP dial-context lifetime rule](../utp/AGENTS.md).
- Distinguish severe peer-origin violations from ordinary disconnects, stalls,
  and compatibility failures; keep bounded well-framed unknown messages ignorable.

Use controlled clocks/dialers, independent wire fixtures, transition fuzzing,
and race tests for queues, races, live-ID release, and request obligations.
