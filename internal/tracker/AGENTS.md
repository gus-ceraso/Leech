# Tracker guidance

Read [DESIGN §10](../../DESIGN.md#10-tracker-subsystem),
[§16](../../DESIGN.md#16-supported-bounds), and BEPs
[3](../../beps/bep_0003.rst), [7](../../beps/bep_0007.rst),
[15](../../beps/bep_0015.rst), [23](../../beps/bep_0023.rst),
[31](../../beps/bep_0031.rst), and [41](../../beps/bep_0041.rst).

## Transactions

- Sanitize initial and redirect URLs alike, preserving unrelated query data.
  A delimiter-heavy query must not create one allocation per delimiter or
  silently narrow the supported URL input contract.
- Track complete-request transmission separately from response success. Loop
  updates retain attempted/transmitted/activated separately, include final-event
  attempts, and carry compact IPv4/IPv6 counts from response decoding (never
  infer compact provenance from hostname resolution). Preserve permanent HTTP
  client-error classification through body failures
  unless a parsed applicable retry hint changes it.
- Use tracker-specific bencode structure limits. One response owns one endpoint
  deduplication set across dictionary, IPv4, and IPv6 peers, retaining first
  endpoint order. Validate the entire compact stride before dropping records.
- UDP transaction retries follow BEP 15 independently of tracker-loop backoff;
  refresh expired connection IDs before announce retransmission. Family
  transactions are independent and both must join on cancellation. Retained UDP
  endpoint sessions are capped at `2 * limits.Trackers`; a session's user count
  includes callers waiting for its per-endpoint transaction lock. Retire only
  unused sessions, closing their socket before freeing capacity. Capacity waiters
  are cancellable and wake when the client closes.
- Tracker destinations may resolve to any address, including private/loopback.
  Returned peer endpoints use the stricter [peer boundary](../peer/AGENTS.md).

## Lifecycle and accounting

- Each unique tracker owns independent state. Reuse the run identity across
  trackers, phases, and families; never probe or reserve the announced port.
- Keep started-transmitted, active, and permanently disabled states separate.
  Never shorten not-before times or conflate HTTP depletion rerequests with UDP
  interval rules. A tracker-local failure cannot suppress other trackers.
- `uploaded` is always zero. Premetadata `left=1` is an approved nonstandard
  sentinel; later `left` is whole-torrent retained-byte accounting, excluding
  synthetic padding. Received payload includes duplicates/corruption in
  `downloaded`; metadata and transport overhead do not.
- Join regular loops before one-shot final events. A transmitted `started` can
  require `stopped` without any successful response. Successful full transfer,
  including cache cleanup, sends `completed` then `stopped`. A cache cleanup
  failure suppresses `completed`, even after all output verifies, but not an
  otherwise applicable `stopped`. Partial selection never sends `completed`.
  Reserve a bounded opportunity for `stopped` if `completed` stalls. No regular
  announce may follow the final-event sequence.

Use captured local traces and controlled clocks for retries, response loss,
unequal families, failure classification, and shutdown ordering. Test both
first-value and last-value query parsers after sanitization, plus race tests
around loop termination and final operations.
