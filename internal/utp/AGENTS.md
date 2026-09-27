# uTP guidance

Read [DESIGN §14](../../DESIGN.md#14-utp),
[§16](../../DESIGN.md#16-supported-bounds), and
[BEP 29](../../beps/bep_0029.rst). This is an outgoing `net.Conn` transport.

- Keep packet/sequence, receive, send, and congestion state testable without
  sockets. `Conn` owns the connected UDP socket and applies their packet actions;
  peer code sees only the resulting reliable stream.
- STATE, including SYN-ACK, does not consume a sequence number. Initialize
  reception at its sequence N (initial ACK N−1); first peer DATA uses N, not
  N+1. BEP 29's setup diagram contradicts its ST_STATE text; follow the text
  and libutp. Fixtures must preserve this invariant. See [AUDIT.md](AUDIT.md)
  for pinned reference sources, regressions, licensing, and validation limits.
- `DialContext`'s context bounds setup. A successfully returned connection must
  survive cancellation of that context because the peer race cancels attempt
  contexts after choosing a winner. Failed/canceled setup closes and joins its
  workers; the caller owns the returned connection's lifetime.
- Use one connected UDP socket per connection. Preserve concurrent `net.Conn`
  read/write/deadline/close semantics and joined closure. Do not add a listener,
  unsolicited SYN acceptance, or server API.
- Filter unrelated connection IDs before parsing extension chains. Ordinary
  packets require recvID; a validated RESET may use recvID or sendID because
  a peer without connection state echoes the offending packet's ID. Validate
  unknown extension framing without retaining ignored headers or bodies.
  Check the current read deadline under `Conn.mu` before consuming buffered
  bytes; after any wake or timer, recheck current state under the mutex so a
  stale timer cannot override a cleared or extended deadline. Check the current
  write deadline before accepting each new payload prefix and recheck it after
  waking for the same reason.
- Sequence numbers count packets, not bytes. Use wrap-aware arithmetic and
  bounded ACK/SACK validation; release acknowledged bytes only once. Count
  only STATE for duplicate-ACK loss detection; incoming DATA's unchanged ACK
  is not loss evidence, though its SACK remains meaningful. Preserve bytes
  before FIN and settle FIN gaps before reporting EOF.
- Bound packet counts and buffered bytes independently. Receive actions already
  contain the ACK to send; do not rebuild it in the socket adapter. Reuse the
  selective-ACK mask while the receive window is unchanged.
- Keep BEP 29 delay control, RTT/RTO saturation, retransmission, and bounded
  recovery from zero congestion/receive windows, even with no unacknowledged
  packet. Retry timers restart on ACK progress, a new flight, or timeout—not
  unrelated packets or later sends. First SACK progress resets backoff; repeated
  SACK does not. Zero delay feedback means no sample, not a baseline of zero;
  congestion credit comes only from newly ACKed payload bytes. Retries belong
  to the owned connection timer path.

Use independent packet vectors and the existing deterministic link for loss,
delay, duplication, reordering, SACKs, window pressure, timeout, wraparound, and
teardown. Real loopback IPv4/IPv6 covers socket/address/deadline behavior. Two
copies of Leech communicating are not sufficient evidence; assert expected
bytes and exact retransmission events, and race-test concurrent I/O and close.
