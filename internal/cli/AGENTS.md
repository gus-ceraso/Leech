# CLI guidance

Read [DESIGN §4](../../DESIGN.md#4-command-line-interface) and
[§15](../../DESIGN.md#15-concurrency-and-lifecycle). `cmd/leech` owns process
exit; this package adapts arguments, reporting, and signals to `session.Run`.

- Parsing has no network or output/cache mutations. Preserve source precedence
  and explicit-option tracking: a list-mode conflict remains an error even when
  the supplied option equals its default.
- Construct parser usage errors through `usageError`, which bounds and sanitizes
  their text before the process adapter prints it.
- Listing ends after metadata validation and does not validate a destination or
  prepare output. Keep its JSON on stdout and diagnostics/status on stderr;
  help and usage follow their separate DESIGN rules.
- Treat wrapped errors as untrusted display input too. Redaction must consume
  complete credential-bearing URLs, including IPv6 brackets and legal URL
  punctuation, without exposing a suffix. Never echo a complete magnet URI.
- Preserve the distinction between primary failure and secondary diagnostics,
  and between completed selection, completed torrent, already-valid resume, and
  retained verified partial output.
- Session diagnostics have fixed shape
  `Diagnostic{Kind DiagnosticKind, Phase string, Endpoint DiagnosticEndpoint,
  Peer peer.Endpoint, Count, IPv4Count, IPv6Count uint64,
  Duration time.Duration, Detail string}`;
  `Peer` is `netip.Addr` plus `uint16` port and zero when absent or zoned. The
  CLI-owned nonblocking queue holds 128 records. Sanitize and truncate Phase,
  Detail, and tracker scheme/host before enqueueing. Each record retains at most
  4096 aggregate text bytes plus fixed fields. Valid zone-free numeric peer
  endpoints render in at most 47 bytes; invalid or absent endpoints are omitted.
  Drop accounting saturates and applies only to debug records. Recoverable
  tracker failure/recovery warnings use `OnWarning`; bounded attempt detail and
  other observations render at debug. Consume
  diagnostics even on noninteractive stderr and join the consumer before final
  output; status remains
  separately TTY/level gated. Tracker identifiers retain scheme and host only.
- Keep phase/progress reporting testable with controlled time and TTY state.
  Phase-entry status uses `OnPhaseStatus`; `OnProgress` remains commit-only.
  Live transfer `OnStatus` refreshes the same display without affecting the
  session no-progress timer. Preserve caller callbacks and install CLI activity
  observation only when status is enabled. Permanent lines clear the displayed
  status without resetting its one-second throttle. One CLI-owned worker retries
  the latest deferred snapshot and joins
  before final output, so a stalled phase still gets status after a fast change.
  Isolate platform terminal checks in `signals_terminal_*`.
- Keep the signal owner live during graceful cleanup so a second signal can
  exit immediately. Reusable session code must not call `os.Exit`.

`RunWithSession` is the local integration seam for tracker, resolver, and
transport dependencies. Use golden output checks and injected failing writers;
run signal tests in helper processes so they cannot exit the test runner.
