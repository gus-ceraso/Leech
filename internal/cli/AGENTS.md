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
- Treat wrapped errors as untrusted display input too. Redaction scans complete
  URL tokens before truncation, including 64 MiB supported URLs, userinfo, IPv6
  brackets, apostrophes, punctuation and embedded controls, without exposing a
  suffix.
  The scanner retains at most 4096 raw bytes and scans at most 64 MiB plus that
  prefix; URL parsing copies only the bounded authority. Never echo a complete
  magnet URI.
- Preserve the distinction between primary failure and secondary diagnostics,
  and between completed selection, completed torrent, already-valid resume, and
  retained verified partial output. `tracker.FinalAnnounceError` is a separate
  best-effort category: retain its count, not its joined error text, and render a
  `warning: nonfatal:` line. Other secondary cleanup errors remain error-level;
  callbacks still receive the original wrapped causes.
- Session diagnostics have fixed shape
  `Diagnostic{At time.Time, Kind DiagnosticKind, Phase string,
  Endpoint DiagnosticEndpoint, Peer peer.Endpoint,
  Count, IPv4Count, IPv6Count uint64, Duration, RetryAfter time.Duration,
  Race peer.RaceObservation, Detail string}`;
  `Peer` is `netip.Addr` plus `uint16` port and zero when absent or zoned. The
  CLI-owned nonblocking debug queue holds 128 records. Sanitize and truncate
  Phase, Detail, and tracker scheme/host before enqueueing. Each record retains
  at most 4096 aggregate text bytes plus typed timing/race fields. Race outcomes,
  stages, and reasons are closed numeric enums, never raw error strings.
  Permanent lines add a fixed UTC millisecond timestamp and level prefix outside
  the message bound; queued events keep observation time, not render time.
  Concurrent queues and deferred secondaries can print out of timestamp order.
  Valid zone-free numeric peer endpoints render in at most 47 bytes; invalid or
  absent endpoints are omitted. Debug and phase/warning queues summarize drops
  separately; status snapshots coalesce without incrementing drop counts. Recoverable
  tracker failure/recovery warnings use `OnWarning`; bounded attempt detail and
  other observations render at debug. One CLI-owned reporting worker serializes
  phase lines, warnings, diagnostics, and status output. Its second nonblocking
  queue holds 256 phase/warning/status records, with at most 4096 text bytes per
  permanent line; live status snapshots coalesce while phase-entry snapshots
  remain ordered. Queue overflow is summarized at
  join. Four secondary errors, each at most 4096 sanitized bytes, are retained
  separately so neither primary nor secondary failure reporting is dropped.
  Enqueue debug records only when debug is enabled, but always preserve caller
  callbacks. Join the worker before final summaries and result. TTY status is
  throttled to one second; redirected transfer progress to 30 seconds, both at
  info/debug. Metadata/resume have no redirected periodic progress. Tracker
  identifiers retain scheme and host only. Summary counters come from
  `RunResult.Summary`, independently of CLI queue drops; useful connections are
  not unique endpoints, and payload rate includes discarded bytes.
  Known reporting gap: `session.Run` waits for final tracker announcements after
  transfer workers join. During that wait, the reporter can render its frozen
  final transfer snapshot with stale peer/rate values and a fresh timestamp;
  stopping the reporter only when `Run` returns does not prevent this.
- Keep phase/progress reporting testable with controlled time and TTY state.
  Queue-pressure tests should use local tracker events and gate peer messages
  with request, processed-event, and release barriers; synthetic peer-message
  churn can starve transfer scheduling under `-race`.
  Phase-entry status uses `OnPhaseStatus`; `OnProgress` remains commit-only.
  Live transfer `OnStatus` refreshes the same display without affecting the
  session no-progress timer. Preserve caller callbacks and install CLI activity
  observation only at info/debug (TTY or redirected). Caller-supplied callbacks
  run synchronously before CLI enqueueing and must return promptly. CLI-owned
  session wrappers only update bounded queue/snapshot state; they never write
  stderr. Permanent lines clear the displayed status without resetting its
  one-second throttle. The reporting worker preserves phase/warning ordering,
  retries the latest deferred snapshot on its ticker or final flush, and joins
  before final output. The consumer never sends back into its own report queue:
  session producers may finish and close it while an older snapshot is rendering.
  Isolate platform terminal checks in `signals_terminal_*`.
- Keep the signal owner live during graceful cleanup so a second signal can
  exit immediately. Reusable session code must not call `os.Exit`.

`RunWithSession` is the local integration seam for tracker, resolver, and
transport dependencies. Use golden output checks and injected failing writers;
run signal tests in helper processes so they cannot exit the test runner.
