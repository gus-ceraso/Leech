# Storage guidance

Read [DESIGN §§8–9](../../DESIGN.md#8-selection-and-storage-mapping),
[§13.1](../../DESIGN.md#131-cache-staging), and
[§15](../../DESIGN.md#15-concurrency-and-lifecycle).

- `Validate` constructs the read-only output plan; `Prepare` mutates selected
  output. Check the complete plan before destructive preparation. Resolve the
  destination root once, then refuse descendant symlinks and unsafe or colliding
  paths. Torrent-relative limits and filesystem representability are separate
  concerns: relative limits exclude `info.name`, while Linux also checks absolute
  syscall path length and component length. Hostile concurrent local races remain
  outside scope. `Plan.ReadAt` uses a per-plan `ReadAtOpener`; use
  `WithReadAtOpener` for focused read-spy tests, keeping production plans on the
  default `os.Open` path rather than adding global hooks.
- Selected mutations must not modify another pathname's inode. Detach hardlinked
  files before overwrite, verified writes, or resume truncation. Copy with one
  block of memory and an initial-size bound; preserve resume suffixes until
  verification permits truncation, and honor cancellation when context is available.
- Preparation and writes close each output before opening the next. `Prepared`
  retains no file descriptors; each operation reports its own close failures.
- Consume the immutable selection plan: selected regular-file intersections
  reach output, padding is synthetic, and unwanted real bytes stay in staging.
  Preparation creates selected zero-length files because no piece commit will.
- `storage.Overwrite` is the zero preparation mode. Preserve `storage.Resume`
  through a partial scan's handoff; see the
  [TransferConfig caveat](../session/AGENTS.md#ownership-and-handoffs).
  Scan one piece mapping at a time rather than cloning every descriptor/range.
  Skipped real bytes prevent reconstruction; pending overlong-file truncation
  waits until that file validates.
- `NewStager` is inert; the session calls `Start` only when transfer is needed.
  Reserve declared piece count and byte credits before creating stage files in
  the private `os.UserCacheDir()/leech` workspace. Cache failure has no memory
  fallback.
- `PieceStage.WriteBlock` retains no caller payload after it returns. Coverage
  offsets are piece-relative; torrent mappings use global half-open ranges.
  `NewPieceSnapshot` copies coverage/provenance for the serialized finalizer;
  the coordinator owns mutable completion and strike state.
- A successful commit requires SHA-1 verification, selected writes, and all
  output closes before stage removal. `OutputCommitted` still matters when
  removal then fails. Successful write/close is sufficient; do not add
  preallocation or `fsync` requirements.
- Propagate storage errors, including short writes, close, truncation, and
  removal. Join/close staging work before removing only this run's workspace;
  preserve the primary error and make cleanup failure replace success.

Use narrow failing-I/O seams and filesystem fixtures for selected/skipped
boundaries, padding, resume, cancellation, budgets, and commit/cleanup failures.
Assert both output bytes and the absence of unselected output.
