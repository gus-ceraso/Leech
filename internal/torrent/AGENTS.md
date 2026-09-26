# Torrent guidance

Read [DESIGN §§7–9](../../DESIGN.md#7-input-and-metadata) and BEPs
[3](../../beps/bep_0003.rst), [47](../../beps/bep_0047.rst),
[52](../../beps/bep_0052.rst), and [53](../../beps/bep_0053.rst).

- Source parsing/normalization has no output/cache mutations. Keep hostname
  resolution separate. Reuse tracker normalization for metainfo and magnets,
  preserving mandatory default-tracker inclusion and flattened unique URLs.
- Metainfo tables are immutable after validation. `File.Index` retains original
  list positions, including padding and symlinks; ranges are half-open in the
  concatenated v1 byte space. File identity and output-path presence are separate,
  so pathless padding must not shift BEP 53 indices.
  Union BEP 53 ranges in work bounded by range and file counts, preserving the
  caller's ranges and inclusive original indices.
- Use the [bencode exact-span contract](../bencode/AGENTS.md) for info hashing.
  Unknown keys stay in the hash even when ignored semantically. Reject hybrid
  input rather than accepting its v1 portion.
- Normalize allowed omissions/attributes before selection. `info.name` supplies
  the safe conventional output name; `dn` is display-only. Retain `private=1`
  for the warning while applying the approved public-torrent behavior.
- Give storage and scheduling one immutable selection plan, including full
  verification ranges across skipped files. Consumers must not reinterpret
  globs or paths. Keep original indexing distinct from output selection.
- Differential-test accelerated selection against Go's `path.Match`: a bracket
  class can consume `/`; `*` and `?` cannot. Include negation, descending ranges,
  escapes, malformed classes, and Unicode. Interpret escapes and classes when
  checking separators and `**`; escaped selectors still use the accelerated
  matcher. Preserve the supported domain while bounding work across files and
  patterns.

Use independent literal metainfo/hash vectors and range/index properties, not
only Leech-encoded values. Fuzz sources, metadata, and selection with useful
valid seeds and near-limit malformed cases. Fetched-info fuzzing needs a matching
expected hash to reach normalization; test hash mismatches separately.
