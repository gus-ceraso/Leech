# Bencoding guidance

Read [DESIGN §7.2](../../DESIGN.md#72-strict-bencoding-and-info-hashes),
[§16](../../DESIGN.md#16-supported-bounds), and
[BEP 3](../../beps/bep_0003.rst).

- `Value.Raw`, byte strings, and dictionary keys alias the input buffer. Retain
  that buffer while using decoded values; do not mutate it underneath them.
- Hash the exact received `info` span, including unknown keys. Re-encoding a
  decoded dictionary is not a substitute.
- `Decode` requires exactly one complete value. Use `DecodePrefix` only for
  formats with a bencoded header followed by binary payload; its caller must
  validate the remaining payload independently.
- Preserve canonical rejection, including duplicate empty keys. Do not repair
  or byte-truncate malformed encodings to fit a bound.
- Wire-byte limits do not bound decoded memory. Keep value, dictionary-entry,
  container-entry, and depth limits independent. Smaller protocol profiles must
  supply suitable decoding limits rather than inheriting metainfo defaults.

Test literal canonical bytes and independently known hashes. Fuzz both full
values and prefix boundaries with malformed and near-limit cases.
