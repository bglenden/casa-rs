# ADR-0013: Non-cryptographic integrity for private run-scoped spill artifacts

Status: accepted
Date: 2026-09-10
Truth class: normative
Supersedes:
Superseded by:

## Context

The managed-spill artifact is a private, run-scoped, deletion-owning temporary
file: one process creates it during the weighting pass, reads it for later
major cycles, and unlinks it when the final artifact owner drops. It is never
persisted, never shared between processes, and never read by a different
build or version.

The version-2 format bound every frame payload with SHA-256 and maintained a
SHA-256 transcript over the file and frame headers. The compiler additionally
SHA-256-hashed each encoded frame for the program descriptor binding, so the
same payload bytes were hashed twice on the write path and re-hashed twice per
reader session on the read path. A retained profile of the bounded serial
fixture attributed roughly 12% of task time to SHA-256, including that
duplicated write-side work.

The threat model for this artifact is accidental corruption and implementation
error, not an adversary. Versioned framing already provides magic, version,
and declared-length checks; the reader enforces monotonic sequence, record and
payload counts, bounded offsets, exact truncation and trailing-byte rejection,
byte-range reads that cannot cross the footer, sealed device and inode
identity, and a poisoned reader that fails the run on any mismatch. The
in-memory seal is compared against the stored footer on every session. The
only interoperability contract in this area is persisted CASA-compatible data
and provider-contract bundles, none of which the spill participates in.

ADR-0008 already refuses unnecessary content digests on a persistent path, and
ADR-0007 refuses hashing executable identity in receipts. The same reasoning
applies more strongly to a private temporary artifact.

## Decision

Private run-scoped spill artifacts must not use cryptographic digests.
Integrity uses versioned framing plus a 32-bit CRC32C checksum:

- the file, frame, and footer headers carry the format version and declared
  byte lengths; reserved header bytes must be zero;
- each frame header stores a `payload_crc32c` computed by the compiler for the
  frame it emits; the writer stores it without re-hashing, the reader verifies
  the payload against it before publishing a window, and the replay descriptor
  binds to the reader-verified value without another hash;
- a CRC32C transcript over the file header and every frame header is sealed
  with the artifact and stored in the footer, so header, order, and count
  tampering is detected against the original in-memory seal;
- all existing structural, identity, truncation, and poisoned-reader checks
  remain.

`ManagedSpillSeal` exposes a `global_crc32c` checksum; measurement accounting
uses `checksum_bytes` and `checksum_calls`. The format version advances to 3.
Version-2 artifacts are never read by version 3 and require no migration
because the artifact is private and run-scoped. This decision applies only to
private run-scoped artifacts; persisted CASA-interoperable data, provider
schema bundles, and prepared-artifact cache identities keep their existing
integrity requirements.

## Consequences

Positive:
- removes the per-frame cryptographic payload hashing from both the write and
  read paths, the duplicated compiler hash, and most of the SHA-256 time in the
  bounded serial profile;
- keeps fail-closed detection of accidental payload, framing, ordering, count,
  and truncation errors, and keeps the descriptor binding at checksum cost;
- removes cryptographic machinery from a format that has no adversary model.

Negative:
- payload corruption detection is a 32-bit checksum: adequate for accidental
  corruption, not collision-resistant against an intentional attacker;
- historical version-2 artifacts are unreadable, which is acceptable for a
  private run-scoped file and must never become a persisted cache contract;
- dataset identity values in the ignored serial-compute probes are recorded as
  JSON evidence rather than asserted, so accepted science and accounting
  evolution cannot turn an unmaintained golden into a false regression; the
  probes keep only structural invariants and stable selection-derived totals.

Neutral / tradeoffs:
- the `F_NOCACHE` bounded page-cache policy, frame sizes, and buffer
  admission are unchanged;
- measurement field names change from `sha256_*` to `checksum_*`.

## Alternatives considered

1. Keep per-frame SHA-256 (status quo). Rejected: disproportionate for a
   private run-scoped file and a measured multi-percent cost, including a
   duplicated write-side hash of identical bytes.
2. Format version only, with no payload checksum. Rejected: the artifact does a
   cache-bypassing disk round trip, so a cheap payload checksum retains
   accidental-corruption detection for negligible cost.
3. One cryptographic digest per artifact instead of per frame. Rejected: the
   read path still hashes all payload bytes, so it removes no read cost and
   keeps cryptographic machinery.
4. A non-CRC fast hash (xxHash and similar). Rejected: comparable cost, but
   CRC32C has hardware acceleration on both supported architectures and is a
   conventional checksum for storage framing.

## Enforcement

This decision is enforced by:
- tests: managed-spill unit tests for version-3 framing, CRC32C corruption
  rejection, version rejection, transcript/seal binding, reserved-byte
  validation, and poisoned-reader behavior; reconstruction descriptor-binding
  and encoded-record validation tests; the serial-compute probes record their
  version-3 checksum scheme, accounting formulas, and dataset identities as
  evidence while asserting structural invariants (the probes require the
  mounted VLA medium dataset);
- lint/import/dependency rules: dependency review keeps checksum crates
  non-cryptographic and direct dependencies explicit;
- CI checks: `just verify` runs the affected `casa-imaging-runtime` and
  `casa-imaging-reconstruction` suites;
- review trigger: any proposal to add a cryptographic digest to the private
  spill, to persist or reuse a spill artifact across process runs or versions,
  or to make the spill a public cache contract must return to this decision;
- guidance: `docs/imaging-architecture/bounded-streaming-performance-spec.md`.

## Drift detection

Suspect drift if:
- a cryptographic digest type or crate appears in `managed_spill.rs` or its
  frame/footer layout;
- the frame header grows a digest field again or a v2 reader reappears;
- spill artifacts are read across process runs, versions, or machines;
- measurement fields regress to `sha256_*`;
- documentation describes the private spill as tamper-proof or
  collision-resistant.
