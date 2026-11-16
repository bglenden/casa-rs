# ADR-0014: Identity is ownership, without content attestation

Status: accepted
Date: 2026-09-18
Truth class: normative
Supersedes:
Superseded by:

Amended: 2026-10-03, on owner direction: private same-run spill keeps no
checksums, and ownership-only association extends to imaging inputs, replay
and reprojection targets (this replaced the spill exemption of 2026-09-18).
2026-10-09, on owner direction: nothing of casa-rs's is stored in the
MeasurementSet (ADR-0008). 2026-11-02, on owner approval: the 2026-10-03
decision, recorded separately until then, is merged here.

## Context

Imaging once hashed product content and reread complete arrays to authorize
publication, kept SHA-256 or CRC framing on private same-run spill, and
minted content identities for models, plans and normal state. None of it had
an independent trust boundary: the generator and its writer, and a spill
file's writer and reader, are the same trusted process. A fresh digest with
no independently trusted expected value detects no corruption; it only gives
corrupted bytes a different identity. The mechanism cost initial hashes,
repeated verification, private backing arrays, copies and staged records on
the timed path.

## Decision

Identity is ownership.

1. **No content attestation.** Production code computes no SHA-256, CRC or
   other digest over scientific arrays (visibilities, models, weights,
   reprojection targets, replay contributions), compiled problems, plans,
   run summaries or spill to mint, verify or authorize an identity, and
   makes no verification-only pass over full arrays. There is no optional
   mode, renamed substitute, memoized seal or compatibility checksum.
   Already available metadata is compared directly, not serialized and
   hashed.
2. **Ownership.** Trusted data within one process is associated by
   ownership, compiled selection and shape, row and channel order, actual
   completion counts and live lifecycle checks. A live owner token is not a
   content fingerprint and is not a portable identity for another run.
   Values and support are validated where they enter or change; continuing,
   preparing a delta or completing does not rehash or reread an unchanged
   model.
3. **Nothing in the MeasurementSet.** Holding the open MeasurementSet is the
   ownership. casa-rs stores no keywords, generations, identities or marker
   files in it (ADR-0008).
4. **Run-local spill.** Private same-run spill is part of the bounded buffer
   system, not a trust boundary. It keeps versioned framing, exact lengths
   and offsets, frame order and counts, shape and inventory, source
   association, file identity, truncation and EOF checks and ordinary I/O
   error propagation, and no payload or header checksums.
5. **Products.** Generation writes bounded owned windows directly into the
   CASA-compatible writer's private staging images. It keeps the member
   inventory and shape, ordered complete window coverage, metadata, WCS,
   masks and beams, fallible writes and flushes, resource admission and the
   atomic replacement of each image. If publication fails the run fails and
   its output set is incomplete; rerunning is required. There is no
   resumable member ledger, partial-publication recovery or rollback, the
   set is not an atomic transaction, and a failed set is never reported as
   a success.
6. **Bounded telemetry.** Progress stays in memory and is O(1) per phase;
   nothing serializes, hashes or walks whole plans, models or history. A run
   leaves one final success or failure summary.

Unchanged: CASA-compatible formats and the integrity their formats define;
checksums that external formats or imported artifacts carry; scientific
algorithms, flags, masks, WCS, beams and numerical acceptance; bounded
memory. Diagnostic fingerprints are allowed in explicitly invoked tests and
benchmarks and never add work to production execution.

## Consequences

Positive:
- no hashing, rereads, backing copies or verification phases on the timed
  path
- one notion of identity across products, models, plans and spill

Negative:
- an equal-length corruption of private spill is not detected by a
  checksum; scientific value checks still apply where data is consumed
- a failed publication leaves an incomplete output set to regenerate

Neutral / tradeoffs:
- no bitwise floating-point output requirement may justify extra production
  buffers, passes or arithmetic beyond scientific acceptance

## Alternatives considered

1. Keep product attestation as an optional mode. Rejected: it keeps the
   mechanism and its cost under another name.
2. Keep CRCs on private spill. Rejected: the writer and reader are one
   trusted process, and framing checks catch truncation and misordering.
3. Make the output set an atomic transaction with member recovery. Rejected
   on owner direction: rerunning the task is the recovery.

## Enforcement

This decision is enforced by:
- tests: product publication tests of write-only bounded ownership, complete
  ordered coverage, unchanged science and metadata and real I/O failures
  (`TESTING.md`); count, order, source and lifecycle checks on owned data
- lint/import/dependency rules: `scripts/check-imaging-dependencies.py`
  refuses content hashing in the imaging and simulation crates
  (`no-content-hashing-in-imaging-crates`); imaging code rule 4 in
  `ARCHITECTURE.md`
- CI checks: `just arch-check`
- review trigger: reintroducing production attestation, a verification-only
  full-array pass or a telemetry exception needs a new ADR and the owner's
  approval, with a concrete failure model and measured CPU, memory, I/O and
  end-to-end cost; a ban on the word "seal" is not a sufficient check
- none / guidance only:

## Drift detection

Suspect drift if:
- a hash, CRC or digest is computed over scientific data, a plan or spill in
  production code, under any name
- a phase rereads a complete product or model only to authorize it
- progress reporting serializes or scans whole plans, models or history
- casa-rs writes anything into a MeasurementSet that CASA would not
- a failed publication is reported as a success or partly recovered
