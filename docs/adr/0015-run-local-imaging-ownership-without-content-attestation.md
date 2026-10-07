# ADR-0015: Run-local imaging ownership without content attestation

Status: accepted
Authority: explicit owner direction, 2026-10-03
Truth class: normative
Supersedes: ADR-0013; content-attestation exemptions in ADR-0014

## Decision

Trusted imaging data within one process is associated by ownership, compiled
selection and shape, row/channel ordering, actual completion counts, and live
source-state/lifecycle checks. Do not hash visibilities, generated models,
reprojection targets, weighting arrays or replay contributions to mint or verify
within-run identities. A live owner/event token is not a content fingerprint and
must not be described as one. It is not a portable identity for reuse in another
run. Do not add a second pass over scientific arrays solely for authorization.

Private same-run spill is an extension of the bounded buffer system, not an
external trust boundary. Keep versioned framing, exact lengths and offsets,
frame ordering/counts, shape and inventory, source association, descriptor/file
identity, truncation/EOF checks and ordinary I/O error propagation. Remove payload
and header-transcript CRCs, their fields, verification passes, timers and resource
claims. No optional mode, renamed substitute or compatibility checksum path.
An equal-length payload modification is not guaranteed to be detected by a
checksum; scientific value checks still apply where data is consumed.

Opaque owned reconstruction preparations retain their real values and masks.
Binding checks source and geometry, numeric bounds and compiled requirements;
it does not attest projected samples, support or interpolation stencils.
Sparse model updates are validated when compiled/applied and associated with
their owner and exact base, without a second scan to hash their terms.

## Independent boundaries

CASA-compatible persisted formats and their required integrity semantics do not
change. Independently justified checksums for external formats or imported
artifacts remain. The aligned external seed mask has a declared expected mask
identity; compare it during the existing ingestion pass, not by rereading the
stored model. Diagnostic fingerprints in explicitly invoked tests or benchmarks
are allowed, but never impose work on normal production execution.

Scientific algorithms, flags/masks, WCS/beams, existing numerical acceptance,
bounded memory, I/O error propagation and individual-image atomic publication
remain required. No bitwise floating-output requirement may justify redundant
production buffers, passes or arithmetic beyond scientific acceptance.

## Historical requirements and regression

ADR-0013 is historical and non-normative. ADR-0014's statements retaining
separately justified private spill checksums are superseded only for trusted
same-run private spill; its product ownership/publication rules remain active.
Older bulk plans, receipts, timings and source-study records describe previous
implementations, not requirements to recreate content proof or CRC machinery.

Architecture checks must exercise the ownership-only APIs and reject hashing
capabilities in trusted transport/spill paths, including helpers under different
names. Tests retain count/order/source/lifecycle and real I/O-failure coverage,
plus unchanged scientific application comparisons. Banning the word "seal" is
not a sufficient regression check.

Reintroduction requires an explicit architectural decision and owner approval,
with a concrete failure model and measured CPU, memory, I/O and end-to-end cost.
