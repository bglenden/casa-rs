# Trusted in-process ownership and bounded operational telemetry

Status: Accepted by explicit user direction, 2026-09-18.

Trusted in-process scientific product generation must not hash product content
or reread complete arrays solely to authorize publication. Product attestation
does not provide an independently justified trust boundary between the generator
and its writer; it introduced initial hashes, repeated verification, private
backing arrays, copies and staged authority records on the same trusted path.
Remove that mechanism, not merely its cost: no optional mode, renamed substitute,
memoized seal or equivalent verification pass.

Generate bounded owned windows directly into the CASA-compatible writer's
private staging images. Retain scientific source/run association, exact member
inventory and shape, ordered complete window coverage, metadata/WCS/masks/beams,
fallible writes and flushes, resource admission, and atomic replacement of each
individual image. The user's subsequent explicit direction also removes
partial-publication recovery: no resumable member ledger, idempotent recovery
protocol or per-member partial/uncertain receipt choreography. If publication
fails, the run fails and its output set is incomplete; rerunning is required.
Previously replaced images are not rolled back, and the entire output set is
not an atomic transaction. Do not report such a failed set as a successful result.
Ownership and lifecycle checks concern metadata and completed operations, not
content attestation. Generation returns only useful scientific metadata, not an
additional readable full-product backing store.

## Superseded requirements

The same rule applies to internally owned reconstruction models, including
handoffs between execution attempts. Model identity tracks ownership, scientific
association and lifecycle, not content. Validate values and support where they
enter or change; a genuinely tighter scientific constraint may require its own
validation, but continuation, delta preparation and completion do not rehash or
reread unchanged models to reauthorize them.

Normal-state completion likewise uses the existing run, model, weighting, replay
and coverage associations. A fresh full-array digest with no independently
trusted expected value detects no corruption: it only gives corrupted bytes a
different identity. Remove these verification-only normal-state content passes
and redundant minor-cycle content IDs; retain explicit diagnostic fingerprints
and separately justified private storage checksums.

Routine node and fence progress stays lightweight in memory. It must not
serialize, hash, clone or rewrite the entire execution receipt, nor do work
proportional to an entire model, execution plan or historical receipt store.
Retain a useful final success/failure summary and only bounded intermediate
persistence with an actual storage/concurrency consumer. Current Resource
Authority admission is authoritative: historical failure receipts are not an
admission input, and planning does not enumerate or reread them. This supersedes
ADR-0010's historical-infeasibility feedback and progress-receipt requirements.

Compare already available metadata directly instead of serializing and hashing
it for equality. Resolve each distinct implementation once per meaningful
boundary. Reuse bounded project discovery inventories outside view rendering,
preserving refresh, exclusions and scan limits.

This decision supersedes the product-generation seal, content commitment,
completion authority and publication-projection requirements in the T13/T22
programme design, `migration-and-acceptance.md`,
`lessons-and-next-tranche.md`, and the product-sealing clauses of ADR-0011.
It also supersedes their partial-publication recovery requirements. It clarifies
the product contract in ADR-0009 and resource accounting in ADR-0010: neither
requires an attestation phase or its allocations, I/O, receipts or timing stages.
Historical delivery records remain evidence of
past work, not requirements to restore the deleted mechanism.

Scientific algorithms and acceptance, CASA-interoperable formats, observation
freshness, reconstruction invariants and genuine persistence/external-input
integrity checks are unchanged. ADR-0013's private replay-spill integrity is a
different persistence boundary and is not superseded. Diagnostic fingerprints
in tests and benchmarks remain allowed outside the production timed path.

## Regression rule

Reintroducing production product/model attestation or verification-only full-array
passes requires a new explicit architectural decision and user approval, with
a concrete failure model and measured CPU, memory, I/O and end-to-end cost.
Enforcement must test the write-only ownership path and actual work, as well as
dependency/interface constraints; a ban on the word “seal” is insufficient.
Exceptions to bounded operational telemetry likewise require a concrete failure
model and demonstrated benefit, not a renamed verification or receipt mechanism.
