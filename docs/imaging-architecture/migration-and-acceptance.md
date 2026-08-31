# Imaging migration and acceptance matrix

Truth class: normative
Last reality check: 2026-08-22
Verification: `just arch-check`

The authoritative machine-readable inventory is
[`resources/imaging-architecture/migration-matrix.json`](https://github.com/bglenden/casa-rs/blob/main/resources/imaging-architecture/migration-matrix.json).
It is owned by GitHub issue #487 under the #486 imaging-architecture programme.

The first-tranche delivery findings and corrected T13-T23 order are normative
in [the lessons and next-tranche contract](lessons-and-next-tranche.md). They
refine dependency and ownership sequencing without changing this matrix's role
as the sole capability and product inventory.

## What one row means

Every current imaging capability, product, solver, frontend, and backend has
exactly one stable row. A row records:

- its sole current production owner;
- `Native` or `TemporarilyUnavailable` disposition;
- exact evidence issues and baseline-manifest locators;
- one versioned Acceptance Contract;
- its destination ticket and transfer point;
- the deletion condition for the displaced implementation; and
- a Migration Obligation whenever it is not native.

The matrix binds seventeen current Rust request, science, solver, product, and
backend enums to rows, including `ProductKind`, both spectral-mode and
deconvolver surfaces, `GridderRequest`, both cube-interpolation surfaces,
`PolarizationCoordinate`, `StandardMfsBackend`, both FFT-backend surfaces, and
the standard-MFS acceleration policy. The polarization map includes the XY,
YX, RL, and LR cross-hands even while their execution routes are temporarily
unavailable; backend maps keep automatic policy, fixed-tile, and each Metal
family distinct. Additions, removals, and aliases therefore cannot silently
redefine an accepted production surface.

Every baseline locator resolves to repository content and is paired with a
pinned SHA-256 digest in `baseline_manifest_digests`. Mutable issue URLs are
not baseline evidence; issue-backed rows use exact committed snapshots with
the issue title, body, URL, and source update timestamp. Baseline and contract
revisions are mechanical integrity updates reviewed with the owning change.

The migration matrix is a repository planning and acceptance record. Production
code neither embeds nor interprets it. `casa-imaging-application` compiles each
`ImagingRequest`, checks the resulting immutable `CompiledProblem` and task-only
requirements against the implementation installed in the build, and either
invokes that sole implementation or returns a typed unavailable result before
planning. Compile, admission, planning, and execution failures are terminal;
there is no differential, fallback, or stage-mixing entry point.

## Acceptance contracts

The matrix defines reusable contracts for scientific products, exact routing,
solver trajectories, cross-surface request round trips, the immutable
Compiled Problem foundation, and both the Resource Authority foundation and
its eventual production integration. Scientific product contracts use a
normalized-RMS ceiling of `0.001` only on declared valid support and only with
a declared denominator.

Product generation is a closed two-phase authority. Planning binds the Product
Graph's exact typed source commitments into an opaque planned generation.
Publication requires a seal minted only after owner-generated completion
evidence matches every commitment and cross-generation dependency. Raw digest
binding, caller-authored staging maps, dynamic source registries, and
backend/resource pseudo-sources are not authority seams. T13 lands topology,
layout, and store structure; T22 performs the first real continuum catalog and
planned-generation/seal cutover after T17-T21 and the model lifecycle can
supply their typed evidence. A later product source role extends that same
closed catalog through its scientific owner's typed commitment/completion,
identity and receipt ratchets, matrix ownership, and acceptance evidence; it
does not add another public authority method.
That scalar ceiling supplements rather than replaces exact topology, WCS,
units, beam, sum-of-weights, flux, centroid, operator-law, and resource gates.

Exact CLEAN component order is diagnostic rather than normative. The optional
trajectory record may retain the first component-selection divergence, Major
Cycle, location, scale, term, polarization, residual peak, threshold, and flux.

## Transfer ratchet

A row transfers to `Native` only when the same change:

1. passes its referenced Acceptance Contract;
2. updates the row, evidence, and contract revision;
3. deletes the displaced production route and moves any still-useful algorithm
   directly to its authoritative owner; and
4. leaves no runtime fallback or dual patch owner.

Before transfer, corrective and performance changes land only in the row's
current owner. After transfer, they land only in the native owner.

T14/#500 is a boundary checkpoint, not a product-row transfer. The compiled
measurement equation ends at an explicitly unnormalized normal state, while
normalization, residual scaling, restoration, PB correction, blanking, and
unit conversion are typed as Product Contract operations. `product.image` is
native. Standard-gridder MT-MFS `product.image-pbcor` transfers in T44/#530 via
the product-owned weight-derived PB tt0; broader response and mosaic PB
correction remain with T47/#533. T13/#499 lands the Product Graph and atomic
store, T22/#508 owns the first continuum Product Generation Authority and
sealed publication, T39/#525 owns common-beam restoration, and T47/#533 owns
broader PB/sensitivity and mosaic normalization, including the still-unavailable
`product.alpha-pbcor`. Displaced writers are removed only under the transfer
ratchet above.

The next tranche corrects one dependency discovered during composition: model
generation is an input to Major-Cycle reconciliation, not its output owner.
T28/#514 therefore lands solver-independent model ingest, reprojection,
generation, and Model Delta application after T15 and before T20. T17 and T28
may run in parallel; T20 joins T19 complete-data evidence with the T28 model
lifecycle. T22 then consumes the typed weighting, normal-state, and model
completions. No earlier ticket may invent those future records or expose a raw
construction path to avoid the ordering.

The architecture checker validates schema, the readable logical dependency
graph, complete Acceptance Contracts, complete row ledger, structured issue
outcomes, content-pinned evidence locators, Migration Obligations, and source
evidence. It classifies every Cargo workspace package, requires native
dependency sets to match exactly, rejects runtime migration-matrix references,
scans native and frontend roots for forbidden execution/device imports, and
keeps the T15 walking skeleton private without redirected source. Focused
mutation tests prove these structural checks and the scientific error ceiling
fail closed; they do not freeze exact test contents or deleted symbol inventories.

`capability.compiled-problem` is Native only for its backend-independent
logical contract and stable identity; later observation and geometry tickets
own concrete manifests. Likewise `backend.resource-authority-contract` is the
Native topology/admission/lease foundation, while
`backend.process-resource-authority` remains `TemporarilyUnavailable` until
production schedulers acquire its leases and retain prediction-versus-actual
execution receipts.

## Preserved issue and evidence crosswalk

The required crosswalk set and a structured per-issue outcome ledger are
encoded in the matrix and pinned by the checker. They cannot disappear through
a coordinated policy/matrix edit without failing `just arch-check`: #35, #40,
#42, #45, #52, #54, #55, #217, #319, #445-#450, #462, #466, #473, and #478.

- #319 is the closed July 2026 consolidation baseline. Its bounded streaming,
  request boundary, fixed-tile/Metal staging, evidence, and deletion ledger
  remain valid; the #486 programme does not reopen it.
- #35, #42, #45, #54, and #55 preserve cube-cycle, cubic sampling, global
  weighting, selection, and moving-source evidence.
- #217, #466, #473, and #478 preserve product masks, bounded cube routing,
  reopenable products/beams, and multifield W-cube behavior.
- #40, #52, and #462 preserve the distinction between W and genuine AW
  projection, typed convolution-function identity, cache interoperability, and
  the deliberately deferred distributed-execution non-goal.
- #445-#450 preserve the full VLASS correctness, bounded-input, acceleration,
  resource, and cross-surface commitments. Their durable checked-in locator is
  `tools/perf/imager/evidence/imaging_performance_evidence_manifest.json`; the
  full-data receipts named in `TESTING.md` remain milestone evidence.

The matrix row is the binding locator. This document is the human orientation
and must not be maintained as a second inventory.
