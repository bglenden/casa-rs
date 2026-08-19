# Imaging migration and acceptance matrix

Truth class: normative
Last reality check: 2026-08-19
Verification: `just arch-check`

The authoritative machine-readable inventory is
[`resources/imaging-architecture/migration-matrix.json`](../../resources/imaging-architecture/migration-matrix.json).
It is owned by GitHub issue #487 under the #486 imaging-architecture programme.

## What one row means

Every current imaging capability, product, solver, frontend, and backend has
exactly one stable row. A row records:

- its sole current production owner;
- `Native`, `LegacyWholeRun`, or `TemporarilyUnavailable` disposition;
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
the issue title, body, URL, and source update timestamp. A baseline replacement
therefore requires an explicit matrix revision and accepted digest update.

`LegacyWholeRun` is an ownership classification, not a compatibility promise.
It permits the current production implementation to remain reachable only
through `casa-imaging-router` once a frontend is migrated to the router. It
never permits a native stage to call into legacy code or a failed native run to
retry through legacy.

`casa-imaging-router` compiles each `ImagingRequest`, derives the required rows
from the resulting immutable `CompiledProblem`, and records the matrix schema,
contract revision, disposition, and complete evidence for each row. It then
invokes exactly one sealed whole-run engine port. Enum-to-row bindings come
directly from the matrix inventories already checked against their Rust enums;
the router does not maintain a second binding table. `TemporarilyUnavailable`
invokes neither port, and compile, matrix, native compile/plan/run, or selected
legacy failures are terminal. Production code has no differential or
stage-mixing entry point; test-only fake ports exercise both owners separately.

## Acceptance contracts

The matrix defines reusable contracts for scientific products, exact routing,
solver trajectories, cross-surface request round trips, the immutable
Compiled Problem foundation, and both the Resource Authority foundation and
its eventual production integration. Scientific product contracts use a
normalized-RMS ceiling of `0.001` only on declared valid support and only with
a declared denominator.
That scalar ceiling supplements rather than replaces exact topology, WCS,
units, beam, sum-of-weights, flux, centroid, operator-law, and resource gates.

Exact CLEAN component order is diagnostic rather than normative. The optional
trajectory record may retain the first component-selection divergence, Major
Cycle, location, scale, term, polarization, residual peak, threshold, and flux.

## Transfer ratchet

A row transfers to `Native` only when the same change:

1. passes its referenced Acceptance Contract;
2. updates the row, evidence, and contract revision;
3. makes the displaced production route unreachable, or quarantines a helper
   still required by another legacy row behind the legacy boundary; and
4. leaves no runtime fallback or dual patch owner.

Before transfer, corrective and performance changes land only in the row's
current owner. After transfer, they land only in the native owner.

T14/#500 is a boundary checkpoint, not a product-row transfer. The compiled
measurement equation ends at an explicitly unnormalized normal state, while
normalization, residual scaling, restoration, PB correction, blanking, and
unit conversion are typed as Product Contract operations. `product.image` and
`product.image-pbcor` therefore remain `LegacyWholeRun`: T13/#499 must land the
Product Graph and atomic store, T39/#525 owns common-beam restoration, T47/#533
owns PB/sensitivity and mosaic normalization algorithms, and T43/#529 owns
wideband `product.alpha-pbcor` behavior. Their legacy writers are removed only
under the transfer ratchet above.

The architecture checker validates schema, the independently pinned logical
graph, canonical inventories, complete Acceptance Contracts, complete binding
row ledger, structured issue outcomes, content-pinned evidence locators,
Migration Obligations, and source evidence. It binds the seventeen variant maps
to their Rust enums, classifies every Cargo workspace package, requires native
dependency sets to match exactly, pins `casa-imaging-router` as the sole owner
of `ImagingRouter` and both whole-run engine ports, requires its direct matrix
embedding, scans native science/runtime/router and Rust/Swift frontend roots for
forbidden legacy/backend/device imports, ratchets the existing legacy
Rust-frontend violations while rejecting additions, and permits only the 16
exact frozen legacy edges plus three exact pre-existing transitional surface
edges. Its mutation tests prove coordinated inventory and issue deletion, row
reclassification, router relocation or duplication, matrix detachment,
contract or graph weakening, unmapped packages, and forbidden logical,
package, module, and Swift edges fail closed.

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
