# Imaging migration and acceptance matrix

Truth class: normative
Last reality check: 2026-08-18
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

`LegacyWholeRun` is an ownership classification, not a compatibility promise.
It permits the current production implementation to remain reachable only
through the future whole-run migration router. It never permits a native stage
to call into legacy code or a failed native run to retry through legacy.

## Acceptance contracts

The matrix defines reusable contracts for scientific products, exact routing,
solver trajectories, cross-surface request round trips, and process resource
authority. Scientific product contracts use a normalized-RMS ceiling of
`0.001` only on declared valid support and only with a declared denominator.
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

The architecture checker validates schema, inventory completeness, stable row
identity, contract references, preserved issue crosswalks, evidence locators,
Migration Obligations, source evidence, native dependency direction, and the
frozen legacy edge set. Its synthetic contract tests prove every unlisted
logical dependency direction is rejected.

## Preserved issue and evidence crosswalk

The required crosswalk set is encoded in the matrix and cannot disappear
without failing `just arch-check`: #35, #40, #42, #45, #52, #54, #55, #217,
#319, #445-#450, #462, #466, #473, and #478.

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
