# ADR-0016: Imaging foundation

Status: accepted
Date: 2026-10-07
Truth class: normative
Supersedes: 0010; the "Migration" section and the attestation wording of 0009;
the joint continuum-plus-line decision of 0011
Superseded by:

## Context

Programme #486 delivered cube, MFS, Clark, multi-worker and Metal imaging as
several parallel routes: four CPU gridding drivers, two Metal drivers, closed
enums selecting gridders, a 20k-line replay cache standing in for re-reading
the MeasurementSet, and a runtime whose receipts, execution DAG, resource
topology and identity hashing were almost entirely write-only. W-projection,
AW-projection and mosaic could not reach the fast cube path or Metal. The
programme's own process machinery (a migration matrix, a 3,578-line checker
hashing Rust function bodies, per-ticket contract revisions) would resist any
consolidation. The owner reviewed this state on 2026-10-07 and approved the
plan in `docs/imaging-architecture/imaging-foundation-plan-20261007.md`.

## Decision

CASA-RS imaging is one measurement operator, one major-cycle pass, one
deconvolution driver and one execution model. The plan document is the
normative design; this ADR records the decisions that supersede earlier ADRs.

1. **One operator.** `casa-imaging-operator` owns `Placement`/`SampleBlock`
   records, a `ConvolutionFunctionSet` trait with exactly four implementations
   (standard spheroidal, W-planes, AW catalog, mosaic PB), a `GridBackend`
   trait with exactly two implementations (CPU, Metal), `GridAccumulator`,
   `MeasurementOperator` and `WeightingGeneration`. Standard, W, AW, mosaic,
   MT-MFS, cube and MFS imaging are parameterisations of these types. Paired
   forward/adjoint choices (kernel set, conjugation, Mueller routing, phase
   gradient, phase-centre phasor, channel and polarisation maps, Taylor
   factors) live in the kernel contract of plan section 5.3.
2. **One pass.** Every major cycle re-traverses the selected MeasurementSet
   through `MajorCyclePass` and grids residual samples. No replay cache of
   visibility-independent factors is retained; a bounded tap-plan cache may be
   added later only as a measured execution strategy over the same operator.
3. **One driver.** `casa-imaging-deconvolution` owns the minor-cycle driver,
   the controller (thresholds, `cycleniter`, convergence and divergence rules)
   and a `Solver` trait with Hogbom, Clark, multiscale and Taylor
   implementations.
4. **Runtime core.** Resource handling is `HostResources`, a `ResourcePolicy`,
   one `admit()` per phase returning an RAII reservation, a sequential phase
   list with a cancel token, the Metal execution state and the bounded worker
   team. There are no execution receipts, execution DAG, lease epochs,
   logical/physical slot ledgers, cost-model profiles, adaptation transitions
   or plan identities. The only runtime record is an O(1)-per-phase run
   summary. ADR-0010 is superseded in full.
5. **No identity hashing.** Compiled problems, plans, products and selections
   are compared directly or by ownership. The only persistent identity is the
   AW convolution-function cache key. ADR-0014 and ADR-0015 remain in force
   and are extended to plans and receipts.
6. **Grid precision.** Metal accumulates in f32; CPU defaults to f64 and
   offers f32 as a user-selectable request field. Acceptance is the 1e-3
   normalised tolerance of the owner's 2026-09-24 direction, not bitwise
   agreement.
7. **AW cache.** CASA CF images are read directly (headers at open, cells on
   demand into a bounded in-memory LRU). Native EVLA generation writes
   CASA-format CF images. There is no private content-addressed store,
   manifest, payload hash or eviction ledger.
8. **Request layer.** One `ImagingRequest` struct defaulted and validated from
   the provider-contracts catalog; one `CompiledProblem`; one availability
   gate. Capabilities that are rejected are not in the catalog.
9. **Diagnostics.** Library crates read no environment variables and emit no
   `eprintln!`; diagnostic switches are request fields; emission is `tracing`.
10. **Crates.** `casa-imaging-model`, `casa-imaging-operator`,
    `casa-imaging-deconvolution`, `casa-imaging-products`,
    `casa-imaging-runtime`, `casa-imaging-metal` (macOS), and
    `casa-imaging-application`. `casa-imaging-reconstruction` is deleted when
    empty. Dependency direction is enforced by
    `scripts/check-imaging-dependencies.py` over `cargo metadata`.
11. **Joint continuum-plus-line reconstruction** is removed from the code.
    ADR-0011's sequential continuum subtraction remains; its joint-basis
    decision is superseded and may be re-proposed as a separate ADR with its
    own scientific contract.
12. **Process.** The migration matrix, contract revisions, evidence
    registries and programme-specific closure rules are retired. Tickets
    IF-0 to IF-11 under issue #648 are the work record; review gates R1 to R4
    in plan section 9.3 are the only review checkpoints; the anti-slop rules in
    plan section 10 are review criteria for every ticket.

## Consequences

Positive:
- One place for each scientific choice; W, AW and mosaic reach every backend
  and the cube streaming path.
- About 130k lines of parallel routes and write-only bookkeeping removed;
  target under 70k non-test imaging lines.
- Tests become laws and tolerances rather than fixture expectations.

Negative:
- Later major cycles re-read the MeasurementSet; the performance pass must
  show this is within the measured checkpoints or add a bounded cache.
- Between tickets some capabilities are temporarily unavailable (Metal between
  IF-2 and IF-4; W/AW/mosaic between IF-2 and IF-3). They fail typed.
- Receipts consumed by the manual `intermediate_profile_evidence.py` tool are
  replaced by the run summary.

Neutral / tradeoffs:
- Metal f32 accumulation differs from CPU f64 within tolerance; this is a
  precision choice, not a correctness gap.

## Alternatives considered

1. Keep the replay cache as the default residual path. Rejected: it is not
   scientifically required, its scratch footprint exceeds the input, and it
   duplicates the operator in a second record-based implementation.
2. Keep ADR-0010's resource authority and trim it. Rejected: its admission
   decisions reduce to one arithmetic formula, and every other structure was
   write-only.
3. Keep the architecture checker and update its ratchets per commit.
   Rejected: it enforced programme process, not architecture.

## Enforcement

This decision is enforced by:
- tests: T0 operator laws and T1 end-to-end capability tests (plan section 7)
- lint/import/dependency rules: `scripts/check-imaging-dependencies.py`
  (crate edges, device APIs only in `casa-imaging-metal`, no `std::env` or
  `eprintln!` in library crates, no content hashing in imaging crates)
- CI checks: `just arch-check`
- review trigger: plan section 10 rules at every IF ticket review; gates R1–R4
