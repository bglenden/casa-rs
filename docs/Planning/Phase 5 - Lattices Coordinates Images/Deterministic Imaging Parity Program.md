# Deterministic Imaging Parity Program

Truth class: historical
Last reality check: 2026-07-16
Verification: none

This document is the completed-program snapshot for the deterministic imaging
parity work formerly tracked by issue `#39`. It preserves the original wave
contract and closeout evidence; it is not an active backlog or current
capability inventory.

Current implementation status lives in `docs/imaging-effects-inventory.md` and
the architecture and testing contracts. Active work is owned by open GitHub
issues, including the remaining focused parity, capability, and execution-plan
tickets.

- Numeric parity uses CASA/casacore imaging behavior as the truth oracle.
- Metadata, serialization, and `image.open()` parity use CASA/casacore image
  table behavior as the truth oracle.
- Older imaging wave/checkpoint/backlog documents are historical context only.
- Historical umbrella tracker: issue `#39` (closed as superseded).

## Program Summary

- Replace opportunistic end-image debugging with stage-by-stage proof.
- Build and freeze seam-level oracle bundles before broadening feature work.
- Prefer reuse of already-proven kernels and same-input fixtures as anchors.
- Only replace code when doing so creates a necessary proof boundary or removes
  an ownership ambiguity.
- Execute work wave-by-wave. The wave is the execution unit, not the
  intermediate checkpoint.
- Once a wave starts, it must be carried through to that wave's closure
  without stopping for intermediate status checks or user confirmation.
- Wave closure includes implementation, verification against the wave gates,
  and the end-of-wave code-review/fix pass.
- Stop and take stock only at wave boundaries, or earlier if a real blocker,
  ambiguity, or tradeoff appears that needs explicit discussion.

## Current Status

- Wave 0 is complete.
- Wave 1 is complete on the prepare-seam frozen-oracle substrate:
  Rust bundle schema, pinned provenance metadata, deterministic artifact
  hashing, and frozen source-backed prepare bundles for the canonical Wave 1
  dataset set are now in place under the Wave 1 freezer workflow.
- Wave 2 is complete:
  the geometric preparation seam is now explicit, `PreparedGeometryTraceBundle`
  is emitted from stable row selection, raw-MS UVW reprojection follows the
  casacore `UVWMachine` path rather than the old approximation, and the Wave 2
  gate tuples are green against CASA `fixvis` on both `ngc5921.ms` and
  `n2403.short.ms`.
- Wave 3 is complete:
  the spectral preparation seam now emits source-backed frame conversion,
  channel contribution, interpolation, and weight-source artifacts, and the
  Wave 3 gate tuples are green against CASA on `refim_point_withline.ms`
  `specmode='cube'`, `refim_Cband.G37line.ms` `specmode='cube'
  outframe='LSRK'`, and `refim_Cband.G37line.ms` `specmode='cubedata'`.
- Wave 4 is complete:
  `refim_point_withline.ms` source-backed dirty products are pinned green on
  the focused cube window, and the first ALMA mosaic dirty mismatch is reduced
  to the exact source-backed repro in
  [#43](https://github.com/bglenden/casa-rs/issues/43) for Wave 13 ownership.
- Wave 5 is complete:
  Rust-written clean products now reopen in CASA/casacore again after fixing
  legacy `StokesCoordinate` image-table serialization, and representative
  header/openability parity is green on the pinned `ngc5921.ms` clean gate.
- Wave 6 is complete:
  the standard scalar MFS natural dirty path is now qualified on the rebuilt
  seams for both `ngc5921.ms` and `M51.ms`, with header parity, sample-pixel
  parity, and image-scale residual/PSF checks green against CASA.
- Wave 7 is complete:
  the dedicated weighting and normalization seam is now explicit via
  `WeightingDiagnostics`, the planned `ngc5921.ms`, `M51.ms`,
  `refim_point_withline.ms`, and `refim_Cband.G37line.ms` weighting / dirty
  gates are green, and the earlier `.sumwt` drift was traced to a parity
  harness SPW-selection bug rather than the imaging core.
  An additional `cubedata` Briggs stress case remains open in
  [#45](https://github.com/bglenden/casa-rs/issues/45), but it is outside the
  pinned Wave 7 gate list and is now tracked explicitly.
- Wave 8 is complete:
  the benchmark harness now reproduces both pinned cube gates through
  `scripts/bench-imager-vs-casa.sh`, including explicit cube interpolation and
  `nsigma`, Rust stage reporting now includes weighting alongside preparation,
  residual refresh, and product-writing timings, and the active gates are fast
  enough to resume correctness work.
  On the same workstation with `BENCH_REPEATS=1`:
  `refim_point_withline.ms` (`specmode=cube`, `deconvolver=hogbom`,
  `nsigma=10`) measures `14.88 s` in Rust vs `16.56 s` in CASA, and
  `refim_Cband.G37line.ms` dirty cube measures `4.24 s` in Rust vs `1.88 s`
  in CASA. Both are well inside the Wave 8 `<= 3 min` and `<= 2.5x CASA`
  gate, and issue [#47](https://github.com/bglenden/casa-rs/issues/47) is
  closed.
- Wave 9 is complete:
  standard major-cycle prediction, per-visibility residual/prediction seams,
  residual refresh, and major-cycle orchestration are requalified on the
  pinned `refim_point_withline.ms` cube Hogbom `nsigma` gate. The late-block
  stop mismatch is closed, issue
  [#44](https://github.com/bglenden/casa-rs/issues/44) is resolved, and the
  remaining intentional divergence from CASA is the documented Hogbom
  off-by-one bug noted in
  [docs/CASA (C++) bugs.md](../../CASA%20(C%2B%2B)%20bugs.md), which casa-rs
  does not reproduce.
- Wave 10 is complete:
  standard minor-cycle and restoration parity is now green on the pinned
  Clark, multiscale, and common-beam gates. The harness now drives CASA with
  the same cycle controls and `smallscalebias` as Rust, the final exact
  residual refresh is guaranteed before returning from a CLEAN run, and the
  planned standard-imaging gates are green on `sim_data_VLA_jet.ms`,
  `ngc5921.ms`, `M51.ms`, `n2403.short.ms`, and
  `refim_point.ms` common-beam restoration. The remaining follow-up is a
  performance/implementation audit of the structured-model FFT residual-refresh
  predictor, tracked in
  [#48](https://github.com/bglenden/casa-rs/issues/48), but Wave 10
  correctness itself is closed.
- Wave 11 is complete:
  scalar full-Stokes plane selection now supports `I`, `Q`, `U`, and `V`
  directly on top of the standard imaging path, with the frontend deriving the
  requested plane from the correct linear-basis (`XX/XY/YX/YY`) or
  circular-basis (`RR/RL/LR/LL`) correlation pairs. The reported polarized
  `.sumwt` semantics now match CASA without perturbing the already-proven dirty
  and CLEAN normalization path. The planned local gates are green on
  `refim_point_linXY.ms` dirty, `polcal_CIRCULAR_BASIS.ms` dirty, and
  `refim_point_stokes.ms` Hogbom CLEAN. `polcal_LINEAR_BASIS.ms` remains part
  of the canonical Wave 11 matrix, but that dataset is not currently present in
  the mounted local test-data tree.
- Wave 12 is complete:
  the existing `wproject` implementation is now qualified against CASA on the
  pinned `refim_point_wterm_vlad.ms` and `n2403.short.ms` gates, with explicit
  `WProjectTraceBundle` artifacts for plane/support/sample planning and CASA's
  conjugation / fixed-plane fallback behavior mirrored in Rust.
- Wave 13 is complete:
  recentering and faceting parity is green on `refim_mawproject.ms`,
  `refim_mawproject_offcenter.ms`, and `refim_mawproject_twopointings.ms`
  after aligning Rust's UVW reprojection sign handling with CASA's
  UVWMachine/MS convention.
- Wave 14 is complete:
  dirty mosaic parity is green on the pinned `refim_alma_mosaic.ms`,
  `papersky_mosaic.ms`, and `refim_oneshiftpoint.mosaic.ms` gates. Rust now
  applies source-backed common-PB defaults for the current corpus and matches
  CASA's non-normalized sensitivity division during mosaic dirty/PSF
  normalization.
- Wave 15 is complete:
  the `nterms=1` half remains green on
  `vla_wideband_2ptg_w_squint.ms field=0`, and true CASA-style `mtmfs` with
  `nterms=2` is now pinned green on the accessible local wideband gate
  `ref_vlass_wtsp_creation.ms field=0 spw=0 specmode=mfs gridder=standard
  weighting=natural`. Rust now exposes a real MTMFS request surface, writes
  `.tt*`, `.alpha`, and `.alpha.error` products, and mirrors CASA's
  principal-solution restore path when assembling restored Taylor-term images.

## Canonical Proof Boundaries

1. `SelectedRows`
2. `PreparedVisibilityBlock`
3. `WeightedVisibilityBlock`
4. `GridPlan/CFPlan`
5. `DirtyProducts`
6. `NormalizationState`
7. `MinorCycleInput`
8. `MinorCycleOutput`
9. `RestoredProducts`
10. `ProductTableBundle`

## Ownership Contract

- `casa-ms` owns row selection, field/pointing identity, phase-center
  resolution, UVW reprojection, per-row phasors, spectral/frame conversion,
  channel contribution maps, correlation mapping, flags, multiplicity, base
  weights, and `sumwt_factor`.
- `casa-imaging` owns geometry-dependent weighting, grid planning, CF planning,
  derivative engines, normalization, major-cycle orchestration, minor-cycle
  solvers, restoration, and product assembly.
- `casars-imager` owns orchestration, staged dataset copies, run manifests,
  oracle-harness entrypoints, and product writing coordination.

## Waves

### Wave 0. Program Reset

- Audit existing imaging wave/checkpoint/backlog docs.
- Move still-in-scope items into this program or the effect matrix.
- Move out-of-scope but still relevant items into GitHub issues.
- Mark prior imaging planning docs as superseded archival context.
- Record the freeze rule: only seam-building, oracle-building, and fixes
  expressed through new seams may land before their wave opens.

### Wave 1. Truth Harness and Frozen Oracles

- Implement the oracle harness and artifact schema.
- Emit frozen bundles for selected rows, prepared visibilities, weighting
  artifacts, grid plans, dirty/PSF/residual/model/image products,
  normalization outputs, and writer header dumps.
- Pin tolerance classes and dataset tiers.

### Wave 2. Geometric Preparation Seam

- Rebuild row selection, field resolution, phase-center choice, UVW
  reprojection, per-row phasors, and correlation mapping into the prepared
  visibility seam.
- Preserve row-native field and pointing identity instead of collapsing the
  world to one global phase-center abstraction.

### Wave 3. Spectral Preparation Seam

- Rebuild frame conversion, channel contribution maps, interpolation
  coefficients, `WEIGHT_SPECTRUM` vs `WEIGHT` behavior, and cube/cubedata
  channel semantics.

### Wave 4. Upstream Dirty-Plane Closure

- Use the new preparation seams to close known upstream discrepancies before
  touching solver behavior.
- The parked `refim_point_withline.ms` `409 vs 407` gap is owned here because
  current evidence already points to prepared-sample / dirty-plane drift.

### Wave 5. Writer Metadata and Openability

- Isolate the minimal writer layer and close coordinate/header/openability
  parity independent of later numeric product expansion.

### Wave 6. Standard Dirty Imaging: Scalar MFS Natural

- Qualify the standard dirty path on top of the new seams with the smallest
  numeric surface first.

### Wave 7. Weighting, Normalization, and Cube Dirty Semantics

- Add the dedicated weighting seam and fully define CASA truth for natural,
  uniform, and Briggs weighting, taper, normalization denominators,
  `normalization_sumwt`, and `reported_sumwt`.
- Extend the standard dirty path to `cube` and `cubedata`.

### Wave 8. Imaging Throughput and Cycle Time

- Pause downstream correctness waves until the active parity gates are fast
  enough to support iterative development.
- Pin Rust-vs-CASA timing baselines for:
  - `refim_point_withline.ms field=0 specmode=cube deconvolver=hogbom nsigma=10`
  - one dirty-only cube gate on a pinned real dataset
- Add explicit progress/timing reporting for:
  - preparation
  - weighting
  - residual refresh / prediction
  - `nsigma` thresholding
  - product writing
- Remove repeated work in the active hotspots:
  - cube spectral preparation / per-row channel assignment
  - repeated MADFM-based `nsigma` estimation
  - cube major-cycle residual refresh / prediction overhead
- Gate:
  - the pinned `refim_point_withline.ms` cube Hogbom `nsigma` release gate
    completes in at most `3 min` wall time and no worse than `2.5x` the timed
    CASA baseline on the same workstation
  - long-running parity gates expose named stage/progress counters instead of
    behaving as opaque black boxes

### Wave 9. Standard Major Cycle

- Requalify model prediction, per-visibility prediction/residual seams,
  residual refresh, and major-cycle orchestration.

### Wave 10. Standard Minor Cycles and Restoration

- Requalify Hogbom, Clark, multiscale, restoring beams, common-beam logic,
  `.model`, `.image`, and final writer outputs for standard imaging.

### Wave 11. Standard Full-Stokes

- Pull standard full-Stokes forward rather than blocking it behind
  widefield/mosaic work.

### Wave 12. Existing WProject Qualification

- Treat the current convolutional `WProject` implementation as an existing
  asset to instrument and qualify.
- Split `GridPlan/CFPlan` cleanly out of the current mixed
  `prepare_w_project_data()` seam.
- Status: green on the pinned wave gates. The seam now emits explicit
  `WProjectDiagnostics` / `WProjectTraceBundle` artifacts, the auto-plane
  scaler matches CASA's fixed-plane fallback, and kernel conjugation now
  follows CASA's `wprojgrid.f` sign convention (`uvw(3) > 0` uses the
  conjugated kernel).

### Wave 13. Recentering and Faceting

- Add recentering and facet infrastructure only after the existing `wproject`
  path is qualified at its current seam boundaries.
- Status: green on the pinned recentering gates. Explicit `--phasecenter`
  support now propagates through the prepare and `wproject` parity paths, and
  the UVW reprojection seam now matches CASA's `UVWMachine` sign convention
  for `(u, v)` when computing rotated UVW coordinates and per-row phase shifts.

### Wave 14. Mosaic, PB, AW, and Heterogeneous Arrays

- Close the current extended multi-field gap in ordered steps:
  - mosaic pointing and PB basics
  - PB-aware normalization
  - AW-style CF planning using precomputed CF caches first
  - heterogeneous arrays and PB-aware restoration/common-beam semantics
- Status: green on the pinned dirty-mosaic/PB gates
  (`refim_alma_mosaic.ms`, `papersky_mosaic.ms`,
  `refim_oneshiftpoint.mosaic.ms`). The closing parity fix was to mirror
  CASA's mosaic normalization semantics by dividing the dirty image and PSF by
  the non-normalized sensitivity image rather than by a `sqrt(PB)` surrogate.
  The Rust path now also infers telescope-specific common-PB defaults for the
  current gate set (ALMA/ACA effective Airy apertures and the EVLA L-band
  common polynomial model) before building the mosaic projector.

### Wave 15. Wideband Continuum

- `nterms=1` continuity is now pinned green on
  `vla_wideband_2ptg_w_squint.ms field=0`:
  dirty products match CASA through the existing single-term MFS path, and a
  restored-product gate is green with the already-qualified Clark solver.
- True CASA-style `mtmfs` with `nterms=2` is now pinned green on
  `ref_vlass_wtsp_creation.ms field=0 spw=0 specmode=mfs gridder=standard
  weighting=natural`:
  dirty `.psf.tt*`, `.residual.tt*`, and `.sumwt.tt*` products track CASA, and
  the one-iteration clean gate is green on `.model.tt*`, `.image.tt*`,
  `.residual.tt*`, `.alpha`, and `.alpha.error`.
- The closing restore bug was CASA's principal-solution residual transform:
  restored MTMFS images and spectral-index products must use the inverse-Hessian
  transformed residual Taylor terms, while the emitted `.residual.tt*` products
  remain the raw residual terms.

### Wave 16. Completion Matrix and Stop Condition

- Replace checkpoint prose with a supported-mode matrix listing the exact
  dataset/parameter tuple, stage artifacts, truth domain, owner, and pass
  status for every required row.
- Reopen new imaging feature work only after all required rows are green.
- Wave 16 closeout now includes a final local performance recheck to ensure the
  late-wave feature work did not regress the earlier Wave 8 throughput gate.
  On the current workstation:
  `ngc5921.ms field=0 spw=0 specmode=mfs gridder=standard weighting=natural
  niter=0` reran at `0.220 s` median in Rust vs `0.173 s` in CASA
  (`1.27x CASA`), and the new Wave 15 MTMFS tuple
  `ref_vlass_wtsp_creation.ms field=0 spw=0 specmode=mfs
  deconvolver=mtmfs nterms=2 niter=1` measured `3.36 s` median in Rust vs
  `2.90 s` in CASA (`1.16x CASA`). Both remain comfortably inside the active
  `<= 2.5x CASA` target.

## Canonical Test Strategy

- Every wave uses the same proof order:
  1. synthetic same-input oracle
  2. source-backed seam-trace oracle
  3. end-product oracle
- A wave is the mutually agreed execution unit. The implementation default is
  to finish the entire wave, including its verification gates and
  code-review/fix pass, before pausing for user confirmation.
- Mid-wave progress updates are informational only. They do not imply a pause
  point or request permission to continue unless an explicit blocker is raised.
- `ImagingTraceBundle` artifacts are persisted whenever applicable:
  - selected row ids/order
  - post-prepare visibilities
  - post-reprojection UVW
  - phase-center metadata
  - per-row phasors
  - spectral source-channel indices and interpolation coefficients
  - `WEIGHT_SPECTRUM` vs `WEIGHT` source choice
  - base weights
  - multiplicity
  - `sumwt_factor`
  - per-sample weighting outputs
  - grid plans / CF plans
  - WProject/AW kernel artifacts
  - gridded UV plane
  - FFT plane
  - pre/post-normalization PSF and dirty planes
  - predicted visibilities
  - residual visibilities
  - writer header dumps
- Each wave closes with a code-review pass over every change created during
  that wave. Findings that affect correctness, proof-boundary stability, or
  oracle reproducibility must be fixed before the wave can close; anything not
  fixed must move into a GitHub issue owned by a later wave.

## Defaults and Deferred Items

- CASA/casacore remains the only behavioral truth oracle.
- LibRA is an architectural/process reference only.
- Standard full-Stokes is in scope.
- Widefield/mosaic full-Stokes with DD Mueller-aware polarization correction is
  deferred to issue `#40`.
- Precomputed CF caches are in scope; full CF generation from antenna models
  is deferred unless a required dataset proves it necessary; see issue `#40`.
- GPU/distributed execution remains outside this program; see issue `#40`.
