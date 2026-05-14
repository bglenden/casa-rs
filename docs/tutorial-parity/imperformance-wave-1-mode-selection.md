# ImPerformance Wave 1 Mode Selection

Truth class: current descriptive
Last reality check: 2026-05-14
Verification: just docs-check; just quick

Wave issue: #246
Child issue: #247

This note fixes the first benchmark targets for ImPerformance Wave 1. The goal
is not to cover every CASA `tclean` mode. The goal is to pick a small set of
current, real imaging use cases that can support reproducible CASA C++ versus
`casa-rs` timing, correctness checks, and stage budgets before optimization
work starts.

## Current-Practice Signals

The local RadioAstronomyOracle corpus does not provide a statistical survey of
all current reductions. It does provide current 2024 tutorial and Synthesis
Imaging Workshop signals about common practice:

- ALMA pipeline material shows a standard imaging recipe that runs continuum
  and line-imaging flows through `hifmakeimlist` / `hifmakeimages`, with
  `specmode='mfs'` appearing early in the pipeline recipe. Citation:
  `ALMA-Pipeline-tutorial-SISS2024.pdf, slide 15`.
- ALMA manual tutorial material uses `tclean` with `specmode='mfs'`,
  `gridder='standard'`, Briggs weighting, and Hogbom Clean for a compact
  calibrator-style example. Citation: `ALMA-Manual-tutorial-SISS2024.pdf,
  slide 137`.
- The same 2024 ALMA tutorial uses standard-gridder MFS continuum imaging with
  `deconvolver='multiscale'`, Briggs weighting, and large image sizes for
  science-target continuum. Citation: `ALMA-Manual-tutorial-SISS2024.pdf,
  slide 145`.
- The ALMA line-imaging example uses `specmode='cube'`,
  `deconvolver='multiscale'`, `restoringbeam='common'`,
  `weighting='briggsbwtaper'`, and `perchanweightdensity=True`. Citation:
  `ALMA-Manual-tutorial-SISS2024.pdf, slide 155`.
- 2024 mosaicking material says that joint mosaic imaging uses
  `gridder='mosaic'`, recommends `mosweight=True`, and recommends
  `perchanwtdensity=True` plus `briggsbwtaper=True` for cubes. It also says
  `gridder='mosaic'` is necessary for heterogeneous-array imaging in CASA
  `tclean`. Citation: `Plunket-Mosaicking2024.pdf, slide 34`.
- 2024 wideband material identifies CASA MT-MFS as one of the main wideband
  deconvolution algorithms and notes compatibility with multiscale
  deconvolution. Citation: `Marvil_Wideband2024.pdf, slide 19`.
- 2024 ALMA tutorial material describes `auto-multithresh` as available in
  `tclean` since CASA 5.1 and deployed in the ALMA Cycle 5 pipeline. Citation:
  `ALMA-Manual-tutorial-SISS2024.pdf, slide 122`.

Interpretation:

- Single-field `standard` MFS remains a core control case.
- Spectral-line cube imaging is a first-class current workload, not a corner
  case.
- `multiscale` and `auto-multithresh` should be treated as current-practice
  clean workloads, not optional embellishments.
- Mosaic imaging is common enough, and expensive enough in current `casa-rs`
  evidence, to be a primary performance target.
- MT-MFS is important for wideband continuum, but should be a Wave 1 sentinel
  workload rather than the first optimization target unless baseline evidence
  makes it dominant.
- W-projection and AW/widefield imaging matter, but the existing #52 surface is
  the right owner for AW/widefield capability. Wave 1 should avoid making #52 a
  hidden prerequisite.

## Selected Wave 1 Modes

| ID | Mode | Why it is in Wave 1 | Target status |
|---|---|---|---|
| `standard-mfs-dirty-control` | `specmode='mfs'`, `gridder='standard'`, dirty-only | Fast control case for harness overhead, gridding, FFT, normalization, and product write cost. | Benchmark and keep near or faster than CASA C++. |
| `standard-mfs-clean-current` | `specmode='mfs'`, `gridder='standard'`, `deconvolver='multiscale'`, optional `auto-multithresh` | Current single-field continuum practice; covers major/minor-cycle overhead without PB/mosaic cost. | Benchmark, correctness check, and stage budget. |
| `standard-cube-line` | `specmode='cube'`, `gridder='standard'`, dirty and bounded clean variants | Current spectral-line practice; separates per-channel scaling from mosaic/PB overhead. | Benchmark and identify cube scaling budget without using #56 runtime controls. |
| `mosaic-mfs-clean-primary` | `specmode='mfs'`, `gridder='mosaic'`, multiscale clean, PB products | Common ALMA/VLA tutorial mode and current high-leverage slow area in `casa-rs`. | Primary follow-on optimization target. |
| `mosaic-cube-bounded` | `specmode='cube'`, `gridder='mosaic'`, small channel count first | Exercises the expensive interaction between cube scaling, mosaic/PB work, and product generation. | Bounded benchmark and bottleneck ledger; large-cube controls stay with #56. |
| `mtmfs-wideband-sentinel` | `specmode='mfs'`, `deconvolver='mtmfs'`, `nterms > 1` | Current wideband continuum algorithm family; useful to keep in view before backend planning hardens. | Baseline-only sentinel unless it becomes the dominant bottleneck. |

## Deferred Or Blocked Modes

| Mode family | Status | Reason |
|---|---|---|
| W-projection speed work | Deferred | `casa-rs` exposes a W-term request path, but Wave 1 should not turn W-projection into the first optimization target without baseline evidence and a supported CASA comparison harness. |
| AW/widefield gridder family | Deferred to #52 | Capability surface and CF-planning controls belong to #52. Wave 1 should leave hooks for future backend/resource planning but not implement AW. |
| CASA-like `parallel` / `chanchunks` | Deferred to #56 | User-visible large-cube runtime controls already have an owner. Wave 1 can measure cube scaling but should not add these controls. |
| GPU/Kokkos/CUDA execution | Deferred | LibRA shows that a backend/resource boundary is useful, but Wave 1 should not introduce GPU dependencies or runtime behavior. |
| Distributed execution | Deferred | Not needed for first local CASA C++ versus `casa-rs` wallclock baselines. |

## Workload Shapes

Exact datasets will be created or wired by #248. Exact harness manifests will be
owned by #252. These shapes define what those tickets must support.

| Mode ID | Size tier | Dataset style | Image / channel shape | Clean settings | Expected products |
|---|---|---|---|---|---|
| `standard-mfs-dirty-control` | small, medium, large | deterministic single-field continuum simulation | 512, 2048, and 4096 pixel images; one MFS plane | dirty-only, natural and Briggs variants | `.psf`, `.residual`, `.image`, timing JSON |
| `standard-mfs-clean-current` | small, medium, large | deterministic single-field continuum with compact plus extended structure | 512, 2048, and 4096 pixel images | multiscale scales including zero; bounded `niter`; `auto-multithresh` on at least medium | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, timing JSON |
| `standard-cube-line` | small, medium, large | deterministic spectral-line cube with known line structure | 16, 64, and 256 channels; 512 to 2048 pixel images | dirty-only plus bounded multiscale/automask clean | cube `.psf`, `.residual`, `.image`, optional `.model`, timing JSON |
| `mosaic-mfs-clean-primary` | small, medium, large | deterministic mosaic with overlapping pointings; include one real tutorial case | 512, 2048, and tutorial-scale images | multiscale clean; PB products enabled; Briggs weighting | `.psf`, `.residual`, `.model`, `.image`, `.image.pbcor`, `.pb`, timing JSON |
| `mosaic-cube-bounded` | small, medium | deterministic line mosaic | 8 and 32 channels; 512 to 1024 pixel images | dirty-only plus short clean probe | cube products, PB products where supported, timing JSON |
| `mtmfs-wideband-sentinel` | medium | wideband continuum simulation | 2048 pixel image; `nterms=2` initially | MT-MFS bounded clean; multiscale sentinel if already supported by path | Taylor-term products, alpha products, timing JSON |

## Target Style

Wave 1 should not fail because `casa-rs` has not yet reached 10x CASA C++
wallclock. It should fail if it cannot produce trustworthy evidence.

| Mode ID | Wave 1 target style | Long-term target direction |
|---|---|---|
| `standard-mfs-dirty-control` | median wallclock ratio, stage budget, correctness delta | 10x CASA C++ stretch target after backend/workspace work. |
| `standard-mfs-clean-current` | median wallclock ratio, major/minor-cycle budget, correctness delta | 10x target for practical single-field continuum workflows. |
| `standard-cube-line` | per-channel scaling, wallclock ratio, correctness delta | 10x target after cube dataflow and backend work; #56 owns user-visible chunk/parallel controls. |
| `mosaic-mfs-clean-primary` | primary bottleneck ledger plus CASA C++ ratio | First follow-on optimization target because previous evidence showed mosaic/CLEAN slower than CASA and the mode is current practice. |
| `mosaic-cube-bounded` | bounded scaling and PB/product cost ledger | Decide whether the next ticket is mosaic gridding, PB/product writeback, or cube runtime controls. |
| `mtmfs-wideband-sentinel` | baseline-only unless unexpectedly dominant | Keep wideband continuum visible before backend structure hardens. |

## First Follow-On Optimization Target

The first optimization ticket after Wave 1 should target
`mosaic-mfs-clean-primary` unless #251 produces contrary evidence.

Rationale:

- Current-practice sources make mosaic imaging a normal `tclean` workload, not
  a niche path.
- Existing Wave 7 evidence already showed mosaic/CLEAN workloads slower than
  CASA C++ while standard MFS dirty imaging was close to CASA.
- The path is likely to exercise the same structural seams needed for later
  cube, PB, workspace residency, backend, and GPU work.

If #251 shows that the dominant cost is MS/table preparation, image writing, or
preview generation instead of imaging proper, split the follow-on target before
optimizing.

## Issue #247 Acceptance Mapping

- Selected modes, excluded modes, and rationale: this document, sections
  "Selected Wave 1 Modes" and "Deferred Or Blocked Modes".
- Workload shapes: section "Workload Shapes".
- Target styles: section "Target Style".
- Blocked/deferred cases: section "Deferred Or Blocked Modes".
- Existing surface mapping: the selected modes map to existing
  `casars-imager` / `casa-imaging` surfaces except where marked as sentinel or
  deferred.
- First follow-on optimization target: section "First Follow-On Optimization
  Target".
