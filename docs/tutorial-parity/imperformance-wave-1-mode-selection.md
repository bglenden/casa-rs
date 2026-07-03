# ImPerformance Wave 1 Mode Selection

Truth class: historical evidence snapshot
Last reality check: 2026-05-19
Verification: `just docs-check`; `just quick`; #251 baseline evidence in `docs/tutorial-parity/imperformance-wave-1-baseline-matrix.md`

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
- MT-MFS is important for wideband continuum, but should be a Wave 1
  standard-gridder sentinel workload rather than the first optimization target
  unless baseline evidence makes it dominant. Mosaic/PB-aware MT-MFS is tracked
  separately in #262.
- W-projection and AW/widefield imaging matter, but the existing #52 surface is
  the right owner for AW/widefield capability. Wave 1 should avoid making #52 a
  hidden prerequisite.

## Selected Wave 1 Modes

| ID | Mode | Why it is in Wave 1 | Target status |
|---|---|---|---|
| `standard-mfs-dirty-control` | `specmode='mfs'`, `gridder='standard'`, dirty-only | Fast control case for harness overhead, gridding, FFT, normalization, and product write cost. | Benchmark and keep near or faster than CASA C++. |
| `standard-mfs-clean-current` | `specmode='mfs'`, `gridder='standard'`, `deconvolver='multiscale'`, optional `auto-multithresh` | Current single-field continuum practice; covers major/minor-cycle overhead without PB/mosaic cost. | Benchmark, correctness check, and stage budget. |
| `standard-cube-line` | `specmode='cube'`, `gridder='standard'`, dirty and bounded clean variants | Current spectral-line practice; includes deconvolution coverage while separating per-channel scaling from mosaic/PB overhead. | Benchmark dirty and bounded-clean variants; identify cube scaling budget without using #56 runtime controls. |
| `mosaic-mfs-clean-primary` | `specmode='mfs'`, `gridder='mosaic'`, multiscale clean, PB products | Common ALMA/VLA tutorial mode and historically high-leverage slow area in `casa-rs`. | Keep in the matrix, but fix generated-data comparability before using timing as optimization evidence. |
| `mosaic-cube-bounded` | `specmode='cube'`, `gridder='mosaic'`, small channel count first | Exercises the expensive interaction between cube scaling, mosaic/PB work, and product generation. | Bounded benchmark and bottleneck ledger; large-cube controls stay with #56. |
| `mtmfs-wideband-sentinel` | `specmode='mfs'`, `gridder='standard'`, `deconvolver='mtmfs'`, `nterms > 1` | Current wideband continuum algorithm family; useful to keep in view before backend planning hardens. | Baseline-only sentinel unless it becomes the dominant bottleneck; mosaic MT-MFS is backlog #262. |

## Deferred Or Blocked Modes

| Mode family | Status | Reason |
|---|---|---|
| W-projection speed work | Deferred | `casa-rs` exposes a W-term request path, but Wave 1 should not turn W-projection into the first optimization target without baseline evidence and a supported CASA comparison harness. |
| AW/widefield gridder family | Deferred to #52 | Capability surface and CF-planning controls belong to #52. Wave 1 should leave hooks for future backend/resource planning but not implement AW. |
| CASA-like `parallel` / `chanchunks` | Deferred to #56 | User-visible large-cube runtime controls already have an owner. Wave 1 can measure cube scaling but should not add these controls. |
| GPU/Kokkos/CUDA implementation | Not a mode; defer from #247 mode selection | LibRA suggests that a backend/resource boundary is useful, and Wave 1 should keep GPU-readiness in view. This ticket should not add GPU dependencies or runtime behavior before the benchmark modes and stage budgets exist. |
| Distributed execution | Deferred | Not needed for first local CASA C++ versus `casa-rs` wallclock baselines. |

## Workload Shapes

Exact datasets will be created or wired by #248. Exact harness manifests will be
owned by #252. These shapes define what those tickets must support. The large
tier is a storage-constrained exception: all large logical workloads select
from one shared ALMA mosaic/cube superset rather than from separate 100 GiB
single-field and mosaic MeasurementSets.

| Mode ID | Size tier | Dataset style | Image / channel shape | Clean settings | Expected products |
|---|---|---|---|---|---|
| `standard-mfs-dirty-control` | small, medium, large-shared | deterministic single-field continuum simulation; large selects field `0` from the shared ALMA superset | 512, 2048, and 4096 pixel images; MFS over the available channel range | dirty-only, natural and Briggs variants | `.psf`, `.residual`, `.image`, timing JSON |
| `standard-mfs-clean-current` | small, medium, large-shared | deterministic single-field continuum with compact plus extended structure; large selects field `0` from the shared ALMA superset | 512, 2048, and 4096 pixel images | multiscale scales including zero; bounded `niter`; `auto-multithresh` on at least medium | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, timing JSON |
| `standard-cube-line` | small, medium, large-shared | deterministic spectral-line cube with known line structure; large selects field `0` from the shared ALMA superset | 16, 64, and 256 channels; 512 to 4096 pixel images | dirty-only plus bounded multiscale/auto-multithresh clean | cube `.psf`, `.residual`, `.image`, `.model` for clean variants, timing JSON |
| `mosaic-mfs-clean-primary` | small, medium, large-shared | deterministic mosaic with overlapping pointings; large uses all fields from the shared ALMA superset | 512, 2048, and 4096 pixel images | multiscale clean; PB products enabled; Briggs weighting | `.psf`, `.residual`, `.model`, `.image`, `.image.pbcor`, `.pb`, timing JSON |
| `mosaic-cube-bounded` | small, medium, large-shared bounded subset | deterministic line mosaic; large uses all fields but a bounded channel slice from the shared ALMA superset | 8, 32, and 32 selected channels; 512 to 4096 pixel images | dirty-only plus short clean probe | cube products, PB products where supported, timing JSON |
| `mtmfs-wideband-sentinel` | small, medium | standard-gridder single-field wideband continuum simulation | 512 to 2048 pixel image; `nterms=2` initially | MT-MFS bounded clean | Taylor-term products, alpha products, timing JSON |

## Target Style

Wave 1 should not fail because `casa-rs` has not yet reached 10x CASA C++
wallclock. It should fail if it cannot produce trustworthy evidence.

| Mode ID | Wave 1 target style | Long-term target direction |
|---|---|---|
| `standard-mfs-dirty-control` | median wallclock ratio, stage budget, correctness delta | First follow-on optimization target: correctness-green, full medium evidence has the most compute-heavy measured path relative to I/O/preparation. |
| `standard-mfs-clean-current` | median wallclock ratio, major/minor-cycle budget, correctness delta | 10x target for practical single-field continuum workflows. |
| `standard-cube-line` | dirty and bounded-clean per-channel scaling, wallclock ratio, correctness delta | 10x target after cube dataflow and backend work; #56 owns user-visible chunk/parallel controls. |
| `mosaic-mfs-clean-primary` | bottleneck ledger plus CASA C++ ratio when products are correctness-comparable | High-value follow-on after generated-data comparability is repaired. |
| `mosaic-cube-bounded` | bounded scaling and PB/product cost ledger | Decide whether the next ticket is mosaic gridding, PB/product writeback, or cube runtime controls. |
| `mtmfs-wideband-sentinel` | baseline-only unless unexpectedly dominant | Keep standard-gridder wideband continuum visible before backend structure hardens; mosaic MT-MFS is #262. |

## First Follow-On Optimization Target

The first optimization ticket after Wave 1 should target the full-shape medium
`standard-mfs-dirty-control` workload.

Rationale:

- #251 produced contrary evidence to the initial mosaic-first guess:
  generated mosaic MFS and mosaic cube timings exist, but their products are
  correctness-red against CASA on the current generated ALMA mosaic-small data.
  Those timings are not a sound optimization target yet.
- The full medium `standard-mfs-dirty-control` result is correctness-green and
  directly comparable: Rust `500.239 s`, CASA `503.295 s`, ratio `0.99x`.
- Its measured `casa-rs` work is compute-heavy enough to give a realistic
  optimization runway: gridding/degridding accounts for `310184 ms`, while
  standard-MFS preparation accounts for `207834 ms`
  (`get_ms_values_into_processing_buffer=141913 ms`,
  `prepare_processing_buffer=61297 ms`).
- The mode is structurally simple: single-field, standard gridder, dirty-only,
  no PB/mosaic correctness ambiguity, no user-visible `parallel` or
  `chanchunks`, and no AW/widefield dependency. That gives the best chance of
  proving a high-leverage backend/workspace optimization before widening to
  harder modes.

The target is workload-level. The first implementation path should split the
work into measured subtargets rather than hiding I/O under compute:

1. Standard-gridder/degridder backend and workspace optimization for the
   `310184 ms` compute owner.
2. MS/table buffer loading and processing-buffer preparation throughput for the
   `207834 ms` preparation owner.
3. Re-run the same full medium workload before claiming any speedup.

If later reruns show that image writing, preview generation, or another
non-imaging-core subsystem dominates this workload, split that owner before
claiming the backend optimization path.

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
