# Imaging Effect Matrix

This matrix is the canonical inventory of required imaging effects for the
deterministic parity program.

- Source of truth for backlog/status remains GitHub issues.
- This matrix is the local implementation contract: required vs deferred,
  minimal proof seam, and the first gate tuple that must go green.
- Umbrella program tracker: issue `#39`.

| Mode / Effect | Required | Truth Domain | First Seam Gate | First Gate Tuple | Notes |
| --- | --- | --- | --- | --- | --- |
| Row selection identity and stable ordering | Yes | CASA imaging | `SelectedRows` | `ngc5921.ms field=0 phasecenter=0 specmode=mfs gridder=standard weighting=natural niter=0` | Includes DDID/SPW/POL mapping |
| Phase-center resolution and UVW reprojection | Yes | CASA imaging | `PreparedVisibilityBlock` | `n2403.short.ms field=0,1 phasecenter=0 specmode=cube gridder=wproject wprojplanes=8 niter=0` | Preserve row-native field identity |
| Per-row visibility phasors | Yes | CASA imaging | `PreparedVisibilityBlock` | same as above | Must be proven before multi-field claims |
| Spectral frame conversion and channel contributions | Yes | CASA imaging | `PreparedVisibilityBlock` | `refim_point_withline.ms specmode=cube` | Explicit source-channel indices and interpolation coefficients |
| `WEIGHT_SPECTRUM` vs `WEIGHT` fallback | Yes | CASA imaging | `PreparedVisibilityBlock` | `refim_point_withline.ms specmode=cube` | Weight source must be emitted explicitly |
| Natural MFS dirty imaging | Yes | CASA imaging | `DirtyProducts` | `ngc5921.ms` and `M51.ms` natural MFS dirty | Wave 6 gate is green on both pinned datasets |
| Uniform/Briggs/taper weighting | Yes | CASA imaging | `WeightedVisibilityBlock` | `ngc5921.ms` natural/uniform/Briggs | Wave 7 planned gates are green; the earlier `.sumwt` drift came from a parity harness SPW-selector bug, not the weighting seam |
| `normalization_sumwt` vs `reported_sumwt` | Yes | CASA imaging + image tables | `NormalizationState` | same weighting gates | Wave 7 now exposes these explicitly via `WeightingDiagnostics`; no unexplained ambiguity remains on the planned gate set |
| Cube / cubedata dirty semantics | Yes | CASA imaging | `DirtyProducts` | `refim_point_withline.ms`, `refim_Cband.G37line.ms` | Wave 7 planned cube gates are green; extra cubedata Briggs stress gap is tracked in `#45` |
| Imaging throughput / cycle time | Yes | pinned local timing baselines | wave-level instrumentation | `refim_point_withline.ms field=0 specmode=cube deconvolver=hogbom nsigma=10` | Wave 8 remains green, and the Wave 16 recheck confirms no late regression on accessible local gates: `ngc5921.ms field=0 spw=0 specmode=mfs weighting=natural niter=0` reran at `0.220 s` Rust vs `0.173 s` CASA (`1.27x`), and `ref_vlass_wtsp_creation.ms field=0 spw=0 specmode=mfs deconvolver=mtmfs nterms=2 niter=1` reran at `3.36 s` Rust vs `2.90 s` CASA (`1.16x`). |
| Major-cycle prediction and residual refresh | Yes | CASA imaging | `MinorCycleInput` | synthetic same-input fixtures + standard real datasets | Wave 9 is green on the pinned `refim_point_withline.ms` cube Hogbom `nsigma` gate; late-block restart/refresh parity is now closed |
| Hogbom / Clark / multiscale minor cycles | Yes | CASA imaging | `MinorCycleOutput` | `refim_point_withline.ms`, `sim_data_VLA_jet.ms`, `M51.ms` | Wave 10 planned standard-imaging gates are green on `sim_data_VLA_jet.ms`, `ngc5921.ms`, `M51.ms`, `n2403.short.ms`, and `refim_point.ms` common-beam restoration. casa-rs intentionally does not reproduce CASA's documented Hogbom `niter` off-by-one bug; treat that as an upstream divergence, not a parity target. The remaining structured-model FFT residual-refresh predictor audit is tracked in `#48` |
| Writer coordinates / `image.open()` parity | Yes | CASA image tables | `ProductTableBundle` | representative `.psf`, `.residual`, `.image`, `.sumwt` | Wave 5 clean gate is green again on `ngc5921.ms`; the legacy Stokes coordinate save layout was the blocking interoperability bug |
| Standard full-Stokes | Yes | CASA imaging + image tables | `PreparedVisibilityBlock` + `DirtyProducts` + `ProductTableBundle` | `refim_point_linXY.ms`, `refim_point_stokes.ms`, `polcal_*` | Wave 11 is green on the available local gates: `refim_point_linXY.ms` dirty, `polcal_CIRCULAR_BASIS.ms` dirty, and `refim_point_stokes.ms` Hogbom CLEAN. The frontend now derives `I/Q/U/V` from the appropriate linear or circular correlation pairs while preserving CASA-style polarized `.sumwt` reporting. `polcal_LINEAR_BASIS.ms` remains part of the canonical matrix but is not present in the currently mounted local test-data tree. |
| Existing `wproject` sample planning | Yes | CASA imaging | `GridPlan/CFPlan` | `refim_point_wterm_vlad.ms`, `n2403.short.ms` | Wave 12 is green on the pinned gates. `WProjectDiagnostics` / `WProjectTraceBundle` now expose plane/support/sample planning, the fixed-plane scaler matches CASA's `0.25 / increment` fallback when auto scaling is clamped, and kernel conjugation matches `wprojgrid.f` (`uvw(3) > 0` uses the conjugated kernel). |
| Recentering / faceting | Yes | CASA imaging | `GridPlan/CFPlan` | `refim_mawproject*.ms` | Wave 13 is green on `refim_mawproject.ms`, `refim_mawproject_offcenter.ms`, and `refim_mawproject_twopointings.ms`. The closing defect was an upstream UVWMachine/MS sign-convention mismatch: CASA flips `(u, v)` before and after UVWMachine-style phase-center reprojection, and Rust now does the same when deriving rotated UVW coordinates and `dphase`. |
| Mosaic pointing and PB basics | Yes | CASA imaging | `GridPlan/CFPlan` + `NormalizationState` | `refim_alma_mosaic.ms`, `papersky_mosaic.ms`, `refim_oneshiftpoint.mosaic.ms` | Wave 14 is green on the pinned dirty-mosaic gates. Rust now routes multi-pointing MFS datasets through an explicit mosaic projector, applies telescope-specific common-PB defaults for the current corpus, and mirrors CASA's non-normalized sensitivity division during mosaic dirty/PSF normalization. Slow-test gates now assert the pinned parity envelope on all three datasets. |
| AW-style CF planning with precomputed caches | Yes | CASA imaging | `GridPlan/CFPlan` | first AW-backed mosaic dataset in corpus | Full antenna-model CF generation is deferred |
| Heterogeneous-array mosaic semantics | Yes | CASA imaging | `NormalizationState` + `RestoredProducts` | `refim_heterogeneous_pointings.ms`, `vla_wideband_2ptg_w_squint.ms` | Includes PB-aware restoration/common-beam inputs |
| Wideband `nterms=1` continuity | Yes | CASA imaging | `DirtyProducts` / `RestoredProducts` | `vla_wideband_2ptg_w_squint.ms field=0 specmode=mfs gridder=standard weighting=natural` | Wave 15 `nterms=1` gate is green on both dirty and restored products using full-SPW single-field MFS selection |
| MTMFS (`nterms>1`) | Yes | CASA imaging + image tables | `RestoredProducts` | `ref_vlass_wtsp_creation.ms field=0 spw=0 specmode=mfs gridder=standard weighting=natural` | Wave 15 is green on the pinned `nterms=2` dirty and one-iteration clean gates. Rust now exposes a dedicated MTMFS request/result surface, writes CASA-style `.tt*`, `.alpha`, and `.alpha.error` products, and mirrors CASA's principal-solution residual transform when assembling restored Taylor-term images. |
| Widefield/mosaic full-Stokes with DD Mueller-aware correction | No | Future issue | N/A | N/A | Deferred after this program; tracked in `#40` |
| Full CF generation from antenna models | No | Future issue | N/A | N/A | Precomputed CF caches are sufficient for this program unless a gate proves otherwise; tracked in `#40` |
| GPU/distributed execution | No | Future issue | N/A | N/A | Outside this deterministic parity program; tracked in `#40` |

## Dataset Tiers

- Tier A: small synthetic fixtures plus one compact real MS per seam
- Tier B: slow gated parity datasets
  - `refim_point_withline.ms`
  - `refim_Cband.G37line.ms`
  - `refim_point_wterm_vlad.ms`
  - `n2403.short.ms`
  - `refim_alma_mosaic.ms`
  - standard full-Stokes datasets
- Tier C: manual/stress datasets
  - `M51.ms`
  - `papersky_mosaic.ms`
  - `refim_oneshiftpoint.mosaic.ms`
  - `refim_heterogeneous_pointings.ms`
  - wideband stress datasets
