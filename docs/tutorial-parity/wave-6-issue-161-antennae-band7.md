# Wave 6 Issue 161 Antennae Band 7

Truth class: current evidence
Last reality check: 2026-05-02
Verification:
- `bash -n scripts/run-wave6-issue161-antennae.sh`
- `cargo check -p casars-imager`
- `cargo test -p casars-imager pbcor_products_apply_primary_beam_cutoff`
- `cargo test -p casars-imager cube_pb_product_normalizes_each_channel_plane`
- `cargo test -p casa-test-support tutorial_dataset_registry_contains_first_wave_candidates`
- `cargo test -p casa-imaging mosaic_projector_sampling_matches_casa_hetarray_default`
- `scripts/run-wave6-issue161-antennae.sh target/wave6-issue161-antennae`

Wave issue: #143
Child issue: #161

## Purpose

Issue #161 is the Wave 6 Antennae Band 7 Imaging implementation issue. This
document records the current #161-only evidence and keeps it separate from
full Wave 6 closeout.

The reproducible panel runner is:

```sh
scripts/run-wave6-issue161-antennae.sh target/wave6-issue161-antennae
```

Set `CASA_RS_FETCH_TUTORIAL_DATA=1` to download missing official tutorial data.
Each run writes stamped panel filenames using a local-time stamp, records the
same stamp in the panel footer and summary JSON, and refreshes non-stamped
`*-panel.png` aliases. Review should use the stamped `panel_png` paths from:

```text
target/wave6-issue161-antennae/wave6-issue161-summary.json
```

Each reproduced image panel contains four columns:

- CASA Guide figure from the tutorial page
- CASA C++ product generated locally from the staged tutorial data
- casa-rs product generated locally from the same staged tutorial data
- `casa-rs - CASA C++` difference image

Image colorbars are labeled in Jy/beam; PB colorbars are labeled as primary
beam. Masked pixels render in gray. Metrics are computed on the shared valid
CASA C++ and casa-rs image support.

## Tutorial Inputs

| Dataset | Tutorial source | Local data product | Registry key |
|---|---|---|---|
| Antennae Band 7 calibrated data | <https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging_6.6.6> | `tutorial-parity/alma/antennae/band7/Antennae_Band7_CalibratedData/Antennae_North.cal.ms` and `Antennae_South.cal.ms` | `alma/antennae/band7/calibrated-data` |
| Antennae Band 7 CASA reference images | <https://casaguides.nrao.edu/index.php/AntennaeBand7> | `tutorial-parity/alma/antennae/band7/Antennae_Band7_ReferenceImages` | `alma/antennae/band7/reference-images` |

## Current Continuum Results

Current run stamp: `20260502T180927-0600`, generated
`2026-05-02 18:09:27 -0600`.

The continuum run completed against local CASA C++ and casa-rs products. The
runner uses the tutorial mosaic geometry for North and South continuum imaging
and a bounded noninteractive `niter=32` proof run for CLEAN products. The CASA
Guide uses interactive cleaning and `niter=1000`; those exact interactive mask
flows are not claimed by this continuum proof.

Wall-clock timing on the local tutorial run:

| Runner | Seconds | Relative |
|---|---:|---:|
| CASA C++ | `42.026` | `1.00x` |
| casa-rs | `193.242` | `4.60x CASA` |

The bounded North two-channel dirty line-cube probe took `3.124s` in CASA C++
and `2.395s` in casa-rs.

### Continuum Metrics

Source-region peak-relative differences use shared valid pixels with
`abs(CASA) >= 25%` of the CASA peak.

| Panel | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | Rust-only valid | CASA-only valid | CASA RMS | Difference RMS | Diff/CASA RMS | source p90 abs(diff)/peak | source max abs(diff)/peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| North dirty `.image` | `56562 / 90000` | `56562` | `56585` | `23` | `0` | `0.0005037589748977789` | `0.0000005228669157339375` | `0.0010379307204204866` | `0.0004913167103931669` | `0.0011243051253385552` |
| North clean `.residual` | `134322 / 250000` | `134322` | `134378` | `56` | `0` | `0.0004842421539966355` | `0.00000042211890958799533` | `0.0008717103748694464` | `0.0005840204276983746` | `0.001556398039597241` |
| North clean `.image` | `134322 / 250000` | `134322` | `134378` | `56` | `0` | `0.0004930845920012612` | `0.00000042435802932331366` | `0.0008606191233860907` | `0.0003973664956661594` | `0.0007032833744921095` |
| South clean `.residual` | `174195 / 562500` | `174199` | `174222` | `27` | `4` | `0.00046585536101634024` | `0.0000013179985508508814` | `0.002829201209524455` | `0.0009942126057312414` | `0.03202537645946853` |
| South clean `.image` | `174195 / 562500` | `174199` | `174222` | `27` | `4` | `0.00047276336698618066` | `0.0000010499899183344412` | `0.0022209629418371017` | `0.0005966732729303848` | `0.0028014619149785915` |

### Inspectable Panels

- `target/wave6-issue161-antennae/alma-antennae-north-cont-dirty-image-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-cont-clean-residual-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-cont-clean-image-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-south-cont-clean-residual-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-south-cont-clean-image-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-line-dirty-probe-image-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-line-dirty-probe-pb-20260502T180927-0600-panel.png`
- `target/wave6-issue161-antennae/wave6-issue161-summary.json`

## Current Line-Cube Probe

This pass adds a bounded North CO(3-2) dirty line-cube probe for the tutorial
command shape that first forced the remaining #161 work: `specmode='cube'`,
`gridder='mosaic'`, two selected channels, and PB products. This is not the
full tutorial line CLEAN or selfcal sequence.

| Probe product | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | CASA RMS | Difference RMS | Diff/CASA RMS | source p90 abs(diff)/peak | source max abs(diff)/peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| North dirty line `.image`, channel 0 | `4096 / 4096` | `4096` | `4096` | `0.007540386763608376` | `0.000010449885420215083` | `0.001385855360980767` | `0.0009621304710746463` | `0.0016088204622905734` |
| North dirty line `.pb`, channel 0 | `4096 / 4096` | `4096` | `4096` | `0.9160840541070369` | `0.001388818455867956` | `0.0015160382386763867` | `0.0021275877952575684` | `0.0031310319900512695` |

## Current North Line CLEAN Diagnostic

Source-first CASA C++ tracing was added for the North line CLEAN path after the
initial `casa-rs` restored-image comparison showed source-position differences
at the tens-of-percent level. The traced CASA path is:

- `_gclean.py` derives `cyclethreshold = max(threshold, peakresidual *
  clamp(maxpsfsidelobe * cyclefactor, minpsffraction, maxpsffraction))`.
- `SIImageStore::getPSFSidelobeLevel()` computes `maxpsfsidelobe` from
  `max(abs(min(psf)), abs(max(psf - fitted_gaussian_psf)))` over all cube
  planes.
- `SDAlgorithmBase::deconvolve()` passes the fixed cube `CycleThreshold` into
  each channel's Hogbom minor cycle.
- `hclean.f` searches peaks in y-major/x-minor order with strict `GT` and uses
  an inclusive `do iter=siter,niter` loop, so a `CycleNiter=32` channel can
  place 33 components while reporting `iterdone=32`.

Fresh run stamp: `20260502-193341`.

| Controller value | CASA C++ | casa-rs |
|---|---:|---:|
| dirty cube global peak | `0.823116` | `0.8245545` |
| max PSF sidelobe | `0.302028` | `0.3022569` |
| cycle threshold | `0.2486043` | `0.2492273` |
| cleaned output channels | `37..49` | `37..49` |

The current fresh artifacts are under:

- `target/wave6-issue161-antennae/line-full-20260502-193341/`
- `fresh-line-clean-metrics-20260502-193341.json`
- `rust-north-line-clean-20260502-193341.log`
- `rust-hogbom-components-20260502-193341-trace.jsonl`

Representative fresh panels with labeled colorbars:

- `antennae-north-line-clean-image-ch37-20260502-193341-fresh-panel.png`
- `antennae-north-line-clean-image-ch42-20260502-193341-fresh-panel.png`
- `antennae-north-line-clean-image-ch45-20260502-193341-fresh-panel.png`
- `antennae-north-line-clean-image-ch47-20260502-193341-fresh-panel.png`
- `antennae-north-line-clean-residual-ch47-20260502-193341-fresh-panel.png`
- `antennae-north-line-clean-model-ch45-20260502-193341-fresh-panel.png`

Current line CLEAN restored-image source-region metrics:

| Channel | source p90 abs(diff)/peak | source max abs(diff)/peak | RMS/peak |
|---:|---:|---:|---:|
| 37 | `0.4665%` | `7.63%` | `4.45%` |
| 39 | `1.23%` | `4.58%` | `1.12%` |
| 42 | `0.167%` | `2.47%` | `1.24%` |
| 45 | `0.294%` | `2.78%` | `1.85%` |
| 47 | `1.38%` | `5.85%` | `3.50%` |
| 49 | `0.371%` | `8.23%` | `3.71%` |

This is a major improvement over the earlier source-position differences, but
it is not yet sufficient to close #161. The remaining mismatch is now localized
to CLEAN/model/residual evolution, not PB masking, channel selection, or uv
continuum subtraction. Component traces match CASA through the early and
dominant Hogbom components; later low-level peak ordering diverges in some
channels after sub-percent dirty/PSF differences have accumulated.

## Tutorial Figure Coverage

| CASA Guide visible product | Current #161 status | Evidence / blocker |
|---|---|---|
| North and South `plotms` amplitude vs channel plots | Inventoried, not reproduced in this #161 pass | Requires plot-data export/panel parity for this dataset rather than imager capability. |
| North dirty continuum image | Reproduced with panel and metrics | Source p90 `0.049%`, source max `0.112%` of CASA peak after matching CASA's HetArray mosaic oversampling. |
| North and South continuum CLEAN residual images | Reproduced with panels and metrics | North source p90 `0.058%`; South source p90 `0.099%`. South residual has a localized source-region max outlier of `3.20%`. |
| North and South continuum restored images | Reproduced with panels and metrics | North source p90 `0.040%`; South source p90 `0.060%`. |
| Line velocity-selection plots | Inventoried, not reproduced in this #161 pass | Downstream line cube workflow is not claimed yet. |
| Interactive clean-mask screenshots | Inventoried, not reproduced in this #161 pass | The runner uses bounded noninteractive CLEAN for reproducible evidence. |
| CO(3-2) line cube images | Partially probed, not full tutorial parity | A bounded North two-channel dirty cube now runs with `specmode='cube'`, `gridder='mosaic'`, per-channel weight density, and PB products. The dirty image source p90 is `0.096%` and source max is `0.161%` of CASA peak; this still does not cover full line CLEAN, `restoringbeam='common'`, or `savemodel='modelcolumn'`. |
| Selfcal channel/model plots and phase calibration plots | Not claimed | Depends on the tutorial line-cube model column and selfcal/applycal chain. |
| Selfcal comparison images | Not claimed | Depends on the line cube/selfcal chain. |
| Contour and moment-map products | Not claimed | Depends on line cube products, then `immoments` and contour-panel generation. |
| FITS exports | Not claimed | Depends on completing the line cube, moment, and continuum export manifest. |

## Current Interpretation

This pass adds the #161 dataset registry entries, a reproducible Antennae
continuum panel harness, and a bounded line-cube mosaic probe. It demonstrates
that the already-landed mosaic MFS path can reproduce the tutorial's North and
South continuum CLEAN image products from same-input CASA C++ and casa-rs runs
with sub-percent source-region p90 differences. It also demonstrates that the
frontend can now keep `gridder='mosaic'` through cube channel preparation and
write CASA-shaped `.image`, `.pb`, `.weight`, `.psf`, `.residual`, and `.sumwt`
cube products for the Antennae line selection.

The latest correctness fix matched casa-rs HetArray mosaic convolution
oversampling to the CASA C++ default of 10. C++ instrumentation on the bounded
line probe reported `conv_support=6` and `conv_sampling=10`; the earlier
casa-rs path used `sampling=79` for 64-pixel images, which produced the
PSF-shaped few-percent dirty-image differences.

It does not close #161. The remaining #161 blockers are the full line CLEAN
workflow and downstream selfcal/moment/contour/FITS products. The next
implementation step is to move beyond the dirty cube probe by supporting the
tutorial line CLEAN details, especially common restoring-beam behavior and
`savemodel='modelcolumn'`, then regenerate the selfcal, moment, contour, and
FITS products in the same panel format.
