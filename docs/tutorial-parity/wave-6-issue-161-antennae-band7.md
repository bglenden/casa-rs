# Wave 6 Issue 161 Antennae Band 7

Truth class: current evidence
Last reality check: 2026-05-02
Verification:
- `bash -n scripts/run-wave6-issue161-antennae.sh`
- `cargo check -p casars-imager`
- `cargo test -p casars-imager pbcor_products_apply_primary_beam_cutoff`
- `cargo test -p casars-imager cube_pb_product_normalizes_each_channel_plane`
- `cargo test -p casa-test-support tutorial_dataset_registry_contains_first_wave_candidates`
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

Current run stamp: `20260502T174920-0600`, generated
`2026-05-02 17:49:20 -0600`.

The continuum run completed against local CASA C++ and casa-rs products. The
runner uses the tutorial mosaic geometry for North and South continuum imaging
and a bounded noninteractive `niter=32` proof run for CLEAN products. The CASA
Guide uses interactive cleaning and `niter=1000`; those exact interactive mask
flows are not claimed by this continuum proof.

Wall-clock timing on the local tutorial run:

| Runner | Seconds | Relative |
|---|---:|---:|
| CASA C++ | `42.621` | `1.00x` |
| casa-rs | `196.387` | `4.61x CASA` |

The bounded North two-channel dirty line-cube probe took `3.343s` in CASA C++
and `3.423s` in casa-rs.

### Continuum Metrics

Source-region peak-relative differences use shared valid pixels with
`abs(CASA) >= 25%` of the CASA peak.

| Panel | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | Rust-only valid | CASA-only valid | CASA RMS | Difference RMS | Diff/CASA RMS | source p90 abs(diff)/peak | source max abs(diff)/peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| North dirty `.image` | `56562 / 90000` | `56562` | `56569` | `7` | `0` | `0.0005037589748977789` | `0.00003076951378478472` | `0.0610798324556468` | `0.013065663439904404` | `0.041348320178813285` |
| North clean `.residual` | `134322 / 250000` | `134322` | `134378` | `56` | `0` | `0.0004842421539966355` | `0.00000042211890958799533` | `0.0008717103748694464` | `0.0005840204276983746` | `0.001556398039597241` |
| North clean `.image` | `134322 / 250000` | `134322` | `134378` | `56` | `0` | `0.0004930845920012612` | `0.00000042435802932331366` | `0.0008606191233860907` | `0.0003973664956661594` | `0.0007032833744921095` |
| South clean `.residual` | `174195 / 562500` | `174199` | `174222` | `27` | `4` | `0.00046585536101634024` | `0.0000013179985508508814` | `0.002829201209524455` | `0.0009942126057312414` | `0.03202537645946853` |
| South clean `.image` | `174195 / 562500` | `174199` | `174222` | `27` | `4` | `0.00047276336698618066` | `0.0000010499899183344412` | `0.0022209629418371017` | `0.0005966732729303848` | `0.0028014619149785915` |

### Inspectable Panels

- `target/wave6-issue161-antennae/alma-antennae-north-cont-dirty-image-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-cont-clean-residual-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-cont-clean-image-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-south-cont-clean-residual-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-south-cont-clean-image-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-line-dirty-probe-image-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/alma-antennae-north-line-dirty-probe-pb-20260502T174920-0600-panel.png`
- `target/wave6-issue161-antennae/wave6-issue161-summary.json`

## Current Line-Cube Probe

This pass adds a bounded North CO(3-2) dirty line-cube probe for the tutorial
command shape that first forced the remaining #161 work: `specmode='cube'`,
`gridder='mosaic'`, two selected channels, and PB products. This is not the
full tutorial line CLEAN or selfcal sequence.

| Probe product | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | CASA RMS | Difference RMS | Diff/CASA RMS | source p90 abs(diff)/peak | source max abs(diff)/peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| North dirty line `.image`, channel 0 | `4096 / 4096` | `4096` | `4096` | `0.007540386763608376` | `0.00043732891898679596` | `0.057998207876742466` | `0.0278877279949779` | `0.0675429373295499` |
| North dirty line `.pb`, channel 0 | `4096 / 4096` | `4096` | `4096` | `0.9160840541070369` | `0.00002234636227047943` | `0.000024393353612362345` | `0.00003415346145629883` | `0.000050187110900878906` |

## Tutorial Figure Coverage

| CASA Guide visible product | Current #161 status | Evidence / blocker |
|---|---|---|
| North and South `plotms` amplitude vs channel plots | Inventoried, not reproduced in this #161 pass | Requires plot-data export/panel parity for this dataset rather than imager capability. |
| North dirty continuum image | Reproduced with panel and metrics | Difference is larger than the CLEAN products: source p90 `1.31%`, source max `4.13%` of CASA peak. |
| North and South continuum CLEAN residual images | Reproduced with panels and metrics | North source p90 `0.058%`; South source p90 `0.099%`. South residual has a localized source-region max outlier of `3.20%`. |
| North and South continuum restored images | Reproduced with panels and metrics | North source p90 `0.040%`; South source p90 `0.060%`. |
| Line velocity-selection plots | Inventoried, not reproduced in this #161 pass | Downstream line cube workflow is not claimed yet. |
| Interactive clean-mask screenshots | Inventoried, not reproduced in this #161 pass | The runner uses bounded noninteractive CLEAN for reproducible evidence. |
| CO(3-2) line cube images | Partially probed, not full tutorial parity | A bounded North two-channel dirty cube now runs with `specmode='cube'`, `gridder='mosaic'`, per-channel weight density, and PB products. The PB probe matches very tightly; the dirty image still has few-percent source-region differences and does not cover full line CLEAN, `restoringbeam='common'`, or `savemodel='modelcolumn'`. |
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

It does not close #161. The remaining #161 blockers are the full line CLEAN
workflow and downstream selfcal/moment/contour/FITS products. The next
implementation step is to move beyond the dirty cube probe by supporting the
tutorial line CLEAN details, especially common restoring-beam behavior and
`savemodel='modelcolumn'`, then regenerate the selfcal, moment, contour, and
FITS products in the same panel format.
