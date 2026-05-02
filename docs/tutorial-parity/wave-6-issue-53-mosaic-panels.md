# Wave 6 Issue 53 Mosaic Panels

Truth class: current evidence
Last reality check: 2026-05-02
Verification:
- `cargo test -p casa-imaging mosaic_clean_reduces_residual_peak_and_tracks_pb_weight_image`
- `cargo test -p casars-imager cli_parses_weighting_mask_and_wterm`
- `cargo test -p casars-imager pbcor_products_apply_primary_beam_cutoff`
- `cargo test -p casars-imager frequency_range_hz_accepts_descending_spectral_windows`
- `cargo test -p casars-imager selected_weight_spectrum_missing_cells_fall_back_to_weight`
- `cargo test -p casars-imager task_contract`
- `CASA_RS_WAVE6_DATASET=alma scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels`
- `CASA_RS_WAVE6_DATASET=vla scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels`
- `CASA_RS_WAVE6_SKIP_CASA=1 scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels`

## Purpose

Issue #53 is the Wave 6 mosaic-first tclean capability ticket. Its visible
proof must not be control-surface plumbing alone: the repository needs tutorial
data products that can be inspected beside CASA Guide figures and CASA C++
products.

The reproducible panel runner is:

```sh
scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels
```

Set `CASA_RS_FETCH_TUTORIAL_DATA=1` to download missing official tutorial data.
Set `CASA_RS_WAVE6_DATASET=alma` or `CASA_RS_WAVE6_DATASET=vla` to run one
dataset while preserving the same panel format.

Each panel contains four columns:

- CASA Guide figure from the tutorial page
- CASA C++ product generated locally
- casa-rs product generated locally
- `casa-rs - CASA C++` difference image

The CASA C++ products apply the primary-beam support mask to `.image`,
`.residual`, `.pb`, and `.image.pbcor` products. The casa-rs writer now attaches
the same kind of default image mask using its own `pb > --pblimit` support. Panel
statistics are computed only on the shared valid support mask, and masked pixels
are rendered in gray. The panel images include labeled colorbars plus the RMS,
difference, valid-pixel, and wall-clock timing metrics needed for visual review.

## Tutorial Inputs

| Dataset | Tutorial source | Local data product | Products compared |
|---|---|---|---|
| ALMA Antennae Band 7 North | <https://casaguides.nrao.edu/index.php/AntennaeBand7_Imaging_6.6.6> | `Antennae_Band7_CalibratedData/Antennae_North.cal.ms` | `.residual`, `.image.pbcor` |
| VLA 3C391 continuum mosaic | <https://casaguides.nrao.edu/index.php/VLA_Continuum_Tutorial_3C391-CASA6.7.2> and <https://casaguides.nrao.edu/index.php/Advanced_Topics_3C391_-_CASA4.1> | `VLA_3C391_FinalCalibratedMosaicMS/3c391_ctm_mosaic_spw0.ms` | `.image`, `.image.pbcor` |

The ALMA archive is downloaded from:

<https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_CalibratedData.tgz>

The VLA imaging-input archive is downloaded from:

<https://casa.nrao.edu/Data/EVLA/3C391/EVLA_3C391_FinalCalibratedMosaicMS.tgz>

The VLA final-calibrated mosaic MS is used because issue #53 is the imager
capability ticket. The raw `3c391_ctm_mosaic_10s_spw0.ms.tgz` path exercises
upstream calibration and split/statwt work before it reaches the tclean mosaic
surface.

## Current Results

### ALMA Antennae

The Antennae North run completed against local CASA C++ and casa-rs products.
It uses the tutorial SPW selection `0:1~50;120~164`, `gridder='mosaic'`,
`niter=32`, `threshold='0.4mJy'`, and PB-corrected output. The casa-rs run
reported `7058235` gridded samples, `5` major cycles, and `32` minor
iterations. The residual panel uses the tutorial residual screenshot as the
original reference column. The PB-corrected image panel uses the tutorial
continuum-image figure as the original visual reference because the guide does
not publish a separate continuum `.image.pbcor` figure.

Wall-clock timing on the local tutorial run:

| Runner | Seconds | Relative |
|---|---:|---:|
| CASA C++ | `13.673` | `1.00x` |
| casa-rs | `40.073` | `2.93x CASA` |

| Panel | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | CASA RMS | casa-rs RMS | Difference RMS | Diff/CASA RMS | Difference max abs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `alma-antennae-north-cont-clean-residual-panel.png` | `134322 / 250000` | `134322` | `242602` | `0.0004842421539966355` | `0.0005555694340461607` | `0.0001939154632399948` | `0.4004514304249153` | `0.0013848665403202176` |
| `alma-antennae-north-cont-clean-image.pbcor-panel.png` | `134322 / 250000` | `134322` | `242602` | `0.0010904309546444929` | `0.0007543814432682847` | `0.0005618691211086539` | `0.5152725339605174` | `0.005424286471679807` |

Source-region peak-relative differences use shared valid pixels with
`abs(CASA) >= 25%` of the CASA peak.

| Panel | CASA peak abs | abs(diff at CASA peak) / peak | source p90 abs(diff) / peak | source max abs(diff) / peak |
|---|---:|---:|---:|---:|
| `alma-antennae-north-cont-clean-residual-panel.png` | `0.001685870112851262` | `0.32450543499802575` | `0.22181278835900722` | `0.8214550633310854` |
| `alma-antennae-north-cont-clean-image.pbcor-panel.png` | `0.008220789022743702` | `0.4258877728062916` | `0.30440361424968043` | `0.6598255297238418` |

Inspectable artifacts:

- `target/wave6-issue53-mosaic-panels/alma-antennae-north-cont-clean-residual-panel.png`
- `target/wave6-issue53-mosaic-panels/alma-antennae-north-cont-clean-image.pbcor-panel.png`
- `target/wave6-issue53-mosaic-panels/wave6-issue53-mosaic-panel-summary.json`

### VLA 3C391

The 3C391 run completed against the official final-calibrated mosaic MS. It
uses `gridder='mosaic'`, `deconvolver='multiscale'`, scales `[0, 5, 15, 45]`,
Briggs `robust=0.5`, `niter=500`, `threshold='1.0mJy'`, and PB-corrected
output. The casa-rs run reported `32039616` gridded samples, `7` major cycles,
and `41` minor CLEAN iterations/components before the divergence guard stopped
the run. This is distinct from the requested CASA-style `niter=500` component
limit, which the guard stops before exhausting.

Wall-clock timing on the local tutorial run:

| Runner | Seconds | Relative |
|---|---:|---:|
| CASA C++ | `72.850` | `1.00x` |
| casa-rs | `148.957` | `2.04x CASA` |

| Panel | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | CASA RMS | casa-rs RMS | Difference RMS | Diff/CASA RMS | Difference max abs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `vla-3c391-multiscale-image-panel.png` | `115741 / 230400` | `115741` | `223251` | `0.009522959519475125` | `0.0084792279050309` | `0.0028014254769549185` | `0.29417593041593904` | `0.015739932656288147` |
| `vla-3c391-multiscale-image.pbcor-panel.png` | `115741 / 230400` | `115741` | `223251` | `0.011692938714363256` | `0.008668273808957848` | `0.004698084600377237` | `0.40178818303445396` | `0.02817557007074356` |

Source-region peak-relative differences use shared valid pixels with
`abs(CASA) >= 25%` of the CASA peak.

| Panel | CASA peak abs | abs(diff at CASA peak) / peak | source p90 abs(diff) / peak | source max abs(diff) / peak |
|---|---:|---:|---:|---:|
| `vla-3c391-multiscale-image-panel.png` | `0.13652297854423523` | `0.11391662997486766` | `0.0885403161919872` | `0.11529145367413884` |
| `vla-3c391-multiscale-image.pbcor-panel.png` | `0.15053795278072357` | `0.1871658910612703` | `0.12656380182156154` | `0.1871658910612703` |

Inspectable artifacts:

- `target/wave6-issue53-mosaic-panels/vla-3c391-multiscale-image-panel.png`
- `target/wave6-issue53-mosaic-panels/vla-3c391-multiscale-image.pbcor-panel.png`
- `target/wave6-issue53-mosaic-panels/wave6-issue53-mosaic-panel-summary.json`

## Current Interpretation

This lands the tutorial-data proof for #53:

- `gridder='mosaic'` now runs with `niter > 0` instead of stopping at dirty-only
  products.
- The product writer emits `.weight`, `.pb`, and optional `.image.pbcor` for
  mosaic MFS runs.
- PB correction uses explicit cutoff semantics through the existing
  `--pblimit` value, and the CASA image products now carry default masks for
  the PB support region.
- Descending-frequency SPWs, as present in the Antennae dataset, now produce an
  ordered positive frequency range for imaging requests.
- Present-but-missing `WEIGHT_SPECTRUM` cells, as present in the 3C391 MS, now
  fall back to scalar `WEIGHT` instead of aborting the imager.

The current mosaic CLEAN implementation uses image-domain PSF residual
refreshes. The generated diagnostics intentionally warn that exact
direction-dependent visibility-domain major-cycle refresh remains a parity
limitation. The 3C391 run also triggers the divergence guard before using the
full requested `niter=500`. Both limitations are visible in the difference
panels and should stay in the issue/PR evidence until a later parity tranche
replaces the image-domain refresh path with the visibility-domain major-cycle
semantics.

The casa-rs PB support mask is broader than CASA C++ for both tutorial products
at the same nominal `pblimit=0.1`. The panel metrics therefore report both
CASA-valid and casa-rs-valid pixel counts and compute image differences on their
shared valid support. This keeps the visual comparison honest while preserving
the PB-shape/support mismatch as explicit follow-up evidence.
