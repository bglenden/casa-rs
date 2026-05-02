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

| Panel | CASA RMS | casa-rs RMS | Difference RMS | Difference max abs |
|---|---:|---:|---:|---:|
| `alma-antennae-north-cont-clean-residual-panel.png` | `0.00035494916856413186` | `0.0005994379285155482` | `0.0004622679601228216` | `0.0030038568656891584` |
| `alma-antennae-north-cont-clean-image.pbcor-panel.png` | `0.0007992851459403191` | `0.0009238644494517046` | `0.0008469827166257012` | `0.014529683627188206` |

Inspectable artifacts:

- `target/wave6-issue53-mosaic-panels/alma-antennae-north-cont-clean-residual-panel.png`
- `target/wave6-issue53-mosaic-panels/alma-antennae-north-cont-clean-image.pbcor-panel.png`
- `target/wave6-issue53-mosaic-panels/wave6-issue53-mosaic-panel-summary.json`

### VLA 3C391

The 3C391 run completed against the official final-calibrated mosaic MS. It
uses `gridder='mosaic'`, `deconvolver='multiscale'`, scales `[0, 5, 15, 45]`,
Briggs `robust=0.5`, `niter=500`, `threshold='1.0mJy'`, and PB-corrected
output. The casa-rs run reported `32039616` gridded samples, `7` major cycles,
and `41` minor iterations before the divergence guard stopped the run.

| Panel | CASA RMS | casa-rs RMS | Difference RMS | Difference max abs |
|---|---:|---:|---:|---:|
| `vla-3c391-multiscale-image-panel.png` | `0.00674954915267406` | `0.006080412428176494` | `0.0021897177505491422` | `0.015739932656288147` |
| `vla-3c391-multiscale-image.pbcor-panel.png` | `0.008287547894255964` | `0.0062712715527269425` | `0.003559600383447083` | `0.02817557007074356` |

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
  `--pblimit` value.
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
