# Wave 6 Issue 169 VLA 3C391

Truth class: current evidence
Last reality check: 2026-05-03
Verification:
- `bash -n scripts/run-wave6-issue169-3c391.sh`
- `cargo test -p casa-test-support tutorial_dataset_registry_contains_first_wave_candidates`
- `scripts/run-wave6-issue169-3c391.sh target/wave6-issue169-3c391`

Wave issue: #143
Child issue: #169

## Purpose

Issue #169 is the Wave 6 VLA 3C391 continuum mosaic breadth slice. It keeps the
3C391 proof separate from full Wave 6 closeout and reuses the #53 mosaic
comparison path for the official final-calibrated 3C391 mosaic MS.

The reproducible runner is:

```sh
scripts/run-wave6-issue169-3c391.sh target/wave6-issue169-3c391
```

Set `CASA_RS_FETCH_TUTORIAL_DATA=1` to download missing official tutorial data.
Each run writes stamped panel filenames and a machine-readable summary at:

```text
target/wave6-issue169-3c391/wave6-issue169-summary.json
```

Each reproduced image panel contains four columns:

- CASA Guide figure from the tutorial page
- CASA C++ product generated locally from the staged tutorial data
- casa-rs product generated locally from the same staged tutorial data
- `casa-rs - CASA C++` difference image

Image colorbars are labeled in Jy/beam, masked pixels render in gray, and
metrics are computed on the shared valid CASA C++ and casa-rs image support.

## Tutorial Inputs

The issue body named a CASA 6.7.2 guide URL. The live guide currently resolves
to the CASA 6.4.1 3C391 page, which says it was checked with CASA 6.5.4. The
local registry records the live source page used for the current evidence.

| Dataset | Tutorial source | Local data product | Registry key | Tier |
|---|---|---|---|---|
| 3C391 raw 10s SPW0 MS | <https://casaguides.nrao.edu/index.php?title=VLA_Continuum_Tutorial_3C391-CASA6.4.1> | `tutorial-parity/vla/3c391/3c391_ctm_mosaic_10s_spw0.ms.tgz` | `vla/3c391/raw-10s-spw0` | slow-parity |
| 3C391 final calibrated mosaic MS | <https://casaguides.nrao.edu/index.php?title=VLA_Continuum_Tutorial_3C391-CASA6.4.1> | `tutorial-parity/vla/3c391/EVLA_3C391_FinalCalibratedMosaicMS/3c391_ctm_mosaic_spw0.ms` | `vla/3c391/final-calibrated-mosaic-ms` | tutorial-parity |

## Current Results

The runner uses the official final-calibrated mosaic MS and the tutorial
multiscale mosaic shape:

```text
tclean(gridder='mosaic', deconvolver='multiscale', scales=[0,5,15,45],
       niter=500, gain=0.1, threshold='1.0mJy',
       imsize=[480,480], cell='2.5arcsec',
       weighting='briggs', robust=0.5, pbcor=True)
```

Current run stamp: `20260503T080756-0600`, generated
`2026-05-03 08:07:56 -0600`.

Wall-clock timing on the local tutorial run:

| Runner | Seconds | Relative |
|---|---:|---:|
| CASA C++ | `75.489` | `1.00x` |
| casa-rs | `257.494` | `3.41x CASA` |

### Image Metrics

Source-region peak-relative differences use shared valid pixels with
`abs(CASA) >= 25%` of the CASA peak.

| Panel | Shared valid pixels | CASA valid pixels | casa-rs valid pixels | Rust-only valid | CASA-only valid | CASA RMS | Difference RMS | Diff/CASA RMS | source p90 abs(diff)/peak | source max abs(diff)/peak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `.residual` | `115741 / 230400` | `115741` | `115869` | `128` | `0` | `0.0033734054058476024` | `0.00017944521708647393` | `0.05319408594514495` | `0.027870328200300703` | `0.08864399900146921` |
| `.image` | `115741 / 230400` | `115741` | `115869` | `128` | `0` | `0.009522959519475125` | `0.0001280453840608434` | `0.013445965384918579` | `0.004870145958775808` | `0.008004669772717498` |
| `.image.pbcor` | `115741 / 230400` | `115741` | `115869` | `128` | `0` | `0.011692938714363256` | `0.0002582965000849592` | `0.022089955860939862` | `0.004811594357833278` | `0.007869195048201764` |

The restored-image and PB-corrected restored-image source-region p90 and max
differences are below the sub-percent Wave 6 signoff threshold for #53-derived
mosaic products. The residual panel is retained as a diagnostic product; its
peak-normalized residual differences are larger because the residual peak is
much smaller than the restored-image peak.

### Inspectable Panels

- `target/wave6-issue169-3c391/vla-3c391-multiscale-residual-20260503T080756-0600-panel.png`
- `target/wave6-issue169-3c391/vla-3c391-multiscale-image-20260503T080756-0600-panel.png`
- `target/wave6-issue169-3c391/vla-3c391-multiscale-image.pbcor-20260503T080756-0600-panel.png`
- `target/wave6-issue169-3c391/wave6-issue169-summary.json`

## Tutorial Figure Coverage

| CASA Guide visible product | Current #169 status | Evidence / blocker |
|---|---|---|
| Antenna layout screenshot | External display artifact | Does not exercise casa-rs calculation; inventoried for guide completeness. |
| Calibrator and target `plotms` visibility plots | Inventoried, not reproduced in this imaging pass | Raw calibration/diagnostic plot breadth is staged by the raw archive and remains separate from the final-calibrated mosaic image proof. |
| Delay, bandpass, gain, fluxscale, corrected calibrator `plotms`/`plotcal` figures | Explicitly deferred | Requires raw `setjy`, `gencal`, `gaincal`, `bandpass`, `fluxscale`, `applycal`, `statwt`, and plotting replay. The raw archive is registered as slow parity for that follow-up. |
| Final-calibrated mosaic amplitude vs UV wavelength plot | Inventoried, not claimed as exact `uvwave` parity | Existing `msexplore` parity covers `uvdist` and related axes; exact `plotms` `uvwave` display parity is not added in this #169 imaging slice. |
| Multiscale restored image after 500 iterations | Reproduced with panel and metrics | Same-input CASA C++ and casa-rs products are generated from the official final-calibrated mosaic MS. Source p90 `0.676%`, source max `0.830%` of CASA peak. |
| Multiscale residual image after 500 iterations | Reproduced with panel and metrics | Same-input CASA C++ and casa-rs `.residual` products are generated by the #169 runner. Source p90 is `2.787%` of the residual peak, so this remains a diagnostic rather than the restored-image correctness threshold. |
| PB-corrected restored image | Reproduced with panel and metrics | Source p90 `0.481%`, source max `0.787%` of CASA peak. The current run has `128` rust-only valid edge pixels and `0` CASA-only valid pixels. |
| Interactive clean-mask and polygon region screenshots | Interactive display artifacts | The runner uses noninteractive CLEAN for reproducible comparison and computes numeric image statistics on shared valid support. |

## Current Interpretation

This closes the #169 imaging breadth target: the 3C391 final-calibrated mosaic
MS is registered, staged, imaged through CASA C++ and casa-rs with the same
tutorial mosaic command shape, and produces human-reviewable original/CASA/Rust
difference panels with source-region differences below 1% of the CASA peak.

The raw calibration replay is intentionally classified as slow parity rather
than claimed here. It is the right next place to implement VLA calibration
breadth, but it is not required to prove that the tutorial final-calibrated
mosaic image products can be duplicated.
