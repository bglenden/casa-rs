# Wave 6 Issue 163 M100 Band 3 Combine Evidence

Truth class: current descriptive
Last reality check: 2026-05-05
Verification: `CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data scripts/run-wave6-issue163-m100-combine.sh`; `CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data scripts/run-wave6-issue163-m100-raw-preflight.sh`; `CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data scripts/run-wave6-issue163-m100-split-parity.sh /Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/alma/m100/band3-combine/work/split-parity-20260504T132927Z`; `cargo test -p casa-ms --test transform`; `cargo test -p casa-images --lib -- image_analysis_schema_and_ui_surfaces_advertise_task_contracts`; `cargo test -p casa-imaging alma_aca_airy_voltage_uses_wide_casa_support`; `cargo test -p casa-imaging hetarray_screen_size_follows_casa_support_scale`; `cargo test -p casa-imaging mosaic`; `cargo build --release -p casars-imager --bin casars-imager`; `MPLCONFIGDIR=target/wave6-issue163-combine-70chan-floatround-compare-20260505T153447Z/matplotlib /Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/compare-wave6-issue163-raw-dirty.py target/wave6-issue163-combine-70chan-floatround-compare-20260505T153447Z --prefix M100_combine_CO_cube_dirty_70chan --panel-channels 0,1,10,26,35,61`

Wave issue: #143
Child issue: #163

This slice covers the image-domain products from the M100 Band 3 data
combination tutorial using the official ALMA science-verification reference
image archive:

- registry key: `alma/m100/band3-combine/reference-images`
- artifact: `M100_Band3_DataComb_ReferenceImages_5.1.tgz`
- SHA-256:
  `04e3e88f1393e93c18eab7fd4a9ae5c57e768dbb8be85259c3006ae9d4c7634b`

The runner writes CASA Guide / CASA C++ / casa-rs / difference panels with
labeled colorbars and numeric residual metrics.

## Current Evidence

Image-domain artifacts from the latest local run:

- summary JSON:
  `target/wave6-issue163-m100-combine-20260504T165138Z/wave6-issue163-summary.json`
- PB subimage panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/pb-subim-panel.png`
- 12m+7m masked moment-0 PB-corrected panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/combine-mom0-pbcor-panel.png`
- 12m+7m masked moment-1 panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/combine-mom1-panel.png`
- TP regrid panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/tp-regrid-panel.png`
- feathered PB-corrected channel panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/feather-pbcor-chan26-panel.png`
- feathered cube panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/feather-cube-panel.png`
- feather moment-0 panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/feather-mom0-panel.png`
- feather moment-1 panel:
  `target/wave6-issue163-m100-combine-20260504T165138Z/feather-mom1-panel.png`

Same-input CASA C++ versus casa-rs metrics from that run:

| Product | Shape | Mask mismatches | p99 abs diff | p99 diff / CASA peak | Max diff / CASA peak |
|---|---:|---:|---:|---:|---:|
| `M100_combine_CO_cube.pb.subim` | `[394, 432, 1, 70]` | 0 | 0.0 | 0.0 | 0.0 |
| `M100_combine_CO_cube.image.mom0.pbcor` | `[800, 800, 1, 1]` | 0 | 0.0 | 0.0 | 0.0 |
| `M100_combine_CO_cube.image.mom1` | `[800, 800, 1, 1]` | 0 | 0.0 | 0.0 | 0.0 |
| `M100_TP_CO_cube.regrid` | `[800, 800, 1, 70]` | 1,615,123 | 0.001099 | 0.0001249 | 0.0002707 |
| `M100_Feather_CO.image` | `[394, 432, 1, 70]` | 0 | 0.0001279 | 0.0001760 | 0.0003457 |
| `M100_Feather_CO.image.pbcor` | `[394, 432, 1, 70]` | 0 | 0.0001396 | 0.0001868 | 0.0003403 |
| `M100_Feather_CO.image.mom0` | `[394, 432, 1, 1]` | 0 | 0.01965 | 0.0004186 | 0.0004771 |
| `M100_Feather_CO.image.mom1` | `[394, 432, 1, 1]` | 0 | 571.55 | 0.0001303 | 2.8147 |

Raw-input preflight artifacts from the latest local run:

- raw preflight Markdown:
  `target/wave6-issue163-m100-raw-preflight-20260504T024950Z/raw-preflight.md`
- raw preflight JSON:
  `target/wave6-issue163-m100-raw-preflight-20260504T024950Z/raw-preflight-summary.json`
- exact tutorial task sequence JSON:
  `target/wave6-issue163-m100-raw-preflight-20260504T024950Z/tutorial-sequence.json`
- CASA `listobs` outputs for the extracted 12m and 7m MS:
  `target/wave6-issue163-m100-raw-preflight-20260504T024950Z/m100_12m_ms.listobs`
  and
  `target/wave6-issue163-m100-raw-preflight-20260504T024950Z/m100_7m_ms.listobs`
- casa-rs `msexplore` JSON summary for the extracted 12m MS:
  `target/wave6-issue163-raw-preflight-20260503T235049Z/m100-12m-msexplore-summary.json`
- CASA-vs-casa-rs split parity report:
  `/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/alma/m100/band3-combine/work/split-parity-20260504T132927Z/split-parity.md`
- CASA-vs-casa-rs split parity JSON:
  `/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/alma/m100/band3-combine/work/split-parity-20260504T132927Z/split-parity-summary.json`

Raw-input status from that run:

| Input | Status |
|---|---|
| 12m calibrated MS | archive verified with SHA-256 `cc44820a6f1b6262b909aade2f9c13eafa4956e7de34949e8e0599a2adac35d1`; extracted; 273,441 MAIN rows, 48 FIELD rows, 4 SPWs, 23 ANTENNA rows, first `DATA` cell shape `[2, 3840]`; casa-rs `msexplore` row-referenced summary reports 47 fields, 22 antennas plus 23 ANTENNA-table rows, and 204 scans |
| 7m calibrated MS | extracted; 177,120 MAIN rows, 24 FIELD rows, 6 SPWs, 10 ANTENNA rows, first `DATA` cell shape `[2, 4080]` |
| TP guide image `M100_TP_CO_cube.spw3.image.bl` | extracted from ACA reference archive verified with SHA-256 `e6bdeb95c2f2847f0e917c894d887f2ebe4803662981b4db71a2a1c6c54bf2ba`; shape `[90, 90, 1, 70]`, rest frequency `115271203999.99998 Hz` |
| TP calibrated-data archive | optional for this guide route when the ACA reference TP image is present; useful only for reproducing TP calibration rather than the data-combination tutorial itself |

The external GLENDENNING downloader for these archives is
`/Volumes/GLENDENNING/casa-rs/tutorial-data/download-wave6-datasets.sh`.
Running it with no arguments prints status only. Use `download issue-163` for
the #163 group or `download all-known` for every concrete Wave 6 URL currently
known to the downloader.

Latest observed GLENDENNING status during this pass:

| Archive | Status |
|---|---|
| `m100-12m` | complete, `15580494468/15580494468` bytes |
| `m100-7m` | complete, `9774558254/9774558254` bytes |
| `m100-tp` | partial, `13061938779/14372792248` bytes |
| `m100-aca-reference` | complete, `24775689/24775689` bytes |
| `m100-data-comb-reference` | complete, `411602337/411602337` bytes |

CASA `split` versus casa-rs `mstransform --no-keepflags` evidence:

| Product | CASA rows | casa-rs rows | Row delta | CASA first `DATA` shape | casa-rs first `DATA` shape | Remaining structural delta |
|---|---:|---:|---:|---|---|---|
| 12m CO split | 69,020 | 69,020 | 0 | `[2, 3840]` | `[2, 3840]` | none in checked row/DDID/SPW/DD dimensions |
| 7m CO split | 24,975 | 24,975 | 0 | `[2, 4080]` | `[2, 4080]` | none in checked row/DDID/SPW/DD dimensions |

Raw split same-input `tclean` probe evidence:

- CASA C++ trace and products:
  `target/wave6-issue163-debug-combine-instrumented-20260505T033309Z/`
- Latest casa-rs products and panels:
  `target/wave6-issue163-debug-combine-rustfix-20260505T041727Z/`
- Release-mode casa-rs products and panels:
  `target/wave6-issue163-debug-combine-release-20260505T043929Z/`
- Release-mode 70-channel casa-rs scale-up:
  `target/wave6-issue163-combine-release-70chan-20260505T044125Z/`
- Latest focused high-channel casa-rs products and panels after CASA-float
  Briggs lookup rounding:
  `target/wave6-issue163-debug-combine-highch-panel-floatround-20260505T145415Z/`
- Latest full 70-channel same-input comparison after CASA-float Briggs lookup
  rounding:
  `target/wave6-issue163-combine-70chan-floatround-compare-20260505T153447Z/`
- Latest full 70-channel casa-rs release-mode products:
  `target/wave6-issue163-combine-release-70chan-floatround-20260505T152523Z/`
- Product summary JSON:
  `target/wave6-issue163-debug-combine-rustfix-20260505T041727Z/combined-raw-dirty-summary.json`
- Release-mode product summary JSON:
  `target/wave6-issue163-debug-combine-release-20260505T043929Z/combined-raw-dirty-summary.json`
- Latest high-channel product summary JSON:
  `target/wave6-issue163-debug-combine-highch-panel-floatround-20260505T145415Z/combined-raw-dirty-summary.json`
- Grid-dump comparison:
  `target/wave6-issue163-debug-combine-rustfix-20260505T041727Z/grid-dump-comparison.md`

The same split 12m+7m inputs were imaged with a focused two-channel dirty cube
probe matching the tutorial's combined mosaic setup:
`gridder="mosaic"`, `weighting="briggsbwtaper"`, `robust=0.5`,
`perchanweightdensity=True`, `pblimit=0.2`, `niter=0`, `pbcor=True`,
`start="1725km/s"`, `width="5km/s"`, and `nchan=2`.

The last visible non-noise-like patch was traced to 12m MAIN row `31561`.
CASA's `BriggsCubeWeightor::getWeightUniform` casts channel-scaled `u,v` to
`Float` before rounding the lookup cell; this puts the row/channel on
`vcell=385`, where the per-channel density is zero. The Rust lookup used final
`f64` arithmetic and rounded to `384`, picking up a nonzero density. The Rust
Briggs lookup now uses CASA-like float rounding. In the targeted
`(470,385)` cell / `114606011019.5368 Hz` trace, the prior unmatched Rust
contribution `[-0.004422, -0.002717]` is gone: CASA has 2083 collapsed samples,
casa-rs has 2083 samples, and the greedy unmatched Rust count is 0.

| Product | Shape | Mask mismatches | p99 diff / CASA peak | Max diff / CASA peak | Notes |
|---|---:|---:|---:|---:|---|
| `.image` channel 0 | `[800, 800, 1, 2]` | 316 total image mask pixels | 0.002850 | 0.006066 | same as `.residual` for `niter=0` |
| `.image` channel 1 | `[800, 800, 1, 2]` | 316 total image mask pixels | 0.003269 | 0.007081 | same as `.residual` for `niter=0`; prior visible patch removed |
| `.image.pbcor` channel 0 | `[800, 800, 1, 2]` | 316 total PB-corrected mask pixels | 0.002613 | 0.008913 | panel has CASA, casa-rs, and difference colorbars |
| `.image.pbcor` channel 1 | `[800, 800, 1, 2]` | 316 total PB-corrected mask pixels | 0.002601 | 0.007850 | panel has CASA, casa-rs, and difference colorbars |
| `.pb` channel 0 | `[800, 800, 1, 2]` | 316 total PB mask pixels | 0.000923 | 0.001307 | PB support follows CASA mask extent |
| `.pb` channel 1 | `[800, 800, 1, 2]` | 316 total PB mask pixels | 0.001124 | 0.001480 | PB support follows CASA mask extent |
| `.psf` channel 0 | `[800, 800, 1, 2]` | 0 | 0.000036 | 0.000206 | sub-0.03% max PSF delta |
| `.psf` channel 1 | `[800, 800, 1, 2]` | 0 | 0.000032 | 0.000206 | sub-0.03% max PSF delta |
| `.weight` channel 0 | `[800, 800, 1, 2]` | 0 | 0.001838 | 0.002610 | sky-coverage image corrected from the earlier 14-16% mismatch |
| `.weight` channel 1 | `[800, 800, 1, 2]` | 0 | 0.001840 | 0.002612 | sky-coverage image corrected from the earlier 14-16% mismatch |
| `.sumwt` channel 0 | `[1, 1, 1, 2]` | 0 | `4.71e-6` | `4.71e-6` | CASA/casa-rs differ by `0.18359375` |
| `.sumwt` channel 1 | `[1, 1, 1, 2]` | 0 | `5.01e-7` | `5.01e-7` | CASA/casa-rs differ by `0.01953125` |

The full 70-channel raw dirty cube now runs with the tutorial spectral setup:
`start="1400km/s"`, `width="5km/s"`, `nchan=70`. The CASA C++ oracle is
`target/wave6-issue163-combine-casa70-20260505T045249Z/casa/`; the matching
casa-rs products are from the 2026-05-05 15:25:23Z release-mode rerun.

| Product | Shape | Mask mismatches | Worst-channel p99 diff / CASA peak | Worst-channel max diff / CASA peak | Notes |
|---|---:|---:|---:|---:|---|
| `.image` | `[800, 800, 1, 70]` | 9,654 | 0.004767 | 0.008886 | same as `.residual` for `niter=0` |
| `.image.pbcor` | `[800, 800, 1, 70]` | 9,654 | 0.003730 | 0.011152 | global max is at PB `0.2185`; inside the tutorial `PB>0.3` analysis region max is `0.007295` |
| `.pb` | `[800, 800, 1, 70]` | 9,654 | 0.001461 | 0.001750 | PB support and mask extent match CASA to small edge differences |
| `.psf` | `[800, 800, 1, 70]` | 0 | 0.0000538 | 0.000247 | PSF parity remains sub-0.03% |
| `.weight` | `[800, 800, 1, 70]` | 0 | 0.001835 | 0.002617 | sky-coverage image corrected from the earlier 10% class mismatch |
| `.sumwt` | `[1, 1, 1, 70]` | 0 | `1.3246e-5` | `1.3246e-5` | scalar sum-weight product matches to roundoff-scale relative error |

Selected full-cube visual panels with labeled colorbars were generated for
channels `0,1,10,26,35,61` under
`target/wave6-issue163-combine-70chan-floatround-compare-20260505T153447Z/`.
For example:

- `image-pbcor-chan26-panel.png`
- `image-pbcor-chan35-panel.png`
- `image-chan26-panel.png`
- `pb-chan35-panel.png`
- `psf-chan35-panel.png`
- `weight-chan35-panel.png`

Release-mode performance for the same two-channel casa-rs probe:

| Stage | Time |
|---|---:|
| total wall-clock reported by `casars-imager` | 33.757 s |
| `prepare_plane_input` | 20.027 s |
| 12m sample adaptation | 7.865 s |
| 7m sample adaptation, first chunk | 2.209 s |
| 7m sample adaptation, second chunk | 0.594 s |
| `run_imaging` | 13.629 s |
| `write_products` | 0.081 s |

The earlier 952.567 s debug build was therefore not representative of release
performance. The same release products reproduce the sub-percent CASA deltas in
the table above.

Release-mode performance for the latest 70-channel casa-rs scale-up:

| Stage | Time |
|---|---:|
| total wall-clock reported by `casars-imager` | 552.409 s |
| `prepare_plane_input` | 67.155 s |
| `run_imaging` | 479.390 s |
| `write_products` | 5.845 s |
| gridded samples | 19,620,351 |

This scale-up used the same split 12m+7m inputs and tutorial cube setup as the
CASA C++ oracle. It completed successfully in release mode and has a
same-input CASA 70-channel comparison with selected review panels.

The main corrected defects found by the instrumentation were:

- CASA builds the HetArray convolution screen from the larger of `imsize/10`
  and the primary-beam support diameter. casa-rs was using only `imsize/10`,
  which truncated the ALMA mosaic screen and pushed `.weight`/`.pb` errors to
  order 10%.
- CASA uses a support-12, oversampling-10 12m convolution function for this
  combined M100 probe. casa-rs now chooses the same support for the 12m groups;
  7m groups keep support 8, with the extra global CASA loop support landing on
  near-zero taps.
- The ALMA/ACA Airy table extent remains the wide CASA-compatible table used by
  the earlier #53 PB-mask fix; the support-screen size, not the voltage-table
  extent, was the missing piece for this #163 divergence.

## Implemented Surface

- `imsubimage` task binary and JSON task protocol.
- `imregrid` task binary and JSON task protocol for linear direction/spectral
  image-template regridding.
- `feather` task binary and JSON task protocol using CASA's Fourier-domain
  high-pass weighting and low-resolution beam scaling semantics.
- `immath` task binary and JSON task protocol for tutorial expressions
  `IM0 * IM1` and `IM0 / IM1`.
- `immoments` support for CASA Guide image-threshold mask expressions such as
  `M100_combine_CO_cube.pb>0.3`, combined with channel selection and
  `includepix` thresholds.
- Python task wrappers for `imsubimage`, `immath`, and the `immoments` mask
  parameter used by this tutorial.
- CASA-openable metadata preservation for `imsubimage`, `immath`, and
  `immoments` products derived from native CASA images.
- M100 reference-image dataset registry entry and repeatable evidence runner.
- Raw M100 12m, 7m, TP, and ACA-reference artifact registry entries plus
  `scripts/stage-wave6-issue163-m100-raw.sh` for status, verification,
  ordinary resumable staging, and extraction under
  `CASA_RS_TUTORIAL_DATA_ROOT`.
- Raw preflight runner
  `scripts/run-wave6-issue163-m100-raw-preflight.sh` records the exact guide
  sequence, emits CASA `listobs`/image-header probes for available extracted
  inputs, and reports missing raw inputs without touching partial downloads.
- Split parity runner `scripts/run-wave6-issue163-m100-split-parity.sh`
  compares CASA tutorial `split(..., keepflags=False)` with native casa-rs
  `mstransform --no-keepflags` on the extracted M100 12m and 7m raw inputs.
- Native multi-MS mosaic dirty cube imaging for the combined M100 12m+7m split
  products, including CASA-style HetArray screen sizing, ALMA/ACA Airy PB
  masks, per-channel Briggs density merging, and visible CASA/casa-rs/difference
  panels with colorbars.
- `mstransform --no-keepflags` support for CASA `split(keepflags=False)`
  row filtering, including rows marked by `FLAG_ROW` and rows whose selected
  `FLAG` cube is fully true.
- `mstransform` handling for optional `WEIGHT_SPECTRUM` cells in real ALMA
  MeasurementSets where the column exists but selected rows may have undefined
  cells.
- `mstransform` compact split-style `SPECTRAL_WINDOW` and
  `DATA_DESCRIPTION` output for selected SPWs, including `DATA_DESC_ID`
  remapping in MAIN rows and channel-selection metadata updates on compacted
  SPWs.

## CASA Source Semantics To Preserve

The raw M100 path should follow these inspected CASA implementation points:

- `casatasks/src/private/task_imregrid.py`: `template="get"` returns a
  coordinate-system record plus shape; an image template infers axes by
  coordinate type when `axes=[-1]`; the actual operation calls
  `image.regrid(..., method=interpolation, decimate=decimate,
  replicate=replicate)`.
- `casatasks/xml/imregrid.xml`: when an image template is used, output shape
  follows the template on regridded axes and the input on untouched axes;
  Stokes axes are not regridded; direction/spectral axes can be selected
  explicitly via `axes`.
- `casatasks/src/private/task_feather.py`: task `feather` sets VP defaults and
  SD scale options on the imager tool, then calls `imager.feather`.
- `casatools/src/code/synthesis/MeasurementEquations/Imager.cc`: `feather`
  creates the output image with the high-resolution image shape and
  coordinates, and if either input has per-plane beams, it loops per
  polarization/channel plane.
- `casatools/src/code/synthesis/MeasurementEquations/Feather.cc`: the low-res
  beam comes from the low-res image if present, otherwise from the low PSF or
  default PB lookup; `sdfactor`, `effdishdiam`, and `lowpassfiltersd` change
  Fourier-domain weighting behavior.

## Deferred Surface

The M100 guide route used for #163 starts from the published 12m/7m calibrated
MS products plus the guide TP image. Reproducing the TP calibration archive
itself remains outside this child issue because the guide data-combination
steps consume `M100_TP_CO_cube.spw3.image.bl`; the native `imregrid`, PB
weighting, `feather`, and moment products from that image are covered above.
