# Wave 6B Issue 167 Automasking Guide

Truth class: current descriptive
Last reality check: 2026-05-07
Verification: `cargo test -p casa-imaging --lib`; `cargo test -p casars-imager --lib`; `just quick`; `just verify`; `CASA_RS_TUTORIAL_DATA_ROOT=/Volumes/GLENDENNING/casa-rs/tutorial-data scripts/run-wave6-issue167-automasking.sh target/wave6-issue167-automasking`

Wave issue: #167
Parent wave: #143 / #127
Registry key: `alma/automasking`

## Tutorial Source

- CASA Guide page: <https://casaguides.nrao.edu/index.php?title=Automasking_Guide_CASA_6.5.4>
- Guide version observed during this wave: last checked on CASA 6.5.4; the current topical inventory keeps the same Automasking Guide route for #167.
- Input artifact: `twhya_selfcal.ms.contsub.tar`, staged by tutorial-data policy under `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/alma/automasking/`.
- Local staging status on 2026-05-06: present at `/Volumes/GLENDENNING/casa-rs/tutorial-data/tutorial-parity/alma/automasking/twhya_selfcal.ms.contsub.tar`; size `257537974` bytes; SHA-256 `9cd1b5f9a3bc80a5758e945d1c398e79a64fec9e2d40cad4336edbe7ea787de6`.

## Extracted Workflow

- `listobs('twhya_selfcal.ms.contsub', listfile='twhya_selfcal.ms.contsub.listobs.txt')`
- Dirty cube probe:
  - `tclean(..., imagename='twhya_dirtycube', specmode='cube', nchan=15, start='0.0km/s', width='0.5km/s', outframe='LSRK', restfreq='372.67249GHz', deconvolver='hogbom', gridder='standard', imsize=[250,250], cell='0.1arcsec', weighting='briggsbwtaper', robust=0.5, restoringbeam='common', niter=0)`
- Base automask cube:
  - same cube setup with `niter=100000`, `threshold='87mJy'`, `usemask='auto-multithresh'`, `noisethreshold=4.25`, `sidelobethreshold=2.0`, `lownoisethreshold=1.5`, `minbeamfrac=0.3`, `negativethreshold=15.0`, `verbose=True`, `fastnoise=False`
- Tutorial-visible outputs:
  - dirty cube image/residual products
  - automasked image/residual/mask products, with the guide explicitly opening `twhya_base_params.image`, `twhya_base_params.residual`, and `twhya_base_params.mask` together in CARTA
  - parameter-variation image/residual/mask products for noisethreshold, sidelobethreshold, pruning, and mask-growth examples

## CASA Source Inspection

Source-backed automask ownership for this wave:

- `casatools/src/code/synthesis/ImagerObjects/SynthesisDeconvolver.cc`: maps `maskType == "auto-multithresh"` to the `multithresh` automask algorithm.
- `casatools/src/code/synthesis/ImagerObjects/SynthesisUtilMethods.cc`: validates and records the public `sidelobethreshold`, `noisethreshold`, `lownoisethreshold`, `negativethreshold`, `minbeamfrac`, and `growiterations` fields into deconvolver parameters.
- `casatools/src/code/synthesis/ImagerObjects/SDMaskHandler.cc`: applies the CASA algorithm by computing median/MAD-derived noise, max sidelobe/noise thresholds, pruning regions smaller than `minbeamfrac * beam area`, beam-smoothed mask cuts, constrained binary dilation with `lownoisethreshold` only after `iterdone > 0`, optional grown-mask pruning, optional negative masks, zero-channel skip flags, and final mask union.
- `casatasks/src/private/imagerhelpers/_gclean.py`: runs an initial `niter=0` deconvolution pass for automask creation, then asks the deconvolver for mask updates after major-cycle residual refreshes when convergence has not yet been reached.

## casa-rs Implementation Scope

Implemented as the #167-owned slice, not as a #196 sub-ticket:

- `casars-imager --usemask auto-multithresh`
- `--sidelobethreshold`
- `--noisethreshold`
- `--lownoisethreshold`
- `--negativethreshold`
- `--smoothfactor`
- `--minbeamfrac`
- `--cutthreshold`
- `--growiterations`
- `--no-dogrowprune`
- `--minpercentchange`
- `--no-fastnoise`
- canonical JSON task request fields `use_mask` and `auto_mask`
- Python `casars.tasks.imager.mfs(..., use_mask=..., auto_mask=...)`
- CASA-style cube clean masks with shape `(nx, ny, 1, nchan)`, so generated or supplied masks can differ by output channel
- `.mask` product writing for effective clean masks, including channel-specific cube masks

The native cube controller now owns standard-gridder `auto-multithresh` updates across major cycles. Initial masks use CASA's no-growth `iterdone == 0` path; later major-cycle residual refreshes may grow existing positive masks and stop channels that remain empty under a noise threshold.

## Same-Input Evidence

Generated artifacts:

- output directory: `target/wave6-issue167-automasking/`
- summary JSON: `target/wave6-issue167-automasking/wave6-issue167-summary.json`
- visual panels: `target/wave6-issue167-automasking/*-chan{0,7,14}-panel.png`

Timing comparison from the same extracted MeasurementSet:

| Case | CASA C++ | casa-rs | Notes |
| --- | ---: | ---: | --- |
| dirty cube | 1.170 s | 1.832 s | release binary built before timing |
| base `auto-multithresh` cube | 5.726 s | 1.294 s | CASA guide base parameters |
| dirty + base automask total | 6.896 s | 3.126 s | comparable bounded guide slice |

The dirty cube path originally measured `3.775 s` for the casa-rs CLI leg. A
follow-up cache for row-local cube source-frequency conversions reduced the
same CLI leg below two seconds in repeated runs. The final same-input evidence
keeps the full CLI product write path in the timing.

Correctness comparison:

| Product | Result |
| --- | --- |
| dirty `.image` / `.residual` | strong parity: maximum channel RMS diff / CASA RMS is `8.44e-7`; maximum abs diff / CASA peak is `1.09e-6` |
| base automask `.image` | guide-level parity: maximum channel RMS diff / CASA RMS is `7.29e-3`; maximum abs diff / CASA peak is `1.57e-2` |
| base automask `.residual` | guide-level parity: maximum channel RMS diff / CASA RMS is `7.90e-3`; maximum abs diff / CASA peak is `3.25e-2` |
| base automask `.mask` | channel-specific masks now match the CASA active-channel surface: CASA masks only channels 5 and 7; casa-rs masks only channels 5 and 7, with channel 5 Jaccard `0.997` and channel 7 Jaccard `0.990` |

Mask detail for the active guide channels:

| Channel | CASA pixels | casa-rs pixels | Intersection | CASA-only | casa-rs-only |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 872 | 873 | 871 | 1 | 2 |
| 7 | 702 | 709 | 702 | 0 | 7 |
