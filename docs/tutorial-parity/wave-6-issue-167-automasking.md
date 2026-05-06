# Wave 6B Issue 167 Automasking Guide

Truth class: current descriptive
Last reality check: 2026-05-06
Verification: `cargo test -p casars-imager`; `just quick`; `scripts/test-python-package.sh`; `just docs-check`

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
- `casatools/src/code/synthesis/ImagerObjects/SDMaskHandler.cc`: applies the CASA algorithm by computing median/MAD-derived noise, max sidelobe/noise thresholds, pruning regions smaller than `minbeamfrac * beam area`, constrained binary dilation with `lownoisethreshold`, optional grown-mask pruning, optional negative masks, and final mask union.

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
- `.mask` product writing for effective clean masks

The native mask generator currently builds a deterministic initial automask from the dirty residual product and applies it to the clean run. It source-aligns with CASA's threshold, pruning, constrained growth, and negative-feature controls, but it does not yet claim CASA's full per-major-cycle mask-regeneration cadence as exact tutorial parity until same-input CASA C++ / casa-rs image and mask artifacts are produced.

## Deferred Evidence

The implementation surface is now present for the Automasking Guide route. Full tutorial closeout still requires extracting `twhya_selfcal.ms.contsub.tar`, CASA 6.7.5-9/CASA C++ reference runs, casa-rs runs from the same inputs, and side-by-side image/residual/mask comparison artifacts.
