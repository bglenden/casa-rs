# Imaging Effects Inventory

Truth class: current descriptive
Last reality check: 2026-05-02
Verification:
- `cargo test -p casa-imaging mosaic_pointing_contribution_follows_casa_simple_pb_center_pixel_rule`
- `cargo test -p casa-imaging mosaic_clean_reduces_residual_peak_and_tracks_pb_weight_image`
- `cargo test -p casars-imager pbcor_products_apply_primary_beam_cutoff`
- `CASA_RS_WAVE6_DATASET=alma scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels`

## Purpose

Wave 6 uses tutorial data to force missing imaging capabilities into the open
before claiming CASA Guide duplication. This inventory records the CASA imaging
effects that matter for the first mosaic tranche: #38, #50, #53, #161, and #169.

The tutorial acceptance data are the Antennae Band 7 and 3C391 mosaics. Smaller
pinned datasets such as `refim_alma_mosaic.ms`, `papersky_mosaic.ms`, and
`refim_oneshiftpoint.mosaic.ms` remain the fast regression oracles.

## Source Seams

| CASA / casacore seam | Effect | casa-rs location | Wave 6 status |
|---|---|---|---|
| `GridFT` / `grd2d.f` `SHIFT` path | phase-center shift before standard gridding | `casars-imager` prepared-sample phase rotation plus `casa-imaging` standard gridder | partial; source-backed standard and wproject gates exist, tutorial proof is tracked by child issues |
| `fwproj.f` / `fmosft.f` `dphase` paths | per-row phase correction in wproject and mosaic gridders | `casars-imager` row preparation, `casa-imaging` wproject/mosaic projectors | implemented for the MFS mosaic proof path; cube tutorial proof remains #161/#169 scope |
| `SimplePBConvFunc::findConvFunction` | homogeneous mosaic PB convolution, beam-frequency bucketing, support search | `casa-imaging::build_mosaic_projector`, `infer_mosaic_beam_frequencies_hz` | implemented for the homogeneous MFS mosaic proof path |
| `SimplePBConvFunc::addPBToFlux` | add PB coverage only when pointing center pixel is inside the image | `mosaic_pointing_contributes_by_simple_pb_center` | implemented as the source-backed #50 rule |
| `PBMosaicFT::getImage` | PB/flat-noise normalization and `pblimit` cutoff | `MosaicGridderConfig::pb_limit`, `casars-imager --pblimit`, `casars-imager --pbcor` | implemented for the homogeneous mosaic MFS proof path; native mask deltas are marginal and recorded in the #53 panels |
| `tclean(gridder='mosaic')` product writing | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, PB/weight-like products | `casars-imager::write_products` | implemented for mosaic MFS `.weight`, `.pb`, and optional `.image.pbcor`; panel proof landed in #53 |
| CASA minor-cycle controllers | cleaned mosaic images, masks, thresholds, cycle controls | standard controller plus final visibility-domain mosaic residual refresh path | implemented for the #53 Hogbom/Multiscale MFS proof; source-region tutorial deltas are now sub-percent |
| CASA cube mosaic path | spectral cube imaging with frequency-dependent PB and common beams | `run_cube` standard gridder only | missing for mosaic; required by #161 line products |
| CASA `pbcor` products | PB-corrected restored images with cutoff semantics | `mosaic_pb_product_from_weight`, `pb_correct_image_product`, `--pbcor` | implemented for mosaic MFS products using the current mosaic weight image and explicit `--pblimit` cutoff |
| CASA `usemask='auto-multithresh'` | automask generation | manual `--mask-box` / `--mask-image` only | missing; #53 follow-up when tutorials require automask products |
| CASA `startmodel` / `outlierfile` | model seeding and outlier-field orchestration | none | missing; keep out of #161/#169 unless the tutorial product needs it |

## Current Mode Matrix

| Mode | Geometry / phase effects | Gridding / PB effects | Deconvolution | Output products | Tutorial impact |
|---|---|---|---|---|---|
| Standard MFS | implemented for selected rows and phase center | standard convolution gridder | Hogbom/Clark/Multiscale/MTMFS paths exist | normal image sidecars | reusable for non-mosaic Wave 6 rows |
| WProject MFS | partial, source-backed dirty gates exist | wproject projector | standard CLEAN controller path | normal image sidecars | not first Wave 6 blocker |
| Mosaic MFS dirty | source-backed center-pixel contribution rule, phase-gradient projector | homogeneous ALMA/EVLA PB models, beam-frequency buckets, natural/Briggs weighting | dirty path remains available | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, `.weight`, `.pb` | reused by #53 panel harness |
| Mosaic MFS cleaned | same as dirty path | same as dirty plus final visibility-domain residual refresh | Hogbom/Clark/Multiscale now run for `niter > 0`; #53 tutorial source-region deltas are below 1% of the CASA peak | restored and PB-corrected products now written for MFS mosaic | first ALMA #161 proof generated; VLA #169 proof uses the same panel harness |
| Mosaic cube | phase/PB must be channel aware | missing from cube runner | missing | missing cube/moment-ready products | #161 line products |
| Heterogeneous mosaic / AW-style | not yet part of Wave 6 proof | partial or future | missing | missing | defer unless #161/#169 force it |

## Theory / Tutorial Cross-Checks

- Faceting requires phase offsets and baseline recomputation per facet
  (`Perley-Geometry2024.pdf`, slide 13).
- Facet imaging uses its own phase reference center per facet before
  reprojection (`Jagannathan-Widefield2024.pdf`, slide 20).
- Joint mosaic imaging is a distinct mode with primary-beam overlap and
  normalization behavior that must be tested explicitly
  (`Plunket-Mosaicking2024.pdf`, slide 34).

## Issue Routing

- #38 owns keeping this inventory source-backed as the tranche grows.
- #50 owns the pointing contribution rule. Current CASA evidence supports a
  center-pixel rule for the `SimplePBConvFunc` path, not a broad PB-wing overlap
  estimate.
- #53 owns the remaining tutorial-visible `tclean` surface. The current tranche
  implements cleaned MFS mosaic and PB-corrected products, while mosaic cube,
  automasking, start models, and outliers remain follow-up scope if required by
  #161 or #169.
- #161 and #169 must show the final proof with tutorial data and human-review
  artifact documents.
- #163, #177, and #181 are follow-on breadth tutorials that reuse the same
  inventory and should open narrower imager tickets if they expose effects not
  already listed here.
