# Imaging Effects Inventory

Truth class: current descriptive
Last reality check: 2026-05-02
Verification: `cargo test -p casa-imaging mosaic_pointing_contribution_follows_casa_simple_pb_center_pixel_rule`

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
| `fwproj.f` / `fmosft.f` `dphase` paths | per-row phase correction in wproject and mosaic gridders | `casars-imager` row preparation, `casa-imaging` wproject/mosaic projectors | partial; mosaic tutorial proof still needed on #161/#169 |
| `SimplePBConvFunc::findConvFunction` | homogeneous mosaic PB convolution, beam-frequency bucketing, support search | `casa-imaging::build_mosaic_projector`, `infer_mosaic_beam_frequencies_hz` | partial; dirty MFS only before #53 |
| `SimplePBConvFunc::addPBToFlux` | add PB coverage only when pointing center pixel is inside the image | `mosaic_pointing_contributes_by_simple_pb_center` | implemented as the source-backed #50 rule |
| `PBMosaicFT::getImage` | PB/flat-noise normalization and `pblimit` cutoff | `MosaicGridderConfig::pb_limit`, `casars-imager --pblimit` | partial; cutoff is now configurable, but cleaned mosaic/PB-corrected products remain #53 work |
| `tclean(gridder='mosaic')` product writing | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, PB/weight-like products | `casars-imager::write_products` | partial; mosaic weight/sensitivity is now emitted as `.weight` when present |
| CASA minor-cycle controllers | cleaned mosaic images, masks, thresholds, cycle controls | `run_cotton_schwab_controller` for standard MFS only | missing for mosaic; #53 owns extending beyond dirty MFS |
| CASA cube mosaic path | spectral cube imaging with frequency-dependent PB and common beams | `run_cube` standard gridder only | missing for mosaic; required by #161 line products |
| CASA `pbcor` products | PB-corrected restored images with cutoff semantics | none | missing; #53 follow-up before tutorial closeout if figures require PB-corrected products |
| CASA `usemask='auto-multithresh'` | automask generation | manual `--mask-box` / `--mask-image` only | missing; #53 follow-up when tutorials require automask products |
| CASA `startmodel` / `outlierfile` | model seeding and outlier-field orchestration | none | missing; keep out of #161/#169 unless the tutorial product needs it |

## Current Mode Matrix

| Mode | Geometry / phase effects | Gridding / PB effects | Deconvolution | Output products | Tutorial impact |
|---|---|---|---|---|---|
| Standard MFS | implemented for selected rows and phase center | standard convolution gridder | Hogbom/Clark/Multiscale/MTMFS paths exist | normal image sidecars | reusable for non-mosaic Wave 6 rows |
| WProject MFS | partial, source-backed dirty gates exist | wproject projector | standard CLEAN controller path | normal image sidecars | not first Wave 6 blocker |
| Mosaic MFS dirty | source-backed center-pixel contribution rule, phase-gradient projector | homogeneous ALMA/EVLA PB models, beam-frequency buckets, natural weighting | intentionally not run when `niter > 0` | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, `.weight` | first #161/#169 blocker |
| Mosaic MFS cleaned | same as dirty path needed | same as dirty plus residual/model major-cycle refresh | missing | missing restored parity evidence | #53 prerequisite |
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
- #53 owns the remaining tutorial-visible `tclean` surface: cleaned mosaic,
  mosaic cube, PB-corrected products, automasking, start models, outliers, and
  any weighting controls needed by #161 or #169.
- #161 and #169 must show the final proof with tutorial data and human-review
  artifact documents.
- #163, #177, and #181 are follow-on breadth tutorials that reuse the same
  inventory and should open narrower imager tickets if they expose effects not
  already listed here.
