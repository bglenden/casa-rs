# Imaging Effects Inventory

Truth class: current descriptive
Last reality check: 2026-07-16
Verification:
- `just docs-check`
- `cargo test -p casa-imaging mosaic_pointing_contribution_follows_casa_simple_pb_center_pixel_rule`
- `cargo test -p casa-imaging mosaic_clean_reduces_residual_peak_and_tracks_pb_weight_image`
- `cargo test -p casars-imager pbcor_products_apply_primary_beam_cutoff`
- `CASA_RS_WAVE6_DATASET=alma scripts/run-wave6-issue53-mosaic-panels.sh target/wave6-issue53-mosaic-panels`

## Purpose

Wave 6 used tutorial data to force missing imaging capabilities into the open
before claiming CASA Guide duplication. This inventory records the CASA imaging
effects established by the completed first mosaic tranche: #38, #50, #53,
#161, and #169.

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
| CASA cube mosaic path | spectral cube imaging with frequency-dependent PB and common beams | `casars-imager` cube preparation plus `casa-imaging` mosaic dirty path | implemented for the #163 two-channel M100 dirty probe; full 70-channel scale-up remains issue scope |
| CASA `MultiTermFT` over mosaic/PB projection | Taylor-term density, PSF, residual, model, restoration, PB correction, and alpha products | `run_mosaic_mtmfs_from_single_plane_stream` with raw-UVW density sidecars and mosaic metadata replay | first supported slice implemented for one MS, MFS, `nterms <= 2`, no W term, natural/uniform/Briggs weighting, user masks, and clean or dirty execution; higher terms and W/AW/pointing/startmodel/outlier/multi-MS combinations reject during planning |
| Visibility source traversal | overlap source reads with row preparation without changing sample order or residency bounds | shared `stream_imaging_source_read_ahead_blocks` producer/consumer path | standard MFS, mosaic MFS/MT-MFS replay, cube/cubedata, and mosaic cube share the path; the current maximum is two live blocks (one producer plus one consumer), with a zero-capacity rendezvous queue, cooperative producer cancellation after consumer failure, and slab guards when read-ahead would reduce residency/locality |
| Dirty-product gridding, FFT, and normalization | accumulate, transform, and normalize PSF/residual/weight grids without changing product semantics | CPU finishers plus guarded Apple MPSGraph resident standard/mosaic finishers and direct disjoint-tile mosaic MFS/MT-MFS accumulation | eligible standard and single-term mosaic f32 products may remain Metal-shared through correction, normalization, and peak reduction; MT-MFS retains its input through the batched inverse FFT, then corrects and normalizes on CPU. Exact-plan MT-MFS grouping keeps f64/Complex64 moments until the f32 tile and bounds compaction/routing scratch with an image-shape and plane-count formula. Auto compares exact input-boundary movement: host-resident products remain on CPU while Metal-shared products avoid host materialization; unsupported resident work returns to the host path, explicit Metal fails closed, and f64 remains CPU |
| CASA `pbcor` products | PB-corrected restored images with cutoff semantics | `mosaic_pb_product_from_weight`, `pb_correct_image_product`, `--pbcor` | implemented for mosaic MFS products using the current mosaic weight image and explicit `--pblimit` cutoff |
| CASA `usemask='auto-multithresh'` | automask generation | `casars-imager --usemask auto-multithresh` with guide-visible threshold, pruning, growth, negative-mask, and fast-noise controls; writes `.mask` product | #167 implements the Automasking Guide slice; full per-major-cycle CASA mask update parity remains evidence-driven issue scope |
| CASA `startmodel` | seed `imagename.model` from one or more model images before deconvolution/model prediction | `casars-imager --startmodel`; task contract `start_model`; Python `start_model` | #219 implements one existing single-plane startmodel image for non-mosaic, single-term MFS. CASA source seams are `task_deconvolve.py::check_starmodel_model_collisions`, `SynthesisParamsImage` startmodel parsing/validation in `SynthesisUtilMethods.cc`, and `SynthesisImager::createIMStore` calling `SIImageStore::setModelImage`; list/MTMFS/regrid/mosaic cases are rejected with explicit errors rather than silently ignored |
| CASA `outlierfile` | parse and orchestrate extra image definitions / outlier fields | `casars-imager --outlierfile`; task contract `outlier_file`; Python `outlier_file` | #220 implements source-backed parsing/inventory for CASA new-format outlier files plus execution for the main image and supported MFS/Hogbom outlier image sets, including the niter>0 joint multi-image CLEAN slice and CASA's `usemask=user` pixel-circle outlier mask used by `refim_twopoints_twochan.ms`. CASA recognizes `imagename`, `imsize`, `cell`, `phasecenter`, `startmodel`, `usemask`, `mask`, `specmode`, `nchan`, `start`, `width`, `nterms`, `reffreq`, `gridder`, `deconvolver`, and `wprojplanes`; unsupported mask forms, cube/MTMFS/w-projection variants, and non-standard gridders/deconvolvers reject clearly |
| CASA `savemodel='modelcolumn'` | predict the final model image into MAIN.MODEL_DATA | `casars-imager --savemodel modelcolumn`; task contract `save_model`; Python `save_model` | implemented for single-MS standard MFS and cube paths; MTMFS and multi-MS requests are rejected; source seam is CASA `SynthesisImager::runMajorCycle` / `SynthesisImager::predictModel` writing `VisibilityIterator::Model` |
| CASA `nmajor` / `fullsummary` | major-cycle limit and returned minor-cycle summary detail | `casars-imager --nmajor`; task contract `nmajor`, `fullsummary`, `iterdone`, `nmajordone`, `stopcode`, and `summaryminor` rows | #221 implements source-backed task parity: `nmajor=-1` is unlimited, `nmajor=0` stops after the initial residual, positive `nmajor` limits post-minor-cycle residual refreshes and reports CASA stop code 9. `fullsummary=false` keeps short minor-cycle rows; `fullsummary=true` adds start-iteration, start-peak, no-mask peak, and per-block stop-code fields. Slow parity on `unittest/tclean/refim_twochan.ms` matched CASA `iterdone=30`, `nmajordone=4`, `stopcode=9`, and 3 `summaryminor` rows for `niter=100`, `cycleniter=10`, `nmajor=3`, `threshold=0.01Jy`; image-product evidence recorded model peak `(50,50)` in both products, identical top 11 model components to CASA within `1e-5`, model RMS/max/corr `1.47e-4` / `8.84e-3` / `0.999871`, residual RMS/max/corr `1.05e-3` / `1.56e-2` / `0.998221`, and restored-image RMS/max/corr `4.94e-4` / `2.96e-3` / `0.999950` |

## Current Mode Matrix

| Mode | Geometry / phase effects | Gridding / PB effects | Deconvolution | Output products | Tutorial impact |
|---|---|---|---|---|---|
| Standard MFS | implemented for selected rows and phase center | standard convolution gridder | Hogbom/Clark/Multiscale/MTMFS paths exist | normal image sidecars | reusable for non-mosaic Wave 6 rows |
| WProject MFS | partial, source-backed dirty gates exist | wproject projector | standard CLEAN controller path | normal image sidecars | not first Wave 6 blocker |
| Mosaic MFS dirty | source-backed center-pixel contribution rule, phase-gradient projector | homogeneous ALMA/EVLA PB models, beam-frequency buckets, natural/Briggs weighting | dirty path remains available | `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, `.weight`, `.pb` | reused by #53 panel harness |
| Mosaic MFS cleaned | same as dirty path | same as dirty plus final visibility-domain residual refresh | Hogbom/Clark/Multiscale now run for `niter > 0`; #53 tutorial source-region deltas are below 1% of the CASA peak | restored and PB-corrected products now written for MFS mosaic | first ALMA #161 proof generated; VLA #169 proof uses the same panel harness |
| Mosaic MT-MFS first slice | mosaic phase/PB metadata plus separate raw-UVW density coordinates are replayed from bounded row blocks | homogeneous mosaic projector with natural/uniform/Briggs weighting and CASA-compatible f32 density/Taylor arithmetic | MT-MFS dirty and clean paths for `nterms <= 2`; unsupported wider projection/control combinations reject before retained visibility materialization | Taylor-term `.psf`, `.residual`, `.model`, `.image`, `.sumwt`, PB/PB-corrected products, and alpha/alpha-error products where requested | CASA oracle products are the correctness gate for #262; large ALMA mosaic MT-MFS is the bounded-memory/performance sentinel |
| Mosaic cube | phase/PB are channel aware in the dirty multi-MS route | ALMA/ACA HetArray screen sizing and PB normalization now match the #163 CASA probe below 1% max image error | dirty path proven; cleaned cube scale-up remains issue scope | `.psf`, `.residual`, `.image`, `.image.pbcor`, `.sumwt`, `.weight`, `.pb` for the #163 probe | #163 M100 12m+7m combined cube |
| Heterogeneous mosaic / AW-style | source-backed HetArray phase-gradient projector | ALMA/ACA Airy PBs, support-sized screens, and sky coverage | dirty path proven for #163; cleaned/full-cube proof still open | M100 two-channel probe products and panels | active Wave 6 capability, no longer deferred |

## Runtime Effects

- Imager task protocol v3 carries `parallel`, `chanchunks`, shared source
  memory/prepare/row-block/read-ahead controls, and dirty-product FFT precision
  and backend policy. `parallel=false` forces one source block, one prepare/grid
  worker, CPU acceleration, and RustFFT for a reproducible serial comparison.
- Diagnostic progress reports planned/tracked/high-water memory, source bytes,
  effective read bandwidth, producer/consumer blocked time and overlap, queue
  and worker state, stage timings, GPU eligibility/selection, device/host bytes,
  command/kernel time, and CPU fallback reasons.
- Apple GPU product finishing is an execution choice, not a product-mode fork.
  Auto selects resident MPSGraph for supported f32 dirty-product grids already
  planned in Metal-shared storage; host-resident grids remain on CPU. Explicit
  Metal bypasses the Auto placement choice, while unsupported or failed
  resident work uses the same CPU product path.
- W-projection Auto planning derives plane count from selected projected W,
  CASA's 1.05 W-range safety factor, and rectangular field angle without a
  fixed plane cap or longest-baseline proxy. Full-grid CPU density and W-project
  worker paths compare exact update, zero-fill, and merge cell touches instead
  of switching at a raw sample count.
- W-projection in-memory and replay inputs share one Metal partial-grid
  dispatcher. Partial count follows update density and live device headroom;
  replay combines chunk results in deterministic host f64 order and narrows
  once, while the source stream overlaps cached and newly read blocks under the
  same two-live-block residency bound.

## Theory / Tutorial Cross-Checks

- Faceting requires phase offsets and baseline recomputation per facet
  (`Perley-Geometry2024.pdf`, slide 13).
- Facet imaging uses its own phase reference center per facet before
  reprojection (`Jagannathan-Widefield2024.pdf`, slide 20).
- Joint mosaic imaging is a distinct mode with primary-beam overlap and
  normalization behavior that must be tested explicitly
  (`Plunket-Mosaicking2024.pdf`, slide 34).

## Issue Routing

- #38, #50, #53, #161, and #169 are complete. Their issue closeouts and this
  inventory preserve the source-backed mosaic tranche evidence.
- #35, #40, #42, #45, #52, #54, #55, #217, #223, and #341 own remaining
  focused imaging capabilities or parity questions.
- #386, #387, #398, #401, and their Wave 8 umbrella #411 own current imaging,
  lattice, image-expression, and performance-harness consolidation.
- Follow-on breadth tutorials reuse this inventory and should open narrower
  imager tickets when they expose effects not already owned by an open issue.
