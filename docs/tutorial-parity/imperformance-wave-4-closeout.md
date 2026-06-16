# ImPerformance Wave 4 Closeout

Truth class: evidence log
Last reality check: 2026-06-16
Verification: benchmark JSONs listed below; `docs/tutorial-parity/imperformance-wave-4-large-dirty-attempts.md`

## Scope

Wave 4 added bounded multi-plane spectral-line execution for standard cube,
`cubedata`, and PB-aware mosaic cube paths. This closeout records the evidence
used for W4-14 planner calibration and the remaining optional large-dirty
backend owner.

All generated benchmark products and logs are under
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete`, which is an
explicitly temporary artifact root.

## Performance Evidence

| Mode row | Dataset tier | Shape | casa-rs s | CASA s | Ratio | Schedule / backend | Slabs / active / workers | Modeled I/O GB | Source GB | Product GB | Peak RSS GB | Correctness status | Result |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| Standard cube dirty, current medium | 32 GB VLA medium | 512 ch, 2048 | 165.212 | 1918.001 | 11.61x | slab-first / CPU slab | 2 / 256 / 10 | 45.919 | 28.739 | 17.180 | 18.581 | `.image`, `.residual`, `.psf` good; `.sumwt` investigate only because tiny uniform product | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-dirty-closeout-medium-current/runs/20260615T044356Z-wave1-vla-single-medium-standard-cube-line-f77694da.json` |
| Standard cube dirty, best large baseline | 107 GB ALMA large | 1024 ch, 4096 | 572.315 | n/a | n/a | slab-first / CPU slab | 3 / 347 / 10 | 153.532 | 16.093 | 137.439 | 17.250 | large CASA skipped; used as large backend/product baseline | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-recovery-large-dirty-unshifted-ifft/runs/20260614T215220Z-wave1-alma-mosaic-large-standard-cube-line-de8dae03.json` |
| Standard cube dirty, current large | 107 GB ALMA large | 1024 ch, 4096 | 923.208 | n/a | n/a | slab-first / CPU slab | 2 / 514 / 10 | 153.490 | 16.051 | 137.439 | 18.498 | large CASA skipped; current path is backend-regressed | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-dirty-fast-single-sample-large/runs/20260615T042423Z-wave1-alma-mosaic-large-standard-cube-line-1727f925.json` |
| Cubedata dirty, medium | 32 GB VLA medium | 512 ch, 2048 | 213.727 | 1887.410 | 8.83x | slab-first / serial CPU plane backend | 11 / 50 / 10 | 47.252 | 30.072 | 17.180 | 8.243 | `.image`, `.residual`, `.psf` good; `.sumwt` investigate only because tiny uniform product | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-11-cubedata-medium/20260616T043109Z-wave4-standard-cubedata-line-medium-97343406.json` |
| Cubedata dirty, large bounded | 107 GB ALMA large | 256 ch, 4096 | 105.825 | n/a | n/a | slab-first / serial CPU plane backend | 7 / 40 / 10 | 38.643 | 4.283 | 34.360 | 13.515 | large CASA skipped; bounded large-dataset planner and I/O row | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T070401Z-wave4-standard-cubedata-line-large-bounded-bdb7948d.json` |
| Mosaic cube dirty, medium-output bounded | 107 GB ALMA large | 8 ch, 2048, 7 fields | 264.706 | n/a | n/a | slab-first / mosaic single-plane stream | 8 / 1 / 1 | partial | 0.400 | product-backed | 11.639 | large-MS bounded row; CASA skipped | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T064609Z-wave4-mosaic-cube-alma-medium-dirty-bounded-ddeacca9.json` |
| Mosaic cube dirty, large turnaround | 107 GB ALMA large | 4 ch, 1024, 7 fields | 80.933 | n/a | n/a | slab-first / mosaic single-plane stream | 4 / 1 / 1 | partial | 0.400 | product-backed | 11.177 | large-MS turnaround row; CASA skipped | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T053705Z-wave4-mosaic-cube-alma-large-dirty-turnaround-db1671a1.json` |

Mosaic cube correctness is covered by small CASA comparison bundles because the
medium and large mosaic rows intentionally skip CASA:

- Dirty: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T053451Z-wave4-mosaic-cube-alma-small-dirty-correctness-1f018d38.json`
- Clean: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T053528Z-wave4-mosaic-cube-alma-small-clean-correctness-976190bc.json`

## Clean Evidence

| Clean row | Shape | casa-rs s | CASA s | Ratio | Backend | Slabs / active / workers | Modeled I/O GB | Correctness status | Result |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: | --- | --- |
| Hogbom, CASA-compatible iteration/control | 512 ch, 2048, niter=100 | 273.021 | 3106.996 | 11.38x | grouped Wave 3 Metal | 13 / 40 / 8 | 69.395 | accepted in W4-08 with model/image/residual/PSF panels; cleaned 186 planes and skipped 326 planes under cube-level controls | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-08-pending-skip/runs/20260616T025901Z-wave1-vla-single-medium-standard-cube-line-clean-hogbom-casa-final-1bc4f17b.json` |
| Clark | 64 ch, 1024, niter=2 | 30.678 | 348.586 | 11.36x | grouped Wave 3 Metal | 2 / 34 / 10 | 4.925 | overall good; `.model` numeric difference tiny but classifier unknown due near-zero normalization | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T060517Z-wave4-standard-cube-line-medium-clean-clark-5c804f6b.json` |
| Multiscale | 64 ch, 1024, niter=2 | 26.697 | 231.006 | 8.65x | grouped Wave 3 Metal | 2 / 34 / 10 | 4.925 | `.model`, `.residual`, `.psf`, `.sumwt` good; `.image` investigate with normalized RMS 3.11e-6 and accepted visual residual | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T062145Z-wave4-standard-cube-line-medium-clean-multiscale-fec0a306.json` |

CASA phase attribution is available for the medium 64-channel clean probe:

- `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T055224Z-wave4-standard-cube-line-medium-casa-phase-probe-479fa5f6.json`
- CASA medians: `make_psf=52.755 s`, `calcres_major_cycle=53.850 s`,
  `minor_cycle=2.828 s`, `clean_major_cycle=102.024 s`.

## Memory And Plane-State I/O

The common planner is source/output-shape driven and compares source-first,
slab-first, and hybrid candidates by modeled I/O under the memory target. The
current slab executor capabilities still restrict standard cube and cubedata
execution to slab-first, but that limitation is logged as an executor capability
instead of a continuum/spectral rule.

| Row | Target GB | Planned active GB | Peak RSS GB | Visibility/source-side buffers | Plane/product-side buffers | Plane-state read GB | Plane-state write GB | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Standard cube medium dirty | 17.180 | 17.180 | 18.581 | row-block stream, no full visibility cache | 17.180 GB modeled product write | 0 | product-backed | Peak RSS is slightly above target in this older row; later cubedata/mosaic rows stay below target. |
| Cubedata medium dirty | 17.180 | 17.180 | 8.243 | row-block stream, no full visibility cache | 17.180 GB modeled product write | 0.000 | 34.360 | Product-backed write-through stores `.psf` and `.residual` with `.image` alias semantics. |
| Cubedata large bounded | 17.180 | 17.180 | 13.515 | 3.775 GB source stream buffer, no full visibility cache | 11.474 GB product scratch, 34.360 GB modeled product writes | 0.000 | 34.360 | New W4-14 large row; 7 product-backed store groups, 13.341 s store time. |
| Mosaic medium-output bounded | 17.180 | 1.268 | 11.639 | 1.133 GB source stream buffer | product-backed write-through | 0 | product-backed | Current executor limitation: `mosaic_single_plane_stream`, one active plane, per-plane source reuse. |
| Mosaic large turnaround | 17.180 | 1.167 | 11.177 | 1.133 GB source stream buffer | product-backed write-through | 0 | product-backed | Bounded large-MS turnaround row; not a parallel mosaic optimization claim. |

## Planner Calibration Decisions

- Keep the common cost-model planner. Continuum and spectral cube remain the
  same source/output-shape problem; continuum is the one-plane degenerate case.
- Do not make slab-first a semantic default. It is the selected candidate for
  the current executable standard cube/cubedata paths because modeled output
  spill dominates source rereads under the executor constraints.
- Keep source-first and hybrid candidates in the planner. They remain important
  when all output state fits or when full/partial visibility caching wins the
  modeled I/O comparison.
- Do not spend more W4-14 time on slab-first versus source-first for large dirty
  standard cube. Current large source reads are about 34 s out of 923 s, so they
  are not the current limiter.
- Keep whole-plane product tiles and zero-copy direct writes. The latest large
  standard cube product write bucket improved, but total wall time regressed
  because the per-plane backend grew. That points away from product layout
  reversal.
- Use grouped Metal by default for eligible cube clean/deconvolution planes.
  Hogbom, Clark, and multiscale medium rows all use the shared per-plane Wave 3
  Metal backend when eligible.
- Keep the mosaic cube executor limitation visible. W4-12 proves bounded
  PB-aware products and small correctness, but mosaic cube is still single-plane
  stream per output channel, not a fully parallel slab executor.
- Treat TB-scale as final confirmation only. The next large dirty owner is the
  optional source-major/batched backend spike; a TB row before that would mostly
  reconfirm the current backend lower bound.

## Rejected Or Parked Attempts

Detailed attempt evidence is in
`docs/tutorial-parity/imperformance-wave-4-large-dirty-attempts.md`.

Retained:

- Whole-plane product tile layout.
- Zero-copy direct whole-plane Fortran writes.
- Centered-IFFT checkerboard shift removal.
- Direct dirty replay instrumentation and the narrow single-contribution fast
  path where it remains part of the shared implementation.

Rejected as default or reverted:

- Direct full physical tile overwrite.
- Deep product tile batching.
- Row-tile product layout as the default.
- Blocked-transpose inverse FFT around the scalar `rustfft` path.
- Unmerged whole-plane product groups.
- Temporary f32 routed dirty grids.
- Indexed spectral assignment lookup.

Parked as optional Wave 4 follow-up:

- #311 Source-major and batched backend spike for large dirty cube performance.
  The evidence says the large dirty problem is now per-plane feed/grid/FFT
  backend cost, not planner source-read scheduling or product tile shape.
