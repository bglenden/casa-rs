# ImPerformance Wave 4 Closeout

Truth class: evidence log
Last reality check: 2026-06-16
Verification: benchmark JSONs listed below; `docs/tutorial-parity/imperformance-wave-4-large-dirty-attempts.md`

## Scope

Wave 4 added bounded multi-plane spectral-line execution for standard cube,
`cubedata`, and PB-aware mosaic cube paths. This closeout records the current
evidence for W4-18 acceleration review. It is not yet a Wave 4 done claim:
blocked rows require either further implementation or explicit Brian review and
acceptance before PR #314 can be marked ready or merged.

All generated benchmark products and logs are under
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete`, which is an
explicitly temporary artifact root.

## W4-18 Acceleration Matrix Status

Generated with:

```bash
python3 tools/perf/imager/wave4_acceleration_matrix.py \
  --evidence-list tools/perf/imager/wave4_acceleration_evidence.json \
  --format markdown
```

The evidence manifest is checked in at
`tools/perf/imager/wave4_acceleration_evidence.json`; it contains the absolute
GLENDENNING result paths for each row below.

| Matrix row | Tier / shape | Serial or single-worker s | Multi-worker CPU/auto s | Metal/default s | CASA s | Key speedup | Correctness | Target status | Evidence |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Standard cube dirty | medium, 512 ch, 2048 | 312.241 | 111.665 | n/a | 1918.001 | 2.80x auto vs serial; 17.18x vs CASA | good | met | `20260616T154241Z-wave4-standard-cube-line-medium-4b817340` |
| Standard cube clean Hogbom | medium, 64 ch, 1024 | n/a | n/a | 17.100 | 220.834 | 12.91x vs CASA | investigate | blocked | `20260616T055224Z-wave4-standard-cube-line-medium-casa-phase-probe-479fa5f6` |
| Standard cube clean Clark | medium, 64 ch, 1024 | n/a | n/a | 30.678 | 348.586 | 11.36x vs CASA | good | blocked | `20260616T060517Z-wave4-standard-cube-line-medium-clean-clark-5c804f6b` |
| Standard cube clean multiscale | medium, 64 ch, 1024 | n/a | n/a | 26.697 | 231.006 | 8.65x vs CASA | investigate | blocked | `20260616T062145Z-wave4-standard-cube-line-medium-clean-multiscale-fec0a306` |
| Cubedata dirty | medium, 512 ch, 2048 | 298.058 | 233.989 | n/a | 1887.410 | 1.27x auto vs serial; 8.07x vs CASA | good | blocked: auto-vs-serial target missed | `20260616T155024Z-wave4-standard-cubedata-line-medium-445d25b0` |
| Cubedata clean Hogbom | medium, 512 ch, 1024 | n/a | n/a | 255.725 | n/a | blocked: no comparable medium CASA/serial/multi row | good on small CASA comparison | blocked | `20260616T144357Z-wave4-standard-cubedata-line-medium-clean-hogbom-decc2963`; correctness `20260616T142710Z-wave4-standard-cubedata-line-small-clean-hogbom-correctness-67779b86` |
| Cubedata clean Clark | small, 24 ch, 512 | n/a | n/a | 2.040 | 14.492 | 7.10x vs CASA | good | blocked | `20260616T142758Z-wave4-standard-cubedata-line-small-clean-clark-correctness-2712545a` |
| Cubedata clean multiscale | small, 24 ch, 512 | n/a | n/a | 3.054 | 9.362 | 3.07x vs CASA | good | blocked | `20260616T143211Z-wave4-standard-cubedata-line-small-clean-multiscale-correctness-05d32735` |
| Mosaic cube dirty | large, 4 ch, 1024 | 79.572 | 45.486 | n/a | n/a | 1.75x vs single-plane stream | good on small CASA row | met | `20260616T130921Z-wave4-mosaic-cube-alma-large-dirty-turnaround-3f72240e` |
| Mosaic cube clean Hogbom | small, 8 ch, 512 | n/a | n/a | 7.128 | 4.259 | 0.60x vs CASA | good | blocked | `20260616T143339Z-wave4-mosaic-cube-alma-small-clean-hogbom-correctness-dbb2bdd3` |
| Mosaic cube clean Clark | small, 8 ch, 512 | n/a | 6.112 | 7.086 | 5.926 | Metal is 0.86x vs CPU; default is 0.84x vs CASA | good | blocked | `20260616T132458Z-wave4-mosaic-cube-alma-small-clean-correctness-7be34f88` |
| Mosaic cube clean multiscale | small, 8 ch, 512 | n/a | n/a | 6.101 | 4.046 | 0.66x vs CASA | investigate; `.image` RMS is 1.7e-6 of CASA support RMS | blocked | `20260616T143414Z-wave4-mosaic-cube-alma-small-clean-multiscale-correctness-5ac88e74` |

Current review conclusion: standard cube dirty and W4-19 mosaic dirty have met
their matrix targets. Standard cube dirty now has a direct medium serial-vs-auto
row, so the 24 GB large row remains large-scale context rather than the speedup
comparator. Cubedata dirty correctness is good, including the `.sumwt`
non-spatial product reclassification, but the auto-vs-serial speedup is only
1.27x against the 2.0x target. W4-21 mosaic clean performance has reached the
goal's short-circuit condition: serious evidence exists, but the required
targets are still not met. W4-20 cubedata clean correctness is covered on small
all-channel rows, and Hogbom now has a representative medium Metal/default
performance row; cubedata clean remains blocked because serial/multi evidence,
medium Clark/multiscale evidence, and comparable medium CASA timing are still
missing.

## Performance Evidence

| Mode row | Dataset tier | Shape | casa-rs s | CASA s | Ratio | Schedule / backend | Slabs / active / workers | Modeled I/O GB | Source GB | Product GB | Peak RSS GB | Correctness status | Result |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| Standard cube dirty, medium serial #311 | 32 GB VLA medium | 512 ch, 2048 | 312.241 | n/a | n/a | slab-first / CPU slab | 11 / 47 / 1 | 47.252 | 30.072 | 17.180 | 8.235 | serial baseline; CASA skipped by design | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-standard-serial/20260616T153712Z-wave4-standard-cube-line-medium-9e01c97a.json` |
| Standard cube dirty, medium auto #311 | 32 GB VLA medium | 512 ch, 2048 | 111.665 | 1918.001 | 17.18x vs CASA; 2.80x vs serial | slab-first / CPU slab | 6 / 89 / 10 | 46.511 | 29.332 | 17.180 | 11.096 | correctness from comparable medium CASA row; `.sumwt` non-spatial false positive reclassified good | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-standard-auto/20260616T154241Z-wave4-standard-cube-line-medium-4b817340.json` |
| Standard cube dirty, previous best large baseline | 107 GB ALMA large | 1024 ch, 4096 | 572.315 | n/a | n/a | slab-first / CPU slab | 3 / 347 / 10 | 153.532 | 16.093 | 137.439 | 17.250 | large CASA skipped; used as prior large backend/product baseline | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-recovery-large-dirty-unshifted-ifft/runs/20260614T215220Z-wave1-alma-mosaic-large-standard-cube-line-de8dae03.json` |
| Standard cube dirty, regressed 30 GB plan | 107 GB ALMA large | 1024 ch, 4096 | 923.208 | n/a | n/a | slab-first / CPU slab | 2 / 514 / 10 | 153.490 | 16.051 | 137.439 | 18.498 | superseded; too aggressive laptop memory target and worse backend timing | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-dirty-fast-single-sample-large/runs/20260615T042423Z-wave1-alma-mosaic-large-standard-cube-line-1727f925.json` |
| Standard cube dirty, current 24 GB large | 107 GB ALMA large | 1024 ch, 4096 | 524.143 | n/a | n/a | slab-first / CPU slab | 9 / 120 / 10 | 153.781 | 16.030 | 137.439 | 14.170 | large CASA skipped; best large dirty row; modeled source is 16.342 GB and backend still dominates | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-standard-cube-large-24gb/20260616T150436Z-wave1-alma-mosaic-large-standard-cube-line-0531c480.json` |
| Cubedata dirty, medium serial #311 | 32 GB VLA medium | 512 ch, 2048 | 298.058 | n/a | n/a | slab-first / CPU slab | 11 / 47 / 1 | 47.252 | 30.072 | 17.180 | 7.946 | serial baseline; CASA skipped by design | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-cubedata-serial/20260616T154508Z-wave4-standard-cubedata-line-medium-eb7053f7.json` |
| Cubedata dirty, medium auto #311 | 32 GB VLA medium | 512 ch, 2048 | 233.989 | 1887.410 | 8.07x vs CASA; 1.27x vs serial | slab-first / CPU slab | 6 / 89 / 10 | 46.511 | 29.332 | 17.180 | 10.294 | correctness from comparable medium CASA row; `.sumwt` non-spatial false positive reclassified good; misses 2.0x auto target | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-cubedata-auto/20260616T155024Z-wave4-standard-cubedata-line-medium-445d25b0.json` |
| Cubedata dirty, large bounded | 107 GB ALMA large | 256 ch, 4096 | 105.825 | n/a | n/a | slab-first / serial CPU plane backend | 7 / 40 / 10 | 38.643 | 4.283 | 34.360 | 13.515 | large CASA skipped; bounded large-dataset planner and I/O row | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T070401Z-wave4-standard-cubedata-line-large-bounded-bdb7948d.json` |
| Cubedata clean Hogbom, medium Metal/default | 32 GB VLA medium | 512 ch, 1024, niter=2 | 255.725 | n/a | n/a | slab-first / grouped Wave 3 Metal | 13 / 40 / 10 | 38.958 | 28.764 | 8.590 | recorded in JSON | small all-channel CASA comparison good; medium run skipped CASA | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-20-cubedata-clean-medium/20260616T144357Z-wave4-standard-cubedata-line-medium-clean-hogbom-decc2963.json` |
| Mosaic cube dirty, previous large turnaround | 107 GB ALMA large | 4 ch, 1024, 7 fields | 80.933 | n/a | n/a | slab-first / mosaic single-plane stream | 4 / 1 / 1 | partial | 0.400 | product-backed | 11.177 | superseded by W4-19 multi-plane evidence | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T053705Z-wave4-mosaic-cube-alma-large-dirty-turnaround-db1671a1.json` |
| Mosaic cube dirty, W4-19 single-plane baseline | 107 GB ALMA large | 4 ch, 1024, 7 fields | 79.572 | n/a | n/a | slab-first / mosaic multi-plane stream forced to one worker | 1 / 1 / 1 | partial | 0.400 | product-backed | recorded in JSON | single-worker baseline for W4-19 | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-19-mosaic-turnaround-forced1-sharedctx/20260616T131206Z-wave4-mosaic-cube-alma-large-dirty-turnaround-b52e9a4d.json` |
| Mosaic cube dirty, W4-19 multi-plane auto | 107 GB ALMA large | 4 ch, 1024, 7 fields | 45.486 | n/a | n/a | slab-first / mosaic multi-plane stream | 1 / 4 / 4 | partial | 0.400 | product-backed | recorded in JSON | 1.75x total speedup vs forced single-plane stream | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-19-mosaic-turnaround-sharedctx/20260616T130921Z-wave4-mosaic-cube-alma-large-dirty-turnaround-3f72240e.json` |

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
| Standard cube medium dirty auto #311 | 24.000 | 24.000 | 11.096 | row-block stream, no full visibility cache; 29.332 GB modeled source reads | 17.180 GB modeled product write | 0 | product-backed | Direct medium auto-vs-serial comparator; 6 slabs, 89 active planes, 10 workers. |
| Standard cube large dirty, 24 GB cap | 24.000 | 24.000 | 14.170 | 7.858 GB source stream buffer, no full visibility cache | 14.205 GB product scratch, 137.439 GB modeled product writes | 0.000 | 137.439 | Best large dirty row: 524.143 s total, 37.337 s source read, 481.638 s backend, 58.435 s product write. |
| Cubedata medium dirty auto #311 | 24.000 | 24.000 | 10.294 | row-block stream, no full visibility cache; 29.332 GB modeled source reads | 17.180 GB modeled product write | 0.000 | 17.180 | Correctness good, but 10-worker auto gives only 1.27x over serial on this row. |
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
  standard cube unless source reads become dominant. Current large source reads
  are 37.337 s out of 524.143 s, so they are not the current limiter.
- Keep whole-plane product tiles and zero-copy direct writes. The 24 GB large
  standard cube row writes 137.439 GB of products in 58.435 s, with no LRU
  readback, zero-fill, or C-order conversion.
- Use grouped Metal by default for eligible cube clean/deconvolution planes.
  Hogbom, Clark, and multiscale medium rows all use the shared per-plane Wave 3
  Metal backend when eligible.
- Keep mosaic cube executor capabilities visible. W4-19 now reports
  `mosaic_multi_plane_stream`, `active_planes > 1`, and `worker_count > 1` for
  representative dirty rows; clean rows use the same slab-plane dispatch but
  still miss the performance targets.
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

Required Wave 4 follow-up:

- #311 Source-major and batched backend spike for large dirty cube performance.
  The 24 GB large row fixes the previous memory-target regression, but the
  remaining large dirty problem is still per-plane feed/grid/FFT backend cost,
  not planner source-read scheduling or product tile shape.
