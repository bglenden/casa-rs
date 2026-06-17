# ImPerformance Wave 4 Closeout

Truth class: evidence log
Last reality check: 2026-06-17
Verification: benchmark JSONs listed below; `python3 -m unittest tools/perf/imager/test_wave4_acceleration_matrix.py`; `cargo test -p casars-imager cube_per_plane_runtime_plan_selects_grouped_metal_for_single_channel_cube_clean --lib`; `cargo test -p casa-imaging --lib`; `docs/tutorial-parity/imperformance-wave-4-large-dirty-attempts.md`

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
| Standard cube dirty | medium, 512 ch, 2048 | 316.815 | 154.741 | n/a | 1918.001 | 2.05x auto vs forced single-worker; 12.39x vs CASA | good | met | `20260616T173624Z-wave4-standard-cube-line-medium-905e11e5` |
| Standard cube clean Hogbom | medium, 64 ch, 1024, niter=10000 | n/a | 282.458 | 137.109 | 811.307 | 2.06x Metal vs CPU; 5.92x vs CASA | blocked: restore edge artifact fixed, remaining model/residual component-order divergence | blocked | `20260617T133112Z-wave4-standard-cube-line-medium-casa-phase-probe-cf20078a` plus unchanged CASA row `20260617T022154Z-wave4-standard-cube-line-medium-casa-phase-probe-0cd0fb24` |
| Standard cube clean Clark | medium, 64 ch, 1024, niter=10000 | 160.122 | 36.607 | 32.429 | 409.289 | 4.37x multi-worker vs serial; Metal default is 1.13x vs multi-worker CPU and 12.62x vs CASA | accepted by Brian on 2026-06-17 after panel review: `.model`, `.psf`, `.sumwt` good; small structured `.image`/`.residual` differences acceptable for Wave 4 | missed-accepted-by-Brian: CASA speed target met, Metal-vs-CPU target missed | `20260617T171149Z-wave4-standard-cube-line-medium-clean-clark-metal-ea3ec28b`; CASA timing from unchanged row `20260617T160130Z-wave4-standard-cube-line-medium-clean-clark-923a9413` |
| Standard cube clean multiscale | medium, 64 ch, 1024, niter=10000 | n/a | 784.810 | n/a | n/a | blocked: no comparable Metal/serial/CASA row | missing comparable deep CASA correctness | blocked | `20260617T013555Z-wave4-standard-cube-line-medium-clean-multiscale-585d9f40` |
| Cubedata dirty | medium, 512 ch, 2048 | 349.241 | 146.788 | n/a | 1887.410 | 2.38x auto vs forced single-worker; 12.86x vs CASA | good | met | `20260616T172006Z-wave4-standard-cubedata-line-medium-1c103335` |
| Cubedata clean Hogbom | medium, 64 ch, 1024, niter=10000 | n/a | n/a | 314.402 | n/a | blocked: no comparable medium CASA/serial/multi row | missing comparable deep CASA correctness | blocked | `20260617T011951Z-wave4-standard-cubedata-line-medium-clean-hogbom-e6a16c03` |
| Cubedata clean Clark | medium, 64 ch, 1024, niter=10000 | 159.595 | 43.880 | 41.658 | n/a | 3.64x multi-worker vs serial; Metal is 1.05x vs CPU | missing comparable deep CASA correctness | blocked | `20260617T010937Z-wave4-standard-cubedata-line-medium-clean-clark-673227b7` |
| Cubedata clean multiscale | medium, 512 ch, 1024, niter=2 | n/a | 130.764 | 121.081 | n/a | Metal is 1.08x vs CPU | missing comparable medium CASA correctness | blocked | `20260616T195624Z-wave4-standard-cubedata-line-medium-clean-multiscale-1464ab93` |
| Mosaic cube dirty | large, 4 ch, 1024 | 79.572 | 45.486 | n/a | n/a | 1.75x vs single-plane stream | good on small CASA row | met | `20260616T130921Z-wave4-mosaic-cube-alma-large-dirty-turnaround-3f72240e` |
| Mosaic cube clean Hogbom | small, 8 ch, 512 | n/a | n/a | 7.128 | 4.259 | 0.60x vs CASA | good | blocked | `20260616T143339Z-wave4-mosaic-cube-alma-small-clean-hogbom-correctness-dbb2bdd3` |
| Mosaic cube clean Clark | small, 8 ch, 512 | n/a | 6.112 | 7.086 | 5.926 | Metal is 0.86x vs CPU; default is 0.84x vs CASA | good | blocked | `20260616T132458Z-wave4-mosaic-cube-alma-small-clean-correctness-7be34f88` |
| Mosaic cube clean multiscale | small, 8 ch, 512 | n/a | n/a | 6.101 | 4.046 | 0.66x vs CASA | investigate; `.image` RMS is 1.7e-6 of CASA support RMS | blocked | `20260616T143414Z-wave4-mosaic-cube-alma-small-clean-multiscale-correctness-5ac88e74` |

Current review conclusion: standard cube dirty, cubedata dirty, and W4-19 mosaic
dirty have met their matrix targets. The dirty standard cube and cubedata rows
now use the same standard spectral cube slab infrastructure, with forced
single-worker rows produced by `standard_mfs_grid_threads=1` and auto rows using
10 plane workers. The 24 GB large row remains large-scale context rather than
the medium speedup comparator. Cubedata dirty correctness is good, including the
`.sumwt` non-spatial product reclassification, and its refreshed auto-vs-forced
single-worker speedup is 2.38x against the 2.0x target.

Clean closeout remains blocked. The matrix now requires clean correctness
evidence to match the selected clean iteration depth, so the earlier shallow
`niter=2` CASA rows no longer satisfy the deep `niter=10000` multiscale or
cubedata clean rows. Clark's original comparable deep CASA row was blocked by
over-cleaning. The 8-channel control probe matches CASA's cube clean-control
iteration vector exactly, and the fresh 64-channel rerun fixes the model
over-cleaning: `.model`, `.psf`, and `.sumwt` are good. `.image` and
`.residual` remain investigate-level structured differences, which Brian
reviewed visually and accepted for Wave 4 on 2026-06-17. The Clark Metal
minor-cycle path clears the 10x CASA target at 12.62x, but only gives 1.13x
over the current multi-worker CPU row, below the 1.5x Metal-vs-CPU target; this
miss is accepted for standard cube Clark in the Wave 4 matrix.
Hogbom has a comparable deep CASA row and now uses the actual Metal Hogbom
minor-cycle backend by default. The CASA-style
restored-model FFT convolution fix removes the visible `.image` edge artifact
in the deep standard cube row: whole-cube `.image` RMS drops from `0.115015` to
`0.029889`, edge16 RMS drops from `0.448945` to `0.032524`, and the worst old
edge channel drops from `0.819604` to `0.062368`. `.residual`, `.model`,
`.psf`, and `.sumwt` are unchanged by the restore-only fix. The row is still
blocked because the remaining `.model`/`.residual` differences are late
Hogbom component-order divergence after `niter=10000`, and the post-fix default
row is only 5.92x faster than CASA rather than the 10x target.

## Standard Cube / Cubedata Refactor Evidence

This refactor removes the internal split between standard cube and `cubedata`
dirty execution. The public modes remain distinct, but their shared path is now
the standard spectral cube slab runner plus an explicit spectral-axis policy.
The table below uses the medium 512-channel, 2048-pixel workload and a 24 GB
decimal memory cap. Current rows were run after the refactor on 2026-06-16; the
fresh auto rows were captured with host load average around 5.3, so they are
accepted as conservative current evidence rather than best-case timing.

| Mode | Pre-refactor forced single-worker s | Pre-refactor auto s | Current forced single-worker s | Current auto s | Current auto speedup | Current CASA speedup | Current auto evidence |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Standard cube dirty | 312.241 | 111.665 | 316.815 | 154.741 | 2.05x | 12.39x | `20260616T173624Z-wave4-standard-cube-line-medium-905e11e5` |
| Cubedata dirty | 298.058 | 233.989 | 349.241 | 146.788 | 2.38x | 12.86x | `20260616T172006Z-wave4-standard-cubedata-line-medium-1c103335` |

The important refactor outcome is that `cubedata` no longer misses the worker
utilization class: in the current conservative rows it is slightly faster than
the contemporaneous standard cube auto row, and the older pre-refactor 233.989 s
blocked cubedata auto result is superseded. A faster post-refactor cubedata auto
row also exists at 116.080 s
(`20260616T165430Z-wave4-standard-cubedata-line-medium-d18dc8d2`), but the
checked-in acceleration manifest uses the fresher conservative row above.

## Performance Evidence

| Mode row | Dataset tier | Shape | casa-rs s | CASA s | Ratio | Schedule / backend | Slabs / active / workers | Modeled I/O GB | Source GB | Product GB | Peak RSS GB | Correctness status | Result |
| --- | --- | --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| Standard cube dirty, medium forced single-worker #311 | 32 GB VLA medium | 512 ch, 2048 | 316.815 | n/a | n/a | slab-first / CPU slab | 11 / 47 / 1 | 47.252 | 30.072 | 17.180 | 8.197 | forced single-worker baseline with `standard_mfs_grid_threads=1`; CASA skipped by design | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-standard-forced1-current/20260616T172347Z-wave4-standard-cube-line-medium-682ac42a.json` |
| Standard cube dirty, medium auto #311 | 32 GB VLA medium | 512 ch, 2048 | 154.741 | 1918.001 | 12.39x vs CASA; 2.05x vs forced single-worker | slab-first / CPU slab | 6 / 89 / 10 | 46.511 | 29.332 | 17.180 | 11.160 | correctness from comparable medium CASA row; `.sumwt` non-spatial false positive reclassified good | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-standard-auto-current2/20260616T173624Z-wave4-standard-cube-line-medium-905e11e5.json` |
| Standard cube dirty, previous best large baseline | 107 GB ALMA large | 1024 ch, 4096 | 572.315 | n/a | n/a | slab-first / CPU slab | 3 / 347 / 10 | 153.532 | 16.093 | 137.439 | 17.250 | large CASA skipped; used as prior large backend/product baseline | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-recovery-large-dirty-unshifted-ifft/runs/20260614T215220Z-wave1-alma-mosaic-large-standard-cube-line-de8dae03.json` |
| Standard cube dirty, regressed 30 GB plan | 107 GB ALMA large | 1024 ch, 4096 | 923.208 | n/a | n/a | slab-first / CPU slab | 2 / 514 / 10 | 153.490 | 16.051 | 137.439 | 18.498 | superseded; too aggressive laptop memory target and worse backend timing | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-large-dirty-fast-single-sample-large/runs/20260615T042423Z-wave1-alma-mosaic-large-standard-cube-line-1727f925.json` |
| Standard cube dirty, current 24 GB large | 107 GB ALMA large | 1024 ch, 4096 | 524.143 | n/a | n/a | slab-first / CPU slab | 9 / 120 / 10 | 153.781 | 16.030 | 137.439 | 14.170 | large CASA skipped; best large dirty row; modeled source is 16.342 GB and backend still dominates | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-standard-cube-large-24gb/20260616T150436Z-wave1-alma-mosaic-large-standard-cube-line-0531c480.json` |
| Cubedata dirty, medium forced single-worker #311 | 32 GB VLA medium | 512 ch, 2048 | 349.241 | n/a | n/a | slab-first / CPU slab | 11 / 47 / 1 | 47.252 | 30.072 | 17.180 | 7.973 | forced single-worker baseline with `standard_mfs_grid_threads=1`; CASA skipped by design | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-cubedata-forced1-current/20260616T172921Z-wave4-standard-cubedata-line-medium-b38c7e90.json` |
| Cubedata dirty, medium auto #311 | 32 GB VLA medium | 512 ch, 2048 | 146.788 | 1887.410 | 12.86x vs CASA; 2.38x vs forced single-worker | slab-first / CPU slab | 6 / 89 / 10 | 46.511 | 29.332 | 17.180 | 10.496 | correctness from comparable medium CASA row; `.sumwt` non-spatial false positive reclassified good; auto-vs-forced target met | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-311-medium-cubedata-auto-current2/20260616T172006Z-wave4-standard-cubedata-line-medium-1c103335.json` |
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
| Hogbom, deep closeout row | 64 ch, 1024, niter=10000 | 137.109 | 811.307 | 5.92x | grouped Wave 3 Metal with Metal Hogbom minor cycle; CASA-style FFT restoration | 2 / 34 / 10 | recorded in JSON | blocked: restored `.image` edge artifact fixed; remaining `.model`/`.residual` differences are late component-order divergence after deep clean; CPU deep row is also bad against the same CASA products | Rust-only post-fix: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-edge-restore-confirm-rust-only/20260617T133112Z-wave4-standard-cube-line-medium-casa-phase-probe-cf20078a.json`; unchanged CASA comparison row: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-closeout-standard-hogbom-medium64-auto-metal-minor-cycle-casa/20260617T022154Z-wave4-standard-cube-line-medium-casa-phase-probe-0cd0fb24.json` |
| Clark, deep closeout row | 64 ch, 1024, niter=10000 | 32.429 | 409.289 | 12.62x | grouped Wave 3 Metal default with Metal Clark minor-cycle/peak-search and CPU initial dirty/residual refresh | 2 / 34 / 10 | recorded in JSON | accepted by Brian on 2026-06-17 after panel review: `.model`, `.psf`, and `.sumwt` good; small structured `.image` and `.residual` differences acceptable for Wave 4; CASA speed target met but Metal-vs-CPU target missed at 1.13x | Rust-only Metal minor-cycle row: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-metal-minor-cycle-medium64-metal-cpuresid/20260617T171149Z-wave4-standard-cube-line-medium-clean-clark-metal-ea3ec28b.json`; unchanged CASA comparison row: `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-medium64-after-control-fix-nophase/20260617T160130Z-wave4-standard-cube-line-medium-clean-clark-923a9413.json` |
| Clark, 8-channel clean-control probe | 8 ch, 1024, niter=10000 | 16.196 | 62.763 | 3.88x | grouped Wave 3 Metal residual refresh; one CASA cube minor-cycle pass per plane | 1 / 8 / 1 | 0.727 | `.model`, `.psf`, `.sumwt` good; `.image` and `.residual` investigate at 0.14% and 0.23% relative RMS after exact clean-control iteration parity | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-one-pass-control-probe/20260617T154447Z-wave4-standard-cube-line-medium-clean-clark-control-probe-de3e2664.json` |
| Multiscale | 64 ch, 1024, niter=2 | 26.697 | 231.006 | 8.65x | grouped Wave 3 Metal | 2 / 34 / 10 | 4.925 | `.model`, `.residual`, `.psf`, `.sumwt` good; `.image` investigate with normalized RMS 3.11e-6 and accepted visual residual | `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/imager/runs/20260616T062145Z-wave4-standard-cube-line-medium-clean-multiscale-fec0a306.json` |

Deep Clark diagnostics:

- Current 64-channel Metal minor-cycle rerun: casa-rs `32.429 s`, CASA
  `409.289 s`; default-vs-CASA speedup is `12.62x`. The completed bundle is
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-metal-minor-cycle-medium64-metal-cpuresid/20260617T171149Z-wave4-standard-cube-line-medium-clean-clark-metal-ea3ec28b.json`.
- The current row uses `cube_per_plane_workers=10`,
  `cube_per_plane_backend=wave3_metal_grouped`, Metal Clark minor-cycle
  peak-search/subtraction, and CPU initial dirty/residual refresh. There are no
  backend fallback reasons. The CPU residual choice is intentional: the
  measured grouped Metal residual row was slower on this workload.
- Current Metal-vs-CPU evidence: multi-worker CPU is `36.607 s`; Metal minor
  cycle plus CPU residual is `32.429 s` (`1.13x`). An intermediate row using
  Metal minor cycle plus grouped Metal residual was `33.451 s` (`1.09x` vs the
  same CPU row). A pre-chunk Metal command-buffer attempt was rejected because
  no-op command encoding made the 8-channel probe slower (`25.275 s` vs
  `16.215 s` CPU/auto); chunking the command stream restored the 8-channel
  probe to `16.212 s` while reducing peak-search worker time to milliseconds.
- Product comparison after the cube clean-control fix: `.model`, `.psf`, and
  `.sumwt` are good. `.image` is investigate with `diff_rms=0.00907558`,
  `diff_rms_over_casa_rms=0.00164630`, and `diff_abs_max=2.83636`.
  `.residual` is investigate with `diff_rms=0.00981412`,
  `diff_rms_over_casa_rms=0.00246270`, and `diff_abs_max=1.40268`.
  Brian reviewed the `.image`, `.residual`, restored `.model`, `.psf`, and
  `.sumwt` panels on 2026-06-17 and accepted this standard cube Clark
  correctness status for Wave 4.
- In the original 64-channel cube row, casa-rs model occupancy is `522391`
  pixels above `1e-7`; CASA model
  occupancy is `94845`.
- casa-rs model sum-abs is `873793.3 Jy`; CASA model sum-abs is
  `323323.0 Jy`.
- casa-rs residual RMS is `0.986`; CASA residual RMS is `3.990`.
- The original cube row reported `cycle_threshold=0` and
  `IterationLimitReached` for all planes, but follow-up diagnostics found two
  separate issues:
  - The Clark controller now refreshes residuals and continues after an
    internal `CycleThresholdReached` stop when a subcycle updated the model,
    matching the Hogbom/multiscale controller pattern.
  - CASA's active `SDAlgorithmClarkClean2` path calls
    `ClarkCleanLatModel` with `MaxNumberMajorCycles=10` and a capped
    `takeOneStep` request of `2000` when `cycleniter >= 5000`. The Rust Clark
    path now mirrors those limits, and the resident cube executor now finishes
    one CASA cube minor-cycle pass per plane instead of privately running a
    full single-plane Cotton-Schwab loop.
- Post-fix control-probe cube row:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-one-pass-control-probe/20260617T154447Z-wave4-standard-cube-line-medium-clean-clark-control-probe-de3e2664.json`.
  CASA and casa-rs both clean exactly `11755` components across the 8 planes,
  with per-plane iteration vector `[1479, 1480, 1464, 1488, 1452, 1464, 1474,
  1454]`. `.model`, `.psf`, and `.sumwt` are good; `.image` and `.residual`
  remain investigate-level with relative RMS `0.00139` and `0.00230`.
- Control-probe cube row before the controller fix:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-active-fix-casa/20260617T143127Z-wave4-standard-cube-line-medium-clean-clark-control-probe-4bd6eeda.json`.
  CASA cleaned 11755 components across 8 planes, while casa-rs cleaned 12696
  and produced structured `.image`, `.model`, and `.residual` differences.
- Single-channel MFS first-cycle diagnostic with natural weighting:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-single-channel-mfs-trace-natural/residual-divergence-summary.json`.
  The first 50 Clark components agree with CASA to float precision, including
  the initial peak `532.455566 Jy`, confirming the dirty image, PSF, weighting,
  absolute-peak selection, and early component selection are aligned.
- Single-channel MFS deep-cycle diagnostic after the controller refresh fix:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-single-channel-deep-cycle-after-refresh/residual-divergence-summary.json`.
  casa-rs and CASA both reach `iterdone=2000`, but casa-rs still differs in
  `.image`, `.model`, and `.residual`, proving controller refresh was necessary
  but not sufficient.
- Single-channel MFS forced first-cycle depth diagnostic:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/w4-clark-single-channel-cycle1479/residual-divergence-summary.json`.
  Both implementations stop at `1479` components, yet `.image` RMS is
  `6.3069`, `.residual` RMS is `7.3869`, and `.model` correlation is `0.3450`.
  This isolates the remaining difference to the Clark minor-cycle local
  active-pixel subtraction sequence after the first roughly 15-20 matching
  components.
- Rejected candidate: changing the exterior-PSF/sidelobe measurement to an
  inclusive distance-boundary interpretation of CASA's `absMaxBeyondDist`
  worsened the single-channel deep diagnostic
  (`w4-clark-single-channel-deep-cycle-after-extpsf`) and was reverted. The
  previous `max_abs_outside_patch` behavior remains.

Post-fix restore diagnostics for the deep Hogbom row:

- Old Rust vs CASA `.image`: RMS `0.115015`, max abs `29.262769`,
  edge16 RMS `0.448945`, interior RMS `0.029709`.
- New Rust vs unchanged CASA `.image`: RMS `0.029889`, max abs `0.821905`,
  edge16 RMS `0.032524`, interior RMS `0.029709`.
- Worst old edge channel was channel 2: edge RMS `0.819604` before and
  `0.062368` after.
- New Rust vs old Rust changed only `.image`; `.residual`, `.model`, `.psf`,
  and `.sumwt` were bitwise unchanged in the confirmation comparison.
- Bottom-edge visual panel:
  `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/edge-restore-panels/old_new_bottom_edge_diff_ch2.png`.

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
| Standard cube medium dirty auto #311 | 24.000 | 24.000 | 11.160 | 2.758 GB live prepared visibility/source-side buffer, no full visibility cache; 29.332 GB modeled source reads | 17.180 GB modeled product write | 0 | product-backed | Current medium auto-vs-forced-single-worker comparator; 6 slabs, 89 active planes, 10 workers. |
| Standard cube large dirty, 24 GB cap | 24.000 | 24.000 | 14.170 | 7.858 GB source stream buffer, no full visibility cache | 14.205 GB product scratch, 137.439 GB modeled product writes | 0.000 | 137.439 | Best large dirty row: 524.143 s total, 37.337 s source read, 481.638 s backend, 58.435 s product write. |
| Cubedata medium dirty auto #311 | 24.000 | 24.000 | 10.496 | 2.763 GB live prepared visibility/source-side buffer, no full visibility cache; 29.332 GB modeled source reads | 17.180 GB modeled product write | 0.000 | 17.180 | Correctness good; current 10-worker auto gives 2.38x over forced single-worker and supersedes the pre-refactor blocked row. |
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
- Use grouped Metal by default only where measured eligibility supports it.
  Hogbom and multiscale medium rows use the shared per-plane Wave 3 Metal
  backend when eligible. Clark Auto now selects the grouped Metal plan for the
  minor-cycle peak-search/subtraction work, but forces CPU initial dirty and
  residual refresh because the grouped Metal residual-refresh path was measured
  slower on the 64-channel deep row.
- Keep mosaic cube executor capabilities visible. W4-19 now reports
  `mosaic_multi_plane_stream`, `active_planes > 1`, and `worker_count > 1` for
  representative dirty rows; clean rows use the same slab-plane dispatch but
  still miss the performance targets.
- Treat TB-scale as final confirmation only. A future source-major or batched
  backend may still be useful for the remaining large-dirty lower bound, but
  Wave 4 does not need another TB row before that architectural work exists.

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
- Per-plane precomputed spectral assignment lookup. The retained refactor still
  uses compact row bindings and grid-assignment indices to avoid per-row
  `HashMap` lookup and `Arc` cloning in the shared cube/cubedata path.

Closeout decision for #311:

- #311 is closed by the shared standard spectral cube dirty execution refactor
  and refreshed medium evidence. Cubedata dirty now shares the standard cube
  worker/planner path, reaches 2.38x over forced single-worker, and is 12.86x
  faster than CASA on the medium row. A future source-major or batched backend
  remains a plausible architecture direction for the large-dirty lower bound,
  but Brian accepted the current standard/cubedata agreement as sufficient for
  this ticket.

Closeout decision for #317:

- #317 is closed by the W4-19 mosaic dirty multi-plane executor evidence. The
  representative large-turnaround mosaic cube dirty row no longer reports
  `mosaic_single_plane_stream`; auto uses `mosaic_multi_plane_stream` with
  `active_planes=4` and `worker_count=4`, improving total time from 79.572 s
  forced single-plane to 45.486 s auto, or 1.75x. Small CASA comparison bundles
  cover dirty mosaic products, including `.image`, `.residual`, `.psf`,
  `.sumwt`, `.weight`, `.pb`, and `.image.pbcor`. Brian approved closing #317
  on 2026-06-16 with the explicit scope split that mosaic clean/deconvolution
  acceleration remains open under #316 and W4-18.
