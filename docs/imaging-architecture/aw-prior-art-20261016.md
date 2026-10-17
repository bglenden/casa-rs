# AW-projection prior art: what made the pre-refactor VLASS rows fast, and what happened to it

Truth class: non-normative primary-source research
Date: 2026-10-16
Work issue: #449 (the 10x programme)

Research notes, 2026-10-16. A read-only study of the casa-rs repository at
`6a9e83f36b` and its GitHub issues. Nothing was built or run.

The figures in section 4 are the study's own estimates, not measurements. The
open owner decisions this raises are recorded on #449: whether the recovery
contract's landmarks are retired, and how ADR-0016 decision 2 treats a tap-plan
cache.

Abbreviations:
- **HP** is the historical VLASS plan `docs/tutorial-parity/vlass-imaging-optimization-plan.md`
  as it stood at `873776457e^`. It is 8,341 lines and was deleted by `873776457e` on
  2026-08-13. Line numbers below refer to that version.
- **RC** is `docs/tutorial-parity/vlass-imaging-recovery-contract.md` at HEAD.
- **FP** is `docs/imaging-architecture/imaging-foundation-plan-20261007.md` at HEAD.
- **L** is `crates/casa-imaging/src/lib.rs` at `d2d0c0dad5`, the last August
  production commit before the main merge. The crate is 86,874 lines; the same
  symbols are still present in 96,376 lines at the main merge `ba0354ef93`.

## 0. The most important finding: two deletions, not one

The fast VLASS mechanisms were not removed by the imaging-foundation refactor.

**Lineage.** These are verified with `git merge-base --is-ancestor`:

`41caf8cd0a` (07-29 work, pushed 08-02) → `8667b5760d` (casa-c) → `388161fd29` →
`f2abbb98bd` → `4d787cb1fb` (casa-d checkpoint) → `d2d0c0dad5` → `ba0354ef93`
(main merge, 2026-08-13).

**First deletion: the August ADR-0009 cutover.**
- Two commits deleted the whole legacy `casa-imaging` crate (96,750 lines) and the
  `casars-imager` monolith (82,961 lines):
  - `fff9c2d553`, "complete native continuum cutover", 2026-08-25;
  - `27b9b602fe`, "complete final-owner cutover", 2026-08-25.
- The programme behind them was the #486 imaging-architecture programme, with its
  migration matrix and "transfer ratchet".
- At that cutover the migration matrix marked AW as "typed unavailable boundary; no
  production implementation". AW was rebuilt later in T50–T53 (#536–#539). The
  matrix is at `27b9b602fe:resources/imaging-architecture/migration-matrix.json`.
- What went with the crate:
  - the frozen-base image-response cache;
  - the compact replay cache;
  - Metal resident/global/grouped replay;
  - the sparse MT-MFS right-hand side (RHS);
  - radix MADFM;
  - the NEON 2×2 sparse initial grid;
  - compensated Metal AW gridding.
- `git log -S'struct AwProjectImageResponseCache'` shows the struct was added by
  `41caf8cd0a` and removed by `27b9b602fe`.

**What #486 re-implemented.**
- A generic gridded-normal replay:
  - `crates/casa-imaging-reconstruction/src/gridded_normal_operator.rs`, added by
    `712b399f8e` (2026-08-24) and `e413729fa8` (2026-08-28);
  - 40-byte scalar records and 96-byte AW records
    (`AW_GRIDDED_NORMAL_OPERATOR_RECORD_BYTES`, `e714fd2339` version, around line 85);
  - `crates/casa-imaging-runtime/src/complete_data_operator.rs`.
- `complete_data_operator/metal_normal.rs` (`565b79a54d`, 2026-10-04, standard MFS).

**Second deletion: IF-2 under D1.**
- `e7d29c8e5b` (IF-2, 2026-10-07, "run every request through the major-cycle pass and
  delete the old drivers") deleted the gridded-normal replay and `metal_normal.rs`.
- `reconstruction/src/image_response.rs` at `e714fd2339` is **not** the response cache.
  It is the flat-noise/flat-sky PB normalisation (`MinorCycleImageResponse`). It moved
  to products in `e83759240d`.

**When the fast path was last production.**
- The August production freeze (PR #464, merged 2026-08-13) had already chosen
  "role-segmented source-major Metal initial gridding with CPU residual refresh" for
  full geometry.
- The VLASS recovery retrospective (`4a29cc1d52:docs/tutorial-parity/vlass-imaging-recovery-retrospective.md`)
  says the CPU residual backend "produced a scientifically accepted result after
  compressed and Metal-residual candidates failed".
- The accepted 12,150² all-63-field, 16-SPW clean took about 40,412 s against
  CASA-B's 82,351.8 s, about 2.0×. The no-new-swap guard was waived (PR #464; TESTING.md at `6ce6fad837`).

**No pre-IF build ever ran the #625 row (63 fields, 16 SPWs, 4096², niter 2000) for
time.**
- #625 was queued and never activated.
- An August cleaned 4096²/16-SPW/63-field run (r22) passed science (PR #464), but no
  wall time is recorded in the repository, PR #464 or the #454 comments.
- The run logs (r22, v24, the 28.65 s log, casa-c) are not available:
  - `/Volumes/GLENDENNING/casa-rs-vlass/issue-446/` now holds only `data` and `masks`;
  - the NAS preservation copy
    `/Volumes/home/casa-rs-glendenning-preserve/GLENDENNING/casa-rs-vlass/issue-446/`
    holds only input tars (`casa-reduced-clean`, `cf-cache*`, `data*`, `masks`,
    `recovery-references`, `t51…`), no `receipts/runs` or `recovery-candidates`.
  - Everything below therefore rests on repository and issue text.

**Governance finding.**
- RC lines 530–569 still require that the single-field landmarks be preserved, and say
  a landmark capability may leave the lineage only when generalised, superseded on the
  same landmark, or "explicitly retired by Brian" (RC:563–568).
- The capability was deleted twice: at the August cutover and by IF-2.
- I found no retirement record in:
  - #445, #454, #486 or #487 (bodies and comments, searched for landmark, 28.65,
    retire, response cache and preservation);
  - the cutover migration matrix;
  - HEAD docs.
- RC at HEAD is therefore stale against the code.

## 1. The three historical results

### 1a. casa-c: 63 fields, SPWs 2/7/12/17, 4096², dirty: 225.10 s against 541.35 s (2.405×)

**Build.** `8667b5760d` ("fix(imager): align dirty VLASS product metadata"). The
binary's SHA-256 is `3a8d671a…`.
- Source: RC lines 339–351; `tools/perf/imager/vlass_recovery_launch_ledger.json`,
  entry `REDUCED-ALL63-DIRTY-4096-4SPW-001`.

**Run configuration.** From the runner at the build commit,
`8667b5760d:tools/perf/imager/experiments/run_vlass_dirty_4096_all_fields_four_spw.sh`:
- execution label `release-metal-grid2-fftw-f64-t8-memory-auto-16GiB`;
- `--standard-mfs-acceleration metal --parallel --standard-mfs-grid-threads 2
  --imaging-prepare-workers 1 --imaging-read-ahead-blocks 1`;
- FFTW f64 with 8 threads, Homebrew FFTW;
- memory target 16 GiB;
- CASA 6.7.5.9's warm CF cache (`cf-cache/6.7.5.9/db96e297…`).

**Mechanism.** The initial grid is the compensated Metal AW gridder, described at HP
lines 302–332:
- two signed u32 atomic limbs per component, plus a second f32 compensation plane;
- `AwProjectMetalCompensation`, L:14049; `awproject_metal_fixed_scales`, L:17279.
- The automatic policy that selects it is `standard_mfs_auto_metal_decision`,
  `8667b5760d:crates/casars-imager/src/lib.rs:25149`, reason `"selected_awproject"`.

**Timing.** Core 220.513 s, of which 217.063 s is the initial PSF grid. Peak RSS 10.0
GB, footprint 13.9 GB, no swaps.

**Like-for-like.**
- The selection, geometry, 18 products and the warm CASA cache are the same as CASA's.
  This is an exact matched pair under the v2 scientific contract (RC lines 385–408).
- With niter=0 there are no major cycles. Replay and response mechanisms are
  irrelevant here; this row only measures the initial grid.
- It is not a "fast" landmark: 2.4× was a recorded performance warning.

### 1b. casa-d: 63 fields, SPWs 2/7/12/17, 4096², niter 2000: 257.40 s against 3,470.197 s (13.48×)

**Build.**
- The measured binary was built from base `f2abbb98bd` plus then-uncommitted changes:
  `/private/tmp/casa-rs-vlass-autoresearch/target/release/casars-imager`, SHA
  `82b01c39…`.
- Those changes were committed as checkpoint `4d787cb1fb` (2026-08-05, "perf(imaging):
  promote separable VLASS phase replay", +2,700 lines in L).
- Run id `20260805T-vlass-all63-clean-4096-four-spw-separable-phase-f2abbb98b-v24`.
- Sources: RC lines 612–656; ledger entry `REDUCED-ALL63-CLEAN-4096-4SPW-SEPARABLE-001`;
  HP lines 7832–7914.

**Run configuration.**
- Initial grid: "the existing exact 192-pixel dynamic sparse CPU initial-grid path,
  seven workers on this host, AArch64 NEON 2x2 updates, and a 512 MiB logical tap
  arena … 112 windows … 119.656 s" (HP:8270–8274).
- Residual majors: Metal replay of a resident compiled AW program with separable X/Y
  phase axes:
  - global replay program 5,224,778,300 B;
  - builder 6,560,435,928 B;
  - peak absorb 7,528,750,264 B.
- Runner flag set, `4d787cb1fb:tools/perf/imager/experiments/run_vlass_clean_4096_all_fields_four_spw.sh`:
  - `standard_mfs_acceleration=metal`;
  - CASA 6.7.5.18's bundled FFTW library;
  - CASA 6.7.5.9's CF cache (warm);
  - env flags `CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GLOBAL_TILE_REPLAY`,
    `…METAL_GPU_RESIDUAL_REPLAY`, `…HYBRID_CLEAN`, `…PREDIVISION_SOURCE_PHASE`,
    `…RAW_FRAME_TAYLOR`, `…RESIDUAL_ONLY_REFRESH`, `…REPLAY_RETENTION_BYTES`,
    `…IMAGE_RESPONSE_CACHE`, `MT_MFS_SPARSE_RHS`, `RADIX_MADFM`,
    `PARALLEL_MODEL/RESIDUAL_TERM_FFT`, `SPARSE_MASK_PEAK_SEARCH`,
    `CACHE_REFRESHED_NSIGMA`.
  - The exact set used by v24 is in its run log, which is not in the repository.
  - Only the separable-phase encoding was made a non-flag production default.

**Stages.**

| Stage | Seconds |
|---|---|
| Core | 250.10 |
| Initial PSF grid | 119.66 |
| Residual degrid/grid | 104.82 (≈9.5 s a residual major) |
| Major refresh | 113.96 |
| Minor cycles | 9.36 |

**Like-for-like.**
- The manifest is the same as CASA's (`tools/perf/imager/workloads/vlass-fragment-all-fields-clean-4096-four-spw-casa.json`):
  - AWProject, 32 w-planes, POINTING, A/WB/conjugate beams;
  - Briggs 1, MT-MFS with nterms 2, scales [0,5,12];
  - the same mask, nsigma 5, cyclefactor 3, flat noise, 19 products.
- CASA used the warm CF cache (`cf_cache_role: warm`). casa-rs read the same CASA
  6.7.5.9 cache, so CF generation was outside both timings.
- 193 components on both sides.
- casa-rs "12 recorded major cycles" counts the initial pass. The later traces at
  `d2d0c0dad5` align 11 CASA boundaries with 11 casa-rs boundaries (HP:8239, 8317).
- Science passed the approved v2 mask-topology contract: image tt0/tt1 NRMSE 7.4e-5
  and 9.7e-5.
- Not like-for-like with today's #625:
  - 4 SPWs, not 16;
  - Metal plus about 7 CPU workers, not serial CPU;
  - experimental env flags;
  - the CASA-bundled FFTW library.
- No casa-rs row of this exact shape has been run on today's build.

**Later August lineage on the same row.** HP lines 8226–8320:
- The exact-support grouped replay at the first measurement took 515.29 s (6.73×),
  because the initial-grid planner regressed to Metal with 256 MiB CF residency
  (29,220 CF loads, 0 hits).
- `d2d0c0dad5` restored the sparse CPU initial grid: 268.49 s (12.92×).
  - Initial grid 132.85 s; residual 101.28 s.
  - Grouped program resident, 2.81 GB, zero spill reads.
  - Requested 7 workers, effective 4.

### 1c. Single-field landmarks: field 1525, 4096², niter 2000

**Contract.** RC lines 530–569 and the `performance_preservation` member of
`tools/perf/imager/vlass_recovery_contract.json`.

#### Four SPWs (2/7/12/17): 28.65 s against 3,631.81 s, "nominally 126.76×"

**Build.**
- The commit is not recorded; the contract binds only the run log's SHA-256 (`f07d3b87…`).
- The code was squashed into `41caf8cd0a` (author date 2026-07-29, committed
  2026-08-02). Its message reads "improving the same-host exact control from 143.15s
  to 28.65s".
- Run log: `20260729-vlass-clean4096-4spw-n2000-image-response-radix-madfm-v4.log`
  (HP:3315–3409).

**Run configuration.**
- Metal AW (`--standard-mfs-acceleration metal`), grid threads 2, FFTW f64 with 8
  threads.
- Env flags `IMAGE_RESPONSE_CACHE`, `MODEL_DELTA_CENSUS`, `RADIX_MADFM`,
  `CACHE_REFRESHED_NSIGMA`, `SPARSE_MASK_PEAK_SEARCH`, `PARALLEL_MODEL_TERM_FFT`,
  `SPARSE_MODEL_PREP`. These are from the later commit-bound landmark runner,
  `873776457e^:tools/perf/imager/experiments/run_vlass_landmark_single_4096_four_spw.sh`.
- Also used: the sparse MT-MFS state, 4 GiB replay retention and global Metal tile
  replay (HP:5151–5160).

**Mechanism.** The frozen-base image response (HP:3315–3358).
- Model-delta census: 167 late major cycles update only pixel (581, 2143).
- After two stable cycles, one exact refresh is run. A central ±0.125 response is then
  measured for each input Taylor term with the exact operator (4 extra exact applications).
- Later residuals are synthesised as base − Δm·H·e_p. The cache is invalidated on any
  new delta position, and the final refresh is always exact.
- The successor hybrid run (29.43 s) did **11 exact refreshes over 171 major cycles**
  (HP:7088–7092).
- CASA did 171 full degrid/grid major cycles (HP:3046–3049).

**Stages.**

| Stage | Seconds |
|---|---|
| Core | 24.81 |
| Major refresh | 11.15 |
| Controller | 4.57 |
| Sparse minor | 2.33 |
| Initial PSF grid | 3.55 |
| Model FFT | 2.07 |
| Residual degrid/grid | 5.59 |
| Residual FFT | 2.34 |

**Not like-for-like.**
1. **Different hosts and CASA versions, as far as the evidence shows.**
   - The evidence indicates that the CASA oracle ran CASA **6.7.5.18** on the Mac mini
     (M2). This is an inference: GLENDENNING is an external SSD, so a GLENDENNING path
     alone does not prove the laptop. The oracle script
     `873776457e^:tools/perf/imager/experiments/vlass_reduced_casa_clean_4096_four_spw.py`
     writes to `/Volumes/Extra Storage (not encrypted)/…` with `"casa_version": "6.7.5.18"`.
   - Its products were later "copied from the Mac mini to GLENDENNING" (HP:5159).
   - casa-rs's 28.65 s ran on the M4 laptop (GLENDENNING paths; HP:3317 "same-host"
     refers to the casa-rs control only).
   - The same-host figure is the c238 seed on the mini: 46.14 s with 2,000 components
     and 171 cycles, using the same four-SPW runner SHA `1a9ba23a…`. That is **78.7×**.
     Source: `tools/perf/imager/vlass_recovery_salvage_catalog.json`.
2. **Compared only with the casa-rs control.**
   - The 28.65 s run was compared with the exact casa-rs control, not with CASA; its
     "promotion" was retracted on 2026-07-30 (HP:3396–3404, 5151–5221).
   - The V6 29.82 s rerun against CASA was 5.7 ppm, with 16 alpha pixels.
   - The scientific-floor pass is the **29.43 s** hybrid run (HP:7418–7499; receipt
     `20260731-vlass-4096-4spw-scientific-floor-v2.json`).
3. **Experimental and single-position.** The response cache was env-gated (L:6436) and
   admitted only for a single pending delta position (L:6810–6816). It never admitted
   the 63-field trajectory (RC:536–539; HP:7765–7775).
4. **Workload otherwise the same:** the same recipe, mask, products and real CLEAN
   from zero.

**Later production rebaseline.**
- The grouped architecture took 38.5 s (94.3× nominal), with the ceiling at 42.35 s
  (owner, 2026-08-07, HP:8113–8117).
- That run then failed science: its CF support was cropped at 1e-6 energy, giving
  NRMSE 0.05–0.15 (HP:8131–8147).
- The exact-support rerun took 55.40 s (65.6× nominal) and passed science
  (HP:8160–8197).

#### All 16 SPWs: 101.646 s

**Build and evidence.**
- v59 is the full-16 promotion, HP lines 7500–7572 and 3629–3640.
- Runner salvage: `16f4487c18` ("Promote the VLASS full-16-SPW candidate", 2026-07-31)
  and PR #457 (78.89 s on the mini).
- CASA reference: 1,690.1155 s, CASA 6.7.5.18 on GLENDENNING, that is the laptop
  (`vlass_reduced_casa_clean_4096_full_16_spw.py`; HP:8204).
  - The ratio is derived: about 16.6×. The contract records no CASA time for this
    landmark.
  - The later exact-support grouped build took 86.58 s (19.5×) at `761a2d8b57`.

**Stages.**

| Stage | Seconds |
|---|---|
| Core | 95.0 |
| Initial PSF grid | 60.5 |
| Minor | 4.5 |
| Residual refreshes | 5.4 |
| Product writes | 3.1 |

- 5 major cycles and 641 components, matching CASA's 5.
- Retained replay programs: 6.98 GB at v21 (HP:3417–3427); 9.93 GB at `761a2d8b57`.

**Like-for-like.** Same host and the same CASA version family. CASA ran only 5
majors, so most of the speedup is the initial grid plus the cheap Metal replay
refreshes.

### 1d. Related rows that bear on the #625 shape

- **The full-16-SPW, 63-field residual-refresh envelope** (RC:59–103):
  - CASA `tclean` with `restart`, `calcres`, `niter=0` took 1,265.70 s.
  - casa-rs "exact tile-ready replay" took 132.34 s, 9.56×.
  - The contract itself calls it a favourable upper bound: casa-rs stops at the
    tile-ready boundary, before FFT and normalisation.
  - The grouped replay later took 65.17 s, 19.4×, also not matched (RC:684–690).
  - It is the only 16-SPW all-field casa-rs datum from before the refactor.
- **12,150² all-63-field, 16-SPW clean** (Aug 12, r4): about 40,412 s against CASA-B
  82,351.8 s, about 2.0×. It used Metal initial grid plus CPU residual (PR #464).
- **Residual-replay memory** (PR #461 body): the frozen Full16 63-field
  residual-replay discriminator went from footprint 19.82 GB and resident 15.16 GB
  (incumbent) to footprint 17.82 GB and resident 9.29 GB (grouped). Both footprints
  are above the 16 GiB policy.
- **Today's #625 at `01ceae21eb`, CPU W1** (#659 comment 6204573957):
  - 4,764.57 s; 7 majors; 390 iterations;
  - passes: initial 603 s, then 655 / 651 / 679 / 692 / 719 / 682 s;
  - footprint 7.08 GB.
  - Restricted to SPW 13 alone, a pass takes 41.0 s (#659 comment 6174692598).

### 1e. Was anything precomputed outside the timed run?

| Row | CF cache | Replay program | FFTW wisdom | Verdict |
|---|---|---|---|---|
| casa-c 225.10 s | Warm CASA 6.7.5.9 cache on both sides | n/a (niter 0) | none named | Matched |
| casa-d 257.40 s | Warm CASA 6.7.5.9 cache on both sides (manifest `cf_cache_role: warm`; runner `cf-cache/6.7.5.9/db96…`) | Built inside the timed run during the initial pass (builder 6.56 GB; part of the 119.7 s) | not named | Matched boundary |
| Single field 28.65 / 29.43 s | Each side used its own 6.7.5.18 cache, warm | Built inside the run | The exact control log is named `…fftw-wisdom-f64-exact-v1`. HP:8023–8049 says the 5,073-byte wisdom file is "a later reproducibility pin rather than an activity claim earned by the original 28.65-second log" | Workload matched; host not |
| Residual envelope 132.34 s | Warm on both sides | Unknown whether the program build was inside the boundary; casa-rs stops at "tile-ready replay" | — | Favourable upper bound (RC:98–103) |
| Today's #625 4,764.57 s | Warm native catalog on casa-rs; warm CASA cache on CASA. Cold adds 159.8 s | None | none | Matched |

## 2. Mechanisms that made those rows fast

| # | Mechanism | Code at `d2d0c0dad5` (L unless noted) | Doc text | Used by |
|---|---|---|---|---|
| M1 | **Frozen-base image response** (exact columns H·e_p for one pixel, per Taylor input×output term pair; 4×N²×f64 = 512 MiB at 4096²) | `AwProjectImageResponseCache` L:5044; `from_central_calibrations` L:5061; calibration and activation L:6810–6898; final synthesis L:6949; env `CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE` L:6436 | HP:3315–3358; RC:536–539, 573–585 | 28.65 / 29.43 s (single field); inert on 63 fields |
| M2 | **Compact source-order replay cache**: per-sample plans and packed AW tap bundles retained across majors | `AwProjectCompactReplayCache` L:10429; `AwProjectCompactReplayWindow` L:10295; `replay_awproject_compact_window` L:35184 | HP:3107–3140 (residual refresh 7.86 → 2.10 s); HP:3076–3088 (tap rematerialisation 758 s, CF loads 585.5 s, 48 hits in 143,926 loads) | all clean rows |
| M3 | **Metal resident replay program**: per-sample observed visibilities, prediction plans, interned kernels and phases; separable X/Y phase atlas. The MS is not touched in residual majors; residual = V − A·m per sample on the GPU | `AwProjectMetalResidentProgram` L:12223; `AwProjectMetalPredictionSample` L:11557 (holds `first/second_observed_re/im`); `build_awproject_metal_global_replay_program` L:26572; `replay_awproject_metal_global_program` L:34351 | RC:612–622; HP:7832–7865 (17.69 → 5.22 GB) | casa-d 257.4 s; landmarks |
| M4 | **Grouped AOT tile replay**: exact-support compiler, sorted groups, persistent integrity-checked spill, next-use-aware residency | `compile_awproject_metal_aot_grouped_tile` L:24333; `refresh_awproject_metal_aot_grouped_tile` L:24582; `replay_awproject_metal_{spilled,resident}_global_programs` L:34961 / 35048 | RC:658–707; HP:7916–8170; PR #461 | 268.49 s; 55.4 / 86.6 s |
| M5 | **Sparse multi-worker CPU initial grid**: 192-pixel tiles, dynamic scheduler, NEON 2×2, 512 MiB tap arena | `AwProjectInitialGridPlan` L:5749; `resolved_awproject_initial_grid_plan` L:5766; `AwProjectNeonReplay2x2` L:15219 | HP:8265–8312 | casa-d initial pass, 119.7 / 132.8 s |
| M6 | **Compensated Metal AW gridding**: two u32 fixed-point limbs plus an f32 compensation plane, f64 readback | `AwProjectMetalCompensation` L:14049; `awproject_metal_fixed_scales` L:17279 | HP:302–332 | casa-c; single-field initial grids |
| M7 | **Mask-sparse MT-MFS RHS**: 384 MiB → 110,688 B of minor-cycle state; minor cycle 1.95 s | L:59640–59760, env `CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS`; `build_sparse_mtmfs_scale_search_positions` L:60500 | HP:3206–3293 | 171-cycle rows |
| M8 | **Exact radix MADFM**, cached refreshed n-sigma, sparse mask peak search; statistics 6.27 → 3.70 s over 171 cycles | `radix_madfm_f32` L:67384 | HP:3352–3358 | 171-cycle rows |
| M9 | **CF locality**: group samples by RR/LL CF quartet; plus prefetch, planning workers and pack workers from the 08-04 autoresearch | (08-04 commits `9254efe960`, `cd745127a5`, `2040a07635`, `f2abbb98bd`) | HP:276–284 (5,861 → 796 CF loads) | all |
| M10 | **FFT details**: f32 casacore-layout model FFT, parallel per-term FFTs, FFTW wisdom | env `PARALLEL_*_TERM_FFT` | HP:3120–3157, 8023–8049 | landmarks |

**Where the wall time went.**
- casa-d: an initial grid of about 120 s (M5) plus about 9.5 s for each of 11 Metal
  residual majors (M3).
- Today's CPU W1 pass is about 41 s per SPW, so a 4-SPW pass is roughly 165 s. That is
  an estimate scaled from #625; it has not been run.
- The per-major gap is about 17×. That figure confounds three things:
  - GPU against CPU;
  - multi-threaded against W1;
  - precompiled per-sample programs (M2/M3) against on-the-fly preparation.
- What it is not is I/O avoidance. #625's MS reads are cheap: the density-pass reads
  take 0.6 s (#659 comment 6207741158).
- The single field's 126× came mostly from M1, which removed about 160 of 171 exact
  passes, together with M7, M8 and M10, which cut the per-cycle fixed costs.

## 3. Fate of each mechanism

| Mechanism | August → #486 | #486 → IF | Stated reason |
|---|---|---|---|
| M1 response cache | Deleted at `27b9b602fe` and never ported | Not present; never discussed by name | The August cutover's transfer ratchet. RC:563–568 forbade silent retirement of landmark capabilities; I found no record of an owner retirement approval for the cutover. The #659 catalogue rejects "an image-domain residual (dirty image minus gridded model)" because it "cancels nearly equal f32 grids and breaks CASA's interpolate-then-subtract order" (comment 6134710065). That is a different construction, but the same argument would be raised. |
| M2 compact replay, M3 Metal program, M4 grouped | Deleted at `27b9b602fe`; its conceptual successor (a different design, not a port) was the gridded-normal replay (`gridded_normal_operator*`, `complete_data_operator*`, `metal_normal.rs`) | Deleted by IF-2 `e7d29c8e5b` under **D1** (FP:71–87; deletion row FP:1025). Reaffirmed at R3 ("D1's re-traversal stands, and nothing argues for a replay cache", FP:1593) and at R4 ("the MAIN-walk profile does not justify a visibility or replay cache", FP:1695). | D1: "not scientifically required"; about 40 B per row-channel, an 86 GB Wave3 reservation against a 36 GB input; valid only while weighting is frozen; forms the residual as a difference of two f64 images. ADR-0016 decision 2 (lines 38–41) and alternative 1 (lines 113–115). §5.9 outcome: "a tap-plan, record or placement cache across majors was rejected by design (D1, R3)" (FP:972–976). The #648 results say it was "not measured", and give deep32 cube sizes (2.6–8.9 GB a major). |
| The narrow part kept | — | `WeightCache`: 4 B per selected row and channel, one domain, admitted at 1/16 of free memory (`crates/casa-imaging-runtime/src/pass/weights.rs:17`; `crates/casa-imaging-application/src/imaging/cycle.rs:770–790`; #659 deviation 7) | — |
| M5 sparse CPU initial grid | Deleted | Superseded by `Partition::regions` grid strips for constant and Taylor bases (`cycle.rs:869–883`) plus CpuBackend's seven-tap/dense kernels | One driver (D4/D5 consolidation) |
| M6 compensated Metal AW grid | Deleted | Metal backend accumulates f32 with atomics (D2). The AW kernel set is refused on Metal: `Unsupported::KernelSetOnMetal` (`crates/casa-imaging-application/src/availability.rs:32–34, 217–219`); `auto` keeps AW on the CPU (#723, deviation 14) | #653 (IF-4) open: "W, mosaic and AW wait for gate R2" |
| M7 sparse RHS, M8 radix MADFM | Deleted | Not ported; the Taylor solver and `PlaneStatistics` are in `casa-imaging-deconvolution` | Never production defaults (env-gated) |
| M9 CF locality and CF cache | The prepared-artifact store replaced the packed CF | **D3**: CASA CF images in a bounded LRU (FP:108–114). **Deviation 10**: walk one DATA_DESCRIPTION at a time, which fixes the thrash (#625 15,498 → 4,764.57 s). Factored native generation is 10.8× faster (#648 rows 19 and 20). Owner, 2026-10-11: the cfcache is not an interop contract. | Kept in spirit and improved |
| M10 FFT | — | NEON FFTW; per-plane FFTs single-threaded on the team (FP:962) | — |

## 4. Assessment: what could come back, and what it might buy

**Constraints throughout.**
- One measurement operator and one pass (ADR-0016 decisions 1 and 2).
- `admit()` before any allocation (D5).
- No env vars in library crates (D9 / ADR-0016 decision 9).
- Device code only in `casa-imaging-metal` (ADR-0016 decision 10).
- The 16 GiB native policy.
- #625's own bar is serial W1. Report serial gains and parallel or GPU gains separately.
- The estimates below are mine. None of them is measured.

1. **AW kernel set on Metal (#653).** This is the largest lever for the casa-d/#625
   shape.
   - August data: about 9.5 s per residual major for 6.4 M samples on the M4 GPU,
     against an estimated 165 s at CPU W1 today.
   - It fits the existing `GridBackend`/`Work::NativeResidualGrid` design; no ADR
     change is needed.
   - Risks:
     - D2's plain f32 atomics. August needed compensated accumulation (M6), and an f32
       Metal residual "failed scientific comparison decisively" at 12,150² (salvage
       catalog, `exact-metal-direct-cf-f32-residual`).
     - Wide-support atomics contention: the R2 measurement #653 asked for.
     - Host preparation becomes the bottleneck. The 08-07 initial pass spent 155 s on
       source preparation against 136 s on Metal.
   - It needs T0/T1 Metal AW laws and possibly an owner refinement of D2
     (compensated f32).
   - Plausible result: residual passes at August's 65–132 s plus an initial pass at
     August's best extrapolated figure (about 480–530 s) give about 870–1,320 s, which
     is 7–10×.
   - 10× needs the initial pass to fall well below that too, for example a Metal
     initial grid without the CF thrash August hit. That is unmeasured.
   - If only the residual passes move, the cap is about 7.5× (see the budget below).
2. **Multi-worker CPU AW.**
   - This already exists through `Partition::Regions`. #625 was timed only at W1.
   - It is low conflict: measure W4 and charge the per-holder CF LRU
     (`AwCatalog::charge_bytes`).
   - Plausible: 2.5–3.5× per pass, so #625 at about 1,400–1,900 s, roughly 5–6×.
     It does not count toward the serial bar.
3. **A bounded tap-plan (placement) cache.**
   - What it holds: per-sample CF key, placement and pointing phasor, without taps or
     values.
   - ADR-0016 decision 2 explicitly permits "a bounded tap-plan cache … as a measured
     execution strategy". The FP §5.9 and #648 records still list it as rejected by
     design, so reopening it is an owner (Ask First) decision.
   - Size: about 25 M samples on #625 at 16–32 B is 0.4–0.8 GB. It fits
     WeightCache-style admission.
   - Gain: about 10–30% per pass on the CPU. Gridding dominates, and the per-row
     pointing conversion is about 10 s a pass. On Metal it would be much larger,
     because it removes the host-preparation bottleneck.
4. **A full record or program replay** (observed V plus plans, as in M3/M4).
   - D1's three objections apply unevenly to the August per-sample design:
     - **"Residual as a difference of two f64 images":** this described the #486
       gridded-normal operator. The August Metal program formed V − A·m per sample;
       `AwProjectMetalPredictionSample` holds the observed values (L:11557). It
       therefore kept CASA's interpolate-then-subtract order.
     - **"Valid only while weighting is frozen":** this is already accepted for
       `WeightCache` (deviation 7).
     - **Memory:** this depends on workload and representation. Wave3 needed 86 GB at
       40 B per sample. #625 at the #486 AW record size of 96 B per row-channel would
       be about 4 GB (about 42 M row-channels; 25.0 M accepted samples in r22). The
       measured M3/M4 representations were heavier: 9.3–15.2 GB resident and
       17.8–19.8 GB footprint for the 16-SPW 63-field residual (PR #461).
   - It remains Ask First, because ADR-0016 alternative 1 rejects it, so the ADR would
     have to be edited.
   - ADR-0016 decision 2 is internally in tension. It says "no replay cache of
     visibility-independent factors" yet allows "a bounded tap-plan cache", which is
     exactly a cache of visibility-independent factors. The owner should resolve that
     wording before item 3 or item 4 is attempted.
   - Spilled replay was slower: 14.3 GB reread a refresh made it 2× slower than
     resident (HP:8000–8006).
   - Worth it only after item 1, and only if host preparation then dominates.
5. **The frozen-base response (M1), generalised to a bounded set of delta positions**
   with exact shadow cycles and fallback (RC:570–585; HP:3405–3414 oracle
   requirements: dyadic calibration, a held-out exact shadow cycle, coefficient
   trust regions, decision-margin certificates, exact fallback).
   - It is not named in D1, and it is mathematically exact for the linear H up to
     rounding.
   - It lives in the runtime's major/minor loop, and calls `run_major_cycle` for the
     calibration columns.
   - Cost: 512 MiB per position at 4096² and about 4.4 GiB at 12,150², so only a few
     positions can be admitted.
   - Buys a lot on many-cycle single-field rows: 171 → 11 exact passes. Little on
     #625 (7 majors) or casa-d (11 majors), whose model delta is not single-position.
   - It needs owner sign-off against the #659 "image-domain residual" rejection.
6. **Sparse MT-MFS RHS (M7), radix MADFM and cached n-sigma (M8).**
   - These are solver-internal (`Solver::State`, `PlaneStatistics`) with no
     architectural conflict.
   - They matter only on 171-cycle rows, worth seconds per cycle; they are negligible
     on #625.
7. **M5 and M10.** Today's Regions partition plus NEON FFTW cover most of them.
   Parallel per-term FFTs are a small fixed-cost win on many-cycle rows.

**The 10× budget for #625**, using 16-SPW data where it exists:
- 10× on #625 is about 907 s in total. Today: initial pass 603 s plus six residual
  passes of 651–719 s, all at CPU W1.
- **Initial grid.**
  - The best August initial grid was the multi-worker sparse CPU path, 120–133 s at 4
    SPWs on 63 fields.
  - Scaled by SPW count to 16 SPWs, that is about 480–530 s. This is an estimate: the
    per-SPW cost was roughly constant today, 41 s per SPW per pass.
  - The 16-SPW single-field v59 initial grid was 60.5 s, but that is 1/63 of the rows.
- **Residual passes.** The only 16-SPW all-field residual figures are 132.34 s
  (incumbent) and 65.17 s (grouped). Both stop before FFT and normalisation.
- **Total.** About 480–530 + 6 × (65–132) ≈ 870–1,320 s. The best August mechanisms
  therefore extrapolate to roughly the 10× line, or short of it, on #625. They were
  never run there, and the resident replay footprint is 17.8–19.8 GB.
- **The initial grid is as large a lever as the residual.** Today's 603 s initial
  pass alone uses two-thirds of the 907 s budget. Accelerating only the residual
  majors, to about 100 s each, caps #625 near 9,074 / (603 + 600) ≈ 7.5×.

**No row passes 10× on the serial CPU alone.** The single-field 4-SPW row is likely
bound by its roughly 172 passes of FFT and normalisation fixed cost. It has not been
measured on today's build, and its CASA time is the mini's.
