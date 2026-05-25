# ImPerformance Wave 2 Acceleration Ledger

Truth class: current descriptive
Last reality check: 2026-05-25
Verification: `bash -n scripts/bench-imager-vs-casa.sh`; `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/stage_wave1_datasets.py tools/perf/imager/test_run_workload.py tools/perf/imager/test_stage_wave1_datasets.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py tools/perf/imager/test_run_workload.py`; `cargo test -p casa-imaging paired_f64_product_grid_matches_separate_updates --lib`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks --lib`; `cargo test -p casa-imaging weighting --lib`; `cargo test -p casa-imaging streaming_density_samples_match_batch_density_weighting --lib`; `cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `cargo test -p casa-imaging positive_tap_span_reconstructs_legacy_positive_taps --lib`; `cargo test -p casa-imaging compact_positive_tap_grid_and_degrid_match_product_taps --lib`; `cargo test -p casa-imaging fused_residual_refresh_matches_separate_degrid_grid --lib`; `cargo test -p casa-imaging standard_mfs_plan_buckets_gridder_accepted_samples --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casa-imaging degrid --lib`; `cargo test -p casa-imaging standard_mfs_thread_count_parser_accepts_numeric_and_auto_values --lib`; `cargo test -p casa-imaging standard_mfs_metal_backend_selection_is_explicit_and_gated --lib`; `cargo test -p casa-tables tiled_selected_row_reads_reuse_shared_tile_cache --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager --example profile_imager`; `cargo build --release -p casars-imager --example profile_imager`; `just quick`; `just docs-check`; `git diff --check`; selected `tools/perf/imager/run_workload.py` and `profile_imager` runs listed below, including the positive compact tap paired profile, bounded serial attribution probes, final full-shape one-worker profiles on 2026-05-20, and bounded fixed-tile single-worker density-direct profiles on 2026-05-22

Wave issue: #263
Child issues: #264, #265, #266, #267

This note records the first ImPerformance Wave 2 acceleration pass on the
correctness-green full-medium VLA standard-MFS target from Wave 1.

## Retained CPU Backend Decision

Current retained standard-MFS CPU backend:

```text
Tile anchor: CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_quadrants
Worker count for retained medium heavy-clean evidence: 4
Residual metadata cache: not retained
Manual spatial/temporal hot splitting: not retained
Adaptive per-chunk tiles: not retained
Queue snapshot plotting hooks: not retained
Detailed experiment summary: docs/tutorial-parity/imperformance-wave-2-experiment-summary.md
```

The retained path uses four center-quadrant fixed halo tiles, direct resident
tile updates, row/channel routed visibility runs, and the fixed-tile inbox
scheduler. This supersedes the diagnostic experiments later in this ledger
where they mention temporal split, selected quadtree split, adaptive chunking,
queue snapshots, or residual metadata caching as selectable env-driven paths.
Those controls were useful evidence-gathering tools but are no longer part of
the production-facing Wave 2 backend surface.

## Fixed-Tile Backend Checkpoint

The first bounded standard-MFS fixed-tile backend is retained as an
environment-gated correctness checkpoint, not as a performance claim:

```text
Backend flag: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
Resident budget: standard_mfs_memory_plan fixed_tile_resident_bytes
Budget override: CASA_RS_STANDARD_MFS_TILE_RESIDENT_MB=<MiB>
Debug resident limit: CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=<count>
Timing artifact: none accepted yet
Decision: retain for bounded streaming backend development
```

The implementation routes standard-MFS PSF/dirty and exact residual refresh
through fixed rectangular halo tiles. It avoids the retained
`StandardMfsVisibilityPlan` executor when the fixed-tile backend flag is set,
and `CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1` exercises deterministic tile
eviction and full-halo merge behavior. The current core API still receives
weighted batches, so frontend streaming and two-pass weighting remain future
work before making a full memory-scaling claim.

Follow-up planner checkpoint:

```text
Core handoff: StandardMfsExecutionConfig.fixed_tile_resident_bytes
Default budget: 512 MiB when CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
Timing artifact: none accepted yet
Decision: retain; tile residency is now planned centrally instead of defaulting to all tiles resident
```

The frontend profile output now includes `fixed_tile_resident_bytes` and
`fixed_tile_resident_limit` in `standard_mfs_memory_plan_actual`. The fixed-tile
core converts the byte budget into a deterministic tile count from the actual
padded grid and halo geometry, with `CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT`
kept only as a direct debug/correctness override.

Correctness checks recorded for this checkpoint:

```text
cargo check -p casa-imaging
CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
cargo test -p casars-imager standard_mfs_memory_planner_reserves_fixed_tile_residency_when_enabled --lib
```

Memory-control repair checkpoint:

```text
Full-shape artifact: target/imperformance-wave2/memory-control-repair-20260521/full-shape-10w-memory-target-16g.log
Plan-log smoke artifact: target/imperformance-wave2/memory-control-repair-20260521/plan-log-smoke.log
Default memory target: total physical memory / 2
Override: CASA_RS_STANDARD_MFS_MEMORY_TARGET_MB=<MiB>
Tile residency default: planner-selected all-resident fine tiles when they fit the active target
Streaming status: fixed-tile standard-MFS clean now replays bounded row blocks through the core runner
Correctness: targeted tests below
Decision: retained as the current fixed-tile memory-control shape
```

The memory planner now queries physical RAM (`hw.memsize` on macOS, `sysconf`
on Unix) and defaults the standard-MFS active-memory target to half of that
value, with `CASA_RS_STANDARD_MFS_MEMORY_TARGET_MB` as the explicit benchmark
override. The `standard_mfs_memory_plan_actual` record includes
`system_memory_bytes`, `memory_target_bytes`, and `memory_target_source`.
Planner accounting now subtracts live row-block and bucket bytes before sizing
the prepare buffer, and fixed-tile runs reject plans that exceed the target
instead of silently overcommitting.

The default fixed-tile CPU path no longer allocates per-task dirty/PSF/residual
tile arrays when the planner can keep all stage tiles resident. Workers update
resident halo-padded tile buffers directly and flush once at stage end. The old
scratch tile-buffer path remains available only when the resident tile limit is
below the full tile count, which keeps deterministic eviction coverage and a
future hot-tile-splitting fallback.

The frontend fixed-tile standard-MFS clean path now uses a replayable bounded
row-block runner. Natural weighting streams directly; Uniform, Briggs, and
BriggsBwTaper do a density pass first and then replay weighted row blocks for
the initial dirty/PSF pass and exact residual refreshes. This removes the
default full-MS `VisibilityBatch` retention shape for fixed-tile clean. The
retained non-streaming prepare shape is guarded so an over-target fixed-tile run
fails with an explicit memory-plan error rather than reproducing the rejected
63 GiB footprint.

Additional validation recorded for this repair:

```text
cargo check -p casa-imaging -p casars-imager
cargo test -p casa-imaging direct_resident_tiles_match_evicted_tile_dirty_and_residual_paths --lib
cargo test -p casa-imaging streaming_weighted_standard_mfs_clean_matches_retained_batches --lib
cargo test -p casars-imager standard_mfs_memory --lib
cargo test -p casars-imager standard_mfs_retained_prepare_guard --lib
```

The full-shape memory-control gate completed on the 512-channel, imsize 2048,
Briggs, multiscale, `niter=2`, 10-worker workload:

```text
Artifact: target/imperformance-wave2/memory-control-repair-20260521/full-shape-10w-memory-target-16g.log
Workload: 512 channels, imsize 2048, niter=2, minor-cycle-length=2, 10 workers
Memory target: 16384 MiB explicit override for this sandboxed gate
Peak RSS: 10955767808 bytes (10.20 GiB), below the 16 GiB target
Tile geometry: edge=32, anchor=center_boundary, origin=2x2, tiles=6400
Resident tiles: 6400, max_live_row_blocks=1, tile evictions=0
Frontend total: 315.290s
Core total: 233.049s
Prepare plane input: 207.409s
PSF grid: 58.059s
Residual degrid/grid: 167.395s
Major-cycle refresh: 109.507s
Decision: retained; memory footprint is under control and the rejected transient tile-buffer shape is removed
```

The outer `/usr/bin/time -l` wrapper exited nonzero after the imaging run
because the sandbox denied its `sysctl kern.clockrate` query. The
`profile_imager` process itself completed and wrote products, and the in-process
profile line reported the peak RSS above. A follow-up smoke run after the
streaming path's plan-log patch verified that fixed-tile streaming emits
`standard_mfs_memory_plan_actual` with `system_memory_bytes=34359738368`,
`memory_target_bytes=17179869184`, and `memory_target_source=env`.

Full-shape fixed-tile timing was not accepted as a candidate result. The first
low-overhead edge-64 center-boundary run completed but regressed enough to make
the next action instrumentation rather than tile-size experimentation:

```text
Artifact: target/imperformance-wave2/fine-tile-full-shape-20260521/full-shape-10w-edge64-center-timing.log
Workload: 512 channels, imsize 2048, niter=2, minor-cycle-length=2, 10 workers
Frontend total: 1560.380s
Core total: 1465.817s
Weighting: 111.100s
PSF grid: 418.877s
Residual degrid/grid: 819.805s
Major-cycle refresh: 401.599s
Decision: rejected as a performance candidate; use only as evidence that tiled scheduler attribution was missing
```

Two profiling attempts before this were also rejected as evidence: the
full-stage tile-bucket probe is too large for the full-shape run, and
`CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1` emitted one tiled scheduler line per row
block, producing more than 16,000 block lines for the full-shape workload. The
profile contract is now changed so `CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1`
emits aggregate `standard_mfs_tile_scheduler_summary` lines, while per-block
lines require `CASA_RS_STANDARD_MFS_PROFILE_BLOCK_DETAIL=1`. The aggregate
summary records task count, per-block task/sample/tap quantiles, bucket build
time, tile-buffer zero time, worker replan/grid time, merge time, flush and
eviction counts, resident tile limit, and stage wall time.

The first full-shape aggregate-instrumented run was intentionally stopped after
it produced enough attribution to reject the current fixed-tile scheduler shape:

```text
Artifact: target/imperformance-wave2/fine-tile-full-shape-20260521/full-shape-10w-edge64-center-instrumented.log
Sample: target/imperformance-wave2/fine-tile-full-shape-20260521/profile_imager-edge64-center-sample.txt
Workload: 512 channels, imsize 2048, niter=2, minor-cycle-length=2, 10 workers
Tile geometry: edge=64, anchor=center_boundary, origin=34x34, tiles=1600
Memory plan: planned_reserved_bytes=804604928, planned_active_bytes=1945455616, reserve_over_budget_bytes=267734016
Weighting density: 54.030s
Weighting reweight: 79.793s
Weighting total: 133.837s
Dirty tiled stage total before interruption: 948.798s
Dirty blocks: 24112
Dirty tasks: 6630802
Dirty samples: 1548245185
Dirty tap visits: 75864014065
Dirty bucket build total: 715.662s
Dirty local tile alloc/zero total: 148.109s
Dirty worker replan/grid total: 145.588s
Dirty merge total: 82.001s
Dirty flush total: 0.302s
Dirty cumulative transient bucket bytes: 43542833500
Live sample physical footprint: 62.4 GiB, peak 63.1 GiB
Decision: rejected as a performance candidate and stopped before final wall time
```

This result changes the next action. The current fixed-tile scheduler is not
limited by tile load balance in this run; it is dominated by materialization and
memory traffic. The dirty stage spends far more time building transient tile
buckets and allocating/zeroing/merging per-task tile buffers than applying grid
taps. The live residual-refresh sample was in
`StandardMfsTiledCpuExecutor::accumulate_residual_grid`, with the main thread
mostly merging residual tile buffers through `add_same_shape_grid` and allocator
reallocation/free paths visible in the stack. The memory planner also reports an
over-budget plan before execution, and the prepared batch shape does not match
the planned row-block shape: the scheduler saw 24,112 batches of up to 65,536
samples rather than large 2,048-row work units.

Retained next steps are therefore instrumentation and shape repair, not another
tile-edge sweep:

- split bucket construction timing into count/plan, allocation, and fill phases;
- report actual prepared batch count and sample quantiles in
  `standard_mfs_memory_plan_actual`;
- add live progress summaries for long tiled stages so residual-refresh stalls
  are visible before stage end;
- remove per-task tile-buffer materialization as the default path by updating
  resident tile buffers directly under the single-live-block scheduler, or by
  using bounded scratch only when hot-tile splitting is explicitly enabled;
- align prepared row-block/batch sizing with the centralized memory planner.

Follow-up streaming attribution and resident-stage repair checkpoint:

```text
Artifact: target/imperformance-wave2/streaming-pass-attribution-20260521/fixed-tile-stage-resident-smoke.log
Workload: one-channel imsize 64 Briggs smoke on the medium MS, fixed-tile backend, 2 workers
Correctness: targeted core/frontend tests below
Decision: retained; this is instrumentation and a scheduler data-flow repair, not a timing claim for the full-shape benchmark
```

The fixed-tile frontend now emits `standard_mfs_streaming_pass` records when
`CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1` is set. The records split each streaming
pass into row blocks, cached first blocks, batches, samples, MS-value loading,
buffer preparation, weighting, consumer time, and pass wall time. The first
prepared row block is no longer discarded after metadata discovery: Briggs,
Uniform, and BriggsBwTaper reuse it for the density pass and the initial
weighted replay, then stream only the remaining row blocks for those passes.
Residual refreshes still replay the full row range because the first block has
been consumed and the design still avoids full-MS prepared-batch retention.

The all-resident fixed-tile core path now keeps the direct tile store alive
across the whole replayed dirty/PSF/residual stage. Before this repair, the
streaming callback shape caused one direct store and one flush per row block
even when the planner had all tiles resident. The smoke artifact now has one
aggregate dirty scheduler summary with `block_count=95`, `tile_flush_count=4`,
and `tile_eviction_count=0`; the per-row-block tasks are still visible through
the aggregate quantiles rather than as separate stage summaries.

The smoke also demonstrates why the next full-shape run must use the new pass
records rather than only the old `core_total`: in the streaming path the core
replay callback necessarily includes frontend row-block loading/preparation
time. For this smoke, `initial_replay` reported `get_ms_values_ms=2264.338`,
`prepare_processing_ms=1890.816`, `weighting_ms=13.294`, and
`consumer_ms=111.056`, while the aggregate dirty scheduler line captured the
tile-task work and final stage flush.

Full-shape follow-up with the same instrumentation:

```text
Artifact: target/imperformance-wave2/streaming-pass-attribution-20260521/full-shape-10w-pass-attribution.log
Workload: 512 channels, imsize 2048, niter=2, minor-cycle-length=2, 10 workers
Peak RSS: 10947215360 bytes (10.20 GiB), below the 16 GiB target
Frontend total: 313.099s
Core total: 231.507s
Prepare plane input: 205.674s
PSF grid: 57.279s
Residual degrid/grid: 166.655s
Major-cycle refresh: 109.544s
Decision: retained as attribution evidence; performance is effectively flat versus the prior 315.290s memory-control run
```

The full-shape timing is not a meaningful speedup claim. It is a small
`2.191s` frontend improvement over the previous memory-control checkpoint, well
inside the range where one run is not enough to claim an optimization win. The
result does confirm the memory target remains controlled and the resident-stage
shape is correct: the dirty and residual scheduler summaries each have one
aggregate stage record, `tile_eviction_count=0`, and `tile_flush_count=5105`.

The new attribution shows where the fixed-tile streaming path is actually
spending time:

```text
metadata_probe: 0.934s total, 16.8M samples
density: 72.742s total, get_ms_values=39.644s, prepare_processing=27.462s, weighting=5.622s
initial_replay: 114.541s total, get_ms_values=39.068s, prepare_processing=27.512s, weighting=1.346s, consumer=46.598s
residual_replay: 109.366s total, get_ms_values=39.498s, prepare_processing=28.209s, weighting=1.354s, consumer=40.290s
```

The next high-leverage work is therefore not another tile-edge experiment. The
frontend still rereads and re-prepares the full selected data for each logical
pass, and the core sees `24,112` prepared batches per replay instead of the
planner's intended larger row-block work units. The scheduler repair removed
default merge and allocation costs for all-resident tiles, but bucket
construction remains large: `21.166s` dirty and `15.004s` residual, with
cumulative transient bucket bytes of `44.079 GiB` and `62.073 GiB`
respectively.

Bounded single-worker serial attribution and the first retained density-pass
repair:

```text
Pre-change artifact: target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-serial-attribution.log
Post-change artifact: target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-density-direct.log
Workload: 64 channels, imsize 1024, Briggs robust 0.5, multiscale, niter=50, one worker
Change: density pass accumulates directly into the streaming density grid instead of materializing full row-block VisibilityBatch values
Correctness: targeted tests below
Decision: retained as a modest serial frontend improvement; continue serial work on repeated replay preparation and grid/degrid consumers
```

The profile now splits `standard_mfs_streaming_pass` records into MS column
loads, geometry loading, row adapter access, and sample adaptation. That
attribution confirmed the density pass was doing full visibility preparation
even though density only needs the accepted sample's UV coordinate and weight.
The direct density path still reads the visibility row so the existing
nonfinite visibility behavior is preserved, but it avoids building full
`PlaneInput` batches and avoids phase-rotated model-ready sample storage for
density rows.

```text
Pre-change density pass: 12.543s total, get_ms_values=6.747s, prepare_processing=5.133s, weighting=0.662s
Post-change density pass: 11.377s total, get_ms_values=6.841s, prepare_processing=4.382s, weighting=0.007s
Frontend total: 61.440s -> 60.220s
Prepare plane input: 38.262s -> 37.491s
Core total: 41.412s -> 41.189s
Peak RSS: 9.55 GiB -> 9.55 GiB
```

Follow-up single-worker micro-cleanups:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-slice-iteration.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-slice-iteration-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-slice-lambda.log
Change:
  fixed-tile one-worker global dirty/residual loops iterate over validated batch slices directly;
  standard-MFS row preparation hoists per-row wavelength scaling so per-channel samples multiply by a row-local scale instead of dividing by c.
Best bounded rerun:
  Frontend total: 60.220s -> 59.714s
  Prepare plane input: 37.491s -> 37.271s
  Core total: 41.189s -> 40.962s
  Initial replay consumer: 7.484s -> 7.301s
  Residual replay consumer: 7.717s -> 7.672s
  Peak RSS: 9.55 GiB -> 9.55 GiB
Decision:
  retained as a small, low-risk serial cleanup but not counted as a Wave 2 serial gate;
  the movement is under 1%, so the next real serial lever remains eliminating
  per-block VisibilityBatch materialization and repeated row-block adaptation.
```

Single-worker density pass DATA removal:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-fixed-tile-no-batch-split-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-density-skip-data.log
CASA source check:
  /Users/brianglendenning/SoftwareProjects/casa/casatools/src/code/msvis/MSVis/VisImagingWeight.cc
  VisImagingWeight density construction uses flagCube, uvw, frequency, and
  unpolarized weights; it does not inspect DATA values.
Change:
  fixed-tile one-worker streaming keeps each row block as one VisibilityBatch
  when CASA_RS_STANDARD_MFS_GRID_THREADS=1, reducing replay batches from 3014
  to 95 per pass;
  Briggs/Uniform standard-MFS density streaming now uses a DATA-free row buffer
  for explicit/collapsed/paired MFS, matching CASA's weighting-density inputs
  and avoiding the density pass visibility column read.
Best bounded rerun:
  Frontend total: 59.620s -> 55.748s
  Prepare plane input: 37.310s -> 33.410s
  get_ms_values: 22.771s -> 19.537s
  prepare_processing_buffer: 14.539s -> 13.874s
  Density pass: 11.368s -> 7.399s
  Density get_data_ms: 3.215s -> 0.000s
  Core total: 40.760s -> 40.801s
  Peak RSS: 9.43 GiB -> 9.47 GiB
Decision:
  retained as a real serial frontend win; the core grid/degrid high nails remain.
  A scalar degrid inner-loop cleanup was also tested in
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-scalar-degrid.log
  but moved only noise-level timing, so it is retained only as a low-risk exact
  arithmetic cleanup, not as a claimed performance step.
```

Validation recorded for this retained serial step:

```text
cargo check -p casa-imaging -p casars-imager
cargo test -p casa-imaging streaming_density_samples_match_batch_density_weighting --lib
cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib
cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib
```

Single-worker paired sample preparation cleanup:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-density-skip-data.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-collapse-before-phase-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-weight-hoist-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-weight-hoist-split.log
Change:
  collapsed paired standard-MFS samples now collapse the two hands first and
  apply the common row phase rotation once to the collapsed visibility;
  standard-MFS density and collapsed paired preparation also reuse row-invariant
  WEIGHT values when WEIGHT_SPECTRUM is absent, split the invariant path outside
  the channel loop, and keep the existing per-channel path for spectral weights.
Best bounded rerun:
  Frontend total: 55.748s -> 52.604s
  Prepare plane input: 33.410s -> 30.329s
  get_ms_values: 19.537s -> 19.535s
  prepare_processing_buffer: 13.874s -> 10.794s
  Core total: 40.801s -> 38.643s
  PSF grid: 9.648s -> 9.136s
  residual_degrid_grid: 29.457s -> 27.808s
  major_cycle_refresh: 19.860s -> 18.725s
  Peak RSS: 9.47 GiB -> 9.47 GiB
Decision:
  retained. The phase-collapse-only rerun moved under 1%, but the row-invariant
  weight hoist repeated as a roughly 5.6% bounded frontend improvement, with the
  gain concentrated in preparation/adaptation. This is still a serial cleanup,
  not a solution for the remaining grid/degrid high nails.
```

Single-worker call-tree checkpoint after paired preparation cleanup:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-samply.json.gz
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-apple-sample.txt
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-apple-sample-run.log
Summary:
  Apple sample captured the density/initial replay window with 1668 samples.
  The largest symbolicated buckets were:
    525 samples in StandardGridder::grid_sample_taps_real_complex_pair_planned_f64
    273 + 180 samples in get_ms_values_into_processing_buffer through tiled
      column row-block loading, decode, memcpy, and shared-tile cache lookups
     64 samples in StandardMfsStreamingWeightingPlan::weight_owned_batches
     27 samples in StandardGridder::plan_positive_taps
Decision:
  next single-worker work should target the tap-application kernel and then the
  tiled table row-block loader/copy/decode path. The sample argues against
  spending more Wave 2 time on broad preparation rewrites before those two
  call-tree hotspots are addressed.
```

Single-worker tap-application storage-slice cleanup:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-weight-hoist-split.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-grid-storage.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-grid-storage-rerun.log
Change:
  standard-MFS serial fixed-tile dirty/PSF and residual refresh now select
  contiguous grid storage once per stage replay and use storage-slice tap
  application kernels. The array-indexed kernels remain as fallbacks for
  non-contiguous grids and other callers.
Best bounded rerun:
  Frontend total: 52.604s -> 51.900s
  Prepare plane input: 30.329s -> 30.336s
  get_ms_values: 19.535s -> 19.553s
  prepare_processing_buffer: 10.794s -> 10.783s
  Core total: 38.643s -> 37.910s
  PSF grid: 9.136s -> 8.783s
  residual_degrid_grid: 27.808s -> 27.416s
  major_cycle_refresh: 18.725s -> 18.688s
  Peak RSS: 9.47 GiB -> 9.43 GiB
Noisy first run:
  Frontend total: 52.604s -> 53.906s
  Core total: 38.643s -> 39.377s
Decision:
  retained as a small exact kernel cleanup because the rerun improves the
  sampled tap-application hotspots directly, but not counted as a major Wave 2
  breakthrough. The next serial high nail remains the row-block loader/copy/
  decode path and residual refresh tap planning/application attribution.
```

Single-worker tiled complex decode cleanup:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-grid-storage-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-complex-decode.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-complex-decode-rerun.log
Change:
  tiled storage complex array decoding now builds Complex32/Complex64 values
  directly from component byte pairs instead of first allocating a temporary
  scalar component vector and then mapping pairs into complex values. This
  targets the sampled tiled 2-D channel-range DATA decode path used by bounded
  standard-MFS streaming.
Best bounded rerun:
  Frontend total: 51.900s -> 50.254s
  Prepare plane input: 30.336s -> 28.664s
  get_ms_values: 19.553s -> 17.938s
  prepare_processing_buffer: 10.783s -> 10.725s
  Core total: 37.910s -> 36.280s
  PSF grid: 8.783s -> 8.393s
  residual_degrid_grid: 27.416s -> 26.139s
  major_cycle_refresh: 18.688s -> 17.801s
  Peak RSS: 9.43 GiB -> 9.47 GiB
Noisy first run:
  Frontend total: 51.900s -> 52.572s
  Core total: 37.910s -> 38.098s
Decision:
  retained. The first run was noise-level mixed, but the rerun shows a
  meaningful get-MS/decode improvement and a 4-6% bounded single-worker
  improvement against the previous retained state. The remaining serial high
  nails are still tap planning/application and tiled row-block memcpy/cache
  overhead.
```

Single-worker typed tile decode plus sample-stream replay:

```text
Artifacts:
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-complex-decode-rerun.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-sample-stream-typed-decode.log
  target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-sample-stream-typed-decode-detail.log
Change:
  tiled selected-channel DATA and FLAG reads now use typed shared-tile decoders
  for Complex32 and Bool instead of staging raw per-row bytes through generic
  ArrayValue decode. The one-worker standard-MFS fixed-tile streaming path now
  replays weighted samples directly after row-block adaptation instead of
  rebuilding full PlaneInput/VisibilityBatch objects for every replay block.
  The sample-stream replay is gated to
  CASA_RS_STANDARD_MFS_GRID_THREADS=1 and can be disabled with
  CASA_RS_STANDARD_MFS_DISABLE_SAMPLE_STREAM=1.
Best bounded detail run:
  Frontend total: 50.254s -> 35.376s
  Prepare plane input: 28.664s -> 16.561s
  get_ms_values: 17.938s -> 8.284s
  prepare_processing_buffer: 10.725s -> 4.382s
  Core total: 36.280s -> 18.740s
  Weighting: 0.000s -> 1.469s
  PSF grid: 8.393s -> 3.481s
  residual_degrid_grid: 26.139s -> 12.059s
  major_cycle_refresh: 17.801s -> 8.629s
  Peak RSS: 9.47 GiB -> 10.32 GiB
Second bounded run without detail:
  Frontend total: 37.091s
  Core total: 18.651s
Decision:
  retained. This is the first recent single-worker structural win rather than
  a few-percent kernel cleanup. The core weighting timer is no longer zero
  because the direct sample path keeps the Briggs weighting stage visible in
  core attribution; frontend wall time still improves by 29.6% against the
  prior retained complex-decode run. Peak RSS increased by about 0.85 GiB and
  remains below the 16 GiB dynamic target.
```

Ten-worker tile-inbox scalar/run hot-path pass:

```text
Artifacts:
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-no-concat-unsampled-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-aos-run-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-aos-batched-publish-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-reused-plan-block-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-shared-eop-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-tile-session-mru-10w.log
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-final-retained-10w.log
Workload:
  64 channels, imsize 1024, Briggs robust 0.5, Hogbom dirty-only,
  CASA_RS_STANDARD_MFS_GRID_THREADS=10, fixed-tile backend,
  tile_edge=32, center-boundary anchor
Retained changes:
  tile visibility runs now store queue samples in one AoS vector instead of
  eight parallel scalar vectors;
  tile-inbox publish statistics are batched on the producer fast path when no
  ready-head notification is needed;
  the frontend reuses the planned sample run-block allocation across row
  chunks;
  MeasFrame::with_standard_eop shares one process-wide EOP table allocation;
  tiled storage read sessions keep a tiny MRU of shared tile Arc handles before
  consulting the global shared-tile cache.
Best retained timing:
  Frontend total: 39.231s -> 29.770s
  Prepare plane input: 34.834s -> 25.200s
  get_ms_values: 12.862s -> 11.546s
  prepare_processing_buffer: 21.972s -> 13.654s
  Core total: 26.104s -> 17.005s
  PSF grid: 12.998s -> 8.445s
  residual_degrid_grid: 12.998s -> 8.445s
  Peak RSS: 9.61 GiB
Final retained verification run:
  Frontend total: 31.326s
  Core total: 17.816s
  Prepare plane input: 26.672s
  PSF grid: 8.850s
  residual_degrid_grid: 8.850s
  Peak RSS: 9.55 GiB
Decision:
  retained. The net bounded dirty-only improvement is about 24% frontend and
  35% core versus the pre-pass scalar-run baseline, with the largest retained
  movement coming from the storage-session MRU and producer/run allocation
  shape. The scheduler remains producer-bound: the retained summary still has
  producer_active=16.838s, worker_active_union=5.556s, and worker utilization
  about 8.5%, so the next architectural win is moving the remaining
  per-lane weighting/planning/routing work out of the producer and into
  tile workers over row-shaped run payloads.
```

Rejected candidates in the same pass:

```text
Artifacts and decisions:
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-batched-publish-10w.log
    rejected: publish batching before AoS regressed to 40.112s frontend.
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-inline-boundary-10w.log
    rejected: inline-boundary hints were neutral/noisy versus the retained state.
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-single-pass-router-10w.log
    rejected: single-pass tile router was slower than the retained two-step
    row-run route.
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-worker-center-taps-10w.log
    rejected: center-based worker tap planning did not beat the existing
    exact positive-tap plan path.
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-product-weights-10w.log
    rejected: compact product-weight lookup worsened the retained MRU timing.
  target/imperformance-wave2/scalar-run-slowdown-20260523/medium-briggs-runblock-tile-session-mru32-cell1-10w.log
    rejected: increasing the per-read-session MRU from 8 to 32 entries regressed
    the comparable dirty-only timing to 33.802s frontend.
```

## Metal Preview Backend Track

The Metal gridding experiment from `codex/metal-experiments` is now part of the
Wave 2 branch under `tools/experiments/metal/`. It is retained as feasibility
evidence and as the runnable shader/work-unit harness for a future optional
macOS backend, not as the current correctness/default path.

Current selector contract:

```text
CPU default: unset CASA_RS_STANDARD_MFS_BACKEND
CPU fixed tile: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
Metal preview: CASA_RS_STANDARD_MFS_BACKEND=metal
Production status: macOS-only preview; dirty/PSF tile gridding runs on Metal,
                   residual refresh falls back to CPU fixed-tile
Rust integration direction: objc2-metal
```

The current Rust backend uses the bounded fixed-tile bucket contract and a
cell-owner tile shader for dirty/PSF gridding. The prototype evidence still
rules out global atomics as the production shape: central UV clustering
increases contention and numerical drift. CPU-expanded sorted tap-contribution
lists are also rejected as a production contract because they move too much
memory and rebuild too much per-sample state before the GPU can run.

Validation on 2026-05-21:

- `cargo test -p casa-imaging standard_mfs_metal_backend_selection_is_explicit_and_gated --lib`
  passes and compares a small Metal dirty/PSF run against the CPU reference
  when the process can open a default Metal device.
- `CASA_RS_STANDARD_MFS_BACKEND=metal cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`
  passes when run outside the shell sandbox so macOS exposes the default Metal
  device; the sandboxed shell reports no default Metal device.
- `swift run -c release MetalGridExperiment --samples 2000 --imsize 128 --distribution uniform --tile-edge 32 --skip-slow-baselines`
  still passes in `tools/experiments/metal`.

The promising direction is bounded tile-cell grouping and reduction over compact
row-block/tile work units. Residual refresh can be fused on device, but it
should use the same grouped tile path and the same bounded resident tile/slab
ownership as CPU fixed-tile. Future device staging, resident device grids,
tile-cell reference buffers, and host readback buffers must be budgeted through
the existing standard-MFS memory planner before execution.

The measured dataset is:

```text
/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms
```

Workloads were regenerated under:

```text
target/imperformance-wave2/medium-plan-current/workloads/
```

## Completed Dirty Target

The dirty target completed after two internal changes:

- the MeasurementSet frontend loads stable geometry columns once per standard
  MFS dirty run instead of reopening `ANTENNA1`, `ANTENNA2`, and optional
  pointing metadata for every row block;
- the streaming dirty standard-MFS accumulator now plans and grids samples
  directly into the reusable PSF and residual grids, avoiding the large
  per-block `StandardMfsPlannedSample` vector that dominated sampled CPU and
  memory traffic.

Result:

| Workload | Result JSON | Rust median | CASA median | Ratio | Correctness |
|---|---|---:|---:|---:|---|
| `wave1-vla-single-medium-standard-mfs-dirty-control` | `target/imperformance-wave2/after-direct-plan-dirty/20260519T052204Z-wave1-vla-single-medium-standard-mfs-dirty-control-bb5e3ae8.json` | `506.759 s` | `528.464 s` | `0.96x` | GREEN: `.image`, `.residual`, and `.psf` compared |

The sampled product deltas remained consistent with the Wave 1 full-medium
baseline:

| Product | `diff_abs_max` | `diff_rms` | `diff_abs_max_over_casa_peak` | `diff_rms_over_casa_rms` |
|---|---:|---:|---:|---:|
| `.image` | `1.960754e-3` | `2.062222e-5` | `2.857953e-5` | `2.847730e-5` |
| `.residual` | `1.960754e-3` | `2.062222e-5` | `2.857953e-5` | `2.847730e-5` |
| `.psf` | `4.678965e-5` | `6.608373e-7` | `7.825216e-5` | `5.617084e-4` |

## Stage Ledger

The completed direct-plan dirty run reported these Rust stage medians:

| Stage | Median |
|---|---:|
| `get_ms_values_into_processing_buffer` | `152845.705 ms` |
| `prepare_processing_buffer` | `62147.769 ms` |
| `prepare_plane_input` | `220444.918 ms` |
| `psf_grid` | `148295.170 ms` |
| `residual_degrid_grid` | `148295.170 ms` |
| `run_imaging` | `296849.504 ms` |
| `total` | `508066.368 ms` |

The direct-plan change was driven by a sample of the superseded paired-grid run:

```text
target/imperformance-wave2/after-paired-grid-dirty/casars-imager-77750.sample.txt
```

That sample showed the remaining Rust time in
`StandardMfsVisibilityPlan::new`, especially memmoves of planned sample
records. Direct planning removed that allocation path for the streaming dirty
accumulator.

## Clean Benchmark Status

The full-medium clean workload is now generated with `.model` in its comparison
set:

```text
target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-clean-current.json
```

The first clean run was intentionally stopped before accepting timing claims
because the sampled Rust process exposed a separate ownership bottleneck in the
non-streaming clean path:

```text
target/imperformance-wave2/full-medium-clean-tracefix/casars-imager-clean-tracefix-99251.sample.txt
```

The sample showed `apply_weighting_with_density_source` cloning full
`VisibilityBatch` values for Briggs weighting, with the process footprint at
about `81 GiB`.

The ownership fix now gives the frontend an owned standard-MFS entrypoint:
`run_imaging_owned()` moves prepared `VisibilityBatch` values into the imaging
core and applies Briggs/Uniform weighting by mutating only the weight vectors in
place after the shared density grid has been built. This preserves the borrowed
`run_imaging()` API for general callers while avoiding a full visibility-payload
clone in the high-volume frontend path. Standard MFS clean runs with no initial
model also start from the existing combined PSF+dirty-residual accumulator,
avoiding a separate full PSF gridding pass before the first minor cycle.

The frontend row-block sizing heuristic is now represented as a standard-MFS
memory plan. The same plan that sizes multi-row prepare buffers now records the
total budget, all named reserves, and the row-buffer budget before assigning
rows to prepare blocks. Known reserves include image working set and
Briggs/Uniform density grid. The planner also names future reserve classes for
gridded visibilities, output images, worker staging, and GPU staging so those
buffers have a central accounting point when later Wave 2 stages introduce
them. `CASA_RS_IMAGING_GPU_STAGING_MB` can reserve GPU staging memory without
changing row-buffer code. Because the standard-MFS prepare path is still
sequential, the planner now defaults to one prepare buffer instead of splitting
the budget across not-yet-scheduled worker buffers; `CASA_RS_IMAGING_PREPARE_WORKERS`
remains an override for explicit experiments. The experimental streaming
standard-MFS residual-grid workers are also represented in the same planner:
when `CASA_RS_STANDARD_MFS_GRID_THREADS` is greater than one, the worker staging
reserve accounts for thread-local density grids and the two local complex grids
needed by the combined PSF/dirty workers before assigning prepare-row buffers.
The core and planner now also accept `CASA_RS_STANDARD_MFS_GRID_THREADS=auto`,
which expands to the process available-parallelism count in both places so
worker staging and actual worker allocation remain aligned.

The Rust stage profile now separates clean-loop work beyond the previous
aggregate `major_cycle_refresh` bucket. New stage medians include
`clean_cycle_setup`, `deconvolver_setup`, `residual_refresh_overhead`, and
`multiscale_scale_refresh` alongside the existing `model_fft`,
`residual_degrid_grid`, `residual_fft`, `residual_normalize`, and
`minor_cycle_solve` fields. These fields are intended to identify whether the
next clean bottleneck is pure gridding/degridding, deconvolver setup,
controller overhead, or multiscale state rebuild before choosing CPU,
threading, or GPU work.

A diagnostic run after the owned-Briggs change stayed memory-stable instead of
repeating the `81 GiB` clone spike. It was interrupted before completion because
the remaining clean runtime is still dominated by full standard-MFS gridding
passes:

```text
target/imperformance-wave2/full-medium-clean-owned-briggs-diagnostics/20260519T144727Z-wave1-vla-single-medium-standard-mfs-clean-current-ab62a9d0.json
```

The captured log reached the end of frontend row preparation at
`169.595 s`, with the centralized memory plan reporting a `512.0 MiB` total
budget, `256.0 MiB` image reserve, `16.0 MiB` Briggs density reserve, and
`240.0 MiB` prepare-buffer budget. New runs will also report the named
gridded-visibility, output-image, worker-staging, and GPU-staging reserves.
The interrupted Rust child was around `7.5 GiB` RSS. This confirms the clone
fix but does not make the full clean workload performance-green.

A follow-up full-medium clean diagnostic with the expanded clean-loop stage
fields was interrupted before Rust medians were available:

```text
target/imperformance-wave2/full-medium-clean-stage-diagnostics/20260519T153033Z-wave1-vla-single-medium-standard-mfs-clean-current-15dac89b.json
```

Its log reached frontend row preparation at `176.500 s`; the memory plan
reported a `512.0 MiB` total budget, `272.0 MiB` reserved, and `240.0 MiB`
prepare-buffer budget for the 2048-pixel, 512-channel clean workload.

To make the next optimization decision without waiting for a full clean run,
the same algorithmic path was profiled on a bounded 64-channel, 1024-pixel,
`niter=2` slice. The diagnostic isolates setup and the first major-cycle
refresh while still exercising Briggs weighting, multiscale deconvolution, and
standard-MFS residual refresh.

| Diagnostic | Row block rows | Grid threads | Prepare plane input | Get MS values | Run imaging | Core total |
|---|---:|---:|---:|---:|---:|---:|
| pre-planner CPU fix | `8,192` | `1` | `40.164 s` | `27.394 s` | `69.837 s` | `69.781 s` |
| planner one-buffer default | `32,768` | `1` | `25.808 s` | `12.920 s` | `69.673 s` | `69.629 s` |
| streaming residual-grid worker prototype | `32,768` | `4` | `27.587 s` | `14.369 s` | `49.416 s` | `49.364 s` |
| weighting plus combined dirty-grid workers | `32,768` | `4` | `37.234 s` | `24.275 s` | `22.014 s` | `21.973 s` |
| bounded thread scaling | `32,768` | `8` | `36.280 s` | `21.451 s` | `18.526 s` | `18.477 s` |
| bounded thread scaling | `32,768` | `10` | `28.253 s` | `15.239 s` | `16.426 s` | `16.388 s` |
| bounded thread scaling | `32,768` | `auto` | `28.481 s` | `15.290 s` | `18.959 s` | `18.916 s` |
| positive-only standard gridder plan | `32,768` | `4` | `26.942 s` | `13.713 s` | `15.699 s` | `15.639 s` |
| compact product tap set | `32,768` | `4` | `27.166 s` | `13.674 s` | `19.083 s` | `19.017 s` |
| compact product tap set | planner-sized | `10` | `33.055 s` | `16.969 s` | `18.244 s` | `18.195 s` |

The one-buffer planner default reduced this diagnostic's prepare phase by
`14.356 s` (`35.7%`) and total frontend runtime by `14.616 s` (`13.3%`). The
streaming residual-grid worker prototype then cut the residual refresh from
`29.552 s` to `9.206 s`, reducing total frontend runtime from `95.506 s` to
`77.028 s` on the same bounded workload. It remains env-gated while the larger
full-medium run and deterministic product comparisons are still outstanding.
The latest clean-loop medians show that the remaining grid/degrid work is split
between the initial PSF/dirty pass and the now-threaded residual refresh. After
threading the owned Briggs density/reweighting path and the initial combined
PSF/dirty grid, the same bounded workload moved most remaining time back to
frontend row preparation. Raising the worker count above the first `4`-worker
checkpoint still helps, but not linearly; on the local 10-logical-CPU machine
the bounded `10`-worker run reduced `run_imaging` to `16.426 s`, while
`auto` landed at `18.959 s` in a later one-repeat run. The next local planner
change added a positive-only sample plan for standard-MFS streaming paths that
only ever use the positive UV tap products. That avoids computing and
flattening the conjugate negative-tap products in the initial PSF/dirty grid
and residual refresh loops. On the same bounded `4`-worker profile, it reduced
`run_imaging` from `22.014 s` to `15.699 s`; two broader experiments in the
same area, a per-worker grid merge helper and paired PSF/residual updates, were
discarded after regressing this profile. A standard-MFS row-block prepare
worker prototype using `CASA_RS_IMAGING_PREPARE_WORKERS=4` was also discarded:
it reduced no measured bottleneck on this dataset, moving prepare from about
`27.0 s` to `30.7 s` because smaller row blocks and parallel column reads cost
more than the row-adaptation overlap saved. The retained follow-up shrank
`ProductTapSet` to the flat index and weight arrays used by the hot contiguous
grids; fallback paths now derive row/column coordinates from the flat index
only when needed. Later one-repeat bounded runs were noisier than the first
positive-only profile, but the compact representation still kept the `4`-worker
core below the earlier `22.014 s` checkpoint. Raising the same run to `10`
grid workers improved core imaging to `18.244 s` but forced the centralized
planner to reserve more worker staging, reducing row-buffer size and moving
`prepare_plane_input` to `33.055 s`; for this bounded workload, `4` workers
remained the better end-to-end tradeoff.

| Core stage | Median |
|---|---:|
| `weighting` | `0.765 s` |
| `psf_grid` | `4.104 s` |
| `residual_degrid_grid` | `12.167 s` |
| `major_cycle_refresh` | `8.140 s` |
| `multiscale_scale_refresh` | `0.845 s` |
| `minor_cycle_solve` | `0.011 s` |

The full 2048-pixel, 512-channel, `niter=2` profile shows the same worker
changes carry to the full-medium shape:

| Full-shape diagnostic | Grid threads | Weighting | PSF grid | Residual grid total | Major refresh | Run imaging | Frontend total |
|---|---:|---:|---:|---:|---:|---:|---:|
| residual-grid workers only | `4` | `440.781 s` | `343.492 s` | `634.129 s` | `291.282 s` | `1546.453 s` | `1725.802 s` |
| weighting workers | `4` | `125.454 s` | `387.984 s` | `590.097 s` | `202.655 s` | `1238.520 s` | `1406.923 s` |
| weighting plus combined dirty-grid workers | `4` | `119.135 s` | `195.249 s` | `455.601 s` | `260.904 s` | `917.914 s` | `1088.750 s` |
| auto worker count | `auto` | `160.042 s` | `139.826 s` | `351.524 s` | `211.994 s` | `827.926 s` | `1004.912 s` |
| positive compact tap plan | `auto` | `104.285 s` | `91.790 s` | `250.545 s` | `159.118 s` | `589.248 s` | `758.891 s` |

The full-shape `auto` run was Rust-only, not paired with CASA, but it confirms
that increasing worker count still helps at the real shape. Compared with the
previous `4`-worker standalone profile, `run_imaging` improved by `89.988 s`
and frontend total improved by `83.838 s`. The benefit came from PSF and
residual grid/degrid traversal; weighting moved the other way in this
one-repeat run and still needs repeated measurement before changing the
recommended full-run setting. The later positive compact tap plan keeps the
same `auto` worker setting but removes unnecessary negative-tap planning from
streaming standard-MFS loops and shrinks each flattened product-tap plan to the
flat index and weight arrays used by contiguous grids. On the same full-shape
diagnostic it reduced `run_imaging` by `238.678 s` (`28.8%`) and frontend total
by `246.021 s` (`24.5%`) against the prior standalone `auto` row. The biggest
core movements were `weighting` (`160.042 s` to `104.285 s`), `psf_grid`
(`139.826 s` to `91.790 s`), and `residual_degrid_grid` (`351.524 s` to
`250.545 s`).

A paired Rust-vs-CASA run of the full 2048-pixel, 512-channel, `niter=2`
diagnostic then completed with product comparison:

| Workload | Result JSON | Rust median | CASA median | Ratio | Correctness |
|---|---|---:|---:|---:|---|
| `wave1-vla-single-medium-standard-mfs-clean-niter2` | `target/imperformance-wave2/threaded-clean-niter2-casa/20260519T190828Z-wave1-vla-single-medium-standard-mfs-clean-niter2-58852a06.json` | `1214.646 s` | `2138.483 s` | `0.57x` | GREEN: `.image`, `.residual`, `.psf`, and `.model` compared |
| `wave1-vla-single-medium-standard-mfs-clean-niter2` | `target/imperformance-wave2/positive-compact-clean-niter2-casa/20260520T030729Z-wave1-vla-single-medium-standard-mfs-clean-niter2-b693a634.json` | `719.611 s` | `2017.326 s` | `0.36x` | GREEN: `.image`, `.residual`, `.psf`, and `.model` compared |

The first paired clean comparison made the Wave 2 Rust path materially faster
than CASA on the same diagnostic, about `1.76x` faster by wall time. The later
positive compact tap plan moved the paired CLI timing to `719.611 s`, about
`2.80x` faster than CASA's `2017.326 s` direct `tclean` wall time. It is still a
shallow clean diagnostic: `niter=2` produced two Rust minor iterations and
should not be read as a science-depth deconvolution run.

The latest sampled product deltas were:

| Product | `diff_abs_max` | `diff_rms` | `diff_abs_max_over_casa_peak` | `diff_rms_over_casa_rms` |
|---|---:|---:|---:|---:|
| `.image` | `3.566742e-4` | `1.085070e-5` | `1.854747e-5` | `7.027464e-5` |
| `.residual` | `4.953146e-5` | `1.074190e-5` | `3.262572e-6` | `7.479129e-5` |
| `.psf` | `8.560910e-7` | `5.215558e-8` | `2.095782e-6` | `1.026938e-4` |
| `.model` | `3.576279e-7` | `1.172043e-9` | `2.996346e-7` | `2.321833e-7` |

The paired harness run reported slower Rust stage timings than the immediately
preceding standalone profile, so the conservative evidence to compare against
CASA is the paired wall-clock result above, not standalone frontend profiles.
In the latest paired run the Rust core remained CPU dominated:
`weighting=98.431 s`, `psf_grid=89.829 s`, `residual_degrid_grid=266.098 s`,
`major_cycle_refresh=176.525 s`, and `prepare_plane_input=138.915 s`. The CASA
phase probe, which is diagnostic and not the headline CASA wall-clock
comparison, was dominated by `make_psf=608.218 s`,
`calcres_major_cycle=669.309 s`, and `clean_major_cycle=785.696 s`, with
`set_weighting=60.419 s`. The probe itself took `2129.522 s`, confirming that
CASA phase diagnostics cost roughly another CASA imaging pass on this shape and
must be opt-in rather than part of routine paired timing.

A 2026-05-20 Rust-only profiler check with
`CASA_RS_STANDARD_MFS_GRID_THREADS=1` on the same full-shape `niter=2`
diagnostic reported `frontend_total=1327.510 s`, `prepare_plane_input=126.145 s`,
`run_imaging=1201.220 s`, and `core_total=1198.218 s`. Against the paired CASA
wall-clock baseline of `2017.326 s`, that is still about `1.52x` faster than
CASA, but only because this was a Rust profile compared to the existing paired
CASA wall time rather than a fresh product-writing paired harness run. Compared
with the latest `auto` Rust profiler row from the paired run, the one-worker
profile was `1.81x` slower overall and `2.03x` slower in the imaging core. The
largest one-worker-to-auto regressions were `weighting` (`312.485 s` vs
`98.431 s`), `psf_grid` (`196.063 s` vs `89.829 s`),
`residual_degrid_grid` (`596.189 s` vs `266.098 s`), and
`major_cycle_refresh` (`400.458 s` vs `176.525 s`). This keeps serial
standard-MFS weighting and grid/degrid traversal in scope for Wave 2: worker
scaling wins the headline comparison, but it is masking substantial
single-thread cost.

The first serial-kernel pass precomputes normalized standard-gridder tap weights
per oversampling offset, precomputes density-cell scale constants, removes a
temporary density-position iterator from Briggs density construction, specializes
the owned no-trace Briggs reweight path, and updates complex grid cells by
their real/imaginary fields in the product-tap loop. On the same one-worker
full-shape diagnostic, that moved `frontend_total` to `1286.448 s` and
`core_total` to `1156.535 s`, a `41.063 s` (`3.1%`) frontend reduction and
`41.684 s` (`3.5%`) core reduction from the first one-worker profile. The
clearest movement was residual refresh/grid planning:
`residual_degrid_grid=560.749 s` and `major_cycle_refresh=362.298 s`, down from
`596.189 s` and `400.458 s`. This is useful but not sufficient; it confirms
that the remaining serial high nail is algorithmic residual refresh and
per-sample traversal, not only scalar-loop overhead.

Fresh call-tree samples on the bounded one-worker clean diagnostic changed the
next target. The first 2026-05-20 core sample was dominated by
`accumulate_streaming_standard_mfs_residual_grid_serial`, `flatten_tap_products`,
`sample_taps`, `degrid_sample_product_planned`, and the f64 product-grid update
helpers; FFT and minor-cycle solve were not material. Replacing streaming-path
product-tap flattening with direct positive-axis tap traversal kept the reusable
executor path unchanged, but removed the temporary 49-product tap set from the
large streaming path. On the same bounded 64-channel, 1024-pixel,
`niter=50`, `minor_cycle_length=50`, one-worker run, this moved
`core_total=31.373 s` to `23.094 s`, `psf_grid=6.595 s` to `4.171 s`, and
`residual_degrid_grid=21.526 s` to `15.790 s`. A post-change sample no longer
showed `flatten_tap_products` in the hot stack; the remaining stack was direct
tap traversal plus `sample_taps`.

The next serial pass attacked API overhead rather than adding more workers.
The shared tiled-storage cache now maps table paths to compact table IDs once
per table and uses those IDs in cache keys, so repeated tile lookups no longer
hash long table paths. The standard-MFS frontend row adapter also avoids
dynamic `ndarray` indexing in the hot DATA/FLAG accessors: selected channels
are converted to local channel offsets once, and array-backed row values use
cached shape/stride metadata for direct local indexing. On the same bounded
one-worker run this moved `prepare_processing_buffer` from the
`8.4-8.8 s` range to `5.3-5.5 s`, with the best retained frontend result at
`43.704 s`. The current sample no longer shows the earlier dynamic
`IxDyn`/`memmove` row-access stack as the dominant frontend cost.

The reusable executor path was brought onto the same compact positive-tap
representation as the streaming path so standard-MFS planning uses one exact
tap representation instead of retaining 49-product tap arrays in some paths.
The f64 grid/degrid slice fast paths now use unchecked indexing only after
`sample_taps` has proved the tap coordinates are inside the grid. The initial
dirty PSF/residual pair update also has a real-PSF plus complex-residual helper
so it no longer updates the PSF imaginary lane with zero. The best retained
bounded one-worker probe after those changes was:

```text
target/imperformance-wave2/calltree-probe/bounded-one-worker-real-complex-pair.log
frontend_total=43.704 s
prepare_plane_input=20.147 s
prepare_processing_buffer=5.288 s
core_total=23.476 s
psf_grid=4.122 s
residual_degrid_grid=16.219 s
major_cycle_refresh=12.149 s
major_cycles=2
minor_iterations=50
```

This is a valid serial improvement over the pre-direct-tap bounded baseline
(`frontend_total=57.788 s`, `core_total=31.373 s`) and over the first
post-direct-tap retained run (`frontend_total=49.524 s`,
`core_total=24.136 s`). It is still not the factor-of-two serial break loose:
the remaining high nail is exact visibility-domain residual refresh.

A deliberately tempting image-domain residual-refresh shortcut was tested and
rejected. It reduced the bounded run to about `34.996 s` and made
`major_cycle_refresh` nearly disappear, but the equivalence test
`trace_residual_refresh_matches_fft_residual_and_prediction_order` caught a
structured-model mismatch (`rms=3.6932018e-2`). That path is not part of Wave 2
unless a correct CASA-equivalent derivation is found.

The next Wave 2 optimization target remains the standard-MFS grid/degrid
traversal inside the full-shape clean path. Minor-cycle execution is still not
material at `niter=2`; the large buckets are CPU gridding/degridding, weighting,
and frontend row preparation.

To avoid chasing the wrong high nail, Wave 2 now keeps two clean benchmark
controls explicit. The generated `standard-mfs-clean-niter2` workload remains a
shallow major-cycle diagnostic with `minor_cycle_length=2`. The normal generated
clean workloads now set `minor_cycle_length=niter`, so medium-tier
`standard-mfs-clean-current` runs `niter=100` without forcing a residual refresh
after every two minor iterations.

A bounded 64-channel, 1024-pixel Rust-only check showed why this matters:

| Workload controls | Frontend | Run imaging | Major refresh | Residual grid | Minor solve | Major cycles | Minor iterations |
|---|---:|---:|---:|---:|---:|---:|---:|
| `niter=50`, `minor_cycle_length=2` | `125.223 s` | `96.820 s` | `75.085 s` | `75.336 s` | `0.265 s` | `26` | `50` |
| `niter=50`, `minor_cycle_length=50` | `34.815 s` | `8.063 s` | `2.963 s` | `4.392 s` | `0.263 s` | `2` | `50` |
| `niter=50`, `minor_cycle_length=50`, planner/reserve update | `33.933 s` | `8.123 s` | `3.031 s` | `4.446 s` | `0.261 s` | `2` | `50` |

The deeper-clean conclusion is that minor-cycle solving is still not a dominant
cost for this multiscale slice. With realistic cycle budgeting, bounded
deconvolution depth shifts attention back to frontend row preparation and the
same standard-MFS grid/degrid traversal; with `cycleniter=2`, the benchmark is a
deliberate stress test of repeated major-cycle refresh, not a representative
science-depth clean.

The planner/reserve update keeps imaging worker-local grid staging in the
central plan but no longer subtracts that future imaging-phase reserve from the
transient MS-read row-block buffer. On the bounded realistic profile this moved
the auto row block from `16,384` to the conservative cap of `32,768` rows and
moved `prepare_plane_input` from `27.038 s` in the progress-probed baseline to
`25.784 s` in the no-progress retained timing. The MFS row adapter now reserves
the row-block sample capacity once per block instead of issuing per-row vector
reservations.

## Serial Attribution Pass

The reprogrammed Wave 2 serial pass fixed the bounded workload at:

```text
64 channels, 1024 pixels, Briggs, multiscale, niter=50,
minor_cycle_length=50, CASA_RS_STANDARD_MFS_GRID_THREADS=1
```

Each row below is a one-repeat release-profile run unless marked as a census or
rejected candidate. Deltas are against the previous retained optimization step;
the cumulative column is against the fresh 2026-05-20 baseline.

| Step | Commit or state | Profile artifact | Frontend | Core | Prepare | PSF grid | Residual grid | Major refresh | Delta vs previous retained | Cumulative vs baseline | Correctness |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|---|
| fresh serial baseline | pre-step local state | `target/imperformance-wave2/serial-attribution/baseline-one-worker-20260520/bounded-one-worker-baseline.log` | `45.180 s` | `23.694 s` | `21.391 s` | `4.137 s` | `16.372 s` | `12.288 s` | baseline | baseline | existing green path |
| tap census counters | instrumentation-only local state | `target/imperformance-wave2/serial-attribution/tap-census-20260520/bounded-one-worker-tap-census.log` | timing invalid | timing invalid | timing invalid | timing invalid | timing invalid | timing invalid | not used for timing | not used for timing | counters only |
| compact positive tap spans | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/compact-span-one-worker-20260520/bounded-one-worker-compact-span.log` | `40.788 s` | `19.164 s` | `21.555 s` | `3.632 s` | `12.431 s` | `8.851 s` | frontend `-4.392 s`, core `-4.529 s` | frontend `-4.392 s`, core `-4.529 s` | GREEN: compact span tests plus required residual/Briggs tests |
| compact product table | rejected candidate, removed | `target/imperformance-wave2/serial-attribution/product-table-one-worker-20260520/bounded-one-worker-product-table-rerun.log` | `40.362 s` | `20.758 s` | `19.538 s` | `4.104 s` | `13.546 s` | `9.495 s` | core regressed `+1.594 s`; residual regressed `+1.115 s` | not retained | GREEN but rejected after two core regressions |
| fused exact residual refresh | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/fused-refresh-one-worker-20260520/bounded-one-worker-fused-refresh-rerun.log` | `38.292 s` | `18.662 s` | `19.559 s` | `3.502 s` | `12.028 s` | `8.578 s` | frontend `-2.495 s`, core `-0.502 s` | frontend `-6.888 s`, core `-5.032 s` | GREEN: fused helper test plus required residual/Briggs tests |
| compact prepared executor samples | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/compact-prepared-one-worker-20260520/bounded-one-worker-compact-prepared-rerun.log` | `38.113 s` | `18.496 s` | `19.549 s` | `3.497 s` | `11.861 s` | `8.416 s` | frontend `-0.179 s`, core `-0.166 s` | frontend `-7.067 s`, core `-5.198 s` | GREEN: compact plan test plus required residual/Briggs tests |

The exact tap census found `197,519,040` accepted samples,
`176,471,366` unique tap keys, and `21,047,674` duplicate samples. Adjacent
duplicate runs were not useful: p50, p90, p99, and max adjacent run size were
all `1`. The estimate also showed why compact spans were the right first
memory move: current planned-sample bytes were estimated at `39.504 GiB`,
versus `7.901 GiB` for the compact span representation.

Aggregation is therefore not enabled in this pass. Adjacent-run aggregation is
rejected by the max-run result of `1`; global sort/reduce aggregation is also
rejected for now because the duplicate rate is only about `10.7%` and would
add a large global sort or hash scratch structure to save a minority of tap
visits. If a later workload shows materially higher duplicate density, the
census gate can be rerun with `CASA_RS_STANDARD_MFS_TAP_CENSUS=exact`.

The compact product-weight table was also rejected. It used a named central
planner reserve while under test, but two bounded runs regressed the core path
relative to compact spans (`20.613 s` and `20.758 s` core versus `19.164 s`),
so the reserve and lookup table were removed. The retained centralized planner
change is the worker-staging reserve for already-existing standard-MFS worker
local grids.

The compact prepared-sample step is deliberately scoped to the reusable
standard-MFS executor. Once the executor has planned tap spans, it stores only
the weighted visibility, grid weight, and compact tap span. It no longer keeps
batch/sample indexes just to reach back into `u_lambda` and `v_lambda`, and the
Cotton-Schwab controller reuses that executor for exact residual refreshes
instead of rebuilding the tap plan from full batches on every major cycle.
The full frontend still carries `VisibilityBatch` values for warning metadata,
fallback streaming paths, W-projection, and trace-heavy modes; dropping those
buffers entirely is a later ownership step, not hidden in this serial pass.

The final full-shape one-worker gate used the generated
`standard-mfs-clean-niter2` shape: `512` channels, `2048` pixels, Briggs,
multiscale, `niter=2`, and `minor_cycle_length=2`. The first run was noisy in
unrelated weighting and PSF buckets, so it was repeated before accepting a
timing claim.

| Full-shape one-worker run | Artifact | Frontend | Core | Prepare | Weighting | PSF grid | Residual grid | Major refresh | Interpretation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| previous one-worker reference | earlier profiler note | `1327.510 s` | `1198.218 s` | `126.145 s` | `312.485 s` | `196.063 s` | `596.189 s` | `400.458 s` | pre-attribution reference |
| final run 1 | `target/imperformance-wave2/serial-attribution/final-full-shape-one-worker-20260520/full-shape-one-worker-final.log` | `1471.488 s` | `1378.809 s` | `89.164 s` | `402.099 s` | `250.722 s` | `611.908 s` | `361.518 s` | rejected as noisy/conflicting |
| final rerun | `target/imperformance-wave2/serial-attribution/final-full-shape-one-worker-20260520/full-shape-one-worker-final-rerun.log` | `1167.428 s` | `1075.047 s` | `88.518 s` | `292.866 s` | `177.577 s` | `508.367 s` | `331.164 s` | retained full-shape timing |

Against the previous one-worker full-shape reference, the retained final rerun
improved frontend total by `160.082 s` (`12.1%`), core total by `123.171 s`
(`10.3%`), residual grid/degrid by `87.822 s`, and major-cycle refresh by
`69.294 s`. This full-shape gate was Rust-only timing; product correctness for
the full-shape `niter=2` diagnostic remains covered by the earlier paired
Rust-vs-CASA comparison, while the new serial kernels are covered by the
targeted tests listed in the verification line.

## GPU Feasibility Checkpoint

The local profiler host reports an Apple M4 GPU with 10 cores and Metal
support. The repo now carries an optional macOS `objc2-metal` dependency for
the standard-MFS Metal preview backend. Other GPU runtime families such as wgpu
or CUDA are still absent, and the standard-MFS execution layer still rejects
unimplemented backend names before execution. The central memory planner
already exposes a named GPU staging reserve through
`CASA_RS_IMAGING_GPU_STAGING_MB`.

The Wave 2 conclusion so far is that a GPU implementation should plug in behind
the existing standard-MFS backend/planner boundary, not inside task routing or
frontend row preparation. The first retained Metal path is deliberately limited
to dirty/PSF tile gridding and keeps residual refresh on CPU fixed tiles until
the residual work-unit contract is hardened enough to move without reintroducing
retained full-MS visibility plans.

Dirty-image-only light screening on 2026-05-24 used the retained medium
64-channel, 1024-pixel, Briggs, center-quadrant CPU result as the comparison
point: `188,889,033` samples, `psf_grid=3.724447s`,
`residual_grid=3.724447s`, or about `50.7M` samples/s per grid. The Metal
experiment harness now supports `--strategies global_atomic` plus
`--no-cpu-reference`, so larger dirty-only scatter screens can run without
building CPU-expanded reduce plans.

Artifact:
`target/imperformance-wave2/metal-dirty-light-20260524/README.md`

| Case | Command shape | GPU s | Samples/s | CPU-reference status | Decision |
|---|---|---:|---:|---|---|
| small correctness check | 2k uniform, 128 grid, `--strategies global_atomic` | 0.000230 | 8.69M | max error `3.07e-08`, relative RMS `8.83e-08` | validates strategy-filter path |
| dirty-light padded-grid analog | 20M central cluster, 1250 grid, support 3, global atomic | 0.483820 | 41.3M | skipped for throughput | rejected as production direction |
| dirty-light padded-grid upper bound | 20M uniform, 1250 grid, support 3, global atomic | 0.440035 | 45.5M | skipped for throughput | rejected as production direction |

Decision: do not promote global atomic Metal scatter for the current
dirty-image-only light workload. Even the uniform padded-grid upper bound is
below the retained CPU gridding rate before adding production routing,
weighting, and backend integration costs. If GPU remains in Wave 2, the next
candidate must be a GPU-resident grouping/reduction contract, not direct
atomics into one shared grid.

Heavy-clean screening on 2026-05-24 used the retained medium 64-channel,
1024-pixel, Briggs, multiscale `niter=500`, center-quadrant CPU result as the
comparison point. That run spent `major_refresh=67.488858s` across 10 residual
refreshes, with `197,519,040` samples per refresh. This is about `6.75s` per
refresh or `29.3M` samples/s; the first logged residual-refresh stage was
`6.433793s`, or `30.7M` samples/s. The Metal experiment harness now also
supports a filtered `--strategies residual_refresh_global_atomic` path.

Artifact:
`target/imperformance-wave2/metal-heavy-residual-20260524/README.md`

| Case | Command shape | GPU s | Samples/s | CPU-reference status | Decision |
|---|---|---:|---:|---|---|
| small correctness check | 2k uniform, 128 grid, `--strategies residual_refresh_global_atomic` | 0.000253 | 7.89M | max error `3.18e-08`, relative RMS `8.77e-08` | validates filtered residual path |
| heavy residual central analog | 20M central cluster, 1250 grid, support 3, residual refresh global atomic | 0.460858 | 43.4M | skipped for throughput | promising screen |
| heavy residual uniform analog | 20M uniform, 1250 grid, support 3, residual refresh global atomic | 0.725857 | 27.6M | skipped for throughput | not promising alone |

Decision: residual refresh remains a plausible Metal target for the heavy
workload, unlike dirty-only global scatter. The central-cluster screen
extrapolates to about `4.55s` for the retained 197.5M-sample residual stage
versus `6.75s` average CPU time. The uniform screen extrapolates to about
`7.17s`, so a production decision still needs a real-data distribution screen
or production-shaped row/run bridge, plus explicit device residency for model
and residual grids across major cycles.

Earlier stopped clean attempts wrote failed result records only:

| Attempt | Result JSON | Status | Note |
|---|---|---|---|
| initial clean benchmark | `target/imperformance-wave2/full-medium-clean/20260519T055802Z-wave1-vla-single-medium-standard-mfs-clean-current-e78b0202.json` | failed | interrupted after a sample showed trace/env overhead plus full-batch clone pressure |
| trace-fixed clean benchmark | `target/imperformance-wave2/full-medium-clean-tracefix/20260519T061626Z-wave1-vla-single-medium-standard-mfs-clean-current-dbf36763.json` | failed | interrupted after a sample showed the remaining full-batch clone ownership bottleneck |
| owned-Briggs diagnostic clean benchmark | `target/imperformance-wave2/full-medium-clean-owned-briggs-diagnostics/20260519T144727Z-wave1-vla-single-medium-standard-mfs-clean-current-ab62a9d0.json` | failed | interrupted after memory stayed stable but the clean run remained in expensive full standard-MFS gridding work |
| expanded-stage clean benchmark | `target/imperformance-wave2/full-medium-clean-stage-diagnostics/20260519T153033Z-wave1-vla-single-medium-standard-mfs-clean-current-15dac89b.json` | failed | interrupted after the full-medium frontend completed, before Rust stage medians were available |

The completed `niter=2` clean diagnostic is performance-green against CASA, but
the full Wave 2 acceleration ladder remains open until the remaining CPU
grid/degrid work is reduced further, GPU feasibility is settled, and broader
clean validation is recorded.

## Fine Fixed-Tile Scheduler Checkpoint

The next CPU backend pass replaces the coarse fixed-tile experiment with
planner-owned fine tiles and a bounded one-live-row-block task scheduler. The
implementation now supports `CASA_RS_STANDARD_MFS_TILE_ANCHOR=zero|center_boundary`.
The center-boundary mode uses the standard gridder's actual integer tap center
as the anchor source and special-cases `origin == 0`, so no empty leading tile
is introduced.

The tile bucket probe now reports the fields needed before accepting timing
claims: tile origin, gridder center, per-stage tile distribution, per-row-block
task and hot-tail summaries, near-origin integer tap-center counts, UV-quadrant
owner distribution, finite/nonfinite counts, estimated bucket bytes, and
all-resident tile bytes. `StandardMfsTileBucketSample` remains a current-block
routing record only; it stores sample index, integer center, final grid weight,
flags, and tap-count work estimate, but not `PositiveTapSet` or tap arrays.

The first scheduler uses `max_live_row_blocks=1`: one task per nonempty tile,
descending estimated tap visits, scoped CPU workers, task-local tile buffers,
deterministic tile-id merge into resident halo-padded tile buffers, and stage
or deterministic-eviction flush by default. `CASA_RS_STANDARD_MFS_TILE_FLUSH=per_block`
is debug-only. Dirty, PSF, and residual-refresh gridding now use the same
bounded tile-task path. Queue depth two, active-tile skip/postpone, and hot-tile
splitting remain gated by measured tail-idle evidence.

The memory planner now records tile edge, center-boundary anchor selection,
`max_live_row_blocks`, live row-block bytes, live bucket bytes, queued task
bytes, resident tile-buffer bytes, global grid bytes, and tile-cell/bin
placeholder bytes. Explicit `CASA_RS_STANDARD_MFS_TILE_EDGE` still wins; absent
that, the planner chooses `32` for smaller fixed-tile standard-MFS runs and
`64` for `imsize >= 2048` or tighter large-image residency. Fixed-tile runs no
longer reserve the legacy full-grid-per-worker staging buffers; those are only
charged to the old worker-local full-grid reference path.

Bounded diagnostic workload: 64-channel, 1024-image, Briggs,
multiscale-clean `niter=2`, direct external-disk MS, fixed-tile backend,
`CASA_RS_STANDARD_MFS_GRID_THREADS=10`, one repeat, no warmup.

| Step | Artifact | Threads | Tile edge/anchor | Frontend | Core | PSF grid | Residual grid/degrid | Major refresh | Correctness | Decision |
|---|---|---:|---|---:|---:|---:|---:|---:|---|---|
| coarse baseline diagnostic | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge256-zero.log` | 10 | 256 / zero | 50.897s | 29.209s | 9.531s | 17.858s | 8.380s | GREEN targeted fixed-tile tests | rejected as CPU scheduler shape: hottest tile 53.95%, per-block nonempty p50=8 |
| fine tile candidate | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge64-center.log` | 10 | 64 / center-boundary | 41.477s | 21.073s | 7.498s | 11.800s | 4.355s | GREEN targeted fixed-tile tests | retained as lower-overhead fallback: hottest tile 8.51%, per-block nonempty p50=51 |
| fine tile candidate | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge32-center.log` | 10 | 32 / center-boundary | 39.693s | 19.407s | 6.524s | 11.089s | 4.617s | GREEN targeted fixed-tile tests | retained as current 1024 default: hottest tile 2.96%, per-block nonempty p50=160 |

The stage-level and row-block-level diagnostics agree that the coarse
zero-anchored 256 decomposition is not a viable CPU scheduling shape. The
center-boundary 32-tile run cuts bounded frontend time by 11.203s versus the
coarse diagnostic and exposes enough per-row-block tile tasks to feed a
10-worker pool on this workload. The 64-tile run is kept as the planner's
large-image or tighter-memory fallback because it has less tile metadata and
halo residency, while still removing the single central-tile bottleneck.

## Single-Worker Fixed-Tile Repair

After the fixed-tile memory repair, bounded one-worker profiling showed that
using the tiled scheduler with only one worker still paid bucket/task overhead
and duplicated tap planning. A direct resident-tile serial experiment removed
bucket allocation but was rejected because sample-order tile updates destroyed
locality: frontend regressed to 102.588s and core to 82.123s. The retained
single-worker path instead bypasses tile buffers entirely and streams directly
into the global standard-MFS stage grids. This keeps the bounded streaming
passes and avoids full-MS visibility retention while removing tile scheduler
overhead when no parallelism is available.

Bounded diagnostic workload: 64-channel, 1024-image, Briggs,
multiscale-clean `niter=50`, direct external-disk MS, fixed-tile backend,
`CASA_RS_STANDARD_MFS_GRID_THREADS=1`, `CASA_RS_STANDARD_MFS_TILE_EDGE=32`,
`CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_boundary`, one repeat, no warmup.

| Step | Artifact | Threads | Frontend | Core | Prepare | PSF grid | Residual grid/degrid | Major refresh | Peak RSS | Correctness | Decision |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| fixed-tile one-worker baseline | `target/imperformance-wave2/single-worker-current-20260522/bounded-one-worker-fixed-tile-baseline.log` | 1 | 66.422s | 45.305s | 39.456s | 10.973s | 32.623s | 21.702s | 9.55GB | GREEN targeted tests | baseline for serial high-nail pass |
| serial resident-tile experiment | `target/imperformance-wave2/single-worker-current-20260522/bounded-one-worker-fixed-tile-serial-direct.log` | 1 | 102.588s | 82.123s | 38.613s | 19.810s | 60.605s | 40.847s | 9.55GB | GREEN targeted tests | rejected: removed buckets but lost tile-local processing order |
| one-worker global-grid bypass | `target/imperformance-wave2/single-worker-current-20260522/bounded-one-worker-fixed-tile-global-serial.log` | 1 | 61.552s | 41.284s | 38.293s | 9.808s | 29.751s | 19.994s | 9.55GB | GREEN targeted tests | retained: avoids tile scheduler when one worker cannot exploit it |

The retained one-worker bypass improves the bounded fixed-tile baseline by
4.870s frontend (7.3%) and 4.021s core (8.9%). Stage movement is concentrated
where expected: PSF gridding improves by 1.165s (10.6%) and residual
grid/degrid by 2.872s (8.8%). The result also rejects a tempting but wrong
serial shortcut: avoiding buckets is not enough if the resulting sample order
causes random updates across hundreds of tile buffers.

## Single-Worker Sample-Streaming Gate

The memory-controlled fixed-tile backend now has a successful full-shape
one-worker gate on the 512-channel, 2048-image, Briggs, multiscale `niter=2`
standard-MFS workload. The first attempted full-shape command accidentally
omitted `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile` and immediately exposed the
legacy retained executor plan estimate (`101.130GB`); that artifact is kept only
as an invocation guard. The corrected fixed-tile streaming run stayed below the
16GiB target and matched the saved CASA products at the same tolerance as the
previous paired comparison.

Full-shape artifact:
`target/imperformance-wave2/single-worker-full-shape-20260522/full-shape-one-worker-fixed-tile-sample-stream.log`.
Product comparison artifact:
`target/imperformance-wave2/single-worker-full-shape-20260522/product-check/current-rust-vs-saved-casa-comparison.json`.

| Workload | Threads | Frontend | Core | Prepare | PSF grid | Residual grid/degrid | Major refresh | Peak RSS | Correctness | Decision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| retained old one-worker reference | 1 | 1167.428s | 1075.047s | n/a | 177.577s | 508.367s | n/a | n/a | prior GREEN | replaced by streaming fixed-tile path |
| fixed-tile streaming full-shape gate | 1 | 328.937s | 294.998s | 315.845s | 69.104s | 218.544s | 149.613s | 10.27GB | GREEN vs saved CASA products | retained: memory-controlled full-shape path |

The product check compared `.image`, `.residual`, `.psf`, and `.model` against
the saved CASA full-shape products. Max absolute normalized deltas remained
small: `.image` `1.85e-5` of CASA peak, `.residual` `3.26e-6`, `.psf`
`2.10e-6`, and `.model` `3.00e-7`.

## Single-Worker Serial High-Nail Pass

After the sample-streaming gate, the bounded one-worker profile moved the
remaining high nails into row-block replay and standard grid/degrid work. The
retained change in this pass reuses the standard-MFS `PreparedSelection` across
row blocks inside each streaming pass, preserving any row/frequency conversion
cache and removing repeated per-block setup. A narrower direct row fast path was
rejected because it moved work into the adapter and regressed the current path.
Core helper inlining and a hand-fused residual storage write were also rejected
as noise: they did not improve frontend wall time.

Bounded diagnostic workload: 64-channel, 1024-image, Briggs,
multiscale-clean `niter=50`, direct external-disk MS, fixed-tile backend,
`CASA_RS_STANDARD_MFS_GRID_THREADS=1`, one repeat, no warmup.

| Step | Artifact | Frontend | Core | Prepare | PSF grid | Residual grid/degrid | Major refresh | Peak RSS | Correctness | Decision |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| sample-stream typed decode reference | `target/imperformance-wave2/single-worker-serial-20260522/bounded-one-worker-weight-hoist.log` | 52.951s | 38.883s | 30.645s | 9.192s | 27.984s | 18.845s | 9.43GB | GREEN targeted tests | previous retained serial point |
| rejected direct row fast path | `target/imperformance-wave2/single-worker-fast-row-stream-20260522/bounded-one-worker-fixed-tile-fast-row-stream.log` | 50.656s | 37.172s | 44.550s | 8.239s | 27.260s | 19.073s | 9.47GB | GREEN targeted tests | rejected: slower than prepared reuse and worsened replay attribution |
| prepared-state reuse | `target/imperformance-wave2/single-worker-prepared-reuse-20260522/bounded-one-worker-prepared-reuse.log` | 49.465s | 36.017s | 43.344s | 7.793s | 26.536s | 18.797s | 9.47GB | GREEN targeted tests | retained: small but real setup/cache cleanup |
| inline-only/core helper check | `target/imperformance-wave2/single-worker-prepared-inline-20260522/bounded-one-worker-prepared-inline.log` | 49.469s | 35.919s | 43.330s | 7.530s | 26.699s | 19.221s | 9.42GB | GREEN targeted tests | rejected as net noise |
| weighted sample block boundary | `target/imperformance-wave2/single-worker-block-sample-20260522/bounded-one-worker-block-sample.log`; rerun `target/imperformance-wave2/single-worker-block-sample-20260522/bounded-one-worker-block-sample-rerun.log` | 49.780s / 47.468s | 35.947s / 33.787s | 43.378s / 41.267s | 8.688s / 7.995s | 25.488s / 24.059s | 16.854s / 16.118s | n/a | GREEN targeted tests | screened: average frontend 48.624s is only 1.7% better than prepared-state reuse, so treat as noise-level pending more evidence |
| rejected scalar-callback planned tap handoff | `target/imperformance-wave2/single-worker-planned-sample-20260522/bounded-one-worker-planned-sample-rerun.log`; `target/imperformance-wave2/single-worker-planned-sample-20260522/bounded-one-worker-planned-sample-block-plan.log` | 52.410s / 52.832s | 38.644s / 38.995s | 46.050s / 46.453s | 8.312s / 8.555s | 28.581s / 28.716s | 20.325s / 20.216s | n/a | GREEN targeted tests | rejected: planning inside the row-sample callback moved work into slower frontend replay shape |
| planned weighted tap handoff | `target/imperformance-wave2/single-worker-planned-sample-20260522/bounded-one-worker-planned-sample-fast-taps.log`; rerun `target/imperformance-wave2/single-worker-planned-sample-20260522/bounded-one-worker-planned-sample-fast-taps-rerun.log` | 48.273s / 48.234s | 34.372s / 33.371s | 41.752s / 41.837s | 7.963s / 7.412s | 24.656s / 24.223s | 16.749s / 16.865s | n/a | GREEN targeted tests | retained for shared work-unit structure; not claimed as a standalone speed win because average frontend delta vs block boundary is under 1% |
| direct planned row-blocks + compact storage apply | `target/imperformance-wave2/single-worker-direct-planned-20260522/bounded-one-worker-direct-planned.log`; rerun `target/imperformance-wave2/single-worker-direct-planned-20260522/bounded-one-worker-direct-planned-rerun.log`; detail `target/imperformance-wave2/single-worker-direct-planned-20260522/bounded-one-worker-direct-planned-detail.log` | 48.067s / 47.162s | 34.402s / 33.533s | 41.584s / 40.756s | 7.962s / 7.574s | 24.689s / 24.220s | 16.782s / 16.704s | 9.47GB in detail run | GREEN targeted tests | retained for architecture and attribution only: removes the transient weighted-sample block and records planned candidate/tap counts, but average frontend/core movement remains under the performance gate |

The retained prepared-state reuse improves the previous retained bounded serial
point by 3.486s frontend (6.6%) and 2.866s core (7.4%). The latest profile still
shows the remaining work is structural: replay preparation is about 43.3s
wall-attributed and the gridding/degridding stages are about 34.2s combined.
Further single-worker wins likely require changing how weighted samples are
represented across the frontend/core boundary or reducing tap planning/grid
application cost, not more wrapper-level cleanup.

Follow-up implementation note: the trace-free one-worker frontend/core handoff
now has a direct planned weighted-sample block API. Frontends convert bounded
weighted row blocks into compact tap-span records without first retaining a
second transient `StandardMfsWeightedSample` vector, and contiguous core
gridding/degridding storage paths apply those compact fields directly instead of
reconstructing a `PositiveTapSet`. The detailed artifact records
`planned_candidates=197519040`, `planned_samples=197519040`, and
`planned_tap_visits=9678432960` for each replay pass on the bounded workload.
The scalar-callback form was explicitly rejected after a 52s frontend
regression. The retained direct-planned form averaged 47.614s frontend and
33.968s core, so it remains an architectural cleanup and attribution hook rather
than a standalone performance claim.

## Fixed-Tile Multi-Worker Attribution

2026-05-22 bounded fixed-tile sweep, 64-channel, 1024-pixel, Briggs,
multiscale-clean `niter=50`, direct external-disk MS,
`CASA_RS_STANDARD_MFS_BACKEND=fixed_tile`,
`CASA_RS_STANDARD_MFS_TILE_EDGE=32`,
`CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_boundary`, one repeat, no warmup.
The one-worker row uses `CASA_RS_STANDARD_MFS_FORCE_TILED_ONE_WORKER=1` so it
measures the same tiled scheduler instead of the normal global-grid one-worker
bypass.

| Workers | Artifact | Frontend | Core | PSF grid | Residual grid/degrid | Dirty stage Gtap/s | Residual stage Gtap/s | Dirty full-stage util | Residual full-stage util | Decision |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-1w-fixed-tile-throughput-rerun.log` | 53.894s | 39.630s | 9.129s | 28.777s | 0.530 | 0.493 | 36.0% | 46.0% | diagnostic baseline |
| 2 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-2w-fixed-tile-throughput-rerun.log` | 45.179s | 31.505s | 7.442s | 22.349s | 0.650 | 0.649 | 21.9% | 30.8% | retained as best low-risk worker count |
| 4 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-4w-fixed-tile-throughput-rerun.log` | 42.954s | 29.264s | 7.068s | 20.473s | 0.685 | 0.722 | 12.7% | 18.5% | best bounded result in this sweep |
| 6 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-6w-fixed-tile-throughput-rerun.log` | 43.055s | 29.069s | 7.134s | 20.166s | 0.678 | 0.743 | 10.7% | 15.0% | no material win over 4 workers |
| 8 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-8w-fixed-tile-throughput-rerun.log` | 45.534s | 31.454s | 7.376s | 22.148s | 0.656 | 0.655 | 8.9% | 12.1% | rejected: useful throughput collapses |
| 10 | `target/imperformance-wave2/multi-worker-throughput-20260522/bounded-10w-fixed-tile-throughput-rerun.log` | 50.869s | 35.532s | 8.211s | 25.161s | 0.589 | 0.571 | 7.5% | 9.8% | rejected: overhead dominates |

Interpretation: block-local utilization was misleading. The earlier
`worker_utilization_pct` field measured only the tiny dispatched tile-task
loops, not the whole stage. Full-stage utilization uses stage wall time times
the configured worker count; it shows that the 10-worker path has only about
7.5-9.8% effective worker occupancy. The multi-worker direct tiled path also
fragments the same bounded work into 3014 scheduler blocks, while the forced
one-worker tiled path sees 95 blocks. The next structural target is
row-block-level bucket coalescing across the batch slice, which requires bucket
samples to carry enough batch identity to index more than one `VisibilityBatch`.

Follow-up line-attribution artifact:
`target/imperformance-wave2/multi-worker-throughput-20260522/bounded-10w-fixed-tile-line-attribution.log`.
The 10-worker dirty scheduler stage spent 14.281s total: 9.300s replay/input
gap, 0.035s batch validation/accounting, 3.312s bucket build, 1.571s block
worker wall, 0.002s flush, and 0.062s unaccounted. The 10-worker residual
stage spent 11.827s total: 8.337s replay/input gap, 0.000s batch
validation/accounting, 1.633s bucket build, 1.782s block worker wall, 0.001s
flush, and 0.074s unaccounted. This directly confirms that the worker pool is
mostly starved by replay/input gaps and excessive small-batch scheduling, not
by tile merge or tile flush.

## Producer/Consumer Scheduler Design

The line-attribution result above led to a design review rather than another
tile-edge or worker-count experiment. The current fixed-tile scheduler does not
match the intended producer/consumer shape: it synchronously replays one
`VisibilityBatch` into one scoped worker block at a time, so workers see many
small isolated task lists instead of persistent tile queues fed by bounded
prepared row blocks.

Design artifact:
`docs/tutorial-parity/imperformance-wave-2-producer-consumer-scheduler-design.md`.

Review artifact: Chrome Oracle conversation `Revised design proposal`, including
one follow-up iteration focused on implementation order, memory ownership, and
acceptance gates.

Retained design decisions:

- add Phase 0 data/lifetime scaffolding before concurrency changes:
  `PreparedTileRowBlock`, sample IDs, row-block accessors, aggregate memory
  leases, shared sample classification, and deterministic scalar records;
- do Phase A row-block coalescing before persistent workers, so one frontend
  row block becomes one scheduler block and the sample-indexing change is
  tested without asynchronous lifetime risk;
- implement persistent workers with per-tile FIFO queues plus a global ready
  heap, with one valid heap head per inactive nonempty tile;
- treat oldest-block-first as a priority policy for memory drainage, not a
  global block barrier;
- split memory accounting into stage, build/scratch, and published-row-block
  lease lifetimes;
- require overlap attribution before claiming the multi-worker structure is
  fixed.

Mandatory proof before accepting the new scheduler shape:

```text
worker_waiting_for_work_time / (worker_count * stage_wall_time)
producer_active_time
worker_active_union_time
producer_worker_overlap_time
producer_only_time
worker_only_time
neither_active_time
producer_blocked_on_memory
live row-block bytes and RSS high water
scheduler and tile lock wait
active tile skip/stale heap counts
dirty/PSF/residual correctness across 1, 2, and 10 workers
```

Decision: retained as the next multi-worker implementation plan. Phase A is a
measured compatibility/data-model checkpoint, not a performance claim; Phase C
is the first phase allowed to claim producer/worker overlap.

### Implementation Checkpoint: Row-Block Scheduler Foundation

Date: 2026-05-22.

Retained changes:

- Phase 0 row-block identity/accounting landed in
  `crates/casa-imaging/src/execution.rs`: compact bucket samples now carry a
  row-block-local `sample_id`, multi-batch sample references resolve through a
  shared row-block access trait, owned `PreparedTileRowBlock` publication has a
  batch-backed storage variant, and row-block byte accounting covers sample
  refs, bucket records, task/range metadata, scalar records, and owned batch
  storage capacity.
- The shared standard-MFS classifier now separates density/PSF eligibility from
  dirty/residual finite-visibility eligibility, preserving the existing
  nonfinite rule: PSF can still receive the sample while dirty/residual
  visibility contribution is skipped.
- Phase A coalescing is implemented for fixed-tile dirty/PSF/residual paths:
  the scheduler now builds one tile-bucket block for the full frontend callback
  slice instead of rebuilding per `VisibilityBatch`, including the
  non-all-resident fallback path.
- Phase B/C owned publication is wired into the production all-resident
  fixed-tile replay path: `run_standard_mfs_weighted_streaming_with_execution_config`
  now receives owned weighted row-block batches, `casars-imager` hands off its
  owned weighted block without cloning, and dirty/PSF/residual direct replay use
  persistent workers over `Arc<PreparedTileRowBlock>` tasks. The persistent
  scheduler uses a global ready heap with oldest-block priority, active-tile
  exclusion, a live-row-block cap clamped to one or two, and deterministic
  task-output reduction order.

Validation artifacts:

```text
cargo check -p casa-imaging
cargo test -p casa-imaging block_tile_buckets --lib
cargo test -p casa-imaging prepared_tile_row_block --lib
cargo test -p casa-imaging standard_mfs_sample_classifier --lib
cargo test -p casa-imaging direct_resident_tiles_match_evicted_tile_dirty_and_residual_paths --lib
cargo test -p casa-imaging persistent_tile_scheduler_matches_direct_dirty_and_residual --lib
cargo check -p casa-imaging -p casars-imager
```

Correctness status: targeted core tests pass. No timing claim is made at this
checkpoint; the next run must collect the required worker-wait and
producer-overlap metrics before accepting Phase C performance.

Decision: retain Phase 0 and Phase A immediately. Retain the persistent
scheduler production handoff as architecture, but do not claim Phase B/C
performance until the required worker-wait/producer-overlap metrics are present
and bounded/full-shape timings show a material improvement.

### Producer/Consumer Scheduler Bounded Gate

Date: 2026-05-22.

Artifact:
`target/imperformance-wave2/producer-consumer-20260522/bounded-10w-queue2-persistent-overlap.log`.

Workload: bounded 64-channel, 1024-pixel, Briggs, multiscale `niter=50`,
`CASA_RS_STANDARD_MFS_BACKEND=fixed_tile`,
`CASA_RS_STANDARD_MFS_GRID_THREADS=10`,
`CASA_RS_STANDARD_MFS_QUEUE_BLOCKS=2`,
`CASA_RS_STANDARD_MFS_TILE_EDGE=32`,
`CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_boundary`, one repeat, no warmup.

| Metric | Dirty | Residual |
|---|---:|---:|
| Stage wall | 14.299s | 12.835s |
| Producer active | 14.293s | 12.830s |
| Worker active union | 1.352s | 1.675s |
| Producer/worker overlap | 1.350s | 1.672s |
| Producer only | 12.944s | 11.158s |
| Worker only | 0.002s | 0.003s |
| Max live row blocks observed | 1 | 1 |
| Producer memory blocked | 0.000s | 0.000s |
| Active tile skips / stale heap entries | 0 / 0 | 0 / 0 |
| Worker utilization, full capacity | 8.85% | 12.16% |

Run totals: `frontend_total=42.977s`, `core_total=28.925s`,
`prepare_plane_input=30.028s`, `psf_grid=7.150s`,
`residual_degrid_grid=19.985s`, `major_cycle_refresh=12.892s`,
`peak_rss_bytes=9548103680`.

Decision: retain the row-block work-unit model, row-block coalescing, owned
publication seam, and overlap instrumentation as architectural progress. Do
not accept Phase C as a performance win: queue depth 2 did not become resident,
because producer read/prepare/bucket work remains slower than the tile work it
feeds. The old replay/input gap is now directly attributed as producer-only
time rather than inferred from worker utilization. A full-shape run is deferred
until the bounded gate shows material producer/worker overlap; otherwise it
would only amplify the same producer bottleneck.

### Implementation Checkpoint: Direct Per-Tile Inbox Scheduler

Date: 2026-05-22.

Retained changes:

- The all-resident fixed-tile dirty, PSF, and residual replay paths now use a
  direct per-tile inbox scheduler instead of publishing central
  `PreparedTileRowBlock` tasks. The producer pushes accepted compact samples to
  that tile's inbox, and workers are scheduled by tile id, drain a bounded
  chunk, release the queue mutex, and update only the resident tile buffer for
  that tile.
- The queue publication unit is now a compact columnar tile chunk: center cells,
  compact tap keys, stage flags, raw weights, sumwt factors, `u/v/w`
  wavelengths, and visibilities are arrays inside one chunk. Workers loop by
  index over the chunk, so flags and weights no longer cross the scheduler
  boundary as isolated scalar queue records.
- The producer carries compact tap identity (`center_x/y`, `kernel_u/v`,
  `support_id`) into the tile chunk. Workers reconstruct the positive tap spans
  from that identity and no longer call `plan_positive_taps()` again for queued
  samples. The chunk does not retain a `VisibilityBatch`, `PreparedTileRowBlock`,
  sample id, `PositiveTapSet`, or expanded tap arrays.
- `StandardMfsTileSampleRouter` now gives the fixed-tile producer explicit
  `DensityNoData`, `PsfNoData`, `DirtyWithData`, and `ResidualWithData` modes.
  This is deliberately named as a standard-MFS tile router, not the future
  MeasurementSet reader. The next MS-facing cursor should be mode-agnostic
  enough to serve MFS, cubes, and mosaics before mode-specific routing.
  The PSF replay path uses `PsfNoData`, so the tile inbox no longer carries
  visibility payloads for PSF-only gridding.
- The ready scheduler uses one global heap of ready tile ids, generation checks,
  per-tile `active` and `ready_enqueued` invariants, bounded worker drains, and
  high-water queued-byte accounting. A unit-test deadlock exposed a byte-credit
  publication race; queued bytes are now charged before samples become visible
  in a tile queue.
- New instrumentation emits `standard_mfs_tile_inbox_scheduler_summary` with
  first-order scheduler accounting: enqueued samples/bytes, queued-byte
  high-water, ready-head count, worker drains, worker tap visits, producer
  active time, worker active union, producer/worker overlap, wait-with-queued
  events, active-tile skips, stale heap entries, and worker sample/tap balance.

Validation artifacts:

```text
cargo check -p casa-imaging -p casars-imager
cargo test -p casa-imaging tile_inbox --lib
cargo test -p casa-imaging standard_mfs_tile_sample_router_modes_preserve_stage_semantics --lib
cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib
cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib
```

Correctness status: targeted scheduler, residual-refresh, and standard-MFS
Briggs clean tests pass. No timing claim is made at this checkpoint; the next
bounded run must show whether early per-tile publication converts the prior
producer-only time into useful producer/worker overlap.

Decision: retain as the new fixed-tile multi-worker architecture. The previous
central row-block scheduler remains only as reference/test scaffolding while
the direct inbox path is measured. Full low-memory blocking credits and a
storage-manager-native MS cursor are still follow-up tightening work; the
current implementation records queue high-water and keeps bounded
`VisibilityBatch` chunks as the frontend cursor output. The next boundary repair
is to make that frontend cursor emit row/visibility-group columnar samples
directly from the MeasurementSet reader, so original per-row flag/weight/data
arrays are traversed once before routing to tile chunks.

### Implementation Checkpoint: MS Imaging Essentials Read Probe

Date: 2026-05-22.

Retained changes:

- Added a probe-only MS reader boundary for row-shaped imaging essentials. The
  row payload preserves native polarization/channel layout as `corr x chan`
  `DATA`, `FLAG`, optional `WEIGHT_SPECTRUM`, per-correlation `WEIGHT`, per-row
  `UVW`, and `spw_id`. It does not collapse polarizations and does not perform
  MFS-specific routing.
- Added a shared read-only `SPECTRAL_WINDOW.CHAN_FREQ` catalog keyed by SPW, so
  channel axes are not copied per row or per sample.
- Added `--ms-imaging-read-probe true` for `casars-imager`. The probe stops
  after reading/adapting MS essentials and reports throughput as JSON; no
  gridding or CLEAN work is run.

Validation artifacts:

```text
cargo check -p casars-imager
cargo run --release -p casars-imager -- \
  --ms /Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms \
  --imagename target/imperformance-wave2/read-probe/wave1-vla-single-medium \
  --imsize 2048 --cell-arcsec 0.5 --field 0 --spw 0 \
  --channel-start 0 --channel-count 512 --gridder standard --specmode mfs \
  --weighting briggs --robust 0.5 --deconvolver multiscale --scales 0,5,15 \
  --niter 2 --wterm none --ms-imaging-read-probe true
CASA_RS_MS_IMAGING_READ_PROBE_ROWS=131072 target/release/casars-imager \
  --ms /Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms \
  --imagename target/imperformance-wave2/read-probe/wave1-vla-single-medium \
  --imsize 2048 --cell-arcsec 0.5 --field 0 --spw 0 \
  --channel-start 0 --channel-count 512 --gridder standard --specmode mfs \
  --weighting briggs --robust 0.5 --deconvolver multiscale --scales 0,5,15 \
  --niter 2 --wterm none --ms-imaging-read-probe true
```

Medium-dataset probe results:

| block rows | blocks | rows read | samples read | logical GiB | total s | read s | total MiB/s | read MiB/s | DATA s | FLAG s | adapt s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32,768 | 95 | 3,086,235 | 3,160,304,640 | 26.63 | 28.878 | 24.408 | 944.2 | 1117.1 | 14.490 | 8.343 | 0.338 |
| 131,072 | 24 | 3,086,235 | 3,160,304,640 | 26.63 | 29.071 | 24.320 | 937.9 | 1121.1 | 14.445 | 8.338 | 0.276 |

Decision: retain the reader/probe seam. Throughput is dominated by bulk DATA and
FLAG reads from the MS, not row-object adaptation. Larger blocks did not
materially improve throughput, so the existing 32k row-block scale is acceptable
for this read path. The next step is to connect this row-shaped payload to the
standard-MFS tile router without reintroducing per-scalar frontend objects.

Core input trait and planned tile-inbox checkpoint:

```text
Smoke artifact: target/imperformance-wave2/ms-reader-core-trait-20260522/vla-bounded-2w-planned-smoke-summary.log
Output prefix: target/imperformance-wave2/ms-reader-core-trait-20260522/vla-bounded-2w-planned
Dataset: wave1-vla-single-medium, field 0, SPW 0, 64 channels, imsize 1024
Backend: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile, CASA_RS_STANDARD_MFS_GRID_THREADS=2
Scheduler stage: planned_dirty
Gridded samples: 188,889,033
Planned candidates: 197,519,040
Planning rejected: 8,630,007
Stage total: 11.421s
Queued bytes high-water: 39,493,200
Worker utilization: 29.808%
Correctness: targeted tests below
Decision: retained; the fixed-tile producer/router now consumes planned sample blocks through the core input trait
```

This checkpoint moves the real fixed-tile CLI path from the older
`VisibilityBatch` replay shape to `StandardMfsPlannedSampleBlockSource`.
The frontend now reads `MsImagingEssentials` row blocks, applies streaming
standard-MFS weighting, plans compact tap identities, and passes bounded planned
sample blocks to the core scheduler. The core fixed-tile dirty/PSF/residual
paths enqueue those planned samples directly into tile-local inboxes without
reconstructing a retained full-MS visibility plan or persistent `PositiveTapSet`
state.

The same command shape accidentally exercised the previous batch-backed inbox
route before the frontend guard was fixed. That smoke logged `stage=dirty` and
`stage_total_ms=15125.357`; the retained planned route logs
`stage=planned_dirty` and `stage_total_ms=11421.287`. Treat this as structural
smoke evidence only, not a formal performance comparison, because it was a
single warm-cache bounded run.

Validation checks for this checkpoint:

```text
cargo check -p casa-imaging -p casars-imager
cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
cargo test -p casa-imaging tile_inbox_caps_keep_low_worker_drains_small_and_high_worker_drains_coarse --lib
cargo test -p casa-imaging tile_inbox_scheduler_schedules_tiles_and_drains_bounded_chunks --lib
cargo test -p casa-imaging tile_inbox_planned_replay_matches_direct_dirty_and_residual --lib
cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib
cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib
cargo build --release -p casars-imager
git diff --check
```

## 2026-05-22 Worker-Side Tap Planning Split

Status: retained as architectural repair.

Artifact path:
`target/imperformance-wave2/worker-tap-planning-20260522/vla-bounded-2w-worker-taps.log`

This checkpoint splits fixed-tile ownership from full tap planning. The
frontend/planned-sample producer now locates only the deterministic
positive-tap center and keeps `u_lambda`/`v_lambda` in the compact handoff. The
direct tile inbox no longer carries `kernel_u`, `kernel_v`, or support id
fields. Tile workers call the standard gridder's full positive-tap planner when
they drain a chunk and immediately apply the sample to the resident tile buffer.

The split keeps exact tile ownership in the producer while moving
prolate-spheroidal weight lookup and full tap-span construction to the gridding
side of the producer/worker boundary. Runtime comparison was not completed in
this checkpoint because the smoke dataset path
`/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms`
was not mounted in this session.

Small local fixture smoke, after the commit:

```text
artifact=target/imperformance-wave2/worker-tap-planning-20260522/small-ms/ngc5921-fixed-tile-worker-taps.log
dataset=/Users/brianglendenning/SoftwareProjects/casatestdata/measurementset/vla/ngc5921.ms
shape=field0 spw0 channel_count=1 imsize=128 natural dirty
stage=planned_dirty enqueued_samples=4042 stage_total_ms=4.951 worker_utilization_pct=2.663

artifact=target/imperformance-wave2/worker-tap-planning-20260522/small-ms/refim-twopoints-twochan-fixed-tile-worker-taps.log
dataset=/Users/brianglendenning/SoftwareProjects/casatestdata/measurementset/vla/refim_twopoints_twochan.ms
shape=field0 spw0 channel_count=2 imsize=128 natural dirty
stage=planned_dirty enqueued_samples=119632 stage_total_ms=52.107 worker_utilization_pct=4.731
```

These local fixtures validate the planned fixed-tile path with worker-side tap
planning, but they are too small for meaningful scheduler-speed claims.

Gross product comparison against the CPU reference path:

```text
artifact=target/imperformance-wave2/worker-tap-planning-20260522/gross-smoke/small-fixture-fixed-tile-vs-cpu-products.json
fixtures=ngc5921.ms field0/spw0/channel0 imsize128 cell30arcsec; refim_twopoints_twochan.ms field0/spw0/channels0-1 imsize128 cell1arcsec
weighting=natural and briggs robust=0.5
fixed_tile_variants=1,2,10 workers
products=image,residual,psf,sumwt
failures=0
worst_fixed_tile_vs_cpu_difference=1.1641532182693481e-10
```

The small-fixture matrix found no gross product problems. Natural runs matched
the CPU reference exactly for both fixtures and all fixed-tile worker counts.
Briggs runs matched exactly on `ngc5921.ms`; `refim_twopoints_twochan.ms`
differed from the CPU reference only at about `1.2e-10` in image/residual,
with finite image, residual, PSF, and sumwt products throughout. The measured
worker utilization remains intentionally ignored as performance evidence here:
these fixtures carry only 4,042 and 119,632 gridded samples, so scheduler
startup and artifact overhead dominate.

Medium bounded gross check, after the external dataset became available:

```text
dataset=/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms
shape=field0 spw0 channel_count=64 imsize=1024 cell1arcsec briggs robust=0.5 dirty-only
artifact=target/imperformance-wave2/worker-tap-planning-20260522/medium-gross-smoke/medium-briggs-fixed-tile-vs-cpu-products.json
profile_artifacts=target/imperformance-wave2/worker-tap-planning-20260522/medium-gross-smoke/medium-briggs-fixed-2w-profiled.log; target/imperformance-wave2/worker-tap-planning-20260522/medium-gross-smoke/medium-briggs-fixed-10w-profiled.log
products=image,residual,psf,sumwt
gridded_samples=188889033
failures=0
worst_fixed_tile_vs_cpu_difference=1.4901161193847656e-08
```

| variant | wall s | user s | sys s | worst product diff | scheduler stage s | worker util | queued high-water | worker drains | wait-with-queued events | decision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CPU reference | 29.45 | 23.08 | 4.50 | reference | n/a | n/a | n/a | n/a | n/a | product reference |
| fixed tile 1 worker | 28.20 | 34.55 | 2.56 | 1.49e-08 | n/a | n/a | n/a | n/a | n/a | correctness smoke only |
| fixed tile 2 workers | 26.42 | 32.83 | 3.08 | 1.49e-08 | 11.491 | 34.3% | 59.3MB | 301,644 | 1,745 | retained as current low-risk worker count |
| fixed tile 10 workers | 34.09 | 36.68 | 42.93 | 1.49e-08 | 19.063 | 5.4% | 10.9MB | 761,555 | 2,946,763 | rejected for current scheduler shape |

The larger fixture confirms no gross product correctness issue from the
worker-side tap-planning split. The performance signal is not favorable for the
current high-worker inbox shape: 10 workers do not hit tile-lock contention
(`active_tile_skips=0`), but the run creates far more scheduler drains and
wait-with-queued-byte events, and system time jumps to about 41 seconds. The
next structural target remains the producer/core handoff representation:
preserve row/channel/correlation visibility runs instead of expanding the
handoff into scalar queued samples and many small scheduler chunks.

Tile-inbox chunk handoff checkpoint, later rejected:

```text
artifact=target/imperformance-wave2/tile-run-handoff-20260523/medium-briggs-adaptive-chunk-vs-cpu-products.json
profile_artifacts=target/imperformance-wave2/tile-run-handoff-20260523/medium-briggs-fixed-2w-adaptive-chunk.log; target/imperformance-wave2/tile-run-handoff-20260523/medium-briggs-fixed-10w-adaptive-chunk.log
shape=field0 spw0 channel_count=64 imsize=1024 cell1arcsec briggs robust=0.5 dirty-only
products=image,residual,psf,sumwt
failures=0
worst_fixed_tile_vs_cpu_difference=1.4901161193847656e-08
```

| variant | producer chunk cap | drain cap | wall s | sys s | scheduler stage s | worker drains | wait-with-queued events | decision |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| previous fixed tile 2 workers | 256 | 1024 | 26.42 | 3.08 | 11.491 | 301,644 | 1,745 | still the low-worker timing reference |
| adaptive chunk 2 workers | 256 | 1024 | 29.23 | 3.35 | 13.697 | 753,525 | 60,435 | inconclusive/regressed in this run |
| previous fixed tile 10 workers | 256 | 1024 | 34.09 | 42.93 | 19.063 | 761,555 | 2,946,763 | rejected high-worker shape |
| adaptive chunk 10 workers | 4096 | 4096 | 26.88 | 4.81 | 12.593 | 75,252 | 153,926 | rejected despite timing improvement |

The adaptive chunk experiment is rejected because it conflicts with the agreed
direct per-tile inbox contract. It reduced high-worker scheduler overhead by
holding producer-owned open chunks and publishing `StandardMfsTileQueueChunk`
work units, but the intended scheduler shape is immediate producer publication
into each tile's `Mutex<VecDeque<StandardMfsTileQueueSample>>`. Keep these
numbers as evidence that scheduler granularity matters, not as the retained
implementation path.

Direct tile-inbox correction:

```text
source_checkpoint=producer callback enqueues one StandardMfsTileQueueSample at a time
queue_shape=Mutex<VecDeque<StandardMfsTileQueueSample>>
worker_shape=drains bounded samples into worker-local StandardMfsTileQueueChunk
performance_artifact=not yet measured after correction
decision=retained as architecture correction
```

The corrected code removes the producer-side open chunk buffer. The producer
still computes the tile owner, but each accepted sample is immediately pushed to
the owning tile inbox and only tile IDs are scheduled globally. Workers drain up
to the worker-count-aware cap into a local SoA chunk before gridding. This
restores the intended separation: publish per visibility, schedule per tile,
drain per worker chunk. A new timing artifact is required before making any
performance claim about this corrected shape.

Run-based direct tile-inbox correction:

```text
source_checkpoint=tile inbox queues StandardMfsTileVisibilityRun work units
queue_shape=Mutex<VecDeque<StandardMfsTileVisibilityRun>>
producer_shape=routes lanes into consecutive same-tile runs; try_lock miss goes to producer-local pending FIFO
worker_shape=schedules tile IDs and drains all queued runs for the active tile before gridding
performance_artifact=target/imperformance-wave2/run-inbox-20260523/medium-briggs-run-inbox-summary.md
decision=retained as architecture correction only; rejected as performance improvement
```

This replaces the scalar direct inbox because scalar publication created too
many drains and ready-head transitions in the 10-worker medium run
(`258.34s wall`, `1020.53s sys`, `14,408,954 drains`). The run-based queue item
keeps the direct per-tile inbox contract while reducing scheduler traffic:
producer-owned imaging chunks remain rejected, but a visibility run is now the
unit published to the tile inbox. New scheduler summaries report
`enqueued_runs`, `worker_runs`, `avg_runs_per_drain`,
`avg_samples_per_run`, `pending_runs`, `pending_bytes`,
`pending_bytes_high_water`, and `try_lock_misses` so timing artifacts can
distinguish routing, queue contention, and worker gridding.

Run-based direct tile-inbox timing:

```text
artifact=target/imperformance-wave2/run-inbox-20260523/medium-briggs-run-inbox-summary.md
shape=field0 spw0 channel_count=64 imsize=1024 cell1arcsec briggs robust=0.5 dirty-only
products=written; correctness covered by unit/parity gates below
decision=retain as architectural correction only; performance is not acceptable yet
```

| variant | wall s | sys s | scheduler stage s | enqueued runs | worker drains | avg samples/run | wait-with-queued events | decision |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| previous fixed tile 2 workers | 26.42 | 3.08 | 11.491 | n/a | 301,644 | n/a | 1,745 | still faster |
| run inbox 2 workers | 49.01 | 18.57 | 34.065 | 8,433,552 | 8,331,616 | 22.397 | 4,407,043 | rejected for performance |
| previous fixed tile 10 workers | 34.09 | 42.93 | 19.063 | n/a | 761,555 | n/a | 2,946,763 | still faster |
| scalar direct inbox 10 workers | 258.34 | 1020.53 | 242.731 | n/a | 14,408,954 | ~13 | 58,541,341 | rejected |
| run inbox 10 workers | 158.63 | 625.34 | 144.778 | 8,433,552 | 8,353,403 | 22.397 | 40,979,758 | better than scalar direct, still rejected |

This confirms the direct run queue is not sufficient while the producer is fed
by scalar planned-sample replay. The remaining handoff problem is upstream of
the inbox: `enqueued_runs=8,433,552` for `188,889,033` lanes means only about
`22.4` lanes per run, and workers still see nearly one drain per run. The next
retained target must feed the inbox with real row/channel visibility runs or add
a measured ready-threshold policy without returning to producer-owned imaging
chunks.

Run-inbox ready-threshold repair:

```text
source_checkpoint=tile inbox queues StandardMfsTileVisibilityRun work units with thresholded ready-head publication
queue_shape=Mutex<VecDeque<StandardMfsTileVisibilityRun>>
producer_shape=routes lanes/runs into the tile inbox immediately; ready-head wakeup waits for queued-sample threshold or stage close
override=CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN=<samples>
artifact_dir=target/imperformance-wave2/run-inbox-threshold-20260523/
decision=retained as scheduler repair; still not a full performance win versus previous fixed tile
```

The threshold does not reintroduce producer-owned chunks. Runs are still pushed
to the owning tile inbox immediately, and only tile IDs are scheduled globally.
The change defers the ready-head transition until a tile has enough queued lane
samples to keep a worker busy, or until stage close forces remaining below-
threshold queues to drain. With the default threshold of 1024 samples, scheduler
traffic drops by about 45x on the medium dirty-only run.

Run-inbox ready-threshold timing:

```text
shape=field0 spw0 channel_count=64 imsize=1024 cell1arcsec briggs robust=0.5 dirty-only
env=CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_EDGE=32 CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_boundary
artifact_2w=target/imperformance-wave2/run-inbox-threshold-20260523/medium-briggs-threshold-2w.log
artifact_10w=target/imperformance-wave2/run-inbox-threshold-20260523/medium-briggs-threshold-10w.log
correctness=targeted scheduler tests pass; parity gates listed below
decision=retain the threshold; next bottleneck remains scalar planned-sample replay into run wrappers
```

| variant | wall s | sys s | scheduler stage s | ready sample min | enqueued runs | worker drains | avg runs/drain | wait-with-queued events | decision |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| previous fixed tile 2 workers | 26.42 | 3.08 | 11.491 | n/a | n/a | 301,644 | n/a | 1,745 | still faster |
| run inbox 2 workers | 49.01 | 18.57 | 34.065 | 1 | 8,433,552 | 8,331,616 | 1.012 | 4,407,043 | rejected |
| threshold run inbox 2 workers | 39.26 | 3.54 | 25.545 | 1024 | 8,433,552 | 182,121 | 46.307 | 236,264 | retained scheduler repair |
| previous fixed tile 10 workers | 34.09 | 42.93 | 19.063 | n/a | n/a | 761,555 | n/a | 2,946,763 | still faster |
| run inbox 10 workers | 158.63 | 625.34 | 144.778 | 1 | 8,433,552 | 8,353,403 | 1.010 | 40,979,758 | rejected |
| threshold run inbox 10 workers | 38.45 | 11.30 | 25.001 | 1024 | 8,433,552 | 182,168 | 46.295 | 1,608,711 | retained scheduler repair |

This is a real repair to the run-inbox scheduler shape: the 10-worker wall time
falls from `158.63s` to `38.45s`, system time falls from `625.34s` to `11.30s`,
and drains fall from `8,353,403` to `182,168`. It is still not the final
producer/worker boundary. The run count and average samples per run are
unchanged because the current feed still wraps scalar planned samples rather
than publishing true row/channel visibility runs. Worker utilization remains
low on 10 workers because producer/preprocessing dominates the stage.

## 2026-05-23: Planned scalar-run replay seam

Artifact directory:
`target/imperformance-wave2/scalar-run-fastpath-20260523/`

Workload:

```text
Medium 64-channel, 1024-pixel, field 0, spw 0, Briggs robust 0.5,
dirty-only standard-MFS fixed-tile streaming profile, 10 workers.
Command shape: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
               CASA_RS_STANDARD_MFS_GRID_THREADS=10
               CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1
               target/release/examples/profile_imager ... --gridder standard
               --channel-count 64 --imsize 1024 --cell-arcsec 1 --dirty-only
```

Implemented a planned scalar-run block interface and promoted it to the default
fixed-tile streaming handoff. When an entire run's planned tap centers belong
to one tile, the producer emits one vector-valued tile run rather than
re-routing each scalar through the run accumulator. When a run crosses tile
boundaries, it is split into consecutive same-tile run fragments in input
order. `CASA_RS_STANDARD_MFS_PLANNED_RUN_BLOCKS=0` is retained only as an
emergency comparison override.

The first scalar-run implementation does not improve the current workload. It
mostly preserves row boundaries around scalar planned samples while the producer
still performs scalar planning, so it does not yet move lane-level work into the
workers. It is nevertheless the default now because the fixed-tile inbox
contract is run-shaped: push visibility lanes as runs, schedule by tile, and
let workers drain vector-valued tile runs.

| variant | artifact | wall s | sys s | scheduler stage | frontend ms | core ms | enqueued runs | worker drains | decision |
|---|---|---:|---:|---|---:|---:|---:|---:|---|
| per-row callback scalar-run experiment | `medium-briggs-10w.log` | 42.26 | 13.18 | `planned_run_dirty` | n/a | 27,384 | 8,822,351 | 182,237 | rejected, row callbacks split adjacent same-tile runs |
| run-block replay before accumulator coalescing | `medium-briggs-runblock-10w.log` | 42.84 | 14.87 | `planned_run_dirty` | n/a | 28,166 | 8,822,351 | 182,187 | rejected |
| run-block replay with shared accumulator | `medium-briggs-runblock-accum-10w.log` | 42.77 | 15.03 | `planned_run_dirty` | n/a | 27,924 | 8,433,539 | 182,176 | rejected as performance claim |
| restored scalar-block comparison before default flip | `medium-briggs-default-restored-profile-10w.log` | 39.49 | 13.90 | `planned_dirty` | 39,430 | 26,380 | 8,433,552 | 182,179 | comparison only |
| run-block seam after gating | `medium-briggs-runblock-gated-profile-10w.log` | 40.15 | 14.19 | `planned_run_dirty` | 40,090 | 26,930 | 8,433,539 | 182,176 | promoted to default for architecture, not speed |
| run-block default after default flip | `medium-briggs-runblock-default-profile-10w.log` | 54.66 | 17.53 | `planned_run_dirty` | 53,670 | 39,924 | 8,433,539 | 182,187 | confirms default path; timing rejected as noisy |
| scalar fallback after default flip | `medium-briggs-scalar-fallback-profile-10w.log` | 54.57 | 17.28 | `planned_dirty` | 54,470 | 38,364 | 8,433,552 | 182,182 | comparison only; same slowdown implies system/run noise |

Correctness status: `cargo test -p casa-imaging
tile_inbox_planned_replay_matches_direct_dirty_and_residual --lib` covers the
planned run-block dirty/residual replay against the direct tiled path.
`cargo test -p casa-imaging
planned_same_tile_samples_enqueue_as_one_visibility_run --lib` covers the
same-tile short circuit. Broader standard-MFS correctness gates remain required
before this branch is closed.

Decision: keep the scalar-run block type and executor entry points as the
default fixed-tile streaming handoff. The old scalar-block path is only an
explicit fallback via `CASA_RS_STANDARD_MFS_PLANNED_RUN_BLOCKS=0`. The next
performance step remains true row/channel visibility runs that move lane-level
flagging, weighting, tap planning, and gridding work into tile workers.

Validation checks for this checkpoint:

```text
cargo check -p casa-imaging -p casars-imager
cargo test -p casa-imaging positive_tap_center_locator_matches_positive_tap_plan --lib
cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib
cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib
cargo test -p casa-imaging tile_inbox_scheduler_schedules_tiles_and_drains_all_runs --lib
cargo test -p casa-imaging tile_inbox_producer_pending_retries_fifo_after_try_lock_miss --lib
cargo test -p casa-imaging planned_same_tile_samples_enqueue_as_one_visibility_run --lib
cargo test -p casa-imaging tile_inbox_planned_replay_matches_direct_dirty_and_residual --lib
cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib
cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib
cargo build --release -p casars-imager
```

## Scalar Run Slowdown Attribution

Date: 2026-05-23

Workload: medium 64-channel, 1024-pixel, Briggs dirty-only,
`CASA_RS_STANDARD_MFS_BACKEND=fixed_tile`,
`CASA_RS_STANDARD_MFS_GRID_THREADS=10`,
`CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1`.

Artifact directory:
`target/imperformance-wave2/scalar-run-slowdown-20260523/`

| candidate | timing artifact | sample artifact | frontend s | core s | stage s | sys s | decision |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| default scalar-run slowdown | terminal-captured profile | `profile-imager-default-10s.sample.txt` | 58.03 | 41.06 | 40.95 | n/a | rejected; sampled in `RawVec::grow_one` / `_realloc` while constructing runs |
| same-tile preallocation only | `medium-briggs-runblock-prealloc-10w.log` | `profile-imager-prealloc-10s.sample.txt` | 54.34 | 40.77 | 40.67 | 16.29 | rejected; mixed-tile fragment path still hit scalar append allocation |
| segment preallocation | `medium-briggs-runblock-segment-prealloc-10w.log` | `profile-imager-segment-prealloc-10s.sample.txt` | 40.22 | 26.81 | 26.69 | 15.75 | retained; restores run-block path to pre-slowdown range |
| segment preallocation, sampled | `medium-briggs-runblock-segment-prealloc-sampled-10w.log` | `profile-imager-segment-prealloc-10s.sample.txt` | 45.56 | 28.33 | 28.22 | 15.72 | diagnostic only; sampling overhead/noise, still showed worker-side run concatenation |
| no worker-side run concatenation | `medium-briggs-runblock-no-concat-unsampled-10w.log` | `profile-imager-no-concat-10s.sample.txt` | 39.23 | 26.10 | 25.99 | 15.58 | retained; removes worker drain copy/reallocation and is slightly faster unsampled |

Observed root cause: the run-block path was not actually publishing large work
units cheaply. The producer built most runs by repeated scalar appends, so the
main thread sampled in `StandardMfsTileVisibilityRun::push_sample` and allocator
growth (`RawVec::grow_one` / `_realloc`). After exact segment preallocation, the
allocator hotspot moved out of the dominant stack and wall time returned to the
pre-slowdown range.

Second observed issue: workers drained tile queues by concatenating queued runs
into one synthetic `StandardMfsTileVisibilityRun`, which copied every per-sample
column vector and reallocated under `append_run`. The retained fix keeps drained
work as `Vec<StandardMfsTileVisibilityRun>` and iterates the runs directly.

Remaining hotspot: sampled current code still spends producer time in
`push_planned_dirty_samples_to_run_accumulator` /
`StandardMfsTileVisibilityRun::push_sample`. That is expected until the queue
payload becomes a true row/channel visibility run instead of scalar-field vectors.
The next candidate should avoid copying `u/v/w`, flags, weights, and visibility
into per-lane vectors when an MS row or contiguous channel span belongs to one
tile.

## 2026-05-23 Producer/Scheduler Instrumentation Pass

Workload:
`wave1-vla-single-medium.ms`, field 0, spw 0, 64 channels, imsize 1024,
Briggs robust 0.5, dirty-only, fixed-tile backend, 10 workers, tile edge 32,
center-boundary anchor, `CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1`.

Artifact directory:
`target/imperformance-wave2/instrumented-producer-20260523/`

| candidate | timing artifact | sample artifact | frontend s | core s | scheduler stage s | peak RSS GiB | decision |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| line-detail probe, ungated | `medium-briggs-producer-detail-10w.log` | n/a | 92.80 | 77.48 | 77.03 | n/a | rejected as measurement artifact; per-lane `Instant::now()` dominated |
| normal detail after line gate | `medium-briggs-normal-detail-10w.log` | n/a | 31.82 | 18.91 | 17.73 | 8.90 | baseline for this pass |
| cached profile env gates + queue pre-reserve | `medium-briggs-core-env-queue-reserve-10w.log` | `profile-imager-core-env-queue-reserve-10s.sample.txt` | 30.95 | 18.45 | 18.33 | 8.90 | retained as low-risk instrumentation/hot-path cleanup; frontend movement below strict timing gate |
| no temporary producer `VecDeque` on common enqueue path | `medium-briggs-no-temp-vecdeque-10w.log` | `profile-imager-no-temp-delayed-10s.sample.txt` | 30.93 | 17.65 | 17.54 | 8.85 | retained as architectural cleanup; removes sampled per-run allocation path, frontend movement still noisy |
| routed final Briggs weighting, invalid line-detail flag | `medium-briggs-routed-weighting-10w.log` | n/a | 82.92 | 70.04 | n/a | n/a | rejected as measurement artifact; `CASA_RS_STANDARD_MFS_PROFILE_LINE_DETAIL=0` still enabled per-lane timings before boolean env parsing was fixed |
| routed final Briggs weighting in tile workers | `medium-briggs-routed-weighting-10w-rerun.log` | `profile-imager-routed-weighting-10s.sample.txt` | 29.81 | 16.97 | 16.85 | 8.88 | retained; moves final Uniform/Briggs `weight_sample` from producer to fixed-tile workers and improves frontend/core by about 3.6%/3.8% versus the previous retained row |
| inline hot routing/tap accessors | `medium-briggs-hot-inline-routed-cell1-10w.log` | n/a | 27.94 | 15.53 | 15.40 | 8.89 | retained; sampled `route_sample` and `positive_taps_at` remained visible after routed weighting, and narrow `#[inline]` hints improved frontend/core by about 6.3%/8.5% versus the previous retained row |
| row/channel vector payload | `medium-briggs-row-vector-payload-10w.log`, `medium-briggs-row-vector-payload-rerun-10w.log` | n/a | 28.55 / 30.33 | 15.16 / 14.95 | 15.04 / 14.83 | 8.90 / 8.92 | retained as architectural boundary change; queued payload falls from 15.65 GB scalar fields to 3.56 GB row/run references and core/scheduler are modestly faster, but frontend wall is not claimed due MS-read variance |
| direct row-run publish + shared tap centers | `medium-briggs-direct-row-run-no-tap-copy-10w.log` | n/a | 27.46 | 14.46 | 14.33 | 8.90 | retained; removes row-block run staging and queued tap-center copies, queued payload falls again to 2.19 GB, and frontend/core improve by about 1.7%/7.0% versus the inline scalar retained row |
| ready threshold 1 | `medium-briggs-ready-min1-10w.log` | n/a | 97.56 | 84.42 | 84.34 | 8.95 | rejected; removing the 1024-sample ready gate exploded ready heads/drains to 8.07M and made scheduler traffic dominate |
| center-quadrant 4-tile partition | `medium-briggs-center-quadrants-10w.log`, `medium-briggs-center-quadrants-4w.log` | n/a | 21.20 / 20.74 | 7.85 / 7.52 | 7.77 / 7.45 | 8.85 / 8.89 | retained as coarse adaptive baseline; four center-boundary quadrants cut scheduler work and halo overhead before hot-tile splitting |
| one-pass routed run splitter | `medium-briggs-one-pass-run-split-routed-cell1-10w.log` | n/a | 29.19 | 16.68 | 16.55 | 8.80 | rejected and reverted; removing the common single-owner pre-scan regressed frontend/core versus the inline retained row |
| unchecked row array access | `medium-briggs-unchecked-row-access-routed-cell1-10w.log` | n/a | 29.57 | 16.93 | 16.80 | 8.84 | rejected and reverted; prevalidating row shapes plus unchecked array access regressed the same target versus the inline retained row |

The first sampled run found an instrumentation bug rather than an imaging
algorithm issue: normal profile mode still reached `getenv` from the hot row
path. The retained fix caches the standard-MFS profile gates once per process
in both frontend and core.

The next sampled stack showed producer-side allocation under
`StandardMfsTileInboxProducer::enqueue_run`: the common path created a temporary
one-element `VecDeque` before every tile enqueue. The retained fix publishes the
single-run common path directly into the tile queue, leaving the pending
`VecDeque` path only for rare `try_lock` misses.

After those fixes, delayed sampling still shows the dominant shape is producer
work versus worker work, not tile lock contention: the producer continues to do
final Briggs weighting and center planning before enqueue, while tile workers
mostly wait. The next plausible >2% structural candidate is to move final
per-lane weighting and route-only center calculation across the frontend/core
boundary so the producer reads and routes compact row/channel runs while tile
workers perform the lane-level imaging semantics.

The routed-weighting step adds that boundary for the fixed-tile multi-worker
path: the frontend emits natural-weighted routed samples, and tile workers apply
the final density-dependent weighting immediately before tap application. The
valid rerun improved the dirty-only medium target from `30.93s` frontend /
`17.65s` core to `29.81s` frontend / `16.97s` core. Worker utilization remains
low (`10.3%`), and the follow-up sample still shows producer/read work as the
larger remaining bottleneck rather than tile locks or scheduler contention.

The follow-up sample also kept tiny routing/tap accessor calls visible in the
hot loops. Adding narrow inline hints for the frontend route path and worker
tap/weight accessors improved the same target to `27.94s` frontend / `15.53s`
core without changing the memory shape.

One attempted producer cleanup after that, a one-pass routed run splitter,
regressed the same target. The common single-owner pre-scan is kept for now; the
next run-shape change needs to be the larger row/channel vector payload rather
than another scalar-run splitter tweak.

A second scalar-path cleanup, unchecked row array access after shape
prevalidation, also regressed. The row access path remains bounds-checked; the
larger row/channel vector payload is still the next structural boundary change.

The row/channel vector payload replaces scalar queue samples with row-backed
tile runs: each run keeps an `Arc` to the row matrices and only stores the
contiguous channel span plus tap centers. A first attempt exposed a routing
adapter bug where row-backed runs were considered empty because their scalar
sample vector is intentionally empty; the regression test
`row_backed_visibility_runs_are_not_discarded_as_empty` now covers that path.
The measured outcome is structurally positive but not a frontend timing win:
logical enqueued queue bytes drop from `15.65 GB` to `3.56 GB`, core/scheduler
time moves from `15.53s`/`15.40s` to `15.16s`/`15.04s` and `14.95s`/`14.83s`
across two runs, while total frontend wall varies upward with slower MS reads.
This is retained because it removes the scalar payload duplication across the
frontend/core boundary without weakening correctness, not because it settles
the worker-utilization bottleneck.

The follow-up direct row-run publish step removes the remaining avoidable
staging in that boundary. The frontend now calls the fixed-tile consumer for
each routed row/channel run as it is produced, instead of filling a
`StandardMfsRoutedVisibilityRunBlock` for the current row block. The tile queue
also keeps a shared `Arc` to the original tap-center run plus a range, instead
of allocating a copied tap-center slice for every tile-local subrun. On the same
medium 10-worker dirty target this reduced logical queued bytes from `3.56 GB`
to `2.19 GB`, reduced producer preprocess from roughly `4.69-4.82s` to
`3.46s`, and improved core/scheduler to `14.46s`/`14.33s`. Worker utilization
is still low (`16.5%`), so this fixes the payload/lifetime sloppiness but does
not close the larger producer-vs-worker balance problem.

The ready-threshold probe answered whether the tile inbox should wake workers
on any nonempty queue instead of waiting for the default `1024` queued samples.
It should not. With `ready_sample_min=1`, the same medium 10-worker dirty target
regressed to `97.56s` frontend / `84.42s` core / `84.34s` scheduler stage.
Ready heads and worker drains rose from about `182k` to `8.07M`, while average
runs per drain fell from about `46` to `1.09`. The default stays at `1024`;
the env override remains available for diagnostics.

The center-quadrant probe adds `CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_quadrants`,
which creates four fixed tiles whose boundary intersection is the standard
gridder's integer center. It is retained as the coarse adaptive baseline for the
next scheduler pass. On the same medium target, the 10-worker run improved to
`21.20s` frontend / `7.85s` core / `7.77s` scheduler stage, and the 4-worker
run improved slightly further to `20.74s` / `7.52s` / `7.45s`. Resident tile
bytes fell from `73.93 MB` to `50.48 MB`; logical enqueued bytes fell from
`2.19 GB` to `0.75 GB`; worker drains fell from `181,712` to about `23,500`;
and measured halo overhead fell to `0.96%`. With only four exclusive resident
tiles, the 10-worker utilization percentage is expected to stay low; the next
step is explicit hot-tile splitting so large quadrant queues can feed more than
one worker through private scratch/reduction buffers when the data justifies it.

Follow-up tile bookkeeping artifacts:

```text
dirty_only=target/imperformance-wave2/instrumented-producer-20260523/medium-briggs-center-quadrants-4w-tile-bookkeeping.log
niter2=target/imperformance-wave2/instrumented-producer-20260523/medium-briggs-center-quadrants-4w-niter2-tile-bookkeeping.log
```

The added per-tile fields show the current 4-worker utilization is capped mostly
by quadrant imbalance, not by tile queue locking. In the dirty-only probe, tile
3 carried `78.43M` of `188.89M` samples (`41.52%`) and `5.18s` of tile-active
work, versus tiles 0 and 2 at about `27-28M` samples and `2.1s` each. The
ideal utilization bound from per-tile active time was `64.51%`, while measured
worker utilization was `42.00%`. The same pattern held in the `niter=2`
residual refresh: tile 3 again carried `41.52%` of samples and `5.54s` active
time, with a `64.60%` tile-active bound and `51.41%` measured utilization.
`active_tile_skips=0`, `stale_heap_entries=0`, and try-lock misses stayed in
the low thousands across about `3.0M` enqueued runs, so queue mutex contention
is not the first-order explanation. Hot splitting tile 3 is the next data-backed
candidate.

The temporal hot-split probe implemented a diagnostic path where multiple
workers could drain separate chunks from the same hot tile, each worker gridded
into a private tile scratch buffer, and the task merged its scratch tile into
the resident tile when it finished. Correctness gates passed, but the bounded
medium timing rejected this shape as a retained optimization, and the code path
has been removed from the production backend:

| Candidate | Artifact | Frontend | Core | Scheduler | Split drains | Worker util | Peak RSS | Decision |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| retained center-quadrant 10w baseline | `target/imperformance-wave2/instrumented-producer-20260523/medium-briggs-center-quadrants-10w.log` | 21.20s | 7.85s | 7.77s | n/a | 16.3% | n/a | reference |
| temporal split, default ready threshold | `target/imperformance-wave2/hot-split-20260524/medium-briggs-temporal-split-profile-imager-hogbom-10w.log` | 26.40s | 13.27s | 13.16s | 6,854 | 57.9% | 9.51GB | rejected |
| temporal split, coarse ready threshold | `target/imperformance-wave2/hot-split-20260524/medium-briggs-temporal-split-coarse-ready-profile-imager-hogbom-10w.log` | 27.07s | 14.08s | 13.98s | 20 | 19.3% | 9.55GB | rejected |

The default-ready run proves the scheduler can create more worker activity, but
wall time regresses because scratch allocation, full-tile scratch writes, and
scratch-to-resident merges add more work than the added parallelism removes. The
coarse-ready run avoids thousands of scratch reductions, but it delays work
publication and still regresses. This supports the earlier concern that
temporal splitting is memory/bandwidth expensive. The next hot-tile direction,
if pursued, should be spatial splitting or a lower-copy subtile/stripe
reduction, not whole-tile temporal scratch. One accidental full-shape
`run_workload.py` attempt during this probe was interrupted and is not used as
evidence.

Spatial hot splitting was added as a quadtree owner-partition experiment. The
partition could either split all four center quadrants uniformly or replace
selected hot parent tiles with four spatial children. It kept the direct
tile-inbox contract: no temporal scratch buffers, no per-sample scheduler
tasks, and one active worker per spatial tile. The measured gains were too small
and inconsistent for the added surface area, so this code path has been removed
from the retained backend.

| Candidate | Artifact | Frontend | Core | Scheduler | Tile count | Hot share | Worker util | Peak RSS | Decision |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| retained center-quadrant 4w baseline | `target/imperformance-wave2/priority-ready-20260524/medium-briggs-center-quadrants-4w-priority-inbox.log` | 20.40s | 7.24s | 7.17s | 4 | 41.52% | 47.3% | 9.55GB | reference |
| all center quadrants split one level | `target/imperformance-wave2/spatial-split-20260524/medium-briggs-all-quadrants-quadtree-4w.log` | 19.84s | 6.97s | 6.90s | 16 | 25.77% | 47.5% | 9.55GB | rejected; extra routing/drain overhead reduces the benefit |
| selected hot tile 3 split one level | `target/imperformance-wave2/spatial-split-20260524/medium-briggs-hot-tile3-quadtree-4w.log` | 19.56s | 6.72s | 6.64s | 7 | 29.24% | 48.8% | 9.55GB | retained for next pass |
| selected tiles 1 and 3 split one level | `target/imperformance-wave2/spatial-split-20260524/medium-briggs-hot-tiles1-3-quadtree-4w.log` | 19.79s | 6.85s | 6.77s | 10 | 25.77% | 48.5% | 9.58GB | rejected; no improvement over splitting tile 3 only |
| selected hot tile 3 split two levels | `target/imperformance-wave2/spatial-split-20260524/medium-briggs-hot-tile3-quadtree-depth2-4w.log` | 19.94s | 7.01s | 6.94s | 19 | 29.24% | 47.2% | 9.55GB | rejected; recursive split adds overhead without reducing the remaining global hot tile |

The retained selected split improves the bounded dirty-only 4-worker core by
about `7.3%` and frontend wall by about `4.1%` versus the current
center-quadrant priority baseline. It does not solve utilization by itself: the
next hot tile after splitting parent tile 3 is the unsplit parent tile 1, and
worker utilization remains just under `50%`. Splitting tile 1 as well did not
beat the selected tile-3-only run on this measurement, so the next parallel pass
should focus on reducing routing/publication overhead or adding a data-driven
split planner rather than blindly increasing tile count.

A heavier clean probe was run to check whether more repeated residual-refresh
work makes spatial splitting more valuable. The workload used the same
64-channel, 1024-pixel medium MS but ran multiscale clean with `niter=500` and
`minor_cycle_length=50`, producing `11` major cycles and `500` minor
iterations.

| Candidate | Artifact | Frontend | Core | PSF grid | Residual grid/degrid | Major refresh | Worker util | Peak RSS | Decision |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| center-quadrant 4w baseline | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w.log` | 97.81s | 85.17s | 3.86s | 70.81s | 67.49s | 50-51% residual stages | 9.55GB | retained baseline winner |
| selected hot tile 3 split one level | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-hot-tile3-4w.log` | 97.25s | 84.40s | 3.40s | 70.50s | 67.62s | 52-53% residual stages | 9.55GB | weak signal only; not enough to justify more manual splitting |
| residual metadata cache, center-quadrant 4w | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w-metadata-cache.log`; RSS rerun `...-metadata-cache-rss-unsandboxed.log` | 95.90s `/usr/bin/time`; 98.78s RSS rerun | n/a | n/a | 61.99s residual stage sum; 65.04s RSS rerun | n/a | 60-61% residual stages | 12.85GB max RSS / 13.46GB peak footprint; metadata cache logical bytes 6.10GB | gated experiment retained for follow-up; needs planner accounting before default |

The heavier run confirms the split is not the dominant lever. The selected
split improved frontend by only `0.6%` and core by `0.9%`; residual
grid/degrid moved by less than `0.5%`, while major-cycle refresh was
effectively flat. The tile-worker utilization rises slightly in the repeated
residual stages, but only to roughly `52-53%`. This points back to the
producer/router and work-publication path, not more static spatial splitting,
as the higher-leverage parallel target.

The residual metadata-cache experiment keeps the center-quadrant partition and
caches invariant row/routing metadata after the initial dirty pass: UVW,
flags, weights, channel lambda scales, tap centers, and run boundaries. Residual
passes still reread DATA, but no longer reread FLAG/WEIGHT/UVW or reroute tap
centers. The residual replay detail confirms `get_flag_ms=0`,
`get_weight_ms=0`, and `get_geometry_ms=0` after the initial pass. Residual
stage total across the ten refreshes moved from `66.96s` to `61.99s`, while
whole-run wall moved from `97.81s` to `95.90s` on the first run. An
unsandboxed `time -l` rerun was noisier at `98.78s`, but reported
`12.85GB` maximum resident set size and `13.46GB` peak memory footprint, below
the default `16GiB` active target on this machine. The cache accounted about
`6.10GB` of logical metadata for a modest and noisy speed signal, so it was not
retained. Any future equivalent must be budgeted directly by the central memory
planner before it is reintroduced.

Adaptive per-chunk tiling was then extended from dirty/PSF into residual
refresh so the heavy-clean case could test the strongest version of equal-work
chunking. The result is useful diagnostic evidence but is rejected as a runtime
backend shape: it balances worker work inside each chunk, but repeated
per-chunk planning and routing across every major refresh more than erases that
gain.

| Candidate | Artifact | Wall/frontend | Core/profile | Dirty adaptive total | Residual adaptive total | Worker balance | Decision |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| center-quadrant 4w baseline | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w.log` | 97.81s | 85.17s | n/a | 70.81s residual grid/degrid | 50-51% residual utilization, hot tile about 41.6% | reference |
| adaptive chunk 4w, 4M lanes/chunk | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-4w.log` | 193.52s `/usr/bin/time` | final profile line not emitted | 5.09s | 5.52s to 8.04s per residual refresh summary | per-chunk residual workers mostly 90-98%, hot share about 25% | rejected; keep as diagnostic proof that equal-work chunking alone is not enough |
| adaptive chunk 4w with partition cache | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-cache-4w.log` | 152.88s `/usr/bin/time` | final profile line not emitted | 5.29s | 4.04s to 5.89s per residual refresh summary | per-refresh partition cache hits 50/50, `plan_tiles_total_ms=0` | improved but still rejected versus center quadrants |

The adaptive scheduler portion improves the local balance signal, but the run
is about `2.0x` slower end-to-end than the center-quadrant baseline. The log
shows why: each residual refresh repeats `50` chunk plans, with tile planning
alone rising from about `1.36s` to `2.15s` per refresh and route/enqueue around
`0.9s` to `1.28s` per refresh. Later adaptive residual streaming passes spend
roughly `13s` to `17s` in the replay/prepare path, while the fixed-quadrant
residual pass is roughly `6s` to `7s`. The retained lesson is that load-balanced
chunk partitions are useful, but not if they are recomputed and rerouted from
scratch for every major-cycle refresh.

An adaptive partition cache was then added because the logged chunk-shape
summaries showed that dirty and every residual refresh recomputed the same
partition for each chunk index. The cache keys by grid shape, halo, requested
tile count, chunk index, run count, lane count, and a hash plus first/last
center sentinels. It keeps only the lightweight partition, not visibility
payloads or tap plans. This removes repeated partition planning from residual
refresh (`partition_cache_hits=50`, `partition_cache_misses=0`,
`plan_tiles_total_ms=0.000` for each residual refresh), improving the adaptive
heavy run from `193.52s` to `152.88s`. The result remains `56%` slower than
the retained center-quadrant baseline, so adaptive chunking is still diagnostic
only. The likely remaining adaptive overhead is repeated route/enqueue plus
per-chunk scheduler/store setup, not tile-shape planning.

UV/tile distribution plots were generated to inspect why the manual hot split
did not move the heavy-clean result:

```text
target/imperformance-wave2/uv-tile-plots-20260524/hot-tile3/uv_tile_distribution_timeseries.png
target/imperformance-wave2/uv-tile-plots-20260524/hot-tile3/uv_tile_distribution_center_zoom.png
target/imperformance-wave2/uv-tile-plots-20260524/hot-tile3/uv_tile_shares_by_time.png
target/imperformance-wave2/uv-tile-plots-20260524/hot-tile3/summary.json
```

The plots sample five active row windows (`0%`, `25%`, `50%`, `75%`, `95%`) in
the same 64-channel, 1024-pixel, 0.5arcsec standard-MFS geometry. They show
that splitting parent tile 3 into four spatial children mostly creates three
nearly empty children; the child nearest the grid origin remains hot. They also
show that the hottest quadrant changes over row-order time: tile 1 dominates
the `0%`, `50%`, and `75%` windows, while the split child tile 3 dominates the
`25%` and `95%` windows. Representative top-tile shares are `49.4%` at the
`25%` window and `63.4%` at the `95%` window. This explains why the stage-level
hot-tile share and the manual split performance were misleading: row-order
windows are much more imbalanced than the whole-stage totals.

A live queue snapshot diagnostic was then added because the static row-window
plots still do not show what workers can actually consume at a given instant.
It wrote sampled CSV snapshots with `queued` tap centers from the live tile
inboxes, `in_flight` tap centers that workers had drained into local chunks but
not finished yet, and a bounded reservoir of `gridded` tap centers that workers
had already completed. This diagnostic was useful for the investigation but was
removed from the retained backend.

Verification and diagnostic artifact:

```text
cargo test -p casa-imaging tile_inbox_scheduler_schedules_tiles_and_drains_all_runs --lib
target/imperformance-wave2/live-queue-snapshots-20260524/queue_snapshot_timeseries.png
target/imperformance-wave2/live-queue-snapshots-20260524/snapshots/
```

The first medium dirty diagnostic run used 4 center-quadrant workers,
64 channels, 1024 pixels, Briggs weighting, and `niter=0`. Runtime was
`40.22s` with snapshot sampling enabled, so it should not be compared as a
performance number. The plot shows queue contents arriving in bursts, but it
also corrects an important ambiguity in the earlier queued-only view:
`queued=0` often means the work has already been drained into an active worker
chunk, not necessarily that all workers are starving. Representative snapshots
show tens of thousands of in-flight samples, and one publish snapshot shows
`745,007` queued samples. This is a better visualization of the producer,
in-flight worker, and completed-work states than either the static row-window
UV plots or the queued-only snapshot prototype.

## Metal Real-Data Residual Refresh Screen

Artifact:

```text
target/imperformance-wave2/metal-real-residual-20260524/README.md
target/imperformance-wave2/metal-real-residual-20260524/medium-briggs-stride10.json
target/imperformance-wave2/metal-real-residual-20260524/medium-briggs-stride10.bin
```

After the dirty-only Metal global-atomic screen was rejected, the next screen
targeted the heavier exact residual-refresh work: degrid from an immutable
model grid and grid weighted residuals. A Rust-side fixture exporter now streams
the real MeasurementSet through the existing standard-MFS Briggs density and
routed visibility paths and writes compact 32-byte `MetalGridSample` records
for the Swift Metal harness. The exporter does not retain the full MeasurementSet
visibility state or persistent tap plans.

Fixture command shape:

```text
cargo run --release -p casars-imager --example export_metal_fixture -- \
  --output target/imperformance-wave2/metal-real-residual-20260524/medium-briggs-stride10.bin \
  --metadata target/imperformance-wave2/metal-real-residual-20260524/medium-briggs-stride10.json \
  --max-samples 20000000 --sample-stride 10 -- \
  --ms /Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms \
  --imagename target/imperformance-wave2/metal-real-residual-20260524/export-placeholder \
  --field 0 --spw 0 --channel-start 0 --channel-count 64 --specmode mfs \
  --weighting briggs --robust 0.5 --deconvolver multiscale --scales 0,5,15 \
  --imsize 1024 --cell-arcsec 0.5 --niter 500 --minor-cycle-length 50 \
  --gridder standard --dirty-only
```

Fixture result:

| accepted samples | written samples | stride | export elapsed | fixture size | center range |
| ---: | ---: | ---: | ---: | ---: | --- |
| 197,519,040 | 19,751,904 | 10 | 22.517s | 603 MiB | x=85..1165, y=403..1105 |

The center range confirms the Metal harness must use the 1250 padded grid for
this image, not the user-facing 1024 image size.

Metal command:

```text
swift run -c release MetalGridExperiment \
  --prepared-samples-bin ../../../target/imperformance-wave2/metal-real-residual-20260524/medium-briggs-stride10.bin \
  --samples 19751904 --imsize 1250 --support 3 --tile-edge 625 \
  --skip-slow-baselines --strategies residual_refresh_global_atomic \
  --no-cpu-reference --repeats 3
```

| run | GPU kernel | download | rate | projected full-refresh kernel |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.496325s | 0.001905s | 39.80M samples/s | 4.963s |
| 2 | 0.435073s | 0.002012s | 45.40M samples/s | 4.351s |
| 3 | 0.466779s | 0.001810s | 42.32M samples/s | 4.668s |

The retained CPU heavy-clean baseline is
`target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w.log`:
frontend `97.811s`, core `85.169s`, total `residual_degrid_grid=70.815s`,
and `major_cycle_refresh=67.489s` across ten major refreshes. That gives about
`7.081s` per residual degrid/grid refresh, or `6.749s` per major refresh.

A small correctness smoke using the same binary loader and the first 2,000 real
fixture samples produced `relative_rms_error=8.625532e-08` against the Swift CPU
reference.

Decision: retain Metal residual refresh as a plausible future optional backend
target, but do not make it the default or call it a clear win yet. On the real
sample distribution, the global-atomic residual kernel projects to a median
`4.67s` full-refresh kernel, about `1.5x` faster than the current CPU
degrid/grid portion. Including linearly scaled sample-buffer creation gives a
rough first-use estimate near `5.04s`. This is useful, but not enough by itself
to justify production integration without chunked Rust/Metal staging,
memory-planner accounting, and a backend contract that avoids full-MS retention.
Dirty-only global atomics remain rejected.

### Metal Residual Backend Integration Screen

Artifact:

```text
target/imperformance-wave2/metal-residual-full-20260524/medium-briggs-niter500-cycle50-metal-residual-4w.log
target/imperformance-wave2/metal-residual-full-20260524/product-cpu.log
target/imperformance-wave2/metal-residual-full-20260524/product-metal.log
target/imperformance-wave2/metal-residual-full-20260524/product-compare-{image,residual,model,psf,sumwt}.log
```

The first Rust-side production-path screen adds an explicit macOS-only residual
selector, `CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal`. CPU fixed-tile remains
the default. The Metal path uses chunked host staging of exact per-sample tap
axis weights and dispatches a global-atomic residual-refresh kernel that degrids
from the immutable model grid and grids the weighted residual. It does not make
Metal the default backend and does not replace dirty/PSF fixed-tile CPU
gridding.

Full heavy workload:

```text
medium 64-channel, imsize=1024, Briggs robust 0.5, multiscale, niter=500,
minor-cycle-length=50, fixed-tile dirty/PSF, 4 workers, center-quadrant tiles
```

| backend | frontend | core | residual degrid/grid | major refresh | refreshes | mean residual refresh | peak RSS/footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPU fixed-tile baseline | 97.811s | 85.169s | 70.815s | 67.489s | 10 | 6.696s | 9.55 GB |
| Metal residual selector | 185.606s | 172.500s | 158.001s | 154.648s | 10 | 15.412s | 9.55 GB |

Metal per-refresh attribution was stable across the ten refreshes:

| metric | value |
| --- | ---: |
| chunks per refresh | 50 |
| chunk sample cap | 4,000,000 |
| samples per refresh | 197,519,040 |
| mean CPU staging plus dispatch wall | 15.393s |
| mean Metal command wait | 5.824s |
| mean readback | <0.001s |

The integrated Metal path is therefore about `2.30x` slower for residual
refresh than the retained CPU fixed-tile backend. The reason is not device
readback; it is the exact host-side staging contract. The Rust backend currently
replays the MeasurementSet stream, recomputes lane weights/taps, copies exact
tap-axis weights into a 88-byte sample record, and submits 50 command buffers
per refresh. That CPU-side staging cost is larger than the CPU fixed-tile
worker gridding it replaces.

A bounded product check used `niter=50` with the same dataset and tile/backend
settings. CPU wall was `32.26s`; Metal-residual wall was `35.53s`. Product
agreement versus the CPU fixed-tile output:

| product | max abs diff | RMS diff | status |
| --- | ---: | ---: | --- |
| image | 2.1822e-2 | 9.36e-4 | within CASA-compatible screen tolerance |
| residual | 2.1822e-2 | 9.36e-4 | within CASA-compatible screen tolerance |
| model | 0 | 0 | exact |
| PSF | 0 | 0 | exact |
| sumwt | 0 | 0 | exact |

Decision: do not make Metal residual refresh the default on macOS from this
implementation. The explicit selector is useful as an evidence path, but the
next Metal design must avoid the host-expanded exact tap-weight sample stream.
Promising follow-ups are device-side tap lookup from compact tap keys, larger
device-resident work units, and grouped tile/cell reduction that shares the
bounded CPU work-unit contract without copying a full expanded tap record per
sample.

### Metal Residual Compact Staging Screen

Artifact:

```text
target/imperformance-wave2/metal-staging-profile-20260525/metal-residual-niter50-compact.log
target/imperformance-wave2/metal-staging-profile-20260525/metal-residual-niter50-gpu-taps.log
```

This screen keeps the explicit `CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal`
selector and CPU default unchanged, but replaces the 88-byte host-expanded
residual sample record with a 32-byte compact record:

- sample center
- tap weight indices
- final grid weight
- finite complex visibility

The Metal residual shader now reads a shared normalized tap-weight table and
applies x/y tap weights on device. The Rust path also reuses the sample and
parameter Metal buffers across chunks instead of allocating a fresh
`newBufferWithBytes` buffer for every chunk.

A follow-up moved positive-tap span planning into the Metal shader. The CPU now
stages finite padded-grid coordinates and no longer calls `plan_positive_taps`
for each Metal residual sample; the shader computes the rounded center,
oversampling offset, support bounds, and tap-weight indices before degridding
and gridding the residual.

Workload:

```text
medium 64-channel, imsize=1024, Briggs robust 0.5, multiscale, niter=50,
minor-cycle-length=50, fixed-tile dirty/PSF, 4 workers, center-quadrant tiles
```

Comparison against the immediately preceding 88-byte staging screen:

| Metal residual staging | frontend | core | residual degrid/grid | major refresh | sample buffer | append total | command wait | staged bytes | peak RSS/footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 88-byte expanded taps | 37.295s | 24.357s | 18.951s | 15.347s | 0.935s | 4.570s | 4.752s | 17.382 GB | 9.55 GB |
| 32-byte tap indices | 35.713s | 22.684s | 17.480s | 14.029s | 0.151s | 4.047s | 4.697s | 6.321 GB | 9.55 GB |
| 32-byte GPU-planned taps | 34.488s | 21.611s | 16.333s | 12.876s | 0.151s | 3.069s | 4.723s | 6.321 GB | 9.55 GB |

Decision: retain compact staging as the Metal evidence path because it removes
the obvious host-expanded tap payload and GPU tap planning cuts another
`1.15s` from the same residual refresh screen. It is still not the default
backend: the `niter=50` Metal path remains slower than the retained CPU
fixed-tile path, and the next Metal step must move more row/vector work to the
device or otherwise reduce host replay/staging before it can be considered for
automatic macOS selection.

### Metal Residual Row-Run Staging Screen

Artifact:

```text
target/imperformance-wave2/metal-row-run-profile-20260525/metal-row-run-niter50.log
```

This screen adds a second explicit residual selector,
`CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run`. It keeps CPU fixed-tile
as the default and leaves the existing scalar Metal selector available as a
baseline. The new selector packs each routed row/channel run as compact
row-run descriptors plus lane, DATA, FLAG, and WEIGHT buffers. The Metal shader
then performs per-lane flag checks, Stokes/polarization collapse, Natural,
Uniform, or Briggs final weighting, tap planning, model degrid, and residual
grid atomics on device. `BriggsBwTaper` and `WEIGHT_SPECTRUM` are explicitly
unsupported in this first row-run kernel.

Same `niter=50` workload as the compact staging screen:

| Metal residual staging | frontend | core | residual degrid/grid | major refresh | append total | command wait | staged bytes | peak RSS/footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32-byte GPU-planned taps | 34.488s | 21.611s | 16.333s | 12.876s | 3.069s | 4.723s | 6.321 GB | 9.55 GB |
| row-run descriptors | 33.081s | 20.031s | 14.630s | 10.999s | 0.962s | 4.681s | 4.568 GB | 9.55 GB |

The row-run path removes another `1.88s` from major refresh and another
`1.70s` from residual degrid/grid versus the previous compact Metal path.
Most of the movement comes from shifting scalar visibility decode, Briggs
weight lookup, tap-center calculation, and polarization collapse from host
staging into the shader. Device command wait is essentially unchanged.

Decision: retain `metal-row-run` as the current Metal evidence path, but do not
make it the default. It is a meaningful staging-contract improvement, yet it
still misses the row-run screen gate for automatic macOS selection and remains
behind the retained CPU fixed-tile path for this medium `niter=50` workload.
The next Metal attempt needs a larger structural change than host-side
row-run packing alone, for example device-resident row payloads across major
cycles or a grouped reduction path that reduces global atomic pressure.

### Metal Row-Run Kernel Bottleneck Screen

Artifacts:

```text
target/imperformance-wave2/metal-row-run-diagnostics-20260525/exact.log
target/imperformance-wave2/metal-row-run-diagnostics-20260525/degrid-only.log
target/imperformance-wave2/metal-row-run-diagnostics-20260525/grid-only.log
target/imperformance-wave2/metal-row-run-diagnostics-20260525/single-tap.log
target/imperformance-wave2/metal-row-run-diagnostics-20260525/tap-plan-only.log
```

This screen adds profiler-only row-run diagnostic kernels behind
`CASA_RS_STANDARD_MFS_METAL_ROW_RUN_DIAGNOSTIC`. The normal `exact` kernel is
unchanged. Diagnostic modes intentionally do not produce valid residual
products; they exist to isolate device command wait. All rows used the same
`niter=50`, 64-channel, 1024-pixel, Briggs workload with `4` CPU grid threads
and 4M Metal row-run lane chunks.

| row-run diagnostic mode | frontend | core | residual degrid/grid | major refresh | command wait | candidate model reads | candidate grid atomics |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact | 33.060s | 20.092s | 14.511s | 10.736s | 4.542s | 19.357B | 19.357B |
| degrid-only | 27.854s | 14.912s | 9.969s | 6.789s | 0.652s | 19.357B | 0 |
| grid-only | 31.616s | 18.856s | 13.897s | 10.712s | 4.574s | 0 | 19.357B |
| single-tap | 27.935s | 15.124s | 10.164s | 6.964s | 0.809s | 0 | 0.395B |
| tap-plan-only | 27.488s | 14.654s | 9.646s | 6.419s | 0.183s | 0 | 0 |

Decision: global atomic accumulation is the current Metal residual bottleneck.
`grid-only` is essentially the same command wait as `exact`, while
`degrid-only` drops the wait below one second even though it still performs the
candidate model-grid reads. The next Metal implementation should therefore
avoid issuing one global atomic add per tap contribution. The plausible next
production direction is a tile/cell grouped reduction path: group row-run
lanes by tile or cell region on device, reduce within a threadgroup or compact
intermediate buffer, and emit far fewer global atomics. More host payload
packing is now a secondary concern for this screen.

### Metal Row-Run Grouped Cell-Owner Screen

Artifacts:

```text
target/imperformance-wave2/metal-row-run-grouped-20260525/grouped-edge8.log
target/imperformance-wave2/metal-row-run-grouped-20260525/grouped-edge4.log
target/imperformance-wave2/metal-row-run-grouped-20260525/grouped-edge2.log
target/imperformance-wave2/metal-row-run-grouped-20260525/grouped-edge1.log
```

This screen adds the explicit selector
`CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run-grouped`. It keeps the
same row-run payload as `metal-row-run`, prepares one compact residual lane on
device, then runs a grouped cell-owner accumulation kernel. The second kernel
scans lane references for each microtile halo cell and emits one global atomic
add per nonzero cell component instead of one atomic per tap contribution.
`CASA_RS_STANDARD_MFS_METAL_GROUP_TILE_EDGE` controls the microtile edge.

Same medium `niter=50`, 64-channel, 1024-pixel, Briggs workload, using 4 CPU
grid threads and 4M Metal row-run lane chunks:

| residual backend | group edge | frontend | core | residual degrid/grid | major refresh | command wait | exact candidate atomics | grouped candidate atomics | grouped scan tests |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| row-run exact | n/a | 33.081s | 20.031s | 14.630s | 10.999s | 4.681s | 19.357B | n/a | n/a |
| grouped | 8 | 32.672s | 19.068s | 13.516s | 9.739s | 2.356s | 19.357B | 43.6M | 38.714B |
| grouped | 4 | 30.581s | 17.406s | 12.291s | 8.955s | 1.775s | 19.357B | 73.3M | 19.752B |
| grouped | 2 | 30.640s | 17.445s | 12.150s | 8.631s | 1.482s | 19.357B | 161.8M | 12.641B |
| grouped | 1 | 29.716s | 16.604s | 11.660s | 8.475s | 1.241s | 19.357B | 452.6M | 9.678B |

Decision: retain `metal-row-run-grouped` as the current Metal residual
evidence path and default its explicit microtile edge to `1`. It is not the
macOS default backend yet. The grouped path improves the row-run residual
grid/degrid stage by about `2.97s` (`20.3%`) and major refresh by about `2.52s`
(`22.9%`) on this screen, but end-to-end frontend remains dominated by the
streaming passes and CPU fixed-tile dirty/PSF work. The edge sweep also shows
the grouped kernel is still scan-bound: shrinking the microtile edge increased
grouped atomics but lowered scan tests and command wait. The next Metal work
should therefore focus on avoiding repeated per-cell full group scans, for
example by sorting/grouping lane references more directly by output cell or by
using a threadgroup-local reduction shape.

### Metal Grouped Reduction And Chunk-Size Follow-Up

Artifacts:

```text
target/imperformance-wave2/metal-row-run-grouped-reduce-20260525/grouped-reduce-edge1-fixedtile.log
target/imperformance-wave2/metal-row-run-grouped-reduce-20260525/grouped-scan-edge1-chunk4m-recheck.log
target/imperformance-wave2/metal-row-run-grouped-reduce-20260525/grouped-scan-edge1-chunk8m-recheck.log
target/imperformance-wave2/metal-row-run-grouped-reduce-20260525/grouped-scan-edge1-chunk16m.log
target/imperformance-wave2/metal-row-run-grouped-reduce-20260525/grouped-scan-edge1-chunk32m.log
```

The attempted threadgroup-local reduction shape compiled and passed the
targeted residual correctness test, but it was rejected on performance. The
simple implementation dispatched a rectangular grid over `max_lane_blocks`, so
most groups paid for invalid reduction blocks. On the same fixed-tile workload
it moved `dispatch_wait` from about `1.31s` to `37.32s` and raised
`residual_degrid_grid` to `47.965s`. This is not retained as a backend path.

After restoring the grouped cell-owner scan kernel, the same workload was swept
by row-run chunk size:

| grouped scan chunk lanes | chunks | frontend | core | residual degrid/grid | major refresh | command wait | peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4M recheck | 50 | 29.582s | 16.748s | 11.756s | 8.540s | 1.312s | 9.55 GB |
| 8M | 25 | 29.173s | 16.458s | 11.483s | 8.269s | 1.025s | 9.53 GB |
| 16M | 13 | 29.062s | 16.338s | 11.311s | 8.059s | 0.861s | 9.55 GB |
| 32M | 7 | 29.062s | 16.286s | 11.341s | 8.168s | 0.824s | 9.55 GB |

Decision: keep the grouped scan kernel and raise the explicit Metal row-run
chunk default from `4M` to `16M`. The 32M screen reduced command wait slightly
more but did not improve the residual or major-refresh timings. The rejected
threadgroup-reduction result narrows the next GPU target: if we revisit direct
output-cell reduction, the work list must be compacted to real `(group, cell,
lane-block)` tasks rather than represented as a dense rectangular dispatch.

### Metal Command-Buffer And Replay Attribution

Artifacts:

```text
target/imperformance-wave2/metal-row-run-grouped-instrumentation-20260525/grouped-default16m.log
target/imperformance-wave2/metal-row-run-grouped-instrumentation-20260525/grouped-default16m-lambda-cache.log
target/imperformance-wave2/metal-row-run-grouped-instrumentation-20260525/grouped-default16m-lambda-cache-nodeetail.log
```

The grouped Metal residual path now reports Metal command-buffer GPU and kernel
timestamps, plus per-chunk staging/encode/wait timings. On the same 64-channel
medium `niter=50` screen, command-buffer attribution shows the GPU kernel is
not the current limiting stage:

| screen | frontend | core | residual degrid/grid | major refresh | residual replay | append/grouping | input buffers | command wait | GPU active | kernel |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| grouped 16M detailed | 31.402s | 18.208s | 12.607s | 8.781s | 8.637s | 1.539s | 0.444s | 0.924s | 0.697s | 0.220s |
| grouped 16M detailed + lambda-scale cache | 30.234s | 17.342s | 11.972s | 8.377s | 8.226s | 1.470s | 0.422s | 0.851s | 0.654s | 0.190s |
| grouped 16M no detail + lambda-scale cache | 29.33s wall | 16.453s | 11.394s | 8.110s | n/a | n/a | n/a | n/a | n/a | n/a |

The routed replay detail split after the cache shows residual replay time is
dominated by CPU-side input/replay work, not Metal arithmetic:

| residual replay component | time |
| --- | ---: |
| MS data/flag/weight/geometry reads | 3.171s |
| routed consumer / Metal chunk append | 3.166s |
| tap-center routing loop | 1.166s |
| row payload construction | 0.249s |
| frequency-scale lookup/cache | 0.219s |

Decision: retain the command-buffer instrumentation and the MFS channel
lambda-scale cache because they clarify attribution and remove a repeated
per-row allocation, but do not claim a major performance win from the cache by
itself. Bigger movement will require changing the residual replay data-flow:
avoid rereading/rerouting the same visibility rows for every major-cycle
refresh, or move more of the row-run routing/staging contract onto the device.
The current grouped Metal kernel is already below one second of GPU-active time
on this screen, so kernel micro-optimization alone cannot move end-to-end wall
time by much.

### Routed Residual Replay Cache

Artifacts:

```text
target/imperformance-wave2/metal-replay-cache-20260525/no-cache-detail.log
target/imperformance-wave2/metal-replay-cache-20260525/cache-detail.log
target/imperformance-wave2/metal-replay-cache-20260525/no-cache-nodeetail.log
target/imperformance-wave2/metal-replay-cache-20260525/cache-nodeetail.log
```

This pass adds the explicit opt-in
`CASA_RS_STANDARD_MFS_ROUTED_REPLAY_CACHE=1`. The cache records row-shaped
`StandardMfsRoutedVisibilityRun` values after the first complete routed replay
and reuses them for later residual-refresh replays. It does not cache
Metal input buffers or device-resident chunks yet, so grouped append/staging
still runs for every refresh.

Same medium 64-channel, 1024-pixel, Briggs, multiscale `niter=50` workload with
4 CPU grid threads, center-quadrant fixed tiles, and
`CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run-grouped`:

| screen | frontend total | core | prepare | residual degrid/grid | major refresh | peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| no replay cache, no detail | 28.952s | 16.258s | 22.971s | 11.235s | 7.983s | n/a |
| routed replay cache, no detail | 24.564s | 11.605s | 14.791s | 6.738s | 3.650s | n/a |
| no replay cache, detail | 31.707s | 18.563s | 25.660s | 12.845s | 8.902s | 9.51 GB |
| routed replay cache, detail | 27.038s | 13.571s | 16.086s | 8.121s | 4.447s | 10.96 GB |

The detailed cache run filled `3,086,235` routed row/channel runs covering
`197,519,040` lanes. The estimated logical cache footprint is `6.28 GB`.
The residual cache hit then replayed the same lanes with no MS column reads and
no tap-center routing:

| residual replay detail | no cache | cache hit |
| --- | ---: | ---: |
| MS column reads | 3.620s | 0.000s |
| prepare/routing loop | 5.129s | 0.000s |
| consumer plus grouped append/staging | included in 3.264s routed consume | 3.950s consumer |
| total residual replay | 8.751s | 4.297s |
| grouped Metal kernel | 0.224s | 0.313s |

Decision: retain as an opt-in measured data-flow experiment, not as the default.
It is the first large movement in the Metal residual path: no-detail frontend
improves by about `4.39s` (`15.2%`), core by `4.65s` (`28.6%`), and
major refresh by `4.33s` (`54.3%`) versus the same binary without the cache.
The result confirms that repeated residual replay/routing is a high-leverage
boundary. The follow-up below caches production-shaped Metal input chunks so
the remaining `~4s` cache-hit replay/append path is not paid on later
major-cycle refreshes. Before defaulting any cache, the planner must
account for the `~6.3 GB` routed-run footprint and choose it only when the
active memory target allows it.

### Metal Grouped Input Chunk Cache

Artifacts:

```text
target/imperformance-wave2/metal-grouped-input-cache-20260525/routed-cache-only-niter500.log
target/imperformance-wave2/metal-grouped-input-cache-20260525/grouped-input-cache-niter500.log
target/imperformance-wave2/metal-grouped-input-cache-20260525/grouped-input-cache-niter150-detail.log
target/imperformance-wave2/metal-grouped-input-cache-20260525/grouped-input-cache-planned-niter150-detail.log
target/imperformance-wave2/next-bottleneck-20260525/grouped-cache-prime-initial-dirty-niter150.log
```

This pass adds a planner-gated
`CASA_RS_STANDARD_MFS_METAL_GROUPED_INPUT_CACHE` path. The cache lives in
`casa-imaging` across major-cycle residual refreshes and stores finalized
host-side `metal-row-run-grouped` chunks plus dispatch parameters. It is enabled
by default only when `CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run-grouped`
and the standard-MFS memory planner can reserve the estimated host bytes. It
does not yet cache Metal device buffers, so each refresh still creates shared
Metal buffers from the cached host chunks.

Same medium 64-channel, 1024-pixel, Briggs, multiscale heavy screen
(`niter=500`, `minor-cycle-length=50`) with 4 CPU grid threads,
center-quadrant fixed tiles, routed replay cache enabled, and
`CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run-grouped`:

| screen | frontend total | core | prepare | residual degrid/grid | major refresh | real | peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| routed replay cache only | 65.148s | 51.841s | 15.216s | 38.017s | 35.290s | 66.35s | 11.35 GB |
| grouped input cache | 50.096s | 36.135s | 15.259s | 22.250s | 19.462s | 50.34s | 13.58 GB |

The grouped-input cache improves the heavy-screen frontend by `15.052s`
(`23.1%`), core by `15.706s` (`30.3%`), residual degrid/grid by `15.766s`
(`41.5%`), and major refresh by `15.828s` (`44.9%`) versus the same binary with
only the routed replay cache.

The niter-150 detail run confirms the intended cache behavior. The first
residual refresh fills `13` grouped chunks covering `3,086,235` row runs and
`197,519,040` lanes; later refreshes hit the cache with `replay_ms=0.000` and
`append_total_ms=0.000`. The host cache footprint reported by the input chunks is
`8.67 GB`; total process RSS peaked at `14.31 GB` on that detail run.

The planned-cache rerun removed build-only `lane_group_ids` and `group_counts`
from cached chunks. Those vectors are needed while finalizing tile-cell groups
but not after `group_descs` and `lane_refs` have been built. This lowered actual
host cache bytes from `8,674,197,960` (`8.67 GB`, `8.08 GiB`) to
`7,802,871,800` (`7.80 GB`, `7.27 GiB`), saving `871,326,160` bytes
(`0.87 GB`, `0.81 GiB`). The planner reserved `7,900,761,600` bytes for the
same medium 64-channel workload, slightly conservative versus actual. The same
run confirmed routine planner selection with
`metal_grouped_input_cache_enabled=true`, `routed_replay_cache_enabled=false`,
and no explicit grouped-cache env override.

Planned-cache niter-150 detail:

| screen | frontend total | core | prepare | residual degrid/grid | major refresh | real | max RSS | peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| planner-gated grouped input cache | 37.913s | 25.113s | 26.392s | 17.474s | 13.695s | 39.16s | 11.07 GB | 13.29 GB |
| cache primed during initial dirty pass | 32.945s | 19.686s | 19.395s | 10.713s | 5.642s | 34.21s | 11.19 GB | 13.01 GB |

The next-bottleneck profile showed the remaining first-refresh cost was not the
Metal kernel: the first grouped residual refresh still reread and rerouted the
MeasurementSet to fill the grouped input cache. The retained follow-up primes
the grouped input chunks while the initial dirty/PSF routed visibility stream is
already passing through the fixed-tile scheduler. The first residual refresh now
reports `input_cache_hit=true`, `replay_ms=0.000`, and `append_total_ms=0.000`
instead of a `~10s` replay/fill. On the same niter-150 screen this improved
frontend wall time by `4.968s` (`13.1%`), core by `5.427s` (`21.6%`), residual
degrid/grid by `6.761s` (`38.7%`), and major refresh by `8.053s` (`58.8%`)
versus the planner-gated grouped input cache row. The initial dirty/PSF pass now
absorbs the cache-build work, so `psf_grid` rises from `3.944s` to `5.218s`;
the net run remains materially faster because the first residual refresh no
longer performs another full routed replay.

Decision: retain as the routine heavy Metal residual-refresh data-flow cache
when selected by the standard-MFS memory planner. Keep the routed replay cache
as explicit opt-in only; once grouped input chunks are cached, routed replay is
redundant after the first residual refresh and costs about another `6.3 GB`.
The next staging attempt made the grouped cache chunks resident as Metal shared
input buffers. It removed per-refresh input copies, but it did not materially
move wall time (`34.19s` real versus `34.21s`) and raised macOS peak footprint
to `17.92 GB`, above the 16 GiB target on the 32 GB laptop. That shape is
therefore rejected as a default. It remains opt-in only through
`CASA_RS_STANDARD_MFS_METAL_RESIDENT_GROUPED_INPUT_BUFFERS=1` for future
dedicated-memory experiments.

The retained replacement wraps the existing host grouped-cache vectors with
Metal `newBufferWithBytesNoCopy` buffers on cached residual hits. This preserves
the planner-accounted host-cache shape while removing the copy. Same niter-150
screen:

| screen | frontend total | core | prepare | residual degrid/grid | major refresh | real | max RSS | peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cache primed during initial dirty pass | 32.945s | 19.686s | 19.395s | 10.713s | 5.642s | 34.21s | 11.19 GB | 13.01 GB |
| cached-hit no-copy Metal buffers | 28.755s | 16.094s | 18.358s | 7.478s | 2.797s | 29.94s | 9.55 GB | 9.55 GB |

The cached residual refreshes now report `dispatch_input_buffers_ms` around
`1.2ms` instead of hundreds of milliseconds to more than one second. Residual
degrid/grid improved by `3.235s` (`30.2%`) and frontend by `4.190s` (`12.7%`)
versus the cache-prime row, while peak footprint also dropped. The remaining
refresh cost is now Metal execution wait (`~0.8-1.0s` per refresh), not host
staging.

Rejected follow-up chunk/kernel screens:

| candidate | frontend total | residual degrid/grid | real | peak footprint | decision |
| --- | ---: | ---: | ---: | ---: | --- |
| resident duplicate Metal input buffers | 32.954s | 10.652s | 34.19s | 17.92 GB | reject: memory regression, no wall gain |
| 32M grouped lanes per chunk | 32.778s | 11.063s | 33.08s | 15.12 GB | reject: fewer chunks but slower residual |
| `group_tile_edge=2` | 29.150s | 7.726s | 29.25s | 9.55 GB | reject: fewer group descriptors but more scan work |
| `CASA_RS_STANDARD_MFS_TILE_INBOX_READY_SAMPLE_MIN=1` | 32.569s | 9.666s | 32.72s | 9.55 GB | reject: more drains/system time, slower dirty/PSF |

Decision: retain cached-hit no-copy Metal buffers as the routine grouped-cache
dispatch path. Keep `group_tile_edge=1` and the 16M lane chunk default.

### Metal Initial Dirty/PSF Screen

Artifacts:

```text
target/imperformance-wave2/metal-initial-dirty-20260525/current-control-after-gated-pipelines-niter150-cycleniter50.log
target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-fused-gated-niter150-cycleniter50.log
target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-gpu-run-accum-niter150-cycleniter50.log
target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-density-prefill-default-niter150-cycleniter50.log
```

This screen adds the explicit opt-in
`CASA_RS_STANDARD_MFS_INITIAL_DIRTY_BACKEND=metal-row-run-grouped`. The path
uses the same grouped row-run work units as Metal residual refresh and fuses
initial dirty and PSF gridding into one grouped Metal dispatch while filling the
grouped input cache for later residual refreshes. The default CPU fixed-tile
initial dirty/PSF path remains unchanged.

The first measurement attempt omitted `--minor-cycle-length 50` and therefore
ran 76 major cycles instead of the comparable 4-cycle niter-150 screen; that
artifact is rejected as a bad-command timing. The corrected screen uses the
same medium 64-channel, 1024-pixel, Briggs, multiscale `niter=150`,
`minor-cycle-length=50`, 4 CPU grid threads, center-quadrant fixed tiles, and
`CASA_RS_STANDARD_MFS_RESIDUAL_BACKEND=metal-row-run-grouped`.

| screen | frontend total | core | prepare | PSF grid | residual degrid/grid | major refresh | real | max RSS | peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPU initial dirty, grouped residual cache | 31.349s | 18.420s | 18.643s | 4.934s | 9.686s | 4.888s | 31.53s | 9.55 GB | 9.55 GB |
| Metal initial dirty, CPU scalar accumulation | 30.893s | 18.063s | 19.958s | 5.735s | 8.608s | 3.030s | 31.08s | 9.51 GB | 13.43 GB |
| Metal initial dirty, GPU run scalar reduction | 30.597s | 17.634s | 19.257s | 5.451s | 8.414s | 3.122s | 31.83s | 9.55 GB | 13.31 GB |
| Metal initial dirty, density-prefilled routed cache | 28.793s | 12.626s | 11.857s | 3.017s | 5.877s | 3.020s | 30.05s | 10.83 GB | 16.16 GB |

The GPU run-reduction version moves initial dirty scalar bookkeeping
(`sumwt`, gridded/skipped counts, and `max_abs_w_lambda`) out of the CPU
producer loop and into a compact per-run Metal reduction. The detailed profile
for the retained experiment reports:

```text
standard_mfs_metal_row_run_grouped_initial_dirty_detail \
  append_grouped_row_run_ms=2459.327 \
  dirty_accumulation_ms=0.000 \
  chunk_finalize_dispatch_ms=580.162
```

That removes the previous `~1.68s` CPU dirty-accumulation lane loop. The
overall corrected-screen movement versus CPU initial dirty is still modest:
frontend improves by `0.751s` (`2.4%`), core by `0.786s` (`4.3%`), residual
degrid/grid by `1.272s` (`13.1%`), and major refresh by `1.766s` (`36.1%`).
The initial dirty/PSF stage itself remains dominated by MS data reads and
host-side grouped row-run construction, so this is not yet a default-on Mac
backend.

Initial decision: retain as an explicit experimental backend seam, not the
default.
The GPU scalar reduction is architecturally useful because it removes duplicated
producer-side lane semantics and keeps the initial-dirty path aligned with the
future Metal backend contract. Default selection waits for a larger end-to-end
gain or for a follow-up that avoids the separate initial dirty data/routing
pass entirely.

Follow-up: that separate initial dirty data/routing pass is now avoided when
the memory plan can do so. With Metal grouped residual cache and Metal initial
dirty selected, the frontend enables a transient routed-run cache by default
for Uniform/Briggs weighting. The density pass reads row-shaped visibility
essentials, accumulates density, and pre-fills routed row/channel runs. The
first initial-dirty replay drains that cache while building the Metal grouped
input cache, so the planner reserves the larger of the transient routed cache
and the grouped input cache rather than treating both as long-lived concurrent
buffers.

The retained density-prefill screen reported:

```text
standard_mfs_routed_replay_cache pass=density status=prefill \
  runs=3086235 lanes=197519040 estimated_bytes=6277202000
standard_mfs_routed_replay_cache pass=initial_routed_visibility_run_replay \
  status=drain_hit consumer_ms=4899.966 total_ms=5729.604
```

Versus the CPU initial-dirty control, frontend improves by `2.556s` (`8.2%`),
core by `5.794s` (`31.5%`), PSF grid by `1.917s` (`38.9%`), and residual
degrid/grid by `3.809s` (`39.3%`). The outer wall-clock wrapper improves by
`1.48s` (`4.7%`). Peak footprint is `16159239880` bytes (`15.05 GiB`), under
the default half-system target on this 32 GiB laptop. This is retained as the
routine Metal initial-dirty data-flow shape, with
`CASA_RS_STANDARD_MFS_ROUTED_REPLAY_CACHE=0` still available as an explicit
disable switch.

Follow-up host packing cleanup: the grouped Metal row-run builder now takes a
contiguous selected-channel fast path when the routed run maps to adjacent
loaded channels and the row DATA/FLAG arrays are contiguous. The generic path
remains for arbitrary channel selections. The retained artifact is
`target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-contiguous-pack-niter150-cycleniter50.log`.
Against the density-prefill retained run, the profile moved core from
`12.626s` to `12.278s`, PSF grid from `3.017s` to `2.894s`, and residual
degrid/grid from `5.877s` to `5.679s`; frontend was effectively noise
(`28.793s` to `28.978s`). This is retained because it removes measured
host-side ndarray/index work from the Metal grouped path and preserves the
fallback representation for non-contiguous selections.

High-reward follow-up: the density-pass routed-run vector is no longer the
routine path for Metal grouped initial dirty. When the planner enables the
Metal grouped input cache, the frontend now builds a grouped Metal input-cache
prefill directly while streaming the Briggs density pass, finalizes it after
the density statistics are known, and hands the cache into the core runner. The
initial dirty stage dispatches from that cache instead of replay-draining
`StandardMfsRoutedVisibilityRun` values into Metal chunks.

The retained artifact is
`target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-direct-metal-prefill-niter150-cycleniter50.log`.
The run reported:

```text
standard_mfs_metal_grouped_input_cache_prefill pass=density status=prefill \
  runs=3086235 lanes=197519040 estimated_bytes=7805842344
standard_mfs_metal_grouped_input_cache_prefill pass=density status=finish \
  finish_ms=16.320
standard_mfs_metal_row_run_grouped_initial_dirty \
  input_cache_hit=true dispatch_wait_ms=1170.218 dispatch_gpu_ms=921.384
```

Versus the prior density-prefill retained run, frontend moved from `28.793s`
to `25.463s` (`11.6%`), core from `12.626s` to `7.497s` (`40.6%`), PSF grid
from `3.017s` to `0.593s`, residual degrid/grid from `5.877s` to `3.164s`,
and peak RSS from `10.08 GiB` to `8.85 GiB`. This is retained: it removes the
separate routed-run drain/repack boundary rather than merely optimizing it.

Follow-up allocation cleanup: the Metal grouped input-cache prefill stream now
borrows the row payload and tap-center slice directly when appending to the
cache, instead of creating short-lived `StandardMfsRoutedVisibilityRun` Arcs for
the Metal-only path. The retained artifact is
`target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-borrowed-prefill-niter150-cycleniter50.log`.
Against the direct-prefill run, frontend moved from `25.463s` to `24.757s`
(`2.8%`), prepare-plane input from `13.888s` to `12.965s`, and the density
stream total from `10.368s` to `9.387s`. Core moved from `7.497s` to `7.717s`
in this run due to Metal dispatch noise, so this is retained for the frontend
allocation/data-flow improvement rather than as a kernel-stage win.

High-reward deconvolution cleanup: the multiscale Cotton-Schwab loop now keeps
the static multiscale state across major cycles and refreshes only the
residual-dependent dirty scale images after a residual refresh. The previous
path rebuilt scale kernels, scale masks, and PSF scale convolutions even though
those are invariant for a fixed PSF and scale list. The retained artifact is
`target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-reuse-multiscale-static-niter150-cycleniter50.log`.
Against the borrowed-prefill run, frontend moved from `24.757s` to `22.724s`
(`8.2%`), core from `7.717s` to `5.599s` (`27.4%`), and
`multiscale_scale_refresh` from `2.002s` to `0.168s`. Peak RSS stayed
essentially flat at `8.85 GiB`. This is retained as a structural correctness-
preserving cleanup of invariant deconvolver state, not a Metal-specific change.

Metal grouped accumulate cleanup: the grouped Metal accumulate kernel now takes
a fast path for exact-center groups, which are the default
`group_tile_edge=1` shape. In that case every halo cell maps directly to one
tap coordinate, so the per-lane tap bounds arithmetic can be skipped while the
generic grouped path remains for larger grouping. The retained artifact is
`target/imperformance-wave2/metal-initial-dirty-20260525/grouped-initial-dirty-exact-center-accum-niter150-cycleniter50.log`.
Against the multiscale-state reuse run, frontend moved from `22.724s` to
`22.087s` (`2.8%`), core from `5.599s` to `5.239s` (`6.4%`), PSF grid from
`0.515s` to `0.468s`, and residual degrid/grid from `3.156s` to `2.866s`.
Peak RSS remained flat at about `8.88 GiB`.

MS read structural cleanup: standard-MFS imaging essentials now read independent
MAIN array columns concurrently by default. The path still uses the existing
MS/Table storage managers and persistent layout unchanged; it only changes the
read/prepare scheduling for `DATA`, `FLAG`, `WEIGHT`, optional
`WEIGHT_SPECTRUM`, and `UVW`. `CASA_RS_MS_IMAGING_READ_THREADS=1` (or
`serial`/`off`) forces the old serial read order for comparisons.

The raw read probe on the medium 64-channel selection moved column-read wall
time from `3.609s` to `1.465s`; full probe wall moved from `11.367s` to
`9.218s`. In the retained heavy workload artifact
`target/imperformance-wave2/ms-read-parallel-20260525/heavy-parallel-read.log`,
the density pass reported `get_ms_values_ms=1.698s` versus `3.708s` in the
forced-serial artifact
`target/imperformance-wave2/ms-read-parallel-20260525/heavy-serial-read.log`.
End-to-end heavy frontend time moved from the previous retained `22.087s` to
`19.668s` (`11.0%`), with `prepare_plane_input` moving from `12.729s` to
`10.394s` and `get_ms_values_ms` from `7.439s` to `5.109s`. Peak RSS stayed
within the same envelope at about `9.55 GiB`.

Rejected high-upside follow-ups:
`grouped-initial-dirty-prealloc-metal-cache-niter150-cycleniter50.log`
preallocated the large Metal cache vectors. It reduced `routed_consume` only
from `2.136s` to `1.950s`, but worsened frontend to `24.344s`, core to
`6.747s`, residual degrid/grid to `4.113s`, and peak RSS to `9.97 GiB`.
`grouped-initial-dirty-fused-density-prefill-niter150-cycleniter50.log` fused
the Briggs density and Metal prefill row walks. It improved density-pass total
to `9.125s` but did not improve the end-to-end run (`22.130s` frontend versus
`22.087s` retained), while making timing attribution less clear. Both are
rejected for now.

## Reproduction

Regenerate the Wave 2 medium manifests:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance \
  --dataset wave1-vla-single-medium \
  --materialize-workloads \
  --output-dir target/imperformance-wave2/medium-plan-current
```

Run the completed dirty target:

```sh
CASA_RS_BENCH_MS_STAGING=direct \
CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
tools/perf/imager/run_workload.py \
  --repeats 1 \
  --run-label wave2-after-direct-plan-dirty-direct \
  --storage-label external-ssd-wave2-medium-direct \
  --output-dir target/imperformance-wave2/after-direct-plan-dirty \
  target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-dirty-control.json
```

Run the completed full-shape `niter=2` clean comparison:

```sh
CASA_RS_STANDARD_MFS_GRID_THREADS=auto \
CASA_RS_BENCH_MS_STAGING=direct \
CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
tools/perf/imager/run_workload.py \
  --repeats 1 \
  --run-label wave2-positive-compact-clean-niter2 \
  --storage-label external-ssd-wave2-medium-direct \
  --output-dir target/imperformance-wave2/positive-compact-clean-niter2-casa \
  target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-clean-niter2.json
```

Set `CASA_RS_BENCH_PHASE_PROBE=1` on that command only when CASA
`PySynthesisImager` phase medians are needed. The default paired run records
Rust/CASA wall times and product comparisons without paying for a second CASA
diagnostic pass.
