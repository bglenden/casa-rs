# ImPerformance Wave 2 Acceleration Ledger

Truth class: current descriptive
Last reality check: 2026-05-22
Verification: `bash -n scripts/bench-imager-vs-casa.sh`; `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/stage_wave1_datasets.py tools/perf/imager/test_run_workload.py tools/perf/imager/test_stage_wave1_datasets.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py tools/perf/imager/test_run_workload.py`; `cargo test -p casa-imaging paired_f64_product_grid_matches_separate_updates --lib`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks --lib`; `cargo test -p casa-imaging weighting --lib`; `cargo test -p casa-imaging streaming_density_samples_match_batch_density_weighting --lib`; `cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `cargo test -p casa-imaging positive_tap_span_reconstructs_legacy_positive_taps --lib`; `cargo test -p casa-imaging compact_positive_tap_grid_and_degrid_match_product_taps --lib`; `cargo test -p casa-imaging fused_residual_refresh_matches_separate_degrid_grid --lib`; `cargo test -p casa-imaging standard_mfs_plan_buckets_gridder_accepted_samples --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casa-imaging degrid --lib`; `cargo test -p casa-imaging standard_mfs_thread_count_parser_accepts_numeric_and_auto_values --lib`; `cargo test -p casa-imaging standard_mfs_metal_backend_selection_is_explicit_and_gated --lib`; `cargo test -p casa-tables tiled_selected_row_reads_reuse_shared_tile_cache --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager --example profile_imager`; `cargo build --release -p casars-imager --example profile_imager`; `just quick`; `just docs-check`; `git diff --check`; selected `tools/perf/imager/run_workload.py` and `profile_imager` runs listed below, including the positive compact tap paired profile, bounded serial attribution probes, final full-shape one-worker profiles on 2026-05-20, and bounded fixed-tile single-worker density-direct profiles on 2026-05-22

Wave issue: #263
Child issues: #264, #265, #266, #267

This note records the first ImPerformance Wave 2 acceleration pass on the
correctness-green full-medium VLA standard-MFS target from Wave 1.

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
cargo test -p casa-imaging direct_resident_tiles_match_scratch_tile_dirty_and_residual_paths --lib
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
cargo test -p casa-imaging direct_resident_tiles_match_scratch_tile_dirty_and_residual_paths --lib
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
