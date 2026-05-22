# ImPerformance Wave 2 Streaming Tile Contract

Truth class: current design contract
Last reality check: 2026-05-21
Verification: External Oracle-style review in Chrome conversation `Bundle review request`; `cargo check -p casa-imaging -p casars-imager`; `cargo test -p casa-imaging standard_mfs_metal_backend_selection_is_explicit_and_gated --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_reserves_fixed_tile_residency_when_enabled --lib`

Wave issue: #263

This contract redirects the next Wave 2 optimization pass away from retained
visibility plans plus worker-local full grids. The first production-directed
backend is a bounded standard-MFS streaming tile backend that keeps only a
planner-approved row block, compact current-block tile buckets, a bounded set of
resident halo-padded tile buffers, and the global stage grids needed by FFT and
deconvolution.

## Backend Contract

The first backend is fixed-tile CPU standard MFS:

```text
Backend name: StandardMfsStreamingFixedTileCpu
Weighting: Natural, Uniform, Briggs, BriggsBwTaper
Stages: density pass, PSF/dirty grid, residual refresh grid
Grid precision: Complex64 accumulation, matching current standard-MFS grids
Numerical contract: CASA-compatible tolerance, not bitwise identity
Tile geometry: fixed rectangular interiors plus fixed halo
Tile scheduler: deterministic bounded residency cache
```

Implementation checkpoint:

```text
Environment flag: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
Active memory target: total physical memory / 2, with optional CASA_RS_STANDARD_MFS_MEMORY_TARGET_MB override
Resident tile budget: central standard-MFS memory plan, with optional CASA_RS_STANDARD_MFS_TILE_RESIDENT_MB override
Debug resident tile limit: CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=<count>
Tile edge override: CASA_RS_STANDARD_MFS_TILE_EDGE=<pixels>
Tile anchor override: CASA_RS_STANDARD_MFS_TILE_ANCHOR=zero|center_boundary
Debug flush override: CASA_RS_STANDARD_MFS_TILE_FLUSH=per_block
Status: memory-control repair checkpoint, not a performance claim
Coverage: standard-MFS PSF/dirty and residual refresh through fixed halo tiles,
          plus fixed-tile streaming clean row-block replay
Fallback: default standard-MFS executor and streaming paths remain unchanged
```

Out of scope for this first backend:

```text
variable rectangular tiling
on-disk visibility or tap cache
bitwise reproducibility mode
production GPU implementation or device staging
whole-cube all-plane residency
persistent per-sample PositiveTapSet plans
per-tile MeasurementSet reread scheduling
```

## Accelerator Boundary

The streaming/tile dataflow is required architecture, not just a CPU tuning
choice. LibRA and other CUDA/Kokkos gridders are useful references and possible
future Linux/NVIDIA accelerators, but they are not a practical first Wave 2
answer for Apple Silicon because the relevant GPU path is CUDA-oriented rather
than Metal-oriented.

The first CPU implementation should still shape its internal backend boundary
as if a later accelerator will consume the same bounded work units:

```text
bounded prepared row blocks
compact per-block tile buckets
explicit tile/slab residency limits
stage-local grid/density/model buffers owned by the memory plan
backend-reported host and device staging bytes
```

The first implementation reserves fixed-tile residency in
`standard_mfs_memory_plan` and passes a byte budget into the imaging core via
`StandardMfsExecutionConfig`. The backend still honors
`CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT` as an explicit debug override.
The planner must account for live prepared row blocks, current-block bucket
records, queued task metadata, resident tile buffers, global grids, density
grids, FFT/deconvolution scratch, and future device staging before execution.
When no legal bounded plan fits the active target, the frontend fails clearly
instead of falling back to the old retained full-MeasurementSet shape.

That boundary lets future backends choose CPU, CUDA/Kokkos, LibRA-derived, or
Metal implementations without reviving the whole-MeasurementSet visibility plan
or worker-local full-grid strategy.

## Metal Preview Backend Track

Apple Metal is now an intended optional backend track for modern macOS, not a
detached side experiment. The production Rust integration direction is
`objc2-metal`; the Swift harness under `tools/experiments/metal/` remains the
benchmark and shader-shape workbench while the Rust work-unit boundary is
hardened.

Metal must consume the same standard-MFS work-unit contract as the CPU
fixed-tile backend:

```text
bounded prepared row blocks
compact current-block tile buckets
integer tap centers from the standard gridder path
bounded resident tile or slab buffers
global density and stage grids only where explicitly planned
```

Metal must not introduce a full-MeasurementSet visibility plan, persistent
per-sample tap plans, or CPU-expanded full tap-contribution lists as a
production contract. Any device staging, tile-cell reference buffers, resident
device grids, and host readback buffers must be represented in the central
standard-MFS memory plan before execution.

Backend selection must be explicit and platform-gated:

```text
Default: CPU standard-MFS path
CPU fixed tile: CASA_RS_STANDARD_MFS_BACKEND=fixed_tile
Metal preview: CASA_RS_STANDARD_MFS_BACKEND=metal
```

The first Rust Metal backend is intentionally narrow:
`CASA_RS_STANDARD_MFS_BACKEND=metal` runs the initial standard-MFS dirty/PSF
tile gridding work unit on Metal on macOS, using the compact current-block tile
buckets. Residual refresh still falls back to the CPU fixed-tile executor, so
bounded fixed-tile residency remains part of the selected execution plan. The
CPU fixed-tile backend remains the correctness/reference path.

## Merge Policy

Use bounded scheduled tile residency as the first real implementation. Per-block
merge is correct, but it should be a debug fallback rather than the default
because it forces every row block to pay tile zero-fill and halo merge costs for
every touched tile.

The default lifecycle is:

```text
1. Read one bounded row block.
2. Prepare only that block.
3. Route accepted samples into per-block tile buckets.
4. Build one task per nonempty tile and sort by estimated tap visits.
5. Dispatch tasks to a bounded worker pool for the current row block.
6. Update resident halo-padded tile buffers directly when all stage tiles are resident.
7. Drop the bucket and row block.
8. Keep tile buffers resident across row blocks while budget permits.
9. On deterministic eviction, stage boundary, or memory pressure, merge the full
   halo-padded tile buffer into the global grid and clear or free it.
```

For MFS, the scheduler may choose all fixed tiles resident for a stage when the
central memory plan allows it. That all-resident MFS case is the default
memory-control path because it avoids per-task scratch tile arrays and main
thread scratch-buffer merges. When the resident limit is lower than the full
tile count, the older scratch/merge path remains a correctness fallback and a
future hot-tile-splitting building block. For cubes, the same abstraction must
become a bounded tile/slab working set.

The first scheduler uses `max_live_row_blocks=1`. Any read-ahead greater than
one must first reserve memory credits for live row blocks, bucket records,
queued task metadata, active-tile bookkeeping, and oldest-block drain behavior.
Workers must skip or postpone active-tile tasks rather than blocking behind
them; queue depth two and hot-tile splitting remain data-gated follow-ups.

Fixed-tile scheduler profiles must report whether the configured workers are
actually being used. The summary and optional block-detail records include
per-worker task, sample, tap-visit, active-time, elapsed-time, capacity,
utilization percentage, tail-idle, and useful-throughput fields such as
tap-visits per second and samples per second. The summary distinguishes
grid-loop utilization from full-stage utilization: `worker_utilization_pct`
uses only the block wall time around dispatched tile tasks, while
`stage_worker_utilization_pct` uses the whole stage wall time and the configured
worker count. Full-stage utilization is the metric that must reconcile with
end-to-end speedup. The stage summary also records direct line attribution for
the non-worker path: `replay_gap_total_ms`, `batch_preprocess_total_ms`,
`bucket_build_total_ms`, `block_wall_total_ms`, `tile_flush_ms`, and
`stage_unaccounted_ms`. Those counters are required evidence before accepting
or rejecting multi-worker scheduler changes. For diagnostic apples-to-apples
scaling tests,
`CASA_RS_STANDARD_MFS_FORCE_TILED_ONE_WORKER=1` forces the tiled scheduler even
when `CASA_RS_STANDARD_MFS_GRID_THREADS=1`; the default one-worker path may
still use the global-grid bypass when memory permits.

## Exactness Policy

The tiled backend must be deterministic, but it does not need to be bitwise
identical to the serial full-grid path. The required contract is algorithmic
equivalence within the existing CASA-compatible product tolerances.

Required deterministic choices:

```text
stable tile IDs
half-open ownership intervals [x0, x1), [y0, y1), except final bounds
stable MeasurementSet row-block order
stable bucket fill order inside each row block
stable tile eviction order
stable tile-to-global merge order
```

## Tile Ownership

Tile ownership is defined over integer global tap-center coordinates from the
same gridder planning path used for gridding. It is not defined over floating UV
ranges.

Every accepted sample has exactly one owner tile. The owner tile writes the full
convolution footprint into its halo-padded local buffer. Flushes must add every
valid cell in the halo-padded extent into the global stage grid, clipped to the
global grid bounds. Merging only tile interiors is incorrect because a sample
owned by tile A may write taps into cells that lie inside tile B's interior.

The center-boundary anchor is a load-balance heuristic, not a correctness
requirement. When enabled, the tile origin is computed from the standard
gridder's actual integer zero/tap-center coordinate modulo the tile edge. If
that modulo is zero, the partition does not create an empty leading tile. The
diagnostic path records the gridder center, tile origin, near-origin integer
tap-center counts, UV-quadrant owner distribution, and per-row-block tile
distribution so the heuristic is verified against the discrete gridder
convention rather than inferred from physical UV arguments alone.

The memory planner owns the default tile-size choice. Explicit
`CASA_RS_STANDARD_MFS_TILE_EDGE` still wins, but the planner may prefer `64`
when image size or memory budget makes all-resident `32` tiles too expensive.
For smaller standard-MFS fixed-tile runs, `32` is the first serious scheduling
candidate because it splits the central UV concentration into many more ready
tasks than the previous coarse `256` decomposition.

## Bucket Contract

A tile bucket is a bounded current-row-block routing structure, not a visibility
or tap cache. It may contain:

```text
sample_index into the current row block
exact integer center_x and center_y
optional compact kernel key
support ID, initially 0
flags such as FINITE_VIS and PSF_ONLY
final grid_weight_f32 for the stage
```

It must not contain:

```text
PositiveTapSet
tap coordinate arrays
tap weight arrays
full weighted visibilities
anything that survives beyond the current row block
```

If the gridder cannot expose a compact tap key in the first patch, workers may
recompute a temporary `PositiveTapSet` from the row-block `u_lambda` and
`v_lambda`, debug-check that its center matches the bucket center, grid
immediately, and drop the temporary plan.

## Weighting

Natural weighting can stream directly because the final grid weight is known
while processing the row block.

Uniform and Briggs require two logical sample visits for exact behavior:

```text
pass 1: stream metadata, flags, UVW, and weights to build the global MFS density
        grid, robust statistics, and tile-load counters; avoid DATA reads where
        possible
pass 2: stream again, compute final weights, bucket samples, and grid
```

The first MFS backend may keep the density grid global. Cube density must later
be plane/slab bounded and represented in the same central memory plan.

## Memory Planner

The standard-MFS memory planner must reject over-budget execution. It must model
at least:

```text
row block bytes
producer/consumer queue depth
tile bucket bytes
active resident tile buffers
halo overhead
global stage grids
density grids
FFT/deconvolver scratch
output products
thread stacks, allocator overhead, fragmentation, and OS/page-cache headroom
```

No path may silently exceed the configured budget by retaining per-sample tap
plans, weighted visibility plans, or unbudgeted tile queues.

## First Implementation Sequence

1. Add fixed tile geometry and ownership lookup.
2. Add compact tap-key support if practical; otherwise use temporary per-sample
   plans inside the worker only.
3. Add the current-block tile bucket builder.
4. Add deterministic resident tile cache accounting.
5. Implement Natural PSF/dirty tiled path.
6. Implement Uniform/Briggs streaming density pass plus tiled pass 2.
7. Implement residual refresh with immutable model-grid reads and resident
   residual tile writes.
8. Add per-block merge as debug mode only.
9. Add correctness fixtures and tolerance thresholds before variable tiles.
