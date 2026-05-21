# ImPerformance Wave 2 Streaming Tile Contract

Truth class: current design contract
Last reality check: 2026-05-21
Verification: External Oracle-style review in Chrome conversation `Bundle review request`; `cargo check -p casa-imaging -p casars-imager`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_BACKEND=fixed_tile CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=1 cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_reserves_fixed_tile_residency_when_enabled --lib`

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
Resident tile budget: central standard-MFS memory plan, with optional CASA_RS_STANDARD_MFS_TILE_RESIDENT_MB override
Debug resident tile limit: CASA_RS_STANDARD_MFS_TILE_RESIDENT_LIMIT=<count>
Status: core backend correctness checkpoint, not a performance claim
Coverage: standard-MFS PSF/dirty and residual refresh through fixed halo tiles
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

That boundary lets future backends choose CPU, CUDA/Kokkos, LibRA-derived, or
Metal implementations without reviving the whole-MeasurementSet visibility plan
or worker-local full-grid strategy.

## Metal Preview Backend Track

Apple Metal is now an intended optional backend track for modern macOS, not a
detached side experiment. The production Rust integration direction is
`objc2-metal`; the Swift harness under `tools/experiments/metal/` remains the
runnable benchmark and shader-shape workbench while the Rust work-unit boundary
is hardened.

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

Until a production Metal executor is wired into the imaging core,
`CASA_RS_STANDARD_MFS_BACKEND=metal` is expected to fail before imaging
execution with a clear preview-backend message. The CPU fixed-tile backend
remains the correctness/reference path.

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
4. Acquire each touched tile from a memory-bounded resident tile cache.
5. Grid the bucket into the resident halo-padded tile buffer.
6. Drop the bucket and row block.
7. Keep tile buffers resident across row blocks while budget permits.
8. On deterministic eviction, stage boundary, or memory pressure, merge the full
   halo-padded tile buffer into the global grid and clear or free it.
```

For MFS, the scheduler may choose all fixed tiles resident for a stage when the
central memory plan allows it. For cubes, the same abstraction must become a
bounded tile/slab working set.

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
