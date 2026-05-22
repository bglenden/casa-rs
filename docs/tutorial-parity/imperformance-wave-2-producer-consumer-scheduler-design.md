# ImPerformance Wave 2 Producer Consumer Tile Scheduler Design

Truth class: current design contract
Last reality check: 2026-05-22
Verification: Oracle-style Chrome review, two-pass design iteration; `just docs-check`; `git diff --check`

Wave issue: #263

This document is the implementation design for replacing the current
standard-MFS fixed-tile multi-worker scheduler with a bounded producer/consumer
pipeline. It does not replace the streaming tile contract; it refines the
scheduler and row-block ownership model needed to make that contract perform.

The immediate problem is now measured. In the 10-worker bounded fixed-tile run,
the workers are not kept busy over the full stage:

```text
Artifact: target/imperformance-wave2/multi-worker-throughput-20260522/bounded-10w-fixed-tile-line-attribution.log
Dirty stage:
  stage total: 14.281 s
  replay/input gap: 9.300 s
  bucket build: 3.312 s
  worker block wall: 1.571 s
  blocks: 3014
  tasks: 494505
Residual stage:
  stage total: 11.827 s
  replay/input gap: 8.337 s
  bucket build: 1.633 s
  worker block wall: 1.782 s
  blocks: 3014
  tasks: 494505
```

The current scheduler is therefore not the user's desired model of an I/O and
prepare producer continuously filling bounded tile queues while persistent
workers drain the largest useful work. It is a synchronous row replay callback
that feeds one `VisibilityBatch` at a time into a scoped worker block. It has no
persistent workers, no cross-row-block tile queues, and no overlap between
producer work and gridding work.

## Goals

The next scheduler must:

- keep visibility state bounded by planner-approved live row blocks;
- avoid full-MeasurementSet prepared visibility retention;
- avoid persistent per-sample tap plans;
- avoid full-grid-per-worker allocation;
- update resident halo tile buffers directly;
- keep the CPU fixed-tile backend as the correctness path;
- preserve CASA-compatible numerical tolerance;
- make all important memory and scheduler waits measurable.

The first production target is standard MFS on CPU. Natural, Uniform, Briggs,
and BriggsBwTaper are in scope. Dirty/PSF gridding and residual refresh are in
scope. Cube/slab execution, production GPU kernels, hot-tile splitting, and flat
row-block storage are later measured follow-ups.

## Stage Shape

Use one standard-MFS tile pipeline executor for grid-producing stages, with the
density pass explicit and separate:

```rust
enum StageKind {
    DirtyPsf,
    PsfOnly,
    ResidualRefresh,
}

enum WeightingPlan {
    Natural,
    PrecomputedDensity {
        density_grid: Arc<DensityGrid>,
        robust_stats: RobustStats,
        weighting_mode: DensityWeightingMode,
    },
}
```

Uniform, Briggs, and BriggsBwTaper run a bounded density and robust-statistics
pass first. Pass 2 receives an immutable `PrecomputedDensity` plan. Pass 2 must
not mutate density or robust statistics.

The stage executor owns execution state, not the MeasurementSet:

```rust
struct StandardMfsTilePipelineExecutor {
    stage_kind: StageKind,
    weighting_plan: WeightingPlan,
    resident_tiles: ResidentTileSet,
    global_outputs: GlobalMfsOutputs,
    scheduler: SchedulerState,
    memory_budget: MemoryBudget,
    scalar_reducer: ScalarReducer,
    worker_pool: PersistentWorkerPool,
}
```

The frontend remains the bounded stream source. The executor consumes prepared
row-block work units and publishes tile tasks to workers.

## Row-Block Ownership

The key design change is that the scheduler operates on an owned immutable
row-block unit. Current borrowed `&[VisibilityBatch]` callback lifetimes are
acceptable for the synchronous path only; they cannot escape into persistent
workers.

The pipeline introduces:

```rust
struct PreparedTileRowBlock {
    block_id: u64,
    storage: RowBlockStorage,
    sample_refs: Box<[SampleRef]>,
    bucket_samples: Box<[BucketSample]>,
    tile_ranges: Box<[TileRange]>,
    tile_tasks: Box<[TileTaskDesc]>,
    scalar_seed: RowBlockScalarSeed,
    memory_lease: Option<PublishedRowBlockLease>,
}

enum RowBlockStorage {
    BatchBacked {
        batches: Box<[VisibilityBatch]>,
    },
    Flat {
        visibility: Box<[ComplexValue]>,
        weights: Box<[WeightValue]>,
        sumwt_factors: Box<[SumwtFactor]>,
        sample_meta: Box<[FlatSampleMeta]>,
    },
}

struct SampleRef {
    batch_index: u32,
    sample_index: u32,
}

struct BucketSample {
    sample_id: u32,
    center_x: u32,
    center_y: u32,
    kernel_u: u16,
    kernel_v: u16,
    support_id: u16,
    flags: SampleContributionFlags,
    grid_weight: f32,
    tap_count: u16,
}
```

The first implementation uses `RowBlockStorage::BatchBacked`, not `Flat`.
Workers never index batches directly; they use sample accessors keyed by
`sample_id`. This lets Phase A keep the existing batch layout without deep
copying payloads, while Phase D can add `Flat` storage behind the same accessors
if profiles show batch-backed access is limiting.

Do not clone all `VisibilityBatch` payloads inside the executor just to create
an owned unit. Phase 0 must either move the prepared row-block payload into
`PreparedTileRowBlock`, or explicitly mark Phase A as borrowed and synchronous.
Phases B and C require owned row blocks, normally through
`Arc<PreparedTileRowBlock>`.

## Row-Block Invariants

Every published `PreparedTileRowBlock` must satisfy:

- `block_id` is unique and monotonically increasing within the stage;
- storage is immutable after publication;
- `sample_id` indexes exactly one logical sample through the row-block accessors;
- bucket samples are grouped by tile ID;
- each accepted sample appears in exactly one owner-tile bucket;
- tile task ranges exactly partition `bucket_samples`;
- `task_count == tile_tasks.len()`;
- completion count is initialized before any task is visible;
- the row-block memory lease covers owned allocation capacities, not just
  lengths;
- scalar records do not retain `Arc<PreparedTileRowBlock>`.

For tile correctness:

- sample ownership is by integer tap center owner tile;
- the owner tile applies all valid taps, including halo taps;
- samples are not duplicated into neighboring owner tiles;
- resident tiles flush into global grids in deterministic tile-ID order.

Debug assertions should include:

```text
accepted_samples == sum(bucket_samples over all tiles)
task ranges are nonoverlapping
task ranges cover all bucket samples
zero-task row blocks are not published
```

## Memory Leases

Use aggregate leases by lifetime, not one lease per vector:

```text
StageLease:
  resident tile buffers
  global output grids
  density grid
  model/residual scratch
  scheduler queue capacity
  completion-log capacity
  future device staging reserve

BuildLease:
  producer scratch reserve
  unpublished row-block construction buffers
  temporary bucket count/fill arrays
  frontend prepare buffers whose lifetime spans construction

PublishedRowBlockLease:
  row-block storage
  sample_refs
  bucket_samples
  tile ranges and descriptors
  row-block-local compact metadata
```

`PreparedTileRowBlock` owns the `PublishedRowBlockLease`. Credits are released
only when the row block is actually dropped. Completion records must not keep
row blocks alive until stage end.

The producer must acquire a conservative build reservation before materializing
a row block. After bucket construction, it adjusts the build reservation to the
actual published row-block bytes and transfers those bytes into a
`PublishedRowBlockLease`.

The byte ledger uses capacities:

```rust
struct RowBlockByteLedger {
    storage_bytes: usize,
    sample_ref_bytes: usize,
    bucket_sample_bytes: usize,
    tile_task_bytes: usize,
    tile_range_bytes: usize,
    scalar_record_bytes: usize,
    allocator_slop_bytes: usize,
}
```

Queue nodes should either be charged under `StageLease` or store compact handles
into row-block-owned task arrays.

## Sample Classification

Density, PSF, dirty, and residual paths must share classification and coordinate
quantization. This is mandatory for Uniform, Briggs, and BriggsBwTaper because
the density pass and weighted gridding pass must agree on the logical sample
population.

The shared classifier should produce explicit masks:

```text
valid_for_density
valid_for_psf
valid_for_dirty_visibility
valid_for_residual_visibility
valid_geometry
valid_weight
finite_visibility
```

Nonfinite visibility invalidates dirty and residual visibility contribution,
but it must not automatically remove PSF or density contribution when geometry
and weights are otherwise valid.

## Scheduler Queue

The first persistent scheduler uses per-tile FIFO queues and a global ready-tile
heap. The strict invariant is:

```text
The ready heap contains at most one valid entry for each inactive nonempty tile
queue.
```

A tile is exactly one of:

```text
empty
queued and inactive, with one valid heap head
active, with no valid heap head
```

Use one scheduler mutex and condition variable first. The critical section is
small compared with grid/degrid work, and correctness is more important than a
premature lock-free scheduler.

```rust
struct SchedulerState {
    tile_queues: Vec<VecDeque<TileTask>>,
    ready_heap: BinaryHeap<ReadyTileHead>,
    tile_active: FixedBitSet,
    tile_generation: Vec<u64>,
    producer_closed: bool,
}

struct ReadyTileHead {
    tile_id: TileId,
    generation: u64,
    block_id: u64,
    estimated_work: u64,
}
```

Pop protocol:

```text
1. Discard stale generation entries.
2. Assert tile is inactive.
3. Mark tile active.
4. Pop only the head task from that tile queue.
5. Release scheduler lock.
6. Worker updates the resident tile.
7. Reacquire scheduler lock.
8. Mark tile inactive.
9. If the tile queue still has a head, push a new generation entry.
10. Notify workers.
```

`active_tile_skip_events` should be zero or near zero. If active tile skips are
common, treat that as a scheduler bug or stale-entry leak, not normal behavior.

Priority is:

```text
oldest runnable block first
largest estimated work second
tile ID as deterministic tie-breaker
```

This is a priority policy, not a global barrier. Do not prevent all tasks from
block N+1 while any task from block N remains. The per-tile FIFO already
preserves same-tile row-block order. The scheduler should bias toward draining
old row blocks for memory pressure, but it must not starve legal work.

## Determinism

The new scheduler has its own deterministic reduction order:

```text
within a tile:
  block_id ascending
  stable row-block sample order
  stable tap order

global flush:
  tile_id ascending
```

It does not need to match the old full-grid or batch-at-a-time order bitwise.
The documented gate remains CASA-compatible tolerance. If the CPU scheduler is
bitwise stable across worker counts, that is useful, but it should not become a
contract that blocks future GPU work.

## Residual Refresh

Residual refresh uses the same row-block and tile-task machinery as dirty/PSF.
Only the worker kernel changes:

```text
producer:
  stream row block
  apply the frozen weighting plan
  classify samples
  build residual tile buckets
  publish tile tasks

worker:
  degrid from immutable model grid
  compute residual visibility
  grid residual into resident residual tile buffer
  emit task scalar record
```

The model grid must be immutable for the duration of residual refresh. The
major-cycle model update and residual-refresh workers must be separated by a
stage barrier unless a later copy-on-write or epoch-guarded model grid is
introduced.

## Scalar Reductions

Do not use contended per-sample atomics for scalar state. Workers emit
task-local records:

```rust
struct TaskScalarRecord {
    block_id: u64,
    tile_id: TileId,
    task_seq: u32,
    normalization_sumwt: f64,
    reported_sumwt: f64,
    gridded_samples: u64,
    skipped_nonfinite_visibility: u64,
    skipped_flagged: u64,
    skipped_invalid_weight: u64,
    skipped_out_of_grid: u64,
    max_abs_w_lambda: f64,
}
```

The reducer folds records in deterministic `(block_id, tile_id, task_seq)`
order. Completion records carry scalar state only; they must not retain row
block storage.

## Implementation Phases

### Phase 0: Data and Lifetime Skeleton

No concurrency change.

Deliverables:

- `PreparedTileRowBlock`;
- `RowBlockStorage::BatchBacked`;
- `SampleRef` table;
- `BucketSample.sample_id`;
- row-block sample accessors;
- aggregate memory lease types;
- row-block byte ledger using capacities;
- shared sample classifier;
- deterministic scalar record and reducer shape;
- zero-task row-block handling.

Tests:

- sample ID resolves correct batch/sample;
- bucket partition covers accepted samples exactly once;
- zero-task row block does not publish;
- nonfinite visibility contributes to PSF/density when appropriate but not
  dirty/residual visibility;
- density and gridding classification agree for Uniform/Briggs/BwTaper fixtures.

### Phase A: Row-Block Coalescing, No Read-Ahead

Keep the current scoped worker model. Change the fixed-tile core so one prepared
frontend row block becomes one scheduler block, instead of breaking it into one
block per `VisibilityBatch`.

Phase A is not throwaway. It creates and verifies the row-block data model,
sample ID abstraction, row-block bucket builder, and dirty/residual task
formation before persistent workers complicate lifetimes.

Phase A does not claim to solve producer/worker overlap. It proves row-block
task granularity and indexing correctness.

Required measurements:

```text
frontend row blocks
VisibilityBatch values per row block
scheduler blocks
tasks per row block
accepted samples per row block
bucket bytes per row block
bucket count/fill time
worker wall per row block
worker idle between row blocks
old replay/input gap attribution
```

Acceptance:

- scheduler block count falls from batch count toward frontend row-block count;
- outputs remain within tolerance;
- memory high-water does not grow beyond the plan;
- remaining replay/input gap is attributed to block overhead or real
  producer/consumer non-overlap.

### Phase B: Persistent Workers, One Live Row Block

Add persistent worker pool, per-tile FIFO queues, ready-tile heap, generation
counters, and condition variable. Keep `max_live_row_blocks = 1`.

The producer publishes one row block, workers drain it, and only then does the
producer move to the next row block. This isolates worker-pool and queue
correctness from asynchronous read-ahead.

Acceptance:

- correctness holds for 1, 2, and 10 workers;
- same-tile ordering is stable;
- scheduler lock and tile lock wait are not material;
- active tile skips are zero or near zero;
- stage flush occurs only after producer closed, all queues empty, all active
  tiles clear, all row-block completion counts reached zero, and all scalar
  records are published.

Tile lock wait above a tiny threshold is a bug detector in this phase. With the
scheduler invariant, normal workers should not fight over the same resident
tile.

### Phase C: Bounded Read-Ahead and Overlap

Add `max_live_row_blocks = 2`.

The producer may build row block N+1 while workers drain row block N. The
producer must acquire build credits before construction and published credits
before publication. It blocks on memory if those credits are not available.

Stage-end condition:

```text
producer_closed
all row-block completion counts reached zero
all tile queues empty
all active tiles clear
all scalar records published
then flush resident tiles by tile_id
```

Acceptance:

- the old replay/input gap is mostly converted into producer/worker overlap;
- worker waiting-for-work falls sharply;
- memory high-water remains under the planner target;
- stage wall improves materially versus Phase B;
- remaining bottleneck is identified as producer work, memory blocking,
  scheduler lock, or hot-tile tail.

Do not tune queue depth beyond two until this phase produces clean attribution.

### Phase D: Measured Follow-Ups Only

Do not implement these until Phase C data justifies them:

- producer-side parallel bucketizer;
- row-block size tuning;
- tile-edge tuning;
- deterministic hot-tile splitting;
- `RowBlockStorage::Flat`;
- GPU or Metal staging;
- deeper queue depth.

If producer bucketization is limiting, split it on the producer side using
deterministic count, prefix-sum, and fill phases. Do not make gridding workers
discover tile buckets from raw row-block ranges first.

If hot-tile tail dominates, split hot tiles into private scratch tile buffers
and deterministically reduce them. Do not add hot splitting preemptively.

## Mandatory Metrics

A performance claim for this design requires these metrics.

Worker starvation:

```text
worker_waiting_for_work_time / (worker_count * stage_wall_time)
```

Initial target: below 10 to 15 percent for dirty/residual gridding portions,
excluding startup and shutdown.

Producer/worker overlap:

```text
producer_active_time
worker_active_union_time
producer_worker_overlap_time
producer_only_time
worker_only_time
neither_active_time
```

Phase C should show `producer_worker_overlap_time >= 70%` of producer active
time, or show that unoverlapped producer-only time is less than 25% of the old
replay/input gap.

Memory accounting:

```text
configured memory target
accounted stage bytes
accounted build bytes
accounted published row-block bytes
accounted scheduler/completion bytes
high-water accounted bytes
RSS high-water
max live row blocks
max live row-block bytes
producer blocked on memory
```

Gate: accounted high-water is at or below the planner target, and RSS high-water
is explainable by tracked bytes plus allocator overhead.

Correctness:

```text
dirty, PSF, and residual outputs for 1, 2, and 10 workers
same weighting mode
same tile edge
same row-block size
same input
scalar counters and density/Briggs statistics
```

Gate: CASA-compatible tolerance, with scalar counters matching expected
semantics.

Scheduler overhead:

```text
scheduler_lock_wait
tile_lock_wait
ready_heap_pop/push count
stale_heap_entry count
active_tile_skip count
tasks completed
tap visits completed
```

Initial target: scheduler lock plus tile lock wait below 2 percent of stage wall
time, and active tile skips zero or near zero.

Stage overlap identity:

```text
stage wall ~= overlapped time
           + producer-only time
           + worker-only time
           + neither-active time
           + flush time
```

`neither-active` plus unaccounted time must be small before accepting a
multi-worker improvement.

## Stop Conditions

Stop and inspect before optimizing if any of these occur:

- row-block memory credits are released before the row block actually drops;
- borrowed batch references escape into persistent workers;
- density and gridding classify different sample populations;
- active-tile skip events are common;
- tile lock wait is material in Phase B;
- producer is blocked on memory while workers are idle;
- Phase C increases memory high-water beyond the target;
- Phase C shifts replay/input gap into unaccounted time instead of overlap.

## Oracle Review Decisions

The Oracle review approved the producer-built per-tile FIFO and ready-heap
direction, but tightened the plan in five places:

- add Phase 0 for row-block ownership, sample IDs, memory leases, classification,
  and scalar reducers before concurrency changes;
- keep Phase A before persistent workers because it isolates row-block indexing
  and task formation;
- use aggregate leases by lifetime: stage, build/scratch, and published
  row-block;
- make oldest-block-first a priority policy, not a global all-block barrier;
- require overlap metrics before claiming the structural worker-starvation
  problem is fixed.

Those points are incorporated above.
