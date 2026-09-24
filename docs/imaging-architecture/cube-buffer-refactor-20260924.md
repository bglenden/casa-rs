# Cube buffer refactor

Truth class: approved implementation plan; milestone-C review pending
Last reality check: 2026-09-24
Verification: focused reconstruction/runtime/application tests and matched CASA runs; see CURRENT

The design and proposed-size sections below record the preimplementation decision.
The current implementation and measured results are in the single T55 `CURRENT.md`
and its linked Review-3 evidence. Passing this cube workload does not by itself
complete T55 or migrate MFS.

## Outcome and sequence

Owner direction: first checkpoint active work, then reduce cube buffers and make
inactive state systematically disk-backed; return to MFS afterwards. A larger
buffer than LibRA/Obit needs a concrete scientific or measured runtime benefit.
"Buffer" includes image arrays, grids, visibility blocks, caches and workspaces.
No cost solely for bitwise numerical reproducibility is justified. Preserve the
standard science and the numerical policy in AGENTS.md.

Later owner instruction: present current/proposed buffers and the LibRA/Obit
comparison before implementation. Production sources are restored to the
checkpoint. An unbuilt compensation-removal edit is preserved separately as
`unbuilt-compensation-removal-held.patch`; it is not an accepted candidate.
The source study and unchanged-binary tracing are complete. The following plan
consolidates the subsequent owner discussion; it is not an implementation or
large-run authorization. The owner subsequently requested a detailed handoff for
a less capable implementation agent, resolution of design choices before coding,
and a few mandatory review stops. The decisions and stops below implement that
request. Do not restart discovery or reinterpret a review stop as permission to
launch another optimization campaign.

Pre-refactor source is commit `10a544fffab22b4b3540dec771031847db51704a`.
Its source archive, incremental Git bundle (base `405d01adc5`), and untracked
diagnostic scripts are verified in the durable internal and GLENDENNING
`casa-rs-evidence/t55/cube-buffers-20260924/checkpoint-10a544fffa` directories.
The commit is a work-in-progress checkpoint, not completed MFS acceptance.

## Scope, evidence and restart handles

Start with the landed standard-grid, natural-weight, Stokes-I streaming cube:
selected input, band imaging, the existing CLEAN controller, residual refresh,
and the existing product writer. Preserve the capability selection; this work
does not implement density weighting, W/AW, Taylor coupling or a second controller.

Durable evidence root:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/cube-buffers-20260924/`.
`buffer-proposal.md` contains the current/proposed/LibRA/Obit buffer census,
revision-pinned source references and qualifications. `parent-w1-v2-*` and
`parent-w4-v1-*` contain command, executable identity, logs and resource results.
The durable copy of this plan is `implementation-plan.md`; the repository file
is the editable source, and the durable copy must remain identical at handoff.
The only live status summary remains
`../tranche5-20260916/CURRENT.md` relative to the evidence root.

Unchanged-binary diagnostic baseline: 42,120 rows, 512 channels, two correlations,
512-square images, 640-square grids. W1: 57.850 s and 6.588 GiB aggregate RSS;
W4: 23.105 s and 8.531 GiB. Tracing was enabled. Seven native W1/W4 fingerprints
match, but this is not a fresh CASA acceptance or a clean performance baseline.
Do not compare an uninstrumented candidate against these times as a measured win.

## 1. Reduce the buffers and their consumers together

Sizes below are logical payloads for that workload, not summed process RSS.
These representation targets are conditional on unchanged scientific acceptance.

| Buffer | Current | Target and migration |
|---|---|---|
| Current residual / dirty image | 2 GiB complex double | 0.5 GiB real float; migrate covered real-valued consumers, not arbitrary complex science modes |
| PSF | 2 GiB complex double | 0.5 GiB real float; preserve normalization, support and beam semantics |
| Natural-weight sensitivity | 1 GiB repeated double values | One double/channel, 4 KiB; broadcast in consumers without constructing a dense cube |
| Dense model values | 1 GiB disk-backed doubles | 0.5 GiB disk-backed floats with separately retained support; avoid per-pixel rich-object storage |
| Initial gridding | Four complex-double grids, 25 MiB/plane | Remove two compensation grids; test two ordinary float grids, 6.25 MiB/plane; retain ordinary double accumulation only for demonstrated accuracy need |
| Refresh / prediction grids | Compensated double residual and double prediction | Remove compensation; precision follows affected comparison, load only required model support planes |
| Completed initial results | 10 MiB/channel, up to 5 GiB pending in W4 | Drain through the existing per-partition commit point; FFT is already dropped, so remove its obsolete retained reservation rather than claiming a new physical saving |
| Prepared visibility replay | About 1.207 GiB before framing | Preserve input float complex samples/weights: about 0.725 GiB; retain double frequency/geometry where needed |
| Clark transform workspace | Two full complex-double arrays, 32 MiB/active plane | Evaluate equivalent real/cropped-linear transforms, about 9 MiB of named scratch; retain batched Clark and verify crop/alignment semantics |

Remove intermediate widening/narrowing copies at their callers, not merely the
final buffer types. Keep block operations and ordinary contiguous kernel slices.
No bitwise reproducibility surcharge. Replace calculated-output bit-equality
tests with existing scientific tolerance checks; exact storage round trips and
shape/mask/metadata checks remain exact. A failed precision comparison justifies
the affected precision, not restoration of unrelated compensation or copies.

Two simultaneous dirty/PSF grids may remain when fused visibility traversal
earns their memory cost. Do not split them solely to match another package's
buffer count. Independently justified extra buffers must have an explicit
scientific or measured I/O/runtime benefit in the final comparison.

## 2. One typed storage and residency mechanism

Use composition, conceptually `Buffer<T>` plus bounded read/write window guards;
these names are illustrative, not a new public application interface. All bulk
numeric types use the same residency policy, including float, double and complex
elements. The manager knows layout/bytes, pinning, dirty/backed state and reuse
hints, not WCS, weighting, Taylor semantics or the meaning of a pixel.

Reuse the existing `PagedArray<T>` tiled storage. Evolve/consolidate the
`NormalArrayStorage` and `ModelSampleStorage` seams and their runtime adapters;
do not preserve f64-only wrappers merely for internal compatibility. Keep
scientific semantics in reconstruction and resource/residency policy in runtime,
with storage below them through the existing dependency direction. A second
memory system per imaging mode, a new standalone framework/crate, and a new
dependency-direction change are not part of this plan.

A pinned window exposes a typed contiguous slice for the kernel. Its lifetime
prevents eviction while borrowed; conflicting writes are excluded. Releasing a
guard unpins and records modification, but does not hide fallible I/O in Drop.
Flush/eviction/read errors return through the ordinary run error path. No
per-pixel cache dispatch, raw byte reinterpretation of arbitrary Rust objects,
or global manager lock held through a numerical kernel or slow disk operation.

| Buffer state | Shared mechanism |
|---|---|
| Active/pinned | Resident until all current access guards release |
| Inactive, needed later, dirty | Retain if useful space remains; otherwise write a large block and release its actual allocation |
| Inactive, valid disk copy | Evict without writing; reload only required windows |
| Dead | Drop/recycle immediately; never spill just to preserve dead scratch |

Logical disk backing is available from creation for spillable state; creating
such a buffer must not allocate the whole cube or necessarily write it all.
Materialize backing blocks only as required. Enough memory means useful buffers
can remain resident without obligatory intermediate I/O. Small metadata stays
ordinary Rust data. Opaque FFT/library allocations use the same accounting but
are non-evictable while live; release/reuse them rather than pretending arbitrary
library objects can be serialized. Preserve existing persistence-boundary checks.

Use known phase/next-use information for deterministic release and eviction.
Use LRU only among remaining unpinned cache candidates; sequential one-pass
access should not displace known-near-future data blindly. Do not build a general
predictive scheduler: the existing execution sequence supplies these hints.

### Resolved storage decisions for implementation

1. **One run-scoped residency coordinator in runtime.** Reuse the current run's
   memory authority; the coordinator is its physical-owner/cache policy, not a
   second independent budget or a process-global singleton. Other modes supply
   typed buffer descriptions and phase needs to this same mechanism. No new
   top-level library or general-purpose virtual-memory framework.
2. **Storage-agnostic typed capability on the reconstruction side.** Generalize
   the existing non-application-facing storage seam over the element type where
   real reuse requires it. Runtime implements it with managed typed blocks and
   existing persistence. Do not add reconstruction dependencies on runtime or
   lattices. Scientific owners retain write authority; immutable generations
   expose read access only. Avoid a universal enum of scientific buffer roles
   in the residency manager and avoid arbitrary byte serialization of Rust
   structs. Frame/run/shape associations stay with existing owners.
3. **Plane-aligned blocks for this channel-local cube.** One managed numeric
   block is one spatial plane. Kernels use one plane at a time; a band's model
   support is loaded as the required individual planes. Retain the existing
   one-output-channel band decomposition initially. No new tuned band size or
   automatic all-channel prefetch. Small product windows borrow a subrange of
   a pinned plane; an operation that truly needs a contiguous multi-block window
   must explicitly admit that gathering allocation. Other modes may declare a
   different natural block shape without implementing another cache.
4. **Resident blocks, not hidden slice copies.** Read guards keep their resident
   block pinned and can share immutable storage. Write access is exclusive and
   marks the block dirty when mutably exposed. A guard must not permit a slice
   to outlive the pin; no implicit copy-on-write. Reserve all required blocks
   before entering an operation. Short per-buffer synchronization is acceptable;
   do not hold the coordinator mutex while waiting for a block, doing I/O or
   executing a kernel. Never wait for more residency while holding an incomplete
   operation's pinned set.
5. **Reuse tiled persistence with explicit, bounded codec staging.** The current
   `PagedArray::get_slice` always allocates/copies a result, has a mandatory
   one-tile cache, and is not Sync without an outer lock. Add a typed transfer-
   into-caller-storage operation at the existing `TiledArrayStorage` seam,
   surfaced narrowly through casa-lattices. Use borrowed slices for writes;
   remove runtime's extra `values.to_vec()`/ArrayD assembly. Reuse the existing
   tile traversal, endian/boolean codecs and standard file format. An explicitly
   counted one-tile backend staging buffer may coexist with the managed block;
   do not retain a second multi-plane cache or claim this unavoidable decode
   copy is zero-copy. Keep file/backend owners per array, not per plane. The
   coordinator accounts and reclaims idle backend staging as well as its blocks.
6. **Do not use cache budget zero as a no-cache trick.** Public typed APIs reject
   zero, and a lower private zero setting selects a whole-array flat cache.
   Use an explicit positive one-tile staging budget. To reclaim it with the
   current API, `temp_close()` flushes and drops the storage owner; `flush()`
   alone does not free LRU capacity. Do not close/reopen on every hot cached
   access. Reclaim idle backing handles at phase/pressure points, and account
   reopen/resize overlap. A later cache-bypass extension needs review rather
   than a second implementation silently added by the handoff agent.
7. **Typed guard mechanics remain internal.** The minimal acceptable design
   is an owned resident block plus a pin token/typed view; use ordinary safe
   borrowing or block-local locking to enforce read/write exclusion. A type-
   erased control trait is allowed only in the coordinator's heterogeneous
   registry, never inside pixel loops. Do not clone data merely to satisfy
   lifetimes. The first review stop checks the actual small Rust interface
   before it is propagated through callers.
   Short-lived private grids/FFT scratch may remain ordinary Vec/Array storage
   under the same admitted workspace owner; do not force cache registration or
   disk backing onto scratch that is pinned for its entire useful lifetime.
8. **Disk validity is separate from residency.** A new model's known zero/default
   blocks can be represented by a default-fill descriptor until first use;
   generated residual/PSF blocks begin unwritten and must fail if read before
   generation. Track written/initialized coverage by blocks/ranges, not by a
   full-array scan. Writeback marks a block backed/clean only after successful
   required I/O; on failure retain the dirty data until normal error teardown
   and fail the run. No resume journal, checksum-attestation state machine or
   hidden errors in destructor-based flush.
9. **Preserve epoch ownership without RAM duplication.** Keep old and new
   residual backing distinct until the existing scientific owner accepts the
   replacement; do not mutate a still-readable prior epoch in place. Old
   unchanged PSF/normalization storage is shared, not copied. Old residual
   residency may be evicted first. Release obsolete backing through ordinary
   ownership teardown after transfer, not a new rollback protocol. Existing
   run-owned scratch lifecycle is distinct from user evidence/worktree cleanup.
10. **Eviction order is deterministic before it is LRU.** Free dead scratch;
    evict blocks whose declared next use is after the next phase; then choose
    least-recently-used unpinned eligible blocks with a stable block-id tie
    break. Dirty eviction is bounded writeback, clean eviction writes nothing.
    Never evict pinned blocks. Preserve recently loaded data needed by the
    imminent phase over a one-pass scan; supply next-use/one-pass hints at block
    scheduling, not per pixel. This is not an exact floating-result order rule.

These are decisions, not invitations for the implementation agent to invent
multiple alternative backends. Stop at review 1 if the selected reuse seam
cannot meet the contract without substantial additional architecture.

## 3. Planner and physical residency must agree

Let N be pixels/plane, G padded grid cells, C channels, W active workers, B output
planes per worker and H required prediction-support planes (including halos).
Byte formulas use actual element widths, checked products and library scratch
bounds. Shared allocations count once. Returned windows, conversion overlap,
thread stacks, queues and I/O scratch count separately when genuinely live.

For each legal overlap of execution phases, require:

`shared non-evictable bytes + active worker workspaces + bounded queues + I/O scratch + resident cache <= usable memory budget`.

Take the maximum over permitted overlaps, not a sum of all phases and not just
the largest isolated phase. Workers and producer/writer I/O can occupy different
phases concurrently. Retention permits must follow physical owners: returning
an accounting permit without releasing/reusing its buffer is not a saving.

Planning and admission rules:

1. Compute one-worker minimum workspace and legal W/B/H choices from shape and
   resources; expose the limiting buffers if no plan fits. Do not tune to the
   512-square fixture or infer a cost from total cube volume alone.
2. Allocate the shared cache from the remaining budget, not independent fractions
   of free RAM per buffer. Account for underlying tiled caches AND returned
   windows; do not stack hidden caches below the manager.
3. Before a phase/job starts, reclaim eligible cache and reserve its full required
   simultaneous pinned set plus progress-making I/O scratch. Do not pin half a
   job and wait forever for the other half; bound pending work and release on
   failure/cancellation. Reclaim cache before unnecessarily sacrificing workers.
4. Bound completed-band storage and apply backpressure if its consumer is slow.
   Preserve logical channel order/coverage without a full-wave result vector.
5. Reduce admitted W/B when active minima require it, never as a replacement for
   eviction. Do not rely on OOM recovery, OS swap or unaccounted mappings.
6. Bound disk bytes and handles as well as RAM, including working state, replay,
   output staging and any required old/new residual overlap. Disk-full/read/write
   failures fail the run with existing publication semantics.

### Resolved phase and admission decisions

The current cube application does not overlap initial imaging, CLEAN, later
major refresh and publication as independent pipelines. Preserve that sequencing.
Within band execution, up to W jobs have different local preparation/accumulation/
completion lifetimes. The existing bounded executor joins that worker wave and
commits its results before starting the next; do not introduce background
imaging/writeback overlap merely to justify a more complex planner. MS preparation
has its existing separate bounded source overlap and must be counted as such.

| Phase | Mandatory simultaneous owners | Eligible to reclaim / avoid |
|---|---|---|
| Input preparation | Selected input slots, native preparation arrays, replay writer/frame and required metadata | No image grids or all-channel image payload; previous run state is not a cache |
| Initial imaging | W band workspaces, W native input windows, shared replay cache, sink transfer/staging, geometry and small channel descriptors | Completed results from previous worker waves; cold generated planes may spill |
| Threshold/CLEAN | Current normal/model/support windows, W actual solver workspaces when parallel, statistics/controller state | Initial grids/input blocks and unused replay cache; cold channel planes |
| Residual refresh | W residual workspaces, required H model-support grids/windows, native input windows, sink staging; old epoch remains logically available | Cold old residual pages, unchanged PSF payload copies, retired CLEAN scratch |
| Product publication | Bounded product-generation windows, writer buffers and the actual still-needed science inputs | Grids/replay/solver scratch; already consumed planes with no further use |

Fix W to the largest requested concurrency whose mandatory working set fits
after cache reclamation; do not maximize optional cache first and then suppress
workers. Derive remaining cache capacity as budget minus mandatory live owners,
rounded down to complete blocks, allowing zero managed cache beyond active pins.
Cache capacity is a ceiling, not an eager allocation or instruction to load
otherwise unused planes. Shrink it before entering a larger-workspace phase.
Do not introduce arbitrary machine-specific fractions as a substitute for these
owner-based formulas. Existing sampled RSS guard remains independent of planner
claims and detects allocator/library/unmodeled-process overhead.

**Four is a comparison point, not a system limit.** W is bounded by the current
request's worker allowance, existing CPU/topology allocation, independent ready
jobs and the calculated working-set budget. With more usable CPU capacity and
memory, admit more than four. Pending descriptors can cover thousands of planes;
only admitted jobs allocate large workspaces. Test planner/queue arithmetic at
W=1,2,4,8,16 and W>C using synthetic topology/shape fixtures. Preserve existing
runtime allocation authority, not a new hard-coded four-worker cap. Do not launch
additional full-workload timing configurations merely because these planner
tests cover larger W. I/O/bandwidth saturation is measured performance evidence,
not an unexplained fixed concurrency limit.
These are imaging workers. The separately authorized two Cargo build jobs stay
unchanged; changing imaging concurrency does not increase build parallelism.

At most W completed one-plane results wait at the current worker barrier. Drain
them in order, transferring/moving their buffers into managed final storage.
If a sink conversion must allocate, include its one-result overlap explicitly.
Do not reserve a second W-sized queue after results have been moved. Source
descriptors may scale with C, but completed pixel arrays and FFT scratch must not.

Useful numerical checks (MiB; float-grid target, B=1, no model in initial phase):

| Named initial-imaging payload / bound | 512-square W1 | 512-square W4 | 2048-square W1 | 2048-square W4 |
|---|---:|---:|---:|---:|
| Two complex-float grids | 6.250 | 25.000 | 95.367 | 381.470 |
| Existing conservative FFT resident bound, narrowed to float | 3.755 | 15.020 | 14.668 | 58.670 |
| Completed dirty+PSF payload, separate worker-barrier phase | 2.000 | 8.000 | 32.000 | 128.000 |

The current padding formula gives 640 and 2500, not 2560. On this 64-bit host,
the existing FFT bound is `769 * padded_axis * sizeof(complex element)` for a
square grid; distinguish this conservative library allowance from measured heap.
Initial grid+FFT named subtotal is 10.005/40.020 MiB for W1/W4 at 512, and
110.035/440.140 MiB at 2048. Input windows, normal/model access, sink staging,
convolution tables, metadata, stacks and cache remain additional named terms,
not an arbitrary percentage. A final result can coexist with the last grid
during conversion; compute that phase maximum from actual construction order.

The previously quoted ~1.03-1.06 GiB small-case subtotal is NOT a total RSS
estimate. Final exact owner/header capacities and empirically opaque library
costs are verification facts for review 1, not unresolved architectural choices.
Use the existing sizing functions as a starting point, delete only demonstrably
dead/duplicate terms, and bind projected bounds to capacity tests. Never subtract
every logical saving from measured 8.531 GiB RSS to manufacture a forecast.

## 4. Integration and deletion sequence

Batch related edits/builds into runnable milestones, not a component-only campaign.

| Milestone | Concrete work and completion evidence |
|---|---|
| A: storage/planner foundation, then review 1 | Implement the selected minimal typed seam and transfer-into-storage operations with one shared budget, real eviction, multi-axis layout and focused tests; bind actual capacity formulas. Do not broadly migrate science callers before the review. The existing checkpoint/source study suffice. |
| B: one complete compact, managed cube candidate, then review 2 | Batch initial gridding, ordered commit drain, scalar sensitivity, model/normal access, CLEAN, refresh and writer migration. Integrate shared cache/phase admission with actual owners. Reach the complete application in both comfortable and constrained memory; do not stop at another collection of components. |
| C: remaining reductions, acceptance and deletion, then review 3 | Finish precision/input/FFT reductions with discriminating checks or present measured scientific reasons for retained costs. Remove obsolete internals, verify scaling of memory with W/spatial size/channel count and all scientific acceptance, report I/O/time/RSS. Do not silently drop approved scope. |

### Exact result-draining connection

Use `PartitionedKernel::commit`, which already runs after each completed
at-most-W dispatch wave and before the next wave in `bounded_stream.rs:1315`.
The indexed worker collection preserves job order; cube partitions use ascending
channel ordinals and unique exclusive regions. No new source chunker, worker
pool, writer thread or reorder map is required.

Move the existing initial-fold/refresh-append consumer from the post-execute
`for normal in wave.bands` loop into a sink owned or mutably borrowed by
`BandKernel`. Resolve the execution binding on the caller before pool entry:
`WorkExecutionContext` contains an Rc-backed scheduler context and must not be
captured in this Send+Sync kernel. Split the existing initial
`CompleteDataSlabResult::from_streaming_cube` operation at its current boundary:
perform its predecessor fence, problem/attempt/node/lease, source generation and
sample-count checks once before dispatch; keep the existing owned
`CompleteDataExecutionBinding`, replay summary and selected/continuum generation
IDs in the sink, not the execution context or a new replay payload. Each adopted
band still validates its specification, shape, channel
coverage and problem association through the existing reconstruction fold.
This moves existing checks, not introduces an attestation object or permission
protocol. `PendingCubeRefresh::new` already validates its context once and has a
context-free `append`; use that same separation for initial results. Do not add
unsafe Send/Sync implementations, globally replace scheduler Rc with Arc, or
move the whole executor across threads to accommodate this connection.

In `commit`,
take the corresponding `Completed` result, append it to this sink, and release
the result before the next worker wave. Keep explicit pending/completed/consumed
coverage so skipped, duplicate or wrong-phase results fail; do not retain consumed
pixel payloads for validation. Executor `complete` checks all jobs consumed and
returns the pending fold/refresh plus compact measurements, not Vec<BandResult>.
After the pool has joined, finish the fold against the caller's original replay
and retain the current final replay-id/coverage checks. Finish refresh through
its existing method at the same point. The terminal replay owner need not move
into the pool. Test all moved binding rejection cases and compile-time Send/Sync
bounds; a short mutex around sink state is acceptable if needed for its actual
type, but the executor has exclusive commit access and no pixel-loop lock.
Keep input descriptor/reader and immutable model lifetime through the same pool
join. Reuse the existing fold's ordered shape/run/channel checks.

Use Partial=usize: after storing a completed result, execute returns its band
ordinal and commit consumes that existing job slot. `WorkIdentity` has private
fields and no accessors; do not expand its API merely to recover that ordinal.
The partial is one word of metadata, not another result owner or pixel queue.
Check ordinal/order and completed-slot state before adoption and retain the
existing failed-execution behavior so a missing result cannot become success.
Remove whole-prefix pixel-retention charges only after the owner is drained.
The current executor already drops FFT at execute.rs:382; correct the stale
returned-FFT allowance without claiming that a live FFT was eliminated.

### Precision and transform sequence

The compact candidate targets real-f32 normal/model storage and ordinary-f32
grid/prediction buffers together with direct narrow input storage. Frequency,
coordinates and scalar sums/normalization may remain f64; scalar arithmetic
precision is not the same as duplicating full arrays. For covered real-output
modes, discard imaginary output only at the existing image formation seam,
not before gridding/FFT. Other algorithms' genuinely complex/Taylor state must
not be narrowed by a global substitution.

If a scientific comparison fails, locate the first divergent stage and test
the specific precision hypothesis once under the escalation contract. Do not
toggle arbitrary tolerances or restore all old arrays. Ordinary f64 for an
identified reduction can remain with recorded accuracy evidence, using the same
pipeline and manager; no user-selectable legacy/precision fallback is introduced.

Clark's equivalent real/cropped-linear transform work is evaluated after the
complete managed cube exists, so it cannot delay integration of the large
storage wins. Keep batched Clark component updates. Test an edge and off-centre
impulse, asymmetric PSF, even/odd supported sizes and non-square shapes against
the existing linear convolution at required tolerances before replacing it.
Do not accept the ~9 MiB projection as proof that a particular FFT library API
or crop convention is correct. Report a demonstrated retained cost at review 3;
an unresolved design/science failure follows escalation, not silent deferral.

Seed code, not an invitation to scan unrelated modules:

- `casa-imaging-reconstruction/src/streaming_cube/{band,memory,completion}.rs`:
  grid owners, byte formulas, field formation; `spectral_operator/normal_storage.rs`
  and `model_storage.rs`: typed scientific access; `minor_cycle/clark.rs`: batched
  CLEAN scratch and equivalent transform evaluation.
- `casa-imaging-runtime/src/streaming_cube/{input,execute,phase,normal}.rs`:
  replay precision/block access, result draining and allocation ownership;
  `cube_state_plan.rs`, `paged_cube_state.rs` and existing resource authority:
  shared admission, physical retention and backing/cache migration.
- `casa-lattices/src/paged_array.rs` and its existing tiled backend: reuse storage
  and cache controls, without importing imaging-specific policy into storage.
- Existing reconstruction-cycle consumers and product writer: migrate bounded
  windows without a second CLEAN controller, full product materialization or
  publication attestation.

Read the selected MS in bounded blocks. Reuse the channel-oriented prepared
store for required band/support ranges and later major cycles; do not repeatedly
decode the whole MS for each plane. Keep replay preparation's cost visible in
end-to-end timings. At no point must the entire MS or all image planes be resident.

Delete displaced resident-versus-paged factory dispatch, duplicate accounting,
f64-only conversion adapters, completed-result collections and obsolete tests
when their replacements are connected. No permanent legacy path or compatibility
shim. Audit directly affected comments/docs, including obsolete sealing/bitwise
requirements; preserve history as non-normative. Accepted ADR changes still
require the applicable explicit authority.

After cube acceptance, return to the interrupted MFS work with the same storage
mechanism. MFS supplies different buffer requirements and phase overlaps, not
another residency manager. Preserve its checkpoint and census meanwhile; do not
claim MFS is migrated or accepted because cube tests pass.

### Execution recipe and evidence reuse

Do not apply the held patch blindly or begin by rerunning the source census.
Start each implementation milestone at the seed functions above. Set
`CARGO_INCREMENTAL=0` and `CARGO_BUILD_JOBS=2`; run affected tests in
`casa-tables`, `casa-lattices`, `casa-imaging-reconstruction` and
`casa-imaging-runtime`, filtered to changed storage/cube/bounded-execution
behavior. Batch the application build after related integration edits. The
existing complete application test is
`t55_real_cube::t55_q_band_rebaseline_preflight` in `continuum_application`,
invoked with `--exact --ignored --nocapture --test-threads=1` by the harness.
Do not run this ignored test naked with guessed environment variables.

Use the full commands/environment in `parent-w1-v2-command.json` and
`parent-w4-v1-command.json` in the evidence root. They invoke `scaling_4x.py` /
`overnight_scaling.py` under the existing `finish_stage.py` aggregate-RSS guard.
For a candidate, change only its unique output label, binary, requested worker
count and deliberately selected memory budget; preserve input/science settings.
Record those changes. The native input is under
`/Volumes/GLENDENNING/casa-rs-evidence/t55/q-band-rebaseline-20260918/overnight-scaling-20260919/rows4x`.
The valid frozen Measures/reference configuration is under
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/mfs-4096-workload-20260923/pilot-runs/reference`;
retain the command's `study.FROZEN` override. The old reference tree caused the
already-recorded missing-table failure; repeating it is not a useful test.

The same `scaling_4x.py compare` route uses the unchanged comparison contract in
`tools/perf/imager/workloads/t55-clark-cube-development.json`. Resolve the
existing CASA product label from the saved reference before comparison; do not
create a new scientific oracle unnecessarily. Verify mount, input/reference
availability and disk headroom before a run. Keep diagnostic flags matched
between parent and candidate, or measure both without them. Evidence labels
must not overwrite prior results. Store concise commands/results and logs in
the durable evidence root; update the one CURRENT only at the review boundaries
or on a material blocker.

## Large-cube design condition

2048x2048x2048 is a normal design case, not an authorized full execution now.
One Float cube is 32 GiB, residual+PSF+model 96 GiB, an extra residual 32 GiB,
and seven full Float products 224 GiB before masks/metadata/input/work storage.
One Float plane is only 16 MiB. Full cube payload belongs on disk; active memory
scales with W/B/H, spatial plane size and the shared cache, plus bounded metadata.

Increasing channels must not force all images/grids/completed results into RAM.
Increasing spatial size legitimately increases the minimum active workspace;
if even W1/B1 plus required support does not fit, reject clearly. This plan does
not promise arbitrary-size spatial FFTs out of core. Test channel counts and
voxel/file offsets beyond 32-bit ranges with checked-layout tests and bounded
storage fixtures; do not generate a huge dataset merely to test arithmetic.

There is a confirmed pre-existing large-shape blocker: `CubeArrayLayout::new`
stores `[logical_scalars]` as one axis, while `TiledArrayStorageLayout::new`
requires each axis <= i32::MAX. Thus 2048^3 is rejected despite file bytes fitting
the format's signed-i64 size. Replace the flattened on-disk axis with existing
multi-axis storage: for native plane order `x*ny+y`, use logical shape
`[ny,nx,channels]`, tile `[ny,nx,1]` (axis zero fastest). The resulting native
offset is `channel*nx*ny+x*ny+y`. Use additional real axes for other already
supported dimensions, not a giant flattened axis. Change range-to-storage
addressing with the layout; retaining old `[start],[len]` reads is incorrect.
This uses the existing tiled format, not a new CASA format or weakened bound.
Test axis orientation and channel boundaries on small fixtures, and 2048^3
metadata/layout with no pixel allocation or dataset creation. Retain checked
total byte products, per-axis format bounds and signed-i64 file-size bound.

## Mandatory review stops for the implementation agent

These are three logical review boundaries, not a review ceremony per commit.
At each stop, update only CURRENT with source revision/diff, tests/commands,
buffer/IO evidence, failures and next action; stop the turn and request review
from the supervising Astra agent. Do not proceed to the next milestone until
that reviewer records GO. A component test pass or the implementation agent's
own assessment is not that review. Routine reversible repairs within a milestone
remain autonomous; scope/science/resource/escalation stops still apply sooner.

### Review 1: ownership, storage and admission foundation

Required evidence:

- One typed manager handles at least f32 and complex-f32 with the same policy;
  meaningful tests also exercise f64/packed support persistence, not just mocks.
- Real low-budget dirty eviction/reload and clean eviction; no extra write on
  clean eviction; live bytes actually fall on reclamation; guard/pin exclusivity
  and error/cancellation release are tested. No I/O error is swallowed in Drop.
- The one-tile codec staging and any returned windows are visible in the budget.
  The common whole-operation admission test can progress when almost full,
  rejects impossible requests before kernel work, and never deadlocks by partial
  pinning. No fallback that allocates an entire array or default cache.
- 2048^3 layout succeeds without huge allocation, the old flattened-axis failure
  is covered, and small 3D fixtures bind orientation and linear-range mapping.
- W=1/2/4/8/16, unequal jobs, W>C, minimum/just-below-minimum budgets and arithmetic
  overflow tests; actual conservative byte formulas reconcile with capacities.

Reviewer checks the small Rust interface, dependency direction, real allocation
ownership, lock/I/O ordering and scope. STOP/REWORK if it adds a second manager,
per-pixel virtualization, full-window copies hidden below a cache, unsafe lifetime
escapes, unexplained buffer multipliers or a large framework. Do not demand
application performance evidence before this bounded foundation review.

### Review 2: complete managed cube, not components

Required evidence:

- The existing selected-input -> initial -> CLEAN -> residual refresh -> product
  writer path runs completely with the new compact storage and same controller.
- W1 and W4 on the approved 42,120-row/512-channel/512-square workload, with the
  unchanged seven-product/nine-check CASA comparison and inspected panels.
- Repeat the candidate at a constrained budget that admits active W4 work but
  cannot hold all its logical normal/model state. Compute that budget from the
  now-verified formulas; record the chosen bytes before the run. Do not simply
  lower workers or omit CLEAN/publication to claim out-of-core success.
- Actual queue peak <= admitted W band results, no C-proportional completed
  payload, bounded cache/IO and aggregate RSS, stable epoch association on refresh.
- One matched parent/candidate timing observation including preparation, replay,
  imaging, CLEAN and publication; repeat only to resolve material uncertainty.

STOP/REWORK if only an alternate test entry point works, spill is a permanent
second science path, correctness fails, buffers are merely unaccounted, or a
large new I/O/runtime penalty lacks an identified cause. Reviewer approves the
remaining bounded reductions before milestone C; no open-ended optimization loop.

### Review 3: scale, simplification and accepted result

Required evidence:

- Larger spatial working-plane tests (2048 square), large logical channel counts
  and 64-bit-offset checks; fixed-budget tests show payload residency depends on
  active planes/cache rather than C. No unauthorized full-size run.
- Final affected tests, unchanged CASA comparisons/panels, and W1/W4 end-to-end
  time/RSS at comfortable and constrained budgets. Retained f64/extra buffers
  have concrete numerical or measured runtime/IO justification; precision and
  FFT attempts have explicit results, including negatives.
- Obsolete cube resident/paged dispatch, f64-only conversions, batch pixel
  collections and duplicate accounting deleted; tests/docs updated without
  removing behavior coverage. Review changed/directly exposed code for over-
  engineering. Existing unaffected scientific modes remain working.
- Updated current/measured buffer and phase-aggregate tables, disk bytes and
  I/O counts. Clearly distinguish working implementation, scientific acceptance,
  memory reduction and timing effects. Return to MFS next using the same manager.

Reviewer returns GO, bounded repair, or architectural escalation with the exact
deficit. No push, merge, release, cleanup or goal completion is implied by GO.

## Acceptance and limits

- Focused tests bind byte formulas to actual buffers across image sizes, channel
  depths, worker counts, initial/refresh phases, and overflow cases.
- Disk-backed tests exercise eviction, dirty/clean reuse, bounded live windows,
  pin safety, conflicting access, deterministic phase eviction, sequential-scan
  cache behavior, slow-consumer backpressure, cancellation, capacity release,
  I/O failure and shape/lifecycle preservation. Exact storage
  round-trip bits remain appropriate; calculated images use scientific tolerance.
- Run the complete application at generous and constrained memory, including
  real CLEAN, refresh and publication. Verify that increasing channel count does
  not require all cube state resident. Include 2048-square working-plane tests
  and large logical shape/offset tests within existing resource/run authority.
  Count block I/O, copies and actual live bytes, not just reservation totals.
- Reuse the 42,120-row, 512-channel, 512-square CASA reference for the unchanged
  seven-product/nine-check comparison and panels. Record current parent/candidate
  W1/W4 time and sampled peak RSS, including preparation, I/O and publication.
  Historical 49.166/20.289 s times are context, not a current parent measurement.
  Match profiling/cache/timing boundaries; do not force a fixed repeated-run
  count. Record any runtime cost of reduced residency and justify retained
  larger buffers against the pinned LibRA/Obit source comparison. Do not claim
  upstream matched RSS without measuring it.
- Retain 16-GiB native planning and aggregate sampled RSS, 8-GiB build/diagnostic
  guards, two Cargo jobs, no fixed wall cutoff. No push, merge, release, cleanup,
  new Obit installation, full 360-time MFS input or full-32GB run.
- Keep restart state in the existing single T55 `CURRENT.md`; source studies and
  run logs live under the durable cube-buffer evidence root. A source buffer
  census or passing component tests do not constitute completed acceptance.

Scientific algorithm changes, acceptance relaxation, external API/format changes,
or unresolved consequential design choices follow the existing escalation rules.
The authorized storage-policy changes do not change CASA persisted formats or
publication failure semantics.

No numerical/storage changes are applied yet. The held patch is unbuilt evidence,
not the next candidate to apply blindly. No active Goal was created. The existing
escalation contract applies to unresolved consequential design/scientific issues;
ordinary in-scope setup and integration repairs do not require new authority.

Local design checks used two bounded Luna Max source inspections, reconciled
against the executing code by the supervising agent. Oracle review has not run:
the Chrome submission was blocked before any context was sent, and explicit
permission to send local design/performance context is pending. Do not describe
this document as Oracle-approved. That pending consultation does not erase the
resolved decisions above or authorize the implementation agent to invent a
different architecture without review.
