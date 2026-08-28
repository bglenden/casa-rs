# First imaging-performance source study

Truth class: primary-source implementation-boundary research
Date: 2026-08-27
Scope: complete-data, constant-basis MFS first slice

## Decision summary

The current architecture already has the right scientific owners, but it does
not yet have one shared bounded streaming execution seam. The first slice
should add that seam without reviving the pre-cutover whole-run imager:

```text
casa-ms selected ordered block traversal
    -> casa-imaging-runtime bounded execution
       (budget, reusable buffers, backpressure, optional read-ahead,
        workers, deterministic-work coordination, measurements)
    -> reconstruction/gridding kernel
       (sample semantics, grid partition, mergeable partial state,
        deterministic reduction, scientific completion)
```

`workers = 1` must use this same scheduler, buffer-pool, kernel, and reduction
path. Applications compose the source, plan, executor, kernel, and product
sink. Frontends supply parameters and units only. The seam must not retain the
MeasurementSet, allocate a full grid per worker, introduce an MFS-only fast
path, or preserve the displaced pre-cutover runner as a fallback.

No optimization should follow from this study alone. Before an implementation
change, freeze one matched current casa-rs/CASA MFS workload and record CASA
products and timings. Then instrument the current casa-rs path. The historical
results below show which mechanisms once worked; they are not a current
baseline.

## Source availability and constraints

- casa-rs current checkout: `5ef9f4729e4ffa787c30dfffed38c1e7eaf69d6b`
  on `codex/imaging-performance-pass`; clean when the study began.
- Requested historical objects are available locally:
  - `7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40`, 2026-08-18,
    immediately before the imaging programme refactor.
  - `009745a080c90c92bffb07de2603136852ab0630`, 2026-08-25,
    the transition checkpoint. The supplied shorter object name resolves to
    this full commit.
  - `fff9c2d553eace4b6a57b1df9ded4773f2263ceb`, 2026-08-25,
    the native continuum cutover.
- CASA source is available at committed `61020062cee290f5466cffed5ec5032e0c7a3434`.
  Its worktree contains unrelated tracked and untracked changes, including
  GridFT-related files, so every CASA citation and inspection used
  `git show HEAD:<path>` rather than the working tree.
- casacore source is available at committed
  `3dd6f3f23417b9cf42314f8967fc84d0dd29dc49`. Its worktree has unrelated
  untracked notes; committed blobs were inspected.
- libRA source is available at committed
  `0ab99e261878334d6588eafa360cef3b673e897f`. Its worktree has one unrelated
  deleted generated Python cache file; committed blobs were inspected.
- The study did not execute CASA, casa-rs, the medium dataset, or the directly
  mounted 32 GB dataset. It therefore provides no new timing, correctness,
  pass-count, copy-count, or peak-residency measurement.

The transition matters only as an ownership map. At `009745...`, the new
`casa-imaging-reconstruction` and `casa-imaging-runtime` crates already existed
beside the old application implementation. Commit `fff9c2...` completed the
cutover, added runtime serial-continuum composition, and deleted roughly
104,000 lines from the old application path (`git show --stat
fff9c2...`). Mechanisms below should move into those accepted owners, not back
across that deletion boundary.

## Current path before optimization

The present implementation has several pieces worth preserving:

- `casa-ms` retains the read-locked source and states that its sole MAIN
  traversal occurs as selected samples are consumed
  (`crates/casa-ms/src/selected_observation/access.rs:36-40`). It emits canonical
  physical-row, channel, correlation order and admits bounded physical row
  blocking (`access.rs:203-243`).
- Its block residency is explicit: available bytes and maximum live blocks are
  authority inputs, while retained, per-row, per-block, preparation, and peak
  bytes are planned (`content_plan.rs:46-109`). The active block plus
  `VecDeque` container is charged (`content_plan.rs:263-273`).
- Block wrappers are recycled synchronously. Exhausting a block refills the
  former active wrapper on the consumer thread, `prefetch_to_capacity` fills
  additional wrappers on that same thread, and a live-block high-water counter
  is maintained (`access.rs:337-345`, `access.rs:378-429`). This is bounded
  wrapper scheduling, not producer/consumer read-ahead or proof of storage
  allocation reuse.
- The wrapper retains its slot and `row_geometry` allocation: construction
  reserves `rows_per_block`, and refill calls `clear()` before pushing new
  geometry (`access.rs:629-669`). Its `SelectedObservationBuffer` does **not**
  retain allocations. Every fill assigns `SelectedObservationBuffer::default()`
  over the old value, then constructs a replacement from newly returned vectors
  (`crates/casa-ms/src/selected_observation_buffer.rs:403-446`, `475-539`). The
  current live-block accounting is therefore a logical-residency bound, not an
  observed allocation/reuse count.
- The outward stream is scalar and owned: each buffered row/channel/correlation
  becomes a `SelectedObservationSample` (`access.rs:267-334`,
  `access.rs:431-513`). Whether that scalarization or its downstream copies are
  material on the target workload is unmeasured.
- `BoundSelectedObservation::traverse` validates and consumes the exhaustive
  canonical stream in one bounded pass and mints completion only after terminal
  validation (`bound_observation.rs:388-452`). The callback, projection, and
  consumer are synchronous (`bound_observation.rs:467-493`).
- Weighting already owns the scientific pass shape. Natural weighting performs
  its final weighting and bounded replay in one traversal
  (`crates/casa-imaging-reconstruction/src/weighting.rs:509-523`). Other
  schemes finish an exact density prepass before the terminal weighted stream
  (`weighting.rs:602-664`).
- Weighting also plans bounded replay input/output, deterministic worker
  partials, and reduction scratch (`weighting.rs:120-185`,
  `weighting.rs:220-283`). Replay input and output vectors are allocated once
  per replay phase and reused (`weighting.rs:372-397`,
  `weighting.rs:1024-1071`).
- The complete-data operator accounts for its grids, convolution cache, FFT
  residency/planning, forward workspace, products, and major-cycle model
  (`crates/casa-imaging-runtime/src/complete_data_operator.rs:37-98`), but its
  public streaming state currently consumes each weighted block synchronously
  (`complete_data_operator.rs:1080-1139`).

This means the missing capability is not another observation iterator or
another scientific runner. It is a runtime-owned executor that connects the
existing bounded source and scientific block phases while making overlap,
workers, buffer leases, and actual measurements explicit.

## Old-to-current mechanism map

| Historical/upstream mechanism | Measured or documented evidence | Current equivalent or absence | Shared owner | Decision |
| --- | --- | --- | --- | --- |
| Bounded, canonical VisibilityBuffer/row-block traversal | CASA reuses one `VisBuffer2` while looping chunks/subchunks and grids each buffer (`SynthesisImagerVi2.cc:1408-1490`). VI2 applies row blocking and advances bounded row ranges (`VisibilityIteratorImpl2.cc:2187-2215`, `2255-2264`, `2303-2329`). | `casa-ms` already owns canonical selected traversal and exact live-block budgets. | `casa-ms` | **Retain.** Expose ordered reusable blocks/leases to the runtime; do not move MS interpretation into the executor. |
| Exact one-pass natural and two-pass global density/weighted execution | Pre-cutover contract records natural streaming and exact density then weighted passes (`7fd6e9...:docs/tutorial-parity/imperformance-wave-2-experiment-summary.md:70-72`). | Reconstruction already represents this exact pass shape. Actual MS pass count is not yet a runtime measurement. | Reconstruction defines; runtime executes and counts. | **Retain and instrument.** Do not fuse away a scientific pass or hide an extra pass. |
| Residual-only later major passes | CASA separates PSF construction (`dopsf=true`) from ordinary model-degrid/residual major cycles (`dopsf=false`) (`SynthesisImagerVi2.cc:1392-1428`, `1447-1475`, `1488-1489`). The last optimized pre-cutover implementation had dedicated bounded residual-refresh construction and accumulation (`fff9c2...:crates/casa-imaging/src/execution.rs:870-969`, `7332-7388`). | Candidate `77c5a6ccf` transplanted invariant normal-state reuse into reconstruction/runtime. The one-channel screen was invalid because selected tile reads were amplified 20.66 times and dominated wall. On the realistic 64-channel, one-final-major discriminator it reduced total serial wall from 586.816469 s to 572.770956 s (2.39%) and the final-major replay from 250.704273 s to 238.037814 s (5.05%). Full-native comparison found all six product arrays bit-identical with exact inventory and metadata parity. | Reconstruction for scientific invariants; runtime for carried lifetime and residency. | **Reject as performance-insufficient.** The mechanism is scientifically valid, but its small terminal gain does not justify additional cross-cycle state and does not credibly close the frozen CASA serial gap. Commit `d49cca7d5` removed it; profile and target the remaining residual/model-prediction kernel instead. |
| Reusable grid/workspace across bounded batches | Old dirty CPU executor owned its gridder and workspace across `accumulate_batches` (`7fd6e9...:crates/casa-imaging/src/execution.rs:59-72`). libRA moves a fixed bucket to HPG then resets/reuses it (`AWVisResamplerHPG.cc:625-677`). | Weighting replay vectors and selected-observation block wrappers/row geometry are reused. Selected-observation column vectors are dropped and replaced on every fill; complete-data block handoff remains serial. | Runtime owns pool/lifetimes; `casa-ms` owns exact source-vector fill; kernel owns workspace contents. | **Adapt.** One budgeted pool of source, prepared, and output block slots; add real allocation/reuse measurements rather than inferring them from wrapper identity. |
| Row/channel run payloads instead of scalar inbox items | Historical tests found row/channel runs removed scalar synchronization; scalar direct inbox took 258.34 s wall, 1020.53 s system, and 14.4 M drains (`imperformance-wave-2-experiment-summary.md:138-143`, `155-156`). CASA and LibRA likewise preserve row/channel/correlation buffer shape through their visibility-buffer and resampler seams. | The realistic 64-channel serial profile proved that the old callback expanded 5.15 GB of logical source data into 230.58 GB of 440-byte scalar handoffs per pass. Commit `bb9c24ac2` now lends a 72-byte traversal envelope, projects the compact reconstruction record once, reuses channel/correlation spectral contributions, and advances coverage at block boundaries. Handoff fell to 37.73 GB per pass while source bytes and reads stayed fixed. The same one-worker discriminator improved from 584.974955 s to 487.939629 s (16.59%); a full-native six-product comparison was bit-identical. | `casa-ms` owns the borrowed traversal lifetime; reconstruction owns the compact retained record, scientific cache keys, and evidence; runtime transports the existing bounded block lease. | **Adapted and retained in Delivery 1.** Preserve canonical traversal, deterministic reduction, block-boundary evidence, and the same `workers=1` path. A future typed row/channel run view may remove more scalar iteration overhead, but is not an implemented production payload in this result. |
| Bounded tile inboxes, ready-work scheduling, producer/worker overlap | Old scheduler tracked queued/pending high-water bytes, producer activity, overlap, worker active time, waits, and drains (`7fd6e9...:crates/casa-imaging/src/execution.rs:2450-2529`, `2634-2681`, `2775-2816`, `2887-2995`). Largest-ready scheduling was a retained winner (`imperformance-wave-2-experiment-summary.md:141`). | No shared runtime producer/worker executor exists. | Runtime scheduler and measurements; reconstruction supplies work keys/cost and kernel. | **Adapt, do not copy API.** General bounded ready-work scheduling, admitted from resource plans, with `workers=1` on the same path. |
| Shared grid divided into disjoint sectors | Active CASA GridFT2 allocates one grid (`GridFT.cc:430-460`), prepares per-buffer coordinates in parallel (`GridFT.cc:720-816`), then lets workers update grid sectors and reduces sector sumweights in fixed index order (`GridFT.cc:850-983`). | Current complete-data owner has shared grids but serial consumption. | Reconstruction/gridding chooses partition and owns grid safety; runtime schedules work. | **Adapt.** Prefer exclusive/tiled grid ownership or bounded partials; preserve deterministic halo/overlap semantics. |
| Fixed-order reduction | CASA reduces sector sumweights in sector order (`GridFT.cc:928-930`, `976-978`). libRA gathers worker grids and sumweights in worker-index order (`MultiThreadedVisResampler.cc:326-381`). | Weighting has deterministic partials and exact reductions; complete-data multi-worker merge is absent. | Reconstruction/gridding | **Retain concept.** The kernel defines stable partition IDs and reduction order; runtime only enforces that order and records it. |
| Bounded tile cache with explicit eviction and statistics | casacore divides a lattice into a fixed number of tiles derived from cache bytes, uses LRU, flushes dirty tiles, and records hits/misses/reads/writes (`LatticeCache.h:43-77`; `LatticeCache.tcc:84-175`, `196-213`). GridFT's class contract describes paging when the grid exceeds its cache (`GridFT.h:73-106`). | Runtime plans full complete-data grids; no shared bounded partial-grid cache exists. The inspected active GridFT2 implementation allocates a full grid, so its header is mechanism evidence, not proof that this path pages. | Runtime budgets/cache lifetime; reconstruction defines tile/halo merge. | **Adapt only when the measured residency requires it.** Do not add spill or paging speculatively to the first resident MFS slice. |
| Bounded visibility bucket preserving order and spillover | libRA's HPG bucket has fixed main capacity, preserves input order, carries overflow to the next fill, and supports reset/shrink (`HPGVisBufferBucket.h:43-61`, `110-188`). HPG raises capacity to at least one VB and sends/reset batches (`AWVisResamplerHPG.cc:541-553`, `625-685`). | casa-ms has bounded ordered blocks; no generic runtime backpressure handoff. | Runtime buffer pool/queue | **Adapt.** Use lease return for backpressure rather than mode-specific spillover. |
| Conditional read-ahead with exact queue telemetry | Historical standard-MFS one-block control disabled the queue entirely and made no speedup claim (`7fd6e9...:tools/perf/imager/imaging_performance_ledger.json:967-1028`). A later 6.06 M-row mosaic MT-MFS pair improved 119.20 s to 111.82 s (6.19%) with two live blocks, 4.38 s overlap, and 92.03 s producer blocking (`...ledger.json:727-817`). | casa-ms eagerly fills admitted blocks synchronously; no overlap. | Runtime | **Gate on baseline evidence.** Add depth-two read-ahead only if source/consumer wait measurements show starvation; otherwise keep depth one through the same executor. |
| Prepared-artifact look-ahead coordinated with compute | libRA asynchronously prepares the next convolution-function set while the main thread grids, with explicit ready/sent coordination (`DataIterations.h:180-211`; `roadrunner.cc:677-800`). | Current runtime owns prepared artifacts but the first standard-MFS slice does not need CF paging. | Runtime, if a later kernel declares the artifact dependency. | **Reject for this slice.** The coordination pattern is reusable; the AW/HPG-specific server is not. |
| Per-worker full grids with row-range scatter and a barrier per VB | libRA scatters VB row ranges by reference (`MultiThreadedVisResampler.cc:310-321`), but creates/copies full grids per worker (`MultiThreadedVisResampler.cc:188-211`, `385-445`) and waits for all workers on every VB (`453-505`). | No current multi-worker complete-data path. | None for the full-grid design | **Reject.** Retain only borrowed row-range partitioning, fixed-order merge, and stage timing. Never allocate a full grid per worker or force a per-block global barrier unless the measured dependency requires it. |
| Mapper-outer traversal to cap image memory | CASA has a separate mapper-outer major cycle specifically for images large relative to RAM when data I/O is minor (`SynthesisImagerVi2.cc:1523-1525`, `1582-1669`). | Current architecture requires one production engine and bounded streams. | None | **Reject for the first seam.** It multiplies source traversals and is an explicit alternate execution architecture. If future memory evidence demands decomposition, express it as a runtime plan under the shared seam. |
| Old standard-MFS-specific memory planner and whole-run executor | Old plan charged source/prepared bytes, worker scratch, live row blocks, resident tiles, and queue entries (`7fd6e9...:crates/casa-imaging/src/execution_plan.rs:17-75`, `228-250`, `747-785`, `872-900`). It also accumulated backend and application decisions. | ADR-0010 and current runtime are the accepted resource authority; the cutover deleted the old runner. | Runtime for physical facts only | **Extract formulas and measurements; reject the owner/API.** No compatibility fallback or mode-specific planner. |

Historical timing results are workload- and implementation-specific. The most
relevant retained old CPU results are a 512-channel, 2048-pixel Briggs run at
328.937 s frontend/10.27 GB peak with saved-CASA product parity, and a
64-channel, 1024-pixel Briggs four-worker run at 97.81 s frontend/9.55 GB peak
(`imperformance-wave-2-experiment-summary.md:106-120`). A later paired 1k run
reported CASA 733.660 s versus casa-rs 41.790 s, but the same record explicitly
rejects an earlier faster result for scientific divergence
(`imperformance-wave-2-experiment-summary.md:122-130`). These results justify
re-testing bounded runs, tile ownership, and deterministic merge; they do not
justify restoring the old executor or claiming current speed.

The old negative results are equally important: deeper queues slowed the heavy
case to 118.99 s; a roughly 6.10 GB metadata cache was noisy; broad MS storage
read and specialized packing were not dominant on that workload
(`imperformance-wave-2-experiment-summary.md:148-167`). Therefore read-ahead,
packing, and caching remain hypotheses until current stage timing and wait data
support them.

## Exact source-buffer reuse seam

The public `VisibilityBuffer` is not a correct direct replacement for the
private selected-observation buffer:

- The private buffer is the closed compiler-owned column contract, including
  `FEED1`, `FEED2`, `TIME_CENTROID`, exact visibility/weight variants, and the
  other selection/provenance scalars
  (`crates/casa-ms/src/selected_observation_buffer.rs:69-115`). The public
  `VisibilityBuffer` has a different, optional analysis/probe surface and omits
  parts of that contract (`crates/casa-ms/src/visibility_buffer.rs:272-317`).
- The public fill explicitly drops `DATA`, `FLAG`, and `WEIGHT_SPECTRUM` before
  reading (`visibility_buffer.rs:598-612`). Its channel readers then obtain new
  typed blocks and move newly allocated vectors into the buffer
  (`visibility_buffer.rs:1154-1271`). Row indices, row weights, UVW, and selected
  scalar vectors do retain or receive their prior vectors for possible reuse
  (`visibility_buffer.rs:325-331`, `613-704`, `1274-1293`).
- Its allocation report compares only capacity values before and after fill and
  labels a buffer reused when the old capacity is at least the new capacity
  (`visibility_buffer.rs:1816-1907`). For channel arrays this can report
  “reused” after the old allocation was dropped and an unrelated equal-or-smaller
  allocation was installed. That report is not allocation-identity evidence.

The mechanism to adapt is the private exact selected-observation fill, not the
public probe API. `fill_selected_observation_buffer` already enforces the closed
column set and forbids whole-cell fallback
(`selected_observation_buffer.rs:403-415`, `565-619`). Beneath it, casa-tables
already separates allocation from typed filling: the tiled reader allocates a
typed destination and then calls cores that fill caller-owned mutable slices
(`crates/casa-tables/src/storage/tiled_stman.rs:3119-3203`). The decode core
accepts `&mut [T]` (`tiled_stman.rs:3277-3293`), as do the direct-copy 2-D and
1-D cores (`tiled_stman.rs:3442-3456`, `2880-2885`, `2959-2965`).

Adapt that existing core with private typed `*_into` entry points through
casa-tables and the private `SelectedObservationBuffer` fill. They should resize
and fill the exact type-compatible vectors already owned by a leased block,
replace only on insufficient capacity or type/shape change, and report those
events explicitly. Keep this API private to the selected-observation owner; do
not widen the public `VisibilityBuffer`, duplicate selection semantics, or make
the runtime understand MS columns. The runtime may own the block lease and
budget, but `casa-ms` must remain the only component that fills and validates
its source storage.

## Proposed shared execution contract

The contract should be expressed as capabilities rather than an old MFS runner:

1. **Ordered block source, owned by `casa-ms`.** It binds one selected
   observation and yields reusable block leases in compiler order. Its private
   exact fill path uses typed `*_into` operations to refill the lease's retained
   vectors. The lease carries stable traversal/block ordinals and exact logical
   source bytes. The source alone interprets MeasurementSet rows, columns,
   flags, coordinates, selection, and completion.
2. **Physical execution plan, owned by `casa-imaging-runtime`.** It admits exact
   live source/prepared/result slots, per-worker scratch, bounded partial state,
   queue depth, read-ahead depth, workers, and reduction scratch under one
   resource lease. No dataset identity or mode-specific threshold selects the
   plan.
3. **Bounded executor, owned by `casa-imaging-runtime`.** A producer obtains an
   empty lease, asks `casa-ms` to fill it, and publishes it to a bounded queue.
   Workers borrow published blocks and kernel scratch. Returning the lease is
   the backpressure signal. Depth one and `workers = 1` remain the same state
   machine, with no thread/queue overhead required when the plan proves only one
   block can exist.
4. **Streaming scientific kernel, owned by reconstruction/gridding.** It
   converts a source block into kernel work, applies flags/weights/operator
   semantics, declares bounded partial state, and exposes stable reduction
   keys. It—not the runtime—defines tile ownership, halos, accumulation
   precision, and deterministic merge.
5. **Deterministic bounded reduction.** Runtime schedules ready work but commits
   partials only in the kernel's stable order. A worker may directly update an
   exclusively owned resident tile; otherwise it emits a bounded tile/sector
   partial. Neither case permits a complete grid per worker.
6. **Composition only above the seam.** The application requests the plan,
   connects source/kernel/sink, and publishes receipts. Frontends do not read
   the MS, build scientific buffers, schedule workers, or merge grids.

This follows the accepted architecture: selected observation bulk remains a
bounded stream (`docs/adr/0009-mathematical-imaging-architecture.md:132`), while
ADR-0010 assigns buffers, workers, queues, leases, and receipts to the unified
resource authority (`docs/adr/0010-unified-imaging-resource-authority.md:67-78`,
`121-148`). ADR-0010 also explicitly treats a duplicated full grid per worker
as an architecture violation (`docs/adr/0010-unified-imaging-resource-authority.md:182-198`).

## Measurement contract before optimization

The instrumentation commit should leave scientific output unchanged and record
at least:

- logical selected-observation traversals and physical MAIN reads, separately;
- row blocks, selected rows/samples, raw column bytes, logical selected bytes,
  and bytes copied at each named handoff;
- allocation count, reuse count, capacity, current residency, and high-water
  residency for source, prepared, weighted, worker scratch, partial, reduction,
  grid, FFT, and product buffers;
- stage wall/CPU time for source read, source projection, weighting density,
  weighting finalization, preparation/conversion, queue wait, gridding,
  deterministic reduction, FFT/scientific completion, and writing;
- producer active/blocked time, consumer receive wait, overlap, queue
  high-water blocks/bytes, and lease-return latency;
- per-worker work units, samples/taps, active time, ready wait, backpressure
  wait, reduction time, and stable partition/reduction IDs;
- planned versus actual workers, queue depth, live blocks, scratch, partial
  bytes, and peak resident bytes.

The matched baseline should freeze parameters, selected row/channel/correlation
content, CASA version/commit, machine/storage location, product comparison
thresholds, and warm/cold-cache policy. Record `workers = 1` first, then the
same path with the admitted worker count. Run the focused workload on the
medium dataset and the directly mounted 32 GB dataset before retaining
read-ahead, multiple workers, or a new partial-grid representation.

## Searches and commands used

All commands were read-only except creation of this note. Representative exact
commands:

```sh
git cat-file -t 7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40
git cat-file -t fff9c2d553eace4b6a57b1df9ded4773f2263ceb
git cat-file -t 009745a080c90c92bffb07de2603136852
git show -s --format='%H %cs %s' <revision>
git show --stat --oneline --summary fff9c2d553eace4b6a57b1df9ded4773f2263ceb
git ls-tree -r --name-only <revision> | rg 'casa-(ms|imaging)'

git show 7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40:docs/tutorial-parity/imperformance-wave-2-experiment-summary.md | nl -ba
git show 7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40:tools/perf/imager/imaging_performance_ledger.json | nl -ba
git show 7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40:crates/casa-imaging/src/execution.rs | nl -ba
git show 7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40:crates/casa-imaging/src/execution_plan.rs | nl -ba

rg -n 'BoundObservation|maximum_live_blocks|prefetch|replay|Residency|consume_weighted_block' crates/casa-ms crates/casa-imaging-reconstruction crates/casa-imaging-runtime
nl -ba crates/casa-ms/src/selected_observation/access.rs
nl -ba crates/casa-ms/src/selected_observation_buffer.rs
nl -ba crates/casa-ms/src/visibility_buffer.rs
nl -ba crates/casa-ms/src/selected_observation/content_plan.rs
nl -ba crates/casa-ms/src/selected_observation/bound_observation.rs
nl -ba crates/casa-imaging-reconstruction/src/weighting.rs
nl -ba crates/casa-imaging-runtime/src/complete_data_operator.rs
nl -ba crates/casa-tables/src/storage/tiled_stman.rs

git -C /Users/brianglendenning/SoftwareProjects/casa show HEAD:casatools/src/code/synthesis/ImagerObjects/SynthesisImagerVi2.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/casa show HEAD:casatools/src/code/msvis/MSVis/VisibilityIteratorImpl2.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/casa show HEAD:casatools/src/code/synthesis/TransformMachines2/GridFT.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/casacore show HEAD:lattices/Lattices/LatticeCache.tcc | nl -ba

git -C /Users/brianglendenning/SoftwareProjects/libRA show HEAD:src/libracore/DataIterations.h | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA show HEAD:src/synthesis/TransformMachines2/HPGVisBufferBucket.h | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA show HEAD:src/synthesis/TransformMachines2/AWVisResamplerHPG.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA show HEAD:src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc | nl -ba
```

Search terms included `row block`, `VisBuffer`, `originChunks`, `prefetch`,
`read_ahead`, `bucket`, `spillover`, `resident tile`, `queue high water`,
`producer`, `worker`, `scatter`, `gather`, `sumwt`, `LatticeCache`, and
`deterministic`.
