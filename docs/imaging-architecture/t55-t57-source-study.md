# T55/T57 CPU, Metal, CASA, and LibRA source inventory

Truth class: primary-source implementation-boundary research  
Last reality check: 2026-09-08  
Verification: source inspection only; no imaging run or benchmark was performed

This is a bounded inventory of mechanisms relevant to T55 and T57. It records
what the pinned sources do and the observable tradeoffs. It intentionally does
not choose a T55/T57 architecture, prescribe a CPU/Metal partition, or claim
that either ticket depends on the other.

## Pinned revisions and access limits

- casa-rs: [`fff9c2d553eace4b6a57b1df9ded4773f2263ceb`](https://github.com/bglenden/casa-rs/tree/fff9c2d553eace4b6a57b1df9ded4773f2263ceb), the native-continuum cutover and requested pre-cutover counterpart.
- CASA 6: [`61020062cee290f5466cffed5ec5032e0c7a3434`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/commits/61020062cee290f5466cffed5ec5032e0c7a3434).
- LibRA: [`0ab99e261878334d6588eafa360cef3b673e897f`](https://github.com/bglenden/libRA/tree/0ab99e261878334d6588eafa360cef3b673e897f).
- HPG: [`79667f64746425eb3cf34c3b83081e1af19df3db`](https://gitlab.nrao.edu/mpokorny/hpg/-/tree/79667f64746425eb3cf34c3b83081e1af19df3db), the HPG dependency revision used by the LibRA installation.

CASA, LibRA, and HPG are implementation/science oracles, not casa-rs public
API or persistence contracts. Historical source mechanisms are not current
performance evidence. The current CPU path's fixed `4 + 4` lane detail is a
local execution fact and is not evidence that CPU physical partitions map
one-to-one to Metal workgroups. No ticket sequencing, acceptance, or readiness
claim follows from this inventory.

## casa-rs pre-cutover mechanisms

| Source | Observed mechanism | Observable tradeoff or limit |
| --- | --- | --- |
| [`parallel_worker.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/parallel_worker.rs#L11-L347), lines 11-73, 75-121 | Validates a hard duration/candidate budget; evaluates at most four topology candidates; uses an LPT-style estimate; runs adjacent/counterbalanced trials and computes timing/confidence statistics. | Bounded planner experiments keep tuning outside the kernel, but they do not establish a production topology or scientific equivalence. |
| [`execution_plan.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/execution_plan.rs#L13-L96), lines 13-96, 744-902, 1496-2045 | Carries workload facts and byte candidates, CPU/Metal resource facts, ingest/FFT/tile/spectral/cache plans, worker bounds, queue capacities, Metal eligibility, allocation decisions, and resolved-stage data. | Feasibility/accounting are explicit planner concerns; the old file also combined several ownership domains and is not itself a current API. |
| [`execution.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/execution.rs#L126-L350), lines 126-350 | Represents fixed tiles, halos, owners, and resident bytes. | Tile ownership bounds grid residency, but the source does not by itself establish that one tile shape is best for every backend or workload. |
| [`execution.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/execution.rs#L1669-L1682), lines 1669-1682; lines 4543-4637 | CPU executor consumes tile buckets and performs bounded accumulation. | CPU scheduling and accumulation are coupled to the historical executor; no GPU-neutral physical partition follows from this path. |
| [`execution.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/execution.rs#L8982-L9082), lines 8982-9082, 9289-9317 | Metal executor consumes tile buckets; packing records centers/taps, flags, weights, and visibility. | The paired CPU/Metal consumption is useful evidence for a shared prepared representation, but not evidence that all backend scheduling or lane assignments must match. |
| [`gridder.rs`](https://github.com/bglenden/casa-rs/blob/fff9c2d553eace4b6a57b1df9ded4773f2263ceb/crates/casa-imaging/src/gridder.rs#L322-L720), lines 322-443, 540-720, 1185-1312 | One gridder owner computes geometry, kernel/normalized weights, positive taps/centers, CPU grid, and CPU degrid operations. | Keeping geometry/tap semantics in one owner avoids backend drift; precision and execution layout still require a separate Numerics Contract. |

## CASA parallel control and partition boundaries

| Source | Observed mechanism | Observable tradeoff or limit |
| --- | --- | --- |
| [`ParallelImagerFactory.cc`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/ImagerObjects/ParallelImagerFactory.cc?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 35-113 | Separates the interactive front end from a worker group. Serial mode uses one worker. MFS duplicates worker communication for imaging/normalization/deconvolution and uses a task communicator for iteration; cube uses `MPI_COMM_SELF` for component processes and a task communicator for iteration. | Communicator roles make global stage/lifecycle boundaries visible, but they are MPI/deployment mechanics, not a Rust work-unit or persistence contract. |
| [`ParallelImagerMixin.h`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/ImagerObjects/ParallelImagerMixin.h?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 48-191 and 212-235 | Keeps one high-level algorithm with communicator roles/overlap, setup barriers, and explicit lifecycle operations. | A density/normalization stage boundary is synchronized at the control layer; the source does not imply that every row partition must share a global barrier. |
| [`ContinuumPartitionMixin.h`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/ImagerObjects/ContinuumPartitionMixin.h?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 41-145 | Derives rank/size and per-rank continuum partition/image/cache suffixes. | Row partitioning is independent of image definition, while rank-specific names and cache identity are deployment-specific. |
| [`CubePartitionMixin.h`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/ImagerObjects/CubePartitionMixin.h?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 41-93, 105-169, 171-236 | Uses rank/barrier/round-robin concatenation, cube partitioning, channel selection, and `.n<rank>` handling. | Cube channel/selection boundaries are stricter than arbitrary row splitting; this path has a rank-specific product layout that is not itself an interoperable casa-rs format. |
| [`Applicator.h`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/Parallel/Applicator.h?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 88-133; [`Algorithm.h`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/Parallel/Algorithm.h?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 94-112; [`Applicator.cc`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/Parallel/Applicator.cc?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 259-369 | Exposes statuses/task operations, assigns tasks to free workers, and marks workers free on completion. | Dynamic assignment can hide worker imbalance, but completion order is not a numerical reduction order. |
| [`GridFT.cc`](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/TransformMachines/GridFT.cc?at=61020062cee290f5466cffed5ec5032e0c7a3434), lines 839-999, 1080-1135 | The pinned source chooses `2x2` sectors when `nth > 3`, `2x1` when `nth > 1`, otherwise `1x1`; grids sector bounds into shared grid storage, tracks per-sector sumweights, and merges sumweights in fixed sector index order. | Sector ownership and fixed-index sumweight merge are useful mechanisms, not a universal tile formula or deterministic cross-backend guarantee. The local worktree has unrelated GridFT instrumentation; these observations use `git show` of the pinned revision, not that modified file. |

## LibRA and HPG mechanisms

| Source | Observed mechanism | Observable tradeoff or limit |
| --- | --- | --- |
| LibRA [`AWVisResamplerHPG.cc`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/AWVisResamplerHPG.cc), lines 100-282, 499-763 | Initializes Mueller/batch/create/failure mappings; maps `HPGDEVICE`; sends partial/spillover work with fences, FFT, and gather; moves/releases CFs; sizes buckets to at least one visibility buffer; reloads CFs; shrinks/moves buckets; grids/degrids, resets, blocks, fills, and sends. | CF state, bounded batches, paired grid/degrid paths, and explicit completion are coupled in one implementation. Reusing the lifetime ideas does not make this backend object graph or device policy portable. |
| Same file, lines 770-926 | Degrid list/fence/copy paths and fenced pointer access. | Host access and storage reuse are completion-sensitive; a copy or pointer read before the fence changes the lifetime requirement. |
| LibRA [`HPGVisBuffer.inc`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/HPGVisBuffer.inc), lines 89-316 | Stores CF extents on device and flattens row/channel/polarization data for visibility, flag, weight, UVW, dphase, and CF indexing. | A contiguous indexed prepared view reduces repeated column interpretation; the exact HPG layout is an implementation detail. |
| LibRA [`HPGVisBufferBucket.h`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/HPGVisBufferBucket.h), lines 42-52, 110-188 | Keeps a main bucket and ordered spillover; supports resize, reset, shrink, move, and append. | Main-bucket capacity alone does not bound retained spillover. A casa-rs implementation must explicitly cap and charge both, including copies and fence-delayed reuse. |
| LibRA [`MultiThreadedVisResampler.cc`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc), lines 173-211, 310-381, 453-505 | Allocates per-thread full grids, scatters row ranges, gathers in worker-index order, dispatches, and waits per visibility buffer. | Ordered gather is explicit; full grids and per-buffer waits increase memory and synchronization. This is evidence of one implementation's tradeoff, not a neutral layout. |
| LibRA [`roadrunner.cc`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/apps/src/RoadRunner/roadrunner.cc), lines 675-817; [`LibHPG.cc`](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/apps/src/RoadRunner/LibHPG.cc), lines 33-127 | Coordinates CF look-ahead with ready/sent/end-of-data states; selects OpenMP/CUDA/serial HPG backends and initializes once. | Look-ahead is an explicit dependency protocol. Environment/backend choice and one-time backend initialization are not proof of a portable scientific execution contract. |
| HPG [`hpg.hpp`](https://gitlab.nrao.edu/mpokorny/hpg/-/blob/79667f64746425eb3cf34c3b83081e1af19df3db/include/hpg/hpg.hpp), lines 1026-1111, 1246-1335, 3072-3144 | Exposes async state, max tasks/batch/per-task memory, CF preallocation/reallocation, output/fence operations, and a pointer-synchronization warning. | Admission limits, allocation lifetime, and fences must agree. The API exposes constraints, not a casa-rs ownership recommendation. |
| HPG [`runtime.hpp`](https://gitlab.nrao.edu/mpokorny/hpg/-/blob/79667f64746425eb3cf34c3b83081e1af19df3db/include/hpg/runtime.hpp), lines 87-126, 331-633, 1100-1126, 1372-1491, 1750-1818, 2010-2099 | Defines runtime state, CF pools, aliased visibility/grid buffers, fence publication, kernels/copies, output fences, execution-space fences, and per-task allocation. | Shared aliases reduce copies only while fence and slot generations are respected; device/host visibility is not implicit. |
| HPG [`gridding.hpp`](https://gitlab.nrao.edu/mpokorny/hpg/-/blob/79667f64746425eb3cf34c3b83081e1af19df3db/include/hpg/gridding.hpp), lines 257-291, 592-681, 772-934, 1236-1266 | Uses pseudo-atomic complex adds, team reductions, atomics, one-team-per-visibility options, and reductions. | Atomics solve overlapping writes for a backend, but their presence does not establish deterministic cross-backend accumulation or bounded scientific reduction. |

## Scope limits and reproduction

The observations above do not settle whether a future implementation uses
exclusive tiles, partial grids, row ranges, spectral slabs, or a different
CPU/Metal decomposition. They also do not settle precision, normalization,
cache retention, CF look-ahead, or the acceptance workload. Those are separate
design and correctness questions.

```sh
git show fff9c2d553eace4b6a57b1df9ded4773f2263ceb:crates/casa-imaging/src/parallel_worker.rs | nl -ba
git show fff9c2d553eace4b6a57b1df9ded4773f2263ceb:crates/casa-imaging/src/execution_plan.rs | nl -ba
git show fff9c2d553eace4b6a57b1df9ded4773f2263ceb:crates/casa-imaging/src/execution.rs | nl -ba
git show fff9c2d553eace4b6a57b1df9ded4773f2263ceb:crates/casa-imaging/src/gridder.rs | nl -ba

git -C /Users/brianglendenning/SoftwareProjects/casa show 61020062cee290f5466cffed5ec5032e0c7a3434:casatools/src/code/synthesis/ImagerObjects/ParallelImagerFactory.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/casa show 61020062cee290f5466cffed5ec5032e0c7a3434:casatools/src/code/synthesis/TransformMachines/GridFT.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA show 0ab99e261878334d6588eafa360cef3b673e897f:src/synthesis/TransformMachines2/AWVisResamplerHPG.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA show 0ab99e261878334d6588eafa360cef3b673e897f:src/synthesis/MeasurementComponents/MultiThreadedVisResampler.cc | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA/dependencies/HPG show 79667f64746425eb3cf34c3b83081e1af19df3db:include/hpg/hpg.hpp | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA/dependencies/HPG show 79667f64746425eb3cf34c3b83081e1af19df3db:include/hpg/runtime.hpp | nl -ba
git -C /Users/brianglendenning/SoftwareProjects/libRA/dependencies/HPG show 79667f64746425eb3cf34c3b83081e1af19df3db:include/hpg/gridding.hpp | nl -ba
```
