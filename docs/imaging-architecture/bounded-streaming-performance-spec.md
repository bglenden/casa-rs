# Bounded streaming imaging performance specification

Truth class: approved implementation handoff
Date: 2026-08-27
Parent programme: #486
Delivery 1 work issue: #540
Delivery 2 source issue: #541, with the MFS production slice to be extracted
Verification: issue-named focused gates; `just docs-check`; `just arch-check`

## Decision

Imaging execution gains one shared bounded streaming Module inside
`casa-imaging-runtime`. Its private Interface connects an ordered reusable
`casa-ms` block source to a reconstruction-owned partitioned scientific kernel:

```text
casa-ms ordered selected-observation source
    -> casa-imaging-runtime bounded executor
       (plan, leases, reusable pools, read-ahead, backpressure,
        workers, cancellation, deterministic commit, measurements)
    -> reconstruction/gridding kernel
       (passes, preparation, partitions, accumulation, reduction, completion)
```

The existing application `compile -> plan -> run` Interface remains the sole
production entry. The application composes owners and publishes their results;
frontends provide parameters and unit conversion only. There is no second
runner, public general-purpose executor, compatibility facade, or fallback.

Delivery 1 deepens #540 with the complete physical streaming seam, real source
storage reuse, bounded depth-two producer/consumer overlap, generic worker
scheduling, and the complete-data MFS caller migrated through the same executor
with `workers = 1`. Delivery 2 extracts the complete-data MFS portion of #541
and adds production multi-worker scientific partitions without changing the
executor Interface.

This implements ADR-0009 and ADR-0010. It does not amend either decision.

## Evidence that fixes the design

### Frozen CASA oracle

The retained Wave 2 paired workload is the correctness and CASA performance
oracle: 64 channels, 1024-pixel image, 0.25 arcsec cell, Briggs weighting, and
`niter=500` on the medium VLA MeasurementSet. CASA took 733.660 seconds. The
CASA run used `parallel=False`, so this timing is the single-process serial
anchor rather than an accelerated reference. The matched casa-rs `workers = 1`
production path must independently take no longer than this anchor before any
multi-worker or device result can count as a performance success. Acceleration
may demonstrate scaling, but it may not compensate for or conceal a serial
miss. The retained product RMS ratios were `1.72e-5` for image, `3.53e-5` for
residual,
`1.02e-5` for model, and `3.04e-5` for PSF. The corresponding historical
casa-rs timing is not a current-main baseline.

CASA is not rerun for these deliveries. The saved products and timing remain
frozen unless the oracle, workload, or comparator contract changes.

### Source and mechanism study

[`imaging-performance-source-study.md`](imaging-performance-source-study.md)
records the inspected current path, pre-cutover casa-rs commits
`7fd6e9d422b0a76ab34fc8cd820b93e0fb6b3a40` and
`fff9c2d553eace4b6a57b1df9ded4773f2263ceb`, transition commit
`009745a080c90c92bffb07de2603136852ab0630`, and the corresponding CASA,
casacore, and libRA mechanisms. This specification adopts their bounded block,
buffer reuse, overlap, exclusive-region, and fixed-order reduction mechanisms;
it rejects the displaced whole-run owner, full grids per worker, per-buffer
global barriers, and mode-specific execution paths.

### Real-shape representation probe

A disposable probe used a real 256-row block from the mounted 106.9 GiB ALMA
MeasurementSet: 1,024 channels, two correlations, and 524,288 samples. All
representations produced matching digests and allocated no bytes while
consuming the captured block.

| Representation | In-memory preparation |
|---|---:|
| owned scalar | 4.719349 ns/sample |
| borrowed scalar | 4.719347 ns/sample |
| borrowed row/channel runs | 3.372700 ns/sample |

Borrowing a scalar did not help. Preserving run-shaped access reduced the
representative preparation time by 28.5 percent. The hot source/kernel payload
is therefore an opaque borrowed block view with row/channel runs or typed
slices. A private scalar iterator may be a convenience, but it is not runtime
transport.

### Complete 106.9 GiB serial diagnosis

Three separate bounded-memory traversals read every row of
`wave1-alma-mosaic-large.ms`: 6,709,290 rows, 6,553 probe blocks, and
13,740,625,920 delivered samples. Every traversal produced identical digest
bits and completed near 153 MB peak RSS. The accepted serial checksum is
12,412,252,160 accepted samples, real-sum bits `423bdb13113a47c7`,
imaginary-sum bits `c1c2e4fdfd889d2d`, and weight-sum bits
`420721a68210e413`.

| Representation | Wall | Source fill | Consume | Net on-disk rate |
|---|---:|---:|---:|---:|
| owned scalar | 160.383 s | 96.441 s | 63.928 s | 0.667 GiB/s |
| borrowed scalar | 179.509 s | 106.592 s | 72.895 s | 0.596 GiB/s |
| borrowed row/channel runs | 170.566 s | 112.429 s | 58.116 s | 0.627 GiB/s |

These are single observations, so their total-rate ordering is provisional.
Their stage shape is decisive: wall time is effectively source fill plus
consumption, with no useful overlap. A two-lease producer/consumer pipeline has
measured headroom of roughly 1.5 to 1.7 times before refill improvements. This
evidence selects depth-two overlap whenever the resource plan admits two live
source slots.

The probe used the public `VisibilityBuffer` only to obtain real stored shapes.
It does not prove private selected-observation allocation identity. The private
exact selected-observation path must provide that evidence in Delivery 1.

### Rejected invariant-normal-state candidate

Commit `77c5a6ccf` tested one causal hypothesis against parent `e30f59302`:
carry invariant PSF, sensitivity, sum weights, and validity across later major
cycles so each replay grids only the changing residual. An initial one-channel
turnaround showed no terminal improvement, but that screen was not a valid
discriminator for channel-scaled compute: its selected DATA tile reads were
amplified 20.66 times and the run was source-I/O dominated. The candidate was
therefore re-evaluated on the realistic 64-channel shape before making a
retention decision.

The serial discriminator used the matched 32 GiB MeasurementSet and CASA
geometry with 64 channels, `niter=1`, and `nmajor=1`. It performs one density
pass, one initial weighted replay, and one final-major weighted replay while
isolating exactly one later major cycle. Parent and candidate took 586.816469
seconds and 572.770956 seconds respectively: a 14.045513-second or 2.39 percent
terminal improvement. The final-major replay fell from 250.704273 seconds to
238.037814 seconds, a 12.666459-second or 5.05 percent improvement; the density
pass rose from 101.805694 seconds to 102.678155 seconds.

A full-native comparator then read every element of the six emitted 1024-pixel
products. Image, residual, model, PSF, mask, and sum-weight arrays were
bit-identical (`diff_rms=0`, `diff_abs_max=0`), with exact inventory and metadata
parity. The mechanism is scientifically valid, but its small terminal gain does
not justify carrying additional cross-cycle state, validation, and residency,
and it does not clear a credible threshold for the frozen 733.660-second CASA
gate. Commit `d49cca7d5` removed the candidate. Invariant-normal-state reuse is
retained as source evidence, not production complexity; the next serial
candidate must target the residual/model-prediction kernel that still consumes
238 seconds in the realistic final-major replay.

### Selected scalar-handoff candidate

An unchanged release binary at commit `6666883ec` was sampled for 20 seconds in
each phase of the same 64-channel, `niter=1`, `nmajor=1`, serial CPU
discriminator. Rust completed in 584.974955 seconds. Density, initial weighted
replay, and final-major replay took 102.125557, 231.761949, and 248.643193
seconds respectively, while their direct source reads took only 10.329587,
10.582272, and 10.708021 seconds. Every pass traversed 4,094,064 rows and
524,040,192 selected samples in 188 bounded blocks with one worker.

The current scalar callback reports 230,577,684,480 semantic handoff bytes per
pass from 5,146,238,448 logical source bytes. That is a 44.80-times expansion
and 440 bytes per selected sample. In the final-major sample, SHA-256 was 18.7
percent of main-thread leaf samples, record movement was 13.7 percent, gridding
was 10.4 percent, and the remaining leaders included repeated sample assembly,
spectral-stencil construction, generation inspection, coverage encoding, and
visibility-address encoding. Source I/O was not the limiting stage.

This selects one replacement candidate before implementation: `casa-ms` keeps
canonical ordered traversal and lends each selected sample through a bounded
borrowed envelope whose lifetime cannot escape the live source block. The
shared runtime transports the same leased block through its one bounded
executor. Reconstruction immediately projects only the fields its kernel
retains, reuses spectral contributions across correlations in the same native
channel, and advances coverage evidence at bounded block boundaries.
`workers=1` remains this exact code path. The candidate must not add an MFS-only
route, materialize selected data, weaken deterministic reduction, or move MS
interpretation out of `casa-ms`.

### Accepted compact serial-handoff result

Commit `bb9c24ac2` implements that candidate without changing the source pass
shape or adding another execution route. The borrowed traversal envelope is 72
bytes, while reconstruction's retained weighting record contains only the
address, visibility, flags, input weight, density/transformed UVW, and phase
shift required by the scientific kernel. Spectral-contribution compilation is
cached by MeasurementSet, field, spectral window, channel, native interval,
and output interval; correlations in the same channel reuse the compiled
stencil. Coverage checkpoints move privately with each immutable replay block,
so the complete-data owner adopts the checkpoint once per block rather than
rehashing each sample. Final visibility identity hashes the ordered address
stream once and keeps separate model and residual value digests.

On the realistic serial 64-channel, `niter=1`, `nmajor=1` discriminator, the
unchanged `6666883ec` control took 584.974955 seconds and `bb9c24ac2` took
487.939629 seconds: 97.035326 seconds or 16.59 percent faster. Both runs used
one worker and exactly one density pass, one initial weighted replay, and one
final-major replay. Every pass traversed the same 188 blocks, 4,094,064 rows,
and 524,040,192 samples.

| Serial stage | Control | Candidate | Improvement |
| --- | ---: | ---: | ---: |
| Density | 102.125557 s | 84.753683 s | 17.01% |
| Initial weighted replay | 231.761949 s | 195.891281 s | 15.48% |
| Final-major replay | 248.643193 s | 204.455013 s | 17.77% |

Logical source bytes remained exactly 5,146,238,448 per pass. Semantic handoff
fell from 230,577,684,480 to 37,730,893,824 bytes per pass, or from 440 to 72
bytes per sample: an 83.64 percent reduction. Direct source-read time remained
effectively unchanged at 10.29-10.50 seconds in the candidate versus
10.33-10.71 seconds in the control. The terminal gain is therefore a serial
kernel/handoff improvement, not a disk-cache or worker-count effect.

The full-native comparator then visited all 5,242,881 stored elements across
image, residual, model, PSF, mask, and sum-weight products. Every array had
`diff_rms=0` and `diff_abs_max=0`, with exact product inventory, metadata,
pixel-mask topology, and finite-value topology. This is one provisional
control/candidate observation; it proves the candidate's causal effect and
scientific identity but not a repeatable timing distribution. The frozen
CASA-anchored `niter=500` serial workload remains the final performance and
CASA-parity gate.

## Scope

Delivery 1 includes:

- typed current-path instrumentation with no scientific-output change;
- private caller-owned `casa-tables` fill operations used by `casa-ms`;
- exact in-place reuse of selected-observation storage;
- the shared bounded executor and immutable physical stream plan;
- one-slot and two-slot execution through the same state machine;
- a two-lease producer/consumer pipeline whenever admitted;
- generic worker leases, scheduling, cancellation, adaptation, and deterministic
  commit coordination independent of operator-specific parallel kernels;
- complete-data constant-basis MFS migration through the executor with
  `workers = 1`;
- deletion of the displaced production MFS streaming loop; and
- focused, medium, 32 GiB, and 106.9 GiB evidence defined below.

Delivery 2 includes:

- stable MFS scientific partitions independent of worker count;
- exclusive tile/sector ownership or bounded ordered partials;
- deterministic MFS reduction through the unchanged executor;
- production MFS execution with `workers > 1`; and
- serial/multicore product equivalence and performance evidence.

## Non-goals

Neither delivery may:

- materialize a MeasurementSet or selected row/sample corpus;
- allocate a complete grid per worker;
- add an MFS-only executor, planner, queue, or fast path;
- restore the pre-cutover whole-run imager or its dependency direction;
- add a compatibility shim, fallback, alternate implementation, or retry route;
- make runtime interpret MeasurementSet columns or scientific semantics;
- make applications or frontends calculate budgets, blocks, workers, tiles, or
  reductions;
- change a CASA-interoperable persisted format;
- change a versioned receipt schema without a separate explicit approval; or
- implement cube, mosaic, W/AW-projection, MT-MFS, or device-specific kernels.

## Ownership and Interface

### `casa-ms`: ordered source owner

`casa-ms` alone owns selection, source order, MeasurementSet columns, DDID and
predicate handling, pointing/geometry evaluation, source errors, and traversal
completion. It opens one canonical ordered traversal for each science-declared
pass and fills opaque source storage supplied under a runtime lease.

The private exact fill path uses typed caller-owned `casa-tables` operations to
resize and fill compatible vectors in place. Replacement is legal only for a
planned capacity, type, or shape transition and is measured explicitly. The
public `VisibilityBuffer` is not the executor Interface.

The block view exposes borrowed row/channel runs or typed slices while keeping
the closed column set private. It carries stable source, pass, and block
ordinals plus exact logical bytes. Source completion is minted only after the
terminal poll and successful ordered consumption of every emitted block.

### `casa-imaging-runtime`: bounded executor owner

The executor is a deep private Module beneath the existing production run path.
Its shape is:

```rust
execute_bounded(context, source, kernel) -> bounded outcome
```

The internal `OrderedBlockSource` Adapter supplies opaque refillable storage,
ordered fill outcomes, and source completion. The internal
`PartitionedKernel` Adapter supplies preparation, stable work, worker
execution, deterministic commit, and scientific completion. These are sealed
workspace implementation Interfaces, not external extension contracts.

Runtime owns source, prepared, scratch, and partial pools; lease epochs;
backpressure; read-ahead; worker lifecycle; ready-work selection; cancellation;
bounded out-of-order completion; stable commit sequencing; and measurements.
It sees source storage size and lifecycle, not MeasurementSet column meaning.

### Reconstruction and gridding: scientific owner

The scientific owner declares the exact pass sequence, block preparation,
partition geometry, halos, accumulation precision, valid work identities,
reduction arithmetic, and completion. Runtime never invents, fuses, or repeats
a scientific pass.

Each work item declares one of two accumulation capabilities:

```text
Exclusive(stable region)
OrderedPartial(stable region, stable commit key)
```

An exclusive region has at most one active mutable lease. An ordered partial
occupies a planned bounded slot and commits in stable key order, never worker
completion order. Neither capability can represent a full grid per worker.

### Application and frontends

`casa-imaging-application` resolves the source, requests the plan, composes the
source and kernel Adapters, invokes the installed implementation, and publishes
the resulting evidence. Frontends continue to parse, convert units, construct
requests, and present results. No execution or scientific calculation moves
upward.

## Execution contract

### Slot lifecycle and backpressure

Runtime owns this lifecycle:

```text
Empty -> Filling -> Ready -> Borrowed/Preparing -> InFlight -> Returning -> Empty
```

The producer may fill only after acquiring an empty source lease. Returning the
last reference to a block is the only backpressure signal; Adapters may not
create side queues. A source slot cannot be refilled until all work and fences
borrowing it have settled.

Depth one follows this same lifecycle inline without unnecessary producer
thread or channel overhead. When two slots are admitted, one ordered producer
fills and arranges the next block while the consumer processes the current
block. The queue is bounded by the admitted leases; there is no unplanned third
block.

Work that outlives a source block either retains its source lease or projects
into a planned reusable prepared slot. Every projection is a named measured
copy. Runtime performs no implicit cloning.

### Serial and worker equivalence

`workers = 1` and larger worker counts use identical source blocks, work
identities, kernel methods, accumulation capabilities, and commit rules.
Runtime may elide transport overhead for one worker, but it may not select
different science code.

A stable work identity derives from the pass ordinal, source ordinal, block
ordinal, kernel partition key, and local work ordinal. Worker scheduling may
change completion order; partition identity and commit order may not change.

### Scientific passes

Natural weighting declares one selected-observation stream. Global density
schemes declare their density pass followed by their weighted/gridding pass.
Major-cycle work adds only the passes declared by its scientific owner. The
plan and completion report logical scientific passes and physical MAIN reads
separately.

### Planning and adaptation

The immutable physical plan admits exact source, prepared, scratch, partial,
commit, reduction, grid, FFT, and product state under one Resource Lease:

```text
retained source
+ source slots
+ prepared slots
+ workers * worker scratch
+ bounded partial/commit window
+ one shared accumulation state
+ reduction scratch
+ FFT and product state
+ measured runtime overhead
<= admitted lease
```

The plan selects slot count, read-ahead depth, workers, queue bytes, scratch,
partial bytes, and legal transitions from physical capacity and reviewed cost
evidence. Dataset names and imaging modes do not select thresholds.

Two live source slots are selected whenever both fit the admitted lease; the
106.9 GiB diagnosis demonstrates useful overlap. Depth one remains legal when
only one slot fits. Worker count and read-ahead may change only through
plan-listed transitions at declared block or pass quiescence points. Every
transition appears in measurements. Current-run observations never silently
rewrite a future cost-model profile.

### Failure and cancellation

The first causal source, kernel, scheduler, contract, fence, or cancellation
failure is retained. Failure stops new admission and source reads, wakes blocked
parties, drains only work needed to settle leases and fences, and prevents
source or scientific completion. Failed runs retain measurements. Execution
does not resize outside the plan, replan, retry, or invoke an alternative.

## Measurement contract

Measurements are typed runtime-owned values and machine-readable performance
artifacts. Existing receipt extension points may project them, but these
deliveries do not independently authorize a versioned persisted-schema change.
Shared atomics or timers on every sample are prohibited; counters aggregate per
block, worker, and stage.

Every run records planned and actual:

- logical scientific passes and physical MAIN traversals;
- blocks, rows, samples, read operations, raw column bytes, logical selected
  bytes, and modeled physical bytes;
- named handoff copies and bytes, with implicit-copy bytes required to be zero;
- allocations, replacements, genuine reuse, current residency, capacity, and
  high-water residency for source, prepared, weighted, scratch, partial,
  commit, reduction, grid, FFT, and product storage;
- source read, source arrangement/projection, density, weighting,
  preparation, queue wait, worker execution, deterministic commit/reduction,
  FFT/scientific completion, and writing time;
- producer active and blocked time, consumer receive wait, measured overlap,
  queue high-water blocks/bytes, and lease-return latency;
- per-worker work units, samples/taps, active time, ready wait, backpressure
  wait, reduction time, and stable work/partition keys; and
- selected workers, read-ahead, live blocks, permitted/adopted transitions,
  exact admitted bytes, tracked peak bytes, and sampled process RSS.

## Delivery 1 implementation and deletion path

Delivery 1 is one issue and one pull request, organized into three reviewable
commits:

1. **Instrumentation and current-Rust control.** Audit the existing uncommitted
   instrumentation against this measurement contract. Retain only conforming
   code, preserve scientific output, and record the current-Rust control.
2. **Shared bounded seam.** Add typed caller-owned table fills, exact private
   selected-observation reuse, the sealed executor, admitted pools,
   depth-one/depth-two operation, backpressure, generic workers, cancellation,
   adaptation, and scheduler law tests.
3. **MFS migration and evidence.** Adapt the complete-data MFS owner, migrate
   the production caller with `workers = 1`, delete the displaced synchronous
   MFS route, and record the focused and data-scale evidence.

The migrated production path may not retain a selectable route through the
current same-thread selected-observation prefill plus synchronous spectral
consume loop. General `casa-ms` traversal capabilities may remain when they
serve non-imaging callers, but they cannot be an imaging fallback. The current
allocate-and-replace body of the private selected-observation fill is replaced
by exact in-place refill; the current MFS application/runtime streaming loop is
removed after its caller migrates.

Before implementation, #540 is updated to reference this specification and
include its Delivery 1 scope and gates.

## Delivery 2 work record

Create one focused issue extracted from #541 after this specification is
accepted. It is blocked by merged #540 and the accepted serial MFS owner, not by
unrelated mosaic, W/AW, or MT-MFS implementations. #541 retains the remaining
operator-parallelization programme scope.

Delivery 2 changes only reconstruction-owned MFS partitioning and reduction
behind the landed executor Interface. It does not reopen source storage,
runtime lifecycle, application composition, or frontend behavior.

## Acceptance

### Focused correctness and architecture

Delivery 1 must prove:

- exact selected-observation order, terminal errors, pass counts, and no
  completion after partial consumption;
- exact in-place storage reuse and honest replacement/allocation accounting;
- one-slot and two-slot state-machine equivalence, bounded backpressure,
  cancellation, first-error retention, and lease return after fences;
- identical scheduler results for one and multiple legal worker schedules using
  a deterministic bounded test Adapter, without a full grid per worker;
- current pinned CASA product-oracle and accepted serial-Rust equivalence for
  the focused complete-data fixture;
- MFS `workers = 1` execution through the sole bounded executor;
- matched MFS `workers = 1` wall time no longer than the single-process CASA
  anchor, without using a multi-worker or device result to offset a miss;
- no application/frontend calculation and no dependency-direction change; and
- deletion or unreachability of the displaced production MFS route.

Run only the issue-named and directly affected focused gates, plus
`just docs-check` and `just arch-check`. Programme #486 does not require routine
`just verify` for this ticket.

### Medium and 32 GiB MFS evidence

Use the instrumentation-only commit as the current-Rust control and the frozen
CASA products/timing as the oracle. Run one matched control/candidate pair on
the medium workload and one on the directly mounted 32 GiB workload. Do not
rerun CASA.

Each candidate must preserve the pinned product thresholds, exact scientific
pass count, and hard resource bounds, and must show an observed wall-time
improvement over its current-Rust control. On the matched medium oracle, the
`workers = 1` candidate must also take no longer than CASA's 733.660-second
single-process anchor. A multi-worker or device result cannot satisfy or waive
that serial gate. Because each is a single pair, label the performance result
provisional; do not claim repeatability or a statistical speedup distribution.
The executable full-data gate is
`tools/perf/imager/workloads/wave3-standard-mfs-single-term-heavy-wave2-serial.json`;
it pins `parallel = false`, CPU standard-MFS execution, and the RustFFT backend.

### Complete 106.9 GiB streaming gate

Run one complete ordered pass over
`wave1-alma-mosaic-large.ms` through the production bounded executor with a
tiny deterministic borrowed-run checksum kernel, `workers = 1`, and read-ahead
at most two. This is a source/executor residency and overlap gate, not a mosaic
science claim.

The gate passes only if it reports:

- exactly one source pass, 6,709,290 rows, and 13,740,625,920 samples;
- exact digest equivalence with its accepted serial checksum;
- live blocks, queue bytes, and tracked residency within the immutable plan;
- two-lease overlap when two slots are admitted, including producer-blocked,
  consumer-wait, overlap, and queue-high-water measurements;
- wall time materially below the measured serial sum of source-active and
  consumer-active time, without assigning a repeatable speedup claim;
- storage growth only during admitted pool warm-up, followed by genuine reuse;
- zero implicit-copy bytes and exact named projection bytes;
- stable sampled RSS after warm-up, no whole-dataset growth, and no OOM; and
- terminal completion only after every lease and fence settles.

Delivery 2 adds one matched serial/multicore MFS pair using the same Interface,
stable partitions, deterministic reduction, and pinned CASA products. Its
multicore result is eligible for a performance claim only after the matched
serial row passes the serial CASA gate above.

## Stop conditions

Stop and report before implementation or retention if the work requires:

- another resource authority, planner, runner, or scientific owner;
- a persisted receipt/schema change not already approved;
- a new dependency direction or external public Interface;
- a hidden source pass, copy, queue, allocation, or whole-MS structure;
- a complete grid per worker or an unbounded partial/commit window;
- a compatibility route or inability to delete the displaced production path;
- weakened scientific or architecture tests;
- activation of blocked #541 scope before #540 merges; or
- shipping Delivery 1 without observed medium and 32 GiB improvement or with a
  failed 106.9 GiB bounded-overlap gate.

## Review and completion

Delivery 1 closes only after its named gates and one independent programme
contract review report no unresolved blocker. The review checks this
specification and repository standards against the exact diff. Merge authority,
issue closure, cleanup, and release remain governed by the programme policy and
repository operating contract.
