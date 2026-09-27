# Canonical bulk imaging: integration and deletion plan

Truth class: user-approved implementation plan; Oracle-reviewed
Last reality check: 2026-09-27
Status: approved 2026-09-27; delegated implementation with Astra review checkpoints
Verification: source inspection, Oracle advice checked locally, retained measurements, just docs-check

## Restart here

This plan responds to the owner's instruction to eliminate recurring item-by-item
imaging transport/preparation, across all modes rather than only the current cube
case. The owner approved this plan and its bounded implementation/verification,
not another open-ended optimization campaign or full-run restart. Read the single current
record before executing anything:

`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.

Durable review/evidence directory:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/bulk-imaging-plan-20260927/`.

Oracle: [Chrome ChatGPT Pro review](https://chatgpt.com/c/6ab92790-4dfc-83e8-924e-c3fb4532ab2a).
The UI identified Pro at its highest visible power, 5/5; a more specific model
version was not displayed. Oracle completed after 13m 10s. The complete prompt is
[bulk-imaging-oracle-prompt-20260927.md](bulk-imaging-oracle-prompt-20260927.md),
mirrored as `oracle-prompt.md` in the durable directory. Extracted advice and
local dispositions are in `oracle-response-notes.md` there. The linked ChatGPT
conversation retains the complete response. No accepted ADR has been edited.

Source basis: `3cb9252c19746c8386e5d1b1f7ff89c81415e1d5` plus the preserved,
uncommitted failed cache/TSM candidate. The candidate is retired for promotion,
not silently reverted or newly accepted. Full W1 was killed at owner request;
W4 must not start. Keep its partial products, store, source and logs.

## Execution ownership and message-only handoff (owner-approved 2026-09-27)

The active Goal belongs to **Astra Streaming cube imaging redesign and scaling**,
thread `01a079dc-23bd-73e1-8a6b-0fd4ccb7f9f3`, host `local`. Astra owns architecture,
acceptance and milestone review. Implementation is delegated to the existing
**Sol Streaming cube imaging redesign and scaling (2)**,
thread `01a0caf5-7c4a-7a23-b5db-ee34b7809e62`, host `local`. Both share this checkout;
only the current work owner edits source and CURRENT. Do not create duplicate
threads or Goals. Sol keeps its configured model; no silent model substitution.

| Handoff | Sol deliverable | Astra decision |
| --- | --- | --- |
| R1 | Local pre-refactor checkpoint; narrow identity/freshness/ADR-0013 consumer evidence; minimal block view/ownership proposal and existing-capability migration mapping | Resolve/approve the contract and dispatch complete Slice A; do not spread new interfaces before this review |
| R2 | Complete cube AND MFS path, focused scientific/error/resource/call-count evidence and bounded end-to-end W1/W4 results | Review the integrated result and dispatch B/C; not satisfied by isolated component tests |
| R3 | B/C all-mode migration, production deletions, all affected required evidence, docs and anti-slop pass | Verify architectural completion separately from full-size performance/science acceptance |

At each review, Astra sends one consolidated findings packet. Sol gets **one**
fix round for that checkpoint. If the return leaves findings unresolved or fixes
them incorrectly, Astra takes over those repairs rather than sending Sol another
round. Do not reset this count through context loss, subagents or relabeling.
After Astra completes the repairs, Sol may own the next explicitly dispatched
slice. New scope/authority and scientific escalation rules still apply.

The user explicitly authorized reciprocal messages for this workflow. At a
review-ready result, genuine blocker, failed fix round, or the existing unattended
checkpoint, the worker must first preserve its evidence, settle its child agents
and commands, and update CURRENT. It then calls `send_message_to_thread` to the
other exact thread above with milestone, result, changed functions/source pin,
commands and outcomes, durable paths, rejected hypotheses/retry use, unresolved
findings and requested next action. Sending the callback is its **last work
action**; it ends the turn with a brief handoff acknowledgement and makes no more
edits until dispatched again. Merely writing a final answer does not wake the
reviewer and is not a handoff.

Neither thread polls the other, starts a status watcher/automation, or runs a
wait/sleep loop to look for its completion. Resume from the explicit callback or
user input. An automatic Goal continuation while ownership is delegated must
not poll, duplicate Sol's work or send a needless status message. Ordinary
bounded command execution/completion within one's assigned slice is allowed.
If callback delivery fails, preserve the exact destination and failure in the
handoff, report it to the user and stop; do not silently claim notification.

Both threads use bounded **Luna Max** subagents for substantial code/output
inventories, source tracing and progress/result parsing: when available,
`gpt-6-luna`, effort `max`, explicit bounded context. Keep architecture, scientific
interpretation and acceptance with Astra, and routine implementation with Sol.
If that configuration is unavailable, report it instead of silently substituting.
Routine direct lookups do not require a delegation ceremony.

First assignment is R1 only; its review is performed by Astra, not another user
approval request unless a genuinely new external contract/authority is needed.
Existing evidence is the starting point: no new broad inventory or resurrection
of retired cache/FFTW-alert investigations.

## Why another statement of "use blocks" is insufficient

The August bounded-streaming specification already called for leased blocks,
compact representations and one executor. The later cube replacement kept a
separate scientific-capability route. Current source still exposes full-sample
inspection followed by another projection/preparation traversal inside calls
named `consume` and `visit_block_range`. Flat output arrays alone did not remove
this work. Likewise, bulk MS reads did not guarantee bulk memory copies or a
replay layout matching the consumer's order.

The completion criterion must therefore include actual production call paths,
work counts and deletion, not interface names, wrapper reuse or passing isolated
component tests. Scalar arithmetic inside a tight numerical loop is not the
problem; repeated object construction, invariant checks, dispatch, tiny copies
and nonlocal I/O around that arithmetic are.

## Verified starting evidence

Full workload: 4,094,064 rows, 512 channels, two correlations, 1024-square output,
natural weighting, LSRK, batched Clark, 0.5 mJy threshold. Same scientific
selection and existing CLEAN/writer; optimized FFTW build.

| Evidence | Result and limitation |
| --- | --- |
| Retained CASA W1 | 21,622.668 s |
| Prior complete casa-rs W1 | 26,620.317 s; 6.90 GiB peak RSS; 18 imaging passes; full science still has 22 review alerts, no waiver |
| Failed enlarged-cache full W1 | Killed at 30,632.016 s during refresh 12; peak RSS 10.47 GB; no final comparison; no W4 |
| Corrected short parent/candidate | 804.399/791.954 s end-to-end; refresh 70.108/59.791 s; unchanged seven-product/nine-check comparison passed; did not predict full cache-state behavior |
| Full preparation | Inspection 187.340 s + preparation 502.529 s + ordered commit 43.759 s = 733.859 s |
| MS source timing | Read 30.167 s, fill 31.536 s; overlaps preparation, not an additional additive stage |
| Prepared coverage work | 222,987,289,914 hash-input bytes and 4,220,979,990 update calls; no isolated attribution of all preparation time to hashing |
| Read-only representative replay | Actual retained store, 8 channels/all row blocks: about 30.25 s wall, 30.17 s in `pread`, 0.52 s CPU; disabling read-ahead did not help |

Native replay is 75,693,263,184 B: 1,076 row blocks, 3,805 rows/block,
512 single-channel frames per block. Disk order is row block then channel;
consumption is output band then all row blocks, producing about 70.35 MB strides.
The enlarged cache reduces a full refresh from 268.315 GB/1,652,736 frame reads
to 75.693 GB/551,988 frame reads, but read wall varies between 95.4 and 1,754 s
for identical counts. Counts are requested bytes/completed frame reads, not
physical device traffic. No claim that CRC, LRU CPU or the disk alone explains
that variation is justified.

Evidence directories under `spectral-full-20260924/`:

- `full512-1024-neon-20260926`: original complete result and comparison.
- `wide-replay-short-v2`: corrected small application pair.
- `full512-cache-repair-20260927`: failed full candidate and terminal stop receipt.
- `retained-store-probe-20260927`: short read-only I/O reproducer and negative
  read-ahead result. Its actual store remains on GLENDENNING; it opens read-only.

## Current ownership and route map

| Seam | Current behavior | Must preserve versus replace |
| --- | --- | --- |
| Application `lib.rs::run_native` | Chooses `CubePhase` or `SpectralCycleExecutor` | Preserve sole application entry and shared `run_native_phases`; remove the transport/executor split after migration |
| `CubePhase::supports` / `cube_geometry` | Natural, empty model, no visibility transform, one source/DD/SPW/pol, channel-local, standard grid, Stokes I, linear, no mosaic/W/AW/instrument model | These are current implementation limits, not permission to lose other modes |
| MS `SelectedObservationBlock` | Bounded selected source columns and row mapping | Evolve/reuse this owner; do not invent another MS interpreter |
| `SelectedObservationProjector::visit_block_range` | Per-correlation sample view and spectral evaluation, then callback | Replace imaging's rich sample/run transport with direct numeric block views; preserve science |
| Runtime `NativeKernel::consume` | Separate inspection and worker preparation walks | Fuse work at its valid scope; no dedicated verification-only sample traversal |
| Reconstruction `NativePreparationWorker::consume_channel` | Per-channel lifecycle/address/geometry checks, weights/flags, per-correlation coverage encoding | Keep required numerical checks/semantics; bind invariants once and process array spans |
| Cube `NativeBlock` / `NativeStore` | Row-major arrays serialized into row-block-major, single-channel frames | Unify layouts and select replay extents matching consumption; no unconditional expanded intermediate |
| General `SpectralCycleExecutor::run_stream` | Weighting chunks feed `consume_bounded_replay_chunk` | Preserve scientific kernels; delete displaced record transport and duplicate orchestration |
| General selected output | `run_selected_output` calls `predict_final_visibility_chunk` | Bring prediction and MODEL_DATA/CORRECTED_DATA writes through the same block path |
| Resource/residency | Existing bounded executor, authority and managed image buffers | Reuse; no new allocator, planner or pool per mode |
| Application CLEAN/products | Shared controller, normal-state ownership and CASA writer | Retain; no second controller or new publication protocol |

The local route inventory was delegated read-only to Luna Max. Main-agent
verification additionally followed `BandPlan::supports` into
`streaming_cube/completion.rs::cube_geometry`: it explicitly rejects W/AW and
mosaics. This closes the inventory's uncertainty about those scientific modes;
an unrelated/mismatched deployment input is still an ordinary error, not fallback.

The optimized ancestor `fff9c2d553eace4b6a57b1df9ded4773f2263ceb` supplies useful
borrowed block views and reusable grid workspaces. It is reference material,
not a package or runner to restore. The existing source study documents CASA
VisBuffer traversal and LibRA visibility buckets. Reuse those locality/lifetime
lessons without recreating their public APIs.

## Invariants for the reviewed design

1. One admitted block transport for all imaging phases/modes, with typed flat
   views and specialized numerical kernels. No per-sample callbacks, heap
   objects, source re-binding or generic dispatch in orchestration.
2. Structural source/selection/layout checks at binding and block boundaries;
   required scientific value/flag checks remain fused into useful loops.
3. The same input block is shared immutably across useful work partitions; only
   admitted active grids/scratch are worker-private. No whole-cube replicas.
4. Physical layout and loop nesting agree, or an explicit bounded tiled
   transpose is justified and measured. Changing a type name is not a repair.
5. Whole-MS materialization and compulsory replay expansion are forbidden.
   Repeated source reads, prepared replay and retained resident blocks are
   physical strategies behind the same science interface, not parallel science
   implementations or runtime error fallbacks.
6. Keep necessary external freshness and persistence integrity. Internal
   execution coverage must not become content attestation under another name.
   Any changed normative identity guarantee needs explicit disposition.
7. All dimensions, buffers, queues, caches and simultaneously live phases fit
   the existing 16-GiB authority. Use actual capacities and overlap, not sums of
   unrelated phases; two Cargo jobs and the RSS guard remain.
8. Migration is finite and mode-by-mode accountable. No "all imaging" completion
   while legacy production transports still serve accepted capabilities.

## 1. Selected design: one bulk boundary, not one universal kernel

Use homogeneous source/DD/SPW/polarization blocks of bounded selected rows and
channel runs. Canonical payload order is `[row][channel][correlation]`, index
`((r * C) + c) * P + p`. Preserve a complete required correlation group. The
block contains flat typed arrays, not a collection of sample objects:

- One source/selection binding and descriptor reference; physical row and
  channel addresses, including gaps, remain explicit.
- Row metadata at row scope: time, antennas/feed/field, UVW and necessary
  geometry/pointing inputs. Reuse existing scientific types where useful.
- Channel metadata at descriptor scope where invariant. Keep genuinely
  row/channel-dependent converted frequencies or coefficients in bounded
  scratch; do not assume a constant frequency transform or approximate time bin.
- Visibility values in their scientific input precision. Select FLOAT_DATA
  versus complex representation outside the element loop; do not widen/copy
  every sample solely to satisfy a generic interface.
- Row/correlation weights or row/channel/correlation weights, selected once per
  block specialization. Do not expand constant weights needlessly. Preserve
  distinct data flags, row flags and weighting-validity semantics.

Main-agent source check confirms the TSM fast-copy path currently has contiguous
channel/correlation data within each row patch, but transposes it into a
channel-major destination. A row-major destination can copy a dense channel run
at once when the selected correlation group is contiguous. Noncontiguous
correlations, channel gaps, endian conversion and packed flags require explicit
bounded gathers/decoding; do not claim universal memcpy or zero-copy.

`casa-ms` remains the only interpreter of MS columns/selection. Reconstruction
owns a minimal backend-free numeric view and its scientific block kernels;
runtime binds borrowed source arrays into that view without copying, admits
storage and schedules work. This does not create a reverse crate dependency.
Define the numeric view in the existing reconstruction/runtime-adapter seam;
do not introduce a new crate or universal tensor/buffer framework.

Conceptual responsibilities (not a demand for these exact API names):

```text
source.read_into(source_request, admitted_block)
prepare_block(bound_science, borrowed_block, admitted_scratch)
kernel.accumulate_block(prepared_view, owned_output_wave)
```

Dispatch weighting representation, scientific mode and kernel specialization
outside the element loop. Actual gridding, degridding, flag/value checks,
interpolation and reductions still process values in tight loops. A numerical
kernel may use an explicit admitted tiled transpose if measured worthwhile;
there must be no hidden scalar-object adapter before or after it.

Block lifetimes protect borrowed arrays until all consumers finish. Refill
reuses their allocations, not merely a wrapper object or an equal capacity.
Binding/shape/source invariants move to source, descriptor and block boundaries.
Compute geometry at its actual varying scope, not per correlation if shared and
not once per row when it genuinely varies with frequency/direction.

Heterogeneous DD/SPW/polarization groups use separate homogeneous blocks and
precompiled descriptors. Do not materialize a whole dataset to sort it. Reuse
one input block across fields/facets/terms; do not expand it into per-facet or
per-Taylor transport records. Changing a physical block boundary must not change
scientific grouping. A bounded assembler may join pieces of a coupled operation
under the same memory authority.

## 2. Traversal and backing store

### Memory-admitted output waves

A wave is a group of output regions whose complete active state fits. Its size
is not the worker count: W1 can use a multi-plane wave and W4 must not imply
four copies of the same full cube. Admit the simultaneous peak of source
blocks, preparation/transpose scratch, worker grids/FFT/convolution workspace,
model support, prediction scratch, output/writer windows, retained indexes and
caches. Keep metadata and cache charges until their actual last consumer, not
merely until a preparation node finishes.

For one immutable major-cycle model epoch:

1. Compile the wave's exact source and model support using existing science.
2. Read each needed source block once per wave where feasible; prepare once and
   apply all useful contributions to the wave's owned outputs.
3. Finish complete prediction from every required plane, Taylor term, field or
   facet before subtracting it. If model support cannot coexist, accumulate
   prediction in an admitted block buffer across model-page visits. Never use
   a partial prediction because a wave ended.
4. Complete the pass before the existing CLEAN controller advances the model.

Huge cubes remain bounded by rereading source ranges for later waves. This is
not a promise of one read per job. Halo/support is derived from actual spectral
and spatial operators, never a hard-coded one-channel neighbor rule. Uniform
and Briggs density normalization, joint/Taylor cross terms, PB/PSF and sum-weight
normalization retain their existing global/scientific scope. Do not fuse phases
across unresolved dependencies merely to reduce passes.

### Direct source first; compact replay only when it earns its cost

The canonical path must operate directly on tiled-MS blocks; an intermediate
file is not mandatory. Direct is the default in the absence of measured evidence
that replay is worthwhile. The later-pass cost comparison includes extraction,
decoding, preparation, verification and replay construction, not just byte count.
Unknown CLEAN pass counts do not justify an optimistic amortization assumption.

Optional replay is another backing source for the SAME block boundary and
scientific kernels, not a second execution route or fallback. Choose backing at
an explicit binding/pass barrier. Source/I/O/integrity errors fail the run;
never retry them through a different backing. A replay captured during initial
ingress is usable only after that capture succeeds.

When justified, replay disk order is:

```text
source / DD -> source-channel group -> row block -> flat column payloads
```

Use large bounded extents related to real source tiles and wave consumption;
the present 20-channel MS tile is a candidate, not a magic constant. Keep row
metadata in one separate row-ordered stream, with admitted caching or sequential
reread. Do not duplicate row metadata in every channel group or jump back to a
distant metadata frame for every data read. Build bounded group streams directly,
without a compulsory second full-artifact concatenation. Bound file handles,
staging, index and retained metadata; page an oversized index.

Retain compact source facts. Persist expanded derived geometry only for a
measured benefit; never require an expanded 70-GB store for every observation.
Frame identity, shape/length, CRC and error handling remain. Grouping records
does not imply writing native padded Rust structs or weakening codecs.

## 3. Validation and identity: resolved rules and one narrow decision gate

| Check | Replacement placement and retained guarantee |
| --- | --- |
| Source/selection/freshness | Existing source-owner binding, relevant mutation/lock and terminal checks; do not substitute mtime/file size or assume locks cover every mutation mechanism |
| Content-derived input identity | Source boundary only, compact canonical bulk encoding; worker count, physical block shape and output waves must not change logical identity |
| Internal traversal/contribution coverage | Exact expected selected address ranges, output ownership and pass identity matched against successful block completion; counts alone are insufficient |
| Shape/lifecycle | Checked construction and block/worker ownership transitions; no repeated per-channel re-binding of invariant facts |
| Scientific values | Required finite-value, flag, weight, polarization and support handling fused into useful numerical loops |
| Private persistence | CRC32C plus framing/identity/bounds on write and physical load; verified immutable RAM hits do not require another hash |
| Product/model publication | Existing ownership transfer; no content attestation or verification-only array reread |

The current `FrozenWeightingCoverageProof` in reconstruction `weighting.rs`
requires a first encoded coverage stream, then checks frozen weighting,
selected generation, transform identity and counts for derived replays.
`SelectedObservationInspection` separately checks source coverage and creates a
selected-content generation. They are not interchangeable. The first contract
review must trace their actual current consumers and mutation tests, not assume
that every digest is either mandatory or removable.

**Chosen target:** bind selected-column identity to selection/schema/address
metadata once at its natural scope, plus canonical selected values/flags/weights
in stable logical chunks; bind chunk identities, lengths and order. No unordered
XOR. Replace trusted internal coverage content proofs with exact structural
coverage and frozen-owner bindings when those preserve the actual failure
contract. A narrow output wave must not silently weaken an existing obligation
to detect mutation anywhere in the selected observation through completion.

**Review-1 decision:** establish whether any current consumer persists/externally
relies on the identity encoding and whether source rereads need content checks
beyond retained generation authority. Record the exact identity version and
required contract supersession before implementation. Do not silently weaken
freshness or alter a persisted/public identity contract. If the stronger current
contract truly requires a full input pass, retain it and report its cost rather
than hiding it in preparation. The relevant scope is the producer/consumer and
mutation-test chain, not a new broad architecture inventory.

Oracle recommended first buffering the exact legacy byte stream. Local
disposition: use that as a diagnostic/control or temporary integrated migration
step if necessary, NOT a separate optimization campaign or a permanent promise
to preserve obsolete private digest bytes. Batching hash calls alone still
hashes roughly 223 GB and does not complete this simplification. Any temporary
encoder has a deletion obligation at the identity cutover. Do not concatenate
independently hashed physical blocks and claim the legacy sequential digest is
unchanged.

Preserve ADR-0010 resource ownership, ADR-0011 scientific coupling and ADR-0014
trusted product/model transfer. Inspect the exact ADR-0013 schema-specific text
at Review 1; propose an amendment/successor if grouped replay changes its named
format obligations. No accepted ADR changes are authorized by this planning turn.

## 4. Finite integration and deletion sequence

There are only three architectural review stops, not one review per component.
Within each approved slice, batch related source/kernel/runtime changes and
builds, use focused checks, and reach the complete application path immediately.

### Review 1 — contract and ownership, before implementation

- Preserve the dirty source/evidence in a local non-main checkpoint before code
  changes; explicitly label the failed candidate as unpromoted. Do not reset,
  delete, push or clean anything as part of checkpointing.
- Use the existing capability catalog and scientific migration matrix to assign
  every currently supported request class and relevant interaction to the phases
  below. Do not add a parallel permanent capability registry. Include the current
  empty/starting-model cases, transforms, polarization, sparse selections,
  moving-source geometry and visibility writes, not just advertised mode names.
- Approve the minimal borrowed numeric view and ownership lifetime; resolve the
  narrow input-identity/ADR-0013 decision above. No public frontend API or
  CASA-visible persisted format changes are implied.
- Adopt the canonical block/no-scalar-orchestration rule in active architecture
  and agent guidance. Record exact supersession of the older one-channel replay
  choice while retaining its historical results and useful image residency.

### Slice A — complete ordinary cube AND MFS

Wire real selected input -> fused bulk preparation -> initial imaging -> existing
CLEAN -> post-model-update residual refresh -> existing product writer for both
natural standard cube and ordinary MFS. Use direct tiled-MS input first. Reuse
current arithmetic/precision to isolate transport and ownership changes; do not
pay for additional bitwise reproducibility. This slice is incomplete if either
mode is only a kernel test, has zero CLEAN work, skips residual refresh, or still
calls the displaced rich sample preparation chain.

Run one representative short end-to-end parent/candidate observation, the
unchanged product comparisons and panels, and W1/W4 resource/call-count checks.
Repeat timing only if the result is ambiguous. No full 32-GB imaging run.

### Review 2 — working application and demonstrated batching

Review the complete cube/MFS outputs, actual route, work counts, memory ownership
and end-to-end timings. Reject a nominal block API that still dispatches per
sample or retains unnecessary copies/identity passes. Verify that W1 does not
force one output plane per wave and that worker count is not fixed at four.
Select direct versus grouped replay for the deep case from the bounded source
comparison below; do not promote based on removed bytes alone.

### Slices B and C — migrate the rest through the SAME boundary

| Slice | Required application domain | Reuse and deletion obligation |
| --- | --- | --- |
| B: general standard-gridder | Natural/uniform/Briggs/taper; cube/MFS/MT-MFS; heterogeneous MS/DD/SPW/correlations; sparse channels/frame conversion; initial model; prediction and MODEL_DATA/CORRECTED_DATA; sequential continuum transform and existing coupled continuum-line semantics | Reuse weight/density, spectral/polarization, Taylor/coupled, fitting and prediction primitives. Retain complete coupling and global normalization. Remove old cube transport once its ENTIRE capability domain is covered |
| C: spatial/widefield | Supported W/AW variants, mosaics, fields/facets, moving-source/pointing geometry, heterogeneous responses, existing weighting/spectral/prediction combinations | Retain convolution/support/AW scientific owners and lifetime through final consumer. Remove separate source/admission/validation routes, not their numerical algorithms |

Each migrated request has exactly one assigned route. Temporary coexistence is
explicit migration state only: no retry/fallback on an error and no permanent
"fast mode" flag. New imaging functionality uses the bulk boundary; it may not
grow the legacy route. Do not add new scientific capabilities under the guise of
migration; preserve every capability already supported.

Visibility writes declare read/write sets before binding. Self-authored writes
advance appropriate column generations at existing barriers; they do not turn
off freshness checking for the MS or mix input epochs. Preserve existing column
write locking and failure behavior, not a new transaction subsystem.

### Review 3 — all-mode closure and deletion

Every existing supported request class and interaction must have current route
and required acceptance evidence. Delete the temporary dispatch table and the
production `CubePhase`/`SpectralCycleExecutor` transport split, displaced scalar
preparation/record adapters and obsolete private replay encodings. Relocate useful
numerical functions before deleting their former owner. Test-only scalar oracles
may remain; unrelated non-imaging MS scalar APIs are not deletion targets.

Concrete targets to retire or narrow to non-production callers:

- Application capability selection between the two transport executors.
- Runtime cube-private preparation/store orchestration and the general
  `WeightingReplayChunk -> consume_bounded_replay_chunk` transport chain.
- Imaging calls to `inspect_block_range` plus `visit_block_range` followed by
  `consume_channel`, and their duplicated invariant/sample-envelope machinery.
- NativeStore row-block-major/single-channel framing and duplicate input layouts.
- Per-sample weighting coverage serialization once its binding replacement is
  approved, temporary identity encoders and obsolete completion adapters.

Do not delete the shared CLEAN controller, scientific operators, useful grids,
managed image residency, external source guarantees or CASA writer. Update
`ARCHITECTURE.md`, the old active plans/specifications, domain terms and the
performance skill together; mark history non-normative where superseded.

## 5. Durable regression checks

Reuse `scripts/check-imaging-architecture.py`, its structural negative tests,
existing application tests and scientific matrix. Enforce both interface access
and measured production work; a word ban or a new function called "bulk" is not
evidence. Expensive diagnostic counters are test/profiling-only; normal telemetry
uses coarse block totals, never a timer/hash/receipt per sample.

For a fixed selected fixture, vary row/channel block boundaries, wave size,
worker count and source backing. Assert:

- Zero rich sample-envelope creation and per-sample framework callbacks in
  migrated orchestration. Kernel entries scale with blocks and genuine scientific
  partitions, not correlations. Checks and geometry evaluations scale with their
  documented scope; necessary per-value science remains present.
- After admitted pool initialization, actual allocations/capacity reuse and copied
  bytes match the design. Dense copy calls scale with contiguous selected spans;
  gapped/endian/flag cases exercise the explicit bounded gather/decode path.
- Expected selected MS tile IDs/ranges/bytes match real reader calls, including
  sparse selection, partial tiles, interpolation boundaries and incomplete last
  blocks. Separately report logical bytes, storage tile bytes, repeated requested
  bytes and read operations; none is automatically physical-device traffic.
- Replay read offsets follow channel-group extents rather than full-row-block
  strides; metadata reuse and index/queue/handle residency remain bounded.
- Input identity, when required, is independent of worker/physical block/wave
  partitioning. Exact counts PLUS address coverage detect omitted, duplicated or
  swapped ranges. Intentional halo reads do not duplicate output contributions.
- Cancellation/I/O errors at every transition fail without false completion or
  fallback. Swapped/truncated/corrupt records, stale source generations, wrong
  descriptor/correlation order and premature buffer reuse are rejected.
- Wrong wave-local normalization and partial model prediction are negative
  science controls. Preserve masks/WCS/beams, products, PSF/PB normalization,
  full-field tolerances and complete-data residual semantics.
- Resource admission includes real simultaneous capacities and long-lived
  metadata, not merely numeric payload; both 16-GiB planning and sampled aggregate
  RSS hold, including tiny-budget/many-plane paging and worker changes.

Keep existing mode gates, e.g. T35/T36 spectral laws, T37/T38 cube operator/CLEAN,
T42/T43/T44 MT-MFS, T31 geometry, T41 moving sources, T48 heterogeneous response,
T59 low memory and the applicable prediction/transform gates. Select exact
commands from the current issue/matrix; do not invent a new redundant gate ladder.
For the present cube, preserve the unchanged seven-product/nine-check comparison,
all per-channel deep-CLEAN checks and panel inspection. Its 22 unwaived full-run
review alerts remain open; transport success does not erase them.

## 6. Only two discriminating measurements before performance selection

No new broad profiling campaign is needed to justify removing the scalar
transport chain. After implementation authority, reuse existing harnesses:

1. **Source locality:** extend the actual-store eight-channel/all-row probe to
   compare current replay, direct tiled-MS extraction and grouped replay for the
   identical dependency union. Include construction separately, extents/read
   latency/CPU, and representative admitted image-memory pressure. A small
   compact file that fits cache is not proof for a wide backing store. Preserve
   full store geometry or label its limitation; no new full CLEAN run is needed.
2. **Fused preparation:** feed real bounded blocks to the existing preparation
   control and integrated block implementation. Compare exact required identity
   semantics and prepared numerical state; count geometry work, copies, encoding
   bytes/hash calls and allocations. Measure enclosing preparation time. Do not
   infer all 503 s is hashing or claim end-to-end gains from a microbenchmark.

These choose source policy and verify the coherent integration, not a queue of
independent small candidates. Keep negative results and stop repeating unchanged
failures. Further optimization follows measured end-to-end bottlenecks only.

## Exit criteria and present authority

Architectural completion means all currently supported imaging uses the one
block transport, source/ownership/resource policy and shared application
composition; obsolete production routes/APIs are deleted; all required science,
failure and resource gates pass. "Cube became faster" is not completion.

Performance completion is separate. The current full-size evidence does not
support a 2x-CASA claim; about 900 s of non-read work per refresh survives the
read timer even in the failed candidate. Report actual end-to-end W1/W4 results
without padding serial time or extrapolating removed bytes into a speed promise.
Any eventual full-size acceptance needs separate run authorization.

The owner has approved implementation under the three review stops and the
coordination rules above. The active Goal has not been marked complete. At
dispatch, no new runtime code is modified and no full W1/W4 run is restarted.
No push, merge, release, cleanup or installation. Preserve 16-GiB planning/RSS,
two Cargo jobs, and the existing 2026-09-27 20:45:50 UTC unattended checkpoint
unless the owner subsequently changes it.
