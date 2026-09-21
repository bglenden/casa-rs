# Bounded streaming cube replacement plan

Truth class: user-approved implementation plan, not an accepted architectural decision
Last reality check: 2026-09-21
Status: milestones 1–3 approved; local preservation checkpoint complete
Review: GPT-6 Pro, [conversation](https://chatgpt.com/c/6aaff6ec-fdbc-83e8-b7eb-8ca40186f3db)
Verification: just docs-check; git diff --check; source pins compared to tested binary

## Start here after context loss

Read this file, then the single current work record:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.
The durable copy of this plan, Oracle prompt/answer, and installation assessment
live outside the worktree at:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/imaging-replacement-plan-20260920/`.

This is a replacement of the cube's data/execution organization, not another
small optimization or an independent rewrite of scientific algorithms. Keep new
code visibly segregated; extract and reuse established numerical kernels.
Do not resume the old autoresearch controller or optimize retired attestation.
Do not infer that an old uncommitted experiment was promoted.

**Current implementation:** the segregated reconstruction `streaming_cube` module
now has shared flat native blocks and direct whole-row worker preparation,
borrowed compact rows, contiguous band grids, exact row support, native prediction
and residual/normal accumulation. The numerical/runtime replacement still uses a
test-only seam, not a production selector. The current check/result and next
executable action are in CURRENT.md. The active Goal covers milestones 1–3.
Local pre-preparation-refactor checkpoint: `608ff13395edc604d5ee8db403db52b8c5da59e8`.
The verified direct-preparation candidate and revised skills are preserved at
`e8729a9058` before the row/channel-kernel change. That change lends numeric source
channel slices, computes group weighting once, and fills native arrays without
constructing `WeightingSelectedSample`/`WeightingSampleValue` per correlation.
Natural weighting retains its global exact sum and shared per-value flag policy;
it does not use the density-weighted cube's resampling state. Current validation
and application measurements, including rejected attempts, remain in CURRENT.md.
The user additionally authorized local Obit timings as reference only, including
the isolated native CPU core/task setup described below.

## Outcome and limits

Target one bounded cube execution path with a compact band-addressable native
visibility store, explicit model-epoch
reads, exclusive output-band writes, and direct owned products to the existing
CASA-compatible writer. Remove displaced replay compilation, orchestration,
storage and APIs when their consumers migrate. The first discriminating slice is
the existing natural-weight, standard-gridder, Clark, LSRK-linear cube. That slice
does not complete all imaging-mode or full-size T55 obligations.

Preserve selection, spectral interpolation/extrapolation, prediction frequency,
polarization, weights/flags, compensated sums where required, PSF/normalization,
CLEAN stopping, masks, WCS, beams, final complete-data residual, and product
inventory. Preserve bounded streaming and correct cancellation/I/O error
propagation. Individual images replace atomically; publication failure leaves an
incomplete run requiring rerun, not whole-set rollback or per-member recovery.

No production product/model attestation, verification-only full-array passes,
whole-plan hashing on progress, or new runtime fallback. Independently justified
persistence-boundary checksums and diagnostic fingerprints remain distinct.

Resources: 16 GiB shared native planner; sampled 16 GiB process-scope RSS guard;
two Cargo jobs; `CARGO_INCREMENTAL=0`. No full MeasurementSet materialization or
per-worker duplicate full cubes. Strict interactive time cutoffs are suspended;
unattended work still needs an agreed checkpoint. No push, merge, release,
cleanup, another optimization controller, or full-32GB run.
The user increased the memory allowance on 2026-09-20 after candidate12; older
4-GiB-planning/8-GiB-RSS observations remain labelled as such. The allowance is
a ceiling, not a requirement to maximize resident bands or fill available RAM.

First-implementation performance acceptance on the same 42,120-row / 512-channel /
512x512 workload: new W1 must be no slower than matched CASA serial (currently
66.526070500 s), and new W1 / new W4 must be at least 3.0. Include preparation,
intermediate I/O and publication; do not pad the serial baseline. Use one matched
observation initially, repeating only to resolve ambiguous timing. All seven
products / nine full-field CASA checks and panels remain required. Obit numbers
are reference evidence, not a substitute acceptance target.

Approval of the final brief must explicitly cover the new execution/ownership
boundary and the staging/cutover strategy. Do not edit accepted ADRs silently.
Maintain ADR-0010's existing resource authority and ADR-0011's scientific coupling;
if a necessary departure is found, propose exact supersession before implementing it.

## Measured starting point

Working tree:
`/Users/brianglendenning/.codex/worktrees/5d43/casa-rs`
Branch: `codex/t55-serial-autoresearch`
Pre-checkpoint HEAD: `0d4ed221fe376963676222cdf543b9392a2d88db` plus the preserved changes.

Durable benchmark root:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/q-band-rebaseline-20260918/overnight-scaling-20260919`.

| Implementation | One worker | Four workers |
| --- | ---: | ---: |
| Retained rows4x parent | 97.718 s | 86.119 s |
| Experimental initial-plane dispatch | 99.301 s | 85.423 s |
| Matched CASA 6.7.6.14 serial | 66.526 s | not measured |

These are single observations. The latest dispatch is scientifically passing but
not a convincing end-to-end win. Its roughly six-second initial-stage saving
mostly disappears elsewhere; the available stage totals are not an exclusive
ledger. Do not attribute the offset without evidence.

Parent binary: `rows4x-candidate-fixed/application`, SHA256
`dd7d737bc53ec32dbabef317d4f3b14a99f5faad9ced131f301d4a6be10d48c3`.
Experimental binary: `initial-planes-candidate/application`, SHA256
`da98a66cbabb4a951efd45b5cc7506010ca888feb4ba25bd8c3c44d3b3a7f84c`.
Their adjacent `source.json` records tracked source; experimental
`untracked-source.json` additionally preserves the new initial-plane module.
On 2026-09-20, byte comparison of the current tracked diff and initial-plane
module against these saved contents passed. The only added files since that
source pin are research/design documents. There are 35 modified tracked files
and three untracked files, with no staged changes. The approved explicit 38-file
manifest `checkpoint-files.txt` was committed locally as
`aa076d36c6f6fc45738bde2095a6d05ab1ef8005`. The worktree was clean immediately
afterward. This preserves the tested experimental source; it does not promote it
as a performance win. Six Markdown hard-break whitespace warnings in that faithful
checkpoint are being removed in the next documentation edit, without amending it.

Workload: corrected simulated VLA Q band, 42,120 genuine rows, 512 selected
channels, two correlations, 512x512 images with 640x640 padded grids; MS logical
size 363,586,532 bytes. Natural weighting, standard gridder, Clark, LSRK linear,
all pixels/channels and PB output. Request iterations 9, cycle iterations 1,
max majors 3, gain 0.1; realized two majors and 31 iterations. This is not a
terabyte I/O test. Data/output trees are under
`/Volumes/GLENDENNING/casa-rs-evidence/t55/q-band-rebaseline-20260918/overnight-scaling-20260919/rows4x`.

Initial compilation expands 43,130,880 source correlation samples to
21,523,671 reduced groups / 107,616,600 records and 4,305,043,032 artifact bytes.
Final replay reads about 4.305 GB in total, not ten full rescans. Its prediction
and accumulation records encode real scientific work, not removed sealing.
Ten initial slabs are advanced sequentially. The four-worker dispatch parallelizes
plane accumulation within a slab, not complete independent channel lifecycles.

Existing evidence: 146 focused tests pass; both new native runs match all seven
parent product fingerprints; full-field CASA comparison passes seven products /
nine numerical checks including beams, with panels inspected. Native sampled
RSS is about 3.73–3.87 GB; CASA about 7.40 GB. The equal 4096-MiB requested
planning settings do not imply equal actual memory consumption.

## Evidence and reuse map

Strongly related implementation:

| Existing owner/path | Reuse or replace |
| --- | --- |
| runtime `spectral_cycle.rs::run_stream` and `SpectralCycleExecutorState` | Replace cube lifecycle/serial slab orchestration, not wrap it |
| runtime `spectral_cycle_plan.rs`, `complete_data_operator.rs` | Reuse resource admission; remove replaced cube-specific replay/planning branches |
| reconstruction `spectral_operator.rs`, `spectral_sampling.rs` | Extract/reuse exact gridding, polarization, resampling, normalization and FFT semantics |
| reconstruction `gridded_normal_operator/spectral_records.rs` | Reference for complete forward/interpolation/accumulation dependency; retire cube consumer after replacement |
| runtime `bounded_stream.rs`, `reconstruction_executor.rs` | Reuse admitted worker team, bounded work and ownership transfer; no second pool |
| casa-ms selected-observation access | Reuse bounded reads/selection; do not put reconstruction science in the storage owner |
| products generation and application `casa_product_sink.rs` | Retain bounded direct generation/write, CASA formats and image atomic replacement |

The seven inspected hotspot files total 34,871 lines, including tests and other
modes. This is not a promised deletion count. The application entry points remain
`execute_continuum` / `execute`; do not expose a second application imaging API.
Before code changes, list exactly which cross-crate cube symbols will disappear;
count those exports before/after, rather than counting unrelated public APIs.
Medium/conditional scope includes weighting preparation, final visibility writes,
and non-cube consumers of the replay store. Caller evidence decides shared
extraction versus deletion.

Inspect the matched pre-cutover kernels at
`fff9c2d553eace4b6a57b1df9ded4773f2263ceb`; the old outer owned-plane executor
is in its parent. Reuse ideas, not deleted packages or its unbounded result
collection. CASA/casacore remains the scientific reference. Obit is a reference
for locality, flat buffers and scheduling, not a replacement science contract.

## Simple base data structures: explicit user direction

On 2026-09-20 the user emphasized simple visibility buffers, complex grids and
other base data structures as a prerequisite for effective optimization, and
explicitly allowed temporary old/new data-structure coexistence with a scheme
to eliminate the displaced structures. This sharpens the plan; it does not
authorize a permanent dual implementation or automatic runtime fallback.

**Data containers describe data, not the imaging workflow.** Keep resource
admission, scheduling, model-epoch association, provenance, persistence and
publication policy in their owning modules, not embedded throughout the numeric
arrays or replicated in every sample. Necessary block/job/run associations stay
explicit at those coarser levels. Simplicity must not discard science metadata.

| Base structure | Intended interface and representation |
| --- | --- |
| Visibility buffer | A bounded set of contiguous numeric arrays for complex values, weights and flags, with explicit row/channel/correlation layout; row geometry once per row and shared spectral/correlation descriptors. No heap object, reference-counted handle or polymorphic wrapper per sample. |
| Complex grid | One owned contiguous complex allocation with explicit shape/layout and cheap borrowed views of disjoint planes or bands. Compensation and other required arrays are explicit sibling buffers, not hidden object state. |
| Real image/model buffer | The same simple ownership/view principles, preserving existing element precision and axis semantics. No eager whole-cube copy to obtain a view or model epoch. |
| Kernel/mapping data | Compact immutable coefficient/mapping tables shared where useful; row-dependent support remains explicit. FFT plans, caches and I/O handles are separate from grid elements and sample payloads. |

These describe the target shape, not four mandatory new wrapper classes.
Prefer existing ordinary vectors/slices and small shape descriptors. Select
array-of-structures versus structure-of-arrays and strides from the actual
traversals/FFT interface, not a universal tensor framework. Document the chosen
layout, element precision, ownership and permitted mutation where the type lives.

Hot kernels take slices/views and explicit scalar or small immutable parameters.
No per-element allocation, locking, reference-count churn, dynamic dispatch,
hidden I/O, or hidden materialization in indexing/accessors. This does not ban
useful block-level interfaces or a reference-counted immutable kernel table.
Construction/admission checks shape, capacity and association; retain necessary
validation when values/support change and ordinary safe bounds checks, not
repeated whole-buffer validation to enter every kernel.

A small interface alone is insufficient: the physical representation and hot
access path must also be simple. Do not hide the old object graph behind a new
name. Extract shared numerical kernels so they operate on the new simple views.

Acceptance for the first slice: show the buffer/grid definitions and actual
gridding traversal; record allocations, bytes per sample/row/plane, copies and
old/new conversions. Tests must cover shape/layout, alias-free partitioning and
buffer reuse. A borrowed view must not allocate or copy array contents.
Representation simplicity is an acceptance criterion alongside correctness,
bounded memory and measured whole-task performance, not optional later polish.

## Representation decision

Build **one private run-scoped native store** with native-channel tiles and bounded
row blocks, followed by uniform band execution. Preserve selected values at their
current precision, addresses, correlation groups, flags/weights and row spectral
geometry. Store metadata shared by a row once where valid; no per-sample object
graph. Do not drop flagged/zero-weight data needed for prediction or interpolation.

The store replaces the covered cube's flattened normal records, not the MS format
or scientific operators. Preparation is inside the task timer. Reuse native
predictions only for identical epoch/sample/polarization/frequency/kernel inputs.
Do not assume every degrid record is redundant. Start with prepare-then-execute,
without special first-slab overlap; its loss is an explicit performance risk.

A direct repeated MS traversal is the narrower alternative, but retains the
repeated preparation/traversal cost we are testing. Keep the compact-store choice
for this single vertical experiment. Factorized geometry replay is a later
alternative only if measurements show recomputation defeats the compact route;
do not implement three variants or add a runtime representation selector.

Deterministic structural checks: no covered-cube flattened normal artifact,
bounded native-store directory/writer buffers, preparation once for this selected
input, no new production attestation passes, and all actual per-epoch/band reads
counted. A new intermediate store must not become an unbounded replacement
bookkeeping system. Reuse appropriate existing storage primitives and integrity
checks at actual persistence boundaries; no redundant whole-store verification.

## Proposed segregated modules

Use a semantic namespace `streaming_cube`, not permanent `v2` or `legacy`
APIs. This is a small initial layout, not a requirement to manufacture files:

- `casa-imaging-reconstruction/src/streaming_cube/{mod,band}.rs`: numerical
  operation, scientific dependency support, compact borrowed input and owned band
  state. Merge files if a separate boundary earns no value.
- `casa-imaging-runtime/src/streaming_cube/{mod,plan,input,execute}.rs`: native
  store, resource plan and scheduling through existing runtime authority; input
  consumes existing selected access, not a new MS reader.
- `casa-imaging-application/src/streaming_cube.rs`: composition only if needed;
  otherwise a direct call from the current application composition.
- Focused tests adjacent to the new numerical/runtime owner; application comparison
  extends the existing T55 harness, not a new production CLI.

Shared science stays in clearly named existing helpers or is extracted once to a
neutral sibling within its current owning crate. Never copy the entire historical
operator into the new directory. Keep the old files in place during test-only
comparison, and list displaced symbols in the cutover record. Do not rename a
large historical tree just to make it look segregated.

### Temporary coexistence and mandatory retirement

Old and new base structures may coexist during migration, as explicitly allowed
by the user. Keep new definitions in the segregated modules and keep them
independent of historical containers. A necessary migration adapter converts at
a named block/job seam, not repeatedly inside the sample/pixel loop; account for
its copies and peak simultaneous storage. Do not force a worse new representation
solely to preserve an old interface.

Initial retirement inventory (all paths below are relative to reconstruction
`src/` unless qualified). These old containers are frozen against new production
consumers. The table describes migration obligations, not completed deletions.

| Displaced representation | New representation | Remaining consumers and temporary seam | Acceptance / deletion point |
| --- | --- | --- | --- |
| `NativeSpectralGroup`, `RetainedNativeSpectralGroup`, `CasaResampledGroup`, `CasaLinearRowResampler` | `streaming_cube::VisibilityRow` borrows flat channel/correlation arrays; `RowAccumulator` retains only a cursor and the previous inline prediction | `spectral_operator.rs::CompleteDataOwnerState`; `gridded_normal_operator.rs::GriddedNormalOperatorCompiler` and `gridded_normal_operator/spectral_records.rs`; their adjacent tests; `weighting.rs` uses retained-group size for admission. No production adapter added yet. | Native prediction/residual/normal and flags/weights/chunk coverage; remove covered cube consumers at milestone 4, then delete types when other-mode consumers migrate. |
| `SpectralSlabOperator`'s separately owned per-plane arrays and lifecycle state | `streaming_cube::BandWorkspace`: contiguous `[channel,x,y]` arrays and borrowed disjoint plane views, geometry/FFT separate from elements | `spectral_operator.rs::CompleteDataOwnerState`, `spectral_operator/initial_planes.rs::InitialPlaneBatch`, adjacent operator/initial-phase tests. Test-only `streaming_cube/reference.rs::Reference` uses the old operator for comparison. | Bitwise fixed-model gridding and full application acceptance; delete cube construction/initial-plane plumbing and reference seam at milestone 4. Other spectral bases, W/AW and mosaic still need explicit migration before deleting the shared owner. |
| `ReducedRecordKey`, `RecordRole`, `StandardRecordScratch`, cube use of `GriddedNormalCompilationPlan` | Direct native-pair prediction/accumulation; shared reconstruction `streaming_cube/input.rs::NativeBlock`, runtime `streaming_cube/input.rs::{NativeStoreWriter,NativeStore}` hold the original native payload in bounded tiles | `gridded_normal_operator.rs`, its `compilation.rs`, `spectral_records.rs`, `bounded_records.rs`, `two_domain.rs`; runtime `complete_data_operator.rs`; reconstruction `tests/major_cycle.rs` and `tests/support/gridded_frames.rs`. The new store is test-only pending application integration, with no old-store adapter. | No flattened cube artifact; complete-data numerical/I/O/resource checks and end-to-end timing. Remove covered cube compiler/store calls at milestone 4; shared non-cube replay remains until its named migration. |
| Cube preparation's `WeightingSampleValue`/`WeightingReplayChunk` payload | `NativePreparationWorker` consumes borrowed row/channel correlation slices through shared weight/taper/finite-value primitives directly into `NativeBlock`; no rich per-sample intermediate | The complete comparison path uses runtime `weighting/native_preparation.rs` and the existing bounded team. `NativeInput` is now entirely test-only; generic non-cube weighting still owns its replay route. | The covered cube's indexed/prepared-sample collection, weighted-chunk conversion and rich per-sample packing API are removed. Scalar/group/worker/batch tests cover the replacement; current application timing and CASA acceptance are required before promotion. |

No application API was added or removed. Nine internal friend-surface exports
(`NativeBlock`, `NativeLayout`, `RowMetadata`, `NativeWeightingPreparation`,
`NativePreparationWorker`, `BandPlan`, `BandMemory`, `EpochBand`, `PreparedFft`)
share the flat payload, preparation, numerical band owner and recyclable FFT
across reconstruction and runtime. `NativeInput` is no longer exported. No
duplicate runtime payload buffer or scientific kernel remains.
Runtime's future cube migration removes its use of
`GriddedNormalCompilationPlan`, `GriddedNormalOperatorCompiler`,
`GriddedNormalOperatorProgram` and associated replay work/storage operations;
the shared exports cannot be deleted while other modes still consume them.
Inventory those consumers again at the actual runtime/cutover boundary.

The first payload layout is explicit: Complex64 values (16 bytes), f64 weights
(8), and two distinct one-byte flags per correlation sample; u32 selected-channel
indices plus f64 transformed frequencies per native channel; address/UVW/phase
and original frequency-pair geometry once per row. `VisibilityRow` itself only
borrows these arrays: no conversion or payload copy. Current tests own bounded
rows in ordinary arrays, not the historical sample graph. For P padded pixels,
the seeded initial phase owns six complex output arrays (96 P bytes per output
plane), one complex forward array (16 P bytes per support plane), and 24 bytes
per output plane for sum weight, compensation and mapped count, plus kernel/FFT
storage. The certified-empty initial phase owns four output arrays (64 P bytes
per output plane), no forward/residual arrays and no model read window. A refresh
owns only two residual arrays (32 P bytes per output plane) plus forward support;
it moves existing PSF/sensitivity/weights instead of forming them again. Empty
arrays have zero payload allocation. `BandPlan::memory` projects preparation,
accumulation, completion and retained-result bytes with checked shape arithmetic.
It includes full prior state while new grids allocate, one loaded model plane,
FFT construction/residency, convolution storage, and image allocations overlapping
grids. Runtime `WavePlan` sums every resident band's peak, not only the number of
workers, and adds the shared source arena, one/two decoded slots, collection
storage, stacks and externally owned shared bytes. Application admission still
has to supply the complete external-owner term and select bounded wave shapes.

`EpochBand` now holds an immutable borrow of the exact `ModelGeneration` while
preparing and executing a band. Preparation reads one canonical y/x support plane
at a time through the existing bounded model owner, preserves invalid support,
and fills only that band's forward grids with shared correction/FFT kernels.
Completion rejects a different model object before doing inverse FFT work;
result primitives carry the existing generation identity. No model copy, hash or
verification-only read is used to associate the job with its epoch. The global
controller barrier and source/run completion checks still belong to runtime;
this small owner does not by itself prove complete-input coverage.

The certified-empty initial dirty image is already the residual and moves directly
to the controller slot; no invariant dirty clone is needed by direct-native
refresh. Seeded initial work retains its independently computed data-side dirty
image by ownership transfer. Refresh checks the prior band's shape/inventory,
releases its obsolete residual and moves its invariant buffers into the new
completion. The run owner must match source/weighting association before this
local handoff. No content verification pass is introduced.

Post-FFT image formation now shares `append_image_plane` with the historical
operator, preserving crop, axis order and standard versus CASA-float correction.
Generated-finite checking is fused into that calculation. Forward grids and
compensation arrays are dropped before image allocation. `completion.rs` supplies
the narrow temporary handoff into existing `SpectralOperatorPrimitives`, not a
historical slab operator. It moves the exact residual into the controller-facing
slot and data-side dirty values into the invariant slot. This removes the old
dirty clone and separate pre-promotion residual slot: one 16-byte-per-image-cell
copy and the corresponding redundant normal-storage field are not needed by
this representation. The shared normal-state storage/fold/controller remains;
its runtime completion binding reuses the existing scheduler fence. No new public API.

Tests prove bounded support-only reads, y/x conversion and invalid support,
model read errors and insufficient windows, rejection of a delayed foreign-epoch
job, bit-exact completed primitives against the promoted historical result, and
shape/blank/unmapped rules. Runtime now schedules real `EpochBand` jobs through
the existing bounded executor, including FFT completion on its installed team.
Tests compare initial and changed-model refresh results across W1/W2/W4, band
depths 1/2/4 and one/two source slots. They check decode counts, moved invariant
buffers, byte-admission boundaries, unmapped waves and joined model-read errors.
These remain focused scheduled-science proofs, not full-application acceptance.
The preparation/controller fixture uses actual selected-observation inspection
and worker-local natural weighting, with reusable flat native buffers.
`NativePreparation` derives band support while writing borrowed row partitions,
and rejects incomplete, duplicate or out-of-order rows. Completed bands
move through `CompleteDataOwnerResult::from_streaming_cube` into the existing
normal-state fold and `MajorCycleOwner`, preserving terminal replay identities
with zero additional coverage-hash work. Runtime `NativePreparation::traverse`
now consumes the real `WeightingExecutionState` bounded selected-source traversal.
The store cannot authorize reconciliation: `complete_replay` still runs only in
the scheduler's settled observation-read callback. Runtime's segregated
`streaming_cube/completion.rs` transfers completed bands into the existing
`CompleteDataSlabResult`/fold with the exact problem, attempt, lease, planned
reconciliation node and settled source predecessor. No new completion authority
or array scan is introduced. The real-MS runtime fixture covers successful
reconciliation, source-fence failure and a wrong reconciliation node; current
verification is recorded in CURRENT.md. The real-source fixture now uses
`CubeStatePlan` for admitted model and normal storage, including its exact
capacity/FD and immutable-heap retention boundaries. The integrated comparison
now uses a contiguous resident normal array when both live generations and the
minimum workspace fit; larger cases use the existing paged backing under the
same scientific ownership interface. The native initial-empty/residual-refresh
chain admits its actual five f64 fields per pixel. The comparison heap census
does not charge that exact retained payload twice; it never subtracts an estimated
reservation or rereads array contents. Model storage remains paged. Initial-band prefix selection
uses the same phase-byte projection as execution and drains each completed wave
to the existing fold before admitting another. `streaming_cube/plan.rs` now composes
the native phase after those source/weighting/storage reservations. Its workspace
is the lesser of the full initial-wave shape bound and remaining authority-owned
memory, but must fit preparation and every minimum one-band wave. The bound
includes preparation arenas/cache, all retained support metadata and row-range
scratch, source slots, worker stacks and explicitly supplied enclosing owners.
Normal storage separates epoch-specific dirty/residual arrays from independently
owned immutable PSF/sensitivity arrays at initial construction. Residual refresh
shares the invariant owner directly and writes only the newly calculated residual;
it never loads old residual arrays merely to discard them or rewrites invariant
arrays. The prior complete state survives candidate failure. Source/operator
associations and model epoch are checked without content attestation, and only
ordered complete residual coverage transfers to the existing CLEAN controller.
Each backing carries its own logical memory allocation/permit; shared aliases
are charged once and do not retain previous residual generations. Physical
encodings, persistence checksums and CASA output formats are unchanged.

Resident normal storage has a stronger admission condition: its fully composed
reservation must leave room for preparation and a complete wave with one band
per useful worker (capped by the number of bands). The wave bound uses the actual
phase's grids and model support; no prior normal windows are materialized during
residual refresh. Otherwise the existing paged storage
keeps that workspace available; genuinely low memory can still constrain the
wave. This prevents a residency threshold from stranding otherwise usable workers
without increasing the memory ceiling or adding a dataset-specific threshold.
Support-vector growth is capped by output channel count; it is not preallocated
as a full band-by-channel table. Worker claims and execution configuration agree.
The native file itself owns its retained capacity/descriptor permit. Full
application admission still needs actual enclosing-owner sizes, input block/tile
selection validation at the full workload and cross-attempt native-store retention; this is not a claim of a
complete application plan or timing.

`NativePreparation` now binds its shape and output frequencies to the compiled
selection. Workers construct `NativeLayout` from their first real selected sample;
no fixture address or second input traversal is used. Worker-local `NativeInput`
packs each transient shared-science result directly, then `PreparedNative`
transfers layout, store and band plans together. The
initial scheduler executor lives in `streaming_cube/phase.rs`, with source
polarizations and channel counts derived from the problem rather than fixture
constants. Failure injection remains test-only. Runtime comparison coverage
includes real RR/LL and XX/YY MeasurementSets. `InitialCube::plan` now composes
the source/weighting, paged-state and native reservations through the existing
physical planner. It transfers its planned bands into preparation without
recompiling them in the read node. `NativePhasePlan::for_initial_source` derives
native row blocks from the selected source I/O-buffer envelope (at least one
complete row), independently of worker/source callback boundaries, and tiles from the
projected memory-bounded wave count. The provisional
full-channel encoding arena bounds the narrower chosen layout; actual spectral
support is still discovered from rows, not approximated by that I/O tiling.
Tests cover smaller memory allowances and 16k-channel storage shapes, not a
16k-channel dataset timing. Complete application composition, later-epoch import
and shared minor-cycle wiring now run through the private compile-time comparison
seam. The application reuses its existing stopping/mask loop and product writer.
The macOS comparison conservatively charges the enclosing process's live heap
in addition to explicit phase bounds; this is not a portable production cutover.

The one-use `InitialMajorPhaseCompletion` wrapper has been deleted. Its unchanged
mask/coupling/minor-cycle operation is now the crate-internal
`MajorCycleOperatorResult::run_reconstruction_cycle`, shared with the historical
caller instead of introducing another CLEAN controller. The existing
`spectral_cycle_plan::base_physical` source/transaction skeleton is reusable
inside the runtime; it does not allocate historical slab operators or replay
records. Later epochs transfer native storage and the original source completion,
using the existing source-free reconstruction transaction and bounded prior-normal
windows rather than rereading the MeasurementSet.
The temporary completion
adapter retires with the old primitive representation if that owner is later
replaced; it must not become a second normal-state/controller implementation.

Shared science remains in `StandardConvolution`, `PreparedFft`, polarization and
spectral sampling helpers. Convolution methods now accept zero-copy ndarray
views without changing their scalar loops. Scalar pair interpolation is shared
with the old resampler, preserving observed/predicted arithmetic separately.
The focused dependency test rejects old containers, replay records, I/O and
per-sample reference-count/lock owners in the new numerical implementation;
`reference.rs` is its explicit test-only exception, deleted at cutover.

Direct preparation borrows source ranges without constructing a channel-run
index. Whole rows are partitioned over the existing admitted team. Workers use
the shared spectral projector, weighting kernels, `accept_polarization_input`
and exact sum accumulators, promoting Float32/Complex32 once to Complex64 in
flat native arrays. There is no retained weighted-sample batch or replay-chunk
conversion in this cube route. Exact integer bins merge without changing the
sum; row-bound spectral calculations never split between workers.

The existing weighting coverage fields are encoded on workers into canonical
row digests. Ordered joining binds those digests with row identity and terminal
counts in a native-specific private domain, independent of worker and batch
counts. This is selected-source coverage, not product/model publication
attestation. Original source inspection and run/fence completion remain with
their existing owner. No new payload reread is introduced.

`NativeStoreWriter::append_parts` encodes ordered borrowed row partitions directly,
without concatenating native arrays. Incomplete rows, mixed source/layout,
duplicate/out-of-order cells and post-error completion fail. Sink I/O errors
retain their original code. Admission counts worker flat arrays, exact-sum
state, source/projector/cache scratch and the writer encoding arena; displaced
weighted-block/index allocations are not charged to the native route. Old chunk
packing exists only as a test reference, not a fallback.

The initial native store represents one homogeneous selected channel/correlation
layout. Source/selection/correlation descriptors remain coarse shared input
metadata; no public persistent schema or reopen/import route is introduced.
Each row block has one metadata frame and then native-channel tiles. Fixed shape
arithmetic supplies every offset, including short final rows/tiles: there is no
resident directory. The writer owns one open private file in the already admitted
`ManagedSpillStorage` directory and transfers that handle directly on successful
row/shape/length completion. It does not reread content at completion. A failed
write cannot complete; a short/error read fails rather than becoming EOF.

For C channels and P correlations, a serialized row costs `56 + C*(8 + 26*P)`
bytes; each metadata/tile frame adds a 4-byte CRC. The CRC includes the expected
frame ordinal and checks truncation/corruption/misplaced frames on actual reads.
Its concrete consumer is the private persistence reader, not product publication.
The preparation row count is derived from the admitted native-block plus encoded
tile arena budget. Its formula counts flat payload capacities and vector headers;
the parent must additionally count the file/path owner, source input, and one
page-aligned cache window, including a possible extra boundary page. There is one
reused encoding arena and one decoding arena per admitted source slot, with no
per-sample allocation. Preparation/serialization, reads, checksums and copies all
remain inside the eventual end-to-end timer.

`NativeSource` implements the existing `OrderedBlockSource`: a mutable store reader
owns file I/O, and one or two executor-owned decoded windows are borrowed by
disjoint partitions. One worker team is reused across all row blocks. This
avoids duplicate worker input buffers and retains existing page-cache control
(`F_NOCACHE` on macOS; release and verification on Linux) without concurrent reads
repopulating pages during strict release checks. This does not serialize numeric
band work or introduce another worker pool. Buffer-source admission and epoch
jobs are now composed in `execute.rs`; source/run completion is connected to the
existing application controller and writer. The source separately reports its
encoding/cache arena and each decoded-slot capacity, reads each row/tile frame
once per wave, requires exact ordered terminal coverage, poisons failed/cancelled
input and preserves original errors through the bounded executor.

Preparation unions all bands' native/model support in one native-pair traversal
per row, rather than rescanning the native axis independently for every band.
One temporary range per band is reset per row; retained model-channel lists merge
the complete contiguous native closure, including interior samples that emit no
fine channel. Reference tests cover reversed axes, gaps, shifts and unmapped rows
and pin pair visits independent of band count. There is no per-row directory.
At execution, `consume_block` recomputes each row's exact
native-pair window inside the shared decoded union, then takes zero-copy subviews.
This matters when Doppler shifts vary: predicting every sample in the wider
union could ask for model planes outside that row's declared closure. The
original global spectral pair and fine-grid phase are preserved when narrowing.

The migration sequence is mandatory:

1. Introduce the simple structures and compare them against the historical path
   through a test-only seam; do not require a flag or another production selector.
2. Move each covered consumer directly to the new structures. Freeze displaced
   types against new consumers; fixes needed to preserve existing science remain
   allowed. A focused dependency check prevents new kernels importing old base
   types except through the explicitly listed temporary adapters/test reference.
3. At capability cutover, delete that capability's old constructors, container
   plumbing, conversions and obsolete tests, replacing behavioral coverage rather
   than weakening it. No failure-triggered retry through the old path.
4. Migrate the remaining in-scope consumers by named milestones. When the last
   consumer is gone, delete the old type, adapters, exports and historical route
   in the same change; remove the dependency-check exceptions. Search and compile
   checks must demonstrate no remaining references.

Unmigrated scientific modes are named remaining work, not justification for an
indefinite legacy layer. A successful cube prototype may be complete while the
broader replacement remains incomplete. Do not declare the replacement finished
with displaced base structures still present, or silently defer their removal.
If another approved capability still needs a type, preserve it and report the
remaining migration explicitly. Shared science schemas and MS persistence types
are not automatically obsolete merely because the new implementation uses them.

Production has one owner per migrated capability and no selectable old/new
algorithm. Temporary data-structure coexistence does not change the existing
implementation/checkpoint approval or resource/scientific boundaries.

## Scientific ownership rule to prove first

An output channel is not automatically an independent evolving CLEAN model.
Current LSRK-linear code predicts at native frequencies using coarse image-plane
contributions, then resamples observed and predicted native pairs into output
channels. Different output bands can therefore read neighboring model channels.

Use immutable model epoch `M_k` as the shared read view and exclusive writable
normal/residual/model-delta bands. Derive each band's native input and model
support from the actual mapping, including extrapolated edges, descending axes,
row-dependent frequency shifts, flags and SPW boundaries. Do not hardcode a
one-channel halo or assume connected bands can advance epochs independently.
Preserve established global reductions, masks and stopping semantics.

For output core B, derive native support S_B as the union of native inputs to
its resampled outputs; derive model support C_B as the union of prediction
contributions to every native sample in S_B. Preserve the original global
output-grid geometry and fine-grid phase: restricting outputs must not rebuild
interpolation as a smaller spectral problem. Use phase barriers before model
mutation and next-epoch prediction; avoid two full model copies. A channel done
with minor CLEAN may still need residual refresh after its neighbors change.

The first test must put nonzero, unequal model values on both sides of a band
boundary and compare prediction plus residual/normal results against the current
operator. Exercise different band widths/worker counts and a chunk ending inside
a native spectral row. Also test edge extrapolation and flag/weight handling.
Perturbing a model plane outside the computed closure must not contribute.
Delay a neighboring job to test mixed-epoch rejection through ownership/run
association, without hashing. Compare blank, flagged and uneven-work channels'
controller decisions as well as pixels. Keep observed and predicted resampling,
weight handling and subtraction in their existing numerical order; Oracle's
schematic native-residual flow is not authority to reassociate arithmetic.
If epoch-consistent output-band evaluation cannot match, fix the dependency model
before building a scheduler. Independent per-plane convergence is not an initial
goal and needs separate scientific evidence.

## Milestones and exit evidence

1. **Checkpoint and dependency proof.** Record the full dirty baseline and existing
   experiment status; checkpoint only the identified in-scope work with approval.
   Add the cross-band reference test above. Capture the precise contract:
   which native samples/model channels feed each output band, and at what epoch.
   Exit: focused test distinguishes correct shared-epoch execution from stale or
   truncated support. Then implement the bounded native store and direct band
   operator on this fixed-model fixture, with truncation/read-error checks and
no expanded normal artifact. Start from the simple buffers/grids above and
   populate their retirement table as types are introduced. Resolve current model-window/controller/writer
   lifetime details locally here. Do not begin with another timing campaign.

2. **One complete serial replacement slice.** Implement compact input with the
   established scientific kernels, from selection/preparation through initial
   normal state, CLEAN, final residual and all existing products. Choose one
   representation: the native store above; do not build several production variants.
   All preparation, intermediate storage and repeated input passes are counted.
   Prepare selected native data once; later bands/epochs read exact bounded
   store support. Do not promise one scan of the entire compact store when all
   channel grids do not fit. Record reads per band wave/epoch and halo reuse.
   Use the current tested initial-plane source checkpoint as the baseline, not
   whichever older number is fastest. W1 first, then W4; add W2 only if it answers
   a scaling question.
   Exit: same-mode numerical checks and one end-to-end W1 comparison; no fake gain
   from moving compilation outside the timer. If clearly slower, retire the
   hypothesis or select one evidence-backed correction; do not micro-polish blindly.

3. **Bounded band parallelism.** Reuse the worker team, exclusive band ownership
   and immutable model epochs. Include FFT, minor cycle, final residual and bounded
   completion/output handling, not just the initial grid loop. Avoid nested
   oversubscription. Exit: focused worker/partition/resource invariance, then one
   W1/W2/W4 observation as needed to show actual scaling and limits.

4. **Application cutover and deletion.** Only after correctness/performance supports
   retention, migrate the canonical covered capability directly and delete its
   old constructors, state, replay compiler/store consumers and adapters. Audit
   remaining consumers before deleting shared mechanisms. Replace old-shape tests
   with ownership/behavior tests; update docs, examples, architecture checks and
   issue/PR acceptance wording at the established milestone, not after every probe.
   Exit: no alternate old route for the migrated capability; retirement rows for
   its displaced structures closed, adapters removed, remaining-mode consumers
   explicitly inventoried; independent contract review at the existing boundary.

5. **Scale and complete remaining acceptance.** Use a larger mode-faithful bounded
   workload only after the local whole-path result; preserve serial comparison as
   well as worker scaling. Full-mode and full-size obligations remain open, not
   silently waived. The full-32GB run still needs separate authorization.

Performance aim: meaningful end-to-end progress toward CASA parity plus useful
multi-worker scaling under stated memory. CASA parity requires roughly a one-third whole-task reduction from the
latest 99.301-s W1 observation; this is an aim, not a forecast. No invented 10x
promise, no promotion
from a component-only win, and no significance claim from one observation.
Repeat only when variation makes the decision genuinely ambiguous; no six-pair
requirement.

## Memory, queues and failure contract

Plan from actual simultaneously live allocations, not dataset-specific slab sizes:

```text
peak = shared + bounded_input + model_read_support
     + sum(active_band_private_workspaces)
     + bounded_completed_output + stacks + library_headroom
peak <= native_budget
```

Use max over phase peaks only when their allocations actually stop overlapping;
otherwise count their live union. For Complex64/f64 grids, bytes are
16 * padded_pixels * complex_array_count + 8 * padded_pixels * real_array_count,
including compensation arrays; use actual types for other arrays. The current
homogeneous native store derives offsets from shape and needs no resident index
or directory cache. Bound writer arenas and open files as well as read buffers.

Each term comes from shape, element widths, layout/capacity and liveness; shared
aliases are charged once. Model read support must remain coherent across the
epoch and must not secretly materialize the full cube. Worker count is bounded by
requested workers, useful runnable bands and the feasible sum above. Band depth,
input block rows, queue capacity and any scratch spill are explicit planner
results. Reject the request if no supported minimum fits; do not use an old path
as fallback. Overflow and budget-boundary tests are required.

Completion backpressure must bound bytes, not just a result count; include outputs
held by workers blocked on enqueue. Permit only a bounded window of out-of-order
work, or direct disjoint bounded writes if the writer contract supports them.
A slow early band must not grow an unbounded reorder map or deadlock the team.
Keep the current serialized writer initially; workers transfer bounded outputs,
not concurrent mutations of one CASA writer. Avoid nested same-pool waits and
nested FFT library threading. A read error must not be treated as EOF.
Stop admission on cancellation/error, drain/join owned work and I/O, propagate the
original failure, then release resources. Do not publish success for a partial set.

## Verification and restart commands

Reuse the established harness; do not run it until implementation is approved.
Tooling root:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/q-band-rebaseline-20260918`.

- `initial_plane_checks.py check` runs reconstruction `spectral_operator::`,
  integration `major_cycle`, runtime `bounded_`, and `compile_plan_run t55_`.
  It now also runs `streaming_cube::` and `spectral_sampling::`; retain new-owner
  coverage as the implementation grows.
- `initial_plane_checks.py build UNIQUE_LABEL` preserves a release application
  binary plus tracked and untracked source identity.
- `scaling_4x.py native UNIQUE_LABEL ABSOLUTE_BINARY WORKERS` uses the exact
  42,120-row fixture; never run `prepare` over existing data.
- `scaling_4x.py compare NATIVE_LABEL casa-rows4x` performs the existing full-field
  seven-product/nine-check comparison. Retain and inspect all comparison panels.
- Launch stages through `finish_stage.stage(label, command, None, seconds=None)`
  with `rss_bytes=16 << 30` to enforce the user-authorized 16-GiB ceiling without
  reinstating waived time cutoffs. Builds may retain the default 8-GiB guard.
  The old `overnight_scaling.stage` and CLI deadline are expired: do not use them.
- Preserve the SDK/environment from the successful build receipt; prior build
  failure mixed CLT SDK27 with the selected Xcode linker. Two jobs, incremental off.
- For this documentation-only plan: `just docs-check` and `git diff --check`.

One current status belongs in CURRENT.md: milestone, changed function, hypothesis,
command/result, source/binary identity, unresolved question and exact next action.
The plan is durable design, not a second running diary. Before resuming, verify
HEAD/dirty state against the recorded checkpoint; do not overwrite new edits.
Reuse unaffected green evidence. New source/build/configuration changes invalidate
only affected evidence. Save logs/results outside the removable worktree.

## Oracle disposition and Obit installation

GPT-6 Pro completed the review in Chrome. Its extracted advice and access limits
are in durable `oracle-review.md`; the full conversation remains open at the link
above. It endorses replacing the cube representation/execution boundary, not a
whole-stack rewrite, and the compact native-store experiment described here.

Main-agent local inspection confirmed coarse-plane prediction terms, edge
extrapolation, native-pair carry, separate observed/predicted interpolation and
special native-weight handling. Thus epoch/band closure is a real requirement,
not generic caution. Oracle could not fetch the local HEAD and used supplied
snippets plus older public source; its answer is not a complete current-code
review. Exact controller/model-window/writer lifetime integration remains a
milestone-1 obligation. Its warning about old per-sample allocations is not a
newly verified current bottleneck.

Adopted: native store; uniform prepare/execute; bounded shared-epoch bands;
whole-task decision; deletion at cutover. Rejected as unsupported: automatic
channel independence, a guaranteed speedup, or choosing by expansion ratio.
Refined: preserve actual arithmetic order; x86_64/X11 formula restrictions apply
to Obit helper components, not proof of native-core incompatibility.

Obit source is already available for convenient examination under
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-source-study-20260920/Obit`
at `ebc1c229e5e3870b5ce3c342bddb7313d986a06f`. Installation is not needed to
read it. The user authorized reference-only timings and a pinned ARM64 CPU core/task build in a
durable isolated prefix; not the unmodified full Homebrew recipe. Its Python
binding/GUI assumptions are dated; native core feasibility is still untested.
See durable `obit-installation-preflight.md` for host inventory and build boundary.
Do not delay the representation test waiting for a third benchmark system.

Qualify MS-to-UVFITS conversion, dirty/PSF/sum weights, and **nonzero-model
prediction/residual** before competitive timing. Match spectral semantics,
precision, flags/weights, polarization, geometry and beams; then match CLEAN
stopping/masks/restoration/PB/products. Obit uses float grids: do not imply equal
F64-compensated work. Report both imaging-only and conversion-inclusive workflow
times. A common simpler diagnostic is useful if the full science cannot match,
but is not acceptance for our LSRK case. The authorized isolated build/reference
runs are complete; see the local timing addendum in
`obit-data-structure-source-notes.md` and its durable REFERENCE.md. Dirty outputs
match across Obit modes; CLEAN outputs differ materially. Neither changes this
plan's CASA acceptance or motivates another comparator investigation now.

## Approval and first action

Requested approval covers the identified local WIP checkpoint and milestones
1–3: one test-only native-store/band replacement through full serial and bounded
parallel comparisons, with the existing resource/science limits. Milestone 4's
production ownership/cutover map is presented after evidence; remaining-mode
obligations are not waived. This approval is now recorded in the active Goal.
The subsequent reference-only Obit request authorizes isolated native core/task
setup using installed dependencies, not global package upgrades, GUI/Python
bindings, Rosetta, or a substantial upstream-porting project.
No new public application API, dependency direction, external persisted contract,
accepted ADR change or independent-channel convergence is approved by this plan.

The checkpoint, nonzero cross-band dependency proof, bounded native store,
shared input, model epochs and phase-specific band completion are implemented.
Real epoch-band jobs now use the native source and existing bounded executor
under checked phase-liveness wave admission. The complete application comparison
path has run the approved workload with one and four workers; both passed the
unchanged seven-product/nine-check CASA comparison. Native-buffer batching is
retained after measured end-to-end improvement with identical products. The
serial-parity and 3x scaling requirements remain unmet. Subsequent changes must
target measured complete-application bottlenecks; see CURRENT.md for current
source identity, timings and the one active experiment.
Do not first rerun all past gates, restart autoresearch, or assemble another
architecture survey. The current source pin and unaffected green evidence are
already preserved.
