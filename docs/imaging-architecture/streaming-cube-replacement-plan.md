# Bounded streaming cube replacement plan

Truth class: proposed implementation plan, not an accepted architectural decision  
Last reality check: 2026-09-20  
Status: Oracle-reviewed proposal awaiting implementation/checkpoint approval; no production edits
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

**Next executable step:** after approval of this brief and the local checkpoint
choice, capture the identified dirty baseline in git, then implement the small
cross-band spectral-dependency test described in milestone 1. No timing campaign,
Obit installation, or production rewrite is authorized merely by reading this plan.

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

Resources: 4 GiB shared native planner; sampled 8 GiB process-scope RSS guard;
two Cargo jobs; `CARGO_INCREMENTAL=0`. No full MeasurementSet materialization or
per-worker duplicate full cubes. Strict interactive time cutoffs are suspended;
unattended work still needs an agreed checkpoint. No push, merge, release,
cleanup, new Goal/controller, or full-32GB run.

Approval of the final brief must explicitly cover the new execution/ownership
boundary and the staging/cutover strategy. Do not edit accepted ADRs silently.
Maintain ADR-0010's existing resource authority and ADR-0011's scientific coupling;
if a necessary departure is found, propose exact supersession before implementing it.

## Measured starting point

Working tree:
`/Users/brianglendenning/.codex/worktrees/5d43/casa-rs`  
Branch: `codex/t55-serial-autoresearch`  
HEAD: `0d4ed221fe376963676222cdf543b9392a2d88db` plus substantial dirty changes.

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
and three untracked files, with no staged changes. Proposed preservation
checkpoint: one local WIP commit on the current branch of the explicit 38-file
manifest `checkpoint-files.txt` in the durable plan directory. Do not stage
anything outside that manifest or call the experiment a promoted performance
win. If state has changed, inspect the delta before committing. No commit made.

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

Keep one compact retirement table in this plan, populated with exact symbols and
callers when the first new type is introduced. Each row records: old type, new
representation, remaining consumers (including tests), temporary adapter if any,
acceptance check and deletion milestone. This is not another receipt system.

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
including compensation arrays; use actual types for other arrays. The native
store index is coarse/paged with a bounded directory cache, not one resident
entry per visibility. Bound writer arenas and open files as well as read buffers.

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
  Add new owner tests to the focused selection; do not silently omit them.
- `initial_plane_checks.py build UNIQUE_LABEL` preserves a release application
  binary plus tracked and untracked source identity.
- `scaling_4x.py native UNIQUE_LABEL ABSOLUTE_BINARY WORKERS` uses the exact
  42,120-row fixture; never run `prepare` over existing data.
- `scaling_4x.py compare NATIVE_LABEL casa-rows4x` performs the existing full-field
  seven-product/nine-check comparison. Retain and inspect all comparison panels.
- Launch stages through `finish_stage.stage(label, command, None, seconds=None)`
  to retain sampled 8-GiB enforcement without reinstating waived time cutoffs.
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
read it. Recommend a separately approved, pinned ARM64 CPU core/task build in a
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
but is not acceptance for our LSRK case. No install has been performed.

## Approval and first action

Requested approval covers the identified local WIP checkpoint and milestones
1–3: one test-only native-store/band replacement through full serial and bounded
parallel comparisons, with the existing resource/science limits. Milestone 4's
production ownership/cutover map is presented after evidence; remaining-mode
obligations are not waived. Obit installation is a separate optional approval.
No new public application API, dependency direction, external persisted contract,
accepted ADR change or independent-channel convergence is approved by this plan.

After approval: refresh the explicit checkpoint manifest against git status,
checkpoint that state locally, then add the nonzero cross-band dependency test.
Do not first rerun all past gates, restart autoresearch, or assemble another
architecture survey. The current source pin and unaffected green evidence are
already preserved.
