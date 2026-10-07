# Cube scaling: Obit-informed implementation plan

Truth class: owner-approved staged implementation plan; not a scientific-acceptance waiver
Last reality check: 2026-09-29
Source baseline: `02d337171120c5b5b07b15c30a927e27567585e4`
Status: retained implementation checkpoint; further cube scaling optimization deferred by owner on 2026-09-29
Verification: local source checks, remote checkpoint verification, `just docs-check`; no new timing run

## Current owner decision — 2026-09-29

Keep W parameterized: support more than four workers, including W8 and larger
counts when CPU capacity, available independent work and memory admission permit.
Do not turn this workstation's preferred W4 configuration into a production
four-worker limit, or replace it with an eight-worker architecture ceiling.

The owner explicitly deferred further optimization of the current cube scaling
path until a larger system with more high-performance cores is available. The
current M4 has four Performance and six Efficiency cores; the fresh unchanged-
code fixed-residual observation was W4 185.818 s versus W8 167.600 s (9.804% less
time), with zero residual difference. Admission and timing varied; this is not
proof of a universal software or hardware speedup limit, nor full-W8 CLEAN
acceptance. Retain the working larger-W path and its correctness tests.

Do not restart this laptop's tuning campaign or launch full W8 from the historical
milestone/sweep instructions below. Resume performance investigation on larger
hardware, or after new owner direction, using the existing representative
workload and actual admitted worker/memory counts before proposing changes.
Existing resource and scientific acceptance requirements still apply; this
deferral does not complete T55, meet an unmet scaling target, or remove unrelated
integration/correctness obligations.

Durable checkpoint and exact evidence links (outside removable worktrees):
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.
The sections below preserve the original approved plan and historical baseline;
this owner decision supersedes their permission to continue performance trials
on the current workstation.

## Decision and scope

Keep one MS reader, two reusable raw blocks, one reusable derived-block workspace,
the existing bounded worker pool and exclusive band-grid ownership. Parallelize
coarse preparation work, then join preparation before parallel band consumption;
join all borrowers before recycling the block. Keep the existing CLEAN controller,
major-cycle model boundary and product writer. Do not build independent readers,
permanent asynchronous channel lanes, another queue framework or another CLEAN
controller in this campaign.

This follows the inspected Obit CPU mechanism: shared flat input, persistent
workers, reusable preparation arrays and tight numerical loops, with joins at
work-batch boundaries. It does not import Obit's private grid replicas/reduction,
different channel-selection policy or scientific algorithms. LibRA corroborates
shared-array worker views with explicit dispatch/join; CASA demonstrates parallel
row preparation. See [the source notes](parallel-visibility-buffer-source-notes.md).

The owner approved implementation on the existing Sol thread, one milestone at
a time with main-thread review and repairs between milestones. Cube remains the
immediate scope; do not spread the
changes to MFS before the cube evidence is convincing. The earlier broader
migration/deletion and acceptance scope is not silently removed or declared done.

## Execution and handoff protocol

Sol implements and benchmarks each dispatched milestone before handing back.
It must not stop at passing component tests, start the next milestone itself,
or poll the main thread. The owner explicitly authorizes Sol to send one
completion or genuine-blocker callback to main thread
`01a079dc-23bd-73e1-8a6b-0fd4ccb7f9f3`, then end its turn. The main thread
reviews the code and measurements, fixes findings itself, and dispatches the next
milestone. Neither thread polls the other. Use bounded Luna Max subagents for
large source/output inventories; retain interpretation and acceptance decisions
with the main reviewer.

Keep turnaround bounded: reuse the recorded unchanged parent/reference evidence,
run the necessary current-parent mechanism control and coherent candidate, and
repeat only if noise or changed work leaves the conclusion unclear. Do not repeat
the full deep timing pair per helper edit or create a new multi-hour CASA reference.
The 32-channel deep application and smaller-row full-width multiwave diagnostic
below are complementary checks, not authorization for a full-size 512-channel
restart. Report elapsed time and actual work; an early-stopped diagnostic is not
deep-clean acceptance.

W is a parameter, not a four-worker architecture limit. Size admission and
transient overlap by the admitted W. Exercise non-power-of-two counts in focused
tests. After the integrated changes, use one short representative fixed-work
W1/W2/W4/W6/W8 sweep where memory/hardware permit; W4 remains the existing paired
application comparison. Do not multiply long application runs merely to populate
a scaling table, and do not claim W8 speedup before measuring it.

## Baseline and opportunity

Full C-array application: 4,094,064 rows, 512 channels, 1024-square images,
natural weighting, Clark CLEAN to 0.5 mJy, seven products.

| Measurement | W1 | W4 |
| --- | ---: | ---: |
| casa-rs application, seconds | 11,459.250 | 6,198.327 |
| Major phases | 17 | 18 |
| Source traversals | 51 | 71 |
| Input/read stage, seconds | about 10,456 | about 5,557 |
| Preparation inside that stage, seconds | about 910 | about 1,383 |
| Inspection/numerical work inside that stage, seconds | about 9,449 | about 4,108 |
| Minor cycles, seconds | about 913 | about 548 |
| Sampled peak aggregate RSS, bytes | 6,590,201,856 | 5,948,637,184 |

CASA serial was **21,622.668 seconds**, not 6,198 seconds. These are single
observations. The exact native outputs were accepted after explicit owner review
of numerical alerts, with zero hard failures. That is not complete T55 acceptance
or approval for future alerts. W4 is 3.489 times faster than CASA but only 1.849
times faster than native W1. Do not add nested timers or treat different major
counts/wave counts as identical work.

Relative to the retained W1, 2x scaling requires W4 <= 5,729.625 seconds, a
468.702-second/7.56% reduction. The actual performance gate uses **candidate W1
and candidate W4**, not the old denominator: serial must not be slowed to improve
the ratio. At most 1,037 seconds would be saved by ideal four-way acceleration of
all 1,383 preparation seconds with everything else fixed. That is an optimistic
bound, not a forecast; do not add it to savings from removing that same work.

W4 residual waves were commonly 139/139/139/95 output bands. Reading 512 input
channels for the first wave makes nominal channel-proportional work 885 rather
than 512 channels per row per phase (ignoring interpolation halos). Restricting
that first window could remove about 42% of this component, not 42% of application
time. Actual dependency closures and storage-tile amplification determine the
real bytes. Initial discovery still needs the full selected axis.

Across all 71 W4 waves, raw-source starvation totaled only 1.472297 seconds.
It is not numerical-worker idle time. Deeper raw prefetch is therefore not the
first candidate. The existing outer adapter worker counter also does not measure
the nested numerical pool; use the actual inner boundary when measuring activity.

## Milestone 1: restricted residual traversals, including the first wave

Touch `casa-imaging-runtime/src/streaming_cube/bulk_phase.rs` and
`weighting/bulk_source.rs`, plus only the source/weighting completion interfaces
directly required by this change.

1. Separate initial global discovery from starting a residual epoch. Retain the
   full initial traversal and its weighting/dependency discovery. For a frozen
   residual epoch, establish the association with the retained weighting/source
   state before reading the first restricted window.
2. Reuse `BulkSourceCompletion::Window`, the retained selected-observation owner,
   and existing generation, channel-range and sample-count validation used by
   `traverse_bulk_next`. Adapt that path to start the first residual wave. Do not
   create a parallel family of receipts or relabel a window as a full traversal.
   Calling `traverse_bulk_next` directly first is insufficient: it requires
   `PendingReplay`, and `PendingWeightingReplay.owner_completion` plus
   `validate_derived_completion` in runtime `weighting.rs` currently require a
   full completion. Adapt those existing representations and downstream finalizers
   to the genuine window result. Keep exhaustive frozen replay sample count
   separate from the count of samples actually delivered by this window.
3. A fresh window completion must represent successful exhaustion of the selected
   row domain, with required order/multiplicity, layout/correlations, exact declared
   native extent and the correct source/weight/model/attempt association. Use
   existing cursor checks during the read already required for imaging. Counts
   alone cannot establish row coverage. Do not add hashes, a second row inventory
   or a validation-only full-width reread.
4. Derive native support from the existing output-to-native operator, then derive
   model support needed to predict those native values. Do not replace these
   closures with an assumed one-channel halo. Preserve original channel identities
   and the complete-axis frequency pair when a view is restricted.
5. All waves borrow the same immutable model snapshot for that major phase. Model
   updates remain at the existing CLEAN boundary. Source-session identity is not
   proof against arbitrary external edits; retain the actual provider's locking,
   generation and immutable-input contract rather than inventing stronger claims.

**Checkpoint:** initial full discovery and nonzero-model residuals produce the
same required science results. Logs prove the first residual wave is restricted
and that no replacement validation pass appeared. Exercise uneven final waves,
first/last channels, row-dependent spectral transforms and support crossing both
edges. Reject missing native/model support, truncated rows, malformed shape,
wrong association, source errors and cancellation. Output cores are accumulated
exactly once; halo reads confer no ownership of neighboring outputs.

## Milestone 2: account for simultaneously live buffers

Touch reconstruction `streaming_cube/memory.rs` and the model-loading boundary in
`streaming_cube/band.rs`; runtime `BulkWave::{prefix,bytes}` and directly affected
phase reservations. Keep contiguous greedy prefix admission, but evaluate a
mixed-state bound instead of summing every band's separate worst phase.

For each admitted band `i`:

- `r[i]` covers the maximum stable band-owned capacity across pending, active and
  retained-result states. Include actual forward grids, residual/normal grids,
  support arrays, convolution data and retained products as applicable.
- `d[i]` is the maximum additional transient capacity above `r[i]` during any
  transition, including model loading and grid/image conversion overlap.
- Shared FFT state is counted separately; do not count each `Arc` as a new native
  plan allocation.

Use the conservative bound:

```text
wave peak = other live run/epoch owners and previous-wave retained state
          + both raw block capacities + derived-block capacity
          + shared FFT/native state + overlapping plan-creation peak
          + sum(r[i]) + sum(largest min(W, band_count) d[i])
```

The top-W term requires a real lifetime invariant, not just a thread-count label.
Keep model loading and conversion as synchronous leaf operations that drop their
temporary planes before returning and cannot spawn/yield nested work while holding
those planes. Test maximum live transitions and error unwinding. Prefer this
existing structural lifetime to a new semaphore/lease system when the full callees
confirm it; if the invariant cannot be enforced, retain the conservative charge
until it can. No assumption that suspended nested jobs hold zero memory.

The concrete initial correction is model-plane loading: 139 x 16 MiB charged
versus at most 4 x 16 MiB concurrently needed, a **2.109-GiB difference in that
reservation term**. It is not necessarily the same reduction in the final maximum
if another phase dominates. Do not remove the genuinely resident forward grids.
Admitting 171 single-plane bands crosses the 512-channel four-to-three-wave
boundary; 256 crosses three-to-two. These are thresholds, not promised capacities.

### FFT accounting: a bounded prerequisite, not a blocker for the proven fix

The wrapper now executes FFTW in-place and shares plans by precision/shape/alignment/
thread key. Its old `2 * grid_cells + 64` complex-value charge per band is not a
description of independent persistent scratch buffers. However, the precise
installed FFTW native allocation bound is **not yet established**.

- Land/test the independently justified model-window correction without pretending
  that the native FFT allowance is zero or already proven reclaimable.
- At `casa-fft` and `spectral_operator` resource reporting, distinguish shared plan
  retention, temporary creation and concurrent execution storage. Inspect the
  installed native allocation path before reclaiming its allowance. If that audit
  cannot establish a safe bound in this milestone, retain its existing conservative
  reservation and report the unresolved portion explicitly.
- Count the union of cache-held and live leased plans; the eight-entry cache is not
  a bound on plans still owned after eviction. Cover alignment variants, both
  precisions, shapes and thread configurations. Do not select an unaligned/slower
  FFT policy just to simplify accounting.
- Creation scratch is currently inside the planner mutex; keep creation/destruction
  serialized. Single-thread FFT execution inside the W-way cube pool avoids nested
  oversubscription. Preserve compatible concurrent new-array execution.

All accounting uses **capacities**, including allocations left after `clear()`
within a traversal. The current `execute` creates a fresh kernel for each
traversal: do not attribute its full-width capacity to the initial kernel being
retained across waves. Size a new wave's workspace deliberately and reuse it
across blocks without per-block allocation churn. Respect the authority's
host-availability reservations and unchanged 16-GiB aggregate guard; low sampled
RSS is a cross-check, not an admission formula.

There is an additional verified reason not to confuse selected width with owned
capacity: `weighting/bulk_source.rs::execute` reconstructs `BulkInputPlan` from
the full problem even for a channel-window stream, and allocates numeric geometry
and `NaturalRowPreparation` from that full channel count. When narrowing these
allocations, give the admitted input plan an explicit local window width, retain
global/original spectral identities separately, and update its resource claim and
shape checks together. Do not reduce the charge while leaving full-width vectors
allocated, or use local offsets as original MS channel numbers.

**Checkpoint:** mixed pending/active/completed cases, worst spectral support,
completion overlap and cancellation remain bounded; the wide diagnostic admits
the predicted bands and reports actual traversal/byte changes. FFT tests cover
shared keys, live owners after cache eviction, alignment, creation and concurrent
execution before its accounting is changed.

## Milestone 3: parallel, reusable shared-block preparation

Touch runtime `weighting/bulk_source.rs::Kernel::consume`, reconstruction
`weighting/bulk_natural.rs`, and the numeric projection path in
`casa-ms/src/selected_observation/access.rs` where necessary. Reuse
`BoundedExecution` and the admitted pool; do not create a second compute pool.

1. Split immutable row-preparation science/configuration from the initial global
   weighting reduction. Residual preparation should not require a mutable global
   accumulator when it has no global reduction.
2. Pre-size and reuse flat row metadata, frequency/geometry and weights/flags
   storage. Partition writable outputs into disjoint coarse contiguous row chunks.
   Project geometry, fill metadata and prepare weights/flags in those chunks from
   immutable source views. Preserve time/frame-dependent coordinate semantics.
   Do not invoke non-shareable MS handles concurrently.
3. Use enough coarse chunks for load balancing within W workers, not one task per
   sample. About four chunks per worker is an initial scheduling choice to validate,
   not a memory formula, fixed four-worker limit or dataset-specific constant.
4. Initial discovery alone reduces chunk-local totals through the existing science
   reduction after successful preparation. Preserve finite handling, row flags,
   full-correlation and parallel-hand group flags, selected correlations and
   per-row/per-channel weight semantics. No partial reduction may publish after an
   error. Ordinary reduction-order rounding is acceptable within unchanged checks.
5. Dense Complex32 visibility values remain borrowed: this route already has zero
   visibility-copy samples. Do not claim to remove a copy that is absent, transpose
   the MS payload or introduce an intermediate replay store. Keep necessary gathers
   for layouts that genuinely need them, measured separately.
6. Join prepared writes, run existing parallel band consumers with exclusive grids,
   join every started borrower, then recycle the source slot. On error/cancellation
   stop scheduling, wake blocked producer operations, join borrowers/producer and
   drop incomplete ownership. Keep existing error propagation and per-image atomic
   publication; no attestation or resumable whole-product transaction.

**Checkpoint:** W1 uses the same science routine through serial iteration; W4
shows preparation work on the actual numerical pool, not only adapter telemetry.
Copy bytes stay zero for the dense route and all buffers remain admitted. Separate
first-block model/FFT setup from steady-state band consumption in diagnostics.

## Bounded verification and timing sequence

Batch related edits/builds within each milestone. Run focused checks at the
changed contracts; do not require a full deep timing pair for every helper edit.
Capture a retained-parent bounded baseline before implementation, then compare the
coherent candidate at the checkpoints. One before/after observation is enough for
a clear result; repeat only noise/borderline results or a necessary current-parent
control when unrelated phases drift. Preserve rejected results.

### Existing deep turnaround

The reusable application test is
`casa-imaging-application/tests/continuum_application/t55_c_array_turnaround.rs`,
`t55_c_array_turnaround::contiguous_spectral_block`.
The durable W1/W4 command receipts under `E` below supply executable/environment
details. Use fresh output/log names and verify a candidate binary's source rather
than blindly invoking the old pinned binary.

```text
CASA_RS_C_ARRAY_FULL_INPUT=1
CASA_RS_C_ARRAY_EXPECTED_ROWS=4094064
CASA_RS_C_ARRAY_CHANNEL=240
CASA_RS_C_ARRAY_OUTPUT_CHANNELS=32
CASA_RS_C_ARRAY_IMAGE_SIZE=1024
CASA_RS_C_ARRAY_NITER=640000
CASA_RS_C_ARRAY_WORKERS=1, then 4 (separate runs)
CASA_RS_C_ARRAY_MEMORY_BYTES=17179869184
CASA_RS_FFT_THREADS=1
```

Use unchanged natural/Clark/0.5-mJy settings, existing frozen CASA reference,
seven-product/nine-check comparison, full science assessment and panel inspection.
New numerical alerts need exact-output review; previous approvals do not transfer.

### Wide, multiwave mechanism check

Do not invent a new 65k-row fixture merely because Oracle suggested that size.
The existing harness already supports the **168,480-row C-array turnaround MS**,
512 output channels, 1024-square images and `CASA_RS_C_ARRAY_MAX_MAJOR_CYCLES`.
Prefer this existing smaller MS with the full selected axis and nonzero model;
record actual completed residual count and use a bounded major limit that exercises
at least two residual refreshes. This diagnostic's early stop is not deep-clean
acceptance. It does not restart the 4,094,064-row/full-512 run.

For a fixed-work residual comparison, add a small **test-only** entry point using
the production residual-phase boundary and the same retained nonzero model. Such
a residual-only harness is not already present. Avoid new production modes or a
second controller. Verify the input/mask actually exist before scheduling this
diagnostic; if unavailable, explicitly choose a bounded row selection from the
existing MS rather than silently substituting a one-row fixture.

Hold the admitted byte budget constant before/after and low enough to retain
multiple waves after the correction; include an uneven final wave and model/halo
support across wave boundaries. Do not fix band count, because better packing is
one result being measured. Also exercise ordinary controller/product completion
on this bounded shape. Compare parent/candidate results across the full shape;
do not compare its products to a CASA reference with different selected rows.
The existing matched 32-channel CASA gate remains the scientific acceptance gate
for this bounded checkpoint. The wide diagnostic does not replace the eventual
full-size comparison.

### Evidence, not a new instrumentation subsystem

Reuse existing wave/read/preparation/copy/backing counters. Add only missing
coarse counters at the actual inner worker boundary:

- native range, rows/samples prepared, logical bytes, storage-read counts and
  modeled/actual physical bytes explicitly distinguished;
- bands and traversals, retained/transient reservations, real buffer capacities,
  peak aggregate RSS and relevant FFT/model-loading counts;
- preparation, first-block model/FFT setup and steady consumption wall times;
  actual inner task work/CPU activity and dispatch-to-join span, without per-sample
  timing or double-counting nested work;
- dense visibility copy/gather bytes and existing raw-source wait.

Four busy workers with weak throughput suggest locality/bandwidth/placement,
not necessarily barriers. Significant idle time with ready work justifies a
later bounded scheduling experiment. Apple M4 has four performance and six
efficiency cores, but old benchmark placement is unknown. Do not assume W4 means
four performance cores or prescribe affinity without evidence.

## Existing focused checks and stopping rules

Use `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=1`, the existing
8-GiB build/comparison guard and package-local `cargo test --lib <filter>`:

- Runtime: `wave_admission_counts_only_the_bounded_completed_wave`,
  `initial_wave_selection_uses_shape_budget_and_drains_before_next_wave`,
  `managed_cache_budget_preserves_a_complete_worker_wave`.
- Reconstruction: `band_memory_accounts_for_actual_phase_buffers_and_completed_ownership`,
  `shared_wide_window_narrows_row_dependent_support_without_copies`,
  `exact_zero_model_planes_skip_forward_work_without_losing_halo_terms`,
  `nonzero_native_prediction_and_band_grids_are_partition_and_chunk_invariant`.
- Add direct tests for corrected `BulkWave` admission and first-residual-window
  ownership; current adjacent tests are not direct coverage of those new rules.

Do not revive the rejected consumed-frequency-window comparison: trial
`e9a2ee2d5c` passed its focused test but ran 584.094432 s versus retained
569.233331792 s, and was reverted by `47d448d8a4`. A shared producer-owned spectral
geometry identifier would be a different hypothesis, not part of these milestones
without new discriminating evidence. Zero-phase trigonometry and other retired
autoresearch results likewise remain preserved, not implicitly reauthorized.

At each meaningful checkpoint, report changed function, actual result, science,
memory and next decision in the single CURRENT summary. Do not proceed through
a scientific failure as if it were a performance success. Follow existing one
evidence-producing retry/escalation rules; routine reversible harness repairs do
not consume that hypothesis budget or authorize another full run.

After the three changes, advance only if bounded end-to-end W4 improves, W1 does
not materially regress, and work counts/science/memory support the explanation.
If the result is unclear, use the single fixed-work worker-activity diagnostic to
choose between scheduling imbalance and occupied-but-slow kernels. Do not start
a speculative collection of micro-optimizations. A fresh full-size deep W1/W4
acceptance run requires separate authority; neither the plan nor the checkpoint
push grants it. No new push, merge, release, cleanup or installation is included.

## Provenance and restart handles

- Oracle conversation, GPT-6 Pro at maximum model power:
  https://chatgpt.com/c/6aba7d39-6bfc-83e8-ad81-b379fa25fd40.
  Initial request supplied timings, constraints, source excerpts and pinned
  upstream URLs. First response independently inspected Obit/LibRA but could not
  retrieve the then-local-only casa-rs commit. After the approved checkpoint push,
  Oracle fetched all three requested pinned casa-rs files through GitHub and
  inspected the existing completion and synchronous-loading paths. Web/raw access
  still failed; native FFTW allocation bounds and underlying model-storage callees
  were not independently verified by Oracle. Treat advice as reviewed locally,
  not authority.
- Follow-up changes incorporated: reuse existing window completion and pending
  replay validators instead of a parallel receipt/authority layer; enforce/test
  synchronous leaf lifetimes instead of adding a semaphore subsystem; correct
  the explanation of fresh per-traversal kernels/full-width capacities; retain
  conservative native FFT charges until justified; keep spectral-geometry run IDs
  and the rejected narrow comparison out of the first milestones. Local plan
  refinement reuses the existing 168,480-row diagnostic fixture instead of the
  suggested new 65k-row fixture. The fixed-model harness extension is explicitly
  new work, not a claimed existing measurement.
- The owner explicitly authorized a non-force push of the existing 37 commits to
  `codex/t55-full-size-validation`. Push completed and both GitHub commit API and
  remote branch ref verify the baseline SHA above. No new commit or merge occurred;
  these new research/plan notes were not part of that checkpoint.
- Durable evidence base `E`:
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/bulk-imaging-plan-20260927`.
  W1 receipt: `ar-cube-20260927T235704-693907000-33adc67d-application-command.json`;
  W4 receipt: `ar-plateau-w4-20260928-v1-application-command.json`.
- Full-size evidence:
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/full512-retained-20260928-exec-v1`.
  Keep original logs, resource receipts and reviewed/unreviewed assessments.
- Single CURRENT:
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/tranche5-20260916/CURRENT.md`.
- A bounded Luna Max read-only lookup located existing harness/test handles and
  the already-rejected trial. Main-agent source inspection verified the relevant
  lifetimes, window path, numeric preparation and actual harness options.

The Oracle skill provided the external design review; the imaging-performance
skill required source-grounded work removal, end-to-end checks and preservation
of rejected results. No production code or imaging run was changed for this plan.
