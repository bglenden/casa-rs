# Clark cube imaging: fresh GPU math and dataflow audit

Truth class: non-normative analysis and measured evidence
Last reality check: 2026-09-29
Source checkpoint: `1ba394d169d6becefb49b2974bd51c29477266f3`
Verification: read-only call-chain/source audit and existing exact-run evidence;
no new timing trial or implementation in this audit. Oracle main review and
focused prediction-reuse/precision follow-up completed and checked locally.

## Conclusion

The current flat numerical buffers and memory-bounded plane waves are a sound
base. The present **spatial-tap-only GPU interface is not yet a good final
GPU dataflow**. It preserves science well, but exports the innermost CPU
convolution loops after the CPU has already performed and materialized most
of the visibility operator. Each nonzero-model refill crosses back to the CPU
between prediction and residual accumulation. This is an integration proof,
not evidence that Clark cube imaging is intrinsically unsuitable for GPUs.

The strongest next architectural hypothesis is a **bounded GPU visibility
residual operator**: native visibility block + compact shared-science mapping
descriptors + resident model grids -> resident residual grids, with prediction,
residual formation and accumulation connected without a host prediction
round trip. Keep the existing CLEAN controller and CPU FFTW initially.
Keep the existing atomic scatter in the first proof; a locality-aware gridding
change needs a demonstrated remaining bottleneck. No GPU speedup is promised.

### Implementation follow-through

The owner subsequently approved this path. The connected residual candidate
now completes the full-row deep32 application in 171.954 s versus a fresh
248.858-s CPU W4 control (single observations, 30.90% lower elapsed time).
The fixed-model residual normalized RMS is 5.637273e-6. The unchanged deep
CASA science assessment has zero hard failures but still requires numerical
alert review; this is not full T55/T57 acceptance. The original audit and
historical timings below are retained, not relabeled as current measurements.

Exact source checkpoints, binaries, logs, comparisons and inspected panels:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/CONNECTED-GPU-RESIDUAL-CHECKPOINT.md`.

## Evidence and timing boundary

Durable bundle:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929`.
See `METAL-CUBE-APPLICATION-CHECKPOINT.md`, `CPU-ROW-PROFILE-REPAIR.md`
and the retained exact binaries/logs/resource receipts there.

Deep32: 4,094,064 rows, 512 stored channels, output 240-271, 32 x 1024²;
natural Stokes I, standard convolutional gridder, existing Clark, 0.5 mJy,
640,000 global iteration ceiling and unchanged mask/LSRK science.
Elapsed time includes selected preparation, initial imaging, all visibility
residual refreshes, intermediate I/O, restoration and publication, plus
first-use device setup. It excludes build/input copying/process startup/
comparison. Single observations, not controlled cold-cache experiments.

| Observation | End-to-end s | Peak aggregate RSS, decimal GB | Note |
| --- | ---: | ---: | --- |
| Earlier integrated CPU W4 | 270.134 | 2.301 | Matched with the old Metal candidate |
| Metal, four host packing workers | 345.067 | 3.558 | Spatial grid/degrid only; 17 passes |
| Repaired current CPU W1 | 566.441 | 2.271 | Recovers CPU integration regression |
| Repaired current CPU W4 | 279.292 | 2.321 | Deep W4 preservation remains unresolved |

The corrected code has **not** been retimed with Metal. Do not compare old
Metal against new CPU as a matched pair. No matched CASA deep32 timer exists:
the exact CASA reference is a window of retained full512 products.

In the matched old CPU/Metal pair, exclusive input+imaging is
239.682/314.878 s, minor cycles 22.951/23.005 s, other control
0.210/0.238 s and remaining boundary 7.292/6.947 s.
The slowdown is in the visibility/imaging path, not demonstrated FFT or
Clark slowdown.

Metal submitted 159,456 command batches, 2,358,180,864 grid requests and
4,257,826,560 degrid requests. GPU command duration 143.435 s is nested
inside 184.863 s submit/wait, itself inside input/imaging. The 41.428-s
difference is not a separately additive phase and not necessarily all
removable launch overhead. Overlapped host-worker sums are not wall time.

The submit/wait clock starts after dispatch validation and encoding, and after
acquiring the runtime mutex (`metal_runtime.rs:533,1270`). It excludes tap
packing/copying, tap validation, encoding and waiting to acquire that mutex.
Those costs are still in the complete consumer/application time. GPU command
start/end timestamps cover a command-buffer interval, not per-kernel active
instruction time. Neither subtraction nor a count-times-average extrapolation
is a whole critical-path attribution.

The 24-byte tap representation implies 158.784 GB of cumulative staging
memcpy; prediction result copies imply another 34.063 GB. These are
executed-count-derived memory-copy bytes, **not PCIe transfers, peak memory,
or measured DRAM traffic**. Apple unified memory does not eliminate copies
between different allocations in our implementation.

## 1. Math and essential dependencies

Let `A` denote the implemented model-to-native-visibility prediction,
including image correction/FFT, convolutional degridding, model-channel
interpolation, phase rotation and polarization projection. Let `B` denote
the implemented visibility-to-image operator, including native-to-output
spectral resampling, flags/weight semantics, polarization reduction,
phase rotation, weighted convolutional gridding, FFT and normalization.

Then the expensive refresh is conceptually:

```text
model M -> A(M) = predicted native visibilities
native observed V -> V - A(M) -> B(V - A(M)) = refreshed residual image
```

Dirty image is `B(V)`; PSF uses the existing unit-response/weight path.
`B(V - A(M))` is schematic notation, not permission to change operation order.
The executing cursor interpolates observed and predicted endpoints separately,
applies paired flags/nearest weights and polarization reduction, THEN subtracts
the reduced values and applies phase/weight in `grid_sample_with`
(`band.rs:1612-1670,1248-1307`). Preserve this literal ordering in a device
implementation; native subtraction followed by interpolation is not assumed
equivalent merely because interpolation is linear.
This notation does NOT assert that our finite-support, resampling and
normalization implementations are exact mathematical adjoints. That identity
must not be assumed to justify reordering or dropping interpolation.

Spatial gridding for one prepared sample is a 7 x 7 weighted scatter:
`G[x+i,y+j] += z * Cx[i] * Cy[j]`. Degridding is the corresponding 49-cell
read/reduction for a prepared model-plane request. The separable convolution
table, UV wavelength scale, grid bounds and center/crop conventions are shared.
Code: `spectral_operator.rs:11840`, `band.rs:1139,1248`;
GPU shaders `metal_cube.rs:29,48`.

Residual prediction is evaluated at native sample frequencies BEFORE
native-to-output interpolation and polarization/weight reduction. Prediction
can draw on neighboring model planes: spectral halos are real mathematical
dependencies, not bookkeeping. Planes have independent spatial grids, but
"one completely independent end-to-end pipeline per output plane" cannot
ignore those halos, shared source ownership or the existing cycle-threshold
and iteration/stopping policy.

More precisely, the resulting prediction is indexed by native frequency, but
each contributing model-plane degrid and phase uses that term's COARSE
evaluation frequency (`spectral_sampling.rs:1019-1075`, `band.rs:814-830`).
Do not substitute the native frequency's UV wavelength scale/phase for every
coarse term. This distinction also permits reuse of a model-plane prediction
at one row across several native interpolation destinations.

Clark performs sequential decisions within a subcycle: pick the current
peak within one plane's active pixels, apply a gain-scaled component, subtract the small
PSF patch from active values, then choose the next peak. Each choice depends
on the previous subtraction. It accumulates pending components and periodically
refreshes the full residual by alias-free FFT convolution. That Clark image
refresh is NOT the much more expensive visibility-domain major refresh.
The current compact active list and batched refresh must be preserved;
a full-image scan/subtraction or GPU submission per component would undo Clark.

The cube controller has a shared entry-statistics/threshold prepass, then runs
one independent minor solver per channel/polarization, concurrently when
admitted. It does NOT choose a cube-wide best component between those solvers.
Each solver has its own Clark state/controller; results commit in canonical
order. Source: `reconstruction_cycle.rs:792-907`,
`reconstruction_executor.rs:150-175`, `minor_cycle.rs:2650-2687`.
This gives genuine across-plane GPU parallelism without changing CLEAN;
unequal plane difficulty still produces load imbalance. Within-plane peak
selection/update could stay on-device for a subcycle, but requires a suitable
reduction/active-list strategy rather than a CPU round trip per component.

CASA/LibRA's inspected `ClarkCleanModel.cc:601-671` uses the same
peak -> component -> active-list subtraction -> next-peak dependency.
The [CASA tclean documentation](https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.imaging.tclean.html#deconvolver)
also separates major/minor cycles. Scientific dependency does not prescribe
our current CPU/GPU boundary or request representation.

## 2. Current dataflow and GPU suitability

| Work | GPU fit of the math | Current structure and consequence |
| --- | --- | --- |
| Tiled source reading/decoding | CPU/I/O task; parallel preparation can overlap GPU | Existing selected-channel bulk source, reusable blocks, no replay store: retain |
| Spectral/phase/polarization/weight work | Many independent sample calculations; suitable for broad GPU kernels if precision and branching are controlled | Host row stencils, SmallVec terms and callbacks are CPU-native; do not upload Rust object graphs |
| Model degridding | Independent output per request; no output atomics; many identical 49-cell gathers | Flat resident Complex32 grids/table are suitable; one GPU thread per request is a reasonable starting point, locality still matters |
| Residual formation then gridding | Fusion can avoid materializing prediction/residual arrays on CPU | Present boundary forces prediction download, double-precision host combination, another row traversal and tap repacking |
| Convolutional gridding | Parallel but competing scatter writes; memory locality/accumulation determine performance | One sample thread executes 98 global scalar atomic additions; no local aggregation. GPU bottleneck attribution not yet measured |
| Image FFT / Clark batch convolution | Regular transforms and elementwise products; good abstract GPU workload | NEON FFTW is already strong. Full operation/boundary cost, not naked FFT time, decides |
| Clark active-pixel update/peak | Update/reduction parallel across pixels; component choices serial within a plane | Compact CPU list suits current small active sets; per-component GPU/CPU synchronization is a poor boundary |
| Restoration/publication | Some image calculations parallel; persisted writing stays existing writer | Small measured share here; moving writer is not the high-impact opportunity |

Apple's [GPU timeline guidance](https://developer.apple.com/documentation/xcode/analyzing-apple-gpu-performance-using-a-visual-timeline)
specifically recommends avoiding many small passes and unnecessary
serialization. Its [occupancy guidance](https://developer.apple.com/documentation/xcode/finding-your-metal-apps-gpu-occupancy)
warns that occupancy alone does not establish efficient work: access patterns
and cache/resource pressure matter. We have not captured those counters for
this workload, so atomic contention, bandwidth saturation and occupancy are
hypotheses, not measurements.

[Romein's 2012 gridding paper](https://astron.nl/~romein/papers/ICS-12/gridding.pdf)
establishes the relevance of a GPU-specific work distribution that reduces
device-memory accesses without requiring a full data sort. Its old hardware
speedups are not predictions for this Mac. Only the search-accessible
author abstract/introduction was available during this audit; direct PDF
fetch returned 404. Do not infer a reviewed implementation from that access.

LibRA's locally inspected HPG adapter also presents a combined operation:
`AWVisResamplerHPG::sendData` moves a populated visibility bucket into
`degrid_grid_visibilities`, or `grid_visibilities` for a zero model. Its
finalization flushes partial buckets, fences, applies the device grid FFT,
then gathers images. See [pinned LibRA source](https://github.com/bglenden/libRA/blob/0ab99e261878334d6588eafa360cef3b673e897f/src/synthesis/TransformMachines2/AWVisResamplerHPG.cc#L622)
and lines 241-284. This is a concrete precedent for the larger operator seam,
not evidence of Apple performance, equivalent cube interpolation, or authority
to copy its buffering, precision policy, or FFT selection. The actual HPG
kernel internals were not inspected here.

### Representation audit

Good and worth retaining:

- Plain contiguous Complex32 grids, separable small convolution tables and
  bounded plane groups, not a full-cube allocation.
- Borrowed native blocks: values/weights/flags in flat arrays, row metadata
  separate, correlation-fastest then channel then row. No visibility object
  copying is needed to invoke the CPU science operator.
- Resident GPU model/output grids reused across all source refills in a wave.
  Models upload and output grids download at wave boundaries, not every refill.
- Existing phase-aware memory admission, restricted-channel source reads,
  shared controller, FFTW and persisted product writer.

Weak as a long-term GPU structure:

- `CubeSpatialBackend` exposes `initialize/degrid/grid/download`
  after host preparation. `SpatialPredictionBatch` owns a separate taps and
  result Vec per model plane; destinations carry host index/Complex64 factor.
  `consume_spatial` allocates a rows x native-window Complex64 prediction
  array, traverses rows to pack prediction geometry, waits/downloads/results,
  then traverses rows again to pack output contributions.
- `metal_wave.rs:268-329,371-399` copies packed taps into another shared
  allocation and copies predicted Complex32 values back into host Vecs.
  Runtime `metal_runtime.rs:1190-1218` scans packed tap bounds again.
  Required bounds validation remains, but a typed/checked producer could
  establish those bounds at introduction rather than rescan a trusted list.
- `MetalBatchAccess::execute` holds the shared runtime mutex through native
  submission and `waitUntilCompleted`. Four host workers are packing
  lanes, not four independent GPU streams; separate plane kernels are encoded
  into a batch, then synchronously drained before reuse.
- Host FFT grid arrays and GPU grid storage are different physical allocations.
  Shared-memory mode alone does not make them alias. A single accounted
  shared backing may be possible; FFTW alignment/layout/lifetime safety must
  be established rather than assuming every Vec can be a Metal buffer.

[Apple shared-storage documentation](https://developer.apple.com/documentation/metal/mtlstoragemode/shared)
requires synchronization before the other processor accesses modified memory.
An asynchronous design therefore needs bounded owned in-flight slots, not
passing a borrowed refill to the GPU and immediately reusing its storage.

### Repeated prediction across neighboring bands

`RowStencil::compile` calls `casa_linear_prediction_terms(output_hz,
row_native_frequency, original_pair_hz)` independently of the output core;
`prepare_prediction_terms` then resolves the same global model-channel identity
to a band's local grid ordinal (`band.rs:814-864`). Neighboring bands can share
native support. Thus their identical model/sample terms and halo FFT grids
can be computed/stored repeatedly. A wave-level identity map can share unique
model grids and native predictions within the admitted wave, without assuming
an entire cube fits or reusing stale row-dependent geometry. The exact unique
request count/saving is not measured. The retained log reports 266,114,160
degrids per residual phase: an aggregate mean of 65 requests per physical row,
for a 32-output-plane wave. Since a term evaluates at its coarse plane frequency, the wave could
evaluate each unique model-plane/row geometry once, then apply its different
spectral factors. Identical global plane content, FFT geometry, phase and
mapping must be established before sharing; total request counts alone do not
prove a factor-of-two kernel or application speedup. This is another reason
to consider a wave/block operator rather than only a larger collection of
per-band taps.

Native layout is correlation-fastest, then channel, then row
(`streaming_cube/input.rs:105-119`). Device work should exploit those flat strides;
one warp/SIMD-group per plane marching across rows would otherwise stride across
all channels. No array-of-Rust-objects upload or mandatory transpose is implied:
choose the thread mapping and only introduce another layout if its complete
cost is demonstrably lower.


## 3. FFT and half-plane conclusions

There are two different Hermitian opportunities:

1. Real Clark component/PSF image convolution: already migrated to in-place
   real/half-spectrum FFTW. For a 1536-square workspace, the two main arrays
   shrink from 36 to 18.0234 MiB. Existing exact-convolution tests cover
   asymmetric PSFs, crop, edges and off-center origins.
2. Visibility grids: do NOT truncate the complex grid to half a plane merely
   because the final sky image is real. Correct conjugate folding, DC/Nyquist,
   normalization and the real-adjoint/prediction convention are required.
   Arbitrarily flagged/resampled complex visibilities need not directly
   populate an exactly Hermitian stored grid. This is not the same cutover.

Measured complete Clark convolution, ms/update or four-plane joined batch:

| Implementation | One plane | Four concurrent planes |
| --- | ---: | ---: |
| Full-complex NEON FFTW | 13.675 | 23.564 |
| Half-spectrum NEON FFTW | 6.653 | 9.033 |
| Half-spectrum Metal MPSGraph | 4.255 | 12.663 |

These include component preparation, cached PSF multiply, inverse,
crop/normalization, CPU-visible residual and reset. Metal wins one plane
but loses to half-spectrum CPU at four planes. FFT is therefore NOT currently
the easiest demonstrated GPU win. Do not revive a blanket FFT replacement.

## 4. Recommended next course, before implementation

Ask Oracle to challenge the operator boundary and select a single bounded
proof, rather than seed another collection of local tap/launch tweaks.

Proposed GPU-facing state remains simple:

```text
bounded native block (owned in-flight slot, flat arrays)
+ compact spectral/geometry/polarization descriptors from shared science
+ bounded resident model grid/halo and output grid views
    -> GPU native prediction -> separate observed/predicted interpolation
    -> flags/weights/polarization reduction -> residual/phase -> accumulation
    -> completed residual grids at wave boundary -> existing CPU FFTW/CLEAN
```

Descriptors must encode existing row-dependent frequency support, flag and
weight rules, phase sign/geometry, spectral ordering and valid grid bounds.
Do not treat cached mapping as invariant when row geometry/frequency changes.
The desired seam is the visibility residual OPERATION, not a second science
definition or an uploaded tree of CPU per-sample objects.

An operation boundary does not require one giant kernel. Dependent kernels
can run in one GPU command chain, with device-side intermediate scratch and
the proper ordering, without reading predictions on the CPU. Prefer this
over fusion that explodes register use or duplicates endpoint predictions.

Start with a representative frozen nonzero model and one COMPLETE residual
refresh across the selected full-row deep32 input, including model setup/FFT,
all refills, packing, synchronization, final CPU-visible residual images and
bounded storage. Prepared-refill microchecks may assist diagnosis but are not
performance acceptance. Discriminator: can eliminating the host prediction
round trip and extra request materialization clearly beat current CPU W4's
complete refresh, followed by a corresponding deep32 application benefit?
Record separate grid/degrid GPU durations and only the relevant counters
on that wave to determine whether scatter locality must change too.
This is a narrow missing measurement, not a new broad profiling campaign.
Before implementing, decide precision explicitly: the current host combines
Complex32 degrids in Complex64 with double-precision phase factors.
Do not silently replace that with unrestricted Float arithmetic. Shared
science descriptors/range reduction and existing numerical gates must show
any ordinary precision change is adequate.

If this proof demonstrates a remaining gridding bottleneck, reassess a separate
tile/work-distribution accumulation hypothesis that
preserves the SAME seven-tap convolution while reducing global writes.
Conjugation, resampling, PSF normalization, Clark policy and weighting must
not be changed to obtain a speedup. Binning/sorting is not automatically free;
its cost/storage belongs in the complete-operator boundary.
The current atomic shader is a correctness comparator, not a permanent
fallback or a benchmark baseline padded to flatter the candidate.

Do not port the controller or all FFTs first. Old Metal's matched visibility
penalty is about 75 s, while old CPU W4 minor cycles total only about 23 s.
Even eliminating those minor cycles would not close that penalty.
Similarly eliminating the 41-s submit/wait-minus-GPU interval alone cannot
justify claiming the old Metal implementation beats matched CPU W4.
Changes must save end-to-end critical-path work, not merely a large sum of
overlapped lane times.

## Acceptance / limits

This audit does not accept a new implementation or waive alerts. All restored
planes in the current deep32 observations satisfy approximately 1e-3 agreement;
zero hard failures, but 16 review alerts remain and raw model/residual
diagnostics remain outside 1e-3 under the existing gate. Full T55/T57
acceptance is not complete. CPU deep-W4 preservation and corrected Metal
timing are separate unresolved evidence.

Any next proof retains selected bulk tiled reads, restricted spectral halos,
bounded native planning and aggregate RSS 16 GiB, two Cargo jobs, existing
seven-product/nine-check CASA comparisons and inspected panels.
No second CLEAN controller, permanent fallback, unbounded cube residency,
attestation, weakened science, MFS expansion, W8 tuning, full512/full32GB
restart, installation, push, merge, release or cleanup is authorized here.

Audit method: parent reviewed the numerical/source/runtime path and checked
the plane-controller findings locally. A bounded read-only source inventory
requested Luna Max; the subagent runtime did not expose its model/effort, so
that selection cannot be independently confirmed. No acceptance decisions
were delegated.

## Oracle review

Oracle: [ChatGPT 6 Pro review](https://chatgpt.com/c/6abc3a60-5e04-83e8-92f1-24489fa665e6),
highest visible Pro setting (5/5), submitted through the signed-in Chrome
session. Main response completed after 11m52s; focused follow-up completed
after 2m51s.

It reviewed the audit and all six attached executing source files. The exact
GitHub commit lookup returned "No commit found"; attachments, not a remotely
accessible checkpoint, were its current-code basis. It did not independently
see the full controller, reader, helpers, writer/gate, logs or panels. Those
facts are supplied evidence verified locally here.

Accepted findings and consequential refinements:

- Use the admitted **wave**, not a per-band tap batch, as the GPU work unit.
  The old counts are consistent with 151 refills x 32 bands x
  (one initial batch + two batches x 16 residual refreshes) = 159,456 commands.
  This count identity supports the call chain; it is not a measured speedup.
- Use two connected device passes, with legitimate bounded prediction scratch;
  do not recompute both endpoint predictions independently for every output.
  Keep the existing seven-tap scatter in the first proof. Atomic dominance
  is not measured, and simultaneous locality changes would confound the result.
- Preserve the literal interpolation/flags/weights/polarization/subtraction
  ordering. Retain CPU decisions for discrete tap/table indices initially:
  floating-point geometry can change an index, not only an accumulated value.
- The runtime mutex is part of today's unsafe Send/lifetime proof. Removing the
  GPU wait while keeping unrestricted mutable `with_bytes` access is unsafe.
  Replace that access model with a bounded wave owner and owned input slots,
  completion tickets, and allocation pinning. Initialize the shared convolution
  table once before any dependent work, not per-band writes while GPU readers
  may be active.
- One queue permits CPU preparation of a later slot while the GPU consumes
  an earlier one; it does not require a second device/queue authority.
  Start with two charged reusable slots, one ordered GPU-only prediction
  scratch region, resident immutable model/halo grids and residual grids.
  Slot states are free/filling/ready/in-flight: **ready means ownership**, not
  a content seal or attestation. On failure stop submission, drain commands,
  preserve buffers until safe and propagate failure; never retry a partly
  accumulated block into the same grid or silently switch backend.
  See [Apple's command model](https://developer.apple.com/library/archive/documentation/Miscellaneous/Conceptual/MetalProgrammingGuide/Cmd-Submiss/Cmd-Submiss.html)
  and [bounded buffering guidance](https://developer.apple.com/library/archive/documentation/3DDrawing/Conceptual/MTLBestPracticesGuide/TripleBuffering.html).
- Native-input staging may add an actual copy. Replacement descriptors may
  be larger/costlier than taps. Count these honestly; 158.784-GB removed
  staging is not automatically time saved. Defer FFTW/Metal backing-store
  aliasing in this proof rather than add another lifetime/layout change.
- First milestone must be a **complete frozen-model residual refresh** across
  the selected full-row deep32 input, including model prep/FFT, all refills,
  descriptor work, staging, setup, synchronization, final download and inverse
  FFT. Select the retained cancellation-heavy epoch before looking at timing.
  A prepared-refill microcheck is useful diagnosis but not milestone acceptance.
  Compare to current CPU W4 and the old tap operator; then run the unchanged
  deep32 application/gate/panels against a contemporaneous CPU control.
- Go only on a clear complete-refresh advantage over CPU W4 and corresponding
  complete-application benefit with unchanged science/resources. Beating the
  inefficient tap backend alone is not enough. If efficient compliant device
  arithmetic or that advantage is unsupported, pause this cutover instead of
  automatically expanding into GPU FFT, GPU Clark or new gridding algorithms.

Locally verified the unsafe-owner comment (`metal_runtime.rs:456-463`),
shared-table writes (`metal_wave.rs:348-351`), timing exclusions and interpolation
order. Oracle's initial numerical proposal was a scoped hi/lo Float32 bridge;
we did **not** adopt that as an unconditional requirement. After the focused
follow-up supplied the missing coarse-frequency helper and precision-policy
constraints, Oracle endorsed coarse-plane prediction reuse and ordinary
Float32 first, with extra precision only for demonstrated scientific need.

Oracle's break-even arithmetic is sound but not a prediction: recovering
74.933 s requires about 23.8% off old Metal's input/imaging boundary if the
remaining boundary is held fixed. The 143.435-s GPU interval and 41.428-s
submit/wait-minus-GPU gap do not identify the missing mechanism's savings.
No acceptance alert, CPU preservation deficit or missing matched timer is waived.

### Focused follow-up: final amendments

The [same Oracle conversation](https://chatgpt.com/c/6abc3a60-5e04-83e8-92f1-24489fa665e6)
completed the two-question follow-up in 2m51s, still at the highest visible
ChatGPT 6 Pro setting. Both amendments are adopted and checked locally.

1. **Reuse coarse-plane predictions in the first proof.** The supplied helper
   establishes that multi-channel CASA-linear prediction evaluates at
   `centres[global_model_channel]` (`spectral_sampling.rs:1019-1075`).
   Compute the degrid result times conjugate phase once per required
   source-row/coarse-model-plane pair. Then form each native destination using
   its original ordered spectral-factor sum and existing polarization
   projection. An immutable wave owner fixes the model epoch/domain/Stokes,
   source identity/layout, global frequency axis, corrected FFT/grid
   conventions, convolution table and row UV/phase. Within that owner, the
   lookup needs only refill-generation row index and union-plane index,
   not hashes or heavy per-sample identity objects. Keep the union bounded
   to the admitted wave/refill and deduplicate model halo grids as well.
   **Exception:** `CasaSingleChannel` evaluates at native frequency
   (`band.rs:1116-1135`); this reuse rule is not universal. Residual scattering
   also evaluates at the fine output frequency, not the coarse prediction
   frequency. The logged 65 requests per row is an aggregate mean, not a
   unique-key count or proof that every row has the same work. Measure actual
   requested and unique prediction counts in the proof.

2. **Float32 first; precision only for demonstrated scientific need.** Oracle
   withdrew its default hi/lo Float32 requirement: the supplied evidence does
   not establish a failure requiring it. Retain CPU geometry/table-index
   decisions and host phase/range reduction initially, with ordinary Float32
   device arithmetic. Include coefficient rounding in fixed-model,
   cancellation-heavy checks. Preserve literal interpolation/reduction order
   and keep current fast-math disabled for the first proof. Run stricter
   existing fixed-model checks before the application comparison. If these
   pass, do not buy closer agreement or identical bits with extra precision.
   If they fail, distinguish mapping/order/lifetime faults from arithmetic,
   then use targeted precision only where a scientific need is demonstrated,
   or stop the cutover. No generic software-double framework is proposed.
   The CPU reference computes `(gather * phase) * factor`
   (`band.rs:1165`), while the old spatial adapter uses
   `gather * (phase * factor)` (`spatial.rs:372,385`).
   Preserve the reference order without inventing a bitwise-output contract.

### Resulting concrete next milestone

Implement one bounded, wave-wide nonzero-model residual refresh: unique
coarse-plane predictions in device scratch, followed by device-side native
endpoint combination, observed/predicted interpolation, flags/weights/
polarization reduction, subtraction, output phase/weight and the existing
seven-tap scatter. Keep CPU FFTW, efficient batched Clark, initial imaging and
the writer unchanged in this first proof. Use two owned reusable input slots
and explicit completion lifetimes; do not expose mutable in-flight buffers.
Keep source tiling/channel restriction and all shape, memory and error checks.

The performance deliverable is a **complete frozen-model residual refresh**
on the existing full-row deep32 workload, including preparation/FFT,
descriptors, staging, all refills, first-use setup, synchronization, final
download and inverse FFT. Compare contemporaneously with CPU W4, not just
the slower tap backend. Record actual requested/unique predictions, copies,
read counts/bytes, peak memory and complete elapsed time. If that establishes
a clear advantage with unchanged fixed-model checks, run the existing deep32
application, seven-product/nine-check CASA comparison and panels against a
contemporaneous CPU control. A refill/kernel-only win is not acceptance.

No new implementation, benchmark or acceptance waiver occurred in this audit.
The existing review alerts and CPU deep-W4 preservation uncertainty remain.
The report is a reviewed next direction, not a measured GPU improvement.
