# Oracle review for project: casa-rs — canonical bulk imaging

Initiated by Codex, 2026-09-27. This is a design review and implementation-plan
request, not authorization to change scientific algorithms or launch benchmarks.

## Question and owner direction

The owner asks us to fix a recurring failure once: all imaging must use genuinely
block-oriented input, preparation, numerical execution and storage, not bulk I/O
that disintegrates into expensive per-sample adapter chains. Design one shared
path, its migration/deletion sequence, and enforceable regression checks so that
cube, MFS, MT-MFS, W/AW, mosaics, weighting and prediction do not rediscover the
same mistakes independently. Prefer simple flat buffers and narrow, deep modules.
Different scientific kernels are necessary; duplicated transport, resource and
validation frameworks are not. Do not turn this into a universal tensor engine.

Please challenge the proposed direction independently. Inspect accessible source
call chains rather than trusting function names. State access gaps explicitly.
Resolve the main design choices, especially the physical layout, replay versus MS
reread, validation/integrity placement, and migration across ALL existing modes.
Return a concrete plan with a few review stops and complete application slices,
not a collection of individually tested but unwired components. Identify the
smallest discriminating experiment for any genuinely unresolved choice.

## Constraints

- Preserve standard CASA scientific algorithms, selection, spectral frames and
  interpolation/extrapolation, polarization, flags/weights, model prediction,
  PSF/PB normalization, masks/WCS/beams, existing CLEAN and persisted writer.
- Preserve approximately 1e-3 normalized agreement and all stricter existing
  scientific gates. Ordinary floating reduction-order differences are allowed;
  bitwise reproducibility, compensation and extra precision need demonstrated
  scientific necessity, not aesthetic justification.
- Bounded streaming, one existing resource authority/residency policy; 16 GiB
  planning and sampled aggregate RSS, two Cargo build jobs. Do not materialize a
  whole MS or duplicate whole cubes per worker. Huge cubes such as 2k x 2k x 2k
  must work in principle with admitted output ranges and required source rereads.
- Existing product ownership rule forbids content hashing or full-array rereads
  solely to authorize publication. Individual images replace atomically; failure
  leaves an incomplete run requiring rerun, not whole-set rollback/recovery.
- Genuine external-input freshness and persistence-boundary integrity are
  distinct requirements. Do not casually delete their guarantees, but challenge
  expensive implementations and identify exact requirement changes if necessary.
- No fallback or permanent parallel old path. Temporary migration coexistence
  needs an explicit mode matrix and deletion criterion. No second CLEAN loop.
- Planning only now. Failed full W1 was stopped at owner request; W4 prohibited.
  No push/merge/release/cleanup/install/full-run authorization. Preserve failed
  evidence and dirty work. Do not revive the failed cache candidate as a win.

## Source access and provenance

Public repository: https://github.com/bglenden/casa-rs
Current HEAD: 3cb9252c19746c8386e5d1b1f7ff89c81415e1d5
Branch: codex/t55-full-size-validation.

Use these revision-pinned sources, and inspect callers/callees when useful:

- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-application/src/lib.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-runtime/src/weighting/native_preparation.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-ms/src/selected_observation/bound_observation.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-ms/src/selected_observation/indexed_block.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-reconstruction/src/streaming_cube/preparation.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-reconstruction/src/streaming_cube/input.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-runtime/src/streaming_cube/input.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-imaging-runtime/src/streaming_cube/execute.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/crates/casa-tables/src/storage/tiled_stman.rs
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/docs/imaging-architecture/bounded-streaming-performance-spec.md
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/docs/imaging-architecture/streaming-cube-replacement-plan.md
- https://github.com/bglenden/casa-rs/blob/3cb9252c19746c8386e5d1b1f7ff89c81415e1d5/docs/imaging-architecture/cube-buffer-refactor-20260924.md
- ADRs 0010 (one resource authority), 0011 (scientific coupling), 0013 (private
  spill CRC32C), 0014 (trusted products without attestation) under the same pin.

Known optimized ancestor is fff9c2d553eace4b6a57b1df9ded4773f2263ceb. Reuse useful
mechanisms, not deleted packages or duplicate production routes. CASA/casacore
define science semantics; LibRA/Obit can supply relevant buffer/loop techniques,
not an obligatory new broad survey. Local code copies exist but are not visible
to you; ask for a decisive missing excerpt, not an exhaustive new inventory.

Current uncommitted changes are NOT visible at those URLs: cache admission now
uses phase live workspace rather than preparation arena; native frame cache
can grow from 331 to 3,228 slots (about 70 to 688 MB on this case), reclaiming
optional image cache; TSM has a bounded selected-span read-ahead repair; tests and
timers accompany these changes. Combined candidate failed full performance and
is retired for promotion. It does not change the fundamental layouts or scalar
call chain below. No source-selection override is present in production.

## Evidence: current full workload and failed candidate

VLA C-array simulated complex sky, LSRK, 4,094,064 rows, 512 channels, two
correlations, 1024-square output, natural weighting, efficient batched Clark,
0.5 mJy threshold, 10,240,000 component cap, no major-cycle cap. FFTW optimized
NEON build; W1 FFT threads=1. Same selection/controller/writer across runs.

- Retained CASA serial: 21,622.668 s (about 6 h).
- Previous casa-rs W1: 26,620.317 s, peak RSS 6.90 GiB, 17 minor cycles and 18
  imaging passes, 2,164,568 components. All hard per-plane checks passed; full
  comparison still has 22 review alerts, no waiver. No full acceptance claim.
- Enlarged cache lowered reads per full refresh from 268.315 GB / 1,652,736
  frame reads / zero hits to 75.693 GB / 551,988 reads / 1,100,748 hits.
- Corrected short 32-output test retained ALL source rows/channels/store geometry:
  parent/candidate end-to-end 804.399/791.954 s, prep 671.233/669.070 s, initial
  55.699/55.703 s, refresh 70.108/59.791 s. Refresh read wall 16.402 -> 6.127 s.
  All seven products/nine unchanged checks passed, worst normalized RMS 2.17e-6.
  Diagnostic source-selection override existed only in both test snapshots.
- Full candidate instead took typical refreshes about 2,645 s vs old 1,290–1,573
  s. Killed at 30,632 s, refresh 12 channel 329/512, RSS peak 10.47 GB. W4 never
  started. Same byte/read counts per refresh, but read_exact_at time varies from
  95.4 s (refresh 5) to 1,654–1,754 s in most later passes. These are requested
  frame bytes and read wall, NOT physical device traffic. LRU, CRC and decoding
  are outside that timer; about 900 s of non-read work also remains per refresh.

Native replay file is 75,693,263,184 B, about 70.49 GiB, with 1,076 row blocks
and 3,805 rows/block. Physical order is row block -> row metadata frame -> 512
single-channel data frames. One data frame is rows*36+4 bytes, metadata rows*56+4.
Execution visits output band -> all row blocks -> metadata plus two adjacent
source-channel frames. Thus reads leap about 70.35 MB between successive row
blocks. Spectral neighboring channels overlap, hence cache hits. Enlarging the
cache removes repeated bytes but does NOT repair locality or explain variable
latency. Source/store/image working-set and access order matter.

A small read-only C replay on the ACTUAL retained file reproduces slow I/O:
8 output channels (240–247), all 1,076 blocks, two passes, same F_NOCACHE and LRU.
Per pass 1.556 GB requested, 10,760 pread calls, 15,064 hits; 30.258/30.241 s wall,
0.522/0.513 s CPU, 30.175/30.159 s pread, p95 17.45 ms, p99 19.39 ms, RSS 690 MB.
Disabling read-ahead only on this descriptor gives 30.318/30.741 s: no benefit.
This isolates slow file reads in 30 s without CLEAN, but omits image-memory and
CRC/decode/science costs. External storage is APFS on NVMe/PCI Express. Do not
assert root cause is the device, OS cache, LRU, or CRC without discrimination.

## Evidence: preparation and data layout, not just storage calls

Measured full preparation summary: inspection 187.339653 s, preparation
502.529188 s, ordered commit 43.758993 s, total 733.859 s. Actual MS source-read
30.167 s and source-fill 31.536 s overlap this work and are not added to it.
Native coverage counters: 222,987,289,914 encoded/hash bytes and 4,220,979,990
hash update calls. Do not attribute all 503 s to hashing without a focused check.

The MS source already uses bulk tiled column reads, not per-cell disk syscalls.
DATA/FLAG tile shape is [2,20,3276], about 1 MiB DATA and 16 KiB FLAG. However
tiled_stman::fill_typed_selected_2d_rows_by_copy does:

```text
for selected_row in row_patches:
    for channel in overlap:
        src = (channel_in_tile * tile_corr_count) * element_size
        dst = (selected_channel * selected_row_count * corr_count
               + output_row * corr_count) * element_size
        copy_from_slice(corr_count * element_size)  # 16 bytes in this case
```

It produces [channel][row][correlation] memory. NativeBlock instead stores
[row][channel][correlation]. Native writer then loops tile(channel), block part,
row, tile channel, correlation and serializes individual little-endian values
into per-channel frames. Therefore even a bulk source has tiny transpose/copy
operations and a second layout transition before replay.

Current runtime native preparation (simplified directly from source):

```text
NativeKernel::consume(storage, execution):
    channels/runs/shape checks
    consumer.inspect_block_range(storage, 0..runs)      # separate full walk
    for admitted worker row range:
        worker.projector.visit_block_range(problem, storage, range, |run|:
            reported = run.samples().next()
            initialize native worker/layout if needed
            contributions = spectral.compile(reported.selected(),
                                               reported.spectral_evaluation())
            native.consume_channel(run.row(), run.channel(), run.correlations(),
                                   reported.row_geometry(), output_hz,
                                   contributions))
    flush worker parts through one ordered commit
```

`inspect_block_range` calls `visit_selected_sample_range` and
`inspection.push_run(row, channel, correlations)` (or counts) for every run.
`SelectedObservationProjector::visit_block_range` calls the same visitor, clears
a bounded evaluations Vec, and for EACH correlation constructs a SampleView,
projects spectral geometry, stores an evaluation, then calls the run callback.
No heap allocation per sample is claimed: much scratch is reused. The repeated
object/check/dispatch work is nevertheless real.

`NativePreparationWorker::consume_channel` checks lifecycle each call, shape,
channel identity, source address, geometry.matches_sample and first-pair bits,
row/channel sequence. Row metadata is installed once but many invariant checks
are repeated per channel. It computes required group flags/weight/taper and
contributions, copies the correlation slice into NativeBlock arrays, calls the
coverage encoder per correlation, and finishes per-row digests. Scientific
per-value checks cannot simply disappear; source/shape facts need correct scope.

NativeBlock already uses simple arrays: Vec<RowMetadata> (physical row, UVW,
phase shift, original frequency pair), Vec<f64> frequencies per row/channel,
Vec<Complex32> values, Vec<f32> weights, Vec<bool> flags/weight_flags, dimensions.
The problem is not merely replacing owned sample objects with borrowed ones.

## Existing mode split and previous promises

Application `run_native` currently selects CubePhase only if no MODEL_DATA or
CORRECTED_DATA write and CubePhase::supports succeeds; otherwise it selects
SpectralCycleExecutor. Shared `run_native_phases<P: MajorCyclePhase>` owns CLEAN
and product publication. CubePhase supports natural weighting, channel-local
basis, empty starting model, no visibility transform, single source/DD/SPW/pol,
at least two input channels, and BandPlan-supported science. Other modes remain
on the record-oriented spectral route. Prepared AW has additional ownership.

The August bounded-streaming spec already promised one source -> bounded runtime
executor -> partitioned kernel, no duplicate runner, compact views and block
coverage. It measured borrowed scalar traversal 4.719 ns/sample, exactly the
same as owned scalar, versus 3.373 for row/channel runs. That lesson did not
prevent this recurrence. The later cube plan migrated only natural standard
channel-local cube, leaving other modes behind a capability split.

The buffer-refactor plan deliberately kept one-output-channel bands initially,
with plane-aligned managed image buffers and resource-aware spilling. Its typed
residency system is reusable and should not be replaced with a new memory
manager. The new plan should supersede the narrow visibility/replay choices
explicitly where needed, not undo the useful image-buffer work.

Dependency ownership: model owns scientific meaning; reconstruction owns
numerical algorithms; runtime composes MS source, execution/resource/residency;
application owns sole production composition, CLEAN and publication. No new
dependency inversion or giant all-purpose subsystem is desired.

## Specific decisions requested

1. Choose a canonical block boundary, concrete minimal data layout and ownership
   interface. Which facts bind once per source/selection, row, channel span,
   worker and block? How do heterogeneous DD/SPW/polarization, gapped channels,
   changing frame geometry and multiple fields/facets work without sample objects?
2. Choose a locality-preserving consumption/storage plan. Should we use grouped
   output-plane waves consuming each source block once, a channel-group-major
   replay layout, direct tiled MS rereads, or a measured combination within one
   canonical interface? Account for prediction halos and mandatory weighting
   passes. Don't force every data set through an expanded 70-GB intermediate.
3. Can one layout sensibly serve cube AND MFS/MT-MFS/W/AW? Distinguish shared
   transport/validation from specialized tight scientific loops, and bounded
   block transpose from hidden per-sample adaptation. Name deletion targets.
4. Classify input generation proofs, coverage digests, block shape/lifecycle
   checks and persistence CRCs by concrete consumer/failure model. Propose exact
   scope/placement reductions; do not rebrand product attestation or silently
   weaken external mutation detection. Flag any ADR/user decision needed.
5. Provide a phased all-mode migration table, earliest complete application
   deliverable (cube plus an MFS exercise to test generality), a few review
   checkpoints, and eventual removal of displaced APIs/routes. Do not postpone
   all integration until every primitive has been polished.
6. Define architecture/performance regressions that prove real batching:
   production-path call/copy/hash/allocation counts, actual selected tile bytes
   and I/O geometry, absence of sample dispatch outside tight kernels, varying
   block/worker sizes, sparse selections, I/O/cancellation errors and unchanged
   scientific gates. Avoid wall-time CI thresholds and word bans.
7. Name the one or two missing discriminating measurements, if any, before
   implementing the chosen plan. What does this evidence genuinely support,
   versus speculation? Do not promise a 2x CASA result from removed bytes alone.

Please conclude with a recommended design, explicit disagreements/risks, and a
restart-ready engineering sequence with clear exit criteria.
