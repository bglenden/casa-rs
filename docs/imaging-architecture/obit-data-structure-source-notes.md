# Obit data-structure and gridding source notes

Truth class: non-normative primary-source implementation research
Last reality check: 2026-09-20
Verification: primary-source inspection plus separately qualified local reference timings

This note records a bounded inspection of the official [Bill Cotton Obit
repository](https://github.com/bill-cotton/Obit) at master commit
[`ebc1c229e5e3870b5ce3c342bddb7313d986a06f`](https://github.com/bill-cotton/Obit/tree/ebc1c229e5e3870b5ce3c342bddb7313d986a06f).
The [Obit project page](https://www.cv.nrao.edu/~bcotton/Obit.html) is the
official project context; the repository is the implementation source. These
observations are research inputs, not casa-rs API, persistence, or acceptance
contracts.

## Local reference timing addendum

On 2026-09-20 the user requested local timings for reference only. A pinned native
ARM64 core/Imager build completed without global installation. Same 42,120 rows,
512 channels and 512x512 pixels: dirty W1 47.586538 s, W4 20.210558 s; all dirty
pixels match exactly. CLEAN9 raw times are 77.163547 / 39.505559 s, but images
differ by 4.2503% relative L2; **do not treat that ratio as equal-output scaling**.
One observation per configuration, no significance claim.

Obit forces different single-plane/grouped-line execution paths at one/four
threads. Native topocentric spectral channels, float grids, per-channel CLEAN
controls, product inventory and storage location differ from the CASA acceptance
reference. These numbers neither satisfy nor lower casa-rs acceptance targets.
No Obit numerical code was modified or copied into casa-rs.

Durable build, input/output validation, exact settings, logs and interpretation:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-build/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/REFERENCE.md`.

## Evidence

### Flat visibility records and descriptor indexing

- `ObitUV` owns one flat `ofloat *buffer` with a float-count `bufferSize`; it
  also has optional `nParallel`, `multiBufIO`, and `multiBuf` arrays for
  parallel-buffer reads ([`ObitUVDef.h`, lines 34-47](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/include/ObitUVDef.h#L34-L47)).
- `ObitUVDesc` carries the record stride `lrec` (floats), random-parameter
  count `nrparm`, 1-relative `firstVis`, buffered count `numVisBuff`, axis
  dimensions/labels, frequency arrays, random-parameter offsets (`ilocu`,
  `ilocv`, `ilocw`, etc.), regular-axis locations (`jlocf`, `jlocif`, ...),
  increments, and total complex-correlation count (`ncorr`)
  ([`ObitUVDescDef.h`, lines 34-45 and 117-180](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/include/ObitUVDescDef.h#L34-L45),
  [`ObitUVDescDef.h`, lines 117-180](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/include/ObitUVDescDef.h#L117-L180)).
- Descriptor indexing scans labels to assign random-parameter offsets, derives
  the correlation count, and sets `lrec = nrparm + size`; regular-axis
  indexing separately identifies frequency, IF, Stokes, and sky axes
  ([`ObitUVDesc.c`, lines 410-449](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVDesc.c#L410-L449),
  [`ObitUVDesc.c`, lines 456-492](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVDesc.c#L456-L492)).
- The base CPU gridder uses that contract directly: `ivis = kvis * lenvis`,
  reads `u,v,w` from the record start, then locates each channel's three
  visibility floats after `nrparm` ([`ObitThreadGrid.c`, lines 1293-1319](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1293-L1319)).

### Bounded strip-mining and I/O

- `ObitUVOpen` treats `nVisPIO` as the maximum transfer target, allocates a
  selector-sized buffer, and keeps the descriptor's `firstVis` and
  `numVisBuff` as the current window state ([`ObitUV.c`, lines 1004-1019](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUV.c#L1004-L1019),
  [`ObitUV.c`, lines 1130-1171](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUV.c#L1130-L1171)).
- When using its internal buffer, `ObitUVRead` validates
  `nVisPIO*lrec <= bufferSize`, performs one backend
  read, and copies the backend's `firstVis`/`numVisBuff` back to the public
  descriptor ([`ObitUV.c`, lines 1296-1342](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUV.c#L1296-L1342)).
- The transfer-size helper accounts for `nThreads`, caps the aggregate
  requested record volume at one billion bytes, and then caps the returned
  `nVisPIO` at the caller's requested `nvis` ([`ObitUVDesc.c`, lines 1387-1420](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVDesc.c#L1387-L1420)).
- In the FITS backend, one read transfers exactly
  `sel->numVisRead * desc->lrec * sizeof(ofloat)` bytes; compressed input uses
  a bounded compression buffer and a per-record conversion/decompression loop,
  while uncompressed input is byte-swapped into the output window
  ([`ObitIOUVFITS.c`, lines 647-725](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitIOUVFITS.c#L647-L725)).
- Multi-buffer reads still issue one underlying block read. When no independent
  calibration is needed, the FITS implementation copies the transformed first
  buffer to the remaining buffers with `memcpy`; calibration/select paths can
  process buffers independently ([`ObitUV.c`, lines 1345-1433](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUV.c#L1345-L1433),
  [`ObitIOUVFITS.c`, lines 1008-1017](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitIOUVFITS.c#L1008-L1017)).

### Gridding ownership and phase boundaries

- `ObitUVGridReadUVPar` reads one UV window at a time, invokes the threaded
  grid operation on that window, and only after EOF performs conjugate folding
  and grid merge ([`ObitUVGrid.c`, lines 516-648](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGrid.c#L516-L648)).
- Base setup derives `nGpI` replicated grids per image from `nPar` and
  `nThreads` (with a minimum of four replicas when at least four threads are
  available), sets contiguous visibility ranges, points every worker at the
  same `UVin->buffer`, and allocates a separate zeroed `grid` for each
  replica; `outGrid` points at the final image/beam grid
  ([`ObitThreadGrid.c`, lines 244-313](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L244-L313),
  [`ObitThreadGrid.c`, lines 315-405](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L315-L405)).
- Each window's visibility ranges are dispatched in batches of at most
  `nThreads`; a short final batch adjusts its upper bound to the actual
  `numVisBuff` ([`ObitThreadGrid.c`, lines 827-866](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L827-L866)).
- A worker prepares each visibility once and then grids its selected channels
  into its private grid. The separate flip phase adds conjugate rows, and the
  merge phase accumulates replica grids into the output in row-major loops
  ([`ObitThreadGrid.c`, lines 960-991](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L960-L991),
  [`ObitThreadGrid.c`, lines 998-1105](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L998-L1105)).

### CPU preparation, reuse, and SIMD

- `fast_prep_grid` rotates `u,v,w` once per visibility, then for each channel
  applies frequency scaling, validity/guardband/baseline tests, tapering, and
  weighted complex-data preparation. It caches per-channel cell coordinates
  and pointers into the convolution table (`iuarr`, `ivarr`, `cnvfnu`, and
  `cnvfnv`) for the subsequent stencil loop ([`ObitThreadGrid.c`, lines 1263-1387](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1263-L1387)).
- `fast_grid7` has compile-time AVX-512 and AVX paths. They duplicate complex
  kernel coefficients, broadcast/interleave the complex visibility, load grid
  rows, multiply by the separable convolution coefficients, and store updated
  rows; the AVX-512 path uses FMA. The C fallback walks contiguous `u` cells
  inside each `v` row and skips zero `cvv` rows ([`ObitThreadGrid.c`, lines 1886-1989](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1886-L1989),
  [`ObitThreadGrid.c`, lines 1990-2028](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1990-L2028)).
- The phase rotation path also has an AVX-512 channel-block implementation
  using vector frequency/phase arithmetic and gather/scatter of visibility
  samples; the remainder falls back to scalar loops
  ([`ObitThreadGrid.c`, lines 2087-2177](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L2087-L2177)).

## Limits and casa-rs relevance

- The private-grid observations describe the base `ObitThreadGrid` path. The
  source has separate multifrequency and wideband setup routines; this note
  does not claim that all spectral modes have identical worker ownership or
  perfect spectral parallelism ([`ObitThreadGrid.c`, lines 419-621](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L419-L621)).
- Shared input-buffer ownership does not mean grids are always shared: the
  base CPU path gives each replica a private accumulator and merges later; the
  multi-buffer I/O path has its own copy/calibration conditions. Replica grids
  can therefore multiply memory, and this source inspection provides no
  workload-specific topology or speed claim.
- The AVX/AVX-512 code is conditional on build macros; no claim is made that a
  particular Obit binary enabled those paths. No benchmark or numerical parity
  result was collected here.
- Cited files carry the Obit GPL notice, including GPL version 2 or later
  language ([`ObitThreadGrid.c`, lines 1-18](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitThreadGrid.c#L1-L18)).
  Treat the source as an implementation reference; any code reuse or derived
  distribution needs an explicit license review. The observations do not grant
  permission to copy Obit code into casa-rs.

The transferable questions include record interpretation outside hot loops,
bounded source ownership, and accumulation locality. The larger scheduling and
representation questions below are more important than copying individual
loops. Obit's source alone does not establish the best casa-rs design.

## Spectral processing: two different parallelization strategies

### Independent processing streams (2008)

Cotton's [memo 2, sections II-III and Table I](https://www.cv.nrao.edu/~bcotton/ObitDoc/Line.pdf)
describes calibrating and splitting channels once with SplitCh, independent
streams that image and deconvolve their assigned channels, and serial assembly
with MCube. Its dual-core, 109-channel VLBA test reported 5.68 minutes for one
single-threaded stream, 5.23 for one two-threaded stream, and 2.70 total for two
independent streams, including splitting and assembly. This is historical
evidence for moving the parallel boundary outward, not a speed prediction for
our machine or a claim of identical numerical contracts.

### Grouped channel advancement (2022 and current source)

[Memo 74, sections II-B, III-D and IV](https://www.cv.nrao.edu/~bcotton/ObitDoc/ParallelLine.pdf)
describes doLine groups with one CPU thread per channel. Imaging-only work
benefited from channel parallelism, but unequal CLEAN workloads left some work
on finished channels until the whole group completed. The 24-channel example
needed 1-18 major cycles per channel. Its apparent single-channel CLEAN advantage
is qualified: that mode used a more relaxed convergence criterion, and all
CLEAN runs used GPU degridding. These are not clean CPU-only or equal-acceptance
comparisons. The lesson is to avoid unnecessary lockstep work, not that spectral
line imaging is inherently unsuitable for parallel execution. Both memo timing
tables were inspected as rendered PDF pages, not just extracted text.

Current [Imager.c:1609-1619](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/tasks/Imager.c#L1609-L1619)
caps parallel channels by threads and selected channel groups.
[Lines 1680-1703](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/tasks/Imager.c#L1680-L1703)
iterate channel blocks and copy/calibrate or average selected data;
[1755-1803](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/tasks/Imager.c#L1755-L1803)
construct the single/group CLEAN owner;
[1869-1896](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/tasks/Imager.c#L1869-L1896)
insert results into the cube before advancing the group. This is not a dynamic
queue that immediately replaces each finished channel with another.

[`ObitDConCleanVisLineSelect`:1125-1150](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitDConCleanVisLine.c#L1125-L1150)
loops over channels, calls parent selection for unfinished ones, and aggregates
completion. [MakeResidualsLine:1933-1941](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitDConCleanVisLine.c#L1933-L1941)
passes done flags to imaging. Thus a parallel-imaging label does not imply
every phase is concurrently executing independent channel lifecycles.

## Other architectural lessons and counterexamples

- [Memo 6, section II](https://www.cv.nrao.edu/~bcotton/ObitDoc/ParallelFacets.pdf)
  changes facet imaging from repeated dataset traversals to one read feeding
  multiple grids. It also documents buffer copies, replicated accumulators,
  and serial normalization/output. Transfer the reuse principle, not the
  entire allocation strategy. Facets share data differently from independent
  spectral channels; direction-dependent calibration can limit reuse.
- [Memo 57, sections II-IV](https://www.cv.nrao.edu/~bcotton/ObitDoc/DoubleBuffer.pdf)
  found no significant improvement from its explicit double-buffering scheme
  on the tested systems. More queues or asynchronous I/O are not automatically
  useful. This does not prove all buffering is useless, especially on other
  hardware or workloads.
- Obit does not eliminate intermediate I/O. In
  [`ObitSkyModelSubUV`:973-1018](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitSkyModel.c#L973-L1018),
  a block is read, the model is calculated/subtracted in that buffer, and the
  modified block is written. Model loading and loops over images/PB channel
  groups also occur. The inspected path is a direct visibility-space alternative
  to our compiled normal replay, not evidence of a globally zero-copy system.

## Application to the current casa-rs problem

Local evidence: `t55/tranche5-20260916/CURRENT.md` and the pinned
`initial-planes-candidate` under
`t55/q-band-rebaseline-20260918/overnight-scaling-20260919` in the durable
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence` tree.
The 42,120-row dataset has 363,586,532 logical bytes; initial compilation writes
4,305,043,032 bytes / 107,616,600 records. That roughly twelvefold expansion
includes a reusable operator representation, not simply another copy of the
input. Current four-worker task time is 85.42 s versus CASA serial 66.53 s;
these are single observations. Initial plane dispatch improved selected stages
but not convincingly the whole task. Obit was not timed on this workload.

Two mathematical details prevent a naive port:

1. [`spectral_records.rs::standard_predictions`](https://github.com/bglenden/casa-rs/blob/9d53664c53ec7fde002250ba03ccb7dc7bc354a7/crates/casa-imaging-reconstruction/src/gridded_normal_operator/spectral_records.rs)
   derives native prediction contributions. `resampled_record_groups` combines
   prediction banks with interpolation factors before the accumulation stencil.
   These records encode scientific work; deleting the representation must not
   delete that work or assume all output channels can evolve independently.
2. [`spectral_cycle.rs::run_stream`](https://github.com/bglenden/casa-rs/blob/9d53664c53ec7fde002250ba03ccb7dc7bc354a7/crates/casa-imaging-runtime/src/spectral_cycle.rs)
   currently overlaps first-slab science and compilation, then traverses later
   slabs sequentially. More threads inside `consume_bounded_replay_chunk` do
   not move this outer lifecycle boundary. Existing plane-owned reconstruction
   and bounded execution mechanisms should be reused, not another thread pool.

### Recommended discriminating prototype (proposal, not implemented)

Compare the existing compiled-record route with a bounded compact-visibility
route for the same scientific workload. Give each worker a plane or the minimum
mathematically coupled channel band, owning its grids and FFT/reconstruction
scratch across substantial processing stages. Arrange selected data for those
owners once where beneficial; preserve exact native-frequency prediction,
resampling, weights, masks, convergence and ordered output semantics. Admit
work using the existing shared memory budget and bounded queues. Do not require
all channels resident or introduce per-worker full-cube grids.

The experiment must include preparation, any compact-data write/read, gridding,
FFT, deconvolution, final residual and product output. Measure total work bytes,
repeated transformations, worker idle time and end-to-end time as well as RSS.
This tests both representation amplification and the scheduling boundary, rather
than polishing one more local loop. Explicitly account for any legitimate
cross-channel dependencies and common stopping decisions before allowing
independent CLEAN advancement. An alternative may lose if repeated computation
costs more than compiled replay, particularly over many major cycles.

Do not import Obit's averaging, precision, FMA/reduction order, faceting or
CLEAN thresholds to obtain a speedup. Its x86 AVX paths are not Apple Silicon
performance evidence. Private-grid replication is not approved for our current
budget. The scientific and CASA-compatible persistence contracts stay fixed.
This research authorizes neither a new algorithm nor implementation; a scoped
design review, including the previously proposed Oracle challenge, comes before
an architectural experiment. No production files changed in this investigation.

## Reproduction and durable evidence

- Source-only sparse checkout (28 MiB observed):
  `/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/obit-source-study-20260920/Obit`.
  Checked HEAD equals the revision above, dated 2026-08-20.
- The same directory's parent contains `Line.pdf`, `ParallelLine.pdf`, their
  page-2 PNG renders, and a durable copy of this report. No installation scripts
  were run and no data were sent to Obit.
- Primary source inventory was delegated to Luna Max; main-agent inspection
  verified key buffer, gridding, task-loop and model-subtraction claims and
  supplied the interpretation. Research/PDF procedures informed this record;
  no timing campaign, production patch, push, merge or cleanup was performed.
