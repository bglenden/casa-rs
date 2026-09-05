# Issue 586 initial-weighted construction source map

Truth class: pre-implementation mechanism and discriminator record
Date: 2026-08-30
Work issue: #586
Parent: #541 / programme #486

## Measured target

The complete frozen-shape serial observation at the issue #581 amended tree
took 443.898842 seconds. Its density pass took 72.588401 seconds and its second
MeasurementSet pass, which performs weighting, initial science accumulation,
gridded-artifact compilation, and artifact writing, took 238.392987 seconds.
Source service was about 10.4 seconds and overlapped 99.43 percent of its
12.669-second fill envelope; the 238.319-second consumer interval is therefore
the attribution target. The compiler body is byte-identical on current
`origin/main`.

The same pass produced a 6,300,504,416-byte gridded artifact and reported
395,038,080 source-group vector allocations, 196,602,895 reduction-map
insertions, 196,612,108 multiplicity-vector allocations, about 63.2 GB of
source-group capacity growth, and 6.29 GB of multiplicity capacity growth. It
emitted 196,602,895 records, so this real workload obtained effectively no
block-local record reduction. These counters establish a candidate family;
they do not attribute elapsed time by themselves.

## Mechanism map

| Source | Proven mechanism | Current equivalent or gap | Decision and owner |
| --- | --- | --- | --- |
| Pre-cutover casa-rs `fff9c2d...`, `casa-imaging/src/execution.rs:18190-18370` | Borrowed row-shaped batches, one planned tap representation, direct paired PSF/residual accumulation, and reusable workspace | Current weighting emits bounded blocks, but the gridded compiler independently rebuilds taps/phase and allocates a vector/map value for nearly every accepted sample | Adapt only after attribution. Reconstruction owns reusable flat group/record scratch and scientific equivalence; runtime owns its admitted lifetime. Do not restore the old runner. |
| Pre-cutover AW replay, `fff9c2d...`, `casa-imaging/src/lib.rs:31206-31412,32010-32075,40871-40985` | Compile bounded source-block programs once, spill under an explicit ceiling, prefetch one ordered segment, and replay without rebuilding routing | Current main already owns a private disk-backed gridded artifact, but its initial compiler uses allocation-heavy tree grouping | Retain the compile-once disk artifact and bounded streaming. Optimize compilation representation, not the artifact ownership or a new cache path. |
| CASA `VisImagingWeight.cc:320-375,760-815` | Retain density lattices and compute imaging weights from bulk visibility-buffer arrays | Current density is already frozen and reused; second-pass source I/O is not dominant | Retain exact two-pass weighting. No extra pass or ungridded cache. |
| CASA `GridFT.cc:715-785,930-985` | Prepare bulk buffer arrays, grid into one shared lattice by disjoint sectors, and reduce small sum-weight states deterministically | Current initial pass calls the science owner and artifact compiler serially per emitted block | Use as bulk/sector evidence only. First determine science-versus-compiler wall; do not add workers before serial improvement. |
| casacore `ArrayColumn.tcc:171+`, `LatticeCache.h:93+` | Caller-provided bulk arrays and bounded lattice caching | Current selected source already refills bounded caller-owned buffers; gridded artifact writing already reuses one I/O buffer | Retain. More read-ahead is rejected by the measured overlap. |
| LibRA `MultiThreadedVisResampler.cc:175-225,300-350` | Persistent workers borrow row ranges | LibRA also allocates a complete grid per worker and gathers whole grids | Reject full-grid replication and gather. Persistent-worker evidence does not authorize threading this serial campaign. |

## Stage-local discriminator

Extend the ignored mounted-medium probe on current `origin/main`. It captures
six bounded blocks spanning 263,250 rows and 33,696,000 selected samples, or
6.43 percent of the full sample count, below its fixed one-GiB residency limit.
Capture and density setup remain outside timing. Through the ordinary private
runtime/reconstruction seam, measure mutually exclusive:

1. weighting and contribution formation plus bounded block emission;
2. initial science-operator consumption;
3. reconstruction `compile_block`;
4. artifact append, checksum, copy, and write; and
5. finish and seal.

Record group/record counts, reduction ratio, every compiler allocation/growth
counter, encoded/copied/written bytes, peak residency, source/work identities,
artifact validation, and final scientific identity. Run baseline-candidate-
baseline interleaved on the same captured cohort. Instrumentation is rejected
if OFF observations differ by more than three percent or observer overhead
exceeds two percent of their mean.

The captured/full sample scale is 15.552. A five-percent full-wall improvement
requires 22.195 seconds, or at least 1.427 seconds saved by the captured
exclusive stage under linear scaling. The whole initial consumer has an
absolute ceiling of 225.724 seconds after retaining its source-fill envelope;
this is not attributed to the compiler.

## Conditional candidate

If `compile_block` is material, replace its per-sample
`BTreeMap<Vec<Record>, Vec<f64>>` with one planner-bounded reusable flat group
buffer, lexical sort, and run-length reduction. Inline the common small
spectral group and count repeated literal-one multiplicities with a bounded
integer. Preserve canonical lexical output and deterministic scientific
meaning. This is a generic reconstruction representation, not an MFS fast
path.

If initial science accumulation dominates instead, test the scientific
property of an empty initial model: skip zero forward prediction and perform
the already-proven identical dirty/residual accumulation once. Do not select
this candidate from mode name or benchmark parameters.

If neither exclusive stage projects conservatively to five percent of full
wall, retire the family. Do not run the 16-channel or complete workload.

## Accepted discriminator baseline

The final current-code release observation used the production compiler and
artifact writer over the six captured blocks. The OFF/ON/OFF totals were
14.805, 14.973, and 15.089 seconds. The two OFF observations differed by
1.903 percent, below the three-percent bound, and the observer added 0.176
percent, below the two-percent bound. Its mutually exclusive ON buckets were:

| Bucket | Captured seconds | Linear full-data ceiling |
| --- | ---: | ---: |
| Weighting and contribution formation | 6.921 | 107.6 |
| Initial science-operator consumption | 4.392 | 68.3 |
| Reconstruction `compile_block` | 3.114 | 48.4 |
| Artifact append, copy, and write | 0.453 | 7.0 |
| Science finish | 0.091 | 1.4 |
| Artifact seal and compiler finish | <0.001 | <0.1 |

All three observations reproduced the weighting identities, normal-state
identity, artifact identity and SHA-256, proof bytes and calls, compiler
allocation counters, payload copies and writes, and residency signatures.
The artifact contained 14,520,731 reduced records and 464,663,392 payload
bytes with SHA-256
`8ba96df08553820c4441f3a87fd84d90f324b21d14c8d8c7985e6164934ce154`.

The baseline changes the candidate order. Artifact I/O is retired as a first
target, and compiler allocation churn is only the third-largest exclusive
bucket. The first implementation candidate is the generic empty-model science
identity already used by CASA and the optimized pre-cutover casa-rs path:
owner-certified `A(0) = 0` permits skipping zero degridding, the duplicate
residual-grid accumulation, and unused final-visibility emission while keeping
the observed-data/PSF accumulation and deterministic identities unchanged.
For this cohort it removes 2,858,652,160 tap-cell visits and 33,696,000 unused
visibility-buffer pushes. Removing half of the convolution visits projects to
about 2.196 captured seconds, above the 1.427-second admission threshold.

Coverage-proof hash batching remains the next measured candidate if the
empty-model identity fails its gate: the current weighting owner makes
33,696,007 SHA-256 updates for 2,864,160,146 canonical bytes although it emits
only 8,227 weighted blocks. Any batching must preserve the exact byte stream
and coverage identity and must independently clear the same admission gate.

Focused verification was green: five reconstruction compiler tests, twelve
runtime artifact tests, the ignored mounted discriminator, warnings-denied
Clippy, formatting, and `git diff --check`.

## Retained empty-model candidate

The implementation uses one private reconstruction binding with two states:
an initial owner-certified zero generation and an evaluated model. Only an
`InitialMajor` pass with `ModelGenerationOrigin::Empty` selects the former.
Ingested zero samples, delta generations, and an empty generation carried into
`ResidualRefresh` retain the evaluated path. Explicit final-visibility output
still emits ordered zero predictions and observed residuals; a replay without
a sink no longer builds unused output samples.

The interleaved release observation was:

| Measurement | Baseline mean | Candidate | Saving |
| --- | ---: | ---: | ---: |
| Stage-local wall | 14.9068 s | 12.6916 s | 2.2152 s / 14.86% |
| Initial science consumption | 4.3451 s | 2.1846 s | 2.1605 s / 49.72% |

The stage-local saving exceeds both the 1.427-second projected-value gate and
the ten-percent total-wall gate. The linear full-data projection is 34.45
seconds, or 7.76 percent of the 443.898842-second serial baseline. This is a
projection only; no complete run has yet been admitted.

All pinned weighting-generation, replay, coverage, normal-state, artifact, and
artifact-SHA identities remained unchanged. Focused private work/correctness
tests passed 3/3, major-cycle integration passed 20/20, the release
discriminator passed, and reconstruction/runtime warnings-denied Clippy,
formatting, and `git diff --check` were green. The broader pre-existing test
`forward_is_linear_and_a_unit_centre_source_is_constant` remains red at both
this tree and baseline `81556dbc5`, with the identical 0.7500004989529645
versus 0.75 tolerance mismatch.

## Matched 16-channel production gate

The candidate then ran the directly mounted medium VLA MeasurementSet with 16
selected channels, 1024 pixels, Briggs 0.5, RustFFT, Hogbom 50 iterations, and
one worker. It completed in 83.619787 seconds. Exact structure remained two MS
passes, two source slots, 31,985 gridded frames, 49,141,788 records, and a
1,574,840,232-byte artifact.

Two observations at the untouched pre-candidate commit recorded initial
weighted consumer intervals of 62.172974 and 62.491394 seconds. The candidate
recorded 57.317897 seconds, a stable 4.855-to-5.173-second or 7.81-to-8.28
percent reduction in the affected production pass. Both baseline density
passes were externally I/O-starved at 50-to-55 MiB/s while the candidate read
at 198.6 MiB/s, so their 102.739749- and 99.701812-second total walls are not
used to inflate the candidate speedup.

Image, residual, PSF, and model products were bit-identical to the frozen
16-channel reference: normalized RMS and maximum normalized difference were
zero for all four. The raw comparison artifact is
`/private/tmp/issue586-ch16-recovered/20260830T160636Z-wave3-standard-mfs-single-term-turnaround-c43999f9.comparison.json`.
The timed candidate log is
`/private/tmp/issue586-ch16-candidate/20260830T160214Z-wave3-standard-mfs-single-term-turnaround-62dc0894.log`.

Receipt publication exposed a pre-existing schema mismatch: the comparator's
canonical unrequested-metadata sentinel is `{status: not_required, parity:
null}`, while the outer run schema requires a boolean whenever `parity` is
present. Repairing that versioned receipt contract is outside this issue's
explicit scope. The raw product comparison completed and reused both product
prefixes; neither Rust science nor CASA was rerun.

## Complete 64-channel CASA gate

The admitted complete run used the directly mounted medium VLA MeasurementSet,
64 selected channels, 1024 pixels, Briggs 0.5, RustFFT, Hogbom 500 iterations,
and one worker. It reused the frozen CASA products and 688.996833-second timing;
CASA was not rerun.

The candidate completed in 462.066116 seconds: 226.930717 seconds less than
CASA, or a 1.491122x speedup and 32.936 percent lower wall time. Its measured
serial phase envelope was:

| Phase | Wall | Fraction of Rust wall | Relevant detail |
| --- | ---: | ---: | --- |
| Density MS pass | 75.029541 s | 16.24% | 33.411245 s source read |
| Initial weighted/artifact MS pass | 213.434899 s | 46.19% | 213.360251 s consumer; 10.720705 s source read |
| Ten later gridded replays | 166.288195 s | 35.99% | 161.587185 s kernel commit; 55.445386 s artifact fill overlapped |
| Minor-cycle, completion, and product-write remainder | 7.313481 s | 1.58% | Derived exclusive remainder |

The prior accepted complete observation recorded 238.392987 seconds for
initial weighted construction. The candidate therefore removes 24.958088
seconds, or 10.47 percent, from the affected complete-data pass. That is 5.62
percent of its 443.898842-second reference wall and confirms the stage-local
projection. The 443.898842-second observation included unmerged #581 serial
route experiments, so it is not used as a direct total-wall before/after
control for this current-main candidate. The valid cross-implementation total
comparison is the candidate's 462.066116 seconds against frozen CASA; the valid
candidate attribution is the interleaved local gate plus the affected-pass
reduction.

Correctness passed far inside the agreed 0.001 normalized-RMS threshold:

| Product | Normalized RMS |
| --- | ---: |
| Image | 1.925707e-7 |
| Residual | 3.258093e-7 |
| PSF | 6.786875e-7 |
| Model | 5.147635e-7 |
| Sum weights | 0.0 |

The run retained exactly two ordered MeasurementSet passes, two source slots,
one worker, 62,272,500 bytes peak source-buffer capacity in each pass, and the
pinned compiler/artifact work signature: 395,038,080 source records,
196,602,895 emitted records, 6,300,504,416 artifact bytes, and 6,291,292,640
payload bytes. The raw comparison artifact is
`/private/tmp/issue586-full-candidate/20260830T161408Z-wave3-standard-mfs-single-term-heavy-wave2-serial-c16928ac.comparison.json`.

This full gate retains the empty-model candidate. It also selects the next
serial discriminator, if this ticket continues: weighting and contribution
formation is still the largest exclusive initial-pass bucket. Coverage-proof
hash batching is the first bounded hypothesis because the six-block cohort
made 33,696,007 SHA-256 update calls for 2,864,160,146 canonical bytes while
emitting 8,227 blocks. It must prove its own wall saving locally before another
production run.

## Review-blocker closeout

The final stage-local seam dispatches once per block. Normal production uses an
unobserved compiler specialization: source cardinality is not counted in its
sample loop, reduced groups are derived from map insertions, and reduced
records are derived from encoded fixed-width bytes. The ignored discriminator
selects the observed specialization and derives source groups and records while
traversing the already-grouped records for reduction. Both specializations use
the same scientific phase functions and encoding.

The fresh OFF/ON/OFF release discriminator passed with exact pinned science,
artifact, proof, allocation, copy, write, residency, and cardinality
identities. OFF observations were 13.025766 and 13.087971 seconds, differing by
0.476 percent. The observed run was 12.850460 seconds, so measured observer
overhead was zero. Its mutually exclusive buckets were:

| Bucket | Captured seconds |
| --- | ---: |
| Weighting and contribution formation | 6.923604 |
| Initial science-operator consumption | 2.230914 |
| Record-key construction | 0.595339 |
| Grouping and reduction | 2.094396 |
| Encoding and checksums | 0.511992 |
| Payload movement | 0.004147 |
| Artifact writes | 0.247914 |
| Completion | 0.177958 |
| Science finish | 0.061814 |
| Callback and residual timing orchestration | 0.003006 |

The artifact retained SHA-256
`8ba96df08553820c4441f3a87fd84d90f324b21d14c8d8c7985e6164934ce154`,
14,520,731 records, and 464,663,392 payload bytes. The split identifies
weighting as the largest remaining bucket but authorizes no additional
optimization in this ticket.

The comparison-metadata null-sentinel repair was removed from this change
because #586 explicitly excludes versioned receipt-schema changes. Raw product
comparison artifacts retain the accepted CASA NRMS evidence. The harness
contract mismatch is recorded separately.

## Fixed constraints

- Frozen CASA remains 688.996833 seconds; do not rerun it.
- Exactly two ordered MeasurementSet passes and two source slots.
- No MeasurementSet materialization, ungridded cache, extra source copy,
  per-worker grid, second executor, mode-specific path, fallback, public
  performance selector, or application/frontend calculation.
- Serial performance is the admission gate. Workers cannot mask a serial miss.
- Product acceptance is normalized RMS no greater than 0.001; exact artifact
  identity is diagnostic unless the candidate is representation-only.
- A red stage-local gate vetoes medium and full runs.
