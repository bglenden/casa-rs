# Metal FFT discriminator — 2026-09-29

Truth class: non-normative measured research
Last reality check: 2026-09-29
Verification: six complete-convolution cases, six guarded application runs,
unchanged CASA checks/panels and focused tests; numerical review remains open.

The later [complete Metal cube application checkpoint](cube-metal-application-checkpoint-20260929.md)
supersedes future-tense application proposals below. It preserves the FFT
discriminator results and rejected candidates; Metal spatial application timing
is now available and is not a win over CPU W4.

## Current: complete convolution discriminator

The formerly proposed test below has now run. Complete update time includes
component accumulation, forward transform, cached PSF multiplication, inverse,
crop, normalization, CPU-visible residual subtraction and workspace reset.
Each plane executes 32 updates of 128 components after two warmups; the PSF has
asymmetric sidelobes and the component list exercises edges and corners.
These are standalone operation timings, not application acceptance.

| Implementation | One plane, ms/update | Four concurrent planes, ms/batch |
| --- | ---: | ---: |
| Existing full-complex NEON FFTW | 13.675 | 23.564 |
| Hermitian half-spectrum NEON FFTW | 6.653 | 9.033 |
| Hermitian half-spectrum Metal (MPSGraph) | 4.255 | 12.663 |

The four-plane CPU number is actual dispatch-to-join batch wall time using
persistent numerical threads, not the previous raw-FFT maximum-lane proxy.
CPU and GPU both cache the PSF spectrum and reuse workspace. GPU execution,
shared-buffer input preparation, output visibility and CPU normalization are
included. Metal uses one batched graph, not one submission per CLEAN component.
Setup/first-use is separately logged; application timing will include setup.
All six complete-operation checks against full-complex FFTW have relative L2
below 5.38e-7 and peak-normalized error below 3.14e-7.

**Decision:** try the CPU half-spectrum implementation first. It halves the
two primary Clark buffers (36 -> 18.0234 MiB per 1536-square padded workspace)
and wins both concurrency cases. Metal wins this single-plane operation but
is about 40% slower than half-spectrum CPU for the four-plane batch. Do not
install Metal FFT as a blanket CPU replacement from this evidence. This does
not retire the complete Metal grid/degrid application milestone.

The production candidate uses one in-place real/half-spectrum component buffer
and one cached PSF half-spectrum, through the existing serialized FFTW planner
and reusable plan cache. No new dependency or FFT implementation. Batched
Clark refresh, padding, off-center origins, crop, normalization, CLEAN policy
and scientific thresholds are unchanged. Explicit planner buffer accounting
shrinks, but the existing conservative native-FFT allowance is retained.
Seven FFT tests and 297 reconstruction tests pass (19 gated tests ignored),
including odd/even rectangular transforms, concurrent plan reuse, asymmetric
off-center edge convolution and deep batched Clark behavior.

### Matched application checkpoint

Parent: `3957b41fac7aefa9c74661b8637a667b78e8e929`. All 4,094,064 rows,
output channels 240-271 (32), 1024-square, natural/Clark, 0.5 mJy, unchanged
mask and 640000-component limit. All runs converged in 17 major phases,
187665-187710 components and 2293-2319 Clark refreshes. Times include selected
input preparation through publication and intermediate I/O, excluding build,
process startup and comparison. Existing CASA reference reused; no new CASA run.

| Run / order | Application s | Read/imaging s | Minor s | Sampled peak RSS bytes |
| --- | ---: | ---: | ---: | ---: |
| W1 parent | 579.585 | 497.676 | 75.701 | 2244722688 |
| W1 half-spectrum | 551.357 | 493.083 | 52.318 | 2264612864 |
| W4 first pair: parent first | 241.359 | 202.332 | 33.280 | 2313060352 |
| W4 first pair: half-spectrum second | 265.187 | 231.970 | 25.548 | 2298675200 |
| W4 reversed pair: half-spectrum first | 264.898 | 232.718 | 24.103 | 2278227968 |
| W4 reversed pair: parent second | 335.879 | 276.383 | 50.835 | 2340749312 |

W1 improves 4.87% end to end in one observation; minor work falls 30.89%.
The smaller Clark buffers do not reduce the aggregate peak, which is elsewhere:
all peaks are about 2.09-2.18 GiB against the 16-GiB cap.

**W4 total benefit is unresolved.** The identical control moved 241.36 ->
335.88 s, while the candidate repeated at 265.19/264.90 s. Preserve the first
negative pair; do not advertise the favorable reversed pair, a pooled average
or a W4 scaling gain. Row bounds (180883), channel windows and wave counts
match. Minor-stage savings are consistent, but input/imaging variation
overwhelms a precise total-speed conclusion. pmset recorded no thermal or
performance warning; this is not proof of stable clocks. One process snapshot
during confirmation showed the benchmark near 381% CPU plus desktop/Codex
activity; causation is not established. No machine settings changed. Only one
reversed pair was added; no further timing campaign is running.

Retain the coherent CPU half-spectrum candidate locally for the measured W1
gain, smaller explicit buffers and faster complete convolution/minor stages.
No conclusive repeatable W4 regression is established either. This is not
full acceptance, a W4 speed claim, or authority to push/merge. Do not turn this
into local W8/core tuning.

### Scientific status and verification

All six exact outputs completed the unchanged full-array seven-product,
nine-check comparison and all-32-plane science assessment: zero hard failures.
Every restored-image plane is below 1e-3 (worst about 9.476e-4); inventory,
masks, finite topology, WCS/metadata, deterministic products, convergence and
beam checks remain intact. Raw component/residual comparison deviations remain
diagnostic outliers under the existing policy, not relaxed criteria.
All six science statuses are still `review_required`, not unconditional passes.

- W1: parent has 15 alert channels; candidate 16. Channel 257 residual-peak
  difference goes 0.496208 -> 0.504065 CASA RMS against the 0.5 review line.
  Its residual RMS and restored-image agreement improve.
- Initial W4: parent has 15 alert channels; candidate 16. Additional channel
  245 is **restored-component** NRMSE 0.000775067 -> 0.001009012 against 0.001,
  not a residual-peak alert.
- Reversed W4: parent and candidate have the same 16-channel alert set,
  including channel 257. The control also exhibits alert-boundary variability;
  that does not waive any exact output's alerts.
- Direct W1 candidate/parent restored-image NRMSE is 0.000284983; PSF, PB,
  mask and sumwt differences are zero. Nonlinear model/residual differences
  are separately retained.
- Inspected the restored-image comparison and all six alert panels. Similar
  field/beam-scale residual structure remains, without an apparent gross new
  positional or morphology defect. The listed alerts remain acceptance items.

Seven FFT and 297 reconstruction tests pass (19 gated tests ignored).
Both release builds use retained NEON FFTW (376 codelets each). Formatting,
diff checks and docs-check pass. The first reconstruction compile error
(test qualification) and final test-line formatting repair were repaired,
with earlier evidence preserved. No broad just-verify or new full-T55/full-512
gate ran for this bounded candidate. No full-T55 or Metal-application completion.

### Next action and durable restart

Finish the approved complete Metal grid/degrid application using shared science
preparation, bounded resident batches and repaired runtime fences, keeping CPU
FFTW/Clark and the existing writer. Input/imaging still occupies about 88-89%
of candidate wall time; it is not all I/O or FFT. Then qualify the connected
path with the approved dirty/shallow/deep turnaround and unchanged CASA checks.
No new FFT-library sweep, full-512 run, installation or local W8 tuning.
Visibility half-grids still require conjugate folding and correct real-adjoint
semantics; the Clark change does not truncate visibility grids.

All owned runs finished. Source remains uncommitted in three files; the research
note is untracked. No push/merge/cleanup. `half-spectrum-candidate-v1.patch`
matches the frozen binaries; `half-spectrum-candidate-final-v3.patch` adds only
documentation and test-line formatting. Earlier snapshots are preserved.
The durable bundle contains frozen binaries, exact commands, resource receipts,
all six application logs, science JSONs and alert PNGs. Reproduction:
`half_spectrum_application.py run|compare parent|candidate 1|4 [confirm-v2]`;
existing labels are exclusive, never overwrite or rerun unchanged labels.

Full comparison JSONs/panels are in corresponding
`half-spectrum-*-casa-comparison` directories under
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/bulk-imaging-plan-20260927/`.
Native planning/sampled aggregate RSS stay 16 GiB, other work 8 GiB, two
Cargo jobs and one FFTW thread per numerical worker.

Evidence under the durable bundle below:
- `clark_convolution_probe.mm`, `clark-convolution-build-v1-command.json`.
- `clark-convolution-{full,half,metal}-b{1,4}-v1.log` and command/resource JSONs.
- `half-spectrum-candidate-v1.patch`: exact source candidate.
- `half-spectrum-fft-tests-v1.log`, `half-spectrum-reconstruction-tests-v2.log`.
- `half_spectrum_application.py`: explicit matched build-freezing, application,
  comparison and panel stages; exclusive evidence paths, no automatic full run.
- The v1 reconstruction log preserves a repaired test-module qualification
  build failure, not a rejected numerical result.

## Earlier raw-FFT screen and reasoning (superseded where noted)

Metal FFT is a plausible targeted addition, not an established blanket win over
today's NEON FFTW. The current Clark convolution dimensions benefit for a single
plane, but the current visibility-imaging dimensions do not clearly benefit.
Four-plane results show that host copies can erase the benefit. Do not replace
every FFT, resurrect the old FFT stack, or infer an application speedup from this
screen. Preserve the complete Metal application milestone.

The next FFT-specific discriminator should be **one complete existing Clark
batched convolution**: cached PSF spectrum, component forward FFT, spectrum
multiply, inverse FFT, existing crop/normalization and CPU-visible residual.
Keep its intermediate arrays GPU-resident; do not dispatch once per CLEAN
component. Compare against the existing full CPU convolution, not naked FFTW,
including setup, native/library memory and all boundary copies. Only if that
wins should it enter the application candidate. This does not authorize changing
Clark's subcycle/major-cycle policy, precision requirements, or science checks.

For the connected application compare dirty (no CLEAN), shallow CLEAN and the
existing deep 0.5-mJy turnaround, with identical rows/channels/image dimensions
for CPU and Metal. Reuse the approved full-row 32-channel 1024-square workload;
no new full-512 run. Start CPU versus Metal grid/degrid with CPU FFTW, and add the
qualified FFT variant on the deep case to isolate its incremental contribution.
Do not require a full factorial sweep or fixed six-pair repetition campaign.
Record actual iterations, Clark refreshes and visibility major phases: equal
niter limits alone do not prove equal work. Use the unchanged deep CASA product
comparison and panels; the microcheck below is not scientific acceptance.

## Historical evidence: depth dependence is not established

Bounded history search found no retained matched shallow/deep Metal FFT pair.
The decisive July FFT comparisons have niter=0 and a **RustFFT**, not FFTW,
CPU baseline.

| Historical case | CPU total-wall median | Metal total-wall median | Qualification |
| --- | ---: | ---: | --- |
| ALMA mosaic MFS, 7 fields x 16 selected channels, 1280-square, f32 | 46.789 s | 38.664 s | Six observations each; high variance and mixed paired signs |
| Same large mosaic, MT-MFS nterms=2 | 130.133 s | 37.167 s | Six observations each; CPU maximum 374.497 s |

For the MFS case, the narrower frontend timer was 40.240 -> 27.864 s and the
PSF FFT timer 116.542 -> 27.152 ms. **Do not label frontend time as total wall.**
The large MT-MFS result is evidence of that old dirty-product route, not a
prediction for current single-term spectral cubes or deep CLEAN.

Checked-in artifacts:
- `tools/perf/imager/evidence/artifacts/20260710T152434Z-wave352-mfs-cpu-metal-counterbalanced.json`
- `tools/perf/imager/evidence/artifacts/20260710T154201Z-wave352-mtmfs-cpu-metal-counterbalanced.json`

At `fff9c2d553eace4b6a57b1df9ded4773f2263ceb^`,
`crates/casa-imaging/src/apple_fft.rs` used cached MPSGraph complex-f32 2D/batch
plans. The ordinary path packed, allocated, executed, exported, synchronized and
unpacked; its resident dirty-product route reduced boundaries and fused some
postprocessing. Reuse the lesson, not the removed implementation. Production
RustFFT was replaced with FFTW at `dab142dbec`. No old Metal-versus-current-FFTW
benchmark was found. Synthetic historical residual-kernel screens are unmatched
and cannot establish a deep-application win.

## Current code and scale

Inspected source `3957b41fac7aefa9c74661b8637a667b78e8e929`:
- `casa-fft/src/lib.rs`: cached measured, direct in-place rank-2 FFTW plans,
  single native thread per numerical worker, complex f32/f64.
- `casa-imaging-reconstruction/src/streaming_cube/band.rs`: model forward FFT
  and grid-to-image inverse FFT; current complex-f32 major-cycle grid is
  1250-square for the standard 1024-square image (1.2 padding plus composite rounding).
- `casa-imaging-reconstruction/src/minor_cycle/clark.rs`: batched linear
  convolution, cached PSF spectrum, two FFTs per refresh. A centered 1024-square
  PSF uses 1536-square alias-free padding. **Not one FFT per CLEAN component.**
- `spectral_operator.rs::PreparedFft` also owns centering; a Metal replacement
  must preserve axes, signs, normalization, padding and centering exactly in meaning.

Existing retained full W4 log sums: read/imaging stages 3516.71 s, minor-cycle
stages 570.381 s, total application 4124.730 s. All minor cycles are only 13.83%
of total: making the entire minor stage free would save at most that fraction
at unchanged convergence, and FFT acceleration saves only part of it.
The 16 minor summaries report 30400 Clark refreshes. Major-cycle FFTs are inside
the read/imaging stages; those stages are not all FFT time. The recent four-second
W8 profile samples early model/FFT setup and is not whole-run FFT attribution.

Full-run evidence:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t55/spectral-full-20260924/full512-band-local-20260929-v1/native-w4.log`.
No new full run or profile was launched.

## New standalone library measurements

Apple M4, macOS 27.0; linked retained `fftw-3.3.11-neon` static library.
Input/output are interleaved complex-f32, deterministic dense complex input.
Forward and inverse are **unnormalized**, matching the existing FFTW primitive.
All eight numerical checks passed: relative L2 error below 5.20e-7 and
maximum coefficient error/reference peak below 8.81e-7. This is transform
qualification only, not image/CASA acceptance.

Each cell below is median milliseconds for five short warm invocations. Cold
planning/first use is logged separately. Forward and inverse were measured
separately, not as a fused pair.

| Actual FFT extent | Planes | Direction | CPU FFTW | Metal resident buffers | Metal plus host input/output copies |
| --- | ---: | --- | ---: | ---: | ---: |
| 1250-square imaging grid | 1 | forward | 4.78 | 4.75 | 5.24 |
| 1250-square imaging grid | 1 | inverse | 5.08 | 4.86 | 7.90 |
| 1250-square imaging grid | 4 | forward | 6.39 | 14.72 | 18.78 |
| 1250-square imaging grid | 4 | inverse | 7.56 | 16.20 | 19.63 |
| 1536-square Clark padding | 1 | forward | 6.19 | 4.16 | 4.62 |
| 1536-square Clark padding | 1 | inverse | 6.47 | 3.26 | 3.85 |
| 1536-square Clark padding | 4 | forward | 11.98 | 9.60 | 16.48 |
| 1536-square Clark padding | 4 | inverse | 10.42 | 8.61 | 15.41 |

Boundary cautions:
- GPU numbers use a cached MPSGraph executable, preallocated shared MTLBuffers,
  caller-supplied output and synchronous CPU-visible completion. They are not
  submission-only or GPU-timestamp-only timings. Internal library work/copies
  remain included; shared memory is not a promise of zero traffic.
- One-plane CPU work executes directly. Four-plane CPU uses four persistent
  workers, one FFTW thread each, shared immutable plan and separate arrays.
  CPU column reports the maximum per-lane execution time, excluding sleeping
  worker startup skew; actual dispatch-to-join wall time is separately logged.
  This is a deliberately favorable active-CPU comparator, **not a measured W4
  application stage time**. Do not claim precise W4 speed ratios from this table.
- CPU input reset is outside timing; GPU host-copy variant includes two explicit
  full complex-buffer copies. The resident variant includes neither host copy.
  A fused convolution will have different, potentially smaller boundary traffic.
- This is a first discriminator with visible noise, not a statistical acceptance
  result. A repeat of 1536/batch4/inverse for OS memory measurement gave
  8.39 ms CPU, 8.22 ms resident GPU, 14.05 ms host-roundtrip GPU. This confirms
  that the small apparent resident batch4 gain is not established.
- No claim is made about 2048/4096 output images or different Apple GPUs.

Memory: in the largest case, explicit GPU input/output total 144 MiB;
Metal's allocation counter after execution reports 288.48 MiB (includes
additional library allocations, not a separately established scratch peak).
The OS-confirmation process had 382.06 MiB maximum RSS and 555.27 MiB peak memory
footprint, including CPU reference/test buffers. The sampled aggregate guard
stayed below 0.4 GiB against its 8-GiB cap; subsecond sampled peaks alone are
insufficient. GPU library memory must enter production admission if integrated.
CPU plan time was about 0.14–0.29 s; GPU first-use times varied with cached
driver compilation, so these are not pristine system-cold-cache benchmarks.

## Libraries and implementation options

[Apple MPSGraph FFT](https://developer.apple.com/documentation/metalperformanceshadersgraph/mpsgraph/fastfouriertransform(_:axes:descriptor:name:))
supports complex multidimensional forward/inverse transforms and explicit
scaling. Installed SDK headers expose wrapping MTLBuffer input/output and
cached executable execution. No installation or third-party dependency was
needed for this probe. MPSGraph's command-buffer encoding documentation warns
that it may commit-and-continue internally; integrate completion through actual
runtime fences, not an assumed untouched command buffer.

[VkFFT](https://github.com/DTolm/VkFFT) is another library-owned multidimensional,
batched Metal FFT option with caller buffer/command integration and MIT licensing.
It was researched, not installed or benchmarked. Do not expand this into a
library bake-off unless the next complete-operation test shows a specific
MPSGraph limitation worth resolving. No handwritten FFT implementation.

## Reproduction and retained limitations

Durable bundle:
`/Users/brianglendenning/SoftwareProjects/casa-rs-evidence/t57/cube-metal-first-path-20260929/`

- `fft_library_probe.mm`, `run_fft_probe.py`: isolated diagnostic, not repo runtime.
- `fft-library-probe-build-v3-command.json`: exact successful build and linked library.
- `fft-probe-{1250,1536}-b{1,4}-{forward,inverse}-v2.log`, command and resource JSONs:
  all eight cases. `python3 run_fft_probe.py` reproduces commands but uses exclusive
  log creation; choose fresh labels before rerunning, never overwrite old evidence.
- `fft-probe-1536-b4-inverse-peak-v1.log`: OS high-water confirmation.
- Initial build v1 hit existing Xcode/compiler vs macOS27 SDK mismatch; v2/v3
  used the installed macOS26.5 SDK. No installation or global SDK change.
- Initial 1250/b1/forward v1 included sleeping-worker wake-up time and is
  superseded by v2; keep it as harness history, not performance evidence.
- No production FFT/backend/scientific code changed, no application benchmark
  or CASA comparison ran, and no acceptance target was completed.

A bounded search was requested with GPT-6 Luna Max; root verified timing
boundaries and corrected an initial confusion between frontend and total wall.
Runtime/source interpretation and acceptance remain with the root reviewer.


## Follow-up: Hermitian half-planes — 2026-09-29

Yes, especially for the current Clark convolution. This is an exact real-signal
representation change, not reduced image size, dropped visibilities, approximate
FFT arithmetic or a change to CLEAN. A real image has conjugate-related Fourier
coefficients; retain N x (floor(N/2)+1) complex coefficients, including the
boundary column, rather than a full N x N complex plane.
[FFTW storage definition](https://www.fftw.org/fftw3_doc/Real_002ddata-DFT-Array-Format.html)
and [real multidimensional transforms](https://www.fftw.org/fftw3_doc/Multi_002dDimensional-DFTs-of-Real-Data.html).

Current `minor_cycle/clark.rs::LinearRefresh` constructs real PSF and real
component arrays but stores both as full complex-f32 planes. A cached
half-spectrum PSF plus one reusable in-place R2C/C2R component buffer would
reduce their primary storage at 1536-square from **36 MiB to 18.0234 MiB per
live Clark workspace**. This excludes unchanged image/residual/active-pixel
buffers and library plans/scratch, and assumes in-place buffer reuse rather
than adding separate real and complex arrays. It also nearly halves spectral
multiplications and spectrum traffic; real FFT arithmetic should decrease,
but elapsed speedup is unmeasured. Zero/Nyquist bins, row padding, off-center PSF
padding and normalization still require exact semantic handling.

This is supported by known-good science code, not only an Obit analogy:
CASA's local `casacore/lattices/LatticeMath/LatticeConvolver.tcc:185-240`
uses a reduced Fourier shape and real-to-complex transform for real convolution.
Obit's pinned
[UV-grid implementation](https://github.com/bill-cotton/Obit/blob/ebc1c229e5e3870b5ce3c342bddb7313d986a06f/ObitSystem/Obit/src/ObitUVGrid.c)
folds negative-u contributions to conjugate cells (lines474/641) and uses
half-plane complex-to-real FFTs (lines876-925). Its CPU fold/merge details
should not be copied mechanically.

The visibility-imaging opportunity is broader but not a drop-in truncation:
current `BandWorkspace::grid_sample` and `StandardConvolution::grid_float`
deposit into a full, not explicitly Hermitian grid, and `finish_images` takes
the real image component. A half-grid replacement must reproduce that real
adjoint through correct conjugate folding and normalization, and preserve
prediction, phase rotations, interpolation and convolution-boundary behavior.
Simply dropping negative frequencies is incorrect. For the current
1250-square grid, one full complex-f32 grid is 11.921 MiB versus 5.970 MiB for
the half-spectrum, excluding folding margins and other arrays. Smaller grids
could admit more bands and reduce MS traversals, but that secondary benefit
needs the real phase-admission calculation; it is not automatically 2x.
Do not generalize to arbitrary complex intermediate fields or all W/AW and
polarization modes without proving the particular operator's symmetry.

Both CPU FFTW and
[Apple MPSGraph](https://developer.apple.com/documentation/metalperformanceshadersgraph/mpsgraph/realtohermiteanfft(_:axes:descriptor:name:))
provide R2C/C2R operations. **Refine the next complete Clark convolution
comparison to include half-spectrum CPU FFTW versus half-spectrum Metal.**
The previous dense-complex FFT probe remains valid for its stated representation
but does not select the best implementation for real convolution. GPU plans and
intermediates still need bounded residency and actual memory accounting.
This follow-up is source-backed design advice, not a new implementation or
benchmark, and does not claim halving total application memory or runtime.
