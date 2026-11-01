# Bounded proposal: repair dense-cell f32 gridding accuracy

Truth class: current experiment and retired experiments
Work issue: #991, under the tclean-first work in #980.

Owner approval, 2026-10-31: the bounded prototype is approved. The owner also
authorized future local experiments with new performance algorithms and
ideas without renewed permission, while preserving fundamental correctness.
The shared operating rule is recorded in `AGENTS.md` (also imported by Claude).

## Outcome: retired for cost

The exact prototype is preserved at
`fe9303b46be3bced29d8ebd8d8de961a0547dc6d`, enabled by its experimental
Cargo feature. Both small cases met their recorded NRMS, masks, finite-value,
beam and sumwt checks. Compact-array PSF NRMS improved from 0.0188361 to
4.4328e-6. However, its major cycle took 8.90285 s against the same-source
control's 0.307545 s (about 29 times the cost); task-process wall was 9.4267 s
against 0.8571 s. These are single paired observations, not statistical
timings. The ordinary small case's major cycle also increased, from 0.00490
to 0.00680 s, though task startup noise hid it in total wall time.

The full-grid conversion/clear and synchronization after every 256 samples
are too costly. The candidate is retired, its source changes removed, and
the normal executable restored. No memory counting-law pass, representative
acceptance or production adoption is claimed. The f64 master was per apply,
so this also did not establish a full-pass error bound.

The compact receipt is
`resources/casa-compat/evidence/tclean-partial-grid-experiment-20261031.json`.
## Second outcome: contribution grouping retired

The next bounded candidate summed bit-identical located spread records in
native host f64 before the existing Metal f32 kernel. It is preserved at
`40bddd122825d758f194e48add6f0ce3540daa73`. It added no grid and charged
6,357,248 bytes of bounded grouping scratch per backend. Six regression
tests covered routing keys, cancellation, masked NaNs, signed weights and
overflow. Actual host-plus-device allocation laws passed for candidate
and control.

The compact candidate's PSF NRMS was 1.8786e-6 against the control's
approximately 0.01884, with its recorded product, mask, finite, sumwt and
beam checks passing. On ordinary `refim_point.ms`, both candidate and
control passed the recorded image NRMS bound but failed the beam checks;
the candidate did not establish scientific acceptance. Broader native
tests ran 109 cases: 103 passed and the same six Metal route-comparison
laws failed their unchanged 1e-6 bound.

The first timing window overlapped compilation and other tests and is
explicitly excluded from performance conclusions. A separate quiet
ABBAABBA window used the same-source control and candidate, Metal/f32,
one worker, niter=0, with four observations of each. No matched CASA
timing was measured. Median seconds were:

| Workload | Control task wall | Grouped task wall | Control major cycle | Grouped major cycle |
|---|---:|---:|---:|---:|
| Compact ALMA, 64 square | 0.2831 | 0.2746 | 0.1880 | 0.1707 |
| Ordinary VLA, 100 square | 0.3384 | 0.6123 | 0.2226 | 0.4861 |

The ordinary task was 81% slower, with its major cycle 2.18 times the
control's cost. The candidate is retired, including its experimental
feature, hash storage, implementation and counters. Do not retry this
hash-based grouping candidate or treat its dense-case improvement as a
production result. The full compact receipt and raw quiet observations
are `resources/casa-compat/evidence/tclean-grouped-spread-experiment-20261031.json`;
recipes, logs and products are archived on the NAS.

The allocation measurement separately exposed a previously uncharged
`Vec<Slot>` allocation. Its exact `RING * size_of::<Slot>()` charge and
the counting law remain, reviewed independently with no findings. This
changes admission accounting only, not the scientific computation.

## Current state and next action

The first two candidates remain retired. A third native correction is now
implemented at `75155cd47f77cbfc4c1e1d6822774b7d1516d1ee`, behind
`casa-imaging-metal/compensated-f32-prototype`, which also enables its CPU
counterpart. The feature is off by default; the ordinary release executable
has been restored. No acceptance tolerance has been widened.

The GPU records each atomic add's rounding loss using TwoSum, into two
additional f32 grids. A GPU renormalization keeps its own remainder across
applies. A regression exposed the previous fold's 1.48e-6 dependence on
batch boundaries; retaining the remainder fixes it. CPU/f32 uses one lazy
Complex32 correction per cell and statically dispatched normalized TwoSum
in the existing tap loops. Reset and region merging preserve their state.
CPU/f64 arithmetic and model storage stay unchanged. No CASA computation
is used at runtime.

Against casatasks 6.7.6.14 / casatools 6.7.6-14, PSF NRMS is:

| Case | Naive GPU/f32 | Corrected GPU/f32 | Naive CPU/f32 | Corrected CPU/f32 |
|---|---:|---:|---:|---:|
| Compact fixed ALMA, 64 square | 0.01884 | 7.42e-7 | 0.01919 | 5.30e-7 |
| Moving Uranus, 256 square | 0.02366 | 8.39e-7 | 0.02306 | 9.05e-7 |
| Ordinary VLA, 100 square | 9.76e-4 | 2.52e-6 | 9.49e-4 | 2.52e-6 |

All recorded products, masks, finite support, beam and sumwt checks pass in
these corrected cases. Moving-Uranus uses an explicit native PB opt-out,
isolating the existing moving-PB gap. Ordinary VLA's previous beam miss is
fixed. W-projection CPU/f32 and mosaic CPU/GPU/f32 also pass the fresh CASA
product checks with the existing explicit CASA-inclusive Hogbom option;
the W case additionally opts out of its separately refused PB frequency.
Those are component checks, not ordinary-default task completion.

All 4,671 workspace tests pass with the experimental feature (44 skipped),
as do doctests, workspace clippy, architecture/SPDX/format checks and CLI
conformance for 21 binaries. The former six Metal route failures pass at
their unchanged 1e-6 bound. Independent review findings about failure
propagation, destruction, interrupted reset and feature-matrix accounting
were repaired. Actual heap/device allocation laws pass; a deliberately
broken reset fails the fresh-signal law. Python packaging after default
adoption and representative T2 acceptance are not claimed.

## Decision requested: measured cost of the retained correction

Quiet same-source W1 ABBAABBA measurements, four observations per build,
include process startup and device initialization with a warm filesystem:

| Case and path | Control median wall, seconds | Corrected median wall, seconds |
|---|---:|---:|
| Compact ALMA, GPU/f32 | 0.2986 | 0.3938 |
| Ordinary VLA, GPU/f32 | 0.3449 | 0.3562 |
| Compact ALMA, CPU/f32 | 0.3616 | 0.7478 |
| Ordinary VLA, CPU/f32 | 0.4980 | 1.0634 |
| Compact ALMA, CPU/f64 | 0.3825 | 0.3857 |
| Ordinary VLA, CPU/f64 | 0.5770 | 0.5774 |

GPU wall cost increases about 32% and 3%; CPU/f32 is about 2.1 times its
control. CPU/f64 is effectively unchanged in these small observations.
User/system CPU and major-cycle times are retained in the receipt; hardware
cycles were not collected. Both binaries link the same Homebrew FFTW
3.3.11 static libraries, with no NEON codelets. These are local comparisons,
not a matched CASA timing or a representative performance claim.

Accumulation storage becomes 24G bytes on the GPU and 16G on CPU/f32,
versus 8G before, for G complex cells. These are accumulator multipliers,
not whole-run RAM multipliers; f32 FFT/model storage is unchanged. Admission
charges the actual correction owners, including host merge destinations,
and may admit fewer planes or owners. Existing memory refusals remain.

Recommendation: accept this cost to meet the scientific accuracy target,
make the corrected implementation the normal f32 path and remove the
experimental gate/naive path. Keep CPU/f64 unchanged. No CASA parameter or
default changes. This adoption needs the owner's decision under AGENTS.md;
it has not been applied. Further representative/packaging/review gates
remain required before release.

The complete scoped receipt is
`resources/casa-compat/evidence/tclean-compensated-f32-experiment-20261031.json`.
Fresh broader probes also record pre-existing default Hogbom counting, PB
coverage and MT-MFS weight-product gaps shared by the f64 control. They
remain tclean work; none is silently accepted as a deviation. The Hogbom
decision is separate in `tclean-hogbom-default-proposal-20261031.md`.

Oracle Pro was rechecked and remained unavailable due the signed-in
account's usage limit. No prompt or substitute review was submitted.

## Evidence and change boundary

At `5ad410a88eb9957e2a0de177218e766d89095d3a`, against casatasks
6.7.6.14 / casatools 6.7.6-14, the 64-square fixed-field MFS case on
`alma_ephemobj_icrs.ms` gives PSF NRMS:

| Native configuration | PSF NRMS |
|---|---:|
| CPU f64 | 8.3539e-8 |
| CPU f32 | 0.0191865 |
| auto, resolved Metal/f32, first run | 0.0188376 |
| auto, resolved Metal/f32, second run | 0.0188382 |

These runs explicitly disabled native PB production to isolate gridding.
The independent ALMA/ACA PB preparation bug has since been repaired and its
ordinary-default six-product CPU/f64 comparison passes separately. The two
auto runs differ from each other by PSF NRMS 1.6331e-6 and residual NRMS
2.0758e-5. Random atomic order does not explain away the 1.9% CASA error.
The compact receipt is `resources/casa-compat/evidence/tclean-accuracy-991-20261031.json`.
Inputs remain durably on the NAS, with a temporary SSD working copy.

The executing code is `MetalBackend::run` in
`crates/casa-imaging-metal/src/backend.rs`: sub-block dispatches currently
add into one f32 grid over the whole pass. Host `sumwt` already uses f64;
that does not protect individual grid cells. CPU f32 reproduces the error.

## Original approved local experiment (retired)

Prototype one bounded partial-sum mechanism, initially for standard MFS
dirty PSF/data on one worker:

1. Reuse the native operator, tap selection and existing f32 kernels.
2. Bound the contributions to each temporary f32 partial grid. Initially
   use deliberately small dispatches to discriminate accumulation error;
   choose any retained bound from a rounding-error argument, not this
   dataset's identity or a fitted threshold.
3. Merge completed partial grids into a Rust f64 master accumulator and
   clear/reuse the partial storage. Convert once at the existing final
   storage/FFT boundary where appropriate. Keep sumwt's existing semantics.
4. Implement this at the shared accumulation seam. Do not add an independent
   imaging route, CASA runtime delegation, a hidden backend fallback, or a
   permanent mode-specific imager.

This is permission for a local experiment, not for an unreviewed release.
No CASA parameters/defaults change. Do not add a public tuning parameter or
change a provider bundle in this experiment. Precision reporting must
describe the actual mixed arithmetic in its evidence.

## Original experiment's memory and acceptance plan

For G complex grid cells, a retained f32 partial plus f64 master costs
approximately 24G bytes, versus the current 8G bytes, before existing
sumwt/plan/FFT storage. Admit and charge the actual owners; add a counting
law for any newly retained storage. Do not bypass memory refusal, increase
the run's memory policy, or put artifacts on the internal disk.

Repeated full-grid merges can be expensive for large images. Measure their
actual work and end-to-end cost, including GPU synchronization and clearing;
do not infer a speedup from fewer rounding errors or omit host work. Keep
the existing resource and performance requirements. Report costs and do not
promote an unsupported tradeoff.

First discriminate accuracy on the frozen 64-square case and an existing
small ordinary MFS case, using serial CPU controls. Test input flags, native
sample order, exact masks, sumwt, finite values and all affected products.
Then use the relevant T1/T1.5 rows before considering promotion. One
universal tolerance is not a replacement for the existing product gates.
W/AW, mosaics, cubes and MT-MFS are not silently declared complete from this
prototype; any retained shared change needs their affected gates.

Keep one candidate and the hypothesis retry limit. A failed experiment is
recorded and reassessed; it does not start an unbounded tuning campaign.
No unattended overnight run is included. Independent review and the
remaining tclean acceptance work remain required.

## Original approval and unavailable review

The owner approved this experiment and removed the prior permission gate for
future local performance experiments on 2026-10-31. Production adoption still
follows `AGENTS.md`. On 2026-10-31,
the signed-in Brian Glendenning Pro account's model picker reported a usage
limit and locked higher power. The Oracle Pro design review was therefore
not submitted; no other model was silently substituted. This is an access
gap, not approval or a review pass.
