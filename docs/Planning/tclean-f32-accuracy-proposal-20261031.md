# Bounded proposal: repair dense-cell f32 gridding accuracy

Truth class: approved implementation and retired experiments
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

The owner approved compensated-f32 adoption and its measured resource cost,
and separately the CASA-inclusive Hogbom default, on 2026-10-31. Both are
normal production paths at `8ee3922736`; the prototype feature and naive f32
alternatives are removed. CPU/f64 accumulation arithmetic is unchanged.
The original compact/moving f32 PSF failures (NRMS about 0.019/0.024) are
repaired. The two earlier candidates remain retired.

Normal adoption checks at `583cff5cc5` passed 4,672 workspace tests (44
skipped), doctests, workspace clippy, architecture/SPDX/format, 21 CLI hosts,
and 755 performance-tool tests with 307 subtests. Editable and wheel each
passed 632 tests (15 skipped); fresh-target sdist passed 99 after repairing
stale Cargo artifacts caused by reproducible archive timestamps. These are
executed checks, distinct from subsequent source changes.

Three follow-up corrections now have fresh pinned CASA 6.7.6.14 /
6.7.6-14 production-route evidence:

- Shared Taylor factors now follow CASA's Float channel-frequency narrowing
  (`7cd1ae440e`), in both gridding and prediction. The focused regression
  went red to green. Of five MT-MFS T1.5 runs, two pass all recorded checks;
  three still fail the unchanged `.weight.tt1` relative-error rule. Other
  products and checks pass. A guarded matrix-scale comparison is proposed,
  with 72 measurement tests; it remains unwired pending owner agreement.
- The legacy VLA PB selector now includes every named band and CASA's NVSS
  aperture in the intervening gaps (`2d91e001c3`). The ordinary-default
  2048-square W/Hogbom20 CPU/f64 and CPU/f32 comparisons pass all seven
  products and recorded metadata checks. PB NRMS is zero. Unsupported EVLA
  frequencies retain their real preflight refusal.
- MFS spectral-envelope conversion now uses the geocentric FIELD-origin
  moving direction, matching CASA MSUtil (`b6a87fa912`). Six fresh fixed
  ALMA512/moving Uranus256 runs, across CPU/f64, CPU/f32 and Metal/f32, pass
  products, exact inventory/masks/finite support, beam/sumwt and recorded
  unit/Stokes/WCS-value checks. Moving spectral increment now matches CASA
  exactly at 125020996.11132812 Hz; the prior 3.5 Hz failure is repaired at
  its unchanged tolerance. ALMA f32 PSF NRMS stays below 1e-6. Moving-field
  PB is still explicitly disabled and remains unfinished.

Independent review found no open actionable findings in the VLA PB and MFS
repairs or the guarded unwired Taylor measurement. Oracle Pro is still
usage-limited and has not supplied a review. The affected consolidated gate
at `ac41519a63` passes 4,676 workspace tests (45 skipped), doctests, workspace
clippy, architecture/SPDX/format, 21 CLI hosts and 827 performance-tool tests
with 307 subtests. Skips do not establish scientific acceptance.

The next representative standard-MT-MFS check uses the original #607 MS
content (90 files match its historical hash; 125 AppleDouble sidecars explain
the archive-tree difference), 512 square, four SPWs and 1,336,320 nominal
correlation-channel samples. All three dirty paths and CPU/f64/Metal/f32
CLEAN pass. CPU/f32 CLEAN fails `.model.tt1` NRMS at 0.0012759712. All
other recorded product checks pass. One instrumented retry confirms the same
components and a correct native linear solve for its supplied inputs; it
does not establish the entire upstream cause. The raw failure remains.

Independent review supports proposing an identity-bound exception: the ideal
published model intensity differs by at most 5.7469e-7 across its common-frame
MFS interval, despite the weak coefficient's larger relative error. Both this
model proposal and the separate weight-moment proposal await owner agreement.
No criterion or verdict has changed. The single diagnosis retry is spent;
another unchanged native run is not the next action.

Same-grid default restart is checkpointed at `0319f6d016`: native T1 verifies
model/binary-mask reuse and refusals, with admitted mask ownership and its
counting law. The review's spectral-mapping scope finding is fixed. The full
normal gate passes 4,683 tests (45 skipped), doctests, clippy, architecture,
SPDX/format, 21 CLI hosts and 827 performance-tool tests/307 subtests. Paired
CASA restart and broader persistence remain pending; priority returned to the
representative f32 decision before further restart work.

The [current receipt](../../resources/casa-compat/evidence/tclean-standard-mtmfs-t2-20261031.json)
and [previous wave receipt](../../resources/casa-compat/evidence/tclean-taylor-pb-mfs-20261031.json)
bind revisions, remaining failures, measured proposals and verified NAS archives.
The actual-product plots and raw verdicts are retained. Single wall times are
not matched performance evidence. After the owner decisions, finish affected
f32 acceptance and continue CASA-paired restart, numbering, stage reuse,
remaining representative modes, numeric returns and persistence. The task is
still unfinished.

## Approved decision: measured cost of the retained correction

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

The owner accepted this cost to meet the scientific accuracy target. The
corrected implementation is now the normal f32 path; the experimental gate
and naive alternative are removed. CPU/f64 and all CASA parameters/defaults
remain unchanged. Independent review found no adoption code defects. Normal
verification is recorded separately from the prototype below. Full
representative acceptance and publication remain required; tclean is still
incomplete.

The complete scoped receipt is
`resources/casa-compat/evidence/tclean-compensated-f32-experiment-20261031.json`.
Fresh broader probes also record pre-existing default Hogbom counting, PB
coverage and MT-MFS weight-product gaps shared by the f64 control. They
remain tclean work; none is silently accepted as a deviation. The Hogbom
decision is separate in `tclean-hogbom-default-proposal-20261031.md`.

The next MT-MFS diagnostic identifies an actual first divergence, without
changing native code: CASA `MultiTermFTNew::modifyVisWeights` narrows channel
frequencies to Float before forming the Taylor variable; native narrows
only after forming it. On the selected LSRK ALMA channels, both output WCS
reference frequencies are exactly 341400000000 Hz. The native expression
predicts sumwt.tt1=0, while CASA's narrowing predicts 0.008119745566 before
publication, which rounds exactly to the observed 0.008119745180 Float.
This supports a bounded follow-up to match the upstream factor and then
remeasure the spatial weight products. It is not yet a weight.tt1 repair or
passing scientific evidence; no new tolerance or exception is accepted.
The W PB refusal is likewise traceable to missing native coverage of CASA's
VLA_NVSS branch below its open 1 GHz L-band interval, rather than a reason
to treat the default request as unsupported by CASA.

These new products, raw receipts and exact recipes are in the separately
reread-verified NAS supplement
`compensated-f32-fixed512-20261031.tar`, SHA-256
`c641b97ba5145db890f1561a562e58c9bac61c7ff97b6cff8ce1b5d7c0e9bc8c`.
The previously verified archive retains the exact tested prototype source.
Both owner decisions are approved and their code changes are applied.
Normal paired comparisons pass all products, exact inventories/masks,
finite values, beams and sumwt on fixed ALMA512, moving Uranus256 (PB
opt-out) and ordinary-default Hogbom10 on CPU64/CPU32/GPU32. Fixed and point
WCS-value checks pass; moving MFS spectral increment is125020999.61227417Hz
versus CASA125020996.11132812Hz on every precision. That remains a failed
metadata check, separately tracked from corrected accumulation. The normal
receipt is `resources/casa-compat/evidence/tclean-adopted-defaults-20261031.json`;
its NAS archive `adopted-tclean-defaults-20261031.tar` was reread-verified
with SHA-2569e55d997b060f172f20b1c59c3ef1a1e6c59492b79707868adf5cd6ccf031450.
The next code slice matches CASA's Taylor-frequency narrowing, then rechecks
the spatial MT-MFS weights without relaxing their comparison.

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
