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

The owner approved the scoped weak Taylor-weight comparison rule and the
specific reviewed CPU/f32 MT-MFS model output on 2026-11-01. The decisions
are separate and recorded in
[`tclean-owner-decisions-20261101.json`](../../resources/casa-compat/evidence/tclean-owner-decisions-20261101.json).
The original model.tt1 NRMS failure remains in its raw receipt. Only the
unchanged b6a87 `clean8/cpu32` output is accepted by identity-bound owner
review; future outputs have no automatic waiver.

The approved matrix-scale gate is wired at `93b2ef256f`, with 120 focused
tests and 875 performance-tool tests/307 subtests passing. Fresh native
mosaic/AW runs at `4eed0a990e` pass all five scoped numeric comparisons and
retained recorded checks; three use the approved weight.tt1 override.
Worst matrix-scaled error is 6.5195e-8, below the 1e-6 limits. Full metadata
is a separate gate. The original f32 accumulation fix and its approved
resource costs remain the normal path; f64 accumulation arithmetic is
unchanged. Earlier failed candidates remain retired.

The fresh comparisons exposed a separate MFS header defect: CASA uses a
SOURCE rest frequency where native used the band midpoint. `4eed0a990e`
repairs the ordinary default, using the first selected SPW and CASA-VI
ordered field without a second MAIN scan. All five fresh weight families
now have matching rest-frequency metadata. DOPPLER-backed lookup and
multiple SOURCE key groups containing stored wildcards remain explicitly
unfinished, rather than silently guessed. Its normal full gate passes
4,692 workspace tests/45 skips, doctests, clippy, architecture/SPDX/fmt,
21 CLI hosts and 875 performance-tool tests/307 subtests.

Default restart now has pinned CASA 6.7.6.14 / 6.7.6-14 evidence on two small
standard Stokes-I MFS fixtures, with pblimit=0.2 and -0.1. All 18 native
10->0->10 sequence rows and six persisted-mask runs pass recorded products,
masks, finite support, beam/sumwt and WCS/restfreq checks. Zero-iteration
reruns retain the model exactly; the nontrivial mask prevents components
outside its support. CASA continues and republishes native products.
`ed9f6d1b3c` additionally recognizes equivalent SIN parameters and resolved
celestial rotations, so all six CASA-to-native continuation rows pass on
CPU/f64, CPU/f32 and Metal/f32. Coordinate serialization/math is unchanged;
real grid differences still refuse. Focused tests and independent review
pass; its full normal gate passes 4,695 tests/45 skips plus the normal checks. This is a bounded restart result,
not general persistence or whole-task equivalence.

The [current receipt](../../resources/casa-compat/evidence/tclean-approved-restart-20261101.json)
binds raw receipts, revisions and the verified NAS archive (112,691,200
bytes, SHA256 59f0ef88a3809bb845e411fbb45d6f825f108570aa32ad0f10b133f90dc3272d).
The earlier representative-model receipt and its raw failure remain intact.
Oracle Pro was usage-limited, not a review pass; separate-agent reviews
found no remaining actionable findings within these bounded changes.

The next scalar slice is implemented at `fd7b8e7b2d`: final maxpsfsidelobe
and cyclethreshold now report statistics already measured by the native
controller. No solver arithmetic, array rereads, persisted report formats,
CASA signatures or provider defaults change. Correctly shaped all-zero image
masks now use the existing zero-mask stop path instead of failing.

[Fresh pinned evidence](../../resources/casa-compat/evidence/tclean-scalars-empty-mask-20261101.json)
passes 18 small lifecycle cases across CPU/f64, CPU/f32 and Metal/f32: ordinary
iterations, global-threshold stop, immediate threshold stop, empty mask,
seeded restart and nonzero-model restart under a persisted empty mask. The
two scalar keys pass their recorded limits; iteration/major/stop integers
match exactly and all recorded scientific products pass. Shared-scale plots
are inspected. Focused laws/T1 tests, clippy and independent review pass;
the full normal gate passes 4,698 tests/45 skips, doctests, workspace clippy,
architecture/SPDX/fmt, 21 CLI hosts and 875 performance-tool tests/307 subtests.
Verified NAS archive is 31,488,000 bytes,
SHA256 73ed966f5947bad37e78ea842f6125a68dcd35768d91c9b51c9114433427aa6b.

Owner-approved contract32 activates `restart=False` at `99b1cfad55`, with
all114 binding names/order/defaults and other task surfaces unchanged. The
[activation receipt](../../resources/casa-compat/evidence/tclean-numbering-activation-20261101.json)
records20 small public CLI/Python product/naming/preservation rows passing;
19/20 selected scalar rows pass the original limits. The owner accepted the
one exact Metal cyclethreshold output (1.5132ppm difference); its raw failure
is retained and no future output inherits the waiver. Python package gates
pass632 editable,632 wheel and99 fresh-sdist tests. The combined normal gate now passes4710 workspace tests/45 skips, doctests,
workspace clippy, architecture/SPDX/fmt,21 CLI hosts and875 performance-tool
tests/307 subtests. Initial stale profile fixtures are repaired; coordinated outliers, occupied chosen orphan namespaces, calc
controls, broader returns/modes and representative acceptance remain unfinished.
CASA appends later calls to old numbered images' histories; native leaves old
files unchanged. The owner separately agreed to preserving prior histories; correct history
for new images remains required.
Singleton-list startmodel is a measured existing input-domain gap.

The signed/clipped pixel-box and explicit-empty-CRTF correction is committed
at`086a67f1d4`.All36 CLI CASA pairs and2 public Python pairs pass their scoped
products/counts; Python additionally gates the two selected scalar returns and
old-file preservation under False numbering. Focused red/green tests, all61
imagerT1 tests, mask memory law, touched clippy/dependency checks and independent
review pass. The combined normal gate passes all4710 workspace tests/45 skips and the
remaining normal checks above. The
[receipt](../../resources/casa-compat/evidence/tclean-mask-boxes-20261101.json)
keeps general/fractional/world regions and broader mask workflows unfinished.

The next bounded slice is`nmajor=0`: six pinned oracle cases and the native
baseline show four early-refused clean cases; dirty0 and a one-major control
already pass products/counts. CASA's zero limit publishes the initial residual
with no minor iterations/code9, while global threshold and zero-mask stops
take precedence. The existing controller supports this; wrapper/request/model
validation guards currently block it. An independently reviewed minimal patch/regression set is in red/green
verification without changing provider/defaults/signatures or solver arithmetic.
`tclean` is not complete.

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
