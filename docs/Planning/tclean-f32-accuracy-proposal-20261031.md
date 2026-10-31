# Bounded proposal: repair dense-cell f32 gridding accuracy

Truth class: retired experiment
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
The next investigation follows the existing weight-image contribution
reducer: sum identical gridding contributions before stamping them, reducing
both repeated work and f32 additions. This is a new bounded hypothesis,
not tuning or retaining the rejected whole-grid merge.

The current small compact-array case is outside scientific acceptance.
CASA is an oracle only; the proposed computation stays native Rust.

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

## Approved local experiment

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

## Memory, performance and acceptance

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

## Approval and unavailable review

The owner approved this experiment and removed the prior permission gate for
future local performance experiments on 2026-10-31. Production adoption still
follows `AGENTS.md`. On 2026-10-31,
the signed-in Brian Glendenning Pro account's model picker reported a usage
limit and locked higher power. The Oracle Pro design review was therefore
not submitted; no other model was silently substituted. This is an access
gap, not approval or a review pass.
