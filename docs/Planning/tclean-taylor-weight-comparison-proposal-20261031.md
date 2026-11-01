# Proposal: compare weak Taylor weight moments on their matrix scale

Truth class: proposed owner decision; existing verdicts remain unchanged
Work issue: #980, following the approved #991 repair.

## Concrete decision

For two-term Stokes-I MFS with mosaic or AW projection, use the comparison
below for a weak `.weight.tt1` moment. Retain ordinary NRMS in the report.
All other product, mask, finite-value, topology, metadata, sumwt, beam and
scientific checks remain binding. This changes verification only; no CASA
parameter/default, native output, provider bundle or CASA installation changes.

Recommendation: approve this narrowly scoped replacement after the guards and
tests below. No current failed run has been reclassified.

## Why the current relative error is misleading here

The first implementation difference was real: CASA narrows a channel frequency
to Float before forming its Taylor variable. Native code now matches that in
both gridding and prediction (`7cd1ae440e`). The focused regression was red
before the correction and green afterward. CPU/f64 mosaic then passed every
product, including `.weight.tt1` at NRMS 2.55e-7.

Residual f32 and AW differences occur in an almost cancelling odd moment.
CASA publishes mixed normalization: only `.weight.tt0` is divided by
`.sumwt.tt0`; higher weight moments stay raw. This follows the documented
`SIImageStoreMultiTerm::divideWeightBySumWt` naming bug. Reconstruct the raw
moment matrix using `G0 = sumwt.tt0 * weight.tt0`, `G1 = weight.tt1`, and
`G2 = weight.tt2`. Using the normalized term zero directly would give the
wrong scale.

For nonnegative imaging weights the natural scale of the off-diagonal
moment is `sqrt(G0 * G2)`. Percent error relative to a nearly zero `G1`
amplifies ordinary rounding without measuring its size relative to the matrix.

## Proposed rule and limits

Eligibility depends on the frozen CASA reference alone: both the aggregate
`rho = sqrt(sum(G1²) / sum(G0*G2))` and its maximum pointwise value in CASA's
usable PB support must be at most `1e-3`. This restricts the replacement to
weak coupling and excludes nearly singular normalized two-term matrices in
that support. Other cases retain the existing numeric comparison rule.

For eligible references require both:

- `E2 = sqrt(sum((native_G1 - CASA_G1)²) / sum(CASA_G0 * CASA_G2)) <= 1e-6`
  over **all valid pixels**.
- `Emax = max(abs(native_G1 - CASA_G1) / sqrt(CASA_G0 * CASA_G2)) <= 1e-6`
  within CASA's PB support, defined by its mask and the recorded absolute
  `pblimit`. Support is independent of the candidate.

The limits are proposed engineering criteria, not universal rounding or
inverse-solver bounds. Independent model, residual, flux, spectral and other
scientific checks remain essential. A legitimate odd moment below the stated
matrix-scale precision budget could be entirely erased and satisfy this one
numeric criterion; this is the acceptance tradeoff being requested.

## Actual measurements

Pinned oracle: casatasks **6.7.6.14**, casatools **6.7.6-14**. Native scientific
revision: `7cd1ae440e331b3ef685221482debd864eb560d6`. References and products
are unchanged from the recorded comparison. All other product checks pass.

| Fixture/path | Raw weight.tt1 NRMS | E2 | Emax in PB support |
|---|---:|---:|---:|
| ALMA mosaic CPU/f64 | 2.55e-7 | 1.19e-12 | 7.79e-12 |
| ALMA mosaic CPU/f32 | 1.92e-3 | 8.95e-9 | 6.52e-8 |
| ALMA mosaic Metal/f32 | 8.84e-4 | 4.12e-9 | 4.20e-8 |
| Native AW CPU/f64 | 1.62e-3 | 2.50e-9 | 2.29e-8 |
| Native AW CPU/f32 | 2.22e-3 | 3.42e-9 | 3.47e-8 |

Reference maximum PB-supported coupling is 4.66e-6 for the mosaic and
2.28e-6 for AW, well within the proposed weak domain. Both reference and
candidate matrices have matching masks, positive diagonals, no zero scales
and no Cauchy violations on these fixtures. Deleting the original mosaic
odd moment would give E2=4.66e-6 and **fail** the proposed limit; the original
frequency-rounding bug is not waived.

The actual-product [comparison figure](/Volumes/GLENDENNING/casa-rs-evidence/tclean-20261031/taylor-frequency-casa/taylor-weight-comparison.png)
uses shared scales for CASA and native images and separately labelled raw and
matrix-scaled differences. Its structured differences are retained in the
measurement; they are not dismissed because the images look alike.

## Implemented measurement guards and review

The new `tools/perf/imager/perf_harness/taylor_moment_comparison.py` is
**unwired** and returns measurements, not acceptance. It requires scalar
sum weights greater than CASA's invertible-normalization threshold `1e-7`,
exact cross-moment/candidate/reference masks, finite nonnegative even
moments, and Cauchy/PSD validity for both matrices. Zero scale anywhere,
including outside PB support, requires both odd moments to be zero. It
rejects invalid support, empty meaningful domains, overflow and unrepresentable
reductions; it neither clips values nor intersects away mask differences.

It processes at most 65,536 cells per chunk and retains scalar state only.
Measured scratch peak was 5.90 MB excluding caller inputs. **72 helper tests**
pass, including cancellation, deletion of the original moment, a localized
error diluted in E2 but caught by Emax, invalid/zero/negative/nonfinite inputs,
weight rescaling, chunking and extreme arithmetic. These tests validate the
measurement implementation, not scientific equivalence by themselves.

Independent separate-agent review found the scale defensible with these
guards and the weak-domain restriction. The review's stable-ratio arithmetic
finding was repaired. Oracle Pro remains usage-limited; no external Pro review
was submitted or counted as passing.

Before activation, integration tests must cover eligibility at and immediately
above `1e-3`, and prove a guard failure cannot yield a passing comparison.
Representative T2 runs and all other unfinished tclean work remain required.

Owner approval is required by AGENTS.md's Ask First rule covering changes to
approved acceptance checks. Approval would authorize wiring this specific
replacement rule; it would not accept missing evidence or close tclean.
