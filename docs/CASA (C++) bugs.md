# CASA (C++) bugs

Notes on bugs or likely bugs observed while doing Rust/C++ parity work against CASA.

Known imaging parity defect: [CASA Högbom `niter` off-by-one](#casa-hogbom-niter-off-by-one-bug).
CASA behaviours casa-rs follows for parity:
[AW-projection hand pairing](#aw-projection-pairs-the-partner-hands-visibility-with-the-conjugate-baseline-cell),
[AW-projection prediction w](#aw-projection-predicts-with-the-unrotated-w),
[MatrixCleaner's stop iteration](#matrixcleaner-counts-the-iteration-that-stops-it),
[exact peak comparisons](#peak-searches-let-rounding-choose-between-equal-pixels).
CASA behaviours casa-rs does not follow:
[odd-size scale convolutions](#multiscale-and-multi-term-scale-convolutions-shift-by-a-pixel-on-odd-image-sizes),
[MatrixCleaner's step residual](#matrixcleaners-step-residual-forgets-earlier-steps).

## AW-projection pairs the partner hand's visibility with the conjugate-baseline cell

- Date noted: 2026-10-09
- Status: likely CASA bug; casa-rs follows it for parity (#667, PR #668)
- Affected code:
  - `casatools/src/code/synthesis/TransformMachines2/AWVisResampler.cc`
    (`getConvFunc_p`, `DataToGridImpl_p`, `GridToData`)

### Summary

For a row with `w ≤ 0`, `getConvFunc_p` selects the conjugate-baseline cell of
the outer correlation's Mueller element (`conjMNdx`, the partner hand:
`RR ↔ LL`, `RL ↔ LR`), and `DataToGridImpl_p` then grids the visibility of the
*selected cell's* hand (`visVecElement = muellerElement % nDataPol`), not the
outer correlation's. The RR grid receives `RR × conj(CF_RR)` for `w > 0` and
`LL × CF_LL` for `w ≤ 0`; `GridToData` likewise reads the partner's model
grid into the outer visibility. Stokes I of an unpolarized source is
unaffected (the two hands are equal); per-hand grids of data whose hands
differ, such as a circularly polarized source, mix the hands on the `w ≤ 0`
rows.

### Why this looks like a bug

The Mueller element of a real power pattern has a Hermitian transform, so
the RR kernel at `−|w|` is the conjugate of the kernel of the mirrored RR
beam at `+|w|`; for the EVLA squint (R and L beams offset in opposite
directions) the mirrored RR beam is the LL beam, which is why the partner
cell stands in for the conjugate. The visibility being convolved is still
the outer hand's. The index `muellerElement % nDataPol` returns the column
of the selected cell's own element instead of the column of its position in
the conjugate Mueller matrix.

### Effect

On `refim_mawproject` (CASA 6.7.6.14, Stokes I, `wbawp=True`,
`conjbeams=True`) the two pairings differ by 3.06e-3 normalised RMS in the
dirty image. casa-rs compiles CASA's pairing (`casa-imaging-operator`,
`aw::routing` into `MuellerRouting`) and agrees with CASA to 2e-7.

## AW-projection predicts with the unrotated w

- Date noted: 2026-10-09
- Status: CASA inconsistency between gridding and prediction; casa-rs follows it for parity (#667, PR #668)
- Affected code: `AWVisResampler.cc` (`GridToData` reads `vbs.vb_p->uvw()(2, irow)`;
  `DataToGridImpl_p` reads the rotated `vbs.uvw_p`)

### Summary

`AWProjectFT` hands the resampler the uvw rotated to the image phase centre
and the phase shift (`vbs.uvw_p`, `dphase_p`) together with the VisBuffer
(`vbs.vb_p`). `GridToData` selects its w-plane and tap conjugation from the
VisBuffer's unrotated w while placing the sample at the rotated u, v with the
rotated phasor; `DataToGridImpl_p` uses the rotated w for both. The two
differ only where the rotation moves a w across a plane boundary or through
zero. casa-rs carries the unrotated w per row (`RowContext::original_w_m`,
`ConvolutionFunctionSet::prediction_w`) and keys the AW prediction on it.

## `importvla` stale `VLACDA` cache crash on old VLA export data

- Date noted: 2026-04-15
- Status: likely CASA `importvla` bug
- Affected code:
  - `casatools/src/code/nrao/VLA/VLACDA.cc`
  - `casatools/src/code/nrao/VLA/VLABaselineRecord.cc`
- NRAO Archive file: `AG189_1_46325.23029_46325.80807.exp`
- Standalone note: [casa-importvla-vlacda-offset-zero.md](casa-importvla-vlacda-offset-zero.md)

### Summary

`importvla` can abort on old VLA export data when a CDA is valid in one logical record
and absent in the next. The immediate failure is the `offset != 0` assertion in
`VLABaselineRecord::attach()`.

### Likely mechanism

`VLALogicalRecord::read()` reattaches all four CDAs on every logical record. `VLACDA`
caches baseline-record objects, so if a CDA was populated on one record and then becomes
absent on the next, `VLACDA::attach()` can try to reattach cached baseline objects using
offset `0`, which triggers the assertion in `VLABaselineRecord`.

### Proposed fix

In `VLACDA::attach()`, if the new CDA is invalid (`itsOffset == 0`, or equivalently no
baseline data are present), clear the cached baseline objects and return before any
reattach path runs.

## `mstransform` channel-mode transformed-grid inconsistency

- Date noted: 2026-04-10
- Status: likely CASA `mstransform` bug or long-standing implementation quirk
- Affected code: `casatools/src/code/mstransform/MSTransform/MSTransformRegridder.cc`

### Summary

In the transformed `mode="channel"` / `regridQuant == "freq"` path, CASA appears to anchor
the output-grid start edge using `transCHAN_WIDTH[firstChan]`, while it spaces the uniform
output grid using `transCHAN_WIDTH[0]`.

That is inconsistent once the frame transformation causes per-channel widths to vary slightly
across the SPW. In the EVLA `refim_Cband.G37line.ms` repro case used during parity work,
this produced an output-axis offset of about `0.046 Hz`.

### Why this looks like a CASA bug, not a casacore bug

Direct Rust-vs-casacore measures conversion for the relevant `TOPO -> LSRK` path matched to
about `0.0015 Hz`, so the underlying frame conversion itself does not appear to be the source
of the discrepancy. The remaining offset is introduced later by CASA's transformed-grid
construction policy in `mstransform`.

### Notes

- This is scientifically tiny in the observed repro, but it is semantically inconsistent.
- The effect shows up in transformed channel-mode cubes where the transformed widths differ
  slightly across the SPW.

### Related references

- casacore PR #1464: [Fix missing frame bias in IAU2000 JNAT<->APP conversions](https://github.com/casacore/casacore/pull/1464)
- casacore issue #1465: [Unit mismatch in setMaximumCacheSize: bytes passed where MiB expected](https://github.com/casacore/casacore/issues/1465)

Those casacore links are not the same bug, but they came out of the same cross-checking and
parity work that surfaced this CASA-side issue.

## CASA Hogbom `niter` off-by-one bug

- Date noted: 2026-04-05
- Status: likely CASA bug / legacy interface mismatch
- Affected code:
  - `casatools/src/code/synthesis/ImagerObjects/SynthesisDeconvolver.cc`
  - `casatools/src/code/synthesis/ImagerObjects/SDAlgorithmHogbomClean.cc`
  - `casatools/casacore/scimath_f/hclean.f`
- Source note: `/Users/brianglendenning/Downloads/casa-hogbom-niter-findings (2).pdf`

### Summary

In the current CASA `tclean(..., deconvolver='hogbom')` path, `niter=1` appears able to commit
two clean components inside a single minor cycle while still reporting `iterdone = 1`.

The effect is not limited to the top-level `niter=1` case. Any CASA Hogbom
minor-cycle call that enters `hclean` with a positive `cycleNiter` may commit
one more component than the reported minor-cycle count. In a Cotton-Schwab run,
that means the model, residual, restored image, and controller stop/refresh
decisions can shift by up to one extra component per Hogbom minor-cycle block.

The concrete repro described in the attached note used:

- dataset: `.../casatestdata/measurementset/vla/sim_data_VLA_jet.ms`
- setup: `imsize=512`, `cell='12arcsec'`, `specmode='mfs'`, `weighting='natural'`,
  `gain=0.1`, `threshold='0Jy'`, `niter=1`
- observed result: CASA reported `iterdone = 1`, but the output `.model` image contained two
  nonzero clean components

### Likely mechanism

The note points to an off-by-one style caller/kernel mismatch:

1. `SDAlgorithmHogbomClean::takeOneStep` seeds `starting_iteration = 0`
2. the Fortran `hclean` kernel iterates over an inclusive `do iter = siter, niter`
3. the returned count is then clamped back down to `niter`

With `siter = 0` and `niter = 1`, that inclusive loop permits two update opportunities,
which matches the observed behavior.

### Repro detail from the attached note

The output `.model` image contained two nonzero pixels:

1. `(264, 331) = 0.6685306429862976`
2. `(265, 331) = 0.6019284129142761`

Their sum matched the reported `modelFlux`, which makes this look like an actual extra component
update rather than a display or reporting artifact.

A second trace on the ALMA TW Hydra tutorial data used `tclean(..., deconvolver='hogbom',
niter=1, cycleniter=1, weighting='briggs', robust=0.5, imsize=250, cell='0.1arcsec')`.
CASA reported one minor iteration, but the model flux was `0.0569704 Jy`; the dirty-image
peak was `0.299844 Jy/beam`, so one strict-gain component at `gain=0.1` would account for
only about `0.0299844 Jy`. The reported model flux is therefore consistent with two committed
Hogbom components in that minor-cycle call.

### Wave 3 heavy benchmark repro

The same behavior appeared again during the ImPerformance Wave 3 standard-MFS heavy
review on the retained Wave 2 medium VLA dataset:

- dataset: `/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms`
- setup: `specmode='mfs'`, `gridder='standard'`, `deconvolver='hogbom'`,
  `imsize=1024`, `cell='0.25arcsec'`, `spw='0:0~63'`, Briggs `robust=0.5`,
  `gain=0.1`, `threshold='0Jy'`, `niter=1`, `cycleniter=1`
- observed result: CASA reported `iterdone = 1`, but the output `.model` image
  contained two nonzero clean components:
  1. `(510, 510) = 9.177456855773926`
  2. `(508, 507) = 8.556220054626465`

In a strict one-component casa-rs comparison this made the one-cycle residual RMS
difference jump to `3.65e-2`. Running casa-rs with its explicit CASA-compatible
Hogbom accounting mode reproduced both components and reduced the residual RMS
difference to `7.64e-5`, matching the dirty gridding numerical floor. On the full
`niter=500`, `cycleniter=50` heavy row, the same CASA-compatible mode reduced the
CASA/casa-rs product differences to `.model` RMS `1.71e-7`, `.residual` RMS
`5.68e-5`, and `.image` RMS `5.87e-5`.

## MatrixCleaner counts the iteration that stops it

- Date noted: 2026-10-09
- Status: CASA accounting quirk; casa-rs follows it for parity (IF-5, #654)
- Affected code: `synthesis/MeasurementEquations/MatrixCleaner.cc`
  (`MatrixCleaner::clean`), read by `SDAlgorithmMSClean::takeOneStep`

`clean` increments `itsIteration` at the top of each pass and only then tests
the threshold (`abs(strength) < threshold()`) and the 50% divergence rule, so
a step that stops on either reports one more iteration
(`numberIterations()`) than it cleaned components. Multiscale `iterdone`,
the `niter` budget and the per-cycle `summaryminor` counts include it; on
`refim_point` with scales `[0, 6, 10]` CASA reports `[22, 3, 25, 3, 47]` for
`[21, 2, 24, 2, 51]` components when the extra iteration is not charged.
casa-rs charges it (`Solver::charges_stop`).

## Multiscale and multi-term scale convolutions shift by a pixel on odd image sizes

- Date noted: 2026-10-09
- Status: likely CASA bug; casa-rs does not follow it (IF-5, #654)
- Affected code: `synthesis/MeasurementEquations/MatrixCleaner.cc`
  (`makeScale`, `makePsfScales`, `makeDirtyScales`),
  `MultiTermMatrixCleaner.cc` (`computeRHS`, `computeHessianPeak`)

Scale functions are centred at `(nx/2, ny/2)` and convolved by FFT without
moving that centre to the origin. Single convolutions are re-centred with
`FFTServer::flip`, which rotates by `ceil(n/2)`; double convolutions
(PSF ⊛ scale ⊛ scale) are not flipped, relying on the two `n/2` shifts
cancelling. Both are exact only for even `n`: on an odd axis the scale-
convolved residual is one pixel off its residual and the PSF cross terms one
pixel off the PSF peak. casa-rs convolves with the scale centred at the
origin, which agrees with CASA on every even axis.

## MatrixCleaner's step residual forgets earlier steps

- Date noted: 2026-10-09
- Status: likely CASA bug; casa-rs does not follow it (IF-5, #654)
- Affected code: `synthesis/ImagerObjects/SDAlgorithmMSClean.cc`
  (`takeOneStep`), `SDAlgorithmBase.cc` (`deconvolve`)

With `cycleniter ≥ 5000` a plane's minor cycle runs in 2000-iteration
`takeOneStep` calls. Each call sets the residual to
`itsDirty − PSF ⊛ (model − prevModel)`, where `itsDirty` is the residual at
the plane's first step and `model − prevModel` only this step's components,
so from the second step on the reported peak residual omits the earlier
steps' subtraction. That peak drives the minor-cycle stop codes 2 and 4.
casa-rs carries each step's residual into the next.

## Peak searches let rounding choose between equal pixels

- Date noted: 2026-10-09
- Status: CASA behaviour casa-rs follows (exact comparisons); it makes
  parity rows on exactly symmetric skies rounding-dependent (IF-5, #654)
- Affected code: `casacore/scimath_f/hclean.f`, casacore `minMax` as read by
  `MatrixCleaner::findMaxAbsMask` and `MultiTermMatrixCleaner`
  (`chooseComponent`), Clark's `ABSMAXF`

Every CASA peak search compares exactly and keeps the first extreme in its
scan order; casa-rs does the same. On a point-symmetric sky such as
`refim_point`, mirror pixels are equal in exact arithmetic (CASA's dirty
and PSF images are exactly point-symmetric even in float32), and the scale
convolutions and subtractions of the minor cycle break the tie by an ulp or
two, so rounding, not the data, picks the component. On `refim_point`
CASA's rounding went both ways: `MatrixCleaner` took the first mirror in
scan order, and the multi-term cleaner the later one (one tie, at [50,47] /
[50,53], value 4.9e-5, in the last cycle). casa-rs's FFTW plans are chosen
by timing, so its `refim_point` multiscale row comes out at 5.4e-6 or
4.5e-2 NRMS from run to run; its MT-MFS row is 3.4e-4 when the tie goes
CASA's way and 3.1e-2 when it does not (as on `main`). Both outcomes are
equally valid cleans; such rows cannot measure parity, so
IF-5's multiscale and MT-MFS acceptance uses skies without the symmetry
(`refim_twopoints_twochan`, `refim_eptwochan`).
