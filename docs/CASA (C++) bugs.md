# CASA (C++) bugs

Notes on bugs or likely bugs observed while doing Rust/C++ parity work against CASA.

Known imaging parity defect: [CASA Högbom `niter` off-by-one](#casa-hogbom-niter-off-by-one-bug).
CASA behaviours casa-rs follows for parity:
[AW-projection hand pairing](#aw-projection-pairs-the-partner-hands-visibility-with-the-conjugate-baseline-cell),
[AW-projection prediction w](#aw-projection-predicts-with-the-unrotated-w),
[MatrixCleaner's stop iteration](#matrixcleaner-counts-the-iteration-that-stops-it),
[exact peak comparisons](#peak-searches-let-rounding-choose-between-equal-pixels),
[multi-term weight normalization](#the-multi-term-weight-normalization-override-is-never-called),
[the Airy beam's blockage term](#the-airy-beams-blockage-term-uses-the-inverse-length-ratio),
[the EVLA's 11052 MHz fit](#the-evlas-beam-table-has-no-11052-mhz-fit).
CASA behaviours casa-rs does not follow:
[odd-size scale convolutions](#multiscale-and-multi-term-scale-convolutions-shift-by-a-pixel-on-odd-image-sizes),
[MatrixCleaner's step residual](#matrixcleaners-step-residual-forgets-earlier-steps),
[an unplaceable PB pointing](#the-analytic-pb-ignores-a-failed-topixel),
[the cube density's two uvw frames](#the-cube-briggs-weightor-grids-and-looks-up-its-density-in-different-uvw-frames),
[single-precision Taylor weights](#multitermftnew-forms-taylor-imaging-weights-in-single-precision),
[facets of an image size that does not divide](#facets-of-an-image-size-that-does-not-divide).
Simulator behaviours casa-rs does not follow:
[TIME as TAI](#the-simulator-stores-tai-as-utc),
[shadowing the front antenna](#the-simulator-charges-shadowing-to-the-antenna-in-front),
[relabelled spectral-window frames](#setspwindow-relabels-every-spectral-windows-frame),
[unconvertible coordinate systems](#setconfig-accepts-coordinate-systems-it-cannot-convert),
[quantities in the wrong units](#the-tools-convert-quantities-whose-units-do-not-conform),
[calls before `setconfig`](#simulator-calls-before-setconfig-abort-the-process),
[a zero integration time](#a-zero-integration-time-adds-rows-without-bound),
[`readantenna`'s observatory fallback](#simutilreadantenna-averages-a-stale-loop-variable),
[`readantenna`'s names after blank lines](#simutilreadantenna-names-antennas-after-blank-lines),
[the measures tool's default frame](#the-measures-tool-converts-with-a-frame-at-mjd-0-and-the-geocentre),
[the model `predict` rewrites](#smpredict-rewrites-the-model-image),
[models the simulator cannot read](#smpredict-accepts-models-the-simulator-cannot-read),
[`setmosaicfield`'s off-centre grid](#setmosaicfield-sits-its-grid-off-the-field-centre),
[`observemany`'s rows before an unknown field](#observemany-adds-rows-before-it-looks-up-later-fields),
[W projection without planes](#smpredict-through-wprojectft-without-planes-crashes-the-simulator).
Measures behaviours casa-rs follows for parity:
[IAU 1980 sidereal time and obliquities under IAU 2000](#iau-2000-keeps-the-iau-1980-sidereal-time-and-ecliptic-obliquities).
Measures behaviours casa-rs does not follow:
[latitudes of unnormalized directions](#direction-latitudes-come-from-an-unnormalized-vector),
[IAU 2000 JMEAN to J2000](#iau-2000-jmean-to-j2000-applies-bias-and-precession-in-the-wrong-order),
[IAU 2000 GMST1](#iau-2000-gmst1-adds-the-tt-day-fraction).
Simulator behaviours casa-rs follows:
[`observemany`'s padded single name](#observemany-pads-one-name-with-empty-ones).
Component-list behaviours casa-rs does not follow:
[a limb-darkened disk without an exponent](#writing-a-limb-darkened-disk-without-an-exponent-aborts-casa).

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

## The multi-term weight normalization override is never called

- Date noted: 2026-10-11
- Status: likely CASA bug; casa-rs follows it for parity (#704, PR #707)
- Affected code: `synthesis/ImagerObjects/SIImageStore.h`,
  `SIImageStoreMultiTerm.h`, `SynthesisNormalizer.cc`
  (`divideWeightBySumWt`)

`SIImageStore` declares `virtual void divideWeightBySumwt()` (lower-case
`wt`). `SIImageStoreMultiTerm` declares `void divideWeightBySumWt()` (upper
case `W`), a different name, so it overrides nothing; its loop that divides
every Taylor weight image by the principal sum of weights is never called.
`SynthesisNormalizer::divideWeightBySumWt` calls
`itsImages->divideWeightBySumwt()`, which reaches the base body and divides
`weight()`, term 0, alone. `MultiTermFTNew::finalizeToSkyNew` writes each
term's raw `getWeightImage` moment, so `.weight.tt1` and `.weight.tt2` stay
the raw Σ W·s^N while `.weight.tt0` is normalized. CASA 6.7.6.14 on
`refim_alma_mosaic` (mosaic, `nterms=2`) agrees: `weight.ttN / sumwt.ttN` is
0.6913 for N = 0, 1 and 2. casa-rs publishes the same.

## The analytic PB ignores a failed `toPixel`

- Date noted: 2026-10-11
- Status: likely CASA bug; casa-rs does not follow it (#702, PR #707)
- Affected code: `synthesis/TransformMachines/PBMath1D.cc` (`apply`)

`PBMath1D::apply` converts the pointing to a pixel with the image's direction
coordinate (`toPixel`) and ignores the call's result, so a pointing the
projection cannot place, such as one 90° or more from the image centre under
SIN, is centred on whatever pixel the failed call left. casa-rs adds no beam
for a pointing 90° or more from an image domain's centre and refuses any
other projection failure.

## The cube Briggs weightor grids and looks up its density in different uvw frames

- Date noted: 2026-10-15
- Status: likely CASA bug; casa-rs does not follow it (#712 review by Fable
  and Astra)
- Affected code (CASA 6.7.5.18 and 6.7.6.14):
  - `casatools/src/code/synthesis/TransformMachines2/BriggsCubeWeightor.cc`
    (`weightUniform`, `getWeightUniform`)
  - `casatools/src/code/synthesis/TransformMachines2/GridFT.cc` (`put`)

`BriggsCubeWeightor`, which weighs cubes under uniform and Briggs weighting
(`perchanweightdensity`), fills its density grid through `GridFT::put`, which
rotates each row's uvw to the image phase centre (`rotateUVW`, `GridFT.cc:778`).
It then looks each row up, and tapers it, at `vb.uvw()`, the unrotated
MeasurementSet uvw (`BriggsCubeWeightor.cc:826, 860-861`). The grid and the
lookup use different frames. When the phase centre is the field centre the
rotation is the identity and nothing changes; for a cube with a phase centre
away from the field centre, or a multi-field cube, a row reads the density of
a different cell from the one its own sample was counted in, so its weight no
longer follows the local density. The MFS weightor (`VisImagingWeight`)
accumulates and looks up in one frame, the MeasurementSet uvw.

casa-rs bins and looks up the cube rule at the rotated uvw, one frame for both
(`the_cube_density_bins_and_looks_up_the_rotated_uvw`), and the standard (MFS)
rule at the MeasurementSet uvw, as `VisImagingWeight` does
(`the_standard_density_bins_the_measurement_set_uvw`). Cubes imaged at their
own field centre agree with CASA either way; off-centre and multi-field cubes
under a density weighting differ from CASA's weights by this mechanism.

## MultiTermFTNew forms Taylor imaging weights in single precision

- Date noted: 2026-10-11; metric approved 2026-10-15
- Status: CASA rounding; casa-rs does not follow it (#690)
- Affected code: `synthesis/TransformMachines2/MultiTermFTNew.cc`
  (`modifyVisWeights`, `modifyModelVis`)

`modifyVisWeights` holds each channel frequency and the Taylor variable
`(ν − ν₀) / ν₀` in `Float`, and multiplies the `Float` imaging weights by its
powers. The odd moments of `.sumwt` are sums that nearly cancel (two channels
at x ≈ ±1/3 leave Σw·x ≈ −0.05 of Σ|w·x| ≈ 8·10⁴), so this rounding shows as
a 1 % relative difference in `.sumwt.tt1` on `refim_twopoints_twochan`. An
emulation of both codes' arithmetic reproduces both published values to
every stored digit; casa-rs, which rounds the Taylor variable to `f32` once
and accumulates in `f64`, is 0.06 % from the exact sum, CASA 0.93 %. No image
reads the odd moments' sums.

casa-rs keeps its arithmetic. The T1.5 comparison judges an odd moment `k` by
|Δ| / √(tt_{k−1} · tt_{k+1}) and every other `.sumwt` by its relative
difference, both against 1e-6, and leaves `.sumwt` out of a case's image
NRMS. The narrow-band mosaic MT-MFS rows (three channels at 340 GHz) still
miss that bound at about 5e-6 and are listed exceptions until an emulation
confirms the same mechanism there and in the odd-moment weight images.

## Facets of an image size that does not divide

- Date noted: 2026-10-15
- Status: CASA behaviour casa-rs does not follow (#664)
- Affected code: `synthesis/ImagerObjects/SIImageStore.cc` (`makeSubImage`)

A facet of an `n`-pixel image split `N` ways is `n / N` pixels (integer
division) from `facet · (n / N)`, so when `N` does not divide `n` the top and
right remainder strips belong to no facet: CASA grids and cleans nothing
there, silently. casa-rs refuses such a layout (`NonDivisibleFacetLayout`).

## The Airy beam's blockage term uses the inverse length ratio

- Date noted: 2026-10-21
- Status: likely CASA bug; casa-rs follows it (#841, PR #843)
- Affected code: `synthesis/TransformMachines/PBMath1DAiry.cc` (`fillPBArray`)

For a dish of diameter `D` with a central blockage of diameter `b`, the
voltage pattern is proportional to `D²·jinc(x) − b²·jinc(x·b/D)`, where
`jinc(x) = 2J₁(x)/x` and `x = πDθ/λ`. `fillPBArray` tabulates
`(areaRatio·jinc(x) − jinc(x·lengthRatio)) / (areaRatio − 1)` with
`lengthRatio = D/b`, so it evaluates the blockage term at `x·D/b` instead of
`x·b/D`.

For ALMA's default pattern (10.7 m, 0.75 m blockage), the power in the main
lobe comes out about 1% above the physical pattern: 0.507 against 0.500 at
the physical half-power radius, and 0.901 against 0.892 at 42% of that
radius. Every Airy pattern CASA builds this way is affected: the simulator's
`setvp`, imaging's primary beams and the mosaic gridder's `HetArrayConvFunc`
dishes.

ALMA's 10.7 m effective diameter was chosen against CASA's beam, so casa-rs
tabulates the same expression (`casa_numerics::AnnularApertureVoltageTable`)
and its predictions match CASA's to 1.6e-7. The simulator's beam test checks
CASA's formula rather than the physical one.

## The simulator stores TAI as UTC

- Date noted: 2026-10-20
- Status: CASA behaviour casa-rs does not follow (#841, plan decision 5)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`observe`, lines 1262-1280)

`observe` converts the reference epoch to TAI and adds its seconds to the
start and stop offsets, then writes the result into TIME, whose MEASINFO says
UTC. The data therefore start 37 s (TAI − UTC since 2017) after the requested
epoch. The geometry is computed with the stored number read as UT1 (line
1546), so CASA's data are self-consistent; only the schedule is shifted.

casa-rs stores the requested UTC, and also reads the stored TIME as UT1 for
the geometry, so its uvw agree with CASA's at equal stored TIME. A parity run
requests a start 37 s later on the casa-rs side and compares rows at equal
TIME (`tools/simulation/compare_sm_ms.py`).

## The simulator charges shadowing to the antenna in front

- Date noted: 2026-10-21
- Status: likely CASA bug; casa-rs does not follow it (#841)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`blockage`, lines 1780-1815)

A row's uvw is `R·(X₁ − X₂)`, so `w > 0` puts antenna 1 nearer the source.
`blockage` zeroes antenna 2's blocked fraction when `w > 0` and antenna 1's
when `w < 0`, which leaves the blockage on the antenna in front, the one that
does the blocking. CASA's own flagger (`FlagAgentShadow`, since casapy 4.1)
flags the antenna behind. casa-rs's simulator flags the antenna behind
(`casa_ms::shadowing::ShadowedAntenna::Behind`); the CASA convention remains
available as `CasaSimulatorFront`.

## `setspwindow` relabels every spectral window's frame

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`initSpWindows`, line 750)

Each `setspwindow` call ends with `measFreqRef().fillColumn(freqType)`, which
writes the new window's frame into MEAS_FREQ_REF for every row of
SPECTRAL_WINDOW. A TOPO window followed by an LSRK one ends with both
labelled LSRK, although their frequencies were given in different frames.
casa-rs writes each window's own frame.

## `setconfig` accepts coordinate systems it cannot convert

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`initAnt`, lines 452-456;
  `longlat2global`, lines 566-575)

For `coordsystem='longlat'`, `longlat2global` logs SEVERE "not yet
implemented"; for an unknown name, `initAnt` logs SEVERE "Unknown coordinate
system type". In both cases the antennas are added at the Earth's centre and
`setconfig` returns True, so the script continues with an array whose
geometry is meaningless. In casa-rs, `longlat` raises RuntimeError and an
unknown name logs CASA's message and returns False; neither adds antennas.

## The tools convert quantities whose units do not conform

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: casatools' argument conversion (`casaQuantity`,
  `casaMDirection`) used by `simulator_cmpt.cc` and the other tools

A tool argument such as `integrationtime` or `freq` is read as a quantity
and converted to the unit the method needs without checking that the units
conform. CASA 6.7.6 takes `integrationtime='10Jy'` as 1e-25 s,
`integrationtime='10m'` (metres, perhaps meant as minutes) as 10 s and
`freq='1s'` as 1 Hz. A direction string whose second or third token does
not read keeps the previous value, so `'J2000 12h00m00 garbage'` has a
declination of 180°. casa-rs refuses both with casacore's "From/to units
not consistent" or a parse error. The conversions CASA defines between
dimensions are kept: a dimensionless number is the SI unit, an angle is a
time at 360° a day, and a length is a frequency through c.

## Simulator calls before `setconfig` abort the process

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`initFeeds`, `observe`)

`setfeed` or `observe` before any `setconfig` builds a table slice over zero
antennas. casacore's array code throws `std::runtime_error` (an empty
Slicer), which the tool binding does not catch, and the Python process
aborts. casa-rs raises RuntimeError from `setfeed`, and `observe` returns
False with "Antenna information not yet defined".

## A zero integration time adds rows without bound

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `simulator_cmpt.cc` (`settimes`, line 221);
  `ms/MSOper/NewMSSimulator.cc` (`observe`)

`settimes` asserts only that the integration time is not negative, and
`settimes()` with no arguments sets 0 s. `observe` then divides the scan's
span by zero to count integrations; over a nonzero span this adds billions of
rows before the disk fills. casa-rs's `observe` returns False for an
integration time that is not positive ("observe: the integration time must be
positive").

## `simutil.readantenna` averages a stale loop variable

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `casatasks/src/private/simutil.py` (`readantenna`, lines
  1783-1791)

When a configuration file gives absolute coordinates, names no known
observatory and has no `COFA` header, `readantenna` means to place the
observatory at the mean antenna position. It averages `x`, `y` and `z`,
which are the loop variables of the UTM branch, rather than the antenna
lists `stnx`, `stny` and `stnz`. For an XYZ file the names are unbound and
the call raises UnboundLocalError; for a UTM file the observatory becomes
the last antenna. It then assigns `xyz2long`'s (longitude, latitude,
height) to (latitude, longitude, height), so the two angles are swapped.
casa-rs converts the mean antenna position, as the comment intends
(`casa_simulation::readantenna`, which lists its smaller divergences).

## `simutil.readantenna` names antennas after blank lines

- Date noted: 2026-10-21
- Status: CASA bug; casa-rs does not follow it (#841)
- Affected code: `casatasks/src/private/simutil.py` (`readantenna`)

`readantenna` skips a line with fewer than four values, but still appends a
default `A%02d` pad and antenna name for it, so the names run ahead of the
positions by one for every such line. CASA's own `WSRT.cfg` has two blank
lines, so
`readantenna` returns 16 names for 14 antennas, starting `A00, A01, RT0`,
and a CASA simulation of WSRT names every dish wrongly. casa-rs skips such
lines entirely.

## The measures tool converts with a frame at MJD 0 and the geocentre

- Date noted: 2026-10-21
- Status: CASA behaviour; casa-rs does not follow it (#841)
- Affected code: `casatools/src/tools/measures/measures_cmpt.cc` (the
  `measures` constructor)

The `measures` tool starts with a frame of a default epoch (MJD 0 UTC), a
default position (the zero vector) and a default direction (the J2000
pole). A conversion that needs a frame element `me.doframe` has not set uses
these without a warning, so `me.measure(me.direction('J2000', '12h00m00',
'45d00m00'), 'AZEL')` in a fresh tool returns an azimuth and elevation for
1858-11-17 at the centre of the Earth (casacore only notes, at INFO, that MJD
0 is outside its Earth-orientation table). After `me.done()` the frame is
empty and the same call fails with "Cannot convert due to missing frame
information". casa-rs's tool starts with the empty frame, so such a
conversion always fails with that message until `doframe` sets the element.

## `sm.predict` rewrites the model image

- Date noted: 2026-10-21
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc`
  (`createSkyEquation`, lines 2319-2344)

`createSkyEquation` opens each model as a `PagedImage<Float>`, gives its
coordinate system an LSRK spectral conversion and Hz world units ("FTMachine
only works in Hz and LSRK") and stores it back with `setCoordinateInfo`, so
predicting rewrites the model's `table.dat`. Later reads of the model,
including another `predict`, see coordinates the user did not write; the
CASA oracle (`tools/simulation/casa_sm_observe_oracle.py --model-root`)
copies the model for every call because of it. casa-rs converts the
spectral axis to Hz in memory and never writes the model.

## `sm.predict` accepts models the simulator cannot read

- Date noted: 2026-10-21
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `casatools/src/tools/simulator/simulator_cmpt.cc`
  (`predict`, lines 620-629); `synthesis/MeasurementEquations/Simulator.cc`
  (`createSkyEquation`, lines 2313-2319 and 2568-2572; `predict`, lines
  2161-2163)

casatools' binding checks each model with `ImageFactory::fromFile`, which
also opens FITS images and complex-valued CASA images. The simulator then
reads each model as a `PagedImage<Float>`. A FITS file is not a table, so
it logs "… is unreadable" and then "No model images found"; a complex image
throws when opened as Float, which `createSkyEquation` catches ("Caught
exception: …"). Either way `sm.predict` logs "Failed to create SkyEquation"
and returns False, and the script continues with DATA not predicted. These outcomes are read from the source
and were not probed. casa-rs's check opens each model as the simulator reads
it, so a FITS or complex model raises RuntimeError "Unable to open image …"
at the call.

## IAU 2000 keeps the IAU 1980 sidereal time and ecliptic obliquities

- Date noted: 2026-10-22
- Status: casacore behaviour; casa-rs follows it for parity (#850)
- Affected code: `measures/Measures/MCEpoch.cc` (`initConvert`, `UT1_GAST`),
  `measures/Measures/MeasMath.cc` (`applyMECLIPtoJMEAN`,
  `applyTECLIPtoJTRUE`), `measures/Measures/MeasTable.cc` (`fundArg`)

With `measures.iau2000.b_use` set, casacore still forms GAST from the IAU
1982 GMST and the IAU 1980 equation of the equinoxes of `Nutation::STANDARD`,
without the 1994 complementary terms, and every HADEC, AZEL and ITRF
conversion takes its LAST from that GAST. TECLIPTIC rotates by the IAU 1980
true obliquity and MECLIPTIC by `MeasTable::fundArg(0)`, under either model.
That polynomial's T² term is −0.0059″, where IAU 1976 has −0.00059″: 0.37
mas in MECLIPTIC by 2026. casa-rs computes all of these as casacore does
(`casacore_sidereal`, `casacore_frames`), which moved its IAU 2000A
J2000→HADEC from 20.4 to 16.7 mas off casacore, the residual of the APP
step alone.

## Direction latitudes come from an unnormalized vector

- Date noted: 2026-10-22
- Status: casacore numerical defect; casa-rs does not follow it (#850)
- Affected code: `measures/Measures/MeasData.cc` (`GALtoJ2000`,
  `GALtoB1950`, `MToB1950`, `MToJ2000`), `measures/Measures/MCDirection.cc`
  (`doConvert`), `casa/Quanta/MVDirection.cc` (`getLat`, `get`)

casacore's galactic and FK4 matrices are 10-digit literals, orthogonal only
to about 7e-11 (1.5e-8 for `MToB1950(4)`), and `MCDirection::doConvert`
does not renormalize after applying them. `MVDirection::getLat()` and
`get()` then take `asin(z)`, assuming a unit vector, so the latitude casacore
reports is off by about (|v| − 1)·tan(b) and becomes NaN once z exceeds 1.
casa-rs converts with the same matrices, renormalizes, and reports the
latitude of the unit vector. The two directions agree to 0.01 µas; the
reported angles differ by up to 51 µas for B1950→GALACTIC on the law's grid
and by 1.6 mas for J2000→B1950 at b = −89.77°, where tan(b) is 250
(`measures_frames_interop`).

## IAU 2000 JMEAN to J2000 applies bias and precession in the wrong order

- Date noted: 2026-10-22
- Status: casacore bug; casa-rs does not follow it (#850)
- Affected code: `measures/Measures/MeasMath.cc` (`deapplyPrecession`)

Under IAU 2000, `applyPrecession` takes J2000 to JMEAN as `in *= B; in *= P`,
v′ = Pᵀ·Bᵀ·v, with B the frame bias and P the precession. The inverse,
`deapplyPrecession`, applies `in = B·in` and then `in = P·in`, which is
P·B·v′, not B·P·v′. The two do not commute, so casacore's JMEAN→J2000 is
not the inverse of its J2000→JMEAN: MECLIPTIC→J2000 and TECLIPTIC→J2000 are
0.14 mas off in 2026, and the error grows with time from J2000. casa-rs
inverts exactly; the frame law compares the IAU 2000 ecliptics from the
equator of date.

## IAU 2000 GMST1 adds the TT day fraction

- Date noted: 2026-10-22
- Status: casacore bug; casa-rs does not follow it (#850)
- Affected code: `measures/Measures/MCEpoch.cc` (`UT1_GMST1`,
  `GMST1_UT1`), `measures/Measures/MeasTable.cc` (`GMST00`)

Under IAU 2000, `UT1_GMST1` turns the UT1 epoch into TT and adds
`GMST00/(2π)` and 6713 days to it. `GMST00` is already the sidereal time of
day (the Earth rotation angle in [0, 2π) plus precession), so the TT day
fraction is counted twice: UT1 51544.5 gives GMST1 58258.280, against the
IAU 1976 path's 58257.779 and a GAST of 58257.779, and LMST inherits the
error. `GMST1_UT1`'s iteration does not undo it: GMST1 58258.280 converts
back to UT1 51544.999. casa-rs uses the IAU 1976 GMST1 under either model,
as casacore's own GAST does, so its GMST1, LMST, GAST and LAST agree with
each other and, under IAU 1976, with casacore.

## Writing a limb-darkened disk without an exponent aborts CASA

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `casatools/src/code/components/ComponentModels/ComponentList.cc`
  (`writeTable`, lines 1016-1022; `readTable`, lines 1155-1161)

A component list holding a limb-darkened disk gets an `Optional_Parameters`
column, and `writeTable` puts each such component's optional parameters in
it; the first is the disk's limb-darkening exponent. A limb-darkened disk
made through `cl.fromrecord` without `optionalParameters` has none, and
writing it throws `casacore::indexError` from the `ComponentList`
destructor, which terminates the process (probed with CASA 6.7.6: `fromrecord`
and `rename` succeed, `close` aborts). Had the empty vector been stored,
`readTable` would read its first element out of bounds. casa-rs leaves the
cell undefined, which CASA reads as the shape's default exponent of 0.05, as
it reads every other row the column does not describe.

## `setmosaicfield` sits its grid off the field centre

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs does not follow it (#841 Phase 2, plan §4
  "Diverged and recorded")
- Affected code: `synthesis/MeasurementEquations/Simulator.cc`
  (`setmosaicfield`, lines 849-901)

`setmosaicfield` tests for an odd count with `(nx/2)!=floor(nx/2)` on
integers. That is always false, so every grid takes the even-count offsets.
Those offsets, `(i − ceil(nx/2)) − 0.5` spacings in right ascension and
`(j − ceil(ny/2.0)) − 0.5` in declination, count `i` and `j` from zero where
one was meant.

Measured with CASA 6.7.6 at 1′ spacing:
- **3 × 3:** right ascension offsets −1.5, −0.5 and +0.5 spacings;
  declination offsets −2.5, −1.5 and −0.5.
- **2 × 2:** −1.5 and −0.5 spacings on both axes.

A centred grid would be at −1, 0 and +1, and at ±0.5.

Over a pole, CASA also sets the right ascension to `|π − α|`, where crossing
the pole adds π.

casa-rs centres every grid: offsets `(i − (n − 1)/2)` spacings, right
ascension divided by the centre's cos δ. It crosses a pole as the sphere
does. The fields keep CASA's names, `{sourcename}_{k}`, numbered down each
column and then across.

## `observemany` pads one name with empty ones

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs follows its outcome (#841 Phase 2)
- Affected code: `casatools/src/tools/simulator/simulator_cmpt.cc`
  (`observemany`, lines 318-326)

casatools means one source name, or one direction, to stand for every
pointing. It grows the list with `Vector<String>::resize(nptg, true)`, which
keeps the existing elements and default-constructs the rest, so the new
entries are empty strings, not copies. With one name and several pointings,
pointing 1's field is `""`. `NewMSSimulator::observe` raises "Field 1:  not
yet defined", and `sm.observemany` logs it and returns False (measured with
CASA 6.7.6). With one direction, the later pointings have none, and each
keeps the previous pointing's phase centre while the field stays the same.

`casars.tools.simulator` pads the lists the same way, so it returns the same
False, and moves only the first pointing. Unlike CASA, it adds no rows
before refusing (next entry).

## `observemany` adds rows before it looks up later fields

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs does not follow it (#841 Phase 2)
- Affected code: `ms/MSOper/NewMSSimulator.cc` (`observe`, the vector
  form)

`observe` adds every pointing's MAIN rows (`ms_p->addRow(nNewRows)`) before
its pointing loop. It looks up the first pointing's field before that, but
each later field only when the loop reaches it. A later field that is not
defined raises with the rows already added: their FIELD_ID, TIME and UVW are
unwritten, and the call returns False. casa-rs looks up every pointing's
field before it plans anything, so the same call returns False with the
same message and leaves no rows.

## `sm.predict` through `WProjectFT` without planes crashes the simulator

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs does not follow it (#841 Phase 2)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc`
  (`createSkyEquation`, line 2465); `synthesis/TransformMachines/WPConvFunc.cc`
  (`findConvFunction`, lines 164-176)

The simulator builds its `WProjectFT` with no w statistics (`minW`, `maxW`
and `rmsW` all −1). For `setoptions(ftmachine='wproject', wprojplanes=…)`
below 1, `findConvFunction` takes its automatic branch, whose largest w
then comes out negative and whose plane count comes out zero or negative.
Measured on CASA 6.7.6: `setoptions` returns True, and `predict` with
`wprojplanes=0` or `-1` kills the Python process with a segmentation fault
(exit 139) after it logs "Fourier transforms will use specified common
tangent point". casa-rs records the planes at `setoptions`, as CASA does,
and its `predict` raises RuntimeError ("W projection needs at least two
planes").

What casa-rs follows of the same machine:
- `wprojplanes=1` returns False from `predict` with CASA's "Convolution
  function integral is not positive".
- `WProjectFT` pads by 1, its constructor's default, whatever
  `setoptions(padding=…)` says.
- Autocorrelations predict zero, since the machine is built with `usezero`
  off.
- Rows beyond the largest w the planes reach (`0.25/|cell|`) predict zero.

## The EVLA's beam table has no 11052 MHz fit

- Date noted: 2026-10-23
- Status: likely CASA bug; casa-rs follows it for parity (#841 Phase 2)
- Affected code: `synthesis/TransformMachines/PBMath1DEVLA.cc` (`init`,
  line 320)

`PBMath1DEVLA::init` fills its map of polynomial beam fits by frequency, in
MHz, one 128 MHz step at a time. In X band the sequence runs 10948, then
10052, then 11180: the second 10052 was meant for 11052. Through
`std::map::operator[]` it replaces the first 10052 fit, so the map has no
11052 entry. The 10052 MHz frequencies use the fit meant for 11052, and
those near 11052 MHz take a neighbour's. The coefficients differ by about 1%
in the linear term, which moves the beam by a few 1e-3 at its half power.
casa-rs's table (`casa_numerics::EvlaPowerPattern`, shared by the
simulator and imaging) is CASA's map as built.
