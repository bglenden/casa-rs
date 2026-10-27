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
[the EVLA's 11052 MHz fit](#the-evlas-beam-table-has-no-11052-mhz-fit),
[the simulator's and tclean's two mosaic machines](#the-simulators-mosaic-machine-is-not-tcleans),
[a moved pointing phased on its field](#smpredict-phases-a-moved-observemany-pointing-on-its-field).
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
[W projection without planes](#smpredict-through-wprojectft-without-planes-crashes-the-simulator),
[total power in cross-correlations](#sdgrid-predicts-total-power-into-cross-correlations-with-no-positive-uvw),
[`close` after a mosaic predict](#smclose-aborts-the-process-after-a-mosaic-predict),
[the beam on a moved pointing](#the-beam-on-a-moved-pointing-stays-where-the-fields-first-row-points),
[scratch columns added at `predict` and `corrupt`](#predict-and-corrupt-add-scratch-columns-to-a-measurementset-without-them),
[`tsys-manual`'s stale and borrowed system temperatures](#tsys-manual-noise-takes-stale-or-borrowed-system-temperatures),
[`tsys-manual` scaling every noise term](#a-tsys-manual-term-scales-every-noise-term),
[the noise term a refused `tsys-manual` leaves](#a-refused-tsys-manual-setnoise-leaves-a-unit-noise-term),
[`tsys-atm`'s atmosphere half a band away](#tsys-atm-noise-takes-its-atmosphere-half-a-band-below-each-channel),
[`settrop`'s dispersive delay alone](#settrop-turns-the-phase-by-the-dispersive-wet-delay-alone),
[`settrop`'s atmosphere half a band away](#settrop-takes-its-atmosphere-half-a-band-below-each-channel),
[`settrop`'s anisotropic screen](#settrops-screen-has-an-anisotropic-spectrum),
[`settrop`'s solutions stamped halfway](#settrop-with-simint-stamps-a-solution-halfway-to-its-intervals-start),
[gain screens read past their transform](#casas-gain-screens-read-past-their-transform),
[gain screens sharing a seed](#gain-screens-of-neighbouring-antennas-share-a-seed),
[random gains about zero](#setgainmoderandom-draws-gains-about-zero),
[the last antenna's unit gain or leakage](#an-autocorrelation-row-resets-its-antennas-simulated-term),
[leakage for two windows only](#setleakage-simulates-only-the-first-two-spectral-windows),
[a simulated term's table written when it is set](#a-simulated-terms-table-is-written-when-the-term-is-set).
Measures behaviours casa-rs follows for parity:
[IAU 1980 sidereal time and obliquities under IAU 2000](#iau-2000-keeps-the-iau-1980-sidereal-time-and-ecliptic-obliquities).
Measures behaviours casa-rs does not follow:
[latitudes of unnormalized directions](#direction-latitudes-come-from-an-unnormalized-vector),
[IAU 2000 JMEAN to J2000](#iau-2000-jmean-to-j2000-applies-bias-and-precession-in-the-wrong-order),
[IAU 2000 GMST1](#iau-2000-gmst1-adds-the-tt-day-fraction).
Simulator behaviours casa-rs follows:
[`observemany`'s padded single name](#observemany-pads-one-name-with-empty-ones),
[one leakage per window for the whole run](#setleakage-draws-one-leakage-per-window-for-the-whole-run).
Component-list behaviours casa-rs does not follow:
[a limb-darkened disk without an exponent](#writing-a-limb-darkened-disk-without-an-exponent-aborts-casa),
[a negative component under the primary beam](#a-negative-component-turns-positive-under-the-primary-beam),
[a single spectral index](#a-single-spectral-index-is-read-past-its-end),
[the beam's component spectrum frame](#the-primary-beams-component-spectrum-labels-topocentric-samples-lsrk),
[`addcomponent`'s single-precision flux](#claddcomponent-stores-the-flux-in-single-precision),
[the `cl` tool's record units](#the-cl-tool-records-a-shapes-axes-in-the-units-it-was-given).
Component-list behaviours casa-rs follows:
[`addcomponent`'s polarization](#claddcomponent-stores-four-flux-values-as-stokes),
[a shape `addcomponent` cannot make](#claddcomponent-keeps-a-point-for-a-shape-it-cannot-make).
ATM data errors casa-rs corrects (`tools/atmosphere/atm-corrections.patch`):
[HCN's misplaced line](#atms-hcn-catalogue-misplaces-a-line-at-106-ghz),
[sea-level air densities](#atms-reference-air-densities-drop-a-digit-at-sea-level),
[the H2(17)O weight](#atm-weights-h217o-by-the-18o-abundance),
[the HDO weight](#atms-hdo-weight-divides-by-three).
ATM behaviours casa-rs follows:
[HCN above 100 km](#atms-hcn-profile-stops-at-100-km),
[copied bottom pressures](#copying-an-atmprofile-copies-its-top-pressures-as-bottom-pressures),
[unpadded ozone isotopologue layers](#atm-leaves-the-ozone-isotopologue-layers-unpadded),
[(16)O(17)O](#atm-omits-16o17o).
ATM behaviours casa-rs does not follow:
[the averaged non-dispersive wet phase](#refractiveindexprofiles-averaged-non-dispersive-wet-phase-reads-a-channel-as-a-water-column).
Calibration behaviours casa-rs does not follow:
[a solve that fits exactly](#viscalsolver2-fails-a-solve-whose-chi-square-is-exactly-zero).
`simutil` behaviours casa-rs does not follow:
[the ALMA tiling's default beam](#calc_pointings2s-alma-tiling-reads-an-undefined-pbcoeff),
[`noisetemp`'s receiver lookup](#noisetemp-reads-the-wrong-receiver-temperature-at-the-edges-of-its-tables),
[directions without an epoch](#direction_splitter-cannot-split-a-direction-without-an-epoch),
[`modifymodel`'s axis swaps](#modifymodel-swaps-axes-into-place-and-refuses-a-cyclic-order),
[`modifymodel`'s Stokes labels](#modifymodel-labels-the-new-images-stokes-planes-by-their-count),
[`incell` as two cells](#modifymodel-indexes-the-map-it-makes-of-two-cells),
[the component-only model's unset width](#an-unset-compwidth-gives-the-component-only-model-a-meaningless-spectral-increment),
[component spectra in another frame](#iamodify-converts-a-component-spectrum-through-the-images-default-measures-frame).

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

The same offset separates the calibration tables the two simulators write
(`setgain` and `setleakage` with `table=`): CASA's TIME is TAI under the UTC
label. `sm.setapply` of a CASA-simulated table to a casa-rs MeasurementSet,
or the reverse, therefore takes its solutions 37 s away, several solutions
of 10 s integrations. The `corrupt-setapply-*` scenarios agree with CASA
because their runs are shifted as above; tables solved from real data carry
true UTC and are not affected.

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

## A negative component turns positive under the primary beam

- Date noted: 2026-10-23
- Status: CASA bug casa-rs does not follow (#841)
- Affected code: `casatools/src/code/synthesis/TransformMachines/BeamSkyJones.cc`
  (`apply(SkyComponent&, …)`, lines 488-511)

With `sm.setvp(dovp=True)`, `SkyEquation::applySkyJones` applies the beam to
each component with `fullspectral` set. That path multiplies the beam-applied
flux, which keeps the component's sign, by a per-channel factor of the
spectrum's sample over `abs(flux)`, which carries the sign again, so a
negative flux comes out positive. Probed with CASA 6.7.6: a −0.4 Jy point
20 arcsec from the pointing centre predicts −0.4 Jy without the beam and
+0.4 Jy times the beam with it. casa-rs keeps the sign;
`sm_components_scenarios.json` keeps negative components away from the beam.

## A single spectral index is read past its end

- Date noted: 2026-10-23
- Status: CASA bug casa-rs does not follow (#841)
- Affected code: `casatools/src/code/components/ComponentModels/SpectralIndex.cc`
  (`sampleStokes`, lines 138-146 and 213-237);
  `components/ComponentModels/SkyCompRep.cc` (`visibility`, line 314)

`cl.addcomponent(spectrumtype="spectral index", index=α)` stores one index.
The per-frequency `sampleStokes` scales only I by it and leaves Q, U and V
alone; the matrix `sampleStokes`, which `SkyCompRep::visibility` and so
every component prediction uses, has no one-index branch and reads indices
1 to 3 out of bounds. For an unpolarised component the out-of-bounds factors
multiply zeros. Read from the source and not probed: a polarised component
with a single index predicts Q, U and V scaled by whatever follows the
vector. casa-rs reads a single index as `[α, 0, 0, 0]`, so the fractional
polarisation is constant, as the matrix sampler's comments describe;
`sm_components_scenarios.json` gives its polarised components no index.

## The primary beam's component spectrum labels topocentric samples LSRK

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `casatools/src/code/synthesis/TransformMachines/BeamSkyJones.cc`
  (`apply(SkyComponent&, …)`, lines 494-529);
  `components/ComponentModels/SkyCompRep.cc` (`visibility`, lines 303-314)

The full-spectral beam path evaluates the beam and the spectrum at each
channel's topocentric frequency, stores the products in a `TabularSpectrum`
at the channels' LSRK frequencies, and labels it LSRK. `SkyCompRep::visibility`
then looks the table up at the topocentric frequencies, which it takes to be
in the component's frame, so each channel reads the table shifted by the
observatory's velocity, up to about 1e-4 of the frequency. casa-rs evaluates
the beam and the spectrum at each channel's own frequency. With the beam,
`sm_components_scenarios.json` agrees with CASA to 7e-5 and 1e-4 normalized
RMS, against 1.3e-5 or better without it: consistent in size with this
shift, though the shift was not isolated.

## `cl.addcomponent` stores four flux values as Stokes

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs follows it (#841, owner decision 2026-10-23)
- Affected code: `casatools/src/tools/componentlist/componentlist_cmpt.cc`
  (`setflux`, lines 831-930); `components/ComponentModels/Flux.tcc`
  (`FluxRep::setValue(const Vector<T>&)`, lines 363-368)

`setflux`, which `addcomponent` calls, sets the flux's representation from
`polarization` and then stores four values with
`FluxRep::setValue(Vector<Double>)`, which resets the representation to
Stokes. So `cl.addcomponent(flux=[1, 0.1, 0.2, 0.9],
polarization='circular')` stores I = 1, Q = 0.1, U = 0.2, V = 0.9 rather than
RR, RL, LR and LL (probed with CASA 6.7.6 for 'Circular', 'circular',
'CIRCULAR' and 'linear'). `polarization` still has effects: an unknown name
raises, and one value with a representation other than Stokes raises. casa-rs
does the same, so a script gets the list CASA would give it.

## `cl.addcomponent` stores the flux in single precision

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `casatools/src/tools/componentlist/componentlist_cmpt.cc`
  (`setflux`, lines 858-870)

`setflux` reads the values into a `Vector<Complex>` (single precision) before
storing them as doubles, so `flux=0.1` is stored as 0.10000000149011612
(probed with CASA 6.7.6). casa-rs keeps the values given; they differ from
CASA's by at most one single-precision rounding, 6e-8 relative.

## The `cl` tool records a shape's axes in the units it was given

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `components/ComponentModels/TwoSidedShape.cc` (`toRecord`,
  lines 265-330)

`TwoSidedShape` remembers the units its axes were set in, and its record
gives them in those units: a Gaussian made by `addcomponent` with
`majoraxis='1arcsec'` records `{'unit': 'arcsec', 'value': 1.0}`. A shape
read from a table has the units `readTable` sets: arcmin for the axes and
degrees for the position angle, with the errors in radians. casa-rs holds
radians and records every shape as CASA records one read from a table
(`{'unit': 'arcmin', 'value': 0.016666666666666666}`); the quantities are the
same, and the table is the same either way.

## `cl.addcomponent` keeps a point for a shape it cannot make

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs follows (#841)
- Affected code: `casatools/src/tools/componentlist/componentlist_cmpt.cc`
  (`addcomponent`, lines 453-526; `setshape`, lines 1285-1386)

`addcomponent` adds a point and then calls `setshape`, ignoring what it
returns. `setshape` logs SEVERE and returns False for a shape name it does
not know, for a limb-darkened disk ("not implemented yet"), and for an axis
that is not an angle (none given, a number, or another unit), so the
component stays a point at the given direction and `addcomponent` returns
True (probed with CASA 6.7.6). casa-rs does the same and logs the message at
ERROR. Scripts that misspell a shape name therefore get points, as they do in
CASA.

What `setshape` raises, `addcomponent` raises too, after removing the
component it began: an axis that does not read as a quantity (`'garbage'`, a
complex number), and what the shape's constructor raises, such as a
Gaussian's position angle outside ±2π (`Gaussian2DParam::setPA`) or axes that
are not positive. A minor axis longer than the major raises as well:
`TwoSidedShape::fromRecord` (`TwoSidedShape.cc`, lines 229-263) means to
refuse it, but compares the minor axis with itself (`near(minorAxisInRad,
minorAxisInRad)`, line 249), so the check never fires and the constructor
raises instead. casa-rs raises in each case with casacore's message, and adds
nothing.

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

## `SDGrid` predicts total power into cross-correlations with no positive uvw

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (#841)
- Affected code: `synthesis/MeasurementComponents/SDGrid.cc` (`get`, the
  `rowFlags` loop)

`sm.predict` after `setoptions(ftmachine='sd')` samples the model at each
row's pointing. `SDGrid::get` leaves out a row whose uvw has a positive
component (`max(uvw) > 0`), which is meant to keep the cross-correlations of
a single-dish predict at zero. A cross-correlation whose u, v and w are all
zero or negative passes that test and is given the pointing's total power as
if it were one dish's autocorrelation. Read from the source; a probe with two
antennas saw only rows with a positive component. casa-rs predicts total
power into autocorrelations only.

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

## `sm.close()` aborts the process after a mosaic predict

- Date noted: 2026-10-23
- Status: CASA bug; casa-rs does not follow it (#841 Phase 2)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc` (`close`,
  after `createSkyEquation` built `MosaicFTNew` with `HetArrayConvFunc`)

After `setoptions(ftmachine='mosaic')` and a `predict` that returns True,
CASA 6.7.6's `sm.close()` kills the Python process: libc++abi prints "Pure
virtual function called!" and raises SIGABRT (exit 134 from a shell).
simobserve's own sequence (`setvp`, `setoptions(ftmachine='mosaic')`,
`predict`, `close`) aborts the same way. It was not traced to the
destructor that makes the call. DATA is complete on disk when it happens:
a rerun whose abort was turned into a quiet exit wrote the same DATA, UVW,
FLAG and TIME bit for bit, and those MeasurementSets agree with casa-rs's
to a few 1e-7 normalized RMS. casa-rs's `close` returns normally. The CASA
oracle (`tools/simulation/casa_sm_observe_oracle.py`) runs such scenarios
(`close_aborts`) in a child process, accepts the abort only when every
`predict` returned True, and records `close` as aborted.

## The simulator's mosaic machine is not tclean's

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs follows for parity, per machine (#841
  Phase 2, #875)
- Affected code: `synthesis/TransformMachines/HetArrayConvFunc.cc`,
  `SimplePBConvFunc.cc` and `MosaicFT.cc` (the simulator's, through
  `MosaicFTNew`) against their `synthesis/TransformMachines2/` namesakes
  (tclean's)

`sm.predict` after `setoptions(ftmachine='mosaic')` degrids through the
earlier heterogeneous-array machine, and `tclean(gridder='mosaic')` through
the later one. They differ in:
- the dish's support radius: 150″ at 100 GHz for every ALMA and ACA dish in
  the simulator; in tclean, `max(150″, fov/5)` for a 12 m dish and
  `max(300″, fov/3)` for any other;
- the screen's least size: none in the simulator, a tenth of the image in
  tclean;
- the frequency cells (`findUsefulChannels`): from the selected channels
  alone in the simulator, `lround(span / 0.5 % of the top)` cells matched in
  `Float`; stepping down from the window's top frequency in tclean;
- the support search: in the simulator, inwards from the lattice's edge
  along the row and the column one cell off its centre, the radius found
  widened by √2; in tclean, outwards from the weight plane's peak;
- the support a row is gridded with: the widest pair's of the window in the
  simulator, each pair's own in tclean;
- the channel frequencies: the window's own in the simulator; tclean
  converts them to the image's frame (LSRK).

On the #875 probe (six 12 m and six 7 m ALMA antennas over three fields at
230 GHz) CASA logs supports `[6, 6, 6]` for the simulator and `[7, 6, 6]`
for tclean, and the two machines' predictions of one model differ by
4.5–5.6e-3 normalized RMS per dish-pair class on a TOPO MeasurementSet and
1.9–2.4e-3 on an LSRK one. casa-rs's mosaic set takes either machine's
rules (`casa_imaging_operator::MosaicRules`): imaging follows tclean's, the
simulator its own.

Both machines reproject rows through `FTMachine::girarUVW`, whose new u and
v take no share of w, where `GridFT`'s `rotateUVW` applies the full
rotation. At offsets of 10–17″ the two differ by a few 1e-4 cells, which
flips the fine offset of a sample near a rounding boundary; with the
mosaic's ten offsets a cell, such a sample moved by up to 10% (#841: 33 of
4,752 rows, until the simulator took `girarUVW` too).

## `sm.predict` phases a moved `observemany` pointing on its field

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs follows for parity (#884, which corrected
  the session's own phasing of #861)
- Affected code: casacore `ms/MSOper/NewMSSimulator.cc` (`observe`, the
  vector form `observemany` calls) and `msvis/MSVis/VisBuffer.cc`
  (`phaseCenter`, from FIELD)

A pointing `observemany` gives a direction keeps its field's FIELD row.
`NewMSSimulator` computes its UVW and elevation at the direction and writes
that direction to POINTING, but leaves FIELD as it was. `predict` reads the
MeasurementSet through a `VisBuffer`, whose phase centre is FIELD's
PHASE_DIR, so it phases the pointing on the field while its UVW belong to
the moved direction. Measured on CASA 6.7.6 (twelve ALMA antennas, a second
pointing moved 10.4″ east): a 1 Jy point at the field's direction predicts
1 Jy at zero phase in both pointings, and one at the moved direction
predicts phases spread over ±180° (rms 101°) in the pointing centred on it.

casa-rs follows. A moved pointing's UVW, elevation limit and POINTING are at
the moved direction (`Scan::pointing_rad`), and its DATA is phased on its
field (`Scan::phase_center_rad`). Total power samples where POINTING points
(#874), as CASA's `SDGrid` does. The phasing is what every imager and
calibrator reading the MeasurementSet assumes: FIELD is the only phase
centre it records, so DATA phased on the moved direction would image
shifted by the offset. The UVW at the moved direction are CASA's
inconsistency. They distort the geometry only at second order, in the
offset times a source's distance from the phase centre.

Evidence on the CPU and on Metal:
- `components-moved-pointing` in `tools/simulation/sm_components_scenarios.json`, an image and a component list without a beam: 8.36e-6 normalized RMS;
- the `openfromms-moved-pointing-*` scenarios of `sm_openfromms_scenarios.json`: 0 and 7e-11.

A primary beam on a moved pointing is a separate matter, and casa-rs
refuses it ([the next entry](#the-beam-on-a-moved-pointing-stays-where-the-fields-first-row-points)).

## The beam on a moved pointing stays where the field's first row points

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs refuses a primary beam, and the mosaic machine,
  on a moved pointing rather than follow it (#884, PR #888 review)
- Affected code: `synthesis/TransformMachines/BeamSkyJones.cc` (`changed`,
  `update`) and `synthesis/MeasurementEquations/Simulator.cc`
  (`makeVisSet`'s sort)

`VPSkyJones`, the simulator's primary beam, inherits `BeamSkyJones::update`,
which takes the beam's centre from POINTING (`VisBufferUtil::getPointingDir`)
at the row it updates on. It updates only when `BeamSkyJones::changed`
fires:
- the MeasurementSet, the array or the field changes;
- the parallactic angle moves past `setvp`'s `parangleincrement`. Its
  default of 360° means never;
- POINTING is in a local frame and the time changes.

The comparisons of the pointing directions themselves are commented out.
`predict` iterates FIELD_ID first, then FEED1, ARRAY_ID, DATA_DESC_ID and
TIME. So every row of a field is beamed where the field's first-visited row
points, whichever direction a later pointing of the field was moved to.

Measured on CASA 6.7.6, ALMA at 230 GHz, a second pointing moved 10.4″ east:
- **Field first, then moved:** a point at the moved direction keeps 0.652 of
  the beam's peak in both pointings. The beam stays on the field.
- **Moved first, then back at the field** (Astra's probe on PR #888): 1.0 in
  both. The beam stays on the moved direction.
- **Total power:** `SDGrid` samples POINTING's moved direction while the beam
  stays where the first row pointed. With one antenna, a constant model and
  the BOX kernel, CASA's moved samples are 0.671.

None of these is the beam the moved pointing sees, which follows the
antennas. casa-rs refuses a primary beam over a model image, a component
list or total power, and the mosaic machine, on a pointing `observemany`
moved off its field. It does not reproduce a cache. `simobserve` gives each
pointing its own field and never reaches this.

## `predict` and `corrupt` add scratch columns to a MeasurementSet without them

- Date noted: 2026-10-23
- Status: CASA behaviour casa-rs does not follow (plan decision 6)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc` (`predict`
  and `corrupt` build a `VisSet`) and `msvis/MSVis/VisSet.cc` (its
  constructor's `addCalSet2` and `initCalSet`)

CASA's simulator writes its steps' results through a `VisSet`, whose
constructor adds MODEL_DATA (1) and CORRECTED_DATA (DATA) to a
MeasurementSet without CORRECTED_DATA. `predict` then sets CORRECTED_DATA to
the new DATA and MODEL_DATA to 1, and `corrupt` sets CORRECTED_DATA to the
corrupted DATA. casa-rs's simulator writes DATA only (plan decision 6), and
after `openfromms` it creates neither column: it keeps those MAIN already
has in step with DATA as CASA does, and leaves a MeasurementSet without
them without them.

## CASA's gain screens read past their transform

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`fBM::initialize`, `GJonesCorruptor::initialize`)

`sm.setgain(mode='fbm')` draws each antenna's amplitude and phase screens
with `fBM::initialize` on `n` even slots of 5 s. It fills a half spectrum
`F` of `n/2` values, all but the last, and transforms it with
`FFTServer::fft` into a real `G` whose length the server chooses: the last
value's imaginary part is zero and the server was sized `n`, which is
neither `2·(n/2) − 2` nor `2·(n/2) − 1`, so `G` has `n − 2` values for an
even `n` and `n − 3` for an odd one. It then copies `n` values out of `G`,
the last two or three from past its end, and `GJonesCorruptor::initialize`
scales each screen by its root-mean-square, those values included.

What lies past the end changes from run to run. Measured with CASA 6.7.6:
- three runs of `corrupt-gain` (`tools/simulation/sm_corrupt_scenarios.json`,
  the same seed) gave amplitude scales of 0.969, 0.966 and 1.005 of the
  documented one;
- in a six-antenna probe with `amplitude=[0.1]`, the phases' scale was
  0.15 to 0.22 of it in one run and 1.0 in another;
- with a table written, one run's last solution was unit gain, as zeros
  past the end would make it;
- in one run of `corrupt-gain-table`, antennas 0–6 and 10 of 12 had unit
  gain throughout, their screens normalised by a value large enough to
  flatten them.

casa-rs transforms every slot, so its screens have the documented scale.
The CASA comparison holds CASA's gains to closure only, reports rather than
checks CASA's unit-gain antennas, and compares the two implementations'
screens by their autocorrelations, which a screen's scale does not change.

`settrop(mode='individual')`'s water-vapour series come from the same
`fBM::initialize` and normalise the same way (#841 Phase 4d): in one
probe, antenna 0's phases were all below 5·10⁻¹⁰ rad. casa-rs transforms
every slot there too. On fewer than six slots the spectrum's bins
`i < N/2 − 1` hold only the mean, so whatever varies in CASA's series is
what it read past the transform; casa-rs refuses `individual` mode on so
short a track rather than add no turbulence.

## Gain screens of neighbouring antennas share a seed

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`GJonesCorruptor::initialize`)

`GJonesCorruptor::initialize` seeds antenna `a`'s amplitude screen for
receptor `r` with `seed + a + r`, and its phase screen with 100 times that.
Antenna `a`'s second receptor and antenna `a + 1`'s first draw the same
screens: measured, their amplitudes are identical. casa-rs keys each
antenna's and receptor's screens apart.

## `setgain(mode='random')` draws gains about zero

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (owner, 2026-10-24: follow
  CASA only if there is a good reason for its behaviour, which there is
  not; #841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`GJonesCorruptor::simPar`)

The `random` mode, which `setgain`'s documentation does not list but
`Simulator::setgain` accepts, returns `camp.re·n₁ + i·camp.im·n₂` per
integration, antenna and receptor, without adding one: gains of a few
hundredths, which all but erase the data. Measured with CASA 6.7.6,
`amplitude=[0.05]` on a 1 Jy point: the table's gains have a mean modulus
of 0.065, real parts of mean 0.0000 and standard deviation 0.048, and DATA
a mean modulus of 0.0046 Jy. The `fbm` gains scatter about one, and the
amplitude is documented as the gain variations' scale. Gain errors are
multiplicative and near unity: a well-designed array's gains factor into
antenna gains to within a per cent, and simulations of gain errors draw
deviations of a per cent and a degree about one (Synthesis Imaging in Radio
Astronomy II, pp. 122 and 354; Heywood, *Calibration II*, NRAO Synthesis
Imaging Workshop 2024, slide 22). Nothing in CASA adds the one back for
`random`; it appears in `GJonesCorruptor::simPar` and
`GJones::createCorruptor` only. casa-rs draws
`1 + camp.re·n₁ + i·camp.im·n₂`.

## An autocorrelation row resets its antenna's simulated term

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/SolvableVisCal.cc`
  (`setSimulate`)

`SolvableVisCal::setSimulate` fills a solution from its chunk's rows,
setting ANTENNA1's value at every row, and at an autocorrelation sets it to
1 for a multiplicative term instead of a draw. An antenna's autocorrelation
comes before its cross-correlations as ANTENNA1, which set it again, except
for the last antenna, which is ANTENNA1 only of its autocorrelation. So
with autocorrelations written, CASA's default, the last antenna has unit
gain throughout (`setgain`) and a leakage of 1 on both receptors
(`setleakage`); measured, antenna 42 of 43 in `corrupt-gain`. casa-rs
simulates every antenna's term. `settrop`'s `Tf` term takes the same loop
(its modes are not ones whose autocorrelations CASA simulates), and casa-rs
simulates every antenna's there too (#841 Phase 4d).

## `setleakage` simulates only the first two spectral windows

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/SolvableVisCal.cc`
  (`setSimulate`, `sizeUpSim`); the cause was not traced

Measured with CASA 6.7.6, five ALMA antennas and `setleakage(table=...)`:
with three, four or eight spectral windows, observed once or twice each,
the table has solutions for windows 0 and 1 only, and `corrupt` logs
"Encountered data spw N for which there is no (simulated) calibration" for
each later window and leaves its rows as they were. `setgain` is not
affected: with four windows its table has solutions for all four, and every
window is corrupted. casa-rs simulates a leakage for every window. The CASA
comparison, `corrupt-leakage`, observes two windows.

## `setleakage` draws one leakage per window for the whole run

- Date noted: 2026-10-24
- Status: CASA behaviour casa-rs follows (#841 Phase 3)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc`
  (`setleakage`), `synthesis/MeasurementComponents/SolvableVisCal.cc`
  (`setSimulate`) and `JonesGenLin`

`Simulator::setleakage` simulates with an infinite solution interval: its
table has one solution per spectral window and antenna, spanning every scan
and field, and `corrupt` applies it alike to every scan (measured: eight
scans of two windows). `corrupt` applies it as the linearised
`JonesGenLin`, which raises "JonesGenLin matrix apply (J::aR) incompatible
with VisVector." on data with two correlations. casa-rs follows both, and
likewise does not read `setgain`'s `interval`, whose documentation says it
is not used: CASA's gains are solved per integration.

## A simulated term's table is written when the term is set

- Date noted: 2026-10-24
- Status: CASA behaviour casa-rs does not follow (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/SolvableVisCal.cc`
  (`setSimulate`, `storeNCT`) and `NewCalTable::writeToDisk`

`setgain(table=...)` and `setleakage(table=...)` write the term's
calibration table as soon as they simulate it, replacing a table of that
name, its subtables copied from the MeasurementSet as it then is. casa-rs
writes the MeasurementSet only at `close`, so it writes the table there, from
the MeasurementSet it wrote: its rows, sorted by spectral window, time and
antenna as CASA sorts them, are the same (`tables` in
`tools/simulation/sm_corrupt_scenarios.json` compares them with CASA's), but
the table appears at `close`, after `reset` too, and its subtables include
what was observed after the call. casa-rs replaces only a table at the path;
anything else there is refused.

## `tsys-manual` noise takes stale or borrowed system temperatures

- Date noted: 2026-10-24
- Status: CASA bugs; casa-rs does not follow them (#841 Phase 3)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`AtmosCorruptor::setCurrTime` and `simPar`),
  `synthesis/MeasurementComponents/SolvableVisCal.cc` (`setSimulate`) and
  `synthesis/MeasurementComponents/Jones.cc` (`JonesScal::invert`,
  `setMatByOk`)

`setnoise(mode='tsys-manual')` adds a unit-amplitude `ANoise` term and a `T`
term flagged `NoiseScale`, whose inverse scales the noise:
σ_ij = c a / sqrt(Δν t) · sqrt(Tsys(A_i) Tsys(A_j)) / (D_i D_j)
(`casa_simulation`'s `corrupt::tsys` has the whole formula). CASA computes
the `T` table when `setnoise` is called, one solution per integration and
spectral window, and applies it at `corrupt` by the nearest time. Measured
on CASA 6.7.6 (the probe scripts and their results are on the NAS under
`casa-rs/issue-841-tsys-3a/probes`; seven antennas of 12 m and 7 m, per-row
variances over 32 to 128 channels):
the formula holds to 4·10⁻⁴ (cross 0.99999 ± 0.00036, autocorrelations
1.0003 ± 0.0009, 12+12, 12+7 and 7+7 m pairs alike, and to 6·10⁻⁴ with every
parameter away from its default), and CASA's `T` table, written with
integrations of 120 s so that the cache below refreshes at each, gives every
row's σ within 1.9·10⁻⁶ of casa-rs's (5,040 rows, two windows). But four
inputs are not the row's own:

- **The airmass is up to 100 s stale.** `simPar` caches each antenna's
  airmass and `setCurrTime` recomputes it only when the solution time has
  moved more than 100 s since (the comment's intent is a 1 % tolerance at
  elevations above 5°). At 10 to 14° elevation with τ₀ = 1 and 2 s
  integrations, CASA's per-integration variance against the formula at the
  integration's own time rises in a sawtooth with a 102 s period from 1.01
  to 1.56, 1.176 ± 0.0014 on average. The cache is not reset at a field
  change either.
- **Rows observed after `setnoise` take another integration's solution.**
  The table holds only the integrations observed by then; a scan observed
  later is scaled by the nearest one. A scan at 51.6° elevation observed
  after `setnoise` got the noise of the earlier scan's last integration at
  72.7°: 0.666 ± 0.002 of its own variance (τ₀ = 0.5).
- **Every row takes the first row's EXPOSURE** (`exposure()(0)`): after
  `settimes` changed the integration time from 10 s to 2 s, the 2 s scan's
  variance was 0.1999 of its own, 0.9994 of the first row's.
- **An antenna without an unflagged row applies as 1.** `setSimulate` skips
  fully flagged rows, so a shadowed or low antenna gets no solution;
  `JonesScal::invert` marks it bad and `setMatByOk` applies it as 1. Its
  rows get the unit noise times the other antenna's scale (variance 1.0006
  of 1 Jy² when both are bad, 1.009 of s_j² when one is).

casa-rs computes each row's σ from its own integration: each antenna's
airmass at the phase centre at the row's TIME, the row's EXPOSURE, its
window and dishes, whatever the flags and whenever the row was observed.
The CASA comparison scenarios (`tools/simulation/sm_noise_scenarios.json`)
observe above 40° elevation with one integration time and no flagged rows,
where the stale airmass moves Tsys by under 0.1 %. casa-rs keeps CASA's
rule for a `T` that is not finite and positive (Tsys overflowing single
precision below the horizon, airmass 1000): that antenna scales by 1.

## A `tsys-manual` term scales every noise term

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (owner, 2026-10-24: follow
  CASA only if its product is the better simulation, which it is not; #841
  Phase 3)
- Affected code: `synthesis/MeasurementEquations/VisEquation.cc`
  (`collapseForSim`)

`collapseForSim` first adds every `ANoise` term's noise to DATA, then
applies every `NoiseScale` `T` term to DATA, in correct mode. So each
`tsys-manual` term's scale multiplies the noise of every term, its own
and the others'. Measured on CASA 6.7.6: two `tsys-manual` terms of one
seed gave variance 4 σ⁴ (1.004 of it) rather than 4 σ², in Jy⁴, and a
0.5 Jy `simplenoise` term with a `tsys-manual` term gave (0.25 + 1) σ²
(0.998 of it) rather than 0.25 + σ²: the 0.5 Jy noise came out scaled by
the system temperature. casa-rs adds each term's noise with its own σ, as
`simplenoise` terms add, so `simplenoise`'s σ stays in Jy whatever else is
set; terms of one seed still add coherently. `simobserve` sets one noise
term and never reaches this.

CASA's product has no physical counterpart. A system temperature is the
sum of its contributions (receiver, atmosphere, spillover, CMB), which a
single `tsys-manual` term already models; a baseline's noise variance
scales as the product of its two antennas' system temperatures; and the
powers of uncorrelated noise add (Thompson, Moran & Swenson, *Interferometry
and Synthesis in Radio Astronomy*, 3rd ed., pp. 55, 266, 386 and 733).
Multiplying one term's σ by another's gives Jy², not a noise in Jy.

## A refused `tsys-manual` `setnoise` leaves a unit noise term

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 3)
- Affected code: `synthesis/MeasurementEquations/Simulator.cc` (`setnoise`)

`setnoise` creates the unit-amplitude `ANoise` term before it checks the
Tsys parameters. A water vapour above 100 mm, or a zenith Tsys that is not
positive or above 10⁶ K, then makes it return `False`, but the `ANoise` stays:
the next `corrupt` adds 1 Jy of noise (CASA 6.7.6, `pwv='200mm'`: variance
0.872 over cross- and autocorrelations, where 1 Jy gives 0.875). casa-rs
refuses such a `setnoise` without adding a term. `tsys-atm` takes the same
path in `setnoise` (the `ANoise` is created before either Tsys mode's
checks; read from the source, not measured), and casa-rs refuses it the
same way.

## `tsys-atm` noise takes its atmosphere half a band below each channel

- Date noted: 2026-10-24
- Status: CASA bugs; casa-rs does not follow them (#841 Phase 4c)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`AtmosCorruptor::initAtm`, `tsys`, `opac`) and
  `synthesis/MeasurementComponents/SolvableVisCal.cc` (`setSimulate`)

`setnoise(mode='tsys-atm')` scales a unit `ANoise` by a `Tf` term ("TF
NOISE") whose Tsys comes from ATM for each channel:
Tsys(A, c) = (hν/k) e^(τA) (1/(e^(hν/k/T_ebb(A)) − 1) + T_rx/(hν/k)) (1 + g),
τ the zenith opacity and T_ebb the sky's equivalent blackbody temperature
at airmass A, with hν/k from the channel's frequency (`casa_simulation`'s
`corrupt::tsys_atm` has the whole model). Measured on CASA 6.7.6 (ALMA's
site defaults, 1 mm of water, 64-channel windows of 100 MHz from 180 GHz,
across the 183 GHz line, and from 321 GHz, 2 s integrations; the probe
scripts and results are on the NAS under `casa-rs/issue-841-tsys-4c`),
every channel's noise variance agrees with that formula, through
`casa-atmosphere`, with no free parameter (ratio 1.0000 and 1.0027, rms
0.0106 and 0.0100 per channel, against a statistical 1σ of 0.0149) once the
atmosphere is taken at the frequencies CASA gives ATM, which are not the
channels':

- **The atmosphere is half a band below the channel.** `initAtm` builds
  each window's ATM grid from `REF_FREQUENCY`, which the simulator sets to
  the first channel's frequency, with the reference at the middle channel:
  ATM's channel i is at `REF + (i − n/2)·Δν` for the first window and
  `REF + (i − n/2 − 1)·Δν` for the others, and `opac` and `tsys` read
  channel c of that grid. So channel c's τ and T_ebb are those of a
  frequency half the band below it (3.2 GHz for the probe's windows), while
  hν/k is the channel's own. Against the atmosphere at each channel's own
  frequency, CASA's variance across the 183 GHz line is off by a mean factor
  of 6.1 and up to 103; at 321 GHz by 0.39 on average and up to 5.5. The
  shift grows with the window's width and matters wherever the atmosphere
  changes across it: the 183 GHz line's brightness changes by tens of
  kelvin over a few GHz at a 5,000 m site (Thompson, Moran & Swenson,
  *Interferometry and Synthesis in Radio Astronomy*, 3rd ed., p. 746,
  Fig. 13.21), and an ozone line at 231.7 GHz nearly doubles ALMA's system
  temperature within a band (B. Svoboda, *Special Topics: Spectral Lines*,
  NRAO Synthesis Imaging Workshop 2026, slide 18).
- **A one-channel window after the first gets no Tsys.** For n = 1 the
  later windows' channel map is read past its end, which is undefined
  behaviour. In CASA 6.7.6 on macOS the index it read pointed past the grid
  (the log reads "ch 972849296/20"); the term's `T` came out bad,
  `setMatByOk` applied it as 1, and the probe's 2 GHz continuum window at
  230 GHz got unscaled 1 Jy noise (variance 0.996 Jy²). Another build may
  read something else.

casa-rs evaluates ATM at each channel's CHAN_FREQ, for every window and a
one-channel window alike, so a channel's σ follows the sky it observes. As
for `tsys-manual`, each row takes its own integration's airmasses and
EXPOSURE ([stale and borrowed system
temperatures](#tsys-manual-noise-takes-stale-or-borrowed-system-temperatures));
the term is also refused when its zenith Tsys at the first window's centre
is not a number, which CASA's check (> 10⁶ K or ≤ 0) lets through.

## `settrop` turns the phase by the dispersive wet delay alone

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 4d)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`AtmosCorruptor::cphase`)

`sm.settrop` adds a `Tf Jones` term whose value for antenna a in channel c
is `exp(i·δ_a·φ(c))`: δ the fractional excess of water vapour over `pwv`
above the antenna (fractional Brownian motion of rms `deltapwv`) and φ
ATM's zenith wet phase delay for `pwv` (`casa_simulation`'s
`corrupt::troposphere` has the whole model). `cphase` takes φ from
`SkyStatus::getDispersiveH2OPhaseDelay`: the *dispersive* part alone, the
refraction the water lines add. Measured on CASA 6.7.6 (seven antennas, 1 mm
of water, 64-channel windows of 100 MHz from 180 GHz, across the 183 GHz
line, and of 15.625 MHz at 230 GHz; `tools/simulation/sm_trop_scenarios.json`,
probes on the NAS under `casa-rs/issue-841-trop-4d`), each row of CASA's
table is one δ times that dispersive delay (on CASA's grid, below) to
2.2·10⁻⁵ (single precision), where the whole wet delay at the channels'
frequencies leaves δ spread across a row by 0.17 of itself (median).

The path a column of water vapour adds is mostly its non-dispersive part,
the refractivity of the water molecule's permanent dipole: `ℒ_V ≃ 6.3w`,
"an excellent approximation for frequencies below 100 GHz", rising to
about 8w in the windows above (Thompson, Moran & Swenson, *Interferometry
and Synthesis in Radio Astronomy*, 3rd ed., eq. 13.19, p. 702). At
`settrop`'s site ATM gives 7.0 mm of path per mm of water at 100 GHz, and
the whole wet delay is 940, 119, 36, 23 and 8.5 times the dispersive part
at 43, 100, 183, 230 and 345 GHz. With `settrop`'s defaults (3 mm of water
fluctuating by 15 %), CASA's phases have an rms of 3° at 100 GHz and 39° at
230 GHz where a fluctuation that size in the water column turns the phase
by about 380° and 910°.

casa-rs takes the whole wet delay, the dispersive and non-dispersive parts
together, so `deltapwv` means the fluctuation of the water column it
names. With CASA's defaults the phases are then fully scrambled at
millimetre wavelengths; a `deltapwv` of 1 % of 3 mm (30 µm of water) turns
them by about 60° rms at 230 GHz.

## `settrop` takes its atmosphere half a band below each channel

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 4d)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`AtmosCorruptor::initAtm`, `cphase`)

`settrop`'s delays come from the same ATM grid as `tsys-atm`'s noise, and
`cphase` reads channel c of it through the same channel map: each
channel's φ is the atmosphere's half a band below it, and a one-channel
window after the first reads the map past its end ([`tsys-atm`'s
atmosphere half a band
away](#tsys-atm-noise-takes-its-atmosphere-half-a-band-below-each-channel)).
In the Phase 4d probe's CASA table, δ (a row's phase over the dispersive
delay) is one value across the row to 2.8·10⁻⁶ of itself with the delay on
that grid, and spreads by 2.5 of itself across the 183 GHz line with the
delay at the channels' own frequencies. casa-rs evaluates ATM at each
channel's CHAN_FREQ.

## `settrop`'s screen has an anisotropic spectrum

- Date noted: 2026-10-24
- Status: CASA bugs; casa-rs does not follow them (#841 Phase 4d)
- Affected code: `synthesis/MeasurementComponents/CalCorruptor.cc`
  (`fBM::initialize`, two dimensions)

`settrop(mode='screen')` blows one two-dimensional screen of fractional
Brownian motion across the array. `fBM::initialize` fills a half spectrum
`F2(i, j)`, i along the wind (the half axis) and j across it, with
amplitudes `|(i, j)|^−(β+½)/2` times a normal draw (its comment: "still
something not quite right with exponent"; an earlier line, left commented
out, had `−(β+1)/2`):

- **The wavenumbers are indices.** The screen is long along the wind
  (CASA's probe screen was 1,359 by 271 pixels), so an index along it is a
  fifth of the wavenumber of an index across it: the spectrum is stretched
  along the wind, and the screen is not isotropic.
- **The full axis is not wrapped.** j runs to the screen's width, where
  the indices past half of it are negative wavenumbers; taking them as
  large ones gives the negative wavenumbers the smallest amplitudes.
- **The exponent.** A two-dimensional fractional Brownian motion whose
  cuts have the spectral exponent β (as `individual` mode's series, and
  `beta`'s documentation, have it) has amplitudes `|k|^−(β+1)/2`.
- **Out of bounds.** It sets the imaginary parts of `F2(s0/2, 0)` and
  `F2(s0/2, s1/2)`, one past the end of the half axis's `s0/2` entries, and
  a half axis of `s0/2` entries transforms to `s0 − 2` real pixels along
  the wind, of which it copies `s0`: the last two from past the end of the
  output, as the one-dimensional series do ([gain screens read past their
  transform](#casas-gain-screens-read-past-their-transform)). CASA's own
  `settrop` test is skipped for "intermittent failures".

Isotropic turbulence's phase spectrum depends only on the spatial
frequency's size, `|q|^−11/3` in two dimensions (Thompson, Moran & Swenson,
3rd ed., sec. 13.1.7, p. 725). casa-rs draws `|k|^−(β+1)/2` on the
wavenumbers in cycles per pixel, the full axis wrapped, with the conjugate
pairs and real bins the real transform needs on the half axis's first and
Nyquist columns (drawn apart, those modes come out at half the power), and
transforms every pixel: on a screen five times as long as wide its
structure function is the same along the wind and across it, where CASA's
differ by five, and its boundary modes have their interior neighbours'
power (`corrupt::troposphere`'s unit tests).

## `settrop` with `simint` stamps a solution halfway to its interval's start

- Date noted: 2026-10-24
- Status: CASA bug; casa-rs does not follow it (#841 Phase 4d)
- Affected code: `synthesis/MeasurementComponents/SolvableVisCal.cc`
  (`setSimulate`)

With `simint` longer than an integration, `setSimulate` iterates the
MeasurementSet in intervals of `simint` and keeps a solution for each
integration, stamped `(t₀ + t)/2`, t₀ the interval's first integration's
time and t its own, and takes the screen's slot nearest that stamp. Measured
on CASA 6.7.6 (`simint=6` on 2 s integrations): the table's times are 0, 1,
2, 6, 7, 8, … s for integrations at 0, 2, 4, 6, 8, 10 s, and `corrupt`,
applying the solution nearest each row's time, gives an interval's later
integrations a slot from earlier in it. casa-rs stamps each solution with
its integration's time and takes the slot nearest it, as CASA does when
`simint` is not set (each integration its own interval). The slots are
`simint` wide either way.

## ATM's HCN catalogue misplaces a line at 106 GHz

- Date noted: 2026-10-24
- Status: ATM data error; casa-rs corrects it (#841 Phase 4,
  `casa-atmosphere`; owner decision, 2026-10-24)
- Affected code: `atmosphere/ATM/ATMRefractiveIndex.cc:796`
  (`mkSpecificRefractivity_hcn`, `fre[18]`)

HCN's rotational lines are spaced by about 88.6 GHz (J=1–0 at 88.631 GHz,
2–1 at 177.260, 3–2 at 265.886; *Synthesis Imaging in Radio Astronomy II*,
printed p. 568, PDF p. 592): the table runs 88.63,
177.26, …, 974.49 GHz, then gives the J=12–11 line as `106.298069519`
before continuing at 1151.45 GHz. The line belongs at 1062.98 GHz (as the
JPL catalogue has it, `c027001.cat`); the decimal point moved. ATM
therefore sums a J=12–11-strength HCN line at 106.3 GHz and none at
1063 GHz. HCN is a trace gas (a few 1e-4 ppmv), but near 106.3 GHz the
misplaced line is not negligible: with the default Chajnantor profile, the
compiled C++ gives a zenith opacity of 0.029368 at 106.298 GHz, of which
HCN is 4.5·10⁻⁴; with the line moved to 1062.98 GHz it gives 0.028919, HCN
1.3·10⁻⁷, so the total is 1.5 % high there (measured in the #894 review).
Away from 106.3 GHz the line's wings fall off (in `at_tool_check.py`'s
bands the correction moves the dry opacity by at most 3.5·10⁻⁵ and Tebb by
1.3·10⁻⁵, at 90 GHz), and at 1063 GHz the missing line is lost among
water's.

casa-rs moves the decimal point back: the line is at 1062.98069519 GHz.
The correction is one of four in `tools/atmosphere/atm-corrections.patch`,
which `extract_atm_tables.py` applies before it generates the port's tables
and `build-atm-oracle.sh` before it compiles the C++ the tests compare
with. `at_tool_check.py` shows the C++ built without the patch equals
CASA's `at` tool (to 7.5·10⁻¹⁵) and the port equals the C++ built with it
(to 8.1·10⁻¹⁵), so the port differs from the `at` tool by the four
corrections alone.

## ATM's reference air densities drop a digit at sea level

- Date noted: 2026-10-24
- Status: ATM data error; casa-rs corrects it (#841 Phase 4; owner
  decision, 2026-10-24)
- Affected code: `atmosphere/ATM/ATMProfile.cc:1360`, `:1367`, `:1374`,
  `:1381`, `:1388` (`st76`, `den[6][50]`)

`st76` converts the minor gases' mixing ratios to number densities with a
tabulated air density. For the mid-latitude, subarctic and US Standard
atmospheres the sea-level entry has lost its leading digit: `.496E+19`,
`.711E+19`, `.549E+19`, `.855E+19`, `.548E+19` cm⁻³ where the next entries
(2.26e19 and so on at 1 km) and the tropical profile's 2.450e19 show about
2.5e19 was meant. The parabola `st76` fits through 0, 1 and 2 km then
falls far below the true density near the ground: in the committed
sea-level mid-latitude summer case, restoring the digit raises the first
layer's interpolated density 4.075 times (#894 review). So O3, N2O, CO,
NO2, SO2, HCl and HCN are up to about four times too scarce in layers whose
middle is below 1 km. Only sea-level-to-1-km sites with those atmospheres
see it, and those gases' columns lie mostly higher.

casa-rs restores the digit: 2.496e19, 2.711e19, 2.549e19, 2.855e19 and
2.548e19 cm⁻³. Air density falls with height, by about e⁻¹ in a dry-air
scale height of 8 km (*Interferometry and Synthesis in Radio Astronomy*,
3rd ed., printed pp. 661 and 668, PDF pp. 700 and 707), and the restored
values fall by 0.88 to 0.91 to
their 1 km entries, as the tropical profile's untouched 2.450e19 does; the
typo's values rise 3 to 5 times instead. The US Standard Atmosphere's
sea-level 1013.25 hPa and 288.15 K give 2.547e19 by the ideal gas law. In
`at_tool_check.py`'s humid sea-level atmosphere (mid-latitude summer) the
correction raises the O3, CO and N2O line opacities by up to 1.8 %, 7.7 %
and 8.5 % and the dry opacity by up to 0.22 % (at 690 GHz); Tebb moves by
at most 1.2·10⁻⁵. Sites at 1 km and above do not change.

## ATM weights H2(17)O by the (18)O abundance

- Date noted: 2026-10-24
- Status: ATM bug; casa-rs corrects it (#841 Phase 4; owner decision,
  2026-10-24)
- Affected code: `atmosphere/ATM/ATMRefractiveIndex.cc:449`
  (`mkEarthSpecificRefractivity`, `ratiohh17o`)

`ratiohh17o = abun_18o/(1+abun_18o+abun_17o+2.0*abun_d)` uses the (18)O/(16)O
ratio (0.0020439) where the (17)O ratio (0.000375) belongs, so H2(17)O lines
are 5.45 times too strong. ATM's own (17)O fractions for oxygen and ozone
use `abun_17o`. casa-rs does too. Over `at_tool_check.py`'s atmospheres and
bands the correction lowers the wet opacity by up to 0.61 % (at 990 GHz)
and Tebb by up to 0.087 % (at 460 GHz), and changes the dispersive wet
phase by up to 4 % where it is small (at 380 GHz); at Chajnantor the zenith
opacity at 230 GHz moves by 2·10⁻⁴.

## ATM's HDO weight divides by three

- Date noted: 2026-10-24
- Status: ATM typo; casa-rs corrects it (#841 Phase 4; owner decision,
  2026-10-24)
- Affected code: `atmosphere/ATM/ATMRefractiveIndex.cc:450`
  (`mkEarthSpecificRefractivity`, `ratiohdo`)

`ratiohdo = 2.0*abun_d/(1+abun_18o+abun_17o+2-0*abun_d)` reads `2-0*abun_d`
(that is, 2) where every sibling ratio has `2.0*abun_d`. The denominator is
3.0024 instead of 1.0030, so HDO lines are about three times too weak.
casa-rs uses `2.0*abun_d`. Of the four corrections this one matters most:
at Chajnantor the zenith opacity rises 0.32 % at 230 GHz (near HDO's
225.9 GHz line) and 0.15 % at 90 GHz (near its 80.6 GHz line), with Tebb
0.24 % and 0.10 % higher; over `at_tool_check.py`'s atmospheres and bands
the wet opacity rises by up to 1.8 % and Tebb by up to 0.47 % (both at
850 GHz).

## ATM's HCN profile stops at 100 km

- Date noted: 2026-10-24
- Status: ATM data gap; casa-rs follows it (#841 Phase 4): there are no
  values to restore, and the air above 90 km (below 0.002 hPa) is about
  2·10⁻⁶ of the column
- Affected code: `atmosphere/ATM/ATMProfile.cc:1429` (`st76`, `hcn[50]`)

`hcn[50]` is initialised with 45 values, so C++ zero-fills its last five
levels (100 to 120 km) and the interpolated HCN falls to zero over them. A
layer whose middle is above 90 km already interpolates toward the zero at
100 km: the tropical fixture keeps, under a 95 km top, a layer with its
middle at 90.8 km and its top at 94.1 km (#894 review). So profiles topped
above about 90 km see the gap; `tools/atmosphere/extract_atm_tables.py`
reports it whenever it regenerates the tables.

## Copying an AtmProfile copies its top pressures as bottom pressures

- Date noted: 2026-10-24
- Status: ATM bug; no effect on what casa-rs ports
- Affected code: `atmosphere/ATM/ATMProfile.cc:567` (copy constructor)

The copy constructor fills `v_layerPressure0_` from `a.v_layerPressure1_`.
`RefractiveIndexProfile` (and so `SkyStatus`) copies its `AtmProfile` this
way, so their `getLayerBottomPressure` returns each layer's top pressure and
`setAltitude` would take a wrong ground pressure. No opacity, path or
brightness calculation reads bottom pressures. `casa-atmosphere` keeps one
profile and never copies it through such a path.

## ATM leaves the ozone isotopologue layers unpadded

- Date noted: 2026-10-24
- Status: latent ATM bug; does not arise with ATM's own profiles
- Affected code: `atmosphere/ATM/ATMRefractiveIndexProfile.cc:723–725`
  (`mkRefractiveIndexProfile`)

For a layer without ozone, `mkRefractiveIndexProfile` pushes a zero onto the
total-ozone vector only. The eight ozone state and isotopologue vectors get
nothing, so they become shorter than the layer count and later layers' values
shift down one place. ATM's standard profiles have ozone in every layer, so
this needs a user profile with zero ozone.

## ATM omits (16)O(17)O

- Date noted: 2026-10-24
- Status: deliberate in ATM; casa-rs follows it
- Affected code: `atmosphere/ATM/ATMRefractiveIndex.cc:3252`
  (`mkSpecificRefractivity_16o17o`)

The (16)O(17)O function returns zero before its line loop. The comment says
the isotope was removed in 2024 while its catalogue is reviewed, because it
gave lines that do not exist. The port returns zero too and keeps the
catalogue in its tables for the record.

## RefractiveIndexProfile's averaged non-dispersive wet phase reads a channel as a water column

- Date noted: 2026-10-24
- Status: ATM bug; casa-rs does not follow it (#841 Phase 4b)
- Affected code: `atmosphere/ATM/ATMRefractiveIndexProfile.cc:2466`
  (`getAverageNonDispersiveH2OPhaseDelay(const Length &, unsigned int)`)

The window average calls `getNonDispersiveH2OPhaseDelay(v_transfertId_[spwid]
+ i)`. `RefractiveIndexProfile` has no overload taking a channel alone, so
the index converts to a `Length` in metres, and the call returns channel 0's
phase for a water column of 0, 1, 2 … metres. The average is then scaled
again by the requested column. For the simulator's Chajnantor atmosphere and
1.3 mm of water this gives 306,132° in a 22 GHz window, where the channels'
mean is a few degrees (`casa-atmosphere`'s `oracle_absorption` test
reproduces ATM's number exactly). CASA's `at` tool does not reach this
function: its averages go through `SkyStatus`, whose override is correct.
`casa-atmosphere` takes the mean of the channels' phases.

## VisCalSolver2 fails a solve whose chi-square is exactly zero

- Date noted: 2026-10-24
- Status: CASA behaviour casa-rs does not follow (#903, #905)
- Affected code: `synthesis/MeasurementComponents/VisCalSolver2.cc:180–185`
  (and `:353–358`, the other `solve` overload), CASA 6.7.5.18

Each Levenberg-Marquardt iteration recomputes chi-square and, if it is
exactly `0.0`, prints "CHI2 IS SPURIOUSLY ZERO!" and returns `False`, so
`Calibrater::genericGatherAndSolve` records no solution for the interval
(and writes no table when no interval solves). Chi-square is exactly zero
only when the current gains reproduce every unflagged sample to the last
bit, which in practice means noise-free synthetic data whose first guess is
already the answer. That is an exact fit, not a failure; CASA's oracle runs
on the casa-rs gain fixtures had to add noise to get a table at all. casa-rs's `solve_graphs` keeps an exact fit as a solution. A
chi-square that is small but not zero follows CASA's convergence rule
unchanged, so noise-free data that converge only linearly can still reach
CASA's 50-iteration limit and fail as CASA's would.

## `calc_pointings2`'s ALMA tiling reads an undefined `pbcoeff`

- Date noted: 2026-10-26
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5a)
- Affected code: `casatasks/src/private/simutil.py:611` (`calc_pointings2`)

For `maptype='ALMA'` (and its aliases `alma`, `ALMA-OT`), when the caller
gives no primary beam (`beam <= 0`), `calc_pointings2` defaults it to
`spacing_asec*pbcoeff*pl.sqrt(3)`. `pbcoeff` is a local of
`task_simobserve.py`, not defined in `simutil.py`, so the call raises
`NameError` (CASA 6.7.6). `simobserve` reaches it with an ALMA map and no
antenna list, and any script that calls `calc_pointings2` directly without
`beam` does too. casa-rs uses `simobserve`'s value, 1.13, as the default
clearly intends (`casa_simulation::simutil::pointings::PBCOEFF`); the
pointings are then those of the beam `spacing × 1.13 × √3`.

## `noisetemp` reads the wrong receiver temperature at the edges of its tables

- Date noted: 2026-10-26
- Status: CASA bugs; casa-rs does not follow them (#841 Phase 5a)
- Affected code: `casatasks/src/private/simutil.py:1124–1134` (`noisetemp`)

`noisetemp` looks the receiver temperature up with
`while(f0[z]<obsfreq and z<len(t0)): z+=1` and then `t_rx=t0[z-1]`:

- **At the first band edge.** A frequency at or above the table's lower
  limit but not above its first edge leaves `z = 0`, and `t0[-1]` is
  Python's last entry. ALMA, ACA and total power at exactly 35 GHz get
  230 K (band 10's) instead of 28 K, the (E)VLA from 0.8 to 1 GHz 28 K (Q
  band's) instead of 10 K, and the SMA at 180 GHz 500 K instead of 67 K.
  casa-rs takes the first temperature there.
- **Above the last edge of an unknown telescope's table.** For a telescope
  `noisetemp` does not know, the frequency edges and temperatures are lists
  of the same length (`[10, 900]`, `[200, 200]`), so above 900 GHz the
  test reads `f0[2]` before checking `z` and raises `IndexError` (CASA
  6.7.6, `WSRT` at 950 GHz). casa-rs keeps the last temperature, 200 K.
  For the telescopes it knows, the edges have one more entry than the
  temperatures and the test does not overrun.

The efficiencies are unchanged. `tools/simulation/simutil_oracle.py` records
both cases, and `tests/simutil_casa.rs` checks that casa-rs departs exactly
there.

## `direction_splitter` cannot split a direction without an epoch

- Date noted: 2026-10-26
- Status: CASA bug; casa-rs refuses the input (#841 Phase 5a)
- Affected code: `casatasks/src/private/simutil.py:876–894`
  (`direction_splitter`)

`direction_splitter` keeps the epoch only when a direction has three
fields, but reads the longitude from field 1 and the latitude from field 2
whatever the count. A direction without an epoch (`'18h00m00 -23d00m00'`)
therefore raises `IndexError`, and a latitude its regular expression
`([+-]?\d+).(\d+).(\d+\.?\d*)` does not match raises `AttributeError`
(CASA 6.7.6). casa-rs refuses both with "can't interpret ... as a
direction", the message `isdirection` gives, so a `simobserve` that is given
such a direction stops with a clear error rather than a traceback. A
direction with more than three fields is split as CASA splits it (no epoch,
fields 1 and 2).

## `modifymodel` swaps axes into place and refuses a cyclic order

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5b)
- Affected code: `casatasks/src/private/simutil.py:2951–2971`
  (`modifymodel`)

`modifymodel` finds which of the model's axes is each of the four it
writes (direction, direction, Stokes, spectral) and then moves them with
`for ax in range(4): if axmap[ax] != ax: arr = arr.swapaxes(ax, axmap[ax])`,
marking both axes done after each swap. That orders a model whose axes are
a transposition of the four (the FITS order, spectral before Stokes), but
not a cyclic one: a model with its spectral axis first (`[spectral, RA,
Dec, Stokes]`) is left out of order, and the check before it raises
"internal error: I'm confused about the shape if your model data cube"
(CASA 6.7.6), with the new image already created and empty. casa-rs
permutes the axes, so any order of the four is written as
`[RA, Dec, Stokes, spectral]`; `tests/skymodel_casa.rs` checks the
cyclic case pixel by pixel.

## `modifymodel` labels the new image's Stokes planes by their count

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5b)
- Affected code: `casatasks/src/private/simutil.py:2838–2912`
  (`modifymodel`)

`modifymodel` returns the model's Stokes labels (`model_stokes`), but the
image it writes takes its Stokes axis from `ia.fromshape`, whose default
coordinate system labels the planes by their count: I, IQ, IQU or IQUV
(`CoordinateUtil::addStokesAxis`). So:
- a model labelled I and V comes out labelled I and Q, the V data then read
  as Stokes Q;
- three unlabelled planes come out IQU, though `modifymodel` warns
  "setting IQV Stokes parameters from the 4th axis of you model" and
  returns `"IQV"`.

casa-rs keeps the model's labels and labels unlabelled planes as
`modifymodel` reports them (IQV for three). Models of Stokes I alone, the
usual case, are unaffected. A model labelled in a circular or linear basis
(RR and LL, or XX and YY) also keeps its labels, so `flatimage` (which
wants a Stokes I plane) and adding components (which wants I, Q, U or V)
refuse it, where CASA relabels it I and Q and goes on; `simobserve`'s sky
model is a brightness in Stokes parameters.

## `modifymodel` indexes the map it makes of two cells

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5b)
- Affected code: `casatasks/src/private/simutil.py:2631–2632`, `2720`
  (`modifymodel`)

Given `incell` as a list of two cells, `modifymodel` converts them with
`incell = map(qa.convert, incell, ['arcsec', 'arcsec'])` and later reads
`incell[0]`. Under Python 3 `map` returns an iterator, and the index raises
`TypeError: 'map' object is not subscriptable` (CASA 6.7.6). casa-rs takes
the two cells, converted to arcsec and used as given (signs included), as
the code did under Python 2. A single cell is unaffected: it becomes
`[−cell, cell]`.

## An unset `compwidth` gives the component-only model a meaningless spectral increment

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5b)
- Affected code: `casatasks/src/private/task_simobserve.py:506`

With a component list and no sky image, `simobserve` observes a bandwidth
of `compwidth`, or 2 GHz when `compwidth` is not a quantity, but sets the
`.compskymodel` image's spectral increment with
`modelcsys.setincrement(type="spectral", value=compwidth)` whatever it is.
For `compwidth=""`, `coordsys.setincrement` puts a meaningless value there:
in CASA 6.7.6 the model's right ascension in radians, read as hertz
(4.976 Hz for RA 19h). casa-rs uses the 2 GHz `simobserve` observes with.
The increment is the model's only spectral information beyond its
reference frequency, since the model has one channel.

## `ia.modify` converts a component spectrum through the image's default measures frame

- Date noted: 2026-10-27
- Status: CASA behaviour; casa-rs does not follow it (#841 Phase 5b)
- Affected code: `casatools/src/code/imageanalysis/Images/ComponentListImage.cc`
  (`_cacheCoordinateInfo`), `components/ComponentModels/SpectralModel.cc:100–116`
  (`refFreqInFrame`)

When `ia.modify` adds a component list to an image, each spectral model
converts its reference frequency into the image's spectral frame
(`refFreqInFrame`) through the measures frame of the image's spectral
coordinate. An image made from a shape (`ia.fromshape`, as `modifymodel`
and `simobserve` make theirs) has that frame at its defaults: the
geocentre, an epoch of 0, and the J2000 pole, the same defaults as
[the measures tool's](#the-measures-tool-converts-with-a-frame-at-mjd-0-and-the-geocentre).
So a TOPO spectrum on an LSRK image, or LSRK spectra on a TOPO image, are
shifted by a velocity no observation has, about 1e-4 of the frequency in
CASA 6.7.6's oracle, which a spectral index turns into a flux difference of
about 1e-4.

casa-rs converts as CASA does when the image's conversion layer carries a
real frame: an epoch and an observatory position (and the direction). The
oracle's LSRD image, with an LSRK conversion layer at the VLA on
2025-06-01, agrees with CASA to 1e-6 (a 1 Jy point at the reference
frequency stays 1 Jy). With no conversion layer, or the placeholder (an
epoch of 0 at the geocentre), casa-rs takes the frequencies as they are:
a meaningful conversion would need the observation's epoch and position,
which such a sky model does not carry.

Components in the image's own frame, the usual case, are unaffected. The
oracle's matched-frame cases (LSRK spectra on an LSRK cube, TOPO spectra
on a TOPO cube) agree with CASA to 1e-6, where the same cube with spectra
in the other frame and only the placeholder differs by 1.1e-4.

## simobserve's calibrator: the last pass, its scans and two component lists

- Date noted: 2026-10-27
- Status: CASA bugs; casa-rs does not follow them (#841 Phase 5c)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6:
  the observing loop, `sm.setdata(fieldid=range(0,nfld))`, and the
  `complist+","+cal.cclist` joins); `Simulator::createSkyEquation`

With a phase calibrator (`caldirection` and `calflux`), CASA 6.7.6's
`simobserve` has three faults. Each was measured by
`tools/simulation/simobserve_parity.py`'s CASA runs.

**The last pass.** The observing loop calls `observemany` for the map's
pointings only when a pass over them completes, and observes the
calibrator after it. Pointings of a pass that `totaltime` ends inside are
collected but never observed. With `totaltime='300s'` over a 105-pointing
map at 10 s, CASA's MeasurementSet holds the calibrator's one scan and no
pointing at all. casa-rs observes the pointings the time allows, as CASA
does without a calibrator. With `totaltime` a whole number of passes,
the two schedules are the same.

**The calibrator's scans.** Before predicting, the task selects the map's
fields, `setdata(fieldid=range(0,nfld))`. The calibrator's field is `nfld`,
so its scans keep the zeros `observe` wrote; noise is then added to them.
casa-rs predicts every scan, the calibrator's included.

**Two component lists.** With a `complist` and a calibrator, the task
predicts from `complist + "," + <stem>.cal.cclist`. `createSkyEquation`
opens that string as one table, which does not exist ("ComponentList …
not readable"), so `predict` returns `False`. The task does not check it:
the MeasurementSet is left unpredicted, every visibility zero, for a model
image with a list or for a list alone. casa-rs predicts both lists, as one
list of their components (`PredictSpec::component_lists`).

The parity cases `calibrator` and `calibrator_lists` compare casa-rs with
CASA's own simulator doing what the task meant: CASA's MeasurementSet
predicted again over every field, from the model image and the lists
merged into one. DATA agrees to an NRMS of 5.3e-9 and 5.5e-9.

## simobserve leaves `<stem>.noisier.T.cal`

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5c)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  `setnoise(..., table=noisymsroot)` and the clean-up)

`tsys-atm` noise is set with `table=<noisy root>`, so CASA writes the `T`
term's table beside the noisy MeasurementSet. The task's clean-up deletes
`<stem>.noisy.T.cal` and `<stem>.noisy.sd.T.cal`, but not
`<stem>.noisier.T.cal`, the one written when leakage comes first. The table
is a by-product the task means to remove; casa-rs writes no Tsys table for
any of the three.

## simobserve's observing loop never ends for pointings of no length

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs refuses the input (#841 Phase 5c)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  the `while (sttime-scanstart) < totalsec` loop)

The loop advances the time by each pointing's scan length. When the
lengths of a pass sum to zero, as they do for `integration='0s'` or a
pointing file of zero times, the time never advances and CASA loops
forever, collecting pointings. casa-rs refuses such a schedule with a
message.

## `simutil.ismstp` takes any zero UVW value for total power

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 5c)
- Affected code: `casatasks/src/private/simutil.py` (`ismstp`)

When `simobserve` corrupts an existing MeasurementSet (`obsmode=''`), it
asks `ismstp` whether the data are total power. For a MeasurementSet whose
first antenna's name has no `TP` and which has more than one antenna, the
test is `uvw.all() == 0.`. That is true when any UVW value is zero, not
when all are. An interferometer's MeasurementSet with one zero coordinate
is then corrupted as a single dish's, with the wrong file names and
sensitivity constant. casa-rs tests whether every value is zero.

## simobserve deletes a sky model that is the model it writes

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs refuses the input (#841 Phase 5c)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  the `newmodel` set-up)

The task writes `<project>/<stem>.skymodel`, or `<stem>.newmodel` when
`skymodel` is the bare name `<stem>.skymodel`. Given the full path
`<project>/<stem>.skymodel`, as a rerun on an earlier run's model would
give it, the output is the input: with `overwrite=True`, the default, CASA
deletes it and then fails to read it. casa-rs refuses it, and the model is
left in place.

## simobserve nests a project path inside itself

- Date noted: 2026-10-27
- Status: CASA limitation; casa-rs departs (#841 Phase 5c)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  `fileroot + "/" + project`)

The task makes the directory `project` and names its files
`fileroot + "/" + project + "." + <configuration>`, so a `project` given
as a path, `a/b`, names its files `a/b/a/b.<cfg>…`, in a directory that does
not exist. Only a plain name in the working directory works. casa-rs names
the files by the directory's last component, `a/b/b.<cfg>…`. For a plain
name the two are the same.

## simobserve writes through a symbolic link in the project

- Date noted: 2026-10-27
- Status: CASA limitation; casa-rs departs (#841 Phase 5c, #920 review)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  `shutil.copyfile(ptgfile, ...ptg.txt)`, `util.write_pointings` after
  `os.path.exists`, and the `shutil.rmtree` calls before each product is
  written)

`overwrite=True` replaces the project's files without asking. CASA writes
`<stem>.ptg.txt` through a symbolic link there in two ways:
- a pointing file copied from `ptgfile` replaces the file outside the
  project that the link points to (`shutil.copyfile` follows it);
- laid-out pointings over a dangling link create the file the link names
  outside the project. `os.path.exists` follows the link, finds nothing to
  remove, and `open(..., 'w')` follows it.

A link to a table that it replaces stops the task instead, since
`shutil.rmtree` refuses links. Its existence checks follow links, so a
dangling link does not count as an existing file under `overwrite=False`.

casa-rs replaces a link in the project with the new file or table and
leaves what the link points to as it was. A link counts as an existing file
under `overwrite=False`, dangling or not. The project's own files are the
only ones `overwrite` replaces (the owner's decision of 2026-10-26, recorded
with the provider contracts' overwrite exception).
