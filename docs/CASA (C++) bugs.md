# CASA (C++) bugs

Notes on bugs or likely bugs observed while doing Rust/C++ parity work against CASA.

Known imaging parity defect: [CASA Högbom `niter` off-by-one](#casa-hogbom-niter-off-by-one-bug).
CASA behaviours casa-rs follows for parity:
[AW-projection hand pairing](#aw-projection-pairs-the-partner-hands-visibility-with-the-conjugate-baseline-cell),
[AW-projection prediction w](#aw-projection-predicts-with-the-unrotated-w),
[AW MT-MFS restart weights](#aw-mt-mfs-restart-multiplies-saved-higher-taylor-weights-again),
[MatrixCleaner's stop iteration](#matrixcleaner-counts-the-iteration-that-stops-it),
[exact peak comparisons](#peak-searches-let-rounding-choose-between-equal-pixels),
[multi-term weight normalization](#the-multi-term-weight-normalization-override-is-never-called),
[a Taylor term's sum of weights per Stokes plane](#a-taylor-term-gives-every-stokes-plane-the-first-correlations-sum-of-weights),
[a Taylor run's `'common'` beam](#restoringbeamcommon-restores-every-taylor-stokes-plane-with-the-first-planes-beam),
[the Airy beam's blockage term](#the-airy-beams-blockage-term-uses-the-inverse-length-ratio),
[the EVLA's 11052 MHz fit](#the-evlas-beam-table-has-no-11052-mhz-fit),
[the simulator's and tclean's two mosaic machines](#the-simulators-mosaic-machine-is-not-tcleans),
[a moved pointing phased on its field](#smpredict-phases-a-moved-observemany-pointing-on-its-field),
[`datacolumn='corrected'` without `CORRECTED_DATA`](#tclean-images-data-when-corrected_data-is-missing-and-says-nothing),
[W projection's sectioned grid edge](#w-projection-grids-a-support-the-grids-edge-cuts-and-never-its-last-row-and-column).
CASA behaviours casa-rs does not follow:
[a Gaussian beam fit to collinear PSF samples](#a-gaussian-beam-fit-to-collinear-psf-samples-is-unstable),
[non-frequency component frequency units](#a-non-frequency-component-unit-corrupts-casas-spectrum),
[nonconforming component flux conversion](#component-flux-conversion-can-relabel-incompatible-units-or-crash),
[duplicate and empty component removals](#component-removal-can-lose-data-or-crash),
[component labels containing invalid UTF-8](#casa-accepts-component-labels-it-cannot-return),
[cyclic component-list type checks](#cyclic-component-list-type-checks-crash-casa),
[complex quantity-array values](#casa-discards-the-values-of-bare-complex-quantity-arrays),
[bare complex64 quantity values](#casa-discards-bare-numpy-complex64-quantity-values),
[automatic-mask convolution on rectangular pixels](#automatic-mask-convolution-requires-square-pixels-in-casa),
[byte order of integer quantity arrays](#casa-misreads-big-endian-integer-quantity-arrays),
[a Taylor run with no weight on its first correlation](#with-no-weight-on-the-first-correlation-every-taylor-plane-is-zero),
[odd-size scale convolutions](#multiscale-and-multi-term-scale-convolutions-shift-by-a-pixel-on-odd-image-sizes),
[MatrixCleaner's step residual](#matrixcleaners-step-residual-forgets-earlier-steps),
[MEM masks on tiled images](#mem-reuses-the-first-mask-tile-for-its-alpha-update),
[an unplaceable PB pointing](#the-analytic-pb-ignores-a-failed-topixel),
[an unused dish choosing the PB](#an-unused-alma-antenna-can-choose-the-standard-primary-beam),
[the cube density's two uvw frames](#the-cube-briggs-weightor-grids-and-looks-up-its-density-in-different-uvw-frames),
[continuum `briggsabs` at robust 0 and noise 0](#continuum-briggsabs-at-robust-0-and-noise-0-weighs-every-visibility-infinite),
[`nsigma` with `fastnoise=False` and no mask](#tclean-cleans-nothing-with-nsigma-fastnoisefalse-and-no-mask),
[single-precision Taylor weights](#multitermftnew-forms-taylor-imaging-weights-in-single-precision),
[a mosaic's single-precision weight image](#a-mosaics-weight-image-is-transformed-in-single-precision),
[facets of an image size that does not divide](#facets-of-an-image-size-that-does-not-divide),
[the standard gridder dropping the final Fourier-grid row and column](#the-standard-gridder-drops-the-final-fourier-grid-row-and-column),
[an unflagged non-finite sample](#an-unflagged-non-finite-sample-turns-the-image-to-nan),
[a tuned cube's frequency selection](#a-tuned-cube-selection-compares-outframe-frequencies-with-lsrk-ones),
[a TOPO cube's cache-dependent endpoint selection](#a-topo-cube-can-gain-or-lose-an-endpoint-with-casas-aberration-cache-history),
[a cube's fine frequency grid at a whole ratio](#a-cubes-fine-frequency-grid-lets-rounding-choose-one-or-two-samples-per-plane),
[the `.gridwt` a cube leaves](#a-cube-without-a-per-channel-density-leaves-imagenamegridwt-behind),
[multiscale on an off-centre PSF](#multiscale-misplaces-a-component-on-a-plane-whose-psf-peaks-off-centre),
[a cube's beam for a plane without one](#a-cubes-beam-for-a-plane-without-one-depends-on-how-memory-splits-the-cube),
[`tb.getcol` of an `int64` column](#tbgetcol-refuses-a-column-of-int64),
[`tb.putkeyword` of an int beyond 32 bits](#tbputkeyword-stores-an-int-beyond-32-bits-as-0),
[`tb` slices that abort the process](#tb-slices-with-a-wrong-rank-or-a-start-outside-the-array-abort-the-process),
[`tb.putcell` of an int beyond 32 bits](#tbputcell-and-putcol-wrap-an-int-beyond-32-bits-in-an-int-cell),
[`tb.putcell` with several rows](#tbputcell-with-several-rows-writes-those-before-a-missing-one),
[`tb.addrows` with a negative count](#tbaddrows-with-a-negative-count-removes-rows),
[`tb.removerows` and `tb.addcols` that fail part way](#tbremoverows-and-tbaddcols-do-part-of-a-call-that-fails),
[`MSIter`'s receptor angles for a window without its own `FEED` row](#msiter-keeps-the-previous-windows-receptor-angles-for-a-window-without-its-own-feed-row),
[a projection name with spaces](#tclean-refuses-a-projection-name-with-spaces-round-it),
[NCP on the celestial equator](#ncp-on-the-celestial-equator-places-every-pixel-at-nan).
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
[invalid leakage becoming one](#invalid-leakage-solutions-become-unit-leakage),
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
[physicality of an empty component list](#checking-all-components-of-an-empty-list-exits-casa),
[non-finite or cyclic selection inputs](#component-selection-aborts-for-non-finite-or-cyclic-inputs),
[a limb-darkened disk without an exponent](#writing-a-limb-darkened-disk-without-an-exponent-aborts-casa),
[a negative component under the primary beam](#a-negative-component-turns-positive-under-the-primary-beam),
[a single spectral index](#a-single-spectral-index-is-read-past-its-end),
[the beam's component spectrum frame](#the-primary-beams-component-spectrum-labels-topocentric-samples-lsrk),
[`addcomponent`'s single-precision flux](#claddcomponent-stores-the-flux-in-single-precision).
Component-list behaviours casa-rs follows:
[the `cl` tool's record units](#the-cl-tool-records-a-shapes-axes-in-the-units-it-was-given),
[`addcomponent`'s polarization](#claddcomponent-stores-four-flux-values-as-stokes),
[a shape `addcomponent` cannot make](#claddcomponent-keeps-a-point-for-a-shape-it-cannot-make).
Concatenation behaviours casa-rs does not follow:
[a merged source record's time](#concat-leaves-a-merged-source-rows-time-unchanged),
[source equivalence decided by its last column](#concats-source-equivalence-lets-its-last-optional-column-decide),
[merged sources' numbering](#concat-renumbers-merged-sources-so-that-one-can-split-and-fields-dangle),
[observation IDs after removing duplicates](#concat-leaves-gaps-in-observation-ids-after-removing-duplicates),
[an input without states](#concat-drops-every-state-when-one-input-has-none),
[writing into its inputs](#concat-writes-into-the-measurementsets-it-appends).
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
Calibration behaviours casa-rs follows:
[a gain table's resolution and total bandwidth](#a-gain-tables-solved-window-records-its-frequency-as-its-resolution-and-total-bandwidth).
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
Flagging behaviours casa-rs does not follow:
[a re-saved version's comment](#flagmanager-gives-the-version-a-save-renames-the-new-comment),
[a summary of correlations it cannot name](#flagdatas-summary-aborts-on-a-correlation-it-cannot-name).
Coordinate behaviours casa-rs does not follow:
[a tabular coordinate's subimage](#a-subimage-offsets-a-tabular-coordinates-world-values-instead-of-moving-its-table),
[a FITS date's time scale](#importfits-reads-every-fits-date-as-utc),
[`feather` on a transposed image](#feather-fails-on-an-image-whose-direction-is-not-on-its-first-two-axes).
MeasurementSet selection behaviours casa-rs follows:
[later `timerange` terms' defaults](#later-timerange-terms-take-their-defaults-from-row-0),
[`FLAG_ROW` beside other selections](#the-timerange-defaults-ignore-flag_row-when-other-selections-are-set).
MeasurementSet selection behaviours casa-rs does not follow:
[the state a refused bracketed range leaves](#a-refused-bracketed-timerange-makes-later-ranges-edge-inclusive).

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
zero. casa-rs carries the unrotated uvw per row (`RowContext::original_uvw_m`,
`ConvolutionFunctionSet::prediction_w`) and keys the AW prediction on it.

## `importvla` stale `VLACDA` cache crash on old VLA export data

- Date noted: 2026-04-15
- Status: likely CASA `importvla` bug
- Affected code:
  - `casatools/src/code/nrao/VLA/VLACDA.cc`
  - `casatools/src/code/nrao/VLA/VLABaselineRecord.cc`
- NRAO Archive file: `AG189_1_46325.23029_46325.80807.exp` (archive revision 11)

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

### Summary

In the current CASA `tclean(..., deconvolver='hogbom')` path, `niter=1` appears able to commit
two clean components inside a single minor cycle while still reporting `iterdone = 1`.

Since provider contract 31 (owner approved 2026-10-31), casa-rs defaults its
extra `hogbom_iteration_mode` option to `casa-inclusive` to reproduce this
behavior. Explicit `strict` limits physical updates to the requested budget.
Migrating an older profile warns when that option was omitted and adopts the
inclusive default; explicit values are preserved. CASA's 92 parameter defaults
are unchanged.

The effect is not limited to the top-level `niter=1` case. Any CASA Hogbom
minor-cycle call that enters `hclean` with a positive `cycleNiter` may commit
one more component than the reported minor-cycle count. In a Cotton-Schwab run,
that means the model, residual, restored image, and controller stop/refresh
decisions can shift by up to one extra component per Hogbom minor-cycle block.

The first repro used:

- dataset: `.../casatestdata/measurementset/vla/sim_data_VLA_jet.ms`
- setup: `imsize=512`, `cell='12arcsec'`, `specmode='mfs'`, `weighting='natural'`,
  `gain=0.1`, `threshold='0Jy'`, `niter=1`
- observed result: CASA reported `iterdone = 1`, but the output `.model` image contained two
  nonzero clean components

### Likely mechanism

An off-by-one caller/kernel mismatch:

1. `SDAlgorithmHogbomClean::takeOneStep` seeds `starting_iteration = 0`
2. the Fortran `hclean` kernel iterates over an inclusive `do iter = siter, niter`
3. the returned count is then clamped back down to `niter`

With `siter = 0` and `niter = 1`, that inclusive loop permits two update opportunities,
which matches the observed behavior.

### Repro detail

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

### Benchmark repro

The same behavior appeared on a medium standard-MFS VLA benchmark dataset:

- setup: `specmode='mfs'`, `gridder='standard'`, `deconvolver='hogbom'`,
  `imsize=1024`, `cell='0.25arcsec'`, `spw='0:0~63'`, Briggs `robust=0.5`,
  `gain=0.1`, `threshold='0Jy'`, `niter=1`, `cycleniter=1`
- observed result: CASA reported `iterdone = 1`, but the output `.model` image
  contained two nonzero clean components:
  1. `(510, 510) = 9.177456855773926`
  2. `(508, 507) = 8.556220054626465`

In a strict one-component casa-rs comparison this made the one-cycle residual RMS
difference jump to `3.65e-2`. Running casa-rs with its CASA-compatible Hogbom
accounting (`HogbomIterationAccounting::CasaInclusive`) reproduced both
components and reduced the residual RMS difference to `7.64e-5`, matching the
dirty gridding numerical floor. On the full
`niter=500`, `cycleniter=50` heavy row, the same CASA-compatible mode reduced the
CASA/casa-rs product differences to `.model` RMS `1.71e-7`, `.residual` RMS
`5.68e-5`, and `.image` RMS `5.87e-5`.

## MEM reuses the first mask tile for its alpha update

CASA 6.7.6.14's no-prior `EntropyI::formGDG` advances its model and residual
lattice iterators but does not advance its mask iterator. Its separate
`formGDGStep` does advance the mask. Consequently the alpha update reads the
first mask tile repeatedly, even when the model and residual have advanced to
later tiles. For a 512 × 512 image whose 256 × 128 first tile is outside an
81 × 81 box mask, an instrumented CASA run gives a zero alpha-update gradient
matrix, although its step matrix has a nonzero chi-square gradient. On the
`refim_eptwochan.ms` two-iteration test, the original CASA model differs from
the native model by 0.164 normalized RMS; an offset box gives 0.164 as well.

A local CASA probe replaced only that Gram-matrix calculation with
`formGDGStep`, which computes the same matrix while advancing the mask. The
probe is identical to unmodified CASA for a 200 × 200 single-tile box, and
its 512 × 512 centred and offset box models agree with casa-rs within
2.2e-7 normalized RMS; the other image products also pass their normal
tolerances. This is evidence for the cause, not a replacement CASA reference.
casa-rs reads each pixel's actual mask for both calculations, rather than
reproducing a result that depends on the image's storage tile layout (#1060).

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

`refim_point_withline.ms` with `spw='0:0~7', niter=50, gain=0.2,
deconvolver='mtmfs'` shows the same thing component by component (#1113).
Running `niter` from 1 to 50 on each side and differencing consecutive
`.model` images gives each iteration's component. Both sides choose the same
pixels through iteration 19. At iteration 20 CASA takes [49,46] and that
casa-rs series takes [51,54], the mirror pixel through the source at [50,50],
with the same strengths to six figures. In five repeats in separate
processes, CASA gave identical images each time; casa-rs gave two outcomes,
one matching CASA and one matching CASA reflected through [50,50]. That
difference (`.model.tt1` 2.4e-2 NRMS, `.residual.tt1` 6.2e-2) falls to
between 6e-8 and 8e-7 on either match, so the case cannot measure parity
either.

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

## AW gridding can continue after a PA cache miss

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 bug. Casa-rs reports an uncovered imported buffer (#1070).
- Affected code: `TransformMachines2/CFStore2.cc` (`nearestPA`),
  `VB2CFBMap.cc` (`makeVBRow2CFBMap`) and `AWProjectFT.cc`
  (`findConvFunction`, `resampleDataToGrid`).

`CFStore2::nearestPA` takes the first angle strictly within
`computepastep`, in the imported catalog's sorted angle order. On a cache
miss `makeVBRow2CFBMap` can leave the previous row-to-buffer mapping in
place. `findConvFunction` can generate a missing buffer when its angle
change detector fires. The gridding path also calls the map builder and
continues when that lookup reports a miss.

On `refim_mawproject.ms` with a 5-degree step and default rotation, CASA
creates 31 PA buffers. The warm replay still logs five integrations with
no in-band PA in field 1 and publishes an image. Its residual differs from
the cold run by 2.95e-3 NRMS. Casa-rs reports a missing imported buffer
before publication, preserving its read-only import policy (#1043).
Field 0 is covered by the same cache, and all seven products match CASA
with exact masks. The default 360-degree control also matches across both
fields. CASA's separate one-degree coalescence in
`CFCache::fillCFListFromDisk` and `CFStore2::resize` remains unverified for
densely spaced imported headers (#1070).

## AW cache creation and reopening can select different PA buffers

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 cold and warm selection differs when PA coverage
  intervals overlap. Casa-rs uses the sorted cache order in both runs (#1070).
- Affected code: `TransformMachines2/AWProjectFT.cc` (`findConvFunction`),
  `CFStore2.cc` (`resize`, `nearestPA`) and `CFCache.cc`
  (`fillCFListFromDisk`).

CASA adds generated buffers in observation order and picks the first PA
strictly inside `computepastep`. Reloading sorts the cache by PA before
applying the same first-match rule. On FIELD 0 of `refim_mawproject.ms`
at 5 degrees, both orders cover all 108 selected integrations using
the same 19 buffers. They select different buffers at 57 integrations.
CASA's cold and warm residuals differ by 3.284e-3 NRMS and 16 mask
pixels. With the same cache pixels, a temporary insertion-order
selector matches all seven CASA cold products. The sorted selector
matches all seven warm products, with exact masks in both comparisons.

Native cache generation plans a centre at each uncovered selected feed
angle and uses CASA's sorted replay order for both fresh and reused
cells. Its cold and warm outputs agree with the CASA warm reference.

## AW aperture rotation can regrid an already rotated cell

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 rotation history affects the residual. Casa-rs
  samples the saved cell at each rotation epoch (#1070).
- Affected code: `TransformMachines2/AWConvFunc.cc`
  (`prepareConvFunction`), `AzElAperture.cc` (`rotate2`) and
  `Utils.cc` (`rotateComplexArray`).

The first change greater than `rotatepastep` regrids the current kernel
with cubic interpolation and records the current feed PA. Later changes
regrid those already interpolated pixels. On field 0 of
`refim_mawproject.ms`, a zero-weight integration at -99.639° followed
by the same nonzero integration at 177.839° changes CASA's 5-degree
residual by 1.122e-4 NRMS versus selecting the final integration alone.
Both paths rotate to the same final PA. The default 360-degree control
has identical arrays in both selections.

Casa-rs uses the same strict trigger and cubic interpolation, starting
from the saved cell at each epoch. As in CASA's major cycle, an
integration with no unflagged cross-correlation cannot advance the
rotation. The two selections at the same final PA have identical arrays
and masks in casa-rs. The independent CASA one-integration
control and the full field match all seven products and masks under the
existing limits. The largest full-field NRMS is 6.737e-5.

## AW pointing can reuse the origin of an earlier convolution buffer

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 bug. casa-rs uses the current buffer's origin (#1070).
- Affected code: `TransformMachines2/VB2CFBMap.cc`, `PhaseGrad.cc`
  (`needsNewPhaseGrad`, `ComputeFieldPointingGrad`) and the resampler's
  `accumulateToGrid.inc` and `accumulateFromGrid.inc`.

The gradient array uses the selected PA buffer's maximum CF width. Its
origin is `width/2` for an even width and `width/2+1` for an odd width.
The resampler reads the gradient about `width/2` in either case. A wider
array can survive a switch to a smaller CF buffer on the same field.
PSF formation visits the weight buffer first, and the residual can use its
origin when the two buffers have opposite parity.

On the offset field of `refim_oneshiftpoint.mosaic.ms`, making the imaging
or weight buffer even produces a 0.0325 NRMS change in `.residual.tt0`
between fresh PSF formation and `calcpsf=False,calcres=True` with that same
PSF. The PSF, weights, inputs and cache are identical between each pair.
The uniformly odd control produces identical CASA residuals. Casa-rs uses
the maximum width of each selected imaging or weight buffer independently.
Its fresh and saved-PSF runs agree with all 21 CASA products from the
saved-PSF control, including masks and both PB-corrected Taylor images.
Prediction uses the conjugate of the imaging phase.

## AW MT-MFS restart multiplies saved higher Taylor weights again

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 bug. casa-rs recomputes the weights with the PSF (#1070).
- Affected code: `TransformMachines2/CFCache.cc` (`loadWtImage`),
  `AWProjectFT.cc` (`getWeightImage`) and
  `AWProjectWBFT.cc` (`initializeToSky`).

With `gridder='awproject'`, `deconvolver='mtmfs'` and the default
`calcpsf=True`, CASA can read each saved `.weight.ttN` when a run continues
at the same image prefix. `CFCache::loadWtImage` multiplies each saved plane
by `.sumwt.tt0`. The higher Taylor weight images were already written as
raw moments (see the preceding normalization bug).
`AWProjectFT::getWeightImage` also reapplies its sinc grid correction to
the saved, already corrected planes.

In `test_mosaic_mtmfs.test_mtmfs_awproject_cbFalse_twofield` on
`refim_oneshiftpoint.mosaic.ms`, a dirty run followed by ten CLEAN
iterations multiplies the central values of `.weight.tt1` and
`.weight.tt2` by 49,971.44, the principal sum of weights. The final
restored images and residuals also depend on the saved weights. Moving
only the three saved weight images aside before CLEAN makes CASA agree
with a fresh CLEAN on the same observation. casa-rs recomputes weights
when it recomputes the PSF. All 19 products and the iteration counts
agree with both CASA controls under the existing tolerances.

The matching `cbTrue_twofield` case reloads the higher weights by 49,980.72
and fits the same reload and sinc correction to 7.4e-8 NRMS. Fresh CLEAN,
same-prefix CLEAN after moving only the saved weights, and continuation with
`calcpsf=False` each agree with casa-rs on all 19 products and their masks.
Both implementations report ten iterations and two major cycles in each
control.

## A Taylor term gives every Stokes plane the first correlation's sum of weights

- Date noted: 2026-11-03
- Status: deliberate CASA shortcut; casa-rs follows it for parity (#1065)
- Affected code: `synthesis/TransformMachines2/MultiTermFTNew.cc`
  (`finalizeToSkyNew`, `finalizeToWeightImage`),
  `synthesis/TransformMachines/StokesImageUtil.cc` (`ToStokesSumWt`)

A single-term gridder (`FTMachine::finalizeToSkyNew`) forms each Stokes
plane's `.sumwt` with `ToStokesSumWt`'s coordinate form, from the
correlations the plane is made of: `min(RR, LL)` for I and V, `min(RL, LR)`
for Q and U. `MultiTermFTNew` calls the two-argument form instead, which
"picks the value from the first correlation plane, and fills it into all
stokes planes", as its comment says, valid when every correlation has the
same weights. So a Taylor term's `.sumwt.ttN` holds the first gridded
correlation's sum on every Stokes plane: of parallel-hand data imaged as
`IQUV`, Q and U have Stokes I's sum where a single term's have zero, and
the residual of every plane is normalized by that sum. CASA 6.7.6.14's
`test_stokes_weighting` (call 8, `mtmfs` of `refim_twochan.ms`) writes
`[143360.94] × 4` in every `.sumwt.ttN`. casa-rs forms a Taylor term's sums
the same way.

## With no weight on the first correlation, every Taylor plane is zero

- Date noted: 2026-11-03
- Status: CASA loses data; casa-rs refuses the run until #1109
- Affected code: `synthesis/ImagerObjects/SIImageStore.cc`
  (`divideImageByWeightVal`)

Because every Stokes plane takes the first gridded correlation's sum, a
Taylor run whose first correlation has no weight at all (RR fully flagged
with LL present, say) has a zero `.sumwt.ttN` on every plane.
`divideImageByWeightVal` sets a plane whose sum is at most 1e-7 to zero, so
CASA writes every plane of every term as zeros and the other hand's data is
lost. casa-rs refuses such a run when it forms the Taylor products, naming
#1109, rather than write images that read as an empty sky.

## `restoringbeam='common'` restores every Taylor Stokes plane with the first plane's beam

- Date noted: 2026-11-03
- Status: CASA behaviour; casa-rs's result is the same (#1065)
- Affected code: `synthesis/ImagerObjects/SDAlgorithmBase.cc` (`restore`),
  `SDAlgorithmMSMFS.cc` (`restore`), `SIImageStore.cc` (`restore`),
  `synthesis/TransformMachines/StokesImageUtil.cc` (`ToStokesPSF`)

`SDAlgorithmBase::restore` passes its restoring beam by non-const
reference to `SIImageStore::restore` on each Stokes plane's sub-store, and
the first plane's call sets it to that plane's beam (casacore's `SubImage`
holds one beam of the set). Planes 1 to P−1 are then restored with plane
0's beam, and `SDAlgorithmMSMFS::restore` records that one beam
(`setRestoringBeam`), so the image has a single beam. casa-rs restores
every plane with the beam that encloses all the planes' fitted beams, as it
does any image's planes under `'common'`, and records that one beam. The
two agree because `ToStokesPSF` gives every Stokes plane the first
correlation's PSF, so every plane fits the same beam. A beam the request
gives is recorded once in the same way.

## A Gaussian beam fit to collinear PSF samples is unstable

- Date noted: 2026-11-11
- Status: CASA 6.7.6.14 instability, with a deterministic failed-fit treatment in casa-rs ([#1140](https://github.com/bglenden/casa-rs/issues/1140#issuecomment-6802113024)).
- Affected code: `synthesis/TransformMachines/StokesImageUtil.cc` (`FitGaussianPSF`),
  `synthesis/ImagerObjects/SIImageStore.cc` (`makeImageBeamSet`, `getPSFSidelobeLevel`).

In a 64-square, two-channel Clark restart at 8 arcsec per pixel, the saved
unit-peak PSF of one channel is a one-pixel-wide line. Changing one
off-peak sample by one Float step, about 5.96e-8, moves CASA's fitted
position angle from 10.3232° to 0.0017° or −56.3750°, and the reported
sidelobe from 0.585933 to 0.156330 or 0.900000. All three PSFs have the
same collinear fit support. That support constrains one directional width,
leaving the two-dimensional ellipse underdetermined.

casa-rs checks the selected fit samples on their integer grid and retries
with the existing lower cutoffs. When every attempt with enough samples
is collinear, the plane uses the ordinary failed-fit beam for its domain.
That is the largest fitted beam by area, or a circular 1e-6 arcsec beam
when the domain has no fitted beam. Restoration, the minor-cycle patch,
and the sidelobe calculation use the same beam. Two-dimensional fits and
saved beam metadata retain their existing treatment.

## MT-MFS via cube can abort when the primary-beam cutoff removes the central PSF

CASA 6.7.6-14 aborts for a standard-gridder MVC run on field 0 of
`refim_oneshiftpoint.mosaic.ms` with a 512-square image, 10 arcsec cells,
three output channels, and `pblimit=-0.8`. The reduced Taylor PSF's
positive maximum is 0.035371 at (275, 168), outside the central 80-square
region. Both a ten-iteration run and a dirty run with restoration enabled
reproduce the abort.

`MatrixCleaner::validatePsf` leaves the peak at (0, 0) when its central
search sees only zeros. `MultiTermMatrixCleaner::computeHessianPeak` then
uses that location to slice its 80-square convolution patch and throws an
uncaught array exception for the negative corner (-40, -40).

casa-rs rejects the clean request with a task error when the Taylor PSF
has no positive finite peak in the central patch. Its dirty path
publishes the cube and Taylor products without attempting that solve.
With the same input and `pblimit=-0.1`, both clean runs complete ten
iterations and three major cycles and stop at the iteration limit.
[Issue #1140](https://github.com/bglenden/casa-rs/issues/1140).

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

Measured on a mosaic (2026-11-03, #818, CASA 6.7.6.14, CPU f64). On a mosaic
a row's uvw is `MosaicFT`'s reprojection (`girarUVW`, in which w does not
reach u and v), so casa-rs bins and looks up a mosaic cube's density there,
while CASA bins at `GridFT`'s rotation and looks up at the MeasurementSet
uvw. On `refim_alma_mosaic.ms` (three fields 7.5″ apart, three channels,
350×280 at 0.06″, `mosweight=True`, where each field's density is its own,
`BriggsCubeWeightor`'s `multiField`):

- one field imaged at its own centre agrees with CASA on every product, the
  PSF to 1e-7 and `.sumwt` exactly, under uniform and Briggs weighting;
- the same field imaged at its neighbour's centre differs, uniform weighting
  PSF NRMS 2.6e-2 and `.sumwt` 4.1e-4: 4,135 of its 130,032 (row, channel)
  weights differ from CASA's `IMAGING_WEIGHT`;
- an evidence-only build that grids the density at `GridFT`'s rotation of the
  MeasurementSet uvw and looks it up at the MeasurementSet uvw, as CASA does,
  matches all 130,032 of CASA's weights to 4.2e-10;
- the three fields together, at the test's centre (the middle field's): PSF
  NRMS 9.3e-3 under uniform weighting, 5.6e-4 under Briggs 0.5 (2.6e-2 is
  sparser: each field's density holds a third of the samples); with one
  density over all fields (`mosweight=False`), Briggs PSF 3.9e-4.

So this ALMA mosaic cube's differences under a density weighting, with or
without `mosweight`, are this entry's mechanism (#789 records the
seven-pointing case). An EVLA mosaic cube (`refim_oneshiftpoint.mosaic.ms`,
three one-channel windows) differs from CASA under uniform weighting even
for one field at its own centre, with or without `mosweight` (PSF NRMS
3.6e-3, ten `.pb` mask pixels), where the standard gridder agrees to 1e-7;
this mechanism does not explain it, and it stays open in #789.

`simanalyze`'s own cube images meet it with one pointing too (2026-11-03,
the simulation round trip, #980). `simobserve` centres a pointing on the
model's centre pixel. For `core5ps.skymodel`'s 1873 pixels of 0.043″, that is
half a pixel, 0.0215″ west and north, from the reference direction
`simanalyze` images at. CASA's `simobserve` and casa-rs's put it at the same
direction. Under `simanalyze`'s Briggs call on a 4-channel cube, the PSF
differs from CASA's by 3.4e-4 and `.sumwt` by 1.2e-5 at that phase centre.
At the pointing's own direction, they agree to 1.2e-7 and 5.6e-8, and the
clean matches iteration for iteration (900, model 1.7e-7). The mosaic of
the same cube differs by 3.3e-3 under Briggs and agrees within 1e-6 under
natural weighting.

## Continuum `briggsabs` at robust 0 and noise 0 weighs every visibility infinite

- Date noted: 2026-11-02
- Status: CASA bug; casa-rs does not follow it (#980)
- Affected code (CASA 6.7.6.14):
  - `casatools/src/code/synthesis/ImagerObjects/SynthesisImagerVi2.cc`
    (`weight`)
  - `casatools/src/code/msvis/MSVis/VisImagingWeight.cc` (the
    `VisibilityIterator2` constructor and `weightUniform`)

Absolute Briggs weighting (`weighting='briggsabs'`, `rmode = "abs"`) divides
each weight by `D·f² + d²` with `f² = robust²` and `d² = 2·noise²`. With
`robust = 0` and `noise = 0` both vanish. For a cube with
`perchanweightdensity`, `SynthesisImagerVi2::weight` refuses the pair
("Absolute Briggs formula does not allow for robust 0 and estimated noise per
visibility 0") before it builds the `BriggsCubeWeightor`; every other density
(a continuum image, or a cube without `perchanweightdensity`) goes to
`VisImagingWeight`, which has no such check, so `weightUniform` divides every
weight by zero and the imaging weights are infinite.

casa-rs refuses the pair in every mode, at the tclean reader (`noise`), the
compiled weighting contract and the operator
(`WeightingGeneration::absolute_density`).

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

## An unflagged non-finite sample turns the image to NaN

- Date noted: 2026-11-02
- Status: CASA behaviour casa-rs does not follow (a NaN image loses the
  product, so casa-rs corrects it)
- Affected code: CASA's gridding path, which accumulates unflagged samples
  without testing them for finiteness

CASA grids a NaN or infinite visibility, weight or UVW in an unflagged
sample like any other, and the transform spreads it, so the products come
out NaN everywhere without naming the sample. casa-rs fails the run instead
(`FiniteValuePolicy::RejectUnflaggedInput`, and `PrepareError::NonFiniteUvw`
for UVW found while compiling), naming the row, channel and correlation so
the sample can be flagged. Flagged samples and grid padding are skipped, as
in CASA. The CASA behaviour is as ARCHITECTURE.md has recorded it since the
review of #792; it has not been rerun against CASA for this entry.

## `tclean` images DATA when CORRECTED_DATA is missing, and says nothing

- Date noted: 2026-10-29
- Status: CASA behaviour casa-rs follows, with a warning (simulation plan
  §3, decision 6)
- Affected code: `synthesis/ImagerObjects/SIMapperCollection.cc` (`grid`),
  `synthesis/TransformMachines2/FTMachine.cc` (`makeImage`),
  `synthesis/ImagerObjects/SynthesisImagerVi2.cc` (`selectData`)

`datacolumn='corrected'` sets `FTMachine::CORRECTED` in `selectData`
without looking at the MeasurementSet. When a VisBuffer has no corrected
visibilities, `SIMapperCollection::grid` and `FTMachine::makeImage` switch
to `OBSERVED`, so `tclean` images `DATA` (less the model in a major cycle).
`SynthesisImager::selectData` once logged "CORRECTED_DATA column does not
exist. Using DATA column instead"; that block is commented out, and CASA
6.7.6's `libcasacpp_synthesis` does not contain the message, so CASA logs
no message for the fallback.

casa-rs's simulator writes `DATA` only, so its imager follows the fallback
and logs CASA's old message as a warning, since the column imaged is not
the one asked for.

VisibilityIterator2's `existsColumn` also counts `CORRECTED_DATA` as
missing in an iteration chunk whose first row's cell is undefined
(`isDefined(0)` on the chunk's attached table), so CASA can image `DATA`
chunk by chunk from a partly written column. casa-rs decides by the
column's presence alone and images an existing `CORRECTED_DATA` as before.
That is a deliberate scope limit, not full parity: casa-tables'
metadata-only probe (`array_cell_is_defined_uncached`) answers only for
lazy, disk-backed `TiledShapeStMan` columns, and one probe would not
reproduce the per-chunk rule.

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
UTC. The data therefore start TAI − UTC at the reference epoch after the
requested epoch (37 s since 2017, 35 s in 2014). The geometry is computed with
the stored number read as UT1 (line 1546), so CASA's data are
self-consistent; only the schedule is shifted.

casa-rs stores the requested UTC, and also reads the stored TIME as UT1 for
the geometry, so its uvw agree with CASA's at equal stored TIME. A parity run
requests a start later by TAI − UTC at the reference epoch on the casa-rs
side, taken from the measures data and never written in as a number, and
compares rows at equal TIME (`tools/simulation/compare_sm_ms.py`).

The same offset separates the calibration tables the two simulators write
(`setgain` and `setleakage` with `table=`): CASA's TIME is TAI under the UTC
label. `sm.setapply` of a CASA-simulated table to a casa-rs MeasurementSet,
or the reverse, therefore takes its solutions TAI − UTC away (37 s today),
several solutions of 10 s integrations. The `corrupt-setapply-*` scenarios agree with CASA
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

## Checking all components of an empty list exits CASA

- Date noted: 2026-11-05
- Status: CASA exits; casa-rs raises a catchable error (owner approved, #1158)
- Affected code: `componentlist::_checkIndices` and `componentlist::isphysical`

CASA 6.7.6-14 exits its Python process when `componentlist().isphysical()`
checks the default `[-1]` selection on an empty list. The index helper expands
`[-1]` into an empty vector and still reads its first element. An isolated
Mac mini run ended with status 255 and no Python exception. casa-rs raises a
`RuntimeError` so a caller can recover. An explicitly empty selection, `[]`,
returns `True`, as in CASA. The owner approved a catchable error on 2026-11-05.

## Component selection aborts for non-finite or cyclic inputs

- Date noted: 2026-11-05
- Status: CASA crash casa-rs corrects (#1176)
- Affected code: CASA `componentlist.select` Python binding

With three point components, CASA 6.7.6.14 terminates an isolated Python
process with signal 11 for `cl.select(float("nan"))`,
`cl.select(float("inf"))`, or a list that contains itself. By contrast,
`cl.select([float("nan")])` selects index 0, and a 100-level acyclic nested
list containing 1 selects index 1. The isolated probe and its receipt are
`tools/simulation/componentlist_selection_crashes_oracle.py` and
`crates/casars-python/python/tests/data/casa_componentlist_selection_crashes.json`.

casa-rs raises `ValueError` for scalar non-finite or cyclic selections and
leaves the selection unchanged. It continues to accept nested acyclic lists
and follows the recorded list-wrapped numeric conversions. This corrects the
process crash: scripts can catch the exception and retain their session.

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
- Status: CASA behaviour casa-rs follows (#1187)
- Affected code: `components/ComponentModels/TwoSidedShape.cc` (`toRecord`,
  lines 265-330)

`TwoSidedShape` remembers the units its axes were set in, and its record
gives them in those units: a Gaussian made by `addcomponent` with
`majoraxis='1arcsec'` records `{'unit': 'arcsec', 'value': 1.0}`. A shape
read from a table has the units `readTable` sets: arcmin for the axes and
degrees for the position angle, with the errors in radians. casa-rs keeps the
input units while the component remains in memory, and uses the table units
after reopening. It previously always reported the table units, which have
the same physical value but differ in the record. The owner approved matching
CASA's in-memory units on 2026-11-05.

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

## Mixed image/component prediction doubles components at flagged samples

- Date noted: 2026-11-12
- Status: CASA bug; casa-rs corrects it (#1053, standing deviation policy)
- Affected code: `synthesis/MeasurementEquations/SkyEquation.cc` (`get` of
  an image model) and `synthesis/TransformMachines/GridFT.cc` (`get`)

CASA 6.7.6.14 first predicts components, then adds each image. `SkyEquation::get`
saves the previous model cube, calls the image gridder in place, and adds the
saved cube back. The gridder overwrites unflagged samples but leaves flagged
samples and rows untouched. Those samples therefore contain the previous
component twice. Independent component-only, image-only and joint predictions
reproduce this for both replacing and incremental `sm.predict` after
`openfromms`: a 0.25 Jy component at the field centre contributes 0.5 Jy to a
flagged sample in the joint run, and 0.25 Jy on its own. At the second field,
the same error doubles its complex visibility, including its phase.

casa-rs masks the image prediction before adding components, so each component
contributes once regardless of flags. Incremental prediction then adds the
stored DATA once, and corruption still reaches flagged samples. This follows
the intended sum of sky models without making flagged results depend on whether
an image was supplied. The raw joint results and independent controls are in
`crates/casa-simulation/tests/data/sm_flagged_samples_casa.json.gz`, generated by
`tools/simulation/sm_flagged_samples_oracle.py`; `tests/flagged_samples.rs`
checks both CASA's excess and the corrected independent sum.

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
if it were one dish's autocorrelation. The original two-antenna probe saw
only rows with a positive component. On 2026-11-11 an eight-antenna,
two-pointing probe reproduced the defect in CASA 6.7.6.14: 56 of 112
cross-correlation rows contain nonzero total power. The other pointing
samples zero brightness; there are also 32 autocorrelation rows.
The raw DATA is retained in `sm_multiple_models_casa.json.gz`, generated by
`tools/simulation/sm_multiple_models_oracle.py`. casa-rs predicts total
power into autocorrelations only; the new test compares those directly
with CASA and requires every cross-correlation sample to be exactly zero.

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

The 2026-11-11 multi-image oracle reproduces the abort with CASA 6.7.6.14
after `sm.done()` as well. Predicting the same mosaic image twice also
produced SIGBUS during cleanup on one run. The isolated child writes its
reference DATA before cleanup; the parent accepts only these identified
cleanup failures and records the process exit separately from prediction.

## Mosaic multi-image prediction depends on image order

- Date noted: 2026-11-11
- Status: CASA bug; casa-rs corrects it under the standing CASA deviations
  rule (#1053)
- Affected code: `synthesis/TransformMachines/SimplePBConvFunc.cc`
  (`convIndex`, `storeImageParams`) and `HetArrayConvFunc.cc`
  (`findConvFunction`), through the simulator's mosaic machine

With CASA 6.7.6.14, `sm.predict(imagename=[a, b])` through the mosaic
machine gives a different result from predicting the two images separately
and adding their DATA. The deterministic oracle uses a 64-square,
single-plane image and a 96-square, four-plane image, at the same cell size
and phase centre. Against the separately predicted sum, `[a, b]` differs
by 0.00353 normalized RMS and `[b, a]` by 0.0326. Swapping the images
changes DATA by 0.0307. `[a, a]` equals twice the separate prediction of
`a`, so this is not a general failure to accumulate images.

The public CASA source at `4ebd5b1508a5d31b74e7b5f6b89313368d30b9ef`
explains this failure: `SimplePBConvFunc::convIndex` keys the convolution
cache by MeasurementSet and spectral window, omitting image geometry.
`storeImageParams` updates the current image coordinates and dimensions,
but does not invalidate the cached main convolution. `HetArrayConvFunc`
then reuses it for the later model. This source inspection supports the
diagnosis; the numerical reproduction above uses the installed 6.7.6.14
oracle rather than claiming that the public revision is its exact source.

casa-rs prepares each image with its own geometry and adds the predictions.
This follows `SkyEquation::predict`'s evident intent to sum all model
images and avoids silently corrupting DATA according to input order. The
test retains the raw joint CASA outputs and checks that they still expose
the defect. Its independent reference for these two cases is the sum of
the separate CASA predictions, with the usual 0.001 normalized RMS limit;
the limit is not relaxed to admit the defective joint output.

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
- the channel frequencies: the window's own in the simulator; tclean
  converts them to the image's frame (LSRK).

Both grid every row of a window with the widest pair's support
(`MosaicFT` hands `sectgmosd2` and the degridders
`max(convSupportPlanes_p)`), each pair normalised over its own; casa-rs
took tclean's rows each with its own support until #980, which put mixed
12 m and 7 m tclean mosaics 2.5e-3 normalized RMS from CASA, and 1.8e-2 for
the 7 m antennas of a mixed array, where they agree to 6e-7.

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

## tclean's mosaic beams for a MeasurementSet know only the dishes met before it

- Date noted: 2026-11-02
- Status: casa-rs diverges (#980); one beam set over every MeasurementSet's
  dishes for every pass
- Affected code: `synthesis/TransformMachines2/HetArrayConvFunc.cc`
  (`findAntennaSizes`, `findConvFunction`)

With a list of MeasurementSets, `findAntennaSizes` adds each
MeasurementSet's dish diameters to those of the ones the visibility
iterator met before it, and `findConvFunction` rebuilds the beams of a
MeasurementSet's window whenever the iterator moves to another
MeasurementSet (`msId_p != vb.msId()`). In the first pass (the PSF and the
weight image) the first MeasurementSet's beams therefore know only its own
dishes, and in every later pass every MeasurementSet's: the screen size
(the widest dish's support radius) and the gridding support (the widest
pair's) of its rows change between passes, so the result depends on the
order CASA visits the list in. casa-rs builds one set from every
MeasurementSet's dishes and grids every pass with it.

On CASA's own ALMA 12 m and ACA 7 m lists (`test_het_mosaic_mfs_alma_listofms`
CAS-14255 and CAS-14350, 12 m data first, whose pair has the widest
support) only the first pass's screen size differs: every product agrees to
1.3e-5 normalized RMS. With the 7 m data made earlier, so that CASA meets
it first, the first pass grids the 7 m rows with the 7 m support alone, and
the products differ by up to 5.4e-4; each MeasurementSet imaged alone
agrees to 7e-7.

## A mosaic's weight image is transformed in single precision

- Date noted: 2026-11-03
- Status: casa-rs diverges (#980); its weight image is transformed in
  double precision, so a `.pb` pixel within single-precision rounding of
  `pblimit` can fall on the other side of CASA's mask
- Affected code: `synthesis/TransformMachines2/MosaicFT.cc`
  (`finalizeToSky`: `convertArray(griddedWeight, griddedWeight2)`, then
  `ft_p.c2cFFT(*weightLattice)` on the `Complex` grid)

`MosaicFT` accumulates the gridded weights in `DComplex` but converts them
to `Complex` before their inverse transform, so the weight image, and the
`.pb` (`sqrt(weight / max)`) and pixel mask `SIImageStore::makePBFromWeight`
form from it, carry single-precision rounding: about 1e-8 of the peak,
which is a few 1e-6 of the weight where the beam meets a `pblimit` of
0.05. Pixels the image's symmetry makes equal show it: on
`refim_sim_hetarray_alma.ms` (`test_het_antenna_mosaic_cube_alma.3`,
channel 2), four mirror pixels have CASA beams 0.0500000405, 0.0500001221,
0.0499998446 and 0.0499997467, whose mean, 0.04999993, is casa-rs's
0.0499999261–0.0499999285 at all four. CASA keeps the first two and masks
the others; casa-rs masks all four. At double grid precision casa-rs
grids and transforms the weights in double precision, rounding each pixel
to single only as it publishes it, and does not add CASA's rounding.

Every other pixel agrees: the cube mosaics of that MeasurementSet's cross
baselines (`.3`) and of all its antennas (`.4`) differ from CASA in 2 and 1
`.pb` mask pixels of channel 2, each within 1.3e-7 of `pblimit`, and in no
pixel of their other channels or of the 12 m, 7 m and two-field cubes
(`.1`, `.2`, `.5`); their products agree to 4e-5 normalized RMS. Such
pixels cannot measure parity.

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
- Status: CASA bug; resumed simulations centre each integration's beam and
  mosaic kernels on its stored POINTING. Fresh moved `observemany` beams
  remain explicitly refused (#884, #1294).
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
antennas. Resumed simulations now separate FIELD's visibility phase from
POINTING's beam and total-power sample direction. For changing pointings,
`tools/simulation/sm_offset_pointings_oracle.py` retains the raw CASA output
and records a second independent CASA control with a distinct, equal-phase
FIELD identity per integration, forcing the stale cache to refresh. Native
beam and mosaic predictions agree with that control. Native total power
continues to use autocorrelations only, correcting the separate `SDGrid`
cross-correlation defect documented above. Incomplete or disagreeing antenna
pointings are explicitly refused before changing the DATA plan.

Fresh `observemany` beams on pointings moved off their fields remain
explicitly refused. `simobserve` gives each pointing its own field and does
not reach this cache defect.

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

## `simutil.xyz2long` subtracts the x offset from y

- Date noted: 2026-11-03
- Status: CASA bug; casa-rs does not follow it (#1053)
- Affected code: `casatasks/src/private/simutil.py` (`xyz2long`,
  `lon = pl.arctan2(y-dx, x-dx)`)

`xyz2long` converts Earth-centred x, y, z on a datum to longitude, latitude
and height, first removing the datum's offsets. Its distance from the polar
axis uses `(x-dx, y-dy)` and its latitude `z-dz`, but its longitude is
`atan2(y-dx, x-dx)`: it subtracts the x offset from y. For a datum whose x
and y offsets differ, the longitude is wrong: `long2xyz` then `xyz2long` on
NAD27 at ALMA returns a longitude 2.2 arcseconds from the one given (CASA
6.7.6.14). casa-rs subtracts `dy` (`readantenna::geodesy::xyz_to_geodetic`),
so `xyz2long` inverts `long2xyz`; for WGS 84, the datum `readantenna` uses,
the two agree.

## `simutil.xyz2long_old` subtracts the x offset from z

- Date noted: 2026-11-03
- Status: CASA bug; casa-rs does not follow it (#1053)
- Affected code: `casatasks/src/private/simutil.py` (`xyz2long_old`,
  `c = (z-dx)**2 / er**2`)

`xyz2long_old`, CASA's older iteration for the same conversion, removes the
datum's offsets as `(x-dx, y-dy, z-dz)` everywhere but in its second term,
`c = (z-dx)**2 / er**2`, which subtracts the x offset from z. Its latitude
is then slightly wrong for a datum whose x and z offsets differ. casa-rs
subtracts `dz` (`readantenna::geodesy::xyz_to_geodetic_old`); its longitude
and, for WGS 84, its latitude are CASA's.

## Ten ITRF observatories' tabulated positions disagree with themselves

- Date noted: 2026-11-03
- Status: measures data defect; casa-rs reads the table as casacore does
  (#1106)
- Affected data: the `geodetic/Observatories` table of the measures data
  (casarundata 2026.02.19 and 2026.10.27); casacore
  `measures/Measures/MeasTable.cc` (`doInitObservatories`)

Each row of `Observatories` gives a position twice: `Long`, `Lat` and
`Height`, and `X`, `Y` and `Z`. casacore builds every observatory from the
first three alone, for an `ITRF` row as a spherical vector whose length is
`Height`, and never reads `X`, `Y`, `Z`. For 10 of the 20 `ITRF` rows the two
descriptions are different places:

| Observatory | distance between them | what is wrong |
|---|---:|---|
| OVRO_MMA | 6,370,350 m | `Height` is a geodetic height: casacore places it 1.2 km from the Earth's centre |
| LOFAR | 6,364,570 m | the same: 49 m from the centre |
| SAO SMA | 4,983,940 m | `Y` has the wrong sign; `Long`, `Lat`, `Height` are SMA's |
| NRO | 19,983 m | unknown |
| SMA | 13,844 m | `Lat` appears to be geodetic beside a geocentric radius |
| e-MERLIN, MERLIN2 | 6,366 m | unknown |
| SUNRISE | 743 m | a placeholder: `X`, `Y`, `Z` are zero |
| EVLA | 34.5 m | rounded `Long`, `Lat` and `Height` |
| ATF | 24.2 m | unknown |

CASA 6.7.6.14's `me.observatory` returns the `Long`, `Lat`, `Height`
position for each, so its simulations, `itrf2locname` and frame conversions
use it. casa-rs reads the table as casacore does
(`ObservatoryEntry::position` in `casa-measures-data`), so the two place
every observatory alike, including the three that casacore puts inside the
Earth. Choosing the other column per row would need knowing which one was
meant, and would make casa-rs and CASA disagree about the same data; the
correction belongs in the table.

## `simutil.msg` posts a verbose message twice, in colour

- Date noted: 2026-11-03
- Status: casa-rs posts it once (#1053)
- Affected code: `casatasks/src/private/simutil.py` (`msg`)

With `verbose=True`, CASA's `msg` posts each message to its log twice at
INFO: once with ANSI colour codes around `[origin]` and the words `WARNING`
and `ERROR`, once with those words still coloured. casa-rs logs it once,
plain, at INFO on `casars.<origin>`, and writes the open report as CASA
does. Without `verbose` the two log alike (CASA 6.7.6.14,
`tools/simulation/simutil_helpers_oracle.py`).

## `simutil.is4d` leaves the image tool open

- Date noted: 2026-11-03
- Status: CASA defect; casa-rs does not follow it (#1053)
- Affected code: `casatasks/src/private/simutil.py` (`is4d`)

`is4d` returns `False` before `ia.done()` for an image without CASA's four
axes in order, so the module's shared image tool stays open and the next
`ia.open` warns "Another image is already open, closing first". casa-rs
opens nothing that outlives the call.

## `MDirection::from_source_name` trims a source name

- Date noted: 2026-11-03
- Status: casa-rs is more lenient (#1110)
- Affected code: casacore `MeasTable::Source`; casa-types
  `measures/direction.rs` (`parse_named_direction`)

casacore finds a source by name with `MUString::minimaxNC`, which keeps
spaces, and so does casa-rs's measures data. CASA 6.7.6.14's `me.source`
reads its argument as a direction string first, splitting it at spaces:
it finds `'  0002-478'`, with leading spaces, but not `'\t0002-478'`, with a
tab. casa-rs's `me.source` splits the same way and agrees on both.
casa-rs's Rust `MDirection::from_source_name`, which TaQL's measures
functions and the derived MeasurementSet columns use, trims every kind of
surrounding white space, a tab too, before it looks a name up, as it does
for planets and `ZENITH`; there a name with leading white space finds a
source that casacore's `MeasTable::Source` would not.

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

## simobserve rounds a `direction` list of one to its mean

- Date noted: 2026-10-27
- Status: CASA artefact; casa-rs simobserve departs (#841 Phase 5c, after
  the #913 review)
- Affected code: `casatasks/src/private/task_simobserve.py` (CASA 6.7.6,
  `dir = direction` for a list) and `simutil.direction_splitter`

CASA passes a `direction` list to `calc_pointings2` as a list, even a list
of one. `direction_splitter` centres a list at its mean, written at
precision 5 and read back. So `['J2000 18h00m00.123456789s …']` lays its
map out about 1e-5 s of right ascension (0.15 mas) from the same direction
given as a string.

casa-rs's `simutil` tool follows CASA here, since its lists must match
CASA's (#913 review). casa-rs's `simobserve` cannot tell the two apart.
The task contract resolves a string to a list of one, as it does for every
parameter that takes a string or a list. So `simobserve` lays a map out at
a single direction exactly as given, whether a string or a list of one. A
list of several is the pointings themselves, as in CASA. The rounding
carries no information, so the exact direction is the better answer.

## A gain table's solved window records its frequency as its resolution and total bandwidth

- Date noted: 2026-10-27
- Status: CASA behaviour casa-rs follows, for CASA's append (#917)
- Affected code: `synthesis/MeasurementComponents/SolvableVisCal.cc`,
  `SolvableVisCal::setCTFrequencies`, CASA 6.7.5.18

A G or T table holds one channel per spectral window. For a window a solve
reaches, `setCTFrequencies` writes that channel's frequency and width from
`FreqMetaData`. It then takes `const Vector<Double>& chres(chfr); //
resolution same as freq` and `Double totbw=sum(chfr);`, so `RESOLUTION` and
`TOTAL_BANDWIDTH` both hold the frequency. On ngc5921 (`gaincal`, `spw='0'`)
the table's window reads `CHAN_FREQ` 1413421909.706 Hz, `CHAN_WIDTH`
1538085.9 Hz, and `RESOLUTION` and `TOTAL_BANDWIDTH` 1413421909.706 Hz.

The values are plainly meant to be widths:
- the same function sets `EFFECTIVE_BW` to the width;
- the older `setOrVerifyCTFrequencies` sets resolution, effective bandwidth
  and total bandwidth to the width;
- `NewCalTable::setSpwFreqs` sets `TOTAL_BANDWIDTH` to the summed width;
- a window no solve reached keeps its summed resolution and the
  MeasurementSet's total bandwidth (`createMemCalTable2`).

Nothing in CASA's apply path reads either column, so the error does not
change calibrated data. But CASA's `gaincal(append=True)` merges the new
solutions' windows into the table's (`NewCalTable::mergeSpwMetaInfo`). It
requires `NUM_CHAN`, `CHAN_FREQ`, `CHAN_WIDTH`, `RESOLUTION`,
`EFFECTIVE_BW`, `REF_FREQUENCY` and `TOTAL_BANDWIDTH` to equal its own
exactly for any window both hold.

A casa-rs table that wrote the widths there was refused ("Error merging
spw=0's meta info"). So casa-rs writes the frequency as CASA does: the
interoperability contract outranks the correction.

The law `casa_appends_to_a_casa_rs_gain_table` (casa-calibration
`slow-tests`) appends CASA's solutions to a casa-rs table. With
`gain_table_windows_match_casa_on_ngc5921` and its two-sideband companion,
it asserts that every column of every window row equals CASA's exactly.

## `imregrid` refuses a one-channel template in another spectral frame

- Date noted: 2026-10-27
- Status: CASA behaviour casa-rs does not follow (#841 Phase 6a)
- Affected code: CASA 6.7.6's `imregrid` (casacore `ImageRegrid` through
  `ImageRegridder`)

casacore does not regrid an axis whose input and template both have one
pixel. CASA 6.7.6 keeps the input's coordinate for it in the output: a
template one channel 3 MHz away, or with its one Stokes plane labelled Q,
gives the input's channel and Stokes I (`tools/images/image_analysis_oracle.py`,
cases `imregrid_one_channel_moved` and `imregrid_one_stokes_plane_relabelled`).
The same template with that one channel in another frame (LSRK to TOPO)
stops instead, with "Failed to set conversion layer state".

casa-rs treats that channel as it treats the others: it is not regridded,
and the output keeps the input's (`casa_images::regrid`). Converting the
frame of an axis that is never regridded serves no purpose.

## `tsdimaging`'s theoretical beam models the kernels, not the gridder's

- Date noted: 2026-10-28
- Status: CASA behaviour casa-rs does not follow (#938 part 1, #841 Phase 6c)
- Affected code: `casatasks/src/private/sdbeamutil.py`
  (`TheoreticalBeam.get_kernel` and the kernel functions it calls)

`tsdimaging` gives its image the beam `sdbeamutil.TheoreticalBeam`
computes: the antenna's Gaussian beam convolved with a cut through the
gridding kernel and with a box as wide as the sampling, then fitted with a
Gaussian. The kernel it convolves with is not the gridder's (`SDGrid::init`)
but a model of it, and the models disagree with the gridder:
- **`GAUSS`.** The gridder takes `gwidth` as a half width at half maximum;
  `sdbeamutil` passes it to a Gaussian that takes a full width. It also cuts
  the kernel at 1.5 `gwidth` where the gridder cuts at `truncate`
  (default 3 `gwidth`).
- **`GJINC`.** `sdbeamutil` ignores `truncate` ("NOT SUPPORTED YET") and
  always cuts at the jinc's first null.
- **`SF`.** The gridder's kernel is `(1 − ν²) grdsf(ν)`; `sdbeamutil` uses
  SciPy's prolate spheroid `pro_ang1(0, 0, c)` with `c = 5.356π/2`, chosen
  to resemble it.
- **`PB`.** The gridder takes the telescope's power pattern (for ALMA, the
  10.7 m Airy pattern of `VPSkyJones`); `sdbeamutil` takes an Airy disk
  scaled to the Gaussian beam's width, for the antenna's own diameter.
- **`BOX`.** `sdbeamutil` makes the box one pixel wide whatever
  `convsupport` is; the gridder's box reaches `2s + 1` pixels.

The reported beam is then not the image's. Against CASA 6.7.6 on the
oracle's setups (`tools/singledish/tsdimaging_oracle.py`), the gridder's own
kernels give beams wider by 0.60% to 3.3% for `GAUSS`, and 0.31% narrower
for `GJINC` with a `truncate`. They differ by at most 6.0e-4 for `SF`, `PB`
and `GJINC` at its defaults. The gridder reads its table at the radius
truncated to a hundredth of a pixel, which widens the kernel slightly. That
accounts for all of `GJINC`'s difference at its defaults (read at the
rounded radius, it agrees with CASA to 1e-6) and for about 40% of `SF`'s.
The rest of `SF`'s is SciPy's spheroid against `grdsf`.

casa-rs cuts through the gridder's own table (`casa_imaging_singledish::
theoretical_beam_arcsec`), so the beam describes the image the gridder
makes. `BOX` at its default support, which both describe alike, agrees with
CASA to 2e-8.

The cost falls downstream. `simanalyze` and `simalma` grid total power with
`SF` of support 6 and scale its flux by this beam's area. A beam 6e-4 wider
is an area about 1.2e-3 larger, so their scaled total-power and feathered
products differ from CASA's by about that factor, at the edge of the 1e-3
acceptance. Phases 6g and 6h measure it.

## `getPointingSamplingRaster` measures the row spacing in raw longitude

- Date noted: 2026-10-28
- Status: CASA bug; casa-rs does not follow it (#938 part 1, #841 Phase 6c)
- Affected code: `casatools/src/code/synthesis/Utilities/SingleDishBeamUtil.cc`
  (`getPointingSamplingRaster`)

CASA estimates a raster's sampling from its pointings. Along the scan it
scales each longitude step by the cosine of the map's central latitude, so
the step is an angle on the sky. Across the scan it projects the steps
between rows' median positions on the perpendicular without that factor. A
map scanned in latitude, whose rows step in longitude, then has its row
spacing overestimated by `1/cos δ`, 15% at δ = −30°. The beam `tsdimaging`
reports widens with it.

casa-rs scales the rows' longitude steps as it scales the along-scan ones
(`casa_imaging_singledish::raster_sampling`). A map scanned in longitude, as
`simobserve` scans one, is unaffected: CASA and casa-rs agree on the
oracle's maps to 1e-15.

## `tsdimaging` writes the weight mask's threshold to six decimal places

- Date noted: 2026-10-28
- Status: CASA bug; casa-rs does not follow it (#938 part 1, #841 Phase 6c)
- Affected code: `casatasks/src/private/task_tsdimaging.py` (CASA 6.7.6,
  `do_weight_mask`: `ia.calcmask("'%s'>%f" % (weightimage, weight_threshold))`)

`minweight` masks the pixels whose weight is at most `minweight` times the
median positive weight. The threshold goes into the mask expression through
`%f`, which keeps six decimal places. A threshold under 5e-7 is written 0,
so every pixel with a positive weight stays unmasked, and one of 1.4e-6 is
written 0.000001, masking less than asked. Weights have no fixed scale, so
a MeasurementSet whose weights are all small loses its `minweight` mask
entirely. casa-rs compares with
the threshold itself (`casa_imaging_singledish::weight_mask_threshold`;
`tools/singledish/tsdimaging_oracle.py`, case `sf_small_weights`).

## `SDGrid` grids nothing with an `SF` kernel of no support

- Date noted: 2026-10-28
- Status: CASA behaviour casa-rs refuses (#938 part 1, #841 Phase 6c)
- Affected code: `casatools/src/code/synthesis/TransformMachines2/SDGrid.cc`
  (`SDGrid::init`)

`SDGrid::init` tabulates the `SF` kernel over `convsupport` pixels, so
`convsupport=0` gives a kernel of zeros. Every pixel's weight is then zero,
the image is empty, and `tsdimaging` only warns that no weight is positive.
casa-rs refuses that kernel, asking for a support of at least 1 or CASA's
default of 3 (`casa_imaging_singledish::GridFunction::from_casa`).

## `simanalyze` writes the fidelity's floor to six decimal places

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 6f)
- Affected code: `casatasks/src/private/task_simanalyze.py` (CASA 6.7.6,
  `"max(abs('%s'), %f)" % (difference, maxdiff/pl.sqrt(2.0))`)

The fidelity image divides the convolved model by the difference, floored at
the difference's median absolute deviation over √2. The floor goes into the
image expression through `%f`, which keeps six decimal places. A floor of
5.19e-7 Jy/beam is written 0.000001, nearly twice its value, and one under
5e-7 is written 0, so a pixel where the difference is zero divides by zero.
Simulations of faint sources have differences that small
(`tools/simulation/simanalyze_analysis_oracle.py`, case `faint`). casa-rs
uses the floor itself (`casa_simulation::simutil::analysis::fidelity`).

## `simanalyze`'s fidelity image is labelled Jy/beam

- Date noted: 2026-10-27
- Status: CASA artefact; casa-rs departs (#841 Phase 6f)
- Affected code: `casatasks/src/private/task_simanalyze.py` (CASA 6.7.6,
  `ia.imagecalc(fidelityim, "abs('%s') / '%s'" % (convolved, absdiff))`)

The fidelity is the ratio of two images in Jy/beam. CASA's image expression
copies its unit from the first image it names, so the fidelity is labelled
Jy/beam. casa-rs writes it without a unit.

## `statim` scales a Jy/beam image without a beam by the pixel area

- Date noted: 2026-10-28
- Status: CASA bug; casa-rs does not follow it (#841 Phase 6f)
- Affected code: `casatasks/src/private/simutil.py` (CASA 6.7.6, `statim`:
  `toJyarcsec=1.` when `ia.restoringbeam()` is empty, then
  `toJypix=toJyarcsec*pixarea`)

`statim` multiplies an image's minimum, maximum and corner rms by a
flux-per-pixel factor. For Jy/pixel, and for Jy/beam with a restoring beam,
the factor is 1, so the statistics stay in the image's unit. For Jy/beam
without a beam it treats the image as Jy per square arcsecond, and the factor
becomes the pixel area in square arcseconds: 0.04 for 0.2″ pixels. It still
reports the unit as Jy/beam, so the numbers are in no unit at all. Without a
beam, Jy/beam cannot be converted to anything. casa-rs reports the image's
own values in its own unit (`casa_simulation::simutil::analysis::statim`;
`tools/simulation/simanalyze_analysis_oracle.py`, image `beamless.flat`).
`simanalyze`'s reported statistics of its image do not meet this case: it
reads the image's beam first, and fails without one.

## Image expressions containing "pa" are labelled as position angles

- Date noted: 2026-10-27
- Status: CASA bug; casa-rs does not follow it (#841 Phase 6f)
- Affected code: casatools
  `imageanalysis/ImageAnalysis/ImageExprCalculator.tcc`
  (`_expr.contains(casacore::Regex("pa\\(*"))`)

`ImageExprCalculator` looks for the `pa()` function with the regular
expression `pa\(*`, which matches "pa" followed by any number of
parentheses, so any "pa" at all. An expression whose image paths contain
"pa" (a project named `pancake`, or a path through `scratchpad`) has its
output's unit set to deg and its Stokes axis replaced by `Pangle`.
`simanalyze`'s difference, absolute difference, fidelity and absolute model
are all image expressions over `<project>/<project>.<configuration>`, so a
project whose path contains "pa" gets a fidelity labelled deg, and
difference and fidelity images whose Stokes plane is a position angle. casa-rs
computes those images directly and keeps the Stokes axis.

## `concat` leaves a merged SOURCE row's TIME unchanged

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casacore `ms/MSOper/MSConcat.cc` (`updateSource`:
  `sourceCol.interval().put(j, newTime)` before
  `sourceCol.interval().put(j, newInterval)`)

When two `SOURCE` rows of one window are equivalent, `updateSource` keeps the
first and widens its validity to span both: it computes the union's centre
and length, then writes the centre into `INTERVAL` and overwrites it with the
length. `TIME` is never written, so the kept row is valid over an interval
centred on its old time, which misses the end of the other row's validity.
casa-rs centres the record on the union
(`casa_observation::merge`, `sources.rs`, `widen`).

## `concat`'s source equivalence lets its last optional column decide

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casacore `ms/MSOper/MSConcat.cc`
  (`sourceRowsEquivalent`)

`sourceRowsEquivalent` compares the required columns with `&&`, then tests
each optional column present (`POSITION`, `PULSAR_ID`, `REST_FREQUENCY`,
`SYSVEL`, `TRANSITION`) by assigning its result to `areEquivalent`, so the last
optional column tested decides: two rows with different rest frequencies but
equal transitions are equivalent, and their lines are merged into one.
casa-rs requires every member to be equal (an optional one stated by both or
by neither).

## `concat` renumbers merged sources so that one can split and fields dangle

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casacore `ms/MSOper/MSConcat.cc` (`updateSource`, "renumber
  consecutively"; `copyField`, `newSourceIndex2_p`)

After removing redundant `SOURCE` rows, `updateSource` renumbers source IDs
row by row: a row whose ID exceeds the count so far gets the next number. A
source of several rows (one per window) after a removed ID therefore gets one
number for its first row and another for the next, splitting it. The IDs it
changes are applied to the fields `copyField` appends, not to the fields
already in the output, which can then name a source ID no row has. casa-rs
numbers merged sources in the order they first appear, every record of a
source keeping its number, and maps every field
(`casa_observation::merge`, `sources.rs`, `regroup`).

## `concat` leaves gaps in observation IDs after removing duplicates

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casacore `ms/MSOper/MSConcat.cc` (`copyObservation`)

`copyObservation` appends the incoming `OBSERVATION` rows, removes each row
equivalent to an earlier one, and maps it to the last earlier equivalent. It
does not shift the IDs of the rows after a removed one, as `copyProcessor`
does with `removedSoFar`, so an input of several observations can have its
rows mapped past the end of the table, or onto another observation, once a
duplicate is removed. casa-rs maps each duplicate to its first equivalent and
numbers the distinct observations consecutively, for processors and
observations alike (`casa_observation::merge`, `distinct`).

## `concat` drops every state when one input has none

- Date noted: 2026-10-29
- Status: CASA behaviour; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casacore `ms/MSOper/MSConcat.cc` (`concatenate`, "merge
  STATE")

When one of the two MeasurementSets has no `STATE` rows, `concatenate` writes
no states at all: it deletes the first MeasurementSet's `STATE` rows and sets
every row's `STATE_ID` to -1, so the intents of the input that had them are
lost. casa-rs keeps them: the rows of an input without states have
`STATE_ID` -1, and the others keep their merged states
(`casa_observation::merge`, `states`).

## `concat` writes into the MeasurementSets it appends

- Date noted: 2026-10-29
- Status: CASA behaviour; casa-rs does not follow it (#937 PR B, #939)
- Affected code: casatools `src/tools/ms/ms_cmpt.cc` (`ms::concatenate`,
  line 6497)

After appending a MeasurementSet, `ms::concatenate` writes a `HISTORY` row
("… appended to …", with its parameters) into the appended MeasurementSet,
an input, rather than into the output; the inputs' own `HISTORY` is never
copied. A task that reads several MeasurementSets to make another should
leave them as they were. casa-rs never writes an input: the output's
`HISTORY` holds what the creating task records of itself
(`CreationOptions::history`), and the inputs' `HISTORY` is a declared loss.

## `tclean` reads a `uvtaper` with no unit as radians

- Date noted: 2026-10-28
- Status: CASA bug; casa-rs refuses the value (#841 Phase 6b)
- Affected code: casatools `synthesisimager_cmpt.cc` (`setweighting`) and
  `msvis/MSVis/VisImagingWeight.cc` (`setFilter`)

`tclean`'s documentation gives `uvtaper=['300.0']` as a taper in
wavelengths ("default units are lambda in aperture plane"). `setFilter` takes
the uv-plane path only when the unit contains "lambda", and otherwise treats
the value as an image-plane angle, here 300 radians. The filter is then
`exp(-π²θ²/(4 ln 2) u²)`, which underflows to zero on every baseline of
`refim_point.ms`. Every imaging weight is zero there, and the image is empty
with a beam of 1e-6 arcsec (CASA 6.7.6). A unitless value is always an angle
in radians to CASA, never the documented wavelengths. Whether it empties the
image depends on the value and the baselines, and a small enough one does
not. casa-rs refuses a `uvtaper` entry without a unit and asks for an angle
or for wavelengths (`casa_imaging_application::ImagingRequest::uvtaper`).

## `tclean` corrupts an absolute directory when numbering a restart

- Date noted: 2026-10-31
- Status: CASA bug; owner approved preserving the correct directory (#944).
  Native `restart=False` numbering is still unfinished.
- Affected build/code: CASA 6.7.6.14, casatasks
  `private/imagerhelpers/input_parameters.py`, `incrementImageNameList`.

With existing products and `restart=False`, CASA finds the next shared
numbered image prefix. Its Python helper removes the first two characters
of each directory with `dirnames[immod][2:]`, assuming a `./` prefix. For an
absolute imagename under `/Volumes/`, this instead produces `olumes/` and
fails to create the numbered PSF. Relative prefixes in the same probe
correctly produce `_2` and `_3`. The exact build, parameters and failure
are recorded on #944 and in the lifecycle receipt linked from the generated
compatibility reference. The approved casa-rs target keeps the requested
absolute directory; this decision is not evidence that restart is complete.

## `tclean` cleans nothing with `nsigma`, `fastnoise=False` and no mask

- Date noted: 2026-11-02
- Status: CASA bug; casa-rs cleans the whole image, as CASA does with
  `fastnoise=True` (#980)
- Affected build/code: CASA 6.7.6.14, casatools
  `synthesis/ImagerObjects/SIImageStore.cc` (`calcRobustRMS`) and
  `SynthesisDeconvolver.cc` (`initMinorCycle`, `setupMask`)

With `usemask='user'`, no `mask`, `nsigma>0` and `fastnoise=False`, the
first `initMinorCycle` estimates the noise with
`calcRobustImageStatistics(*residual(), *mask(), ...)`. `mask()` creates the
`.mask` image there, filled with zeros. `setupMask` then finds an existing
mask and keeps it ("Initializing to existing mask") instead of setting the
whole image to 1, as it does when no mask exists. The run stops at once with
the zero-mask stop code 7, an empty `.mask` and an empty model (CASA 6.7.6.14
on `refim_twochan.ms`, `imsize=100`, `cell='8.0arcsec'`, `niter=300`,
`nsigma=3`). With `fastnoise=True` the noise estimate never touches `mask()`
and the whole image is cleaned. casa-rs cleans the whole image in both cases:
without a mask its noise estimate is Chauvenet's over every valid pixel, as
CASA's would be once the full mask existed.

## A faceted `tclean` image carries no primary-beam mask

- Date noted: 2026-11-02
- Status: CASA bug; casa-rs masks a faceted residual and restored image
  where the primary beam is under `pblimit`, as CASA masks every other
  image (#980)
- Affected build/code: CASA 6.7.6.14, casatools
  `synthesis/ImagerObjects/SynthesisNormalizer.cc`
  (`divideResidualByWeight`) and `SIImageStore.cc`
  (`divideResidualByWeight`, `copyMask`)

`SIImageStore::divideResidualByWeight` copies the primary beam's `mask0`
onto the residual whenever the image has a primary beam and `pblimit >= 0`
("A T/F mask in the residual will confuse users"), and restoration copies
the residual's mask onto the image. With `facets > 1`,
`SynthesisNormalizer::divideResidualByWeight` normalizes each facet's
sub-image store instead, and `copyMask` finds no `mask0` default mask on a
facet's sub-image of the primary beam, so it copies nothing: the published
`.residual` and `.image` have no mask, although `.pb` masks the same
pixels. On `refim_twopoints_twochan.ms` (`imsize=200`, `cell='8.0arcsec'`,
`facets=2`, `niter=30`, CASA's `test_multifield_facets_mfs`) CASA's `.pb`
masks 2,471 pixels below `pblimit` 0.2 and its `.image` and `.residual`
mask none; casa-rs's `.pb`, `.image` and `.residual` mask those 2,471
pixels, and every pixel value agrees (normalized RMS 8e-7 and 3e-6).

## `tclean` seeds a Stokes I image from a Stokes Q start model

- Date noted: 2026-10-28
- Status: CASA behavior retained with a warning (owner decision #980,
  2026-10-31; supersedes #970's refusal)
- Affected code: casatools `synthesis/ImagerObjects/SIImageStore.cc`
  (`setModelImageOne`, `regridToModelImage`) and casacore
  `images/Images/ImageRegrid.tcc`

`regridToModelImage` regrids a start model on its direction and spectral
axes only. `ImageRegrid` copies the Stokes axis without comparing its
coordinate, so a one-plane Stokes Q model seeds the model of a Stokes I run.
Its pixels are then predicted, subtracted and restored as Stokes I. casa-rs
now matches this positional copy for equal-count Stokes axes and warns when
the labels differ (`casa_imaging_application`'s `StartModel`). The owner
requested matching CASA with a warning on 2026-10-31 (#980), replacing the
earlier refusal. This is not a physical polarization conversion; published
coordinates still describe the requested target image. Unequal-plane-count
forms remain unassessed for equivalence.

A degenerate spectral axis is replicated the same way: an MFS start model of
one plane seeds the MFS plane at whatever frequency it is stated. That is
physically sound for a continuum model, and casa-rs keeps it. A cube's start
model must have the cube's spectral axis.

## `flagmanager` gives the version a save renames the new comment

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#931, #997 review)
- Affected code: casatasks `private/task_flagmanager.py` (mode `save`,
  lines 51–83)

Saving a version under a name that already exists first renames the old
version to `<name>.old.<seconds since 1970>`, so both stay restorable. The
task then rewrites the old entry of `FLAG_VERSION_LIST` as
`tmpname + ' : ' + comment`, where `comment` is the comment of the new save.
CASA 6.7.6.14 lists `{0: {'name': 'v1.old.1793292269', 'comment': 'second'},
1: {'name': 'v1', 'comment': 'second'}}` after saving `v1` with `'first'`
and then `'second'`; the first comment is lost. casa-rs renames the version
the same way and keeps its own comment (`flagmanager`'s
`keep_existing_version`).

## `flagdata`'s summary aborts on a correlation it cannot name

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#931, #997 review)
- Affected code: casatools `flagging/Flagging/FlagMSHandler.cc` (`open`) and
  `FlagDataHandler.cc` (`generatePolarizationsMap`)

With a polarization whose `CORR_TYPE` is RX, RY, LX and LY (casacore Stokes
types 13 to 16), CASA 6.7.6.14's `flagdata(mode='summary')` warns
"Correlation product unknown found: 13" and "The 0 th correlation is
unknown: 13", then aborts the Python process with an uncaught
`std::out_of_range` from `map::at`; there is no summary. casa-rs names
every correlation by casacore's `Stokes::name`, so the summary keys those
correlations `RX`, `RY`, `LX` and `LY` (`listobs::stokes_name`).

## A subimage offsets a tabular coordinate's world values instead of moving its table

- Date noted: 2026-10-29
- Status: CASA bug; casa-rs does not follow it (#1001, PR #1005 review)
- Affected code: casacore `coordinates/Coordinates/CoordinateSystem.cc`
  (`subImageInSitu`) and `TabularCoordinate.cc` (`toWorld`,
  `setReferencePixel`)

A `TabularCoordinate` with a nonlinear table converts a pixel through its
channel corrector first and its linear part (`crval`, `cdelt`, `crpix`)
second. `subImageInSitu` moves a tabular coordinate's reference pixel by the
subimage's start, as for a linear axis, but the corrector stays in the input's
pixels. A subimage's world values are therefore the input's at the same
pixel, offset by `cdelt × start`, not the input's at the pixel they were cut
from. For a table at pixels `[0, 1, 3, 4]` with world values
`[10, 11, 15, 22]`, `coordsys.subimage(originshft=[0, 1])` gives world values
`[13, 14, 16]` at pixels 0 to 2, where the input has `[11, 13, 15]` (CASA
6.7.6). A tabular spectral coordinate is not affected: `subImageInSitu`
rebuilds its table over the subimage's channels. casa-rs moves the table's
pixels with the reference pixel (`casa_images` `imsubimage`), so a subimage
reads the input's world values at the pixels it was cut from. CASA's
`imsubimage` ignores a box on an image without a direction coordinate, the
only way casa-rs cuts a tabular coordinate's axis, so no task reaches this in
CASA.

## `tb.getcol` refuses a column of `int64`

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casatools `src/code/stdcasa/StdCasa/CasacSupport.cc`
  (`fromValueHolder`)

`tb.getcol` of a scalar column of `Int64` raises `RuntimeError: Unknown casa
DataType!` in CASA 6.7.6.14, while `tb.getcell` of the same column returns the
value as a Python `int` and `getcol` of an `Int` (32-bit) column returns an
`int64` array. `fromValueHolder`, which turns the column's `ValueHolder` into
the Python value, has a `TpInt64` case for a scalar but none for `TpArrayInt64`
(the converter for records in the same file has both). The recording is
`crates/casars-python/python/tests/data/casa_table_tool.json` (`cells`,
`getcol('id')`; the fixture's `id` column is `Int64`). casa-rs returns the
`int64` array, as it does for every other integer column.

## `tb.putkeyword` stores an int beyond 32 bits as 0

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casatools `ValueHolder` conversion for table keywords

`tb.putkeyword('name', 2**40)` returns `True` and `tb.getkeyword('name')` then
gives `0`, both for a new keyword and for an existing 32-bit `Int` one
(CASA 6.7.6.14; the `casa_bugs` scenario of
`crates/casars-python/python/tests/data/casa_table_tool.json`). The Python
`int` is narrowed to casacore's 32-bit `Int` without a range check. casa-rs
stores a new keyword whose value does not fit 32 bits as a 64-bit integer, and
refuses such a value for an existing 32-bit keyword with
`RuntimeError: ... does not fit the integer keyword NAME, which holds fewer
bits`, rather than store a wrong number.

`cl.putkeyword` uses the same CASA conversion: in CASA 6.7.6.14, writing
`2**40` to either a new keyword or an existing 32-bit keyword stores `0`.
casa-rs keeps a new value as a 64-bit integer and refuses an overflowing
replacement of a 32-bit keyword, so neither call silently loses the value
(#1053).

## `tb` slices with a wrong rank or a start outside the array abort the process

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casatools `table::getcellslice`, `getcolslice`, `putcellslice`
  and `putcolslice`, over casacore's `Slicer`

In CASA 6.7.6.14 a slice whose `blc`, `trc` or `incr` has the wrong number of
elements for the array, or whose `blc` lies outside the array, throws an
`ArraySlicerError` that nothing catches, and the Python process ends
(`libc++abi: terminating due to uncaught exception of type
casacore::ArraySlicerError`, exit status 134). The default `incr=[1]` is such a
slice for an array of more than one axis, so `tb.getcellslice('DATA', 0, [0, 0],
[1, 3])` aborts. casa-rs raises a `RuntimeError` for each of them and takes a
one-element `incr` as the step of every axis. The recording
(`crates/casars-python/python/tests/data/casa_table_tool.json`, `slices` and
`ms_slices`) holds only the cases CASA survives. CASA is also inconsistent
about `trc=-1`: `getcellslice` refuses it ("All elements of trc must be greater
than or equal to 0"), `getcolslice` takes it as the last element; casa-rs
follows both.

## `tb.putcell` and `putcol` wrap an int beyond 32 bits in an int cell

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casatools `ValueHolder` conversion to a column's type

`tb.putcell('ANTENNA1', 0, 2147483648)` returns `True` and the cell then holds
`-2147483648`; `-2147483649` becomes `2147483647` and `4294967297` becomes `1`
(CASA 6.7.6.14; the `int_overflow` scenario of
`crates/casars-python/python/tests/data/casa_table_tool.json`, which the
replay leaves out). `putcol` wraps the same way. A Python `int` is narrowed
to the 32-bit column type without a range check, as for `putkeyword` above.
casa-rs raises `RuntimeError: a value is out of range for a column of Int` and
leaves the cell, rather than store a wrong number. Astra's review of #1034
noted the departure.

## `tb.putcell` with several rows writes those before a missing one

- Date noted: 2026-10-30
- Status: CASA behaviour that casa-rs does not follow (#1025, the `table` tool)
- Affected code: casacore `TableProxy::putCell`

`tb.putcell('id', [0, 3], 22)` on a table of three rows writes row 0, then
fails on row 3 with `no such row`: `TableProxy::putCell` checks and writes one
row at a time, so a call that raises has still changed the table
(`rows_vector` scenario). casa-rs checks every row first and raises with the
table as it was. The recording follows the failed call with a write that puts
row 0 back, so that its later reads agree.

## `tb.addrows` with a negative count removes rows

- Date noted: 2026-10-31
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casacore `TableProxy::addRow` (the count is an `Int` handed
  to `Table::addRow(rownr_t)`)

`tb.addrows(-1)` on a table of three rows returns `True` and leaves two;
`addrows(-2)` leaves none, and the table reopens with no rows
(`addrows_negative` scenario). The count converts to a huge unsigned number
and the table's row count wraps. casa-rs raises `RuntimeError` ("the number
of rows must not be negative") and changes nothing.

## `tb.removerows` and `tb.addcols` do part of a call that fails

- Date noted: 2026-10-31
- Status: CASA behaviour that casa-rs does not follow (#1025, the `table`
  tool; Astra's review of #1036)
- Affected code: casacore `BaseTable::removeRow(Vector)` and
  `TableProxy::addColumns`

`tb.removerows([2, 2])` on a table of three rows removes the first row 2,
then raises "rownr 2 too high ... (#rows=2)", leaving two rows: the row
numbers are taken from the highest down and each is checked against the
table as it then is. (A number beyond the table, even among valid ones, fails
before any row goes: it is the highest.) `tb.addcols` of `aaa` and `id` on a
table that has `id` adds `aaa` and then raises "column already exists":
`addColumns` builds the whole description first (a malformed field fails the
call with nothing added) and then adds the columns one by one, in the
record's sorted order (`structure_partial` scenario). casa-rs checks the whole
call before it changes anything: both raise CASA's message and leave the table
as it was. The successful calls, repeats included (`removerows([0, 0])`
removes two rows), are CASA's.

## `importfits` reads every FITS date as UTC

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1017)
- Affected code: casacore `coordinates/Coordinates/FITSCoordinateUtil.cc`
  (`getObsInfo`, which reads `TIMESYS` from the header record, and the WCS
  date wcslib holds)

`exportfits` writes an image's observation date in the epoch's own time
scale, `DATE-OBS` with `TIMESYS` naming the scale (`ObsInfo::toFITS`).
`importfits` gives the date back with the same calendar value but labelled
UTC, whatever `TIMESYS` says. An image observed at 2020-05-31 12:00 TAI
exported by CASA 6.7.6 with `TIMESYS = 'TAI'` imports at 2020-05-31 12:00
UTC, 37 seconds later. Where the scale is lost was not pinned down:
`getObsInfo` reads `TIMESYS` from the header record and the date from
wcslib's `mjdobs`, so the scale is gone by then. casa-rs reads `MJD-OBS` or `DATE-OBS` in the scale `TIMESYS` names, UTC
when it names none (`casa_coordinates::fits::from_fits_header`), so the
round trip keeps the date.

## `feather` fails on an image whose direction is not on its first two axes

- Date noted: 2026-10-30
- Status: CASA limitation; casa-rs does not follow it (#1002, PR #1022
  review)
- Affected code: casatools `synthesis/MeasurementEquations/Feather.cc`
  (its FFT, cursor and weight-image paths)

`feather` finds an image's direction axes for part of its setup, but its
lattice cursors and FFTs assume the direction plane is the first two axes.
CASA 6.7.6.14's `feather` of a pair of 16 by 16 pixel images, one Stokes and
two channels, with 2″ and 6″ beams, succeeds over [RA, Dec, Stokes, Freq].
The same pair over [Stokes, RA, Dec, Freq] or [Freq, RA, Dec, Stokes], as
`imtrans` makes it, fails with `LatticeStepper::setCursorShape: cursorShape
<=0 or > latticeShape`. casa-rs feathers each direction plane wherever its
axes are. The transposed pair's result is the transpose of the
direction-first pair's (`casa-images` `casa_axis_maps` test
`feather_takes_the_direction_wherever_its_axes_are`), and the direction-first
result is checked against CASA's in `image_analysis_casa`.

## `tb.copyrows` adds the rows before it finds a cell it cannot convert, and wraps integers

- Date noted: 2026-10-31
- Status: CASA behaviour that casa-rs does not follow (#1025, the `table`
  tool; Astra's review of #1039)
- Affected code: casacore `TableCopy::copyRows` and `ROTableRow`/`TableRow::put`
  (`RecordInterface::as...`)

`copyRows` adds an output row and then puts its cells one by one. When a
shared column has a type the input's cell cannot be converted to (a `double`
output column for a `dcomplex` input, `RecordInterface::asDouble - invalid
data type`; an `int` output column for a `float` array input,
`TableRecordRep::get_pointer - incorrect data type used for field ...`), the
call raises with the rows already added and their cells never set: reading
them gives whatever the heap held (`copyrows_types` scenario of
`crates/casars-python/python/tests/data/casa_table_tool.json`, which does not
compare the table after a refusal). casa-rs checks the types before it changes
anything and raises with the output table as it was.

Which conversions CASA accepts was probed pair by pair and is what casa-rs
accepts (`tools/table/cell_convert.rs`), with one exception: an integer that
does not fit the output column's type is wrapped as a C cast does (a `Short`
-5 becomes the `uChar` 251, a `uInt` 4294967291), the same narrowing as
`putcell` above. casa-rs raises `RuntimeError: a value is out of range for a
column of ...`, before the output is touched, rather than store a wrong
number.

## Later `timerange` terms take their defaults from row 0

- Date noted: 2026-11-02
- Status: CASA quirk casa-rs follows for parity (#980,
  `casa-ms` `selection::time_expr`)
- Affected code: casacore `ms/MSSel/MSTimeParse.cc` (`getDefaults`)

A `timerange` time with missing fields (`09:00:00`, `*`, `2017/03/01`) takes
them, and a single time's tolerance and a bracketed range's edge width take
the `EXPOSURE`, from the "first logical row". `getDefaults` sets
`firstLogicalRow = 0` and searches for the first logical row only while the
static `defaultTimeComputed` is false, which its first call makes true. Every
comma-separated term calls it once, so the first term reads the first logical
row and every later term reads row 0. With `field='1'` on an MS whose FIELD 1
rows start a day after row 0, CASA 6.7.6.14 selects `10:30:00.5` on FIELD 1's
day in `10:30:00.5,10:40:00.5` but looks for `10:40:00.5` on row 0's day
(`fields` cases of
`resources/casa-compat/evidence/msselection-time-casa-20261102.json`).
casa-rs resolves the terms the same way (`TimeSelectorDefaults`).

## The `timerange` defaults ignore `FLAG_ROW` when other selections are set

- Date noted: 2026-11-02
- Status: CASA quirk casa-rs follows for parity (#980)
- Affected code: casacore `ms/MSSel/MSTimeParse.cc` (`getDefaults`)

`MSSelection` builds its time parser with `honourRowFlags` true. With no other
selection expression, the first logical row is the first row whose `FLAG_ROW`
is false. With one, `getDefaults` tests `FLAG_ROW` but both branches of the
test read the other expressions' condition the same way, so the first logical
row is the first row they select, flagged or not. casa-rs follows both
(`msselection_first_logical_row`).

## A refused bracketed `timerange` makes later ranges edge-inclusive

- Date noted: 2026-11-02
- Status: CASA bug casa-rs does not follow (#980)
- Affected code: casacore `ms/MSSel/MSTimeGram.yy` (`brangetimeexpr`)

The grammar keeps a bracketed range's state in the process globals
`MSTimeEdgeInclusiveRange` and `MSTimeEdgeBuffer`, set when `[` (or `N[`) is
read and cleared only when a range completes. A bracketed range that is
refused (`[09:00:00]`, `10.25[09:00:00~08:00:00]`) leaves them set, so the
next unbracketed range anywhere in the process is edge-inclusive with that
width until a range completes. In CASA 6.7.6.14, `09:00:00~09:01:00` after
`10.25[09:00:00~08:00:00]` selects 35 rows of a 2 s MS instead of 30, and
`09:00:00.5+00:00:30` after `0.0[09:00:10~09:00:00]` loses both end rows
(`bracket_state_leaks` in
`resources/casa-compat/evidence/msselection-time-casa-20261102.json`, whose
other cases each run after a completed range clears the state). casa-rs keeps
no state between parses.

## `MSIter` keeps the previous window's receptor angles for a window without its own `FEED` row

- Date noted: 2026-11-02
- Status: casacore behaviour that casa-rs does not follow (MSv2 adapter,
  `Feeds::receptors`)
- Affected code: casacore `ms/MeasurementSets/MSIter.cc` (`setFeedInfo`,
  lines 779-814 on `master`)

The MeasurementSet definition makes a `FEED` row with `SPECTRAL_WINDOW_ID`
-1 valid for every spectral window. `setFeedInfo` instead decides once per
table: if every row is -1 it uses them for every window, and otherwise
(`spwDepFeed_p`) it copies into its cache only the rows whose window is the
iterator's current one. A window with no row of its own for an antenna and
feed (a -1 row beside rows for other windows, or no row at all, as for
ALMA's WVR window) leaves that cache entry untouched: `receptorAngles_p`
is resized to the same shape, which casacore's `Array::resize` does not
reinitialise, so `receptorAngles()` gives the previous window's angles (or,
for the first window iterated, zeros: the new array's elements are
value-initialised). XRADIO 1.3.1's converter makes the
same per-dataset choice and writes such a dataset with no receptor angles.
casa-rs reads an antenna and feed's rows in the window, else its rows for
every window, and states no receptors where it has neither. Where `FEED`'s
rows are all -1 or all name windows, which is how CASA's simulator, ALMA
and casa-rs's MSv2 writer write it, the receptors are the same.

## `spw` "no match" warnings refuse the expression

- Date noted: 2026-11-02
- Status: CASA behaviour casa-rs follows for parity (#980, `casa-ms`
  `selection::spw_expr`)
- Affected code: casacore `ms/MSSel/MSSSpwErrorHandler.h`,
  `MSSelection.cc` (`initErrorHandler`, `setErrorHandler`)

`MSSSpwErrorHandler` only logs its reports (`handleError` posts a `WARN`), so
a `spw` term matching nothing (`0,7` on a 4-window MeasurementSet, an unknown
name) looks meant to be a warning. But it does not override `clone()`, and
`MSSelection` installs `mssEH->clone()`, a base `MSSelectionErrorHandler`,
which throws: CASA 6.7.6.14 refuses `0,7` with "Spw Expression: No match
found for 7," (`resources/casa-compat/evidence/msselection-spw-casa-20261102.json`).
casa-rs refuses it too.

## A `spw` frequency range with a negative step can loop forever

- Date noted: 2026-11-02
- Status: CASA bug casa-rs does not follow (#980)
- Affected code: casacore `ms/MSSel/MSSpwIndex.cc` (`matchFrequencyRange`)

A stepped window range `f0~f1^s` with `s < 0` steps by each window's least
`CHAN_WIDTH`, in single precision. A window whose widths are negative then
steps away from `f1` and never leaves the loop; a step too small to move a
single-precision frequency stalls it. CASA 6.7.6.14's `ms.msselect` does not
return for `4GHz~5GHz^-1MHz` on a MeasurementSet with a descending window
(`not_returning` in the evidence file). casa-rs refuses such expressions.

## `spw` channel quirks casa-rs follows

- Date noted: 2026-11-02
- Status: CASA quirks casa-rs follows for parity (#980)
- Affected code: casacore `ms/MSSel/MSSpwIndex.cc`, `MSSpwParse.cc`,
  `MSSpwGram.yy`

A single window frequency (`1.4GHz`) matches a window within its largest
*signed* `CHAN_WIDTH` of the window's midpoint, so never a window whose widths
are negative. A channel range in hertz reads the channel order from the sign
of the first `CHAN_WIDTH`, not from the frequencies. When a window's first
frequency channel range finds no channel, `selectChannelsFromIDList` skips
one range instead of the window's ranges, so the next window takes the
skipped window's later ranges (`0~2:4.5GHz~4.55GHz;0~1` gives window 2 the
ranges `0~1` and `-1~63`). In a window list, a frequency replaces the list
but not its write position (`100GHz~101GHz;0` lists the matched windows, then
window 0). `( ... )` skips `endOfCeremony`, so the expression keeps every MAIN
row. All are in the evidence file; `casa-ms` reproduces them, and `tclean`
refuses the parenthesized form and a window kept without channels, whose
imaging in CASA is not established.

## A tuned cube selection compares `outframe` frequencies with LSRK ones

- Date noted: 2026-11-03
- Status: CASA bug casa-rs does not follow (#980,
  `casa-imaging-application` `compile::frequency_bands`)
- Affected code: casatools `synthesis/ImagerObjects/SynthesisImagerVi2.cc`
  (`selectData`, `andFreqSelection`), `SynthesisImager.cc`
  (`tuneSelectData`), `CubeMajorCycleAlgorithm.cc` (`task`)

A cube is gridded by `CubeMajorCycleAlgorithm`'s own `SynthesisImagerVi2`.
Its `selectData` states each channel range `spw` names as a frequency band in
tclean's `outframe` (`MSUtil::getFreqRangeInSpw` over the range's channel
centres at every time of the window's rows) and stores it in `freqBegs_p` and
`freqEnds_p`. Where `tuneSelectData` tunes the selection (the first visibility
buffer's window has 30 or more channels, or the model is saved to
`MODEL_DATA`), it calls `selectData` again with the image's band, pixel −1.5 to
`nchan + 0.5`, in LSRK, and `andFreqSelection` narrows each stored band that
band overlaps to their intersection, without converting either: a band in
`TOPO` or `BARY` numbers is intersected with one in LSRK numbers, and the
result is read as LSRK (`FrequencySelectionUsingFrame(LSRK)`). The two frames
differ by up to tens of km/s, so with channels narrower than that, an image
whose band overlaps the stored one in those numbers loses the image's edge
channels (or gains channels CASA never meant to read) at every time. The same
`andFreqSelection` narrows two of a window's own channel ranges to their
intersection when a Doppler shift across the track makes their bands
overlap, which keeps almost nothing of either. casa-rs states a tuned
selection's bands in LSRK before intersecting them with the image's, and
keeps each named range's band whole; with `outframe='LSRK'`, the default, and
ranges further apart than the shift, the selections agree. No case of CASA's
test suite reaches either difference: the one tuned cube with channels
narrower than the shift, `test_cube_D2`, images in LSRK, where casa-rs and
CASA select the same channels at every time (its edge plane keeps 2460.56 of
5020.51 in both).

## A TOPO cube can gain or lose an endpoint with CASA's aberration cache history

In CASA 6.7.6.14, the image's TOPO spectral coordinate has an LSRK conversion
layer. `FTMachine::initMaps` evaluates image-plane frequencies through that
layer and `FTMachine::interpolateFrequencyTogrid` applies strict interpolation
bounds to the resulting frequencies and the row's LSRK channels.

The `GEO_BARY` hop evaluates `Aberration` with coefficients from the shared
`MeasTable::theirMulAber` cache. `MeasTableMul::getArray` may reuse the
coefficients of a nearby epoch. Pre-populating that cache before an identical
`tclean` run moves the converted grid by a few floating-point steps. In the
original out-of-order-row VLA fixture, the last plane's sum of weights changed
from 16,697.6055 to zero. Two nearby frequency grids also flipped a whole
plane, in opposite directions. A separate fresh-coefficients probe reproduced
the flips. [Reproduction and measurements in #1198](https://github.com/bglenden/casa-rs/issues/1198#issuecomment-6800135096).

For a channel-index TOPO cube in the standard gridder that spans one LSRK
spectral window at unit width with natural weighting, casa-rs can compare in
the source coordinate when the rounded terminal centre coincides with the
stored edge. It compares the exact requested span `(n - 1) * d` to the exact
span between the stored endpoint centres. This distinguishes an in-band
terminal centre from a genuinely out-of-band one even when both round to
the stored edge. Saved controls fall 7/32 of a representable step inside
and 3/8 of a step outside. The source proof retains the former and excludes
the latter from gridding.

The ordinary strict row bounds and flag rules still apply. The full TOPO
image coordinate, primary-beam calculation and prediction basis retain
all requested channels.

A tracked BARY cube can lose the same physical endpoint through a different
round trip. On the three-channel `refim_point_linRL.ms` TOPO input with
`start=0, width=1`, CASA's TOPO and BARY image centres describe the same
frequencies. The first LSRK row frequency is 999988784.6184165 Hz.
CASA's TOPO grid evaluates the first centre one representable step above
it, while its BARY grid evaluates that centre one step below. The following
13 buffers are at least 83 Hz above the first image centre and legitimately
exclude it. Both the standard and AW CASA gridders drop the whole first
plane in the BARY request. The release AW comparison has 94,625 differing
mask pixels on that plane and takes four clean iterations instead of three.

casa-rs retains the first buffer's contribution in both coordinates. CASA's
TOPO AW run on the same rows and cache supplies the reference for the
BARY products. CASA Measures verifies the corresponding image centres
in LSRK within 0.5 microhertz. The products, masks and clean counts agree
against that reference. A separate explicit BARY start nearly 99 kHz below
the first channel gives a blank first plane in both standard gridders.

## A cube's fine frequency grid lets rounding choose one or two samples per plane

- Date noted: 2026-11-03
- Status: CASA rounding casa-rs does not follow (#980;
  `test_cube.test_cube_5` differs by it)
- Affected code: casatools `synthesis/TransformMachines2/FTMachine.cc`
  (`interpolateFrequencyTogrid`)

For linear interpolation onto planes wider than a channel, CASA places
`floor(width)` points per plane, with `width` the ratio of the plane
spacing (`imageFreq_p`, from the image's spectral coordinate and its LSRK
conversion layer) to the visibility buffer's first channel spacing (its
channels converted to LSRK by the visibility iterator). Where planes are a
whole number of channels wide, the ratio is that number exactly at the
image's epoch, and after it the Doppler shift moves it by about 1e-6, below
or above. `test_cube_5` (`refim_point.ms`, 50 MHz TOPO channels,
`width='100MHz'`, `outframe='TOPO'`) has a ratio of 2 at its first
integration: the image's epoch is that integration's time and its
direction the field's to the last bit, and the shift then lowers the ratio
below 2 at every later integration, where both implementations place one
point per plane. At the first integration CASA's two conversions round
apart and its ratio falls below 2, so it places one point there too;
casa-rs's ratio is 2 to rounding and it places two. Every plane of casa-rs's
cube therefore holds the first integration's weight twice: its sum of
weights exceeds CASA's by exactly that integration's share of the track,
0.0017154 (the rows of one of its 480 integrations, unflagged, over all),
and its PSFs differ by 7e-4 to 1.3e-3 normalized RMS, the returned
`maxpsfsidelobe` by 3e-4. Which side of 2 a ratio that is 2 exactly falls
on is rounding, which casa-rs does not reproduce (`AGENTS.md`: a near-tie
broken the other way is equally valid).

## A cube without a per-channel density leaves `<imagename>.gridwt` behind

- Date noted: 2026-11-03
- Status: CASA behaviour casa-rs does not follow (#980;
  `test_onefield.test_onefield_pcwdT_and_pcwdF.2`'s inventory differs by it)
- Affected code: casatools `synthesis/ImagerObjects/SynthesisImagerVi2.cc`
  (`runCubeGridding`), `SynthesisImager.cc` (`getWeightDensity`,
  `setWeightDensity`), `SIImageStore.cc` (`gridwt`),
  `CubeMajorCycleAlgorithm.cc` (`task`)

A cube weighted by Briggs or uniform weighting with
`perchanweightdensity=False` forms one `VisImagingWeight` density in the
imager defining the cube. To hand it to the `CubeMajorCycleAlgorithm`
imagers that grid the cube, `runCubeGridding` asks `getWeightDensity`, which
writes the density grid to a new paged image `<imagename>.gridwt` and passes
its name in the control record (`weightdensity`); each gridding imager reads
it back with `setWeightDensity`. Nothing removes the file. A later run into
the same name forms its density again and overwrites it, and reads the file
only for the shape of an image store that has no other product
(`SIImageStore`'s constructor, whose comment there asks "How can this be
right ?"). Its
pixels are a uv-plane density, but its coordinates are the image's sky
coordinates, with a `FIELD_ORDER` axis added for several fields. casa-rs
forms the density once and grids with it in the same process, so it writes
no such file; CASA 6.7.6.14 writes `tst2.gridwt` in
`test_onefield_pcwdT_and_pcwdF`'s second call, whose other products agree.

## W projection grids a support the grid's edge cuts, and never its last row and column

- Date noted: 2026-11-03
- Status: CASA behaviour casa-rs follows for parity (#980,
  `casa-imaging-operator` `GridEdge::Sectioned`)
- Affected code: casatools `synthesis/TransformMachines2/WProjectFT.cc`
  (`put`), `FTMachine.cc` (`findGridSector`, `tweakGridSector`),
  `synthesis/fortran/wprojgrid.f` (`sectgwgridd`, `sectgwgrids`,
  `onmywgrid`, `sectdwgrid`, `owp`)

`WProjectFT::put` grids through the sectioned `sectgwgridd`: each sector
`findGridSector` cuts grids the taps of a sample that fall on it and adds
`weight · Σ Re(cwt)` over those taps to `sumwt`, wherever the sample's anchor
lies (`onmywgrid` asks only that the support meet the sector). A support
that overruns the grid is therefore gridded in part and weighed by the part,
where the unsectioned `gwgrid` and the degridder (`sectdwgrid` through
`owp`) drop it. The sectors start at the grid's first column and row (both
`findGridSector` and `tweakGridSector` add one to the 0-based start, giving
the 1-based `x0 = 1`, and `onmywgrid` clips a support's low end to it), and
`findGridSector` cuts the last one short (`nxsub = nxp - x0 - 1` before that
increment), so they cover every column and row but the grid's last: no tap
lands before the grid, and the last column and row are never gridded,
though `owp` predicts a support that reaches them. casa-rs grids the taps
on the sectors and weighs them, as CASA does. Dropping the whole sample, as
casa-rs did, left `test_cube_wproj_onefield`'s 1.5 and 1.8 GHz planes
(`refim_oneshiftpoint.mosaic.ms`, four planes, whose supports overrun the
grid at those frequencies) short of CASA's `sumwt` by 3.06 and 14.57 and
their PSFs 3.6% and 5.3% apart in normalized RMS; they now agree with CASA
6.7.6.14 to 8e-7. (An earlier reading of the sectors as starting at the
0-based column 0, so that a tap one column below the grid would be written
before it and weighed, mistook `findGridSector`'s start before its final
increment for the start it returns.)

## The standard gridder drops the final Fourier-grid row and column

- Date noted: 2026-11-12
- Status: CASA 6.7.6.14 bug. Casa-rs keeps the full support of samples that fit the standard grid. [MVC comparison](https://github.com/bglenden/casa-rs/issues/1069#issuecomment-6801927969)
- Affected code: `synthesis/TransformMachines2/GridFT.cc` (`put`),
  `FTMachine.cc` (`findGridSector`) and `synthesis/fortran/fgridft.f`
  (`sectggridd`, `onmygrid`).

`onmygrid` accepts a standard-kernel support that fits the entire grid.
`findGridSector` shortens the final sector by one extra cell before it
converts the sector start to one-based indices. For one sector on a
320-square grid, CASA passes `(x0, y0, nxsub, nysub) = (1, 1, 319, 319)`.
The sectioned Fortran kernel consequently drops taps on the final row or
column and omits their fraction of the weight. The omitted taps shift the
cube PSF and residual upstream of spectral-index masking.

In the frozen 256-square, 0.3-arcsec, three-channel dirty MVC comparison,
CASA's unmodified replay reproduces the reference exactly. Its `.alpha`
and `.alpha.error` validity masks differ from casa-rs at the same two
pixels near the strict residual-peak cutoff. Replaying the captured CASA
gridding inputs with its short sector reconstructs its Fourier grid to
`6.1e-16` normalized RMS. Extending only those two sector lengths to 320
in a diagnostic CASA process yields agreement on all 19 products under
the existing limits, with identical masks and maximum NRMS `7.27e-6`.
The standard operator's edge regression keeps nonzero taps and full
weight on both boundaries.

## A CRTF spectral range rounds each end to a channel, toward zero

- Date noted: 2026-11-03
- Status: CASA quirk casa-rs follows for parity (#980)
- Affected code: casatools `imageanalysis/Annotations/AnnRegion.cc`
  (`_extend`), casacore `images/Regions/WCBox.cc` (`doToLCRegion`),
  `lattices/LRegions/LCBox.cc`

A region's `range=[f1, f2]` masks the channels from `Int(p1 + 0.5)` to
`Int(p2 + 0.5)`, where `p1 ≤ p2` are the ends' pixels on the image's spectral
axis. `Int` truncates toward zero, then the low end is clamped to 0 and the
high end to the last channel. A region whose ends cross is left out, and the
other regions are kept. Because of the truncation, a range ending up to 1.5
channels below the cube masks channel 0, while one starting half a channel
above it masks nothing. An MFS image's one channel is its whole bandwidth
wide. casa-rs follows this, measured with CASA 6.7.6.14 on
`refim_point.ms`'s cube and MFS image: `rg.fromtext` and tclean's `.mask`
for 20 ranges, in the unit test
`compile::domains::tests::a_spectral_range_masks_the_channels_casa_masks`.

## A mask string of several lines is an empty mask in `tclean`

- Date noted: 2026-11-03
- Status: CASA bug casa-rs corrects (#980)
- Affected code: casatools `synthesis/ImagerObjects/SDMaskHandler.cc`
  (reading `mask` text)

CASA 6.7.6.14's tclean given `mask='circle[[50pix,50pix],10pix]\nbox[[10pix,10pix],[12pix,12pix]]'`
makes an empty mask and stops at code 7 without cleaning. The same regions
as a list, or as lines of a region file, mask both regions. casa-rs reads
every line of the text as a region of the mask, as the list and the file
read.

## `tclean` checks a `vis` list for repeats of its first entry only

- Date noted: 2026-11-03
- Status: CASA bug casa-rs corrects (#980)
- Affected code: casatasks `private/mslisthelper.py` (`check_mslist`), called
  by `ImagerParameters.checkAndFixSelectionPars`

CASA 6.7.6.14's tclean refuses `vis=['a.ms', 'a.ms']` with "a.ms is contained
in the list more than once." (ValueError, before any imaging). The check
compares each later entry with the first one only, after the list is sorted by
time, and as text. So `['a.ms', 'b.ms', 'b.ms']` and `['a.ms', './a.ms']` pass
it, and their samples are read twice. The message states the evident intent:
no MeasurementSet twice. casa-rs refuses any repeat, comparing canonical paths,
with CASA's message. Evidence: CASA's refusal of `[refim_point.ms,
refim_point.ms]` on the oracle host, 2026-11-03, and the T1 law
`vis_list::a_measurement_set_named_twice_is_refused`.

## A common beam's circular case is the reciprocal of its axis

- Date noted: 2026-11-03
- Status: CASA bug casa-rs corrects (#980)
- Affected code: casatools `imageanalysis/ImageAnalysis/CasaImageBeamSet.cc`
  (`_transformEllipseByScaling`, used by `getCommonBeam`)

`_transformEllipseByScaling` computes a scaled ellipse's axes as
`c1 = sqrt(1/k1)` and `c2 = sqrt(1/k2)`. When they are exactly equal, a circle,
it returns `sqrt(k1)` for both axes instead: the reciprocal of the axis. That
contradicts the general branch and the function's evident intent. casa-rs
returns the axis. Both take the circle branch only for exactly equal axes, which
scaling rarely produces. An earlier casa-rs absolute tolerance of 1e-12 took
sub-arcsecond beams in radians for circles of the reciprocal axis, about 3e6,
and the common-beam search then never ended. casa-rs works in radians where
CASA works in arcseconds; the common beams agree within one of the algorithm's
0.1% growth steps (`casa-numerics` law
`the_common_beam_of_sub_arcsecond_beams_is_found_whatever_the_unit`).
The two searches can stop at different enclosing beams. For four nearly
equal channel beams (axis ratio 1.17, the simulation round trip's cube,
2026-11-03), the axes differ by 2.2e-4 and the position angle by 0.036°.

## `tclean` refuses a projection name with spaces round it

- Date noted: 2026-11-03
- Status: a casa-rs choice (#1064)
- Affected code: casatools `synthesis/ImagerObjects/SynthesisUtilMethods.cc`
  (`SynthesisParamsImage::fromRecord`, reading `projection`)

CASA 6.7.6.14's tclean refuses `projection=' TAN'` with "Invalid projection
code :  TAN", as it refuses `'tan'` and `'XYZ'`: casacore's
`Projection::type` reads the name exactly. casa-rs's parameter catalog trims
every string parameter before tclean reads it (`projection`'s normalization
is `trim`), so casa-rs images `' TAN'` in TAN. A name of the wrong case or
one casacore does not know is refused as CASA refuses it. No result of
CASA's depends on the refusal, and following it would make `projection` the
one string parameter the catalog does not trim. Evidence: CASA's refusals in
`crates/casa-imaging-application/tests/data/projections-casa-6.7.6.14.json`,
and the `tclean_tests` rows `casa_values_become_the_request_casa_rs_runs`
and `values_casa_rs_cannot_honour_are_refused`.

## NCP on the celestial equator places every pixel at NaN

- Date noted: 2026-11-03
- Status: CASA defect casa-rs corrects by refusing (#1064)
- Affected code: casatools `synthesis/ImagerObjects/SynthesisUtilMethods.cc`
  (`SynthesisParamsImage::buildCoordinateSystem`)

CASA writes `projection='NCP'` as SIN slanted by `[0, cot δ₀]`, with no
guard for an image centred on the celestial equator, where `cot δ₀` is
infinite: the image records `[0, inf]` and every pixel's world direction is
NaN, so no pointing, beam or mask can be placed on it. casa-rs refuses
such a run before imaging, with an error that says why
(`PrepareError::NcpAtEquator`, the law
`ncp_on_the_celestial_equator_is_refused`). Any other declination, however
close to 0, is imaged as CASA images it.

## Cubic spectral interpolation reads past a short axis and fits flagged values

- Date noted: 2026-11-03
- Status: CASA bug casa-rs corrects (#1066)
- Affected code: casacore `scimath/Mathematics/InterpolateArray1D.tcc`
  (`interpolatePtr`'s `cubic` case and `polynomialInterpolation`), called by
  casatools `synthesis/TransformMachines2/FTMachine.cc`
  (`interpolateFrequencyTogrid`, `interpolateFrequencyFromgrid`)

tclean's `interpolation='cubic'` fits casacore's cubic polynomial through the
four points about each frequency. `polynomialInterpolation` takes four
points whatever the axis holds (a `DebugAssert` only), so a row of two or
three selected channels, or an image of two output channels, reads past the
end of its arrays. casa-rs fits the polynomial through every point such an
axis has. The cubic case also notes "TODO: implement flags properly - ie
don't use flagged points": a sample takes the flags of the two channels it
lies between, but its value fits all four, flagged ones included, so a
flagged channel stored as NaN makes an unflagged sample NaN and the image
NaN with it. casa-rs fits a flagged channel's stored value as CASA does and
counts a non-finite one as zero. The model half follows CASA too: a residual
sample is `Σ f·(V − M)` over its four channels, and the model `M` is zero at
a flagged one, since `interpolateFrequencyFromgrid` writes the prediction
through `swapyz(flipgrid, copyOfFlag, flipdata)`, which skips every entry
`vb.flagCube()` flags. Weights are interpolated the same way;
`VisImagingWeight` has already given a flagged channel zero weight, which
casa-rs follows. Evidence: the operator laws in
`casa-imaging-operator/tests/cubic.rs` and the cubic cases of
`resources/casa-compat/tclean-cube-axes-cases.json`.

## A one-channel start model is regridded onto a cube by reading past its axis

- Date noted: 2026-11-03
- Status: CASA bug casa-rs corrects, by refusing (#1067)
- Affected code: casatools `synthesis/ImagerObjects/SIImageStore.cc`
  (`regridToModelImage`), casacore `images/Images/ImageRegrid.tcc`
  (`regrid1D`) and `scimath/Mathematics/InterpolateArray1D.tcc` (`linear`)

`SIImageStore::regridToModelImage` regrids a start model over its direction
and spectral axes with `ImageRegrid` and `Interpolate2D::LINEAR`. Along
frequency, `regrid1D` hands casacore's linear interpolation the input's
channel pixels, and for a model of one channel onto a cube of several the
interpolation steps its bracketing index to -1 and reads before the start
of its arrays. casa-rs refuses a one-channel start model on a cube's
spectral axis ("a spectral regrid interpolates between two channels"). A
one-plane model of an MFS image keeps its plane at any frequency, as CASA's
regrid leaves a degenerate axis. Beyond the model's channels CASA's
interpolation takes the end channel's value and only masks it; the model
image has no mask, so those planes start from the end channel's model.
casa-rs follows that, and also CASA's restoration of such a plane where no
sample reaches it: `SIImageStore::restore` restores every plane, and
`makeImageBeamSet` gives a blank PSF the largest beam, so the restored image
there is the model convolved with that beam. Evidence: `casa-images` `regrid::spectral` laws, the
start-model axis laws, and CASA's `test_modelvis_7` and
`test_startmodel_regrid_cube`.

## A restarted AW cube's weight image grows with the other channels' sums

- Date noted: 2026-11-03
- Status: CASA bug casa-rs does not follow (#719)
- Affected code: casatools `synthesis/TransformMachines2/CFCache.cc`
  (`CFCache::loadWtImage`), `synthesis/ImagerObjects/SIImageStore.cc`
  (`divideWeightBySumwt`, `divideResidualByWeight`)

`SynthesisImagerVi2` names the run's image as the weight prefix
(`setWtImagePrefix`), so `AWProjectWBFT::initializeToSky` reads an existing
`.weight` back as its average PB (`CFCache::loadAvgPB`), and `loadWtImage`
multiplies it by `max(sumWtTmp.get())`: the largest sum of weights over all
channels. `.weight` was divided by each channel's own sum
(`divideWeightBySumwt`), so a second tclean call on the same image name scales
channel `c`'s weight by `max(sumwt)/sumwt[c]`. `divideResidualByWeight`, which
divides by the PB peak times the weight's root, then moves its residual and
every product after it. The result depends on whether an earlier call made the
image and on how other channels were flagged, and no sky could change it. Each
reload also applies `AWProjectFT::getWeightImage`'s sinc correction again, a
smaller effect of about 1e-5 NRMS that a channel with the largest sum keeps.

`refim_oneshiftpoint.mosaic.ms`, three channels, `niter=10`: a fresh call's
`.weight` peaks are 1.0000, 1.0041 and 1.0233 and its channel 2 residual peak
is 0.1019; after a `niter=0` call on that name they are 1.0000, 1.0081 and
1.0472, and 0.1139. casa-rs computes the weight image afresh, and agrees with
CASA's fresh call within 5.7e-7 NRMS on every product; on one window, where
`max(sumwt)` is the channel's own, CASA's restart also stays consistent and
agrees within 5e-5. `test_mosaic_cube.test_cube_awproject_*` make that second
call, so they differ from casa-rs on the channels the reload scales. Evidence:
`resources/casa-compat/evidence/tclean-aw-cube-20261103.json`.

## A one-channel AW cube's restart writes its images with no default mask

- Date noted: 2026-11-03
- Status: a CASA inconsistency casa-rs does not follow (#719)
- Affected code: not traced

A second tclean call on the same image name, on a cube of one channel with
`gridder='awproject'`, leaves CASA's `.image` and `.residual` with no default
mask, where a fresh call and a restart of a cube of several channels have
`mask0`. casa-rs writes `mask0`, the PB cut, each time. The pixels agree within
5e-5 NRMS. `aw_restart_one_window` in
`resources/casa-compat/tclean-aw-cube-cases.json` shows it and fails on the
mask alone.

## A cube's beam for a plane without one depends on how memory splits the cube

- Date noted: 2026-11-04
- Status: CASA behaviour; casa-rs does not follow it (#1130)
- Affected code: `CubeMajorCycleAlgorithm.cc` (`dividePSFByWeight`, `makeImageBeamSet`
  per chunk)

A blank plane, or one whose PSF peaks off-centre, takes the largest beam of the
image store that `makeImageBeamSet` runs on. In a cube each chunk runs it on its own
sub-store, and the number of chunks follows the memory available. Forcing two
sub-cubes through `system.resources.memory`, on `refim_point_linRL.ms` at 4 channels:

| Run | Blank channel 3's beam | The store's largest |
|---|---|---|
| 1 sub-cube | 130.47" | 130.47" |
| 2 sub-cubes | 98.12" | 130.47" |

With 2 sub-cubes channel 0 also stops being off-centre, peaking at the centre with
a beam of 196.38". So a large cube's beams and Clark patches can depend on the
machine. casa-rs takes the largest of the whole image domain, which is CASA's answer
for one sub-cube, and doesn't depend on the machine.

## Multiscale misplaces a component on a plane whose PSF peaks off-centre

- Date noted: 2026-11-04
- Status: CASA bug; casa-rs does not follow it (#1130)
- Affected code: `MatrixCleaner::clean`, `MatrixCleaner.cc` (`modelSub`, `scaleSub`)

`MatrixCleaner` updates the model and the residual through a pair of boxes, one
about the position it found and one about the PSF's peak (`blcPsf = blc +
itsPositionPeakPsf - positionOptimum`). It reads the model's update from the scale
image through the second box, though the scale's delta sits at the image's centre.
`LCBox::verify` clips the two boxes separately and `makeBoxesSameSize` trims them
to one size, which needn't keep them paired.

So a component is exact only when the PSF peaks at `nx/2, ny/2`. Otherwise the
model takes it at about `position + centre - psf_peak`, and where a box clips, the
PSF is subtracted away from the position too, and a scale-0 component can miss the
model altogether. This applies to any `itsPositionPeakPsf` off the centre, which
`findPSFMaxAbs` looks for only in the central box, and not just to a plane the beam
fit calls off-centre (outside the inner quarter).

`refim_point_linRL.ms`, `stokes='I', specmode='cube', nchan=2, imsize=100,
cell='8.0arcsec', gain=0.17, niter=20, deconvolver='multiscale', scales=[0,3]`.
Channel 0's PSF peaks at (87,36), with 0.99982 at the centre, and CASA logs that
peak. A component found there lands at (50,50), where the first one appears in
`.model`. Later ones are further off: after 20 iterations CASA's channel 0 model
holds negative components such as (20,61) at -0.527, and its peak stays at 0.843.

| Channel 0 | CASA 6.7.6.14 | casa-rs |
|---|---|---|
| First component in `.model` | (50,50) | (87,36) |
| Iterations in the first cycle | 20 | 6 |
| Peak after that cycle | 0.843 | 0.733 |

`off_centre_multiscale` in `tclean-psf-off-centre-cases.json` differs for this
reason, by 1.2 NRMS on `.model`. casa-rs stores a component where it subtracted it,
at its position. A plane whose PSF peaks at its centre is unchanged. Hogbom and
Clark place a component at its position, and `MultiTermMatrixCleaner` cuts its
scales at the image's centre, so a one-term mtmfs placed it correctly when tried
(two terms weren't verified).

## A flat-noise cube predicts its start model without dividing it by the PB

- Date noted: 2026-11-04
- Status: CASA bug; casa-rs does not follow it (#1127)
- Affected code: `CubeMajorCycleAlgorithm::subImageStore` (by reading; not
  instrumented)

`.model` of a flat-noise image is the physical model times the PB, so a run must
divide a start model by the PB before it predicts, as
`imager_base.runMajorCycle` does through `dividemodelbyweight` for an MFS image.
The cube path divides the sub-image's model, then, in `subImageStore`,
`setModelImage` copies the raw start model over it, so the prediction uses
apparent units.

Two calls on `refim_oneshiftpoint.mosaic.ms` with `gridder='mosaic',
specmode='cube', imsize=256, cell='10.0arcsec', spw='0', phasecenter=0, gain=0.2,
pblimit=0.1`, the first with `niter=20`, the second continuing with
`niter=10, startmodel=<first>.model`:

| Peak residual | CASA 6.7.6.14 | casa-rs |
|---|---|---|
| The first call's last | 0.0139643 | 0.0139643 |
| The second call's first | 0.0196765 | 0.0139643 |
| The second call's first, the same two calls as MFS | 0.0139643 | 0.0139643 |

A run that continues from its own model should start from the residual it ended
with. CASA's cube doesn't: the gap, 0.0057, is about the 1.1 Jy component times
`1/PB - 1` at its pixel, where the PB is 0.9947. casa-rs divides the model, as
CASA's MFS does. `tools/casa-compat/tclean_summary_matrix.py --case
mosaic-seed-cube --case mosaic-seed-cube-no-cut` shows it, and those two cases
differ for this reason.

## Automatic-mask convolution requires square pixels in CASA

- Date noted: 2026-11-05
- Status: CASA 6.7.6.14 limitation; casa-rs keeps supporting rectangular pixels (#1140)
- Affected code: `SDMaskHandler::autoMaskByMultiThreshold` and
  `Image2DConvolver::convolve`

With `usemask='auto-multithresh'`, `imsize=64`, `cell=['8arcsec',
'16arcsec']`, a residual with a 5-by-5-pixel island and a stored 72-arcsec
circular beam, CASA fails when it reaches convolution: “Pixels must be square,
please regrid your image so that they are.” `Image2DConvolver::convolve` checks
the two direction increments and throws before it convolves. casa-rs keeps its
existing rectangular-cell support: pruning divides the beam solid angle by the
product of the two pixel increments, and smoothing evaluates the Gaussian at
both angular pixel offsets. The T1 restart law
`tclean_automatic_restart_prefers_the_residual_beam` checks the resulting mask;
`rectangular_pixel_smoothing_uses_both_cell_sizes` checks a rotated Gaussian.
The CASA failure was observed directly; there is no CASA mask to compare.

## CASA misreads big-endian integer quantity arrays

- Date noted: 2026-11-05
- Status: CASA bug; casa-rs does not follow it (#1183).
- Evidence: `tools/casa-compat/quantity_record_endian_oracle.py`, CASA 6.7.6.14 on a little-endian Mac.

A quantity record containing `np.array([1], dtype=">i4")` returns 16777216 m in CASA; the native-endian array returns 1 m. Big-endian 16-bit arrays return 256 m and 64-bit arrays return 0 m after integer narrowing. Signed and unsigned integer arrays show the same byte-order corruption, and arithmetic uses the corrupted values. casa-rs reads the numerical array value and returns 1 m for each. It still applies CASA's signed 32-bit narrowing to that value.

## CASA discards the values of bare complex quantity arrays

- Date noted: 2026-11-05
- Status: CASA silently loses input values; casa-rs raises an error (#1170).
- Evidence: `tools/casa-compat/quantity_record_complex_array_oracle.py`, CASA 6.7.6-14 on the Mac mini.

`qa.quantity(np.array([1+2j]))` returns a dimensionless zero in CASA; the
same happens for zero-dimensional and multidimensional complex64 and
complex128 arrays. A complex number cannot be converted to a real quantity
without losing information, so casa-rs raises `Wrong QuantumHolder to convert
asQuantumDouble` instead. Complex arrays used as ordinary record data retain
their values and shape.

## CASA discards bare NumPy complex64 quantity values

- Date noted: 2026-11-05
- Status: CASA silently loses input values; casa-rs raises an error (#1170).
- Evidence: `tools/casa-compat/quantity_bare_complex_scalar_oracle.py`, CASA 6.7.6-14 on the Mac mini.

`qa.quantity(np.complex64(1+2j))` returns a dimensionless zero in CASA,
while an equivalent Python complex or NumPy complex128 value raises an error.
CASA also treats the complex64 value as zero in other quantity operations.
casa-rs preserves both parts and raises `Wrong QuantumHolder to convert
asQuantumDouble` rather than silently discarding them.

## A non-frequency component unit corrupts CASA's spectrum

- Date noted: 2026-11-06
- Status: CASA changes the spectrum before raising and later aborts; casa-rs refuses the unit without changing the spectrum (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/casa-compat/componentlist_convertfrequnit_oracle.py` and `tools/casa-compat/componentlist_setfreq_oracle.py`, CASA 6.7.6-14 on the Mac mini, with failing cases isolated and core dumps disabled.

After adding a component with frequency `1.4GHz`,
`cl.convertfrequnit(0, 'm')` raises a `RuntimeError` in CASA, but it has
already installed the incompatible unit. A subsequent `cl.getspectrum(0)`
also raises, and closing the component list aborts the process. casa-rs
checks the unit first, raises a catchable error and leaves the frequency,
its record unit and the component list unchanged.

`cl.setfreq(0, 1, 'm')` also changes the spectrum before raising; the next
read raises and closing aborts. casa-rs rejects this unit before changing
either the reference frequency or its record unit.

The same failure occurs if a stored spectrum uses `MHz` and a later
`qa.define('Hz', '1m')` makes the current frequency definition incompatible.
casa-rs uses coherent Hz for the stored physical frequency in that case,
retaining a readable record and allowing an attached table to be saved.

## Component flux conversion can relabel incompatible units or crash

- Date noted: 2026-11-06
- Status: CASA silently changes the dimensions of a flux and crashes on an empty list; casa-rs refuses both without changing components (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/casa-compat/componentlist_convertfluxunit_oracle.py`, CASA 6.7.6-14 on the Mac mini; the empty-list call runs in an isolated process with core dumps disabled.

A component-list table may hold a flux in `Jy.km/s`. Given such a component,
CASA's `cl.convertfluxunit(-1, 'mJy')` reports success, scales the flux by
unit factors and labels it `mJy`, despite the incompatible dimensions. It
also modifies earlier components when the incompatible one is later in the
list. casa-rs checks every selected source before changing any component and
raises a catchable error if one does not conform. On an empty list, CASA's
`cl.convertfluxunit(-1, 'mJy')` terminates with SIGSEGV; casa-rs raises a
catchable error. A target with dimensions unlike Jy still returns `False`, as
CASA does, before it examines the indices.

## Component removal can lose data or crash

- Date noted: 2026-11-05
- Status: CASA loses a component on duplicate indices and crashes on an empty list; casa-rs preserves values and reports a catchable error (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/casa-compat/componentlist_removal_oracle.py`, CASA 6.7.6-14 on the Mac mini; empty-list calls run in isolated processes with core dumps disabled.

With four components labeled `0`, `1`, `2`, `3`, CASA's
`cl.remove([1, 1])` leaves `0`, `3`; `cl.recover()` appends two copies of
`1`, so component `2` is lost. casa-rs treats repeated indices as one:
it leaves `0`, `2`, `3` and recovers `1` once. On an empty list, CASA's
`cl.remove()` and `cl.remove([-1])` both terminate the process with SIGSEGV;
casa-rs raises a catchable `RuntimeError` without changing the list.

## CASA accepts component labels it cannot return

- Date noted: 2026-11-05
- Status: CASA accepts an unreadable label; casa-rs rejects the input and preserves the previous label (#1053).
- Owner decision: Correct CASA data corruption as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/casa-compat/componentlist_setlabel_oracle.py`, CASA 6.7.6-14 on the Mac mini.

After adding a component, `cl.setlabel(0, b'\xff')` returns `True` in CASA,
but `cl.getlabel(0)` then raises `UnicodeDecodeError`: the previous readable
label has been replaced by bytes the Python binding cannot return. casa-rs
raises `UnicodeDecodeError` before changing the label. Valid UTF-8 byte strings
and text are accepted, and both implementations stop at the first NUL byte.

## Cyclic component-list type checks crash CASA

- Date noted: 2026-11-05
- Status: CASA crashes; casa-rs returns the placeholder result (#1053).
- Owner decision: Correct CASA crashes as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_iscomponentlist_oracle.py`, CASA 6.7.6-14 on the Mac mini, in an isolated process with core dumps disabled.

Passing a list that contains itself to `componentlist.iscomponentlist`
terminates CASA's Python process with a segmentation fault before the
placeholder can return. The method otherwise logs a warning and returns
`False`. casa-rs detects cycles while inspecting the ignored argument and
returns that same placeholder value without crashing.

The same cyclic argument also crashes CASA's `componentlist.sample`
placeholder. `tools/casa-compat/componentlist_sample_oracle.py` records the
isolated crash. casa-rs returns the placeholder's empty float array instead.

## `componentlist.replace` crashes while converting ignored arguments

- Date noted: 2026-11-06
- Status: CASA crashes; casa-rs handles the inputs safely (#1053).
- Owner decision: Correct CASA crashes as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_replace_crashes_oracle.py` and `crates/casars-python/python/tests/data/casa_componentlist_replace_crashes.json`, CASA 6.7.6-14 on the Mac mini, with isolated processes and core dumps disabled.

`cl.replace(0, record)` with a record containing itself, a cyclic list of
`whichones`, and scalar NaN or infinity as `whichones` each terminate CASA's
Python process with signal 11 before the placeholder can return. The method
otherwise warns and returns `False`. casa-rs detects record cycles and returns
the placeholder result; for cyclic or non-finite index arguments it raises a
catchable `ValueError`, as for component selection (#1176). No component is
changed.

`componentlist.asciitocomponentlist('a', 'b', direction=record)` likewise
crashes while converting a self-referential record, even though CASA's
placeholder ignores that record. `tools/simulation/componentlist_ascii_oracle.py`
records the crash in an isolated process. casa-rs returns the placeholder's
zero and warning without reading a file or changing the list.

## Invalid component direction frames use an uninitialized reference

- Date noted: 2026-11-06
- Status: CASA uses an uninitialized frame after a parse failure; casa-rs rejects it before mutation (#1053).
- Owner decision: Correct CASA crashes and corruption as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setrefdirframe_invalid_oracle.py` and `crates/casars-python/python/tests/data/casa_componentlist_setrefdirframe_invalid.json`, CASA 6.7.6-14 on the Mac mini.

`componentlist.setrefdirframe(0, "bad")` logs that the direction frame was
not changed, but the C++ code continues with `newFrame` uninitialized after
`MDirection::getType` fails. On the tested CASA build, four invalid names and
an invalid byte string each raise an `MDirection` assertion and leave the
direction unchanged; an uninitialized value can behave differently in another
build. casa-rs logs the
parse failure and raises a predictable `RuntimeError` before changing the
component. The error text differs from the observed CASA assertion.

## AW projection chooses frequency cells from averaged channel widths

- Date noted: 2026-11-05
- Status: CASA 6.7.6.14 divergence; casa-rs uses the actual channel frequency (#1121).
- Affected code: `MSSelection::getChanFreqList`, `CFBuffer::initMaps` and
  `AWVisResampler`.

CASA constructs its per-channel AW convolution-cell map from the first
selected channel frequency and the average channel width, rather than each
selected channel's `CHAN_FREQ`. For irregular channel centres this can pick a
cell on the other side of a frequency boundary. In a controlled three-channel
VLA fixture with centres 1349.88, 1350.01 and 1350.15 MHz, widths of 100 kHz,
and cache cells at 1200 and 1500 MHz, CASA maps the second channel using
1349.98 MHz. That selects the 1200 MHz cell even though its actual 1350.01 MHz
centre is closer to the 1500 MHz cell.

A CASA control using 125 kHz widths maps that channel to the latter cell. Its
start was adjusted to keep the output coordinates identical; the visibilities,
weights, flags, selected channels and output grid were otherwise the same.
The two CASA results differ by 0.104 normalized RMS in the PSF and 0.158 in the
primary beam. On that control, casa-rs agrees with CASA on all compared
products, masks and metadata, with worst normalized RMS 5.50e-7. The width
metadata really differs between the controls; the comparison isolates the
cell-selection effect on this natural-weighted fixture rather than asserting
that channel widths are generally interchangeable. The per-row frame conversion
can itself move a channel across a cell boundary, so casa-rs recovers the stored
channel frequency for AW cell selection while retaining the converted frequency
for UV placement. Rounding near an exact cell tie remains permitted.


## AW cube interpolation restores the native channel map

- Date noted: 2026-11-11
- Status: verified CASA 6.7.6.14 bug ([#1121](https://github.com/bglenden/casa-rs/issues/1121#issuecomment-6698483927)).
- Affected code: `FTMachine::interpolateFrequencyTogrid`,
  `AWProjectFT::put` and `setupVBStore`, and `AWVisResampler::DataToGridImpl_p`
  in `synthesis/TransformMachines2`.

`AWProjectFT::put` calls `matchChannel` again after interpolating the data,
weights and flags. The second call restores the map for native channels.
`setupVBStore` supplies native reporting frequencies. The AW loop uses the
length of the interpolated flag axis to index both native vectors.

On `refim_point_linRL.ms` with `start=1, width=1, nchan=2` and linear
interpolation, a trace of CASA's running sampler showed two interpolated
entries and the native map `[-2, 0, 1]`. Only ordinals 0 and 1 were visited.
Plane 1 had zero sum weight. CASA's standard gridder populated both planes
on the same input. With `start=0, width=2, nchan=2`, the trace instead showed
four entries. The AW loop reached ordinal 3 with positive weight and used
the CF selection path even though the map and frequency vectors had length 3.

casa-rs keeps each interpolated sample's output plane and frequency together
through gridding and convolution-cell selection. This is the mapping used by
the shared spectral resampler.

For converted TOPO-to-LSRK sampling, the offset and width-two comparisons
use the executing standard CASA gridder's interpolated samples. Putting
the captured samples on a copy of the measurement set reproduces the
standard linear products. CASA AW nearest then grids those samples with
the original channel count and convolution cache. In the width-two
control, the channel-width metadata is set so CASA selects the cell for
the actual sample frequency, accounting for its averaged-width CF lookup
described above. Both comparisons retain the image grid and check the
output masks and predictions.

For BARY input and a TOPO image, the release AW gridder can segfault in
`AWVisResampler::DataToGridImpl_p`. A trace of the three-channel
`refim_point_linRL.ms` fixture with its test copy tagged BARY shows five
interpolation frequencies before the native AW map is restored. The standard
gridder visits the same five frequencies without the stale native map. Across
14 buffers it flags all but the middle sample. Materializing those samples
reproduces the standard products. CASA AW nearest on those samples and the
original cache agrees with casa-rs AW linear on products and masks. Both
clean runs report three iterations, four major cycles and stop code 1.
The saved MODEL_DATA agrees within 7.6e-7 normalized RMS.

## AW pointing can read an uninitialized frame on an ephemeris field

CASA 6.7.6.14's `PointingOffsets` owns a default-constructed
`VisBufferUtil`. With `usepointing=False`, its first field direction
lookup falls through to `getEphemBasedPhaseDir`. That function resets
the epoch on the empty frame, raising `Attempt to reset non-existent frame
member Epoch`. Initializing only the epoch exposes a missing observatory
position. A local comparison control initializes both from the epoch
already passed to the call and the VLA observatory used by the normal
buffer-based constructor. The remaining CASA imaging code is unchanged.

On the three-channel VLA AW fixture with an attached test ephemeris,
`specmode='cubesource', interpolation='nearest'` maps channels with the
source-shifted frequencies from `FTMachine::matchChannel`. The executing
`AWProjectFT::setupVBStore` supplies the stored 1.5 and 2 GHz frequencies
to the UVW and phase calculation. casa-rs follows both rules. The dirty
and clean pairs with the frame-initialized CASA control agree on all
17 products, masks and clean counts, with maximum product normalized RMS
6.16e-7 and saved MODEL_DATA normalized RMS 5.09e-7. The unscaled native
comparison had PSF normalized RMS 2.24e-3.

## CASA discards zero-dimensional floating quantity-record values

- Date noted: 2026-11-05
- Status: CASA silently loses input values; casa-rs preserves them (#1170).
- Owner decision: Correct CASA data corruption as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/casa-compat/quantity_record_zero_dim_oracle.py`, CASA 6.7.6-14 on the Mac mini.

For `qa.quantity({'value': np.array(1.25, dtype='float64'), 'unit': 'm'})`,
CASA returns a one-element array containing `0.0 m`. `qa.convert` likewise
converts zero. CASA handles the same array correctly when passed without a
record. casa-rs preserves `1.25 m` and converts it to `125 cm`, using the
same one-element output shape. Its `getunit`, `getvalue` and scalar arithmetic
still report CASA's wrong-holder error for the record.

## Unsafe component reference-direction inputs

- Date noted: 2026-11-06
- Status: CASA crashes or corrupts component directions; casa-rs rejects the inputs before mutation (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setrefdir_oracle.py`, `tools/simulation/componentlist_setrefdir_crashes_oracle.py`, and their recorded CASA 6.7.6-14 fixtures. Crash cases run in isolated processes with core dumps disabled.

On an empty list, `cl.setrefdir(-1, '1rad', '0.5rad')` terminates CASA with
SIGSEGV, as does a cyclic quantity record. A non-finite coordinate returns
`True` but replaces the direction with NaN. Passing `None` or an integer too
large for the binding as the component index raises `SystemError` only after
changing every component's direction. casa-rs rejects these inputs without
changing any components, and reports the crashes as catchable errors.

## Component-list concatenation copies the wrong components

- Date noted: 2026-11-06
- Status: CASA copies unintended rows; casa-rs copies the requested rows (#1053).
- Owner decision: Correct CASA data loss and contradictions of evident intent as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_concatenate_oracle.py` and `crates/casars-python/python/tests/data/casa_componentlist_concatenate.json`, CASA 6.7.6-14 on the Mac mini.

For a source list labeled `source0`, `source1`, `source2`, CASA's
`concatenate(source, [2, 0])` appends `source0`, `source1`; the source loop
checks each requested index but copies the component at the index's position
in the request. Duplicated or reordered requests can therefore silently
replace the components the user asked for. casa-rs appends `source2`,
`source0`. A request longer than the source list can also raise an assertion
after appending some wrong rows; casa-rs appends every valid requested row.
CASA also appends components before raising `SystemError` when
the index-vector conversion leaves a Python exception pending (for example
`which=None`); casa-rs reports the error without modifying the destination.

## Unsafe component flux-polarisation conversion

- Date noted: 2026-11-06
- Status: CASA crashes on an empty list and mutates values before reporting failed index conversion; casa-rs rejects both inputs without mutation (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_convertfluxpol_oracle.py` and its CASA 6.7.6-14 fixture; the crash probe runs in an isolated process with core dumps disabled.

`cl.convertfluxpol(-1, 'Linear')` on an empty list terminates CASA with
SIGSEGV. With components present, passing `None` or an integer too large for
the Python binding as the index raises `SystemError` after changing every
component's flux representation. casa-rs raises catchable errors and leaves
all component values unchanged.

## Unsafe component flux updates

- Date noted: 2026-11-06
- Status: CASA aborts, crashes, or loses input values; casa-rs preserves the input or rejects it before mutation (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setflux_oracle.py` and its CASA 6.7.6-14 fixture. Crash cases run in isolated processes with core dumps disabled.

For a component, `cl.setflux(0, 2, error=0.2)` aborts CASA because its
four-element error vector is assigned a one-element array. casa-rs stores
`[0.2, 0, 0, 0]`, preserving the supplied error. On an empty list,
`cl.setflux(-1, 2)` terminates CASA with SIGSEGV. An index of `None` or an
integer too large for the binding causes CASA to change every component and
then raise `SystemError`; casa-rs reports a catchable error without mutation.

CASA accepts complex flux values and four-element errors, silently discarding
the imaginary parts; casa-rs rejects nonzero imaginary parts before mutation,
because the Stokes record interface cannot return them. CASA also accepts NaN
and infinity, and narrows a finite `1e200` to infinity when it temporarily
stores the flux in single precision. casa-rs rejects non-finite inputs and
retains finite double-precision values, consistent with the existing
`addcomponent` precision decision above.

## Component shape changes discard direction errors

- Date noted: 2026-11-06
- Status: CASA loses direction errors and can modify a component before reporting an argument error; casa-rs preserves them (#1053).
- Owner decision: Correct CASA data corruption as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setshape_oracle.py` and its CASA 6.7.6-14 fixture.

Given a component with nonzero direction errors, `setshape(0, ...)` copies
only its direction and replaces both errors with zero. casa-rs preserves the
original direction and both errors when changing the shape. Passing
`optionalparms=None` can also update the shape before CASA raises
`SystemError`; casa-rs reports the error without changing the component.
Passing a cyclic `optionalparms` list terminates CASA with SIGSEGV; casa-rs
rejects the cycle before changing the component.
CASA accepts NaN as a Gaussian position angle or shape error and writes NaN;
casa-rs rejects a non-finite shape or error before mutation.

## Component spectrum changes can discard data

- Date noted: 2026-11-06
- Status: CASA clears a frequency error or leaves a partial update after an error; casa-rs preserves existing data (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setspectrum_oracle.py` and its CASA 6.7.6-14 fixture; the cyclic-input probe runs in an isolated process with core dumps disabled.

Changing a component's spectral model with `setspectrum` resets its existing
frequency error to zero. casa-rs keeps the original frequency error and unit.
A tabulation in TOPO for a component whose reference frequency is LSRK raises
`Cannot transform frequency` only after CASA replaces the old spectrum;
casa-rs validates the frame conversion before mutation. An unconvertible
numeric string in a tabular vector is silently stored as zero in CASA;
casa-rs rejects it without changing the component. CASA also stores NaN
spectral indices, polynomial coefficients and tabular fluxes, and terminates
with SIGSEGV for a cyclic tabular vector. casa-rs rejects these inputs before
mutation. A failed Python conversion can likewise cause CASA to change the
spectrum before raising `SystemError`; casa-rs preserves the original.

## Simulating more than the signed loop range

- Date noted: 2026-11-06
- Status: CASA's component counter overflows for requests above `INT_MAX`; casa-rs rejects them (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `componentlist::simulate` in CASA's `componentlist_cmpt.cc` uses an `int` loop counter against a `long` request; `test_componentlist_simulate.py` checks refusal without allocating billions of components.

`componentlist.simulate(2147483648)` increments a signed 32-bit counter past
its maximum if it reaches the requested count. Signed overflow is undefined,
and the request is liable to exhaust memory first. casa-rs refuses counts
outside the loop's valid range before changing the component list.

## Stokes spectrum errors can alter existing components

- Date noted: 2026-11-06
- Status: CASA can discard an error or leave a partial spectrum update; casa-rs preserves existing data (#1053).
- Owner decision: Correct CASA crashes, out-of-bounds reads and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/componentlist_setstokesspectrum_oracle.py` and `casa_componentlist_setstokesspectrum.json`; CASA's `componentlist::setstokesspectrum` reads `index[0]` without checking its length.

`setstokesspectrum` can change a reference frequency before returning false
for a power-log polynomial, or before raising `SystemError` for a failed
vector conversion. A tabulation in TOPO for an LSRK reference can replace the
spectrum before raising a frame-conversion error. CASA also accepts a length
as a reference frequency and silently treats its numeric value as hertz; and
an empty index is read out of bounds. casa-rs validates inputs and preserves
the old component on these failures. Like `setspectrum`, replacing the model
preserves an existing frequency error instead of resetting it to zero.

## Measure-offset inspection can corrupt integers or crash

- Date noted: 2026-11-06
- Status: CASA truncates large integers in offset metadata and crashes on a cyclic Python record; casa-rs preserves the value or rejects the cycle (#1053).
- Owner decision: Correct CASA crashes and data corruption as classes; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/measures_getoffset_oracle.py` and its CASA 6.7.6-14 fixture; the cyclic-input probe runs in an isolated process with core dumps disabled.

When an otherwise valid direction offset contains an extra integer value of
`2**53 + 1`, CASA's `getoffset` returns `1`. casa-rs returns the supplied
integer exactly. A self-referential input record terminates CASA with SIGSEGV;
casa-rs raises a catchable `ValueError` before conversion. Array-valued and
nested measure offsets remain explicitly unimplemented in casa-rs (#1053).

## Extra-measure value inspection can crash on cyclic input

- Date noted: 2026-11-06
- Status: CASA crashes; casa-rs rejects the cyclic record (#1053).
- Owner decision: Correct CASA crashes as a class; see `docs/simulation/owner-decisions-20261105.md`.
- Evidence: `tools/simulation/measures_addxvalue_oracle.py` and its CASA 6.7.6-14 fixture; the cyclic-input probe runs in an isolated process with core dumps disabled.

Passing a self-referential dictionary to `measures.addxvalue` terminates
CASA with SIGSEGV while converting the input record. casa-rs raises a
catchable `ValueError` before conversion. Excessively nested records are
also refused, and offset, multivalued and unsupported measure kinds are
explicitly unimplemented (#1053).

## An unused ALMA antenna can choose the standard primary beam

- Date noted: 2026-11-11
- Status: casa-rs chooses the one dish class used by the selected
  cross-correlations for this single-MeasurementSet standard continuum
  case (#1274).
- Affected code: casatools `synthesis/ImagerObjects/SynthesisImagerVi2.cc`
  (`makePB`) and `SynthesisImager.cc` (`makePBImage`).

CASA 6.7.6.14 reads ANTENNA row zero from the first MeasurementSet for its
analytic primary beam. It warns when diameters differ. An ALMA-labelled
observation then uses the ACA beam if that first diameter is below 12 m,
even when that antenna does not appear in a selected cross-correlation.

In public `M100-X220-shortened.ms`, the 264 selected field-6 rows use only
12 m dishes. ANTENNA row zero describes a 7 m dish. Changing only that
unused row's diameter to 12 m in a scratch copy leaves CASA's dirty PSF,
residual, restored image and sum weight numerically identical. The
`.pb.tt0` changes by 0.7893 normalized RMS and the PB-corrected restored
image by 0.6723. The number of pixels above PB 0.2 falls from 16,384 to
12,149. The 12 m reference uses the original visibility rows.

For a single ALMA MeasurementSet with both dish classes in its ANTENNA
table, standard continuum imaging chooses the analytic primary beam
from the selected cross-correlations. It refuses a selection that
requires both classes.

## Invalid leakage solutions become unit leakage

- Date noted: 2026-11-12
- Status: CASA defect corrected under the standing data-corruption rule (#1053)
- Affected code: `JonesGenLin::setMatByOk` in
  `code/synthesis/MeasurementComponents/Jones.cc`, called by
  `VisJones::syncJones` while `Simulator::corrupt` applies a D table.
- Reference: CASA 6.7.6.14; historical public C++ revision
  `4ebd5b1508a5d31b74e7b5f6b89313368d30b9ef` reproduces the mechanism.

With either receptor flagged, `JonesGenLin::setMatByOk` replaces both
leakage coefficients with complex one. A missing antenna solution has the
same effect. For diagonal gains, one is the neutral coefficient; for linear
leakage, zero is neutral. `JonesGenLin::applyRight/applyLeft` therefore add
the parallel hands into the cross hands. With all correlations initially
`3+4j`, an autocorrelation with one flagged D receptor becomes `9+12j` in
both cross hands. The simulator does not persist the temporary calibration
flags, so those large artificial signals remain unflagged.

casa-rs uses zero for both D coefficients when either interpolated receptor
is invalid, and for an antenna with no solution. Other antennas retain
their requested time interpolation. G tables retain CASA's per-receptor unit
substitution. Both paths preserve the input data flags, weights and sigma.
This corrects fabricated leakage without changing valid D or G solutions.

`tools/simulation/sm_apply_flags_oracle.py` preserves unmodified CASA outputs
and independent CASA controls with explicit zero leakage for the affected
antenna. The native comparison uses those neutral controls only at times
whose interpolated solution is invalid; valid times use the original valid
CASA table control. C++ consumption of native invalid-solution tables is
compared with the unmodified defective CASA outputs separately. The
reference fixture retains both results so this exception cannot silently
become a relaxed numerical tolerance.


## Natural-spline interpolation skips the first interior back-substitution

- Date noted: 2026-11-12
- Status: corrected under the standing evident-intent rule (#1053)
- Affected code: `InterpolateArray1D<Double,Float>::interpolatePtr`, natural
  spline back-substitution, called by `CTPatchedInterp::resampleInFreq`.
- Reference: installed CASA 6.7.6.14. Historical casacore source
  `083f0509fd76a03d6ec738c60e9dcfaf3e6c56a5` has the same mechanism:
  its backward loop stops at `i > 1`, leaving knot 1 partially eliminated.

A scalar Tf table with four equally spaced, real amplitudes `[1,2,1/2,5/4]`
should have zero endpoint second derivatives and interior second derivatives
`[-49/10,23/5]` in unit-spacing coordinates. The defining equations are
`4*m1+m2=-15` and `m1+4*m2=27/2`. At the first three half knots, the
natural spline is exactly `[289/160,203/160,47/80]`.

CASA leaves the first interior derivative at `-15/4`, rather than `-49/10`,
and evaluates the first half knot as `1.734375`, rather than `1.80625`.
The first derivative is then discontinuous at the first interior knot,
contradicting the stated natural-spline interpolation. With input DATA
`3+4j` and identical antenna gains, CASA writes approximately
`9.02417+12.03223j` instead of `9.78762+13.05016j`. The complete four-channel
visibility result differs by normalized RMS 0.0780.

casa-rs completes back-substitution at every interior knot. The fixture
`sm_apply_frequency_casa.json.gz` keeps the raw CASA output and an independent
CASA control: a nearest-interpolated table explicitly containing the exact
half-knot amplitudes. That control is also checked against the closed-form
receptor equation; the native spline is compared against it. C++ consumption
of the original native spline table is checked separately against the raw
CASA result. This exception changes no numerical tolerance.


## Simulator time-spline setup uses an uninitialized interpolator

- Date noted: 2026-11-12
- Status: corrected under the standing crash/evident-intent rule (#1053)
- Affected code: CASA 6.7.6.14 `CTTimeInterp1` constructor and
  `interpolate`, with casacore `Interpolate1D<Float,Array<Float>>`.

For G/D/T/B/Tf/Df calibration tables, `sm.setapply(interp='spline')`
selects the spline method before supplying the interpolator's data.
`Interpolate1D::setMethod` accesses the uninitialized sample count and
knot arrays. All eight independent four-time curve/analytic probes terminate
with signal 11 during `setapply`; `corrupt` is never called. The later time
registration supplies data for linear and cubic methods, but has no spline
branch. The same interpolator also skips the first interior back-substitution
identified in the natural-spline defect above.

casa-rs initializes a natural amplitude/continuous-phase spline from the
loaded time series, completes every interior back-substitution and retains
its coefficients. Exact times and queries outside the range retain the
registered knot or endpoint; one knot is constant and two knots form a line.
Flags combine only the adjacent time samples, without removing flagged
knots, as CASA's other time interpolators do.

`sm_apply_time_casa.json.gz` retains each raw CASA termination and separate
nearest-CASA controls. A dense NumPy linear-system solution, independent of
the native tridiagonal solver, samples the corrected amplitude/phase curves
at the requested times and supplies them to CASA's nearest interpolation.
The four real amplitudes `[1,2,1/2,5/4]` additionally give exact half-knot
amplitudes `[289/160,203/160,47/80]`. C++ consumption of the original native
knot tables uses the separate original-table nearest controls, because the
CASA spline setup itself crashes. No numerical tolerance is relaxed.


## Simulator short cubic time axes can alter DATA before failing

- Date noted: 2026-11-12
- Status: corrected under the standing data-loss rule (#1053, #1054)
- Affected code: CASA 6.7.6.14 `CTTimeInterp1::interpolate` and simulator
  corruption writes.

With two calibration times coincident with the first and last data times,
G and scalar-T cubic application first writes the exact-time solution.
The following interior query then asks for a four-time stencil and raises
`RuntimeError`; the already changed DATA remains. The reference fixture
retains both partial-write outcomes. With two or three knots and an initial
interior query, CASA raises before changing DATA. Single-time, exact and
outside-range queries do not need a cubic stencil.

casa-rs preserves those mathematical success/refusal rules, but evaluates
the applied term before staging any DATA writes. A short-axis interior
failure therefore leaves the entire input DATA intact. The regression test
checks both exact-first partial-write cases as well as the initially interior
failures. This preserves the simulator's existing fail-closed write contract.


## Simulator per-scan calibration does not synchronize its scan number

- Date noted: 2026-11-12
- Status: corrected under the standing crash rule (#1291)
- Affected code: CASA 6.7.6.14 `VisCal::syncMeta(const VisBuffer&)`,
  called by simulator corruption, and `CTPatchedInterp` per-scan lookup.

The old VisBuffer synchronization copies the observation ID but never the
scan number, leaving `currScan` at its initialized value of -1. The newer
VisBuffer2 synchronization copies both. A calibration table with valid scan
numbers activates per-scan lookup; the simulator supplies -1 as the segment
index. All five independent active-per-scan probes terminate with signal11.
Tables whose SCAN_NUMBER is -1 disable the modifier and still work.

casa-rs uses each simulated or opened scan's actual SCAN_NUMBER and retains
OBSERVATION_ID as well. Independent CASA controls explicitly partition the
input table and select the corresponding one-field-per-scan DATA, using the
ordinary nearest/linear/cubic method within each partition. These controls
cover nearest/linear behavior, missing segments, scan-over-observation
precedence and a short cubic-axis refusal. The ordinary per-observation
cases run directly in CASA. No tolerance is relaxed.

### Simulator test troposphere initializes before its times exist

`sm.settrop(mode="test", pwv=0.01)` after four 10-second integrations on
an eight-antenna observation returns False in CASA 6.7.6.14, logging
`logic error in AtmCorr::init() - slot times not initialized.`
`SolvableVisCal::setSimulate` calls `TJones::createCorruptor` before filling
its corruptor's slot times; the test initializer checks that flag immediately.
The source's explicit deterministic ramp is consequently unreachable.

casa-rs implements the source-defined ramp for default integration sampling:
for integration i of N and antenna a of A, its excess is
`(float(i)/N + float(a)/A) * float(pwv) * 10`. It uses the existing corrected
whole wet phase delay at each channel's CHAN_FREQ, then applies the resulting
unit phasors through the ordinary Tf path. Seed, beta and deltapwv do not enter
this debugging formula. Positive custom simint uses CASA's uncombined iterator chunks: a new
chunk starts when an integration reaches the requested interval from the
chunk's first integration, with scan/field/window boundaries preserved.
Saved solutions use the midpoint of each chunk's first and last integration.
`tools/simulation/sm_trop_test_interval_oracle.py` gets those chunks from the
installed CASA iterator and evaluates the source formula through CASA ATM.

This corrects an initialization-order failure that contradicts the defined
mode, under AGENTS.md's standing CASA-bug rule. Evidence:
`tools/simulation/sm_trop_test_oracle.py` records the actual failure and evaluates
the formula through independent CASA ATM;
`crates/casa-simulation/tests/trop_test_ramp.rs` compares native Tf parameters
and DATA to that reference, including repeated corruption and reopened data.
The existing wet-delay and channel-frequency corrections still apply.


### Simulator on-the-fly simple noise crashes on a newly defined window

In CASA 6.7.6.14, observe four integrations, call
`setnoise(mode="simplenoise", OTF=True)`, define another spectral window,
observe it and call `corrupt`: the process exits with signal 11. The same
control succeeds on the original window. The other five combinations of
simple/manual/atmospheric noise and OTF on/off safely skip the added window;
all six combinations apply their original solutions to later integrations
on an existing window.

casa-rs gates the noise term to the windows present when it was created,
preserving the new window's DATA and predictions. This corrects the crash
under the standing crash rule. `tools/simulation/sm_later_noise_oracle.py`
records each control in a separate CASA process; the native regression is
`crates/casa-simulation/tests/later_noise.rs`, with actual Metal coverage in
`tests/backends/offline_noise.rs`.

### Multi-feed observations accumulate the previous feed rotation

CASA 6.7.6.14's `NewMSSimulator::observe` loads a feed's fixed BEAM_OFFSET
before its integration loop, then rotates that same vector in place by each
integration's parallactic angle. Later integrations therefore accumulate
previous rotations instead of rotating the fixed antenna-frame offset once
at their current epoch. Four integrations with two/three feeds and offsets
of a few 1e-4 radians differ from otherwise identical single-integration
calls by up to 0.045/0.092 m in UVW. Feed zero, whose offset is zero, agrees
exactly; equatorial mounts require no rotation.

casa-rs rotates the fixed offset at the current integration's epoch. This
corrects a result that depends on how the same observation is split into
calls and contradicts the fixed BEAM_OFFSET metadata. The independent
reference consists of actual CASA single-integration controls, matched by
TIME, antennas and FEED1/FEED2. FEED/POINTING metadata, row order and scan
numbers still match the combined observation. Evidence is captured in
`tools/simulation/sm_feeds_oracle.py` and `tests/feeds.rs` (#1293).

### Reopened simulator labels appended rows with the physical window ID

CASA 6.7.6.14 accepts an existing MS whose DATA_DESCRIPTION rows map to
spectral windows in a different order. However, `sm.observe` writes the
requested physical SPECTRAL_WINDOW row number into MAIN.DATA_DESC_ID.
With DD window IDs `[1, 0]`, appending window 0 writes DD 0, which labels
the new rows as window 1. With DD window IDs `[1]`, appending window 1
writes DD 1, which is outside the DATA_DESCRIPTION table.
`NewMSSimulator::observe` writes `spWin` to `dataDescId`, without resolving
it through DATA_DESCRIPTION.

casa-rs resolves the requested window to its first existing description,
preserving the window's meaning and producing an in-range foreign key.
It refuses append when the requested window has no description. This
corrects data corruption under the standing CASA-bug rule. The raw CASA
results remain in `sm_data_descriptions_casa.json.gz`; the native and C++
readback checks explicitly validate the corrected appended label against
the input mapping. All other row cells and observation metadata still
compare to CASA. Evidence: `tools/simulation/sm_data_descriptions_oracle.py`
and `crates/casa-simulation/tests/data_descriptions.rs`.

### Simulator gain application ignores a reversed correlation order

CASA 6.7.6.14's supplied G-table corruption produces the same four ordered
products for `[XX, XY, YX, YY]` and `[YY, YX, XY, XX]`, even when an aliased
DATA_DESCRIPTION row selects a POLARIZATION row declaring the latter
CORR_TYPE and CORR_PRODUCT. With constant input DATA `3+4i` and receptor
gains `2+0.1i` and `3-0.2i`, its first product remains the XX result
`(3+4i)*|2+0.1i|²` instead of the declared YY result.

casa-rs applies `g[a,p] * conj(g[b,q])` according to each description's
actual receptor pair, correcting mislabeled science under the standing
CASA-bug rule. The oracle retains the raw CASA result; the native and C++
readback checks reverse each channel's four G products explicitly for
that case. Other descriptions, unselected rows and metadata still match
CASA directly. Evidence: the `alias_polarization_gain` case in
`tools/simulation/sm_data_descriptions_oracle.py` and `tests/data_descriptions.rs`.

### Simulator image direction conversion changes the physical sky

In CASA 6.7.6.14, a J2000 image with only its direction display conversion
changed to B1950 produces different simulated DATA. The pixels, native
direction coordinate, spectral coordinate and observation remain identical.
`DirectionCoordinate::toWorld(MDirection&)` calls the world-value overload,
which applies the conversion layer, then tags those converted angles with
the native `type_p`. `FTMachine::initMaps` uses that mislabeled direction
for its image tangent and UVW rotation.

A display conversion changes coordinate presentation rather than physical
pixel directions. casa-rs predicts from the native direction coordinate,
ignoring that presentation layer. The oracle retains the raw CASA result
and separately predicts an independent CASA control that clears only the
display conversion. Native and C++ readback comparisons require that
control's science while retaining the raw case's metadata and quality
checks. Evidence: `frame_conversion_b1950` in
`tools/simulation/sm_image_layouts_oracle.py` and
`crates/casa-simulation/tests/image_layouts.rs`.

## Simulator field selection drops the configured focus distance

CASA 6.7.6.14 `simulator.setdata(fieldid=[0], msselect=...)` changes the
simulator's `nField` to the length of the field selection without updating its
configured `distance_p` vector. `Simulator::createSkyEquation` then skips the
focus distance when `distance_p.nelements() > nField`. The same rows selected
using only `msselect="FIELD_ID == 0 && ..."` retain the configured focus and
produce different simulated DATA. A row selection must not change the physical
sky's finite distance.

casa-rs retains the last configured nonzero field distance for image prediction,
independently of the selected fields. `tools/simulation/sm_field_distance_oracle.py`
records the raw CASA field-selection result and an independent CASA control
selecting exactly the same rows through TaQL alone. The regression compares
corrected DATA to that control and keeps raw quality and geometry checks. The
ordinary zero-distance selection behavior is unchanged.

## AWP2 includes an MJD-zero angle for a fully flagged integration

`AWPLPG::init` surveys every selected visibility buffer before the major
cycle checks row validity. `Utils::getCurrentTimeStamp` returns zero when a
buffer contains no unflagged cross-correlation. CASA then computes a feed
angle at MJD zero and uses it when sizing the parallactic-angle lattice.
This can change the kernels used to grid the unflagged data.

On CASA 6.7.6-14, a three-integration field-1 selection from
`vla_wideband_2ptg_w_squint.ms` gives a maximum PA of 1.11158 rad. Setting
`FLAG_ROW` on the first integration's 351 rows changes that maximum to
1.60999 rad. The native Measures model rejects the MJD-zero conversion.
casa-rs refuses this combination with a diagnostic before allocating its
convolution kernels. [Issue #1071](https://github.com/bglenden/casa-rs/issues/1071).

## AWP2 can shift a PA plane when the convolution holder grows

`AWConvFuncHolder::appendConvFuncs` saves the old holder shape in
`newshp`. When a new PA needs more support, it grows and centres the
old data but copies the incoming PA using that old shape. This can
place the new plane away from the centre used by gridding and
degridding.

On the three-integration unflagged field-1 selection described above,
the holder grows from 306 to 360 fine pixels. The second PA's W=0 tap
peak stays at (153, 153) instead of the grid origin (180, 180). Before
the repair the raw CASA and casa-rs residual images differ by 0.99856
NRMS. An independent CASA control that grows the holder before
`appendConvFuncs` takes its working shape places the peak at (180,
180). All eight products then agree with casa-rs within 2.50e-7 NRMS,
with exact masks and sum of weights. casa-rs always centres the PA
planes. [Issue #1071](https://github.com/bglenden/casa-rs/issues/1071).

## AWP2's Float sensitivity FFT can flip the primary-beam cutoff

AWPLPG accumulates the sensitivity grid in double precision.
`MosaicFT::finalizeToSky` converts that grid to Float for its inverse FFT.
`SIImageStore::makePBFromWeight` and `divideResidualByWeight` then apply the
strict `abs(pblimit)` cut. CASA's single-precision FFT can move a value
across that cutoff by rounding alone.

The named CASA 6.7.6-14 `test_widefield_mvc_squint_awp2` clean uses two fields
of `vla_wideband_2ptg_w_squint.ms`, a 2000-square Stokes I image, uniform
weighting, four W planes, a 15° PA step, and `pblimit=-0.1`. Four of
20 million cube PB mask pixels differ between the release and native runs.
The uncut beams at those pixels are within 1.3e-7 of 0.1. In output
channel 3 (zero-based), one cutoff pixel raises the clean residual error
to 1.132e-3 NRMS. Its error away from the cutoff crossings is 1.84e-4.

A local CASA C++ control replaces only the Float inverse sensitivity FFT
with CASA's own `FFT2D::c2cFFTInDouble`, preserving Float image storage
and the strict cutoff. It reproduces all four PB mask flips within CASA,
including the residual difference in channel 3. Native keeps its existing
double-precision transform and the same cutoff. It agrees with this CASA
control on all 25 clean products, with exact masks and cube sum of weights,
the same 20 iterations, two major cycles and stop code 1, and a maximum
per-plane NRMS of 1.60e-5. The channel-3 residual agrees within 7.58e-6.
[Issue #1071](https://github.com/bglenden/casa-rs/issues/1071).

## Simulator image prediction retains the first window's polarization map

CASA 6.7.6.14 image prediction over windows with different `CORR_TYPE` arrays
can route later windows using the first window's map. For example, an IQUV
image predicted into XX/YY followed by YY/XX retains XX/YY values in the
second window's slots; circular and two/four-correlation combinations also
misroute or lose image values. `FTMachine::initMaps` builds `polMap` from the
initial buffer, while `GridFT::get` refreshes channel mappings at window changes
without refreshing that polarization mapping.

casa-rs prepares the existing forward operator for each selected correlation
setup and uses only the matching setup for a scan. The mixed-correlation oracle
retains the raw combined-window CASA result and independently predicts each
window through a fresh CASA simulator to obtain the correct per-window map.
Seeded flags, quality and untouched-window cells still use their raw references.
Combined image/component controls additionally reuse the independent image-only
plus component-only correction for the flagged-component duplication documented
above. No CASA tolerance is relaxed.
