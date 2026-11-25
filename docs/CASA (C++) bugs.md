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
[a non-finite rest frequency](#coordsyssetrestfrequency-can-store-an-infinite-frequency),
[a whitespace-only rest-frequency string](#coordsyssetrestfrequency-can-crash-on-a-whitespace-only-string),
[a masked coordinate reference-location edit](#coordsyssetreferencelocation-can-crash-when-an-axis-is-masked),
[an omitted or undersized coordinate offset](#coordsys-relative-conversions-can-abort-on-missing-values),
[empty input to the spectral velocity conversions](#coordsys-spectral-velocity-conversions-can-crash-on-missing-input),
[spectral vectors with non-native byte order](#coordsys-doppler-conversion-can-misread-byte-swapped-arrays),
[coordinate matrices with invalid rank or collapsed samples](#coordsys-linear-transform-input-can-abort-or-make-a-record-unreadable),
[a statistics logfile reused by a later call](#imagestatistics-can-overwrite-a-logfile-omitted-from-the-current-call),
[per-polarization flux on a selected channel](#image-statistics-can-use-the-wrong-beam-for-a-selected-channel),
[renaming an image while another tool holds it open](#image-rename-can-move-an-open-table-before-failing-to-reattach),
[removing an open table through a symbolic link](#image-removal-can-delete-an-open-table-through-a-symbolic-link),
[reading the validity of a decisive Boolean image expression](#calcmask-can-crash-on-decisive-boolean-validity),
[calculating an expression with a mixed-precision vector](#calc-can-crash-on-a-mixed-precision-vector),
[publishing image-calculator output before validating its metadata source](#imagecalc-can-publish-pixels-before-rejecting-the-metadata-source),
[reading a same-named FITS file after changing the working directory](#imagecalc-can-read-a-different-fits-operand-after-a-directory-change),
[FITS import of an empty primary HDU](#fits-import-can-abort-on-an-empty-primary-hdu),
[restoring-beam removal with a malformed record](#beam-removal-can-delete-metadata-before-rejecting-a-record),
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
[polarized AW sensitivity from one correlation](#polarized-aw-projection-publishes-the-first-correlation-sensitivity),
[signed Taylor moments with empty cross-hands](#mosaic-and-w-projection-drop-negative-taylor-moments-with-empty-cross-hands),
[single-precision Taylor weights](#multitermftnew-forms-taylor-imaging-weights-in-single-precision),
[AW baseline pointing groups](#aw-projection-can-use-another-antenna-groups-pointing),
[a mosaic's single-precision weight image](#a-mosaics-weight-image-is-transformed-in-single-precision),
[facets of an image size that does not divide](#facets-of-an-image-size-that-does-not-divide),
[the standard gridder dropping the final Fourier-grid row and column](#the-standard-gridder-drops-the-final-fourier-grid-row-and-column),
[an unflagged non-finite sample](#an-unflagged-non-finite-sample-turns-the-image-to-nan),
[a tuned cube's frequency selection](#a-tuned-cube-selection-compares-outframe-frequencies-with-lsrk-ones),
[a TOPO cube's cache-dependent endpoint selection](#a-topo-cube-can-gain-or-lose-an-endpoint-with-casas-aberration-cache-history),
[an overflowing FIELD ID](#an-overflowing-field-id-can-select-field-zero),
[a bare FIELD escape](#a-bare-field-escape-can-silently-change-the-selection),
[direction regridding of a CLEAN mask](#roundoff-in-direction-regridding-can-enlarge-a-clean-mask),
[expanding a CLEAN mask](#expanding-a-clean-mask-can-drop-its-direction-and-validity),
[a cube's fine frequency grid at a whole ratio](#a-cubes-fine-frequency-grid-lets-rounding-choose-one-or-two-samples-per-plane),
[the `.gridwt` a cube leaves](#a-cube-without-a-per-channel-density-leaves-imagenamegridwt-behind),
[multiscale on an off-centre PSF](#multiscale-misplaces-a-component-on-a-plane-whose-psf-peaks-off-centre),
[a cube restart grids at the saved alternate PSF direction](#a-mosaic-cube-restart-grids-at-the-saved-alternate-psf-direction),
[a cube's beam for a plane without one](#a-cubes-beam-for-a-plane-without-one-depends-on-how-memory-splits-the-cube),
[`tb.getcol` of an `int64` column](#tbgetcol-refuses-a-column-of-int64),
[`tb.putkeyword` of an int beyond 32 bits](#tbputkeyword-stores-an-int-beyond-32-bits-as-0),
[`tb` slices that abort the process](#some-tb-slice-calls-with-invalid-coordinates-abort-the-process),
[`tb.putcell` of an int beyond 32 bits](#tbputcell-and-putcol-wrap-an-int-beyond-32-bits-in-an-int-cell),
[`tb.putcell` with several rows](#tbputcell-with-several-rows-writes-those-before-a-missing-one),
[`tb.addrows` with a negative count](#tbaddrows-with-a-negative-count-removes-rows),
[`tb.removerows` and `tb.addcols` that fail part way](#tbremoverows-and-tbaddcols-do-part-of-a-call-that-fails),
[`MSIter`'s receptor angles for a window without its own `FEED` row](#msiter-keeps-the-previous-windows-receptor-angles-for-a-window-without-its-own-feed-row),
[a projection name with spaces](#tclean-refuses-a-projection-name-with-spaces-round-it),
[NCP on the celestial equator](#ncp-on-the-celestial-equator-places-every-pixel-at-nan),
[PCO coordinates on the central meridian](#pco-pixel-to-world-returns-nan-on-the-central-meridian).
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
[a solve that fits exactly](#viscalsolver2-fails-a-solve-whose-chi-square-is-exactly-zero),
[GSPLINE append overwriting an earlier field](#gspline-append-overwrites-the-previous-fields-fit).
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
[re-saving a flag version through an alias](#flagmanager-can-overwrite-a-saved-version-through-an-alias),
[an unrecognized flag merge choice](#flagmanager-replaces-working-flags-for-an-unrecognized-merge-choice),
[the choice of samples to clip](#flagdata-can-choose-the-wrong-samples-when-float_data-is-present),
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


## FLAG_CMD rewrites may truncate a longer saved command

CASA 6.7.6.14 reads FLAG_CMD.COMMAND into a NumPy string array during
`split` and `mstransform` post-processing. The array holds strings no longer
than the longest original command. Replacing a spectral selector with a longer
one can cut off later arguments before the table is written. The paired
`ms_flag_commands_oracle.py` test reproduces the truncation when an inactive
channel bin is negative and checks the complete rewritten command independently.
casa-rs writes the full command to the variable-length table column.
[Owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

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

## `importvla` leaves a failed MeasurementSet at the output path

CASA 6.7.6.14 leaves an empty MeasurementSet when `importvla` finds no matching
data, for example when selecting L band on `AT166_1`. It also leaves a partially
filled MeasurementSet if the first archive is readable and a later path is a
directory. `importvla` then rejects a retry at the same path because the output
already exists. casa-rs stages the MeasurementSet, task history and `Original`
flags together and publishes them on success, so a corrected request can be
retried at the same path. Pre-existing products are preserved.

## `importvla` can ignore a frequency tolerance given as a quantity string

On CASA 6.7.6.14, `AT166_1` produces eight spectral windows with
`frequencytol='3000000000.0Hz'` and five with `frequencytol=3000000000.0`.
The filler reads the magnitude of a parsed quantity and can use the channel
width when string extraction yields zero. casa-rs interprets quantity
strings in Hz when supplied explicitly and checks both forms against CASA's
numeric tolerance. Calls that omit the tolerance use the first CDA channel
width, matching CASA's default on the AS758 archive. The original argument
remains in `HISTORY`.

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
fields.

CASA imports sorted PA headers through `CFStore2::resize` with a strict
one-degree tolerance. When a new angle matches a buffer, CASA replaces its
angle and loads the new buffer's cells. Consecutive angles within one
degree therefore leave only the last paired buffer. The row selector then
uses the requested compute step over the surviving angles. A one-integration
control on `refim_mawproject.ms` uses distinct payloads at 177.8 and 178.2
degrees. CASA's dense and later-buffer-only references are exactly equal.
Keeping both buffers in casa-rs had changed the residual by 0.011 normalized
RMS and flipped 82 mask pixels. The coalesced import matches all seven
products, exact masks and controller counts, and leaves the cache files
untouched (#1070).

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

The same history extends across Högbom major cycles. In the field 0
eight-iteration CLEAN, CASA starts each residual refresh by rotating
the cell left by the previous pass from 90.5703° back to -86.0481°.
Recomputing the final residual in a fresh CASA run with the saved model
and the same inputs changes the residual by 1.879e-4 NRMS. The static
360-degree control repeats exactly. Casa-rs starts each rotation from
the saved cell and uses the selected epoch for both prediction and
gridding. It matches all eight CLEAN products, masks, and iteration
counts, with worst NRMS 2.384e-4 over the full field (#1070).

The wideband two-term case on the 1.2, 1.5 and 1.8 GHz windows of
the same field runs the rotation chain separately during the Taylor
passes. The stock CASA run repeats exactly but differs from casa-rs
by 2.769e-3 NRMS in `residual.tt0` and 3.726e-3 in `residual.tt1`.
Fourteen alpha-mask pixels differ. In a CASA control,
`AzElAperture::rotate2` keeps the same trigger and output angle but
hands `rotateComplexArray` the original cell pixels and original
angle on every call. With just that intervention, both residuals
agree within 1.192e-6 NRMS, both alpha masks agree exactly, and all 19
products pass, including the weak odd Taylor weight check. A
debugger run without the intervention reproduces all 19 stock
products exactly. The three windows share a feed angle and usable
integration times, so casa-rs uses one schedule for all three.

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
- Status: CASA writes blank products; casa-rs refuses with a diagnostic (#1109)
- Affected code: `synthesis/ImagerObjects/SIImageStore.cc`
  (`divideImageByWeightVal`)

When usable data reach the grids but the first grid plane has no weight,
the Taylor sum-weight rule gives zero to every Stokes plane.
`divideImageByWeightVal` blanks a plane whose sum is at most 1e-7.
casa-rs refuses a Taylor product whose principal sum is nonpositive.

A first correlation explicitly flagged throughout is an earlier case.
`FTMachine::setSpectralFlag` applies the documented ordinary Stokes rule:
a flag on any selected correlation flags the whole channel.
Flagging RR in `refim_point_linRL.ms` leaves RL, LR and LL unflagged,
but CASA 6.7.6.14 writes a blank PSF and then aborts on invalid array bounds.
casa-rs reports that RR is fully flagged before publishing images.
A MeasurementSet containing just RL and LR images QU successfully on
the same data. The complete-correlation control still agrees with CASA.
[Imaging issue #1109](https://github.com/bglenden/casa-rs/issues/1109)

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

## A mosaic cube restart grids at the saved alternate PSF direction

CASA 6.7.6.14 takes the saved PSF coordinates when it creates a temporary
channel subimage for `calcpsf=True` in a cube restart. `CubeMajorCycleAlgorithm::subImageStore`
constructs a `SimpleSIImageStore` from that subimage, and the constructor
takes its coordinates from the PSF when one is present. The gridded
sensitivity and PB then correspond to the saved PSF direction, though
CASA writes them into the main image's coordinate system.

On the three-channel natural `refim_alma_mosaic.ms` cube in #1062, change
only whether `tclean` starts fresh or resumes a saved cube whose PSF is
centred 7.5 arcsec away. When recomputing the PSF, CASA's residual, PB and
weight image differ from the fresh run by 0.318, 0.167 and 0.337 NRMS.
Replacing only the direction coordinate in a copy of the saved PSF makes
all three products and the recomputed PSF agree exactly with the fresh
run. Reusing the PSF with `calcpsf=False` preserves the saved PSF and its
coordinates and does not trigger the defect.

casa-rs recomputes the cube's PSF at the requested centre and records
that direction in its WCS. The main products use the main image's
geometry. The regression
`reforming_a_mosaic_cube_psf_uses_the_requested_geometry` checks the
PSF, beams, PB, weight, residual and sums on a cube restart.

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
this mechanism does not explain it. The terminal density cell described
under #789 has a separate cause.

The EVLA `refim_mawproject.ms` alternate-PSF cube in #1062 also
exercises per-channel Briggs density. The raw PSF NRMS is 0.01356,
whether it uses the requested image centre or a separate PSF centre.
Disabling per-channel density brings both PSFs below 1e-7. With
per-channel density on, replaying the 13,608 CASA row weights for each
centre independently brings both sets of eight image products within
2.14e-6 NRMS, with exact masks and sums of weights. The native density
grid and lookup still use the same frame.

The exact external-table Venus cube in #818 uses `briggsbwtaper` and
`mosweight=True`. CASA adds two guard planes at each end of the REST
density axis. The matched probe across seven fields compared 21,000
weights. With CASA's GridFT accumulation and stored-UVW lookup (using
the stored frequency for the Float coordinates and bandwidth distance),
the weights agree within 3.73e−8 absolute and 8.60e−8 normalized RMS.
GridFT accumulation alone still differs by 1.35e−2 in field 5. Replaying
the complete captured CASA weights independently matches the weighted
cube's seven products within 1.26e−6 per plane, with exact masks and sum
weights and matching WCS, controller counts and beams. The native
single-frame weights give residual NRMS 2.10e−2 and 83 cutoff crossings
against the raw CASA weights. Applying the same rotated coordinates to
both binning and lookup preserves a sample's contribution to its own
cell. This is the same two-frame exception as the fixed-source ALMA
mosaics, now verified on the moving-source bandwidth-taper path.

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

### simanalyze TP seed and feather PSF displays

CASA 6.7.6.14 deletes `cell_asec` after SF TP gridding, then uses it to plot
an interferometric PSF in the same invocation. A TP seed plus `showpsf=True`
raises `UnboundLocalError`. The graphics oracle restores only that display
scale from the selected image cell; native graphics use the PSF image's WCS.

After feathering, CASA changes `imagename` to the feathered stem, then tries
to open `<stem>.feather.psf`, which feather does not create. The oracle and
native figures display the original synthesis PSF. Numerical imaging and
feather calculations are unchanged.

The simalma combination figure deletes existing flat and convolved model
images as part of drawing its panels. Native plotting uses private copies
for the summary calculation and preserves the numerical products, following
the accepted simulation plan.

### simalma configuration names and zero-minute TP exposures

CASA 6.7.6.14's `task_simalma.py` records an unknown cycle as integer `-1`
then compares it with string `'-1'`. A valid ALMA/ACA configuration whose
name has no cycle number raises `TypeError` on Python 3. Native simalma
classifies the configuration directly; the independent oracle corrects only
that comparison. The compact ALMA/ACA integration case exercises these names.

The task formats TP exposure with `%dmin`. A positive `tptime` below one
minute becomes zero and cannot observe its planned pointings. Native simalma
refuses that request before replacing a project. Whole-minute truncation for
valid TP exposures is preserved.

## CASA omits the final row and column of its cube density grid

`BriggsCubeWeightor::fillImgWeightCol` builds its density with `GridFT`.
In `FTMachine::findGridSector`, a sector reaching the grid's right edge
sets its width to `nxp - x0 - 1` while `x0` is zero-based. The function
then makes the origin one-based for the Float kernel `sectggrids`, which accumulates
through `x0 + nxsub - 1`. The final in-band column is excluded. The
same calculation omits the final row. The weightor can look up those
cells, but their accumulated density is zero and the visibility gets
zero weight.

On CASA 6.7.6.14, field 0 of `refim_oneshiftpoint.mosaic.ms` imaged at
its own centre as a 512² uniform-weighted mosaic cube at 10 arcsec has
two affected rows, one each in spectral windows 1 and 2. They map to
the last column. CASA's captured row weights are zero for both, while
casa-rs gives each 0.5. Changing only the cell size to 9.9 arcsec puts
the same rows in interior columns and CASA gives both 0.5. The natural
mosaic control agrees directly. The standard uniform control agrees
because those rows fall outside its gridding kernel's full support.

Casa-rs keeps the final in-band row and column and rejects cells
outside the density grid. An evidence-only replay of the captured CASA
weights through native gridding reproduces the EVLA mosaic to 7.32e-7
normalized RMS per plane or better, with exact masks, including the ten
pixels affected by the dropped weights (#789).

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

The compact-array scheduling cases in `tools/simulation/simobserve_parity.py`
retain the raw task projects under `casa-raw`. Their independent control uses
CASA's `agentflagger` tool to clear flags and apply its behind-antenna shadow
agent, then predicts again so newly unflagged rows carry the sky signal.
Calling the tool avoids adding unrelated task HISTORY to the reference.

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

The same defect doubles the phase of complex correlations. In a linear
window, a component with nonzero Stokes U and V has XY = U+iV. The beam path
multiplies each channel's sampled XY by the original XY/abs(XY), rotating it
again; YX acquires the conjugate rotation. In the SIM-10 VP-table controls,
both points have the same original Stokes flux [1, 0.1, 0.02, 0.03] Jy.
CASA's auto-baseline crosshands show exactly this extra unit-phase factor,
while I and Q agree. casa-rs preserves the requested correlation phase.
`sm_voltage_patterns_oracle.py` retains raw CASA rows and supplies corrected
rows by dividing selected correlations by that original unit-phase factor.
No scientific tolerance changes; the remaining documented LSRK-label error
is below the existing 1e-3 threshold in these controls.

The interferometric `simobserve` control `if_joint_polarized` also exercises
this defect in circular products and the joint-sum defect described below.
It keeps the raw project, predicts its image and component separately with
CASA, removes the component's extra original unit-phase factor, and adds
the independent terms. The component is a single constant-spectrum source;
the reference asserts that domain rather than applying one phase to an
arbitrary component list.

## An uncovered first mosaic image suppresses a valid model

CASA 6.7.6.14 accepts `sm.predict(imagename=[outside, inside])` but produces
zero DATA when all pointings lie outside the first image, even though the
second model alone produces signal. `sm_mosaic_coverage_oracle.py` retains
that raw result and captures the valid model alone in a fresh simulator.
Its outside-only control is zero, so linearity independently gives the
expected sum. casa-rs tests coverage for each image and preserves the valid
contribution. The unchanged 1e-3 threshold applies to the actual C++ control.
This is a data-loss defect casa-rs corrects under SIM-10 (#1297).

## Saved Jones-image beams lose or reverse image polarization

- Date noted: 2026-11-14
- Status: CASA bug casa-rs does not follow (#1297, SIM-10)
- Affected code: `synthesis/TransformMachines/PBMath2DImage.cc`,
  `applyJonesFastX` and `apply(SkyComponent&, ...)` in CASA 6.7.6.14.

The component path reverses its input and output correlations around the
Jones product. The image path constructs the same reversed Jones matrix
but applies it directly to the image correlations. An asymmetric voltage
matrix therefore has different polarization effects on an image and on
its component representation. A one-Stokes sky also retains only one
beam-applied correlation, losing induced Q/U/V. Giving CASA four Stokes
planes with Q/U/V zero changes the prediction even though the sky is the
same. In the named controls, the original one-Stokes image produces equal
XX/YY and zero crosshands, while the equivalent four-plane sky has unequal
parallel hands and nonzero crosshands.

casa-rs applies the same coherency product to image and component signals
and grids all four resulting Stokes planes. `sm_image_beams_oracle.py`
retains the raw CASA rows. Its independent image control supplies the
equivalent four-plane sky and reverses the beam correlation values to
compensate for CASA's image indexing. Those actual C++ rows are the
corrected reference, with the usual 1e-3 threshold unchanged. The scalar
real, real/imaginary and complex beam controls require no such correction. The total-power
controls in `sm_total_power_oracle.py` retain this correction for a
nonzero I/Q/U/V sky, including an ordinary GridFT control (#1299).

`PBMath2DImage::updateJones` also returns its cached beam when the sky
coordinates and shape are unchanged, without testing the pointing or
parallactic angle. An A/B/A observation separated by four hours therefore
keeps the first visit's attenuation for every field. casa-rs refreshes the
beam on each pointing/field transition and retains the visit's orientation
when reusing prediction grids. The `real_image_revisit` control preserves
CASA's raw cached output and compares with actual CASA predictions made
using a fresh simulator for each visit, removing that stale cache. The same
cache suppresses within-scan orientation refresh: the four-integration
`sm_image_beam_refresh_oracle.py` control preserves the raw output and
uses a fresh simulator for each integration. casa-rs refreshes when the
requested angle threshold is crossed; the independent refreshed signal
differs from a frozen beam by 0.56%.

## EVLA component dosquint is a no-op

CASA 6.7.6.14 accepts `sm.setvp(dosquint=True)` for EVLA, but logs
"No Beam Squint". The synthetic 14.5 GHz controls in
`sm_squint_oracle.py` produce identical DATA with squint disabled, enabled
and a 0.01 degree angle-refresh threshold. The corresponding VLA controls
change by 0.00760 and 0.00923 NRMS, so the workload measures both effects.

`PBMath1DEVLA` declares its own private `squint_p`; its frequency-selection
code updates that member, while the inherited `PBMath1D::apply` reads the
base class's zero offset. casa-rs preserves the observed EVLA response for
CASA equivalence. Applying a physically different EVLA correction would
change existing results and is outside this comparison target. VLA band
offsets and its angle-refresh behavior are implemented separately.

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

`GridBoth::get` adds this same SDGrid result to its interferometric result.
Its GridFT has autocorrelations disabled. casa-rs therefore uses single-dish
signals on autocorrelations and interferometric signals on cross-correlations;
`sm_ft_modes_oracle.py` retains raw joint output and uses independent GridFT
cross-correlations as its corrected reference.

The joint PB control also exposes failed-call data loss: with a 64-pixel
image and gridding support 134, CASA returns false but erases selected DATA.
casa-rs rejects the insufficient extent before recording the prediction and
keeps the input DATA. The raw failure and the successful 256-pixel joint PB
calculation are retained in the same fixture.

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

## Legacy imager cleanup aborts after common-beam prediction

CASA 6.7.6.14 returns success from `imager.ft` with
`setoptions(ftmachine='mosaic')` on a 1.5 GHz VLA MeasurementSet. Both
scratch and virtual predictions reach disk. The next `imager.done()`
call raises SIGABRT with “pure virtual method called.” A separate CASA
process reopens the virtual recipe and reads its model through `uvsub`.
The same cleanup failure occurs with ATCA, GMRT, WSRT, GBT, NRAO140FT,
and the `NONE` telescope pattern.

casa-rs publishes both products and closes the tool normally. The
common-beam oracle verifies that CASA finished prediction before the
cleanup failure and checks live and reopened output against their own
references.
[`imager_common_mosaic_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_common_mosaic_oracle.py) ·
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

## Legacy MosaicFT cannot reopen a DEFAULT telescope beam

CASA 6.7.6.14 selects `SimplePBConvFunc` for the literal telescope name
`DEFAULT`. Its live `VPSkyJones` uses a numeric beam from the first dish
diameter. The virtual writer records the telescope name. On reopening,
`MosaicFT::fromRecord` looks up the name through `PBMath` and rejects the
recipe with “Failed to recovermosaic FTmachine” when the lookup is unknown.

casa-rs uses the first dish's numeric beam when reopening the record.
The paired oracle uses the pinned live prediction as that beam's reference
and checks the documented failure from the CASA reader.
[`imager_common_mosaic_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_common_mosaic_oracle.py) ·
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

## Legacy nift can abort while preparing a virtual model

In CASA 6.7.6.14, `imager.setoptions(ftmachine='nift')` constructs an
`rGridFT`. Live scratch prediction agrees with GridFT for the paired
visibility fixtures. `CubeSkyEquation::init` falls through to a `GridFT`
cast for the `rGridFT` object. Some virtual and faceted scratch runs
abort during `ft` in that path.

casa-rs uses the shared interferometric prediction and publishes a
`GridFT` virtual recipe. CASA reads that recipe through `uvsub`. The
paired oracle checks the scratch prediction against pinned `nift` and
the virtual prediction against a readable pinned GridFT reference.
[`imager_nift_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_nift_oracle.py) ·
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

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

## Legacy MosaicFT can crash with nonzero FEED beam offsets

On CASA 6.7.6.14, a two-field heterogeneous ALMA MeasurementSet with
nonzero receptor-zero `FEED.BEAM_OFFSET` caused the public legacy
`imager.ft` call to exit with signal 11. The fixture has an ALT-AZ mount,
explicit POINTING rows and the `mosaic` Fourier machine. casa-rs applies
the first antenna's rotated feed offset to the buffer pointing and
predicts the scratch and virtual models normally.

The paired control calculates the rotation from the pinned CASA measures
and writes the shifted directions into POINTING with zero feed offsets.
The native scratch and virtual predictions matched this CASA control
within 1.84e-6 normalized RMS. The fixture is in
`tools/casa-compat/imager_mosaic_oracle.py`.
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

## Faceted MosaicFT can omit its first facet in later scratch buffers

With CASA 6.7.6.14, `imager.defineimage(facets=2)` and
`imager.setoptions(ftmachine='mosaic')` assign MosaicFT to the first
facet and ordinary GridFT to the others. In a heterogeneous ALMA
MeasurementSet with measured POINTING rows, a bulk scratch prediction
retained the MosaicFT contribution in its first integration and omitted
it in the other eleven. The GridFT contribution was present throughout.

`CubeSkyEquation::getSlice` gives the per-facet transforms a copy of the
visibility buffer. The copy constructor does not initialize or copy the
first antenna's cached direction flag. The original buffer's first
direction is available to the initialization and the virtual reader.

casa-rs evaluates the first antenna's pointing for each integration and
keeps the working visibility buffer across facet transforms. The oracle
compares a native bulk scratch prediction with independent CASA calls
selecting one integration each. It also compares both virtual writers
and readers against a CASA virtual prediction over both fields.
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520) ·
[`imager_facets_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_facets_oracle.py)

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

The seven-pointing B10 Högbom control with `mosweight=True` (#818)
has one affected pixel, at zero-based `(603, 1020)` with `pblimit=0.2`.
The unblanked beams reconstructed from the two weight images are
0.2000000128 in casa-rs and 0.1999999561 in CASA. The same pixel crosses
the cut in the PB, restored image, PB-corrected image and residual masks.
A diagnostic cut at either 0.1999998 or 0.2000002 gives identical support
everywhere on the original arrays. The production cut remains at 0.2.
All nine products agree within 1.05e−6 normalized RMS on the common
support, with exact sum weights and matching WCS and beams.

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

A primary beam follows POINTING independently of FIELD's phase centre
([the next entry](#the-beam-on-a-moved-pointing-stays-where-the-fields-first-row-points)).

## The beam on a moved pointing stays where the field's first row points

- Date noted: 2026-10-24
- Status: CASA bug; fresh and resumed interferometric simulations centre
  each integration's beam and mosaic kernels on POINTING (#884, #1294,
  #1297). Fresh total-power beam offsets remain on #1299.
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
antennas. Fresh and resumed simulations separate FIELD's visibility phase from
POINTING's beam and total-power sample direction. For changing pointings,
`tools/simulation/sm_offset_pointings_oracle.py` retains the raw CASA output
and records a second independent CASA control with a distinct, equal-phase
FIELD identity per integration, forcing the stale cache to refresh. Native
beam and mosaic predictions agree with that control. Fresh fixed/changing
`observemany` controls in `sm_fresh_pointings_oracle.py` use the same
independent cache reset and capture CASA's IERS inputs; CPU and Metal image,
component and mosaic DATA agree below 3e-7 NRMS with unchanged tolerances.
Native total power
continues to use autocorrelations only, correcting the separate `SDGrid`
cross-correlation defect documented above. Incomplete or disagreeing antenna
pointings are explicitly refused before changing the DATA plan.

Fresh total-power `observemany` sampling also follows POINTING; the six
fixed/changing BOX/PB/VP controls in `sm_fresh_pointings_oracle.py` retain
raw output and use the same independent cache reset (#1299). `simobserve` gives each pointing its own field and does
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

The `schedule_calibrator_partial` control also retains the raw project and
reruns the same CASA task with its final queued `observemany` flushed when
`srces` is nonempty. Only that condition changes: CASA still constructs the
schedule and writes the scans. The calibrator component-list comparison
allows only the already documented `cl.addcomponent` float32 rounding of
flux; direction and frequency tolerances stay unchanged.

## simobserve ignores failed frequency-frame predictions

- Date noted: 2026-11-14
- Status: CASA error-propagation defect; casa-rs corrects it under the standing deviation policy (#1301).
- Affected code: `casatasks/src/private/task_simobserve.py`, the unchecked `sm.predict` calls.

With `outframe='Undefined'`, CASA 6.7.6.14's simulator returns False and
logs `Transformations to/from frame "Undefined" are not possible.` For a
component prediction in `outframe='REST'`, it returns False and logs
`No MeasFrame specified for conversion of Frequency`: the task supplies no
radial-velocity frame. The task ignores both failures, reports success and
publishes an entirely unpredicted zero-DATA MeasurementSet. Image-only REST
prediction succeeds and is retained.

casa-rs reports the same prediction cause before publishing the invalid MS.
The project can retain the model and pointing products prepared before the
failure. This prevents apparent successful science from a rejected calculation.
The `if_*_undefined` and `if_components_rest` controls retain CASA's raw task
projects, require all raw DATA to be zero, independently verify False from
CASA's simulator, and require the native error and matching pre-prediction
products with no native MS. This is a corrected failure outcome, not a claim
that CASA's task itself raises that error.


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

The same defect is present for channelized `bandpass(bandtype='B')` in
CASA 6.7.6.14: `RESOLUTION` is each channel's frequency and
`TOTAL_BANDWIDTH` is their sum. CAL-06 preserves those values for the same
append-interoperability reason. `calibration_lifecycle_oracle.py` exchanges
B tables in both directions and compares every SPW column.

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

## `image.regrid` can compare the lengths of different unchanged axes

CASA 6.7.6.14 preserves the native spectral coordinate and pixels for
`image.regrid(axes=[2], method="cubic")` on a direction and spectral cube
when the template has no spectral coordinate. But on a cube of shape
`[8, 9, 5]` the same call fails because the shape check compares spectral
length 5 to the first output length 8. Casa-rs checks the shape of the
unchanged spectral axis and keeps the image's original grid and samples.

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

## `simanalyze` retains missing or shadowed Python task bindings

- Date noted: 2026-11-15
- Status: corrected by native Rust composition (#841 Phase 6g)
- Affected code: CASA 6.7.6.14 `task_simanalyze.get_concatweights` and its
  `from .simutil import *` import

A multi-MS call reaches the obsolete `gentools(['tb', 'msmd'])` without
defining that factory and raises `NameError`. The simutil star import also
shadows the public feather callable with a module, causing a feather call
to raise `TypeError: 'module' object is not callable`. The independent
`simanalyze_multi_oracle.py` recipe restores only those bindings before
running the unchanged algorithms. Production computation is native Rust.

### Disabled upstream simulation test bodies

CASA 6.7.6.14 disables sixteen `test_task_simobserve` cases. Exercising them
exposes undefined `comp_nchan`/`ptgfile` variables, the deliberate CAS-5095
component-list/SD refusal, nine obsolete "Parameter verification failed"
assertions (the task now raises parameter-error dictionaries), and an
atmospheric noise comparison tied to an older random draw. Native tests
exercise the intended leakage equations, the actual SD refusal and invalid
parameter checks; analytic noise comparisons retain the upstream 10% bound.
The original assertions and their failures remain in the upstream runner.

The disabled combined-IF/TP `test_task_simanalyze` case encounters the
shadowed feather binding above. Its expected `<project>.image0` path also
omits `.sd`: `task_simanalyze` writes `tpimage = project + '.sd.image'` and
`temp_out = tpimage + '0'`. The compact native counterpart checks the actual
IF/TP products and compares the feathered image to the existing independent
CASA fixture at 1e-3 NRMS. Its original 1458-pixel, 128-channel dimensions
need a 13.13 GB native TP scaling/regridding reservation. With that reservation
explicitly authorized on the 24 GiB Mac mini, the full case now completes
including feathering, at a measured 12.50 GB peak process footprint (#1493).
The simulation wrapper charges reused FFT scratch per direction plane and
reserves only flat-image work when analysis and graphics are disabled;
the earlier cube-wide scratch charges incorrectly requested 43.6 GB for
feathering. CASA C++ reads all six full-size/intermediate products and their
defined pixels are finite. The raw obsolete-path assertion remains unchanged.

## `simanalyze` changes a caller's seed-image unit in place

- Date noted: 2026-11-15
- Status: caller data preserved (#841 Phase 6g, accepted Phase 6 decision)
- Affected code: CASA 6.7.6.14 `task_simanalyze`, before `simutil.imtclean`

CASA opens `modelimage` and sets its brightness unit to Jy/pixel before
calling tclean. A total-power or previously restored synthesis image loses
its Jy/beam label, including when it was supplied by the caller. Native
simanalyze preserves CASA's numerical seed convention in a private copy
and leaves the supplied image's pixels, mask and unit unchanged. The TP
seed case compares the synthesis results and verifies the source label.

## Repeated `simanalyze` concatenation can duplicate observations

- Date noted: 2026-11-15
- Status: new native simulation concatenations replace prior results (#841 Phase 6g)
- Affected code: CASA 6.7.6.14 `task_simanalyze`, multiple interferometric MS branch

The task invokes concat on `<project>.concat.ms` without clearing an earlier
result; concat appends when that output exists. Default discovery can also
select that old combined MS alongside its original inputs. Native simanalyze
excludes the previous combination when the originals are present and stages
a fresh combination before replacing its own output. Input observations
remain read-only.

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

## `simanalyze` deletes previous products before imaging preparation succeeds

- Date noted: 2026-11-15
- Status: CASA data-loss bug; casa-rs preserves inputs and previous products (#180)
- Affected code: `casatasks/private/task_simanalyze.py` (CASA 6.7.6.14,
  deletion of `<imagename>.image` and `<imagename>.model` before `imtclean`)

A rerun with an invalid start model destroys the previous image/model before
the imager can reject its units or resources. Selecting the previous
`<imagename>.model` as `modelimage` deletes that selected input too.
casa-rs refuses input/output collisions before mutation and uses the native
imager's existing atomic publication. Failed preparation preserves prior
products. `simanalyze_task_casa.rs` checks the colliding model and a K-unit
start model, then verifies the previous image and model are unchanged.

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

## Simulated MeasurementSets name a processor that does not exist

- Observed in CASA 6.7.6-14's public `sim_multi_arrays_and_TP` m51c 12 m
  and 7 m inputs and their concat result.
- Evidence: `scripts/concat_parity/task_oracle.py` and the concat task
  reference fixtures.

MAIN has `PROCESSOR_ID=0` while PROCESSOR has no rows. Zero therefore names
no valid processor. Native concatenation writes the MS null sentinel -1
in this case, preserving the absence of a processor without a dangling
foreign key. The parity comparison explicitly compares that logical absence;
a populated PROCESSOR table and its references are compared normally.


## `concat` does not scale its first input's sigma spectrum

- Observed in CASA 6.7.6-14.
- Affected code: `casatasks/private/task_concat.py`, the first MeasurementSet's
  weight-scaling block. Appended inputs are handled by `MSConcat`.
- Evidence: `scripts/concat_parity/task_oracle.py` and the concat reference
  fixtures, with weight factor 4 on the chronologically first input.

CASA multiplies its first input's WEIGHT and WEIGHT_SPECTRUM by 4 and divides
SIGMA by 2, while leaving SIGMA_SPECTRUM unchanged. This makes the spectral
uncertainties inconsistent with the same sample's weight and sigma. The
native task divides both sigma representations by the square root of the
weight factor for every input. The fixtures retain the actual CASA result,
and the regression checks the corrected cells against that variance law.

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
  Native numbering keeps absolute paths for both the main image and outliers.
- Affected build/code: CASA 6.7.6.14, casatasks
  `private/imagerhelpers/input_parameters.py`, `incrementImageNameList`.

With existing products and `restart=False`, CASA finds the next shared
numbered image prefix. Its Python helper removes the first two characters
of each directory with `dirnames[immod][2:]`, assuming a `./` prefix. For an
absolute imagename under `/Volumes/`, this instead produces `olumes/` and
fails to create the numbered PSF. Relative prefixes in the same probe
correctly produce `_2` and `_3`. The exact build, parameters and failure
are recorded on #944 and in the lifecycle receipt linked from the generated
compatibility reference. The native implementation keeps the requested absolute directory and
uses the same suffix across the main and outlier prefixes.

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
  where the primary beam is under `pblimit`, including every Taylor
  coefficient (#980, #1063)
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

The named `test_multifield_facets_mtmfs` case on the same input with
`deconvolver='mtmfs'` and `nterms=2` has the same 2,471 PB mask pixels
on each joined residual and restored Taylor plane. Stock CASA omits
all four masks. casa-rs copies its PB mask to each plane. A comparison
of the full pixel arrays, including the masked pixels, agrees within
5.38e-6 normalized RMS across this case's products.

## CASA's cube workers reject facet stores with no polarization representation

In CASA 6.7.6.14, `tclean` fails before publishing a PSF for a 2×2
faceted, Stokes I, two-channel dirty cube on `refim_point.ms`,
`refim_eptwochan.ms` or `refim_twopoints_twochan.ms`. The worker reports
“data polarization type is not defined.” `SIImageStore::init` sets the
representation to UNKNOWN. `SynthesisImagerVi2` leaves the call that
copies the parent's representation into a facet commented out, while it
copies the representation into channel chunks. `runCubeGridding` sends
the unset value to `CubeMajorCycleAlgorithm`, which rejects it. The
unfaceted control completes.

Casa-rs uses the observation's polarization plan for every facet and
finishes natural-weighted, same-frame standard dirty and Högbom cubes with
nearest-channel sampling. For `refim_point.ms`, independent CASA faceted
MFS runs of the selected channels verify the PSFs, residuals, restored
images, models, masks and facet weights. An unfaceted CASA cube on those
same channels verifies the frequency axis and cube PB (#1063). When the
first facet has no usable weight on a channel, casa-rs takes that
channel's PSF from the first populated facet. This extends its scalar
facet behavior to cubes. CASA's normalizer always uses facet zero,
whose empty PSF cannot guide deconvolution in the populated facets.

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

## `flagmanager` can overwrite a saved version through an alias

In CASA 6.7.6.14, `flagmanager` can open a MeasurementSet through a symbolic
link and write flag versions beside the link's target. The Python save
preflight looks for the old version beside the alias. Re-saving the same
version through the alias can therefore overwrite the saved flags and comment.
The `tools/casa-compat/flagmanager_alias_oracle.py` comparison reproduces the
loss. It verifies that casa-rs archives the previous flags and comment beside
the target and can restore them. An existing alias-side catalog from an older
casa-rs release remains directly accessible.

## `flagmanager` replaces working flags for an unrecognized merge choice

CASA 6.7.6.14 accepts unrecognized `merge` strings in restore mode and
silently applies `replace`. For example, `merge="OR"` removes working flags
that were set after the version was saved. The
`tools/casa-compat/flagmanager_store_oracle.py` comparison verifies this
for both `FLAG` and `FLAG_ROW`. It also verifies `replace`, `or`, and
`and` against independent flag patterns. casa-rs raises `ValueError` for
an unrecognized choice and preserves the working flags.

## `flagdata` can fail to decode an invalid FIELD diagnostic

CASA 6.7.6.14 rejects a FIELD selector consisting of a bare nonbreaking
space, but its Python binding raises `UnicodeDecodeError` while decoding
the parser diagnostic. The first UTF-8 byte of the invalid token is
reported on its own.

casa-rs reports the invalid whitespace with a readable task error.
The paired check verifies that both calls leave flags and visibility
data unchanged (#1617).

A leading nonbreaking space in an ARRAY selector triggers the same
decoding failure. The array oracle checks both the CASA failure and the
readable native refusal, with flags and visibility data unchanged.

The same decoding failure occurs for a leading nonbreaking space in
`intent` and `uvrange`. The paired checks cover both refusals and verify
that quoted intent patterns still match valid UTF-8 text in STATE.

## `flagdata` can choose the wrong samples when FLOAT_DATA is present

CASA 6.7.6.14 can evaluate the magnitude of `FLOAT_DATA` when
`flagdata(mode="clip", datacolumn="DATA")` opens an MS containing both
physical columns. The paired fixture holds complex DATA within the requested
range and puts signed floats outside it. CASA flags the samples from the
float column. casa-rs computes the range from the requested complex DATA.

The same iterator substitution affects TFCrop's `DATA` and `RESIDUAL_DATA`
fits. A fixture with distinct spikes in the complex and float columns shows
that CASA flags the float source. Native TFCrop keeps the complex source and
matches the CASA masks for a control containing only complex observations.
[TFCrop source oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_sources_oracle.py)

With `datacolumn="FLOAT_DATA", clipzeros=True`, CASA also flags finite
negative values outside the zero tolerance. casa-rs applies the zero
tolerance to the magnitude of each sample. An explicit inside or outside
range on FLOAT_DATA uses the signed values, as CASA does. The
`tools/casa-compat/flagdata_clip_range_oracle.py` comparison verifies both
choices with independent expected flags.

## Clip visibility operators can flag unrequested samples

In CASA 6.7.6.14, `flagdata(mode="clip", correlation="IMAG_XX")`
can also flag YY. The visibility mapper scans the operator for product
names and treats the I in IMAG as a request for Stokes I. The paired
fixture puts the XX imaginary value outside the range and its derived
Stokes I value inside it. CASA flags both contributing planes at that
channel. casa-rs evaluates the explicit XX product.

TFCrop also matches the I in `IMAG_Q`. Its paired control adds the same
imaginary spike to XX and YY. The derived Q remains flat, while CASA fits
I and flags both planes. Native TFCrop evaluates Q and returns the usual
unknown-polarization error for a bare `IMAG` operator. It uses the same
native Stokes equations as clip. [TFCrop correlation oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_correlations_oracle.py)

With `correlation="REAL_XX", clipzeros=True`, CASA compares the signed
real value to the zero tolerance. A finite value of −3 is flagged.
casa-rs compares the magnitude to the zero tolerance and uses the
signed value for any explicit range. The clip operator oracle checks
both cases against independent expected flags. [Standing owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## Derived Stokes clip expressions can flag the wrong channels

In CASA 6.7.6.14, the clipping visibility mapper computes linear Stokes U
as `(XY - YX) / 2` and circular Stokes Q as `(RL - LR) / 2`. Equal
cross-hands with a value of 3 therefore produce zero and are flagged by
an inside range of −0.5 to 0.5. The corresponding Stokes values are 3.
Circular Stokes V is divided by `2i`, which gives `REAL_V` zero for
RR = 4 and LL = 0. The corresponding Stokes V is 2.

casa-rs uses `(XY + YX) / 2`, `(RL + LR) / 2`, and `(RR - LL) / 2`
for these three products and flags their contributing planes when the
result meets the clipping criterion. MeasurementSet RFlag uses the same
corrected linear Stokes U equation `(XY + YX) / 2`, verified with an
equal-cross-hand (`XY = YX`) control in [MS RFlag
oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_rflag_ms_oracle.py). The paired Stokes
clip oracle reproduces the CASA flags and compares the native flags with
independently calculated values. [Standing owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## Manual flagging and extension can flag channels between requested stride steps

In CASA 6.7.6.14, manual flagging with `spw='0:0~3^2'` changes all four
channels of the window. Using the explicit list `spw='0:0;2'` selects
channels 0 and 2. The `tools/casa-compat/flagdata_apply_backup_oracle.py`
comparison reproduces the extra flags and checks that casa-rs retains the
two requested channels. [Standing owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

With XX channel 3 already flagged, `mode='extend'`, `flagnearfreq=True`,
and `spw='0:1~7^2'`, CASA flags the unselected channels 2 and 4. The native
task grows to the neighboring selected channels 1 and 5, as CASA does with
the explicit channel list `0:1;3;5;7`.
[Extension oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_extend_oracle.py)

## Clip and TFCrop channel averaging can abort on a strided selection

In CASA 6.7.6.14, `flagdata(mode="clip", channelavg=True, chanbin=2,
spw="0:0~3^2", correlation="XX")` aborts with `ArraySlicerError` on a
four-channel spectral window. The equivalent explicit selection `0:0;2`
works. Native flagdata groups the actual selected channels, averages the
two samples and sets new flags on channels 0 and 2 when the bin meets the
clip range. The channel clip oracle checks the mask independently from
the input samples and from CASA with the equivalent explicit selection.

TFCrop reproduces the same abort with `flagdimension="time"`,
`channelavg=True`, `chanbin=2` and `spw="0:8~23^2"` on a 32-channel
window. The independently injected broadband impulse flags just channels
8, 10, 12, 14, 16, 18, 20 and 22 on that integration. RFlag also reproduces the
`ArraySlicerError` abort with `channelavg=True`, `chanbin=2` and
`spw="0:0~30^2"`, while its native strided selection and CASA's explicit
`0:0;2;...;30` selection produce the same physical mask.
[TFCrop channel averaging oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_channelavg_oracle.py)
· [MS RFlag oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_rflag_ms_oracle.py)

## Clip channel averaging can flag nonzero data as zero

In CASA 6.7.6.14, the observed-data iterator runs its weighted kernel
even with `chanbin=0`. A positive visibility with zero sigma is replaced
by zero for clipping and then flagged by `clipzeros=True`. Native flagdata
passes the sample through at width zero. The paired oracle checks both
implementations against the result with averaging disabled.

The complex weighted kernel also squares the normalization. With two
positive weights of `1e-25`, the square underflows in single precision.
The iterator supplies zero to the clip agent despite a finite nonzero
weighted mean. Native flagdata divides by the real sum of weights. The
oracle checks the expected finite mean, the normal signed-weight behavior
and the zero-total-weight case independently.

The same underflow occurs in CASA 6.7.6.14 with casaplotms 2.9.1 after time
and channel averaging a corrected minus model expression. In a bin with
corrected values of 2 and model values of 1, calibrated channel weights
of `1e-25` produce a residual of 0 in CASA. Observed sigma values of
`1e13` instead hide the bin. Native PlotMS compares the real weight sum
directly and preserves the residual of 1 in both cases. The paired
oracle also checks signed nonzero totals and the separate observed mask
when an accumulated inverse variance overflows during time averaging.

In observed-only vector time, channel and antenna averaging, sigmas of
`[2^63, 2^62]` and `[2^62, 2^62]` produce the original time weights
`[1, 4]` and `[4, 4]` in units of Float's minimum positive normal value.
CASA hides the channel bin on each baseline through the same normalization
underflow. Native PlotMS preserves the complex channel means and uses the
original spectral sums of 5 and 8 for antenna reduction. Its three output
points match a CASA control that scales both sigma pairs down by `2^60`.
The shared antenna has Real and Imaginary values of 29/13 (#2240).

The parameter bridge narrows widths before comparing them to the window
length. A width of `4294967298` wraps into two channels, which can flag two
samples even though the whole-window mean falls outside the clipping
range. Native flagdata applies the same whole-window rule as other widths
larger than the window, for both scalar and per-window forms.

## PlotMS scalar time plots can fail after encountering a sparse weight spectrum

With `avgchannel="2"`, `avgtime="20"`, and `scalar=True`, CASA 6.7.6.14
with casaplotms 2.9.1 can export a corrected-minus-model plot when the
first selected input buffer starts with an undefined `WEIGHT_SPECTRUM`
cell. Reversing the integration times in the same fixture makes the
iterator reach a fully defined buffer before the incomplete one. CASA then
returns `INVALID_ARGUMENT` during export. Native PlotMS resolves spectral
weight availability for each input buffer and uses the scalar `WEIGHT`
values for the buffer that lacks a spectrum. Its four exported coordinates
match the separate CASA control with `WEIGHT_SPECTRUM` removed. Both
implementations agree on the original chronological fixture (#2231).

## PlotMS can refuse a correlation stored in different physical slots

Select `YY` from two baselines in the same integration and spectral window.
The first data description records `[XX, YY]`, and the second records `[YY]`.
CASA 6.7.6.14 with casaplotms 2.9.1 exports either baseline separately and
returns `INVALID_ARGUMENT` when both are selected for scalar baseline averaging.
casa-rs reads each row's selected `YY` slot and combines the two samples in
the scalar output. Its amplitude of 1.2 and phase of 64 degrees match a CASA
control with the original samples and `YY` in slot zero in both descriptions.
The control also uses different `XX` values to verify correlation selection
and preserves the original collapsed baseline metadata (#2234).

The pinned task also refuses different physical `YY` slots when vector time
averaging feeds a per-antenna average. The paired fixture records
`[XX, XY, YX, YY]` on the first baseline and `[YY]` on the second. After
selecting `YY`, both buffers have the same selected shape. casa-rs emits one
point per antenna, with Real DATA and Imaginary CORRECTED_DATA coordinates
of (2, 4), (1, −2), and (3, −6). These match the CASA control with `YY`
in slot zero in both descriptions (#2239).

## Time-averaged clipping and TFCrop can substitute zeros for defined inputs

In CASA 6.7.6.14, time-averaged `flagdata` compares zeros when clipping or
fitting TFCrop or RFlag over `RESIDUAL_DATA`, `FLOAT_DATA`, `WEIGHT` or
`WEIGHT_SPECTRUM` on the paired fixtures. Those modes share
`FlagDataHandler::setTimeAverageIter`, which leaves those averaged buffers
zeroed, so CASA misses nonzero averaged values and flags the selected samples
when clipping inside a narrow range around zero. The time clip, TFCrop
time-averaging, and MS RFlag oracles check those effects against the same
physical visibilities and statistics.

Native flagdata averages observed visibilities with inverse variance,
uses the same weights for the model when deriving an observed residual,
and accumulates each weight spectrum over its actual contributing
integrations. The test calculates the expected masks from the stored values,
then compares flags and data using both CASA and casacore readback.
[Time clip oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_clip_timeavg_oracle.py)
· [TFCrop time averaging oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_timeavg_oracle.py)
· [MS RFlag oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_rflag_ms_oracle.py)

## Decorated flag selections can alter unnamed correlations

In CASA 6.7.6.14, manual, quack and elevation flagging ignore the correlation scope
when a recognized visibility operator appears in the expression. For example,
`correlation='ABS_XX'` changes all four stored linear-correlation planes,
including the cross-hands. `NORM_UNKNOWN` can also select all products.
The `tools/casa-compat/flagdata_routine_expressions_oracle.py` comparison
reproduces the extra flags and checks casa-rs against explicit CASA selection
of the named planes. `ALL` still selects every stored correlation. An
expression with no known product leaves the flags unchanged.
An elevation comparison verifies that `ABS_YY` selects only YY in
casa-rs. CASA changes all four planes with that expression. An extension
comparison starts with four flagged XX channels. With `growfreq=50`,
`growtime=0`, and `extendpols=True`, CASA fills both XX and YY for
`ABS_XX`. Native extension fills all eight XX channels and preserves
YY, matching the explicit CASA `XX` product.
[Elevation oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_elevation_oracle.py)
· [Extension oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_extend_oracle.py)

The shadow oracle also reproduces CASA's widening of decorated correlation
selections. `ABS_YY` and `ABS_UNKNOWN` change both stored planes in CASA.
Native shadow flagging matches explicit `YY` for the first expression and
leaves the flags unchanged when the expression names no stored product.
[Shadow oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_shadow_oracle.py)

The reference-antenna integration oracle verifies the same protection for
`mode='antint'` with `ABS_XX`. Native changes match explicit `XX` and
preserve the other correlation's stored flags.
[Antenna integration oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_antint_oracle.py)

## Time-averaged clipping and TFCrop can derive row flags from a subset of channels

With `timeavg=True`, `channelavg=True` and `spw="0:1~2"` on a
four-channel window, CASA 6.7.6.14 can set `FLAG_ROW` when the selected
channels meet the clip range. Because TFCrop shares
`AveragingTvi2::writeFlag`, `mode='tfcrop'` with `timeavg=True`,
`channelavg=True`, `spw="0:8~19"` and `extendflags=True` reproduces the same
row-flag promotion over unflagged unselected channels, and time-averaged flag
writes can also clear pre-existing `FLAG_ROW=True` or vary on affected bins.

Native time-averaged clipping and TFCrop preserve pre-existing row flags and
set new row flags only when all original cells in the changed row are flagged.
The paired oracles verify both the selected sample flags and the original-axis
row status, including whole-window controls where CASA writes the same row flags.
[Time clip oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_clip_timeavg_oracle.py)
· [TFCrop time averaging oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_timeavg_oracle.py)

## Shadow flagging can drop the last digit of an antenna's Z coordinate

In CASA 6.7.6.14, the parser for `flagdata(mode='shadow', addantenna=...)`
can drop the last character before the closing bracket in a position
line. An extra dish with an ITRF Z coordinate of `3554865` is read at
`355486` metres when the line ends in `3554865]`. In the five-antenna
fixture, the truncated value leaves all the affected baselines unflagged.
The padded file and dictionary with the supplied coordinates agree on
32 shadowed rows.

casa-rs reads the complete coordinate and reproduces the mask verified
by the padded file, dictionary, and independent Measures projection.
[Shadow oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_shadow_oracle.py)

## Reference-antenna integration flagging can crash on an autocorrelation

In CASA 6.7.6.14, `flagdata(mode='antint', antint_ref_antenna='A0')`
can crash when the reference antenna has a stored autocorrelation and
all four cross-baselines are also present. The agent allocates four
reference-baseline entries but tries to accumulate flags from five rows.

Native antint uses the cross-baselines to decide whether the integration
meets the threshold and applies that decision to all selected rows, including
the autocorrelation. The pinned oracle probes the intermittent crash in
subprocesses and independently verifies the complete native sample and row masks.
[Antenna integration oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_antint_oracle.py)

## Reference-antenna flag decisions can leak to later spectral windows

In CASA 6.7.6.14, two spectral windows can share an integration timestamp.
When only SPW 0 has enough flagged reference baselines, `mode='antint'`
flags both windows. When only SPW 1 has enough flags, CASA flags SPW 1
alone. The agent retains its decision by timestamp across windows.

Native antint evaluates each visibility-iterator integration separately.
Its stored sample masks match explicit CASA selection of each spectral
window and preserve the other window's flags in the combined operation.
[Antenna integration oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_antint_oracle.py)

## TFCrop can fit frequency during a time-only pass

CASA 6.7.6.14 identifies the active axis by its length in part of TFCrop.
With 32 integrations and 32 channels, `flagdimension='time'`,
`timefit='poly'` and `timecutoff=3`, a stationary narrowband signal flags
channel 16 in all 32 integrations. With 33 integrations, the time-only pass
leaves the same stationary signal clear.

Native TFCrop uses the named time axis regardless of the buffer shape.
Both fixture sizes yield the same empty flag mask for this signal.
[TFCrop fitting oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_fit_oracle.py)

## TFCrop post-extension can flag channels outside an explicit selection

CASA 6.7.6.14 applies TFCrop's automatic extension to the full spectral
window. With `spw='0:0~7'` on a 32-channel window, an existing XX flag at
channel 16 is copied to YY even though the selection excludes channel 16.

Native TFCrop uses the full-window counts and writes the extension results
within the selected range. The paired fixture also verifies that seven
existing flags in an eight-channel selection do not meet the 80 percent
frequency threshold for the 32-channel window.
[TFCrop fitting oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_fit_oracle.py)

The same extension can copy an XX flag to YY when the caller selects only
XX. Native TFCrop grows flags into the requested physical planes. An input
flag on YY still contributes to extension in selected XX. A derived Stokes
fit writes its contributing planes as requested.
[TFCrop correlation oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_tfcrop_correlations_oracle.py)

## An invalid flagdata interval can yield inconsistent time-averaged flags

In CASA 6.7.6.14, run `flagdata(mode="clip", timeavg=True, timebin="40s",
clipminmax=[1.2, 2.2], ntime="-1s")` on fresh copies of the same 120-row
fixture. Consecutive calls in one process can flag 324 or 674 samples.
The same inconsistency occurs with `ntime=float("nan")`. The handler
warns and leaves its interval uninitialized. casa-rs uses the default
scan interval, which consistently flags the same 324 samples as explicit
`ntime="scan"`. The original parameter is recorded in HISTORY.
[`flagdata_routine_controls_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_routine_controls_oracle.py)

## Flagdata report display can stall in headless CASA

With CASA 6.7.6.14 in a headless Python installation without
`casaplotserver`, `flagdata(mode="summary", action="calculate", display="report")`
reports the missing module and fails to return within ten seconds. The
casa-rs headless provider returns the ordinary summary record and preserves
the input flags and metadata. The oracle checks the returned record against
pinned CASA's summary with display disabled.
[`flagdata_routine_controls_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/flagdata_routine_controls_oracle.py)

## Weight clipping can ignore defined channel weights

In CASA 6.7.6.14, `flagdata(mode="clip", datacolumn="WEIGHT_SPECTRUM")`
can use scalar WEIGHT for all rows when the first selected row has an
undefined spectrum. The same behavior occurs with `datacolumn="WEIGHT"`.
For example, clipping inside 0.9 to 1.1 flags a channel in a later row
whose spectral weight is 2 because its scalar weight is 1.

casa-rs uses scalar weights for the undefined cells in this case and uses
the stored spectra where present. The paired test verifies the extra CASA
flags and the native channel mask independently. It also checks that an
undefined cell following a defined first spectrum still raises a read
error. [Standing owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## Residual clipping can use the wrong observed column

In CASA 6.7.6.14, `flagdata(mode="clip", datacolumn="RESIDUAL_DATA")`
uses FLOAT_DATA as the observed operand when an MS contains both DATA
and FLOAT_DATA. On the paired fixture this leaves a DATA residual of 1
unflagged when clipping inside 0.9 to 1.6.

casa-rs subtracts the complex MODEL_DATA from DATA when both observed
columns exist. An MS whose only observed column is FLOAT_DATA uses that
column and keeps the imaginary part of the model in the residual. The
oracle verifies the masks and original flag backups. [Standing owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## An overflowing observation selector flags a different observation

In CASA 6.7.6.14, `flagdata(observation=4294967296)` selects observation 0.
Passing the same decimal value as text also selects observation 0. The
selector converts an ID to a signed 32-bit value without a range check.

casa-rs refuses out-of-range observation IDs before writing flags. The
paired oracle applies both selectors in CASA to verify which rows change
and checks that casa-rs preserves the input flags. [Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## An array selector can flag another subarray or terminate CASA

CASA 6.7.6.14 reads the array ID in `flagdata(array="4294967298")` as
subarray 2. The comparisons `>1002` and `>=1002` can also abort with a
negative-shaped array while preparing an auxiliary list of IDs.

casa-rs rejects the overflowing ID and applies comparisons directly to
the stored ARRAY_ID values. The oracle verifies both aborts in separate
processes and checks the flags for real rows with ARRAY_ID 1003.
[Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## `flagdata` can flag other feeds when a feed is selected

CASA 6.7.6.14's `flagdata` accepts a feed expression but does not apply it to
the row selection. In a MeasurementSet containing same-feed and cross-feed
rows, `flagdata(feed="0&2")` flags both kinds of row without a warning.

casa-rs applies the feed selection before calculating or writing flags.
The paired oracle reproduces the extra writes in CASA and verifies the
native row selection against CASA `split` and the explicit feed pairs.
[Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## A NumPy autocorrelation switch can flag cross-correlations

CASA 6.7.6.14 accepts `flagdata(autocorr=numpy.bool_(True))`. Its manual flag
agent uses the default false switch and writes flags on cross-correlations.
A Python `True` restricts the same call to autocorrelations.

casa-rs honors the true value in both scalar forms. The paired oracle
reproduces the extra writes and checks the native mask against CASA's
Python-Boolean product and the stored antenna pairs.
[Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

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

## A blank Stokes string can crash coordinate construction

- Date noted 2026-11-19
- Affected code is casatools `src/tools/coordsys/coordsys_cmpt.cc` in
  `addcoordinate`.

CASA 6.7.6.14 exits with SIGSEGV on
`coordsys().addcoordinate(stokes=" ")`. In casa-rs that input adds no
Stokes coordinate, matching the empty-string case.

When `coordsys.fromrecord` receives a nonstring `parentName`, CASA reports
a record type error after installing the decoded coordinates. casa-rs
validates the parent-name field before replacing the existing system.

CASA also terminates the process when a coordinate replacement vector has
two dimensions, such as `worldreplace0=[[1.0]]` for a one-axis Stokes
coordinate. Casa-rs reports a record type error and retains the former
coordinate system.

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
`int64` array, as it does for every other integer column. This also
affects the `_COUNT_` column of a result from TaQL `COUNT`:
`getcol("_COUNT_")` raises the same error; `getcell("_COUNT_", row)`
returns the count. See `taql_count_int64` in that recording (#1028).

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

## Some `tb` slice calls with invalid coordinates abort the process

- Date noted: 2026-10-30
- Status: CASA bug; casa-rs does not follow it (#1025, the `table` tool)
- Affected code: casatools slice calls that pass coordinates directly to
  casacore's `Slicer`

On a 3×4 CASA image opened as a table, CASA 6.7.6.14 aborts on
`tb.getcolslice("map", [0, 0], [2, 3], [1])` with an uncaught
`casacore::ArraySlicerError` and exit status 134. The one-element step has a
different rank from the cell. casa-rs takes that step on every axis and
raises a `RuntimeError` for invalid coordinates. The `slices` and
`ms_slices` oracle recordings contain calls for which CASA returns or raises
a Python exception.

`getcellslice` on the same image expands the default step `[1]` to every
axis and raises Python exceptions for invalid coordinates. It accepts either
`-1` or `[-1]` to select the first or last index of every axis for the
respective corner. CASA rejects negative indices inside a multidimensional
corner for `getcellslice`. For `getcolslice`, a `-1` top-right index selects
the last element of that axis. casa-rs follows both conventions.

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

## `imhead` displays a bogus observation date after calendar overflow

On a CASA image, CASA 6.7.6.14 accepts `imhead(mode='put', hdkey='epoch',
hdvalue={'value': 1e20, 'unit': 'd'})` and then reports
`-5884082/10/30/00:00:00.000000`. The stored MJD exceeds the signed integer
range used by `MVTime::ymd` for the Julian day. casa-rs rejects the out-of-range
edit before changing the image. Dates inside the representable range still
work, including 1.01 billion MJD days, which both render as
`2767144/12/15/00:00:00.000000` (#977).

## `imhead` can save a direction reference frame that prevents reopening the image

In CASA 6.7.6.14, `imhead(mode='put', hdkey='equinox', hdvalue='APP')`
returns successfully on a J2000 image. It stores APP as the native
frame and retains J2000 as the conversion frame. Reopening the image then
fails with `Missing information in Frame specified for conversion`.
The same behavior occurs for JMEAN and AZEL. The saved direction coordinate
does not supply the epoch or observer position needed by those conversions.
casa-rs checks the conversion before updating the image and preserves the
existing readable coordinates if the context is missing (#977).

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

## `feather` drops units and restoring beams in its per-plane path

CASA 6.7.6.14 combines the `papersky_standard.intimage` and
`papersky_standard.sdimage` upstream cubes into pixels calibrated in the
high-resolution beam. The parent output image has an empty brightness unit
and no restoring beams. casa-rs retains the high-resolution `Jy/beam` unit
and the matching per-plane beam set. On the upstream test cubes, all three
pixel planes agree with CASA to a normalized RMS below `3e-7` (#977).

CASA's per-plane slicing also limits the low-resolution sky plane to the
high-resolution pixel bounds before regridding. With centered 8- and
12-column images, this discards low-resolution signal in columns 8–9
that maps to high-resolution columns 6–7. casa-rs pairs the channel and
Stokes indices while regridding the complete low-resolution sky plane.

## `feather` rounds an integer-aligned image edge inconsistently

CASA 6.7.6.14 can lose or extrapolate boundary values when its sky-coordinate
round trip lands beside an exact integer pixel. In the smaller-low-image
fixture, the reference pixels differ by exactly three columns. Placing the
low-resolution crop at that offset with zero padding before CASA's Fourier
combination gives an independent reference. Native feathering of the original
crop agrees with it to a normalized RMS of `1.93e-7`. CASA's direct regrid
of the crop differs from that reference by `6.01e-4` (#977).

## `feather` uses direction increments as radians

In CASA 6.7.6.14, `StokesImageUtil::MakeGaussianPSF` uses the numeric
direction increments as radians when sampling the restoring beam for
`feather`. The increments still carry the coordinate's declared units.
With a 64 × 64 × 1 × 4 cube, 1 arcminute pixels, and beam major axes of
3 and 8 arcminutes, storing the direction units as arcminutes makes the
crossover kernel an impulse. CASA returns the low-resolution image
multiplied by the beam-area ratio `(3/8)^2`. Expressing the same grid in
radians retains the high-resolution signal. The two CASA products differ
by 0.476 normalized RMS relative to the radian result.

casa-rs samples the beam using direction increments in radians. On those
inputs its two products agreed within `7e-8` normalized RMS, and the native
product agreed with the radian CASA result within `7e-7`. The
`direction_units_do_not_change_the_fourier_combination` regression uses
the pinned CASA radian reference with the same physical grid stored in
arcminutes (#977).

## `imregrid` can terminate after a direction pixel axis is removed

In CASA 6.7.6.14, make the right-ascension axis degenerate and drop it with
`imsubimage(dropdeg=True)`. Copy the product and shift the Declination
reference pixel by 0.25 in the copy. With the recorded `dropdeg_ra.image`
fixture, `imregrid(axes=[0], decimate=1)` from the original to the shifted
template terminates CASA with a bus error after creating the output mask.
casa-rs returns a recoverable error and leaves the output absent. The
coordinate layer can still open and transform both input grids (#977).

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

## PlotMS reuses antenna angles when fields share a timestamp

- Date noted: 2026-11-24
- Status: casa-rs evaluates each buffer at its selected field's phase centre (#1032)
- Affected code: CASA `VisibilityIteratorImpl2::configureNewChunk`,
  `azel` and `feed_pa`, called by the PlotMS MeasurementSet cache

CASA 6.7.6.14 with casaplotms 2.9.1 keys the antenna azimuth, elevation
and feed parallactic angle caches only by time. Two consecutive field
buffers with the same timestamp reuse the first field's coordinates when
both are selected. For Channel/Ant-Elevation, CASA's separate plots give
82.98457° and 75.14170° for the first antenna, while the joint plot shows
82.98457° for both fields. The casa-rs joint plot agrees with the separate
CASA selections for all 30 plotted points and their identities
(`plotms_ms_metadata_axes_oracle.py --only antenna-horizontal-same-epoch`).
The same joint-cache issue was observed for Ant-Azimuth and Ant-ParAngle.

## PlotMS can refuse a second ephemeris export in one session

- Date noted: 2026-11-24
- Status: casa-rs uses the current static request for each export (#1032)
- Affected code: the pinned casaplotms 2.9.1 public task and application

With CASA 6.7.6.14, exporting Channel/Radial Velocity to TXT and then
requesting a PNG with the same selected Uranus MeasurementSet produced
`INVALID_ARGUMENT` with no PNG. The same PNG request succeeds when it
is the session's first export. Time/Distance (rho) has the same
session dependence. The paired test starts a fresh reference session
for each export so each comparison uses the requested data and axes.

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


The tuned bounds also pass through `SynthesisImager::doubleToString`,
which uses `std::to_string`. Its six fractional digits are part of
the channel selection. On the LSRK input cubic AW fixture with BARY
output, CASA converts the upper tuned edge to 1524999999.9999998 Hz
and passes 1525000000.000000 Hz to the iterator. A stored channel
starting at that rounded edge is included. The same TOPO request
already has an upper edge 2.4 μHz above it. casa-rs applies the
observed rounding before the inclusive channel intersection. A
channel whose edge remains beyond the rounded bound stays outside.

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

The same cache dependence affects the first plane of an LSRK image of
TOPO data. In CASA 6.7.6.14, `refim_eptwochan.ms` imaged with the
standard gridder, natural weighting, two channels, `start=0, width=1`
and linear interpolation can give first-plane sumwt of either 0 or
552. Evaluating one TOPO-to-LSRK frequency at 300 s before the earliest
integration with the stock Measures tool changes the result from 0 to
552. Both runs report the same first centre, 999988750.3872572 Hz,
and second-plane sumwt of 321786.

`VisibilityIteratorImpl2::getFrequencies` converts the visibility
frequencies at each buffer's epoch. `FTMachine::interpolateFrequencyTogrid`
samples the image's LSRK grid within those row bounds. On the native
first buffer the lower visibility frequency is 999988750.3872573 Hz.
The one-point fine grid derived from the image edges puts its first
sample exactly there. At the next integration even the lowest visibility
frequency is about 2 Hz higher, so only the first buffer contributes
to the first plane. The same bound rejects a fine point below the
first visibility frequency by one representable step.

With CASA's first plane retained and the direction mask supplied on the
output grid, the CASA and native Högbom runs each keep 156 mask pixels per
plane. Sumwt and controller totals agree exactly. All seven images agree
within 1.21e-7 normalized RMS. The original cold-CASA zero plane is
cache-dependent. [Measurements and regression in #823](https://github.com/bglenden/casa-rs/issues/823).

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

For the channel-index TOPO cube on LSRK data in #1256, that ratio
can differ for the whole track. In the five-channel VLA fixture, stock
CASA uses one sample per plane at `width=2`. With fresh aberration
coefficients in both runs, changing only the cached UT1−UTC value to
the value at the image epoch changes the ratio from just below two to
exactly two, doubling the sample count.

For a standard, natural-weighted, linear channel-index TOPO cube on
LSRK data, casa-rs evaluates the grid in LSRK and takes the lower
sample count at an exact integral ratio above one. Ratios on either
side still use CASA's floor. Strict interpolation bounds continue to
apply to every gridded sample. The width-two control agrees with stock
CASA, and the width-one control still agrees.
[Trace and controls in #1256](https://github.com/bglenden/casa-rs/issues/1256#issuecomment-6881726364).

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

## Explicit channel-zero range can terminate Python

On a `[3, 4, 8]` image, CASA 6.7.6.14 can terminate with SIGSEGV
when `image.collapse("mean", axes=2, chans="range=[0pix,0pix]")`
reads the spectral bounds. `AnnotationBase::setFrequencyLimits` treats two
numeric zeros as an absent range, while the caller indexes both bounds.
Casa-rs selects channel zero for the explicitly unit-bearing range.

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

## A one-channel mosaic cube can lose its residual mask during a major cycle

- Date noted: 2026-11-14
- Status: CASA cache-dependent defect. The corresponding one-channel
  restart check remains on [#944](https://github.com/bglenden/casa-rs/issues/944).
- Affected code: `SynthesisImagerVi2::runCubeGridding`,
  `SIImageStore::copyMask`.

On CASA 6.7.6.14, image one channel of `refim_alma_mosaic.ms` with
`gridder='mosaic'`, `specmode='cube'`, natural weighting, Högbom,
480 pixels, 0.06 arcsec cells, `pblimit=0.2` and `pbcor=True`.
CLEAN eight iterations, edit one pixel of the residual mask, then
resume for twelve iterations with `calcpsf=False, calcres=False`.
CASA logs that `residual/mask0` is already open in the table cache
during both calls. Its mask copy removes the old default before trying
to create the replacement. The error is caught as a T/F mask warning.
The residual and restored image are left without masks even though the
saved PB has `mask0`. In the corresponding two-channel runs the copy
succeeds and both products receive the saved PB mask.

casa-rs keeps the generated PB mask on the positive-cutoff cube path.
In the initial one-channel CLEAN, the raw CASA masks differ by 45,662
pixels on each of these two products. All nine products have normalized
RMS at most 5.20e-7 and the controller counts agree. See
[#1424](https://github.com/bglenden/casa-rs/pull/1424) for the separate
verified two-channel saved-product behavior.

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

## CASA drops NumPy scalars from mixed quantity-record sequences

- Date noted: 2026-11-19
- Status: CASA loses input values; casa-rs rejects the record (#1170).
- Owner decision: Correct CASA data loss under the standing rule in `AGENTS.md`.
- Evidence: `tools/casa-compat/quantity_record_sequence_edges_oracle.py` and its CASA 6.7.6.14 fixture.

For `{"unit": "m", "value": [1, np.uint64(2)]}`, CASA returns a scalar 1 m.
For `[1.5, np.uint32(2), 3.5]`, it returns the vector `[1.5, 3.5]`.
A NumPy boolean in a mixed sequence is dropped the same way. The Python
binding leaves these scalar types out as it constructs its variant. casa-rs
raises the malformed-quantity-record error for those sequences, matching
CASA's existing error for a sequence containing only those scalar types.
Callers see an error before computing on a shortened quantity. Unsigned
NumPy arrays retain their existing integer-array behavior.

When a sequence mixes Python booleans and numbers, CASA also drops
`np.timedelta64` and `np.longlong` values. casa-rs refuses these mixed records
as well, so a timedelta's storage unit cannot turn it into a measurement.

## CASA masks quantity-record integer overflow with a wrapper error

- Date noted: 2026-11-19
- Status: The CASA Python wrapper returns with a pending exception; casa-rs reports the underlying overflow (#1170).
- Evidence: `tools/casa-compat/quantity_record_sequence_edges_oracle.py` and its CASA 6.7.6.14 fixture.

Native Python integers outside the signed 64-bit range, such as `2**63` or
`[2**63]` in a quantity record's `value`, cause CASA's Python conversion to
return a result with a pending `OverflowError`. CPython reports a
`SystemError` with the overflow as its cause. casa-rs reports
`OverflowError` directly so callers can handle the range failure. In-range
native integers still narrow to the same signed 32-bit value as CASA,
including the singleton list, tuple, and rectangular nested forms.
Mixed boolean/floating sequences also report overflow before promoting a
native integer outside that range to a floating value.

## An empty floating epoch vector can terminate CASA

In CASA 6.7.6.14, a `float64` NumPy array of shape `(0,)` or `(1, 0)`
inside the `m0.value` field of an epoch record can terminate the process
during `coordsys.setepoch`. casa-rs returns `False` and preserves the
current observation date (#1626).

CASA can also misread a byte-swapped `float64` epoch vector. For example,
a one-element vector containing `20.0` days can be stored as a subnormal
value. casa-rs converts the byte order and stores `20.0` days.

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

For Briggs and uniform weighting on the same BARY-to-TOPO axis, the
control also captures the density-weighted samples at CASA's weight
interpolation call. The PSF and residual passes give identical weights.
The materialized data reproduce the standard products within 1.64e-4
normalized RMS. Natural-weighted CASA AW nearest on the materialized
weights reproduces the weighted casa-rs AW products within 8.1e-7,
with identical masks and iteration counts. The saved MODEL_DATA
agrees within 7.6e-7 normalized RMS.

The stale map also affects cubic AW cubes. On six buffers of
`refim_point_withline.ms` using LSRK channel frequencies, the release
AW gridder drops planes on aligned and half-channel-offset axes. With
eight selected channels on an aligned grid it can segfault. Capturing
the standard CASA gridder's cubic samples reproduces its dirty
products exactly when the samples are put on a nearest-neighbor grid.
CASA AW nearest on those samples and the original cache reproduces
the native AW dirty products and beam fits. For the offset clean,
the control cubic-interpolates the CASA AW model back to the stored
channels, subtracts it before standard CASA's cubic interpolation,
grids the resulting samples with CASA AW nearest and restores with
CASA. The dirty and final clean products agree within 9e-7 normalized
RMS, the masks and controller counts agree exactly, and MODEL_DATA
agrees within 1.03e-6 normalized RMS. The independent prediction
uses the exact requested 1.275 GHz start and 50 MHz spacing to apply
the strict halo bounds. The 1.65 GHz stored channel equals the lower
bound of the upper halo and remains excluded. CASA's image coordinate
evaluates that boundary one floating-point step lower, which can
admit the equality case.

For native-LSRK cubic cubes with Briggs and uniform weighting, CASA's
standard gridder applies the per-channel density weights by nearest
neighbor while interpolating the data cubically. Capturing both gives
identical standard dirty products when replayed with the weights in
`WEIGHT_SPECTRUM`. CASA AW nearest applies those captured weights
once, with the original cache. The clean comparison again predicts on
stored channels before residual resampling. The two dirty and clean
pairs agree with casa-rs on their masks, beams, controller counts and
MODEL_DATA.

For TOPO input to the same LSRK cubic cube, the measured input spacing
after conversion is slightly smaller than the 50 MHz image spacing.
CASA GridFT builds ten fine-grid samples for the eight image channels.
The control keeps the ten samples on the materialized MeasurementSet
and assigns them to the original eight-channel output axis. This
reproduces all standard dirty products exactly for natural, Briggs
and uniform weighting. The clean control evaluates predictions at
each buffer's converted frequencies before sampling the residuals.

For LSRK input into a tracked BARY cubic cube, CASA's
`BriggsCubeWeightor::getWeightUniform` narrows the visibility buffer's
reporting LSRK frequency divided by c to Float before forming the
density coordinates. Applying the equivalent BARY frequency and scaled
baseline before narrowing moved one sample from v = 619.5 to v =
619.49994. The adjacent cells gave uniform weights of 1/6 and 1/4.
The AW catalog route now undoes the wavelength rescaling for this lookup.
The Briggs and uniform controls each agree across 17 dirty and clean
products and 48 fitted beams. The worst normalized RMS is 9.38e-7
for products and 1.11e-6 for MODEL_DATA (#1121).

With native TOPO rows and a natural-weighted tracked TOPO cubic image,
CASA converts each visibility buffer to LSRK. At 50 MHz spacing the first
buffer's interpolation includes two lower halo frequencies outside the
eight image planes. The later buffers interpolate directly on the eight
centres. Replaying the in-image samples on their LSRK grid through
CASA AW nearest with the original cache gives the same dirty and clean
products as casa-rs. The 17 products and 48 beams agree, the masks
and controller counts match exactly, and the largest product and
MODEL_DATA normalized RMS are 9.16e-7 and 9.41e-7 (#1121).

In the Briggs and uniform variants of this TOPO cube, the density lookup
takes the nearest native weight in LSRK. CASA's unit conversion and
per-hop Doppler arithmetic place plane 5 of the first buffer one Float64
step above the midpoint. The upper channel changes 55 uniform weights
and the resulting PSF. Retaining the LSRK interpolation grid and applying
the per-hop conversion to the native centres reproduces the CASA weights.
When the image and native channel widths coincide, the spacing of the
first two selected native channels determines whether CASA uses a finer
grid or converts each reporting centre directly. The density grid moves
the reporting reference pixel before evaluating its first spacing. Its
lookup converts each LSRK channel back to the reporting frame before
rounding the pixel. A tuned model-column run can select different first
channels, so the clean control uses the tuned CASA iterator. The three
weighted dirty and clean pairs agree across 51 products and 132 beams
with exact masks and controller counts. The largest normalized RMS is
8.80e-7 for products and 8.98e-7 for MODEL_DATA (#1121).

For TOPO input on a tracked BARY axis, CASA refuses a 50 MHz output
width because the selected native width converts to 50.0024 MHz. At
50.01 MHz, all six buffers have two cubic halo samples before the
eight in-band LSRK frequencies. CASA AW nearest with those in-band
samples and the original cache reproduces the dirty and clean
natural-weighted products. The reported BARY WCS comes from direct
CASA, and the clean control predicts on the original channels before
resampling the residual. All 17 products and 48 beam comparisons pass,
with exact masks and controller counts. Maximum normalized RMS is
9.72e-7 for products and 9.26e-7 for MODEL_DATA (#1121). Briggs and
uniform on that grid use the captured CASA density weights. All 34
weighted products and 96 beams pass, with exact masks and controller
counts and maxima of 9.00e-7 product NRMS and 1.23e-6 MODEL_DATA NRMS.

For BARY input on a tracked TOPO axis, each of the six buffers has the
same native LSRK frequencies. On the eight-plane, 50.01 MHz grid, the
two lower cubic halo samples are outside the image. The same-cache
CASA AW control uses the eight in-band frequencies. The three
weightings pass all 51 dirty and clean products and 144 beams, with
exact masks and controller counts. Maximum product normalized RMS is
9.32e-7 and MODEL_DATA normalized RMS is 1.32e-6 (#1121).

The BARY-to-tracked-BARY cubic case uses the same BARY data and
50.01 MHz width. Each buffer again places eight LSRK samples inside the
image after two lower halo samples. The matching-cache AW replay agrees
for natural, Briggs and uniform weighting across 51 products and 144
beams, with exact masks and controller counts. The largest normalized
RMS values are 9.62e-7 for products and 1.08e-6 for MODEL_DATA (#1121).

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

For linear interpolation on this fixture, CASA shifts the input channel
centres and widths before regridding. A frequency-valued start and width
already describe the REST grid. The 500 MHz width is below CASA's
converted 500.028 MHz minimum. At 500.1 MHz, direct CASA reports the
requested 1.30 GHz start and 500.1 MHz spacing. Its AW resampler
restores the native channel map after interpolation, which associates
in-band values with the wrong native ordinals. The unmodified AW path
with the frame initialized gives PSF NRMS 0.844 against the shared
resampler's output.

The comparison takes the two in-band samples from CASA GridFT, replays
them with the original AW cache and preserves the wavelength products
at each epoch. The clean control predicts on the stored channels,
resamples the native residual, and restores with CASA. All 17 products
and 12 beams agree with exact masks and controller counts. Maximum
product normalized RMS is 3.29e-7 and MODEL_DATA is 3.54e-7 (#1121).

The same native-ordinal problem affects an eight-plane REST cube with
cubic interpolation. The 20-channel VLA fixture selects channels 3–14
and asks for a 1.275 GHz start and 50.01 MHz spacing. The frame-initialized
CASA AW run differs by 0.191 in PSF normalized RMS and 0.549 in residual
normalized RMS. The CASA GridFT capture and wavelength-preserving
same-cache replay agree on all 17 dirty and cleaned products and 48 beams.
Masks and controller counts match exactly. Maximum product normalized
RMS is 4.30e-5 and stored-channel MODEL_DATA is 1.82e-6 (#1121).

For Briggs and uniform weighting of this moving-source AW cube,
`BriggsCubeWeightor` accumulates its density through GridFT at the
rotated baselines and REST wavelengths. Its lookup maps the native
channel to a REST plane and uses the stored baseline and frequency for
the density cell. CASA pads this one-field selection by one plane on
each side. Applying the imaging AW wavelength scale to the density grid
would move its samples to different cells. The native path keeps the
density grid and lookup coordinates of this CASA mode. CASA's captured
weights and original cache give 34 passing dirty and clean products,
96 beam checks, exact masks and controller counts, maximum product
normalized RMS 9.09e-7, and MODEL_DATA below 1.81e-6 (#1121).

## Offset data-frame AW cubes reuse the native channel map

With `specmode='cubedata'`, CASA publishes the native spectral grid as
`Undefined` and leaves frequencies unconverted. The aligned natural
nearest control agrees directly. On the 20-channel TOPO fixture with
`spw='0:3~14', start='1.275GHz', width='50MHz', nchan=8`, raw CASA AW
uses native ordinals after interpolation. Its PSF normalized RMS is
0.557 for both linear and cubic. Capturing CASA GridFT's interpolated
samples and replaying them through the matching AW cache preserves
their frequencies and plane assignment.

The CASA quantity conversion divides by the unit-scale ratio, so the
first centre is 1274999999.9999998 Hz. Rounding it to 1275000000 Hz moves
the strict prediction halo boundary onto the 1.65 GHz stored channel
and incorrectly excludes that channel. The exact conversion matches
CASA without expanding the halo. Together the aligned nearest and
offset linear/cubic comparisons pass 51 products and 132 beams, exact
masks and controller counts, maximum product normalized RMS 9.67e-7
and MODEL_DATA below 1.73e-6 (#1121).

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

## `image.toworld` and `image.topixel` abort on a multidimensional numeric record

Attach a 4 by 4 float image, then call either method with
`{"numeric": np.array([[1., 2.], [3., 4.]])}`. CASA 6.7.6.14 terminates
with SIGABRT and `Vector<T>: ndim of other array > 1`. The casa-rs tool
raises `RuntimeError` because the numeric field must be one-dimensional.
The direct array form of a pixel position still accepts CASA's flattened
input.

## `image.topixel` can write outside the world vector for a sliced direction measure

Create a CASA image with shape `[1, 8, 6]`, call `child = ia.subimage(dropdeg=True)`,
then call `child.topixel(child.toworld([2, 1], "m"))`. CASA 6.7.6.14 aborts with
`free(): invalid pointer` after assigning the removed longitude through a
negative world-axis index. casa-rs uses the surviving declination and spectral
measure and returns `[2, 1]`, consistent with the numeric, quantity, and
string encodings of the same position.

## Measures spectral conversions reject or truncate valid vectors

CASA 6.7.6.14 `me.tofrequency` and `me.toradialvelocity` return only the
first result for a Doppler record whose `m0` contains two velocity values.
`me.torestfrequency` likewise drops values when either input is a vector.
Both elements convert successfully in separate scalar calls. This silently
loses requested scientific results; the C++ helpers contain element loops,
which also show the intended vector behavior.

With no explicit frame, `me.todoppler('BETA', frequency, '1.42GHz')` rejects
a BARY frequency vector `[1.3e9, 2.6e9] Hz` with `No MeasFrame direction
specified for conversion of Frequency`. The same scalar inputs return
valid Doppler values without a frame. This algebraic conversion does not
need a direction; the vector path introduces a frame-dependent conversion.

casa-rs preserves every input element and broadcasts a scalar against a
vector. Its expected values come from independent CASA scalar calls for
each element, including the approaching source at 2.6 GHz. Original CASA
vector outputs and failures remain in the fixture alongside those controls.
Input offsets follow each helper's semantics: `todoppler` reads the raw
frequency/velocity, while conversions from Doppler resolve its reference
offset. The correction follows the standing rule to correct scientific
data loss and behavior that contradicts CASA's evident intent (#1252).

Evidence: `tools/simulation/measures_spectral_oracle.py`,
`crates/casars-python/python/tests/data/casa_measures_spectral.json`, and
`test_measures_spectral.py` (the `scalar_controls` cases).

### Cyclic spectral arguments terminate the process

CASA 6.7.6.14 terminates with SIGSEGV for a cyclic dictionary or list passed
to `me.doppler(v0=...)`, and for a cyclic record passed to `me.todoppler`.
The first native spectral candidate exposed the same recursion failure in
shared Python value capture. casa-rs now bounds recursive record and sequence
capture at 128 levels and raises `ValueError` naming #1252. Capture state is
restored on failure, so repeated failures and a subsequent valid call work.
The isolated subprocess regressions run with core files disabled.

## Measures geometry constructors lose values and mislabel units

CASA 6.7.6.14 `baseline` and `uvw` accept three metre quantities as
Cartesian coordinates or longitude, latitude and length as spherical
coordinates. Passing the length first, for example
`baseline('ITRF', '3m', '10deg', '20deg')`, causes the binding to reorder
the quantities into an invalid sequence and return a zero baseline.
The angle-first form produces the intended 3 m vector. casa-rs accepts
either order and refuses incompatible dimensions such as three Hz values,
which CASA likewise turns into a zero vector. CASA's quantity conformance
check and angle-first records supply the independent controls.

For `uvw('ITRF', '1m', '2m', '3m', off)` with a valid UVW offset, CASA's
constructor drops the offset, although `measure(absolute, 'ITRF', off)`
computes and returns the correct relative value and reference origin.
casa-rs gives the constructor the same behavior as the generic
conversion. The fixture retains both CASA results.

CASA's spherical `earthmagnetic('ITRF','3nT','10deg','20deg')`
constructor stores a vector of magnitude 3e-9 with units labelled nT.
casa-rs returns the magnitude 3 in nT, checked against CASA's quantity
conversions and an explicit Cartesian magnetic record. CASA also emits
metres as the unit for magnetic `addxvalue` outputs. casa-rs returns those
magnetic components in nT.

## Failed measures geometry calls can expose earlier outputs

After a successful `touvw` or `expand` call, CASA 6.7.6.14 can return
an empty primary record alongside auxiliary coordinates and rates from
an earlier call, even on a fresh tool. casa-rs returns empty records for
all outputs on failure, preserving the tuple layout. For non-geometric
records such as `earthmagnetic`, CASA's `expand` can temporarily change
the type to UVW and return zero coordinates in metres labelled with the
original type. casa-rs refuses the incompatible input.

The inputs, raw outputs and independent controls are in
`tools/simulation/measures_geometry_oracle.py` and
`crates/casars-python/python/tests/data/casa_measures_geometry.json`.
These corrections follow the standing rule on data loss, unit corruption,
and behavior contradicting evident intent (#1252).

## Magnetic reference changes can corrupt field strength

With a frame of 2020-01-01 UTC at ALMA, CASA 6.7.6.14 converts the
Cartesian field `[10000, 20000, 30000]` nT from B1950 to APP with a
magnitude of 22289.454 nT, down from 37416.574 nT. Supplying that same
Cartesian field in APP and requesting B1950 produces three NaNs.
In `MCEarthMagnetic` the direct FK4 E-term correction can act on an
unnormalized magnetic vector, and `MVEarthMagnetic::adjust()` leaves the
norm unchanged during corrections. The same normalization omission is
visible in diurnal aberration: converting this field from J2000 to ITRF
increases its magnitude by 0.009120 nT. An IGRF output uses the ITRF
coordinates and inherits the same error.

Casa-rs applies the casacore direction corrections to a unit vector,
then restores the input field strength. The geometry fixture retains
the raw CASA magnetic outputs and separately captures CASA's direction
conversion at the same angles. The normalized FK4 controls return
finite vectors in both directions and preserve the input strength
(#1252).

## Comet numeric queries can terminate without an epoch

After loading `Ganymede_55437-56293dUTC.tab` with an empty observing frame,
CASA 6.7.6.14 terminates with SIGSEGV in both `cometdist()` and
`cometangdiam()`. The binding dereferences the frame's epoch without
checking whether one was set. This also applies when `done()` empties
the frame while retaining the active table for the comet queries.

Casa-rs reports the missing epoch and returns the same `-1 AU` or
`-1 rad` sentinel that CASA uses when the table has no sample at a
requested epoch. Setting an epoch enables the queries on the retained
table again. The oracle runs each missing-epoch probe in its own process
with core files disabled (#1252).

## Direction shifts can use the wrong longitude quadrant

CASA 6.7.6.14 derives the longitude increment for `me.shift` from an
inverse sine. This confines that increment to ±90° even when the
requested offset points across either pole or into the other longitude
quadrant. From longitude 0° on the equator, a 120° eastward shift returns
longitude 60°. From longitude 0° and latitude 80°, a 20° northward shift
returns the original direction.

Casa-rs uses both east and meridian components to determine the longitude
quadrant. For a direction starting at a pole, the local north axis
is undefined and casa-rs uses CASA's longitude convention. Independent
CASA direction records at longitude 120° on the equator and longitude 180° at latitude 80° yield separations of 120° and
20° from the respective starting directions. The fixture retains those
controls alongside the raw shift results. This follows the standing rule
for results that contradict the documented operation (#1252).
[CASA measures documentation](https://casadocs.readthedocs.io/en/latest/api/tt/casatools.measures.html)

## A rejected increment edit can leave a singular coordinate system

In CASA 6.7.6.14, create a coordinate tool with `cs.newcoordsys(linear=2)`.
The call `cs.setincrement([3.0, 0.0])` raises a singular-matrix error and
leaves the increments at `[3.0, 0.0]`. Casa-rs validates the edited record
before replacing the active system, so a rejected edit leaves the original
increments and the transform usable.

## A rejected unit conversion can leave some coordinates changed

In CASA 6.7.6.14, create a coordinate system with direction and spectral
coordinates and two linear axes. Applying
`cs.setunits(["deg", "deg", "m", "m", "m"])` raises an incompatible-unit error
for the spectral axis and still converts the direction and linear axes.
Casa-rs validates the entire conversion before replacing the coordinate
record, so a rejected call preserves all units and the transform (#1626).

## Batch coordinate conversion can reshape ragged inputs or misread byte order

CASA 6.7.6.14 can pad or truncate a ragged sequence while converting it to
the coordinate matrix used by `toworldmany` and `topixelmany`. NumPy arrays
with non-native byte order can produce different numeric values from the
same array in native byte order.

casa-rs requires rectangular input and converts the array's byte order
before interpreting its values (#1626).

## Frame display can discard the coordinate conversion context

In CASA 6.7.6.14, `showframe()` fails to display many radial-velocity
references even after the epoch, observatory, and direction have all
been set. For a TOPO velocity of 15.123 km/s at ALMA on
2011-01-03 17:00 UTC with a J2000 direction of (10°, 20°),
it reports that the frame epoch is missing. On that same tool,
`measure(velocity, "LSRK")` returns approximately -12.87952549 km/s.

Casa-rs evaluates the displayed LSRK velocity with the explicit frame,
using the same native conversion as `measure`. The fixture retains
CASA's raw display errors and separately records the direct conversion
for all seven affected references. The missing-frame errors still apply
when the required epoch or direction is absent.

At the same observatory, CASA's display also reports a missing
position for LAST and LMST epochs. Casa-rs uses the explicit position
to display TDB, UT1, and TT. The fixture retains the display errors
and verifies the derived time scales against CASA's direct conversions
(#1252).

## Replacing a comet table can leave the previous frame direction cached

In CASA 6.7.6.14, set an epoch and observatory, attach a COMET
direction, and then call `showframe()` or project a radial velocity.
Replacing the attached ephemeris updates direct COMET conversion
immediately. The frame display and velocity projection can still use
the previous table's direction until `doframe(direction("COMET"))` is
called again. The supplied B1950 and TOPO controls at ALMA differ by
more than 230 m/s in the GEO to TOPO velocity projection.

Casa-rs refreshes the computed direction when a new table is attached.
The recorded fixture retains CASA's stale results and verifies the
refresh against its results after reattaching the direction (#1252).

## Frame display can report UT1 outside the Earth-orientation table

With the bundled measures snapshot and an explicit UTC epoch in 2030
or 2100, CASA 6.7.6.14 displays a UT1 value despite missing
Earth-orientation coverage. Casa-rs displays the raw epoch and the
available TDB and TT values, and writes `UT1 = unavailable`. This keeps
the frame visible after `framenow()` when the Earth-orientation table
does not cover the current date. The fixture retains CASA's exact
display and uses direct TDB and TT conversions as independent controls
(#1252).

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


The same setup crash is confirmed through public `applycal(interp=['spline'])`
on the shifted twelve-time G fixture in `tools/casa-compat/applycal_oracle.py`
(#1518): the isolated CASA process terminates with signal 11. Native task
application additionally checks the independent middle half-knot value
`203/160` for the four-amplitude natural spline above. This is separate from
the same-input CASA comparisons; it does not relax their tolerance.

## Applycal per-scan setup crashes with scans in multiple observations

- Date noted: 2026-11-17
- Status: corrected under the standing crash/evident-intent rule (#1518)
- Affected code: CASA 6.7.6.14 `CTPatchedInterp` MS constructor and scan lookup.

A two-observation MS/calibration table with scan 1 in observation 0 and scan 2
in observation 1 terminates with signal 11 for `nearestperscan`,
`linearperscan` and `linearperobsperscan`. Each public-task probe runs in an
isolated process. The source sizes the MS scan axis using
`getScanNumbers(0,0)`, although subsequent lookup uses scans from all
observations. Native groups the selected scalar headers by their actual scan
numbers and gives per-scan precedence over per-observation partitioning.
All three native results agree with CASA's working single-observation
controls for the same samples, calibration values and scan numbers. The
original native observation IDs are preserved. These controls are recorded
separately from same-input comparisons.

## Nearest-field ambiguity guard is disabled by sort options

- Date noted: 2026-11-17
- Status: corrected under the standing evident-intent rule (#1518)
- Affected code: `CTPatchedInterp::setFldMap`, casacore `Sort`/`genSort`.

CASA's nearest-field mapper explicitly intends to reject duplicate minimum
angular separations. Its `genSort` options instead OR `Sort::QuickSort` with
`Sort::Ascending`, whose enum value is -1. This enables every option,
including `NoDuplicates`, so the intended duplicate-minimum check cannot
observe a duplicate. With two calibration FIELD rows having identical
PHASE_DIR/REFERENCE_DIR/DELAY_DIR and different gains, CASA succeeds and
applies an arbitrary tied gain. The public oracle and a fresh CASA process
both reproduce this behavior.

Native restores the explicit ambiguity check, including MS fields outside
the requested selection, and raises a runtime error during setup. DATA,
flags and weights remain unchanged; a newly required CORRECTED_DATA is
initialized from DATA, and no successful-task HISTORY is written. The
oracle records this as an intentional CASA-bug correction, separately from
its matching MAIN/API/state cases. No numerical tolerance changes.

## Applycal nearest-field mapping fails for legacy BPOLY

- Date noted: 2026-11-17
- Status: corrected under the standing crash rule (#1518)
- Affected code: CASA 6.7.6.14 `BJonesPoly::loadMemCalTable` and the
  subsequent nearest-field mapping of the converted B table.

BPOLY conversion ignores the supplied field selector and assigns
`FIELD_ID=-1` to all converted rows. With `gainfield=['nearest']`, CASA
reports that the converted table has no available calibration and the
isolated public task aborts. Casa-rs applies the field-independent BPOLY
solutions for this input, as it does for explicit field selectors. The
public oracle compares the result against an independent CASA run with
the same polynomial table and default gainfield.

## BPOLY writes nonfinite coefficients when every channel is masked

- Date noted: 2026-11-18
- Status: corrected in the native polynomial solve (#31)
- Affected code: CASA 6.7.6.14 `BJonesPoly::selfSolveOne` and the CLIC
  `POLYANT` normal equations.

In a 12-channel window, `maskcenter=12` excludes the entire spectrum. CASA
attempts the singular fit and reports the resulting table in `solve_tables`
with nonfinite coefficients. The native task returns an empty `solve_tables`
entry when no interval can be fitted. A singular interval is omitted when
later intervals still produce valid solutions. Output creation and
replacement use the shared calibration table lifecycle. The pinned BPOLY
oracle checks the CASA product, the native return, and retained intervals.

The direct polynomial setter rejects degrees below -1. CASA 6.7.6.14 can
abort in `solve` while constructing a negative-sized coefficient array.

Configuring an existing BPOLY output with `append=False` deletes it
immediately in CASA 6.7.6.14. If the subsequent fit raises, the earlier
table is gone. Casa-rs keeps that table until a new solution replaces it.

## BPOLY reads a second channel from a singleton selection

- Date noted: 2026-11-18
- Status: corrected in the native polynomial grid (#31)
- Affected code: CASA 6.7.6.14 `BJonesPoly::selfSolveOne`.

The grid spacing calculation indexes the second selected frequency even
when a window contains only one selected channel. With one selected channel
in each of two windows and `combine='spw'`, the tested CASA binary collapses
the fit domain to the first frequency. The native grid uses the underlying
window spacing, keeps both physical frequencies, and stores occupied grid
points only. A degree-zero regression checks the fit against the normal
equations for the two selected channels.

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

## AW projection can use another antenna group's pointing

`BaselineType::findAntennaGroups` writes group means into an array indexed
by the antennas' positions in the sorted selection. `makeVBRow2BLGMap`
passes the two baseline group ordinals to
`PhaseGrad::ComputeFieldPointingGrad`, which indexes that antenna array
with them. A one-group selection works because every antenna holds the
same mean. With several groups, a group's ordinal can select an antenna
from another group.

The CASA 6.7.6-14 four-corner MT-MFS test on
`refim_heterogeneous_pointings.ms` selects even-even and odd-odd
baselines at two times, with `usepointing=True` and
`pointingoffsetsigdev=[20,20]`. In the first time's image the even
antennas point at (924,1124), but CASA sends (924,924) to
`PointingOffsets::gradPerPixel` for their baseline. Odd-antenna baselines
receive (924,1124). Both groups' y coordinates are likewise exchanged at
the second time, at x=1124. Casa-rs takes the group mean of each baseline's
actual antenna pair.

A local CASA debugger control substitutes only the correct per-baseline
pointing pixels at `gradPerPixel`. Using the original saved CASA
convolution functions, the principal dirty PSF goes from 1.33 NRMS
against casa-rs to 1.08e-7. Seventeen dirty products pass, with exact
masks and sums of weights. The near-zero odd Taylor weight still fails
its existing pointwise comparison. When both runs use the corrected
CASA dirty inputs and the saved standard-gridder PSFs, all 19 CLEAN
products agree to 5.02e-7 NRMS or better, with exact masks, ten iterations
and one major cycle. [Issue #1070](https://github.com/bglenden/casa-rs/issues/1070).

## AW Float tap products perturb cancelling Taylor sensitivities

`AWVisResampler::DataToGridImpl_p` multiplies the saved weight
convolution tap by a Float complex pointing phasor, then by a Float
imaging weight. Both products round before the double-complex grid
accumulation. `PhaseGrad::ComputeFieldPointingGrad` also stores the
product of the rounded axis phasors as Float complex. The higher Taylor
orders can expose this rounding where frequency contributions nearly
cancel, and the principal sensitivity can round across a sharp PB cut.

In CASA 6.7.6-14, the original one-group MT-MFS case on
`refim_heterogeneous_pointings.ms` has 40 Taylor moment matrices
outside PB support that fail the PSD bound. One pixel crosses the
original `abs(pblimit)=0.01` when compared with casa-rs. In the
four-corner case with CASA's baseline group assignment corrected, the
odd weight misses its existing pointwise limit by a factor of 88.
Both differences are already present in the grids before the FFT.

An independent replay using the saved CASA kernels, measured Taylor
frequencies and source Float arithmetic reproduces the three captured
grids to at most 5.8e-8 NRMS. A second replay retains the Taylor
coefficients and widens the pointing and weighted tap products to
double. Feeding only its weight grids back through CASA's FFT and
product pipeline clears both failures at the original frequency and
cutoff. Against this control, casa-rs agrees on all 18 dirty products
for each selection, with exact masks, sums of weights and controller
counts. Worst product NRMS is 2.25e-6. The odd weight passes the same
pointwise and PSD checks. The native operator retains its double
arithmetic. [Issue #1070](https://github.com/bglenden/casa-rs/issues/1070).

The same four-corner selection can move one PB cutoff pixel when both
imagers generate their own convolution functions. The 512-square CASA
kernel transforms use SIMD FFTW, while the native comparison used the
scalar build. The six imaging and six weight cells differ by at most
2.37e-7 NRMS. Their PBs at (662,1443) are 0.0099999797 and 0.0100000488
after controlling the tap rounding described above. Setting only CASA's
FFTW thread count to one reproduces its original kernels exactly.
Setting `FFTW_NO_SIMD` only for the 24 kernel transforms makes all
12 CASA kernel arrays equal to the independently generated native
arrays. With those CASA kernels and the widened sensitivity control,
all 18 products pass at the original cutoff, with exact masks and
controller counts and worst NRMS 2.53e-6. This is a rounding difference
between FFT plans. Native retains its strict cutoff and its configured
FFT backend.

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

## Zero observer radius creates spurious diurnal aberration

- Date noted: 2026-11-13
- Status: CASA defect; casa-rs uses the zero-radius limit (#1295).
- Affected code: casacore `MeasMath::applyTOPOtoHADEC` and
  `MVPosition::readjust`; CASA `FTMachine`'s default reference position.

The simulator's unset reference position is the geocentre. Its diurnal
aberration factor is zero, but `MVPosition::readjust(0)` scales the unit
correction direction by `1e-6`. The resulting spurious aberration changes
HADEC, horizon and terrestrial image prediction. The same C++ calculation at
an ITRF radius of one micrometre has negligible physical aberration and
removes the discontinuity. For the image-frame control's first baseline,
C++ gives path delays of -14.597005554443669 m at exactly zero radius and
-14.597027123114067 m at the one-micrometre limit. Direct C++ calls reproduce
this without the Python position-record conversion.

casa-rs retains zero aberration at zero radius. The horizon-frame oracle in
`tools/simulation/sm_image_layouts_oracle.py` prepares and predicts images in
the same limit frame; it records this correction in the fixture. Separate
nonzero-delay C++ UVWMachine controls at two visibility epochs check both the
rotation refresh and the retained construction-time phase centre. The
end-to-end controls keep the existing 1e-3 tolerance.

## GridFT's visibility phase uses a single-precision pi constant

- Date noted: 2026-11-13
- Status: CASA defect; casa-rs retains double-precision phase arithmetic (#1295).
- Affected code: `synthesis/fortran/fgridft.f`, `locuvw`.

`locuvw` declares `pi` double precision but initializes it with the
default-real literal `data pi/3.14159265358979323846/`. The literal is rounded
to single precision before conversion. Calling the shipped CASA 6.7.6.14
Fortran routine directly with a 100 m delay and 100 GHz frequency returns
`(-0.8458330035, -0.5334478021)`; double-precision pi gives
`(-0.8427074474, -0.5383717657)`. The returned phasor agrees with pi first
rounded to float32. Large absolute delays can therefore exceed the usual
DATA tolerance even when the C++ and Rust UVW and physical path delays agree.

casa-rs keeps the double-precision constant. The image-frame DATA controls
use matching observer frames and a small initial phase offset; an independent
C++ projection control also checks a nonzero delay. This avoids assigning
CASA's phase-constant error to the direction conversion.

## The simulator mosaic beam axis mislabels irregular frequencies

- Date noted: 2026-11-13
- Status: CASA defect; casa-rs evaluates beams at the actual frequencies (#1295).
- Affected code: `synthesis/TransformMachines/HetArrayConvFunc.cc`,
  `findConvFunction`.

`findUsefulChannels` preserves an irregular selected frequency vector, but
`findConvFunction` constructs the beam image's spectral coordinate with only
its first frequency and first spacing. For `[100, 150, 210, 300]` GHz the
beam planes are consequently evaluated at `[100, 150, 200, 250]` GHz. A
four-plane image with a central source and an offset source reproduces the
error even in J2000, independently of direction-frame conversion.

casa-rs builds each beam at its actual frequency. The oracle retains CASA's
raw DATA and obtains the 210 and 300 GHz DATA and CORRECTED_DATA columns from
independent constant-frequency CASA predictions of the same sky. The first
two channels, model column, unselected rows and quality columns retain their
ordinary CASA references. These controls agree with native prediction at
5.4e-7 NRMS in the J2000 case, without changing the 1e-3 threshold. Reproduce
a frequency control with `sm_image_layouts_oracle.py --case
frame_j2000_mosaic --constant-frequency 210000000000 --out OUT --fixture FILE`.

## MVC restoration can drop a newly generated higher-term residual mask

In CASA 6.7.6.14, seed standard, two-term MVC on field 0 of
`refim_oneshiftpoint.mosaic.ms` with 512 pixels per axis, 10 arcsec cells,
three channels and `pblimit=0.1`. Remove both saved Taylor residual
default masks, scale the residual planes independently, and set a 50 Jy
off-axis pixel in term zero. Resume with `calcpsf=False, calcres=False`,
five iterations and a 2 Jy threshold.

The first major cycle replaces both Taylor residual tables and their
masks in `PyMtmfsViaCubeSynthesisImager.cube2tt`. Tracing table identity
and mask support shows 62,955 valid pixels on both terms immediately
before `SDAlgorithmMSMFS::restore`. After restoration, term one's table
header has been replaced again and its default mask is absent. Term
zero retains all 62,955 valid pixels. The restored term one likewise
has no mask. Restoration is meant to copy each residual's mask to its
image and preserve the underlying residual. The observed mask loss
occurs during restoration after the Python helper has recreated tables
that the minor cycle also accesses.

casa-rs retains the freshly generated residual mask and copies its
support to the corresponding restored image. The masked and unmasked
initial convergence controls follow CASA's saved term-zero mask,
including all valid pixels when that initial mask is absent. The
additional both-unmasked probe agrees on the controller and numeric
products. Only the two term-one masks differ, by 199,189 pixels. Reinstating
just those masks in CASA after restoration gives all 24 products,
exact masks and a maximum normalized RMS of 8.309e-6.
See [#944](https://github.com/bglenden/casa-rs/issues/944) and
[#1414](https://github.com/bglenden/casa-rs/pull/1414).

## Simulator component-list copies lose the limb-darkening exponent

CASA 6.7.6.14 accepts a limb-darkened disk through a component record/table.
`ComponentList::readTable` applies `Optional_Parameters` to its shape, but
`SimpleComponentFTMachine::get` then copies each partition through a record.
`SkyCompRep::fromRecord` saves `optionalParameters` without applying them to the
shape. Prediction therefore uses the default exponent 0.05 even when the table
requests 0, 1, 2.5 or 8. This contradicts the persisted component model.

casa-rs retains the requested exponent, rounded to the shape's stored float32,
and evaluates `Γ(η+1) (2/r)^η Jη(r)`, with `η=1+n/2`. The simulation oracle
retains raw CASA DATA. Its corrected reference combines actual CASA point-source
DATA with the requested shape evaluated by CASA's bundled GSL, using independent
Cartesian tangent-plane geometry. That same GSL/geometry control first agrees
with the actual default-exponent CASA prediction at 3.7e-7 NRMS. Native CPU
W1/W3 predictions agree with these controls below 1e-6 NRMS, including polarized
spectra, selected replacement/increments and unchanged scratch/quality cells.
The uniform-disk and projected-sphere analytic limits are checked separately.

## Simulation numerical diagnostics fail before returning their calculations

- Date noted: 2026-11-14
- Status: CASA defects; casa-rs corrects these simulation diagnostics (#1303).
- Affected code: CASA 6.7.6.14 `casatasks/private/simutil.py`, `sfBeam1d` and `sensitivity`.

`sfBeam1d` passes the length-one arrays returned by `qa.getvalue` to Python
scalar operations. Under NumPy 2 this raises `TypeError: only 0-dimensional
arrays can be converted to Python scalars`. The reference control explicitly
extracts just the three scalar quantities (beam, cell and sampling); its
SciPy spheroid, convolutions and spline calculation remain unchanged. Native
widths agree with seven such controls to 1e-9 relatively. Units, invalid
quantity messages, zero support, support below -1 and absent sampling are
covered too. CASA's literal default support -1 has negative c; SciPy returns
a constant angular profile there, so native preserves that box-kernel result.

`sensitivity` without an antenna list leaves `padnames` undefined and raises
`UnboundLocalError`, even with a valid telescope, dish diameter and antenna
count. Native returns the identical-dish radiometer estimate at the requested
elevation: the existing Tsys baseline sigma divided by
`sqrt((time/integration) * nant*(nant-1)/2 * 2)` for two polarizations. It
uses the existing receiver and manual/ATM models without inventing antenna
positions. Nonfinite inputs and invalid Tsys are refused before the noise
model can neutralize a bad gain into a plausible estimate. Independent controls
use the previously validated radiometer
equations, CASA receiver efficiencies and CASA's ATM tool. Image noise needs
a configuration, since diameter/count alone do not define an image's UV coverage.

With a configuration, native follows the task's synthetic-track and middle
unflagged T-solution calculation through the existing simulator. Its accepted
UTC, per-row noise and physical-channel atmosphere corrections still apply.
Six ALMA/ACA/VLA references, including manual/atmospheric noise and the ALMA
high-frequency double-sideband case, agree within the ordinary 1e-3 tolerance.
An additional equivalent-unit/time/bandwidth case also agrees. With manual
noise and omitted `tau0`, CASA instead fails inside `setnoise` with a negative
Tsys and a Python `SystemError`; native requires an explicit manual opacity
and reports that cause. This does not choose a new scientific default.

`doimnoise=True` calls `imclean` without its `psfmode` argument, raising
`TypeError` for missing `stokes`. Inserting that argument exposes the removed
legacy `clean` function (`NameError`). Native carries out the declared small
128-square, natural-weighted Stokes-I Clark calculation with the existing
imaging application and returns `(theoretical_noise, image_rms)` in Jy. The
reference uses CASA `tclean` on exactly the same native-written noisy MS;
the native image never constructs its reference. The two 128x128x1x1 Jy/beam
images agree at 1.905e-7 NRMS, and their measured RMS agrees at about 1e-9
relatively. This completes the numerical diagnostic without completing the
general `imclean` wrapper or introducing a new task.

The raw errors, controlled calculations and reproduction commands are kept
with the simulation diagnostic evidence and
`tools/simulation/simutil_diagnostics_oracle.py`. These apply the standing
policy to correct CASA failures that contradict the calculation's evident
intent; ordinary successful configurations and scientific tolerances are retained.

## Public simutil ephemeris compares a quantity record with an integer

- Date noted: 2026-11-11
- Status: CASA 6.7.6.14 raises with its default duration; casa-rs accepts the quantity (#1304).
- Owner decision: Correct behavior that contradicts documented or evident intent under the standing AGENTS.md CASA deviations rule (owner #1054).
- Evidence: `tools/simulation/simutil_graphics_oracle.py`, `casa_simutil_graphics.json.gz` and `test_simutil_graphics.py`.

Without an `ms`, `simutil.ephemeris` tests `self.totaltime > 0` before
converting it to hours. The constructor's own default is a quantity record,
`{'value': 0.0, 'unit': 'h'}`, so Python 3 raises `TypeError: '>' not
supported between instances of 'dict' and 'int'`. A supplied quantity such
as `{'value': 40.0, 'unit': 's'}` fails the same way, after drawing only
part of the figure.

casa-rs checks the quantity's value and converts a positive duration to
hours before drawing its observation bar. The default zero duration draws
no bar. The fixture retains CASA's raw failures separately from successful
numeric-zero controls. The positive-duration regression checks the full
curve and transit against the independent CASA control, and the bar's
width against the analytic 40 seconds. This corrects the default call and
the intended quantity input without changing the elevation calculation.

## `tsdimaging` crashes after gridding an empty POINTING table

- Date noted: 2026-11-15
- Status: corrected in the native task (#938 part 2)
- Affected code: CASA 6.7.6.14 `task_tsdimaging.set_beam_size`

With the POINTING rows removed from the total-power oracle's MS, CASA grids
from the FIELD direction and writes both image products. Its subsequent
raster-sampling step raises `ValueError: not enough values to unpack
(expected 2, got 0)`, leaving the image without the normal weight mask.
The native task completes the FIELD fallback, writes the weight-median mask,
and leaves the restoring beam unset when there is no measurable raster.
`tsdimaging_pointing_oracle.py` retains the raw exception and raw image, then
calls CASA's unchanged `do_weight_mask` as a completion control. Native image
and weight agree with that control to 2.1e-7 normalized RMS.

## `tsdimaging`'s empty antenna selector can include cross-correlations

- Date noted: 2026-11-15
- Status: the accepted simulation core selects autocorrelations (#938)
- Affected code: CASA 6.7.6.14 `task_tsdimaging` data selection

CASA forwards an empty antenna selection to the imager without the `&&&`
autocorrelation restriction; a nonempty antenna receives that restriction.
Later beam estimation explicitly selects autocorrelations. The native
single-dish task consistently grids autocorrelations, including when the
antenna parameter is empty. A cross-correlation is not a total-power
spectrum. This follows the approved core's autocorrelations-only decision;
interferometric imaging remains the separate tclean operator.

## An overflowing FIELD ID can select field zero

CASA's `MSFieldGram.yy` converts decimal FIELD ID tokens using `atoi`.
On the two-field selection fixture, stock CASA 6.7.6.14 reads the 322
rows of FIELD_ID 0 for `field='4294967296'`, which exceeds the column's
signed 32-bit range. Native tclean rejects an overflowing ID before
looking it up in FIELD, so it cannot select an unrelated field.
[Issue #54](https://github.com/bglenden/casa-rs/issues/54) and
[the casacore FIELD grammar](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/ms/MSSel/MSFieldGram.yy).

## A reversed feed interval aborts MeasurementSet selection

With FEED_ID 0 and 2 present, CASA 6.7.6.14 `split(feed="2~0&&")` aborts
the Python process. The feed grammar builds an array with negative length
and the uncaught `ArrayShapeError` terminates the process.

The native transform checks the range before reading MAIN. It returns a
feed selector error and leaves the input and output paths untouched (#1614).

## Transform correlation arrays can disagree with their polarization metadata

On a MeasurementSet with correlations XX, XY, YX and YY, CASA 6.7.6.14
`split(correlation="YY,XX")` and `mstransform(correlation="YY,XX")` write
visibility and flag arrays in YY, XX order while POLARIZATION labels them
XX, YY. `getCorrMaps` sorts the metadata indices, and the visibility
iterator gets its slices from the parser's original order.

Selecting XX, XY and YY can leave CORR_TYPE and CORR_PRODUCT undefined in
an otherwise readable output. `fillPolTable` returns early for a partial
selection of more than two products, and the caller continues writing.

The native transform uses the original stored axis order for both metadata
and arrays and writes complete metadata for every selected subset (#1614).

## Transforming an MS can omit defined FLAG_CATEGORY arrays

CASA 6.7.6.14 `split` and `mstransform` leave FLAG_CATEGORY undefined for
an input with defined cells of shape (four correlations, eight channels,
two categories). The rank check in
`MSTransformManager::checkFillFlagCategory` compares the entire shape with
the scalar 3.

The native transform preserves defined category flags and slices their
correlation and channel axes with the other MAIN arrays (#1614).

## A bare FIELD escape can silently change the selection

Outside a quoted pattern or slash regex, the casacore FIELD scanner prints
the backslash and consumes the next non-newline byte without adding it to
the selector.
On the two-field selection fixture with names `Alpha.1` and `Alpha2`,
stock CASA 6.7.6.14 reads the same 322 rows for `\xAlpha.1` as it does
for `Alpha.1`. It likewise reads the same 736 rows for `\qAlpha2` as it
does for `Alpha2`. Stock tclean proceeds to image the first selection.

Native tclean explicitly rejects the invalid bare term before selecting any
rows. This keeps an unintended prefix from changing the user's selection.
[Issue #54](https://github.com/bglenden/casa-rs/issues/54) and
[the casacore FIELD scanner](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/ms/MSSel/MSFieldGram.ll#L63-L69).

## Roundoff in direction regridding can enlarge a CLEAN mask

On `refim_eptwochan.ms`, use a standard natural-weighted 32 × 32 Stokes I
MFS image with 8 arcsec cells and the binary mask
`box[[10pix,10pix],[21pix,21pix]]`. Shift only the saved mask's first
reference pixel from 16 to 16.5. The direction map is exactly
`(x, y) → (x + 0.5, y)`. Linear interpolation then gives 156 positive
pixels, at `x=9..21` and `y=10..21`.

Stock CASA 6.7.6.14 produces 172 positive pixels. Its default
decimation-10 direction mapping and linear interpolation give 16 samples
from 6.39e-14 to 2.03e-12 just outside that rectangle, on analytically
zero rows. `SDMaskHandler::fillMask` includes every positive sample.
Calling CASA's same linear image regrid with decimation 1 changes the
extra support and leaves 166 positives.

Native tclean uses the exact affine direction map for a shared tangent
plane. It samples signed mask values before taking positive support.
Both native solvers match CASA when CASA reads the analytically aligned
156-pixel support. The direct CASA result with the rounding-only pixels
leaves the Högbom products other than the mask unchanged within rounding.
It changes the two-iteration MEM model by 0.315 normalized RMS and the
restored image by 0.00389. The added pixels have zero analytical support and their locations follow
roundoff in the coordinate grid.

[Issue #823](https://github.com/bglenden/casa-rs/issues/823) and
[the casacore coordinate-grid interpolation](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/images/Images/ImageRegrid.tcc#L1392).

## Expanding a CLEAN mask can drop its direction and validity

For a continuum mask used on a cube, CASA's
`SDMaskHandler::expandMask` creates a temporary image with the output
direction coordinates, then copies the input mask's raw pixels into it.
Casacore's `Lattice::copyDataTo` leaves the input coordinates and default
pixel mask behind.

In the natural-weighted two-channel `refim_eptwochan.ms` cube, stock CASA
6.7.6.14 expands the half-pixel shifted continuum mask to 144 pixels per
channel. These are exactly the unshifted rectangle. When an aligned
input contains one default-invalid positive pixel, CASA keeps that pixel
on both channels.

Native tclean uses the saved direction grid and default validity before
expanding support. Its shifted result contains the 156 analytically
positive pixels on each channel, exact against CASA given the aligned
156-pixel support. It excludes the default-invalid pixel on both channels.

[Issue #823](https://github.com/bglenden/casa-rs/issues/823) and
[the casacore pixel-copy implementation](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/lattices/Lattices/Lattice.tcc#L290).

## A sky polygon just outside the image can mask its first column

On the natural-weighted 32 × 32 Stokes I MFS `refim_eptwochan.ms` fixture,
a saved J2000 polygon projects wholly left of the image. Its vertices in
pixel coordinates are `(-0.6, 2.2), (-0.2, 2.5), (-0.2, 9.7), (-0.5, 10.2)`.
No pixel centre lies inside. Stock CASA 6.7.6.14 nevertheless fills
column zero at rows 3 through 10 and performs five Högbom iterations.

`LCPolygon::truncateEnd` converts its fractional upper coordinate to an
integer toward zero. When the upper crossing lies between −1 and 0, it
becomes 0 and admits the first column. The near-integer branch also
converts a crossing near −1 toward zero after adding 0.1. Native
tclean floors both upper-bound branches before clipping. A wholly outside polygon therefore produces
an empty mask and stops with code 7, as stock CASA does when the polygon
is farther outside. Ordinary off-grid, partially clipped and
pixel-boundary polygons retain CASA's exact support.

The same clipping defect affects fractional CRTF pixel boxes. For
`box[[-0.6pix,5.2pix],[-0.2pix,10.7pix]]` and one whose upper x bound
is −0.999999, stock CASA paints column zero at rows 6 through 10.
Both native masks are empty and return stop code 7. The ordinary
fractional, near-integer and partly clipped controls retain CASA's support.

[Issue #823](https://github.com/bglenden/casa-rs/issues/823) and
[casacore's upper crossing bound](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/lattices/LRegions/LCPolygon.cc#L198-L208).

## PCO pixel to world returns NaN on the central meridian

- Date noted 2026-11-15
- Status corrected in casa-rs (#1101, #1494)
- Affected code WCSLIB 8.2.2 `pcox2s` in CASA 6.7.6.14

Run `tclean` on `refim_twochan.ms` with `projection="PCO"`, a 100 × 100
image, 8″ cells and phase centre `J2000 19:59:28.500 +40.43.37.50`. The dirty
peak is at pixel [50, 53]. CASA's `coordsys.toworld` returns NaN in both
sky coordinates there. Pixel [50, 50] and the off-meridian [50.001, 53]
have finite coordinates. CASA's `coordsys.topixel` places the known
sky direction, the reference longitude at the reference declination plus
24″, at [50, 53]. CASA shows this behaviour when reading either a CASA
or a casa-rs image with the same PCO record.

At zero intermediate x, WCSLIB initializes both residuals to zero and
divides one by their difference in the first weighted interval step.
The native inverse solves for the same sky with a finite latitude. On
the central meridian the projected latitude is the native latitude.
The frozen PCO checks verify both offset signs using CASA's finite
world-to-pixel direction and compare ordinary pixels in both directions.
The projection and parameters remain CASA-readable and image products
agree at the original tolerance. [WCSLIB 8.2.2 source](https://www.atnf.csiro.au/computing/software/wcs/wcslib-releases/wcslib-8.2.2.tar.bz2)


## Mosaic and W projection drop negative Taylor moments with empty cross-hands

In CASA 6.7.6.14, `MosaicFT::getImage` and `WProjectFT::getImage` check
`max(weights) == 0.0`
to decide whether all gridded weights are zero. The imaging weights start
nonnegative, but `MultiTermFTNew` multiplies by a signed spectral offset
for odd Taylor terms. Negative parallel-hand sums and zero sums on
unobserved cross-hands have a maximum of zero. This skips the FFT and
drops the whole dirty or PSF moment. The signed sum of weights still reaches disk, as does the separate
sensitivity image in a mosaic.

For `mtmfs_polarized.mosaic_IQUV` (`refim_mawproject.ms`, SPW 1,
two terms), stock CASA writes a zero `.psf.tt1` and `.residual.tt1`
despite a negative `.sumwt.tt1`. The same request in IV retains both
terms and gives a central PSF first moment of about −8.01e−8. The
narrowband principal spectral coefficient is poorly constrained. The
same behavior also occurs across SPWs 0, 1 and 2, where the IV run has
usable frequency coverage and the central PSF first moment is about
−0.0196. A reference frequency of 1.4 GHz makes the total first moment
positive on those three SPWs and the stock IQUV run retains it.

The same SPW 1 request with `gridder='wproject', wprojplanes=16` drops
the odd PSF and residual moments in IQUV and retains them in IV. The
native I and V Taylor products and controller agree with that stock IV
run. With four observed correlations, the W-projection IQUV control on
`refim_point_linRL.ms` agrees directly with stock CASA.

casa-rs keeps nonzero signed moments for both gridders, so
requesting additional empty Stokes planes cannot erase valid data. Its
I and V products agree with stock CASA's IV controls, and it leaves Q and
U data and models zero. The shared PSFs, sensitivities and sum-of-weights
tables still cover all four Stokes planes. [Imaging issue #1112](https://github.com/bglenden/casa-rs/issues/1112)


## Polarized AW projection publishes the first correlation sensitivity

CASA 6.7.6.14 computes the mean of the complex weight transforms across
polarizations in `AWProjectWBFT::makeWBSensitivityImage`. The accumulator
is a separate matrix. It never replaces the first plane of the input
lattice, so the subsequent loop takes the magnitude of the original
first plane and copies it into every output polarization.

With `refim_mawproject.ms` (RR and LL), `gridder='awproject'`,
`wbawp=True`, `conjbeams=True`, `psterm=False`, both PA steps at 360,
`imsize=512`, `cell='10arcsec'`, two Taylor terms, `reffreq='1.4GHz'`,
`stokes='IQUV'` and `pblimit=0.1`, this changes the common weight image
by about 4.4% NRMS and changes the valid PB support. The request uses the
phase centre and field selection of `test_widefield_wbaproj_mtmfs`. A native probe that
takes only the first correlation reproduces the stock products and
controller with the same cache. The same first-correlation behavior is
also verified on the four-correlation `refim_point_linRL.ms` fixture.

The native AW path combines the parallel-hand transforms for the common
IQUV sensitivity. Its Stokes I images, normalized weights and masks
agree with a separate stock Stokes I run, which combines both parallel
hands on one grid plane. The first-correlation rule for the published
Taylor sum of weights follows CASA. It makes the IQUV sum of weights
half the Stokes I sum for the equal-weight RR and LL case. Compare the shape
of higher weight terms after dividing by their published sums.
[Imaging issue #1112](https://github.com/bglenden/casa-rs/issues/1112)

## Linear mosaicking counts weights for masked restored pixels

In CASA 6.7.6.14, `LinearMosaic::addOnToImage` applies the restored image's
mask to the weighted signal and uses the primary beam's mask for the
coverage. With a constant 1.5 restored image and a primary beam of 0.5,
masking only the restored pixel gives zero signal and a coverage weight of
0.25. In an overlap, that weight dilutes the flux from a valid pointing.
With a 6 × 5 pointing aligned in a 10 × 6 output, CASA's copy path can
carry both values through a pixel masked in both inputs. At the masked
pixel in the same constant example it wrote 3 and 0.25.

casa-rs uses both masks for both contributions. A pixel masked in either
input contributes neither signal nor coverage, and valid pointings still
contribute there. [Imaging issue #1515](https://github.com/bglenden/casa-rs/issues/1515)

## `bandpass` suppresses its runtime exceptions

- Date noted: 2026-11-16
- CASA version: 6.7.6.14
- Affected code: `casatasks/private/task_bandpass.py`, `bandpass`

Calling `bandpass` with `field='missing-field'` on an otherwise valid MS
logs the C++ selection exception but returns a dictionary with an empty
`solve_tables` instead of raising it.
The task's `finally` block returns `mycb.returndict()`, suppressing any
exception from selection, solving or writing. The existing caltable survives
this selection failure. casa-rs preserves that product and raises its existing
execution error so a failed calibration is visible to a calling script.
`tools/casa-compat/calibration_lifecycle_oracle.py` exercises both outcomes.

## `gaincal` can append rows without storing their gain parameters

- Date noted: 2026-11-16
- CASA version: 6.7.6.14
- Affected code: `SolvableVisCal::storeNCT`, `TableCopy::copyRows`

Create a G table with ten rows, remove `CPARAM` with the table tool, then
run `gaincal(append=True)` on the same input. CASA succeeds and leaves twenty
rows without a `CPARAM` column: copying common columns silently discards the
new gain solutions. casa-rs refuses this incompatible append schema and
preserves the previous product. Losing scientific values is not a useful
compatibility behavior. `calibration_lifecycle_oracle.py` reproduces both
outcomes; `output_lifecycle` checks the native refusal and preservation.

## Appending a first gain or B table tries to open nonexistent spectral metadata

- Date noted: 2026-11-16
- CASA version: 6.7.6.14
- Affected code: `SolvableVisCal::discernAndSetSolnFrequencies`

With `append=True` and an absent caltable, the frequency precheck opens
`caltable/SPECTRAL_WINDOW` unconditionally and fails. This precedes
`storeNCT`, whose append logic checks whether the destination is readable
and otherwise writes a new table. casa-rs follows that latter intent:
append creates a first table, then requires compatible metadata for later
appends. A solve without successful solutions still creates nothing.
`calibration_lifecycle_oracle.py` records CASA's failure (suppressed by the
bandpass wrapper) and native creation on otherwise identical valid inputs.

## `table.summary(recurse=True)` use-after-free and `statistics` `NaN` ordering

In CASA 6.7.6.14, calling `tb.summary(recurse=True)` on a table with a `Table:`
subtable keyword passes a destroyed `String` buffer when recursing into subtable
paths in `TableProxy::printValueHolder`, aborting with a `UnicodeDecodeError` or
corrupted path. Calling `tb.statistics` on floating columns containing `NaN` also
violates strict weak ordering in casacore's `GenSort::kthLargest`, making `median`,
`medabsdevmed`, `quartile`, `min`, and `max` depend arbitrarily on input array index
positions.

casa-rs safely canonicalizes and deduplicates `Table:` keyword subtable paths in
`tb.summary(recurse=True)` without invalid memory access, and in `tb.statistics`
ignores `NaN` for `min`/`max` while returning `NaN` deterministically for moments
and order statistics on `NaN` inputs. [Table tool issue #1028](https://github.com/bglenden/casa-rs/issues/1028)

## Calibration append accepts NumPy booleans in validation but rejects them in C++

- Date noted: 2026-11-17
- CASA version: 6.7.6.14

`gaincal(..., append=numpy.bool_(True))` passes the generated `cBool`
validator, which explicitly accepts `bool` and `numpy.bool_`. The unchanged
scalar reaches `calibrater.setsolve`, whose SWIG bool conversion raises
`TypeError` instead of appending. The existing calibration table is unchanged.
The same call with builtin `True` succeeds.

casa-rs converts the valid NumPy scalar to a native boolean and appends the
same solutions as builtin `True`. This follows the validator's declared
domain. `tools/casa-compat/calibration_lifecycle_oracle.py` reproduces the
CASA error and compares the two native append products. Invalid boolean
arguments retain the native exception policy documented in
`docs/python/api.md`.

## `polfromgain` can leave an incomplete corrected gain table

CASA 6.7.6.14 copies the input gain table to the requested destination before
correcting it. With an unflagged nonfinite gain in a fitted antenna, the task
raises `Spurious fractional polarization!` and leaves the copied table behind.
Casa-rs stages the correction and publishes the result only after it succeeds.
The paired regression checks the exception and the native destination state.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polfromgain_oracle.py)

## Leakage application can correct the wrong correlations or abort on a malformed table

CASA 6.7.6.14 applies the linearized leakage correction to positional slots
in a four-product visibility vector. With `CORR_TYPE=[XX, YY, XY, YX]` and
consistently reordered MAIN cells, it modifies `YY` and leaves `YX` without
its correction. Casa-rs resolves the products from `CORR_TYPE` and applies
the correction and flag dependencies to the corresponding cells.

A `D Jones` table with one receptor in each `CPARAM` and `FLAG` cell causes
CASA to abort with `ArrayShapeError`. Casa-rs returns a catchable error with
the offending calibration row and shape.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibration_leakage_apply_oracle.py)

## `polcal` can change the measured cross-hands when referencing a singular leakage fit

CASA 6.7.6.14's `Dlls` solver calls `LSQFit::invert` with rank-deficient
factorization disabled and continues to solve if the factorization fails.
It then subtracts the reference antenna's first leakage term and adds its
conjugate to the second receptor. This offset can change the fitted visibilities
when the parallel-hand model amplitudes differ.

For the paired linear-feed `solint='int'` fixture, the CASA table gives a
cross-hand normalized RMS residual of 0.209 against the planted measurements.
Casa-rs resolves the rank deficiency with the requested first-receptor
reference constraint in the fit and gives a residual of 8.60e-7. Both fits
preserve the requested zero first receptor on the reference antenna.
CASA also shifts the numeric values of flagged cells when changing
reference antennas. Casa-rs writes zero to flagged cells. The paired
test compares flags and valid coefficients and applies both products.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polcal_leakage_solve_oracle.py)

## The last cross-hand model can suppress a `polcal` position angle solution

CASA 6.7.6.14's joint leakage and angle modes fit a complex source multiplier
across all usable measurements in an interval. `DJones::guessPar` uses it
only if the final inspected model correlation has nonzero amplitude. A
`Df+X` or `Dfgen+X` interval with polarized source information in seven
channels reports a zero angle when an eighth channel has zero cross-hand
model and comes last. Reversing the same data and model reports the fitted
angle again.

Casa-rs applies the phase from the constrained fit in both orders. An empty
fit retains the identity angle correction.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polcal_joint_leakage_oracle.py)
· [Full Jones oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polcal_general_leakage_oracle.py)

## A flagged channel shifts Stokes-I normalization to other channels

In CASA 6.7.6.14, `VisEquation::divideByStokesIModel` advances the visibility,
model and weight pointers only when a channel has unflagged parallel hands
and positive model intensity. A skipped channel shifts which samples the
later channels' normalization factors modify. In a circular-feed
`polcal(poltype='PosAng')` solve with channel-dependent flux and position
angle changing in time, flagging channel 0 changes the fitted position
angles on channels 1 and 2.

Casa-rs computes Stokes I from each channel's own model and scales only that
channel's data and weights. Changing the flag on channel 0 leaves the
solutions in the other channels unchanged in this fixture.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polcal_position_angle_oracle.py)

## Linear polarization fitting can report a solution without angle coverage

In CASA 6.7.6.14, a linear-feed `Xfparang+QU` solve with equatorial antenna
mounts and a constant source model can pass the count of time segments even
though every segment has the same feed parallactic angle. In this case the
least-squares fit can report zero Q and U and write unflagged channel phases
that depend on rounding in the cross-hand averages. The same constant-angle
case in the circular basis already raises a singular-fit error.

Casa-rs detects the rank deficiency of the linear source fit and raises a
catchable error before writing an unconstrained calibration table.

The separate `gaincal` `XY+QU` modes accept two preaveraged segments in
CASA, which can produce a numerical Q/U estimate even though the source
model has three unknowns. Casa-rs reports the same singular-fit error when
those two segments leave the source model underdetermined.

If only one usable segment survives in a gaincal solution channel, CASA can
write an arbitrary unflagged phase and use it in both channel ambiguity
tracking and the source fit. Casa-rs flags that channel and estimates the
source polarization and phase continuity from the constrained channels.

In the direct VI1 calibrater, default preaveraging can leave a single
measurement for the XY phase fit. CASA writes unflagged phases from the
underconstrained line fit, with zero Q and U. Casa-rs retains the table and
flags its unconstrained channels. In the VI2 amplitude-only circular-feed
case with constant cross-hand magnitudes, CASA's fitted phase depends on
roundoff in the complex division used to remove the input phase. Casa-rs
reports a neutral phase. The amplitude-only comparison with varying
magnitudes agrees with CASA. [Direct tool oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_xy_tool_oracle.py)

With broad angle coverage and a real polarized input, the shallow or steep
line fit can also receive an exactly constant imaginary or real component.
CASA's `Xfparang+QU` returns a phase 90 degrees away from the planted one
and near-zero Q and U for horizontal and vertical tracks in the paired
fixture. Casa-rs recovers the phase from the component with nonzero
variance.

In the circular basis, a single channel with both cross hands set to zero
can produce an unflagged NaN phase in CASA. The same NaN propagates into the
spectral window's fitted and returned Q and U. Casa-rs flags the
unconstrained channel and estimates Q and U from the remaining channels.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/polcal_parallactic_oracle.py)

## Direct position angle calibration aborts with an open MeasurementSet

CASA 6.7.6.14's `calibrater.posangcal([12.], tablein, tableout)` aborts with
an uncaught C++ string after opening a circular-feed MeasurementSet. The
failure occurs with valid G and D calibration tables on both visibility
iterators. An unattached tool returns `False`.

Casa-rs returns `False` while unattached and raises a catchable
`RuntimeError` while attached, before modifying or creating a calibration
table. The public `polcal(poltype='PosAng')` task is available for solving
and applying a position angle correction.
[Paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibration_polarization_workflow_oracle.py)

## Fitting through the legacy calibrater can crash without CORRECTED_DATA

In CASA 6.7.6.14, use `calibrater.setvi(old=True)`, open a MeasurementSet
without `CORRECTED_DATA` with `addcorr=False`, and select one populated field.
Calling `calibrater.modelfit([], niter=0)` exits with a segmentation fault.
The `uvmodelfit` task opens with `addcorr=True` and initializes the column.

The native fitter reports an error when the corrected samples are missing.
The task initializes the column before fitting, as CASA does (#1520).

## Fluxscale negative and underdetermined spectral-fit orders

CASA 6.7.6.14 `fluxscale` allocates coefficient arrays with `fitorder+1`
entries before checking a negative order. With three valid spectral windows,
`fitorder=-1` logs "Using fitorder=1", then writes outside the zero-width
array and segfaults (isolated Python oracle exit 139). casa-rs implements the
advertised fallback to order 1 and allocates the corresponding two entries.

With two valid windows and `fitorder>1`, `SolvableVisJones::fluxscale`
advertises order 1 but leaves `myfitorder` at its initializer, zero. It fits a
constant instead. With three windows and degree 3 or 5 it returns an
underdetermined solution, including negative covariance diagonals; one probe
returned a cubic coefficient of 1560 and a covariance diagonal of -2.95e20.
casa-rs reduces the fitted degree to the number of valid windows minus one,
warns, and pads the requested coefficient/error vectors with zeros. Valid,
determined fits retain the requested degree and CASA's covariance convention.
The existing singular order-one behavior is retained.

For exactly determined fits, casacore `LSQFit::solve` subtracts accumulated
moments (`sum(w*y*y) - x*b + x*(N*x-b)`) and divides by at least one when
estimating residual scatter. Roundoff can become a positive uncertainty
although the data determine every coefficient exactly. A five-window degree-4
probe returned coefficient 24697.456405169 and uncertainty 0.0038844, and a
flux-density error of 6.1e-7. casa-rs retains zero residual uncertainty when
there are no residual degrees of freedom, extending its existing two-window
linear-fit convention. The oracle separately bounds these CASA artifacts to
1e-3 of the corresponding coefficient/flux scale and checks native zeros;
determined coefficients, covariance, and overdetermined uncertainties retain
their direct numerical comparisons. Existing stricter order-one checks remain.

Source: `synthesis/MeasurementComponents/SolvableVisCal.cc`,
`SolvableVisJones::fluxscale`, coefficient allocation and `myfitorder` branches.
Reproduction: `tools/casa-compat/fluxscale_oracle.py` builds the three-window
input; the negative-order crash must be run in a separate Python process.
Regression: `casa-calibration`'s spectral-fit tests and captured CASA gain data.

## Explicit gencal wildcards can index outside the calibration array

In CASA 6.7.6.14, `gencal(vis, caltable, 'amp', antenna='*', parameter=2.0)`
passes `-1` as an antenna index and aborts in `ArrayBase`. With `spw='*'`
CASA similarly passes a negative window index and segfaults. A negated
antenna clause such as `A1;!A3` can pass `-3` and abort after processing A1.

For the specified gain, phase, delay and opacity types, casa-rs treats each whole-axis
`*` as that axis's empty selector, which broadcasts the value. It refuses a
negative antenna index before publishing the table, preserving any existing
product. The paired `calibration_gencal_specify_oracle.py` compares the wildcard
results to CASA's empty selectors and checks safe rejection of the negated
clause (#32).

The legacy `antposvla` path also aborts with an empty antenna selector when
logging an element of the empty antenna array. Casa-rs safely rotates the
first offset triple and broadcasts it to all antennas. Explicit antenna
selections retain CASA's repeated in-place rotations when the parameter
vector cycles. The paired
`tools/casa-compat/calibration_gencal_antpos_oracle.py` checks both forms (#32).

## A one-receptor SYSPOWER shape aborts switched-power calibration

On CASA 6.7.6.14, `gencal(caltype='rq')` with one receptor per SYSPOWER
array aborts in `ArrayBase::validateConformance`. Casa-rs validates each
input shape and raises an error before publishing the calibration table.
The switched-power oracle checks the isolated CASA abort and native refusal
on the same MeasurementSet (#1535).

CASA also uses the first scan from an empty selection when a SYSPOWER
timestamp falls between visibility scans. The reference case records scan 0.
Casa-rs records the calibration table's unknown scan value, `-1`, and keeps
the power sample and its original time.

## Opacity priors can index a nonexistent second receptor

CASA 6.7.6.14 constructs a scalar solution for `gencal(caltype='opac')`.
Selecting the second receptor with `pol='L'` or both with `pol='X,Y'`
terminates the Python process in the calibration array slicer. Casa-rs raises
a task error and preserves an existing table. The empty selector, `R` and
`X` select the scalar value.

Source: `synthesis/MeasurementComponents/SolvableVisCal.cc`,
`SolvableVisCal::specify`. The paired
`tools/casa-compat/calibration_gencal_opacity_oracle.py` runs the CASA probes
in isolated processes and checks native table preservation (#32).

## Legacy iterative leakage solving can terminate the Python process

CASA 6.7.6.14 accepts `calibrater.setsolve(type='D')` and `type='Dgen'`
with `setvi(old=True)`, then segfaults in `solve()` on the circular-feed
cross-hand fixture. The direct leakage oracle verifies that setup succeeds
before each isolated CASA crash. Casa-rs uses its native iterative solver
on the same selected dataset, and verifies the solutions, flags and formal
errors against CASA's working VI2 path. CASA's `DLLS` and `DfLLS` solvers
produce tables in both iterators.

## Fluxscale history can contain uninitialized object identifiers

`MSHistoryHandler::addMessage` writes every required HISTORY column except
`OBJECT_ID`. Repeated CASA 6.7.6.14 fluxscale calls return arbitrary integers
there (one run produced -1045601744 and 32549); freshly allocated storage can
instead read as zero. casa-rs initializes this required field to zero. The
oracle compares all other history content, checks the native zero, and treats
the current timestamp and implementation version as process-specific metadata.

CASA 6.7.6.14 also accepts `refspwmap=[]` at the Python boundary but indexes
element zero unconditionally in `SolvableVisJones::fluxscale`, causing a
segfault. Native empty mapping means identity, as `refspwmap=[-1]` does.

## Fluxscale listfile can overwrite scientific table files

CASA 6.7.6.14 opens `listfile` with `ofstream` truncation without checking
whether it names a file inside the input MS, input caltable or output product.
Such a path can overwrite a scientific table file. Stream failures are also
unchecked, so the task can report success without producing the listing.
casa-rs refuses overlapping listing/product paths before mutation and reports
listing write errors. This preserves data and implements the requested file
output rather than silently losing it. Source: `SolvableVisJones::fluxscale`,
the `oListFile` stream blocks. The direct calibrater writes the listing before
publishing or appending to the caltable, so retrying after a listing error
does not duplicate rows. The direct oracle checks the pinned success with a
directory as the listing path and native preservation of an existing table.

## `tablerow.__getitem__` process abort in CASA 6.7.6.14

In CASA 6.7.6.14, indexing a `tablerow` accessor via `r[rownr]` (including valid
and out-of-range row indices, e.g. `r[0]` or `r[10]`) terminates Python with
segmentation fault (signal 11, exit code 139) inside the SWIG `casac::tablerow`
wrapper (`__getitem__`), even though `r.get(rownr)` and `len(r)` succeed.

casa-rs implements `tablerow.__getitem__` safely under the GIL-detached Rust
table accessor, supporting negative indices, slices, and sequences of row
indices, and raising `IndexError` cleanly on out-of-range indices.
[Table tool issue #1028](https://github.com/bglenden/casa-rs/issues/1028)

## `table.fromASDM` can report success after discarding XML values

In CASA 6.7.6.14, call `table().fromASDM("out.table", "Scan.xml")` with
`<ScanTable><row><scanNumber>12</scanNumber></row></ScanTable>`. The
table contains one row but its `scanNumber` is 0. The legacy SAX code
only enables its value callback for fields listed in a working directory
file named `taglist.txt`; even with the tag in that file, this build
did not store the value. With a nonexistent XML file CASA returns
`True` and creates no table.

casa-rs uses the same ordered column descriptions and converts the XML
values directly. It reports invalid or unreadable input before replacing
the destination. The ASDM XML converter reads one table at a time; it
does not import ASDM datasets or their binary data. See #1028.

## FITS `TNULL` cards can disappear from `table.fromfits` output

CASA 6.7.6.14 preserved the integer -32768 in a FITS `1J` column but
did not add a `NULL` column keyword for its `TNULL2 = -32768` card.
`FITSTabular::nullsFromHDU` looks up each indexed card with a
zero-based column index. casa-rs preserves both the unscaled integer
cell and the matching null marker so readers can interpret it. See #1028.


## The Python callibrary reader executes calibration-library file contents

CASA 6.7.6.14 builds and executes a Python statement from each record line in
`casatasks.private.callibrary.callibrary.read`. A file can run arbitrary
Python in the calling process. The casa-rs helper accepts the historical
record literals and rejects executable expressions. It clears any partial
records and reports the line using the malformed-library error path.

## Callibrary joint scan maps mark other observations unavailable

- Date noted: 2026-11-17
- Status: corrected under the standing evident-intent rule (#1518)
- Affected code: CASA 6.7.6.14 `CLPatchPanel` MS constructor, missing-scan catch.

With scan 1 in observation 0 and scan 2 in observation 1 in both the MS and
G table, `obsmap='self' scanmap='self'` skips all calibration. The constructor
tries each requested scan within each calibration observation. When a scan
is absent in the current observation, it calls `recordBadMSIndices(reqMSobs,
theseMSscan, ...)`, marking that scan bad in every requested observation,
including the observation where the scan exists. With an existing corrected
column, CASA leaves its values and weights unchanged; ordinary calflag adds
no flags. This contradicts the explicitly independent observation/scan maps.

casa-rs checks availability within the actual joint mapped context and
calibrates its valid buffers. The public task oracle retains the raw CASA
result and compares native output to an independent CASA control with two
explicit library slices, `obs='0' scan='1'` and `obs='1' scan='2'`, retaining
both self maps and the identical input MS/calibration table. That control
matches native corrected data, flags and weight/sigma columns. Missing joint
contexts still use the ordinary unavailable-calibration rules. No tolerance
is relaxed, and this correction is separate from same-input task matches.

## Gaincal can crash while opening a tracked virtual image

On Linux, CASA 6.7.6.14 saved a virtual GridFT image with
`tclean(phasecenter='TRACKFIELD', savemodel='virtual')` on an MS with an
attached Uranus ephemeris. Calling `gaincal` on that MS terminated Python
with signal 11. CASA wrote a nonconstant prediction to `MODEL_DATA` when
`savemodel='modelcolumn'` used the same image settings and visibilities.

casa-rs reads the saved source, tracking epoch and image direction and
evaluates the tracked prediction in the visibility buffer. The
`gaincal_oracle.py --case ephemeris` fixture compares native gains and
downstream application from the virtual image to CASA gains and
application from its scratch prediction. It also covers a tracked mosaic,
an external ephemeris table and moving-field parallactic angles. See #30.

## Empty or ragged nested sequences in quantity records crash or corrupt values

- Date noted: 2026-11-18
- Status: CASA crashes or truncates/zero-pads data; casa-rs refuses the inputs (#1170).
- Affected code: `casatools` Python-to-`Variant` / `QuantumHolder` record conversion.

In CASA 6.7.6.14, `qa.quantity({'value': [[]], 'unit': 'm'})` terminates Python
with a segmentation fault (exit code 139). For ragged nested floating-point
lists, CASA silently discards trailing elements in longer rows (`[[1.0, 2.0],
[3.0]]` becomes `array([[1.], [3.]])`) or zero-pads shorter rows (`[[1.0], [2.0,
3.0]]` becomes `array([[1., 0.], [2., 3.]])`).

casa-rs checks nesting depth and row shapes in `crates/casars-python/src/convert.rs`
and rejects empty or ragged nested Python sequences with CASA's catchable
`Illegal Quantum record in QuantumHolder::fromRecord` `RuntimeError` instead of
crashing or corrupting values (#1252, #1170).


## Component record imports announce Stokes but store an unknown polarization

CASA 6.7.6.14 `cl.add` and `cl.fromrecord` accept a flux record with four
values and no `polarisation` or `error` field. `FluxRep::fromRecord` logs
"Using the default of Stokes", then overwrites that default with its local
`UNKNOWN_POLARISATION` enumerator. `getcomponent` consequently reports
`polarisation='Unknown'`. A real-valued error vector instead makes the import
fail because the local enumerator is still unknown.

casa-rs retains the announced Stokes basis for accepted records, preserving
all four flux values. It preserves CASA's rejection of the real error-vector
form. This avoids storing an unusable polarization label or losing the
component's numerical values. The correction follows the standing owner
rule in `AGENTS.md` for behavior that contradicts CASA's evident intent;
there is no new owner exclusion.

Evidence: `tools/simulation/componentlist_record_spectra_oracle.py`,
`casa_componentlist_record_spectra.json` (`missing_pol_no_error`), and
`test_componentlist_records.py` (#1252).

### Gaussian component record imports abort on invalid axes or position angles

CASA 6.7.6-14 `cl.add(record)` terminates the process with SIGABRT when a
Gaussian record has zero or negative axes, a minor axis longer than its major
axis, or a position angle outside ±360 degrees. `GaussianShape::fromRecord`
constructs the shape before rejecting these values, and its constructor's
`AlwaysAssert(ok())` escapes the tool boundary.

casa-rs rejects the invalid component with a catchable `RuntimeError` before
appending it. Valid components retain CASA's geometry and position-angle
normalization. This follows the standing rule to correct CASA crashes; callers
retain their existing list and Python session. The isolated CASA subprocess
receipts are in
`crates/casars-python/python/tests/data/casa_componentlist_record_validation.json`,
regenerated by `tools/simulation/componentlist_record_validation_oracle.py`.
## Duplicated `imstat` axes crash CASA

- Date noted: 2026-11-18
- Status: CASA aborts; casa-rs refuses the request (#977).
- Affected code: `casatools` image statistics, `ImageStatsCalculator::statistics`.

In CASA 6.7.6.14, `imstat(imagename, axes=[0, 0])` aborts the Python process
(SIGABRT, exit 134) inside the statistics calculation; the same happens for any
duplicated axis. An out-of-range or negative axis is refused with the catchable
`Invalid cursor axes: [4].` instead, so the crash on duplicates is an
uncontrolled path, not a policy.

casa-rs refuses a duplicated axis with `Invalid cursor axes: [0, 0]: an axis
is named twice; CASA 6.7.6.14 aborts on it` (crates/casa-images/src/analysis.rs,
`parse_axes`), keeping every other axes form CASA accepts, including the
minimum-forms `-1` for all axes (#977).

## Replacing one Multi-MS model can remove another partition's Fourier grid

In CASA 6.7.6.14, split an MS with `partition(separationaxis='baseline', numsubms=2)`,
then write an image with `ft(mms, field='0', model=...)`. The two physical
SOURCE records can name the same Fourier image. Replacing the model on the
first partition removes that image. A subsequent `uvsub` on the other
partition fails to open the image its record names.

casa-rs gives independent SOURCE or MAIN records their own Fourier grids.
Replacing one record leaves the other partition's grid and recipe
readable in CASA and casa-rs. Replacement also preserves retired grids
in SOURCE. SOURCE does not identify every MAIN that can still name an
older grid, including a MAIN with stale metadata after a failed write.
A replacement can remove a retired grid in its own MAIN directory after
checking the current model records. The standalone
`calibration_ft_oracle.py` fixture exercises the replacement sequence.

## TaQL derived geometry treats an ephemeris field offset as a direction

CASA 6.7.6.14 `msmetadata.phasecenter(field, epoch)` follows the attached
body and applies the field offset. The TaQL `mscal.hadec1()` and
`mscal.azel1()` functions use the stored `PHASE_DIR` directly for the same
field. For the Uranus fixture, `mscal.hadec1()` reports a declination of
0.00190 rad while the attached phase centre is near 0.20677 rad.

The calibration engine and diagnostics read the attached direction at the
requested epoch. The `calibration_ephemeris_geometry_oracle.py` fixture obtains
the per-epoch phase centres from CASA and evaluates CASA geometry on fixed
snapshots of those directions (#989).

## Rejected Tsys weight initialization can leave empty spectra

CASA 6.7.6.14 can add `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM` to a
MeasurementSet before rejecting `initweights(wtmode='tsys', dowtsp=True)`
with an invalid `gainfield` or interpolation setting. On that failure the
new columns have undefined cells and the original weights are unchanged.

casa-rs validates the setup before it creates either column, so a rejected
call leaves the original spectrum schema available for retry. The paired
`calibration_initweights_tsys_oracle.py` cases cover both setup errors
and the unchanged weight, sigma and flag values (#1524).

The public `calibrater.initweights('ones', spwmap=None)` call can also set
every weight to one before raising `SystemError`. casa-rs rejects the
malformed window map before updating any columns. The direct-tool oracle
checks both outcomes (#1524).

## `immath(mode="pola")` crashes with `UnboundLocalError` on unitless `polithresh`

- Date noted: 2026-11-18
- Status: CASA crashes with an uncaught Python `UnboundLocalError`; casa-rs accepts unitless thresholds in image brightness units (#977).
- Affected code: `casatasks/private/task_immath.py`, `_immath_new_pola`.

In CASA 6.7.6.14, when `mode="pola"` and `polithresh` is given without a unit
(e.g. `polithresh="0.1"`), `_immath_new_pola` skips creating the temporary
linear-polarization image `lpol` because `if (myqa.getunit(polithresh) != ""):`
wraps both unit conversion and `mypo.linpolint(..., outfile=lpol)`, yet the
later `if (polithresh):` block unconditionally evaluates `lpol` and raises
`UnboundLocalError: cannot access local variable 'lpol'`.

casa-rs parses `polithresh` as an optional-unit quantity (`crates/casa-images/src/analysis/immath/pol.rs`),
treats a unitless `polithresh` as already expressed in the input image's
brightness unit, and thresholds linear polarized intensity normally (#977).

## `immath` polarization debiasing ignores non-conforming `sigma` units

- Date noted: 2026-11-18
- Status: CASA silently corrupts/masks output; casa-rs rejects non-conforming or missing units when the image carries a brightness unit (#977).
- Affected code: `casatools/casacore` quantity conversion in `task_immath.py`, `_immath_new_poli`.

In CASA 6.7.6.14, `_immath_new_poli` converts a positive `sigma` via
`myqa.convert(qsigma, iunit)` and reads `.getvalue()` without verifying that
`sigma`'s unit conforms to the input image's `bunit` (for example passing
`sigma="1K"` or unitless `sigma="10"` on a `"Jy/beam"` image). On mismatch
`qa.convert` leaves the value/unit invalid so downstream `imagepol` debiasing
silently over-thresholds and masks the entire output plane.

casa-rs validates unit conformance via `get_conformant_value` when `sigma > 0`
(`crates/casa-images/src/analysis/immath/pol.rs`) and refuses non-conforming or
unitless `sigma` against non-empty image `bunit` with a clear error (#977).

## Median fringe smoothing wraps delay values as phase

In CASA 6.7.6.14, `smoothcal(smoothtype='median')` on a `Fringe Jones`
calibration table applies phase wrapping to every FPARAM component. The
condition `ipar == 0 || 4` in `FringeJones::smooth` is always true. A
valid 8 ns delay therefore became about 1.717 ns in a window with only
that sample. The same branch also skips the median filter for delays and
dispersive delays.

casa-rs wraps only the two phase parameters and applies the numerical
median to the delay, rate, and dispersive delay parameters. The use of
fringe rates for predicting phase wraps and the phase outputs follow
CASA (#1525).

## Re-referencing a delay table crashes CASA

In CASA 6.7.6.14, `rerefant(refantmode='strict', refant='A0')`
terminates the process when the input is a `K Jones` delay table.
`SolvableVisJones::applyRefAnt` reads CPARAM before dispatching on the
table type. Delay tables use FPARAM, and only Fringe overrides the
reference calculation for real-valued parameters.

casa-rs raises a task error for unsupported table types before publishing
a result. The paired oracle runs this CASA check in a child process (#1525).

## `statwt` creates spectra then fails on an explicitly selected subset

CASA 6.7.6.14 initializes a required `WEIGHT_SPECTRUM` or `SIGMA_SPECTRUM`
column for the original MS when `chanbin` requests channel bins. When the
selected table predates the new column, CASA then raises a `RuntimeError`
and asks the caller to rerun the selection. The first call writes the
initial spectrum across the MS and leaves the original flags and scalar
columns intact.

casa-rs initializes the physical MAIN tables and subsequently applies
statistical weights to the requested rows in one call. Unselected rows
retain the spectrum obtained by expanding their original scalar values.
The paired fixture compares this result with CASA's documented two-call
sequence and checks unselected rows (#1524).

## Column statistics change when the same NaN moves between rows

CASA 6.7.6.14 derives `table.statistics` and `calstat` quantiles from
an ordering that includes NaN. The rows `[0, 3, 1, NaN, 4, 8, 9]`
gave a finite interquartile range of 7 and a NaN median. Moving that
NaN to the start gave an interquartile range of 8, a median of 3 and
NaN for both bounds. The sum and variance were NaN in each case.

casa-rs returns deterministic bounds from the non-NaN samples, counts
all samples and returns NaN for the moments and quantiles when a NaN
makes those quantities undefined. This preserves the existing native
table tool behavior in the public calibration task (#1526).

## Accumulating a bandpass updates a fraction of the Jones cells

CASA 6.7.6.14's `calibrater.accumulate` is available with
`setvi(old=True)`. In the paired eight-channel B Jones fixture it
changed 20 of 160 valid complex cells. At each of two solution times,
only the first five channels of antenna 0 received the Jones product.
The other valid factors remained as they were.

casa-rs multiplies the per-receptor, per-channel factors for every
antenna. The analytic self-accumulation check squares all 160 valid
factors and checks flags separately (#1525).

## Accumulation depends on the physical order of overlapping field times

In CASA 6.7.6.14, the old accumulation method passes the incremental
table's time column to `CTTimeInterp1`. The interpolator uses binary
search on those times. Physically grouping rows by field can leave that
column out of chronological order when time ranges overlap.

CASA changed valid gains when the paired fixture was sorted by time.
casa-rs orders its time series before interpolation and matches the
time-ordered CASA control. It preserves the prior numerical payload
at flagged cells. In a selected gain-field comparison, CASA replaced
one flagged payload with complex unity. The flags and valid values
matched (#1525).

## Selecting a field in simultaneous calibration rows modifies other fields

The old CASA 6.7.6.14 accumulation method groups rows by spectral
window and time. It uses the first row's FIELD_ID to decide whether
to modify the entire group. In a fixture that divides antenna
solutions at the same timestamp between two fields, selecting FLUX
also changes PHASE rows. Selecting PHASE skips all the rows.

casa-rs applies the correction within each selected field. The paired
fixture checks the product on every eligible row and checks the
unselected rows byte for byte (#1525).

## Accumulation repairs a legacy input even when a separate output is requested

CASA 6.7.6.14's old accumulation path upgrades a pre-4.1 cumulative
table when it opens the source. The paired input lost its original schema:
CASA added OBSERVATION_ID and OBSERVATION before publishing the requested
separate product.

casa-rs applies the same legacy upgrade inside the output snapshot.
The result matches the upgraded CASA product and a separately named original
retains its legacy schema (#1525).

## Accumulation can terminate during contextual time interpolation

With the old CASA 6.7.6.14 visibility iterator, accumulation terminated
the process on the test fixtures for `spline` and `linearperscan`.
`linearperobs` completed when there was one observation and terminated
on the two-observation probe.

casa-rs uses the calibration application's time interpolator with the
cumulative rows' scan and observation metadata. In local comparisons,
the per-scan output matched two independent single-field CASA controls.
An independent SciPy natural spline matched 52 valid complex factors
in the spline comparison (#1525).

The public `calibrater.accumulate(t=None)` call can leave a Python
conversion error pending and terminate CASA 6.7.6.14. casa-rs raises
`TypeError` before touching the input or output table (#1525).

## Image record restore can crash or damage an existing output

CASA 6.7.6.14 segfaults in `ia.fromrecord` if an otherwise valid image
record has an empty outer `coordsys` record. A native restore of a record
with a two-axis coordinate system and `shape=[2, 3, 2]` throws
`Failed AlwaysAssert setCoordinateInfo` after replacing the output with
an unreadable directory. Casa-rs reports both errors in Python and
preserves the prior native image and its history (#1625).

## Direct image methods and gzip-compressed FITS

CASA 6.7.6.14 reads gzip-compressed FITS through `image.fromfits` and
`image.newimagefromfits`. The same file raises an unable-to-open error in
`open`, `newimage`, `newimagefromfile`, `fromimage`, and
`newimagefromimage`.

casa-rs also accepts these files through the direct FITS attachment and copy
methods. They preserve the FITS pixels, blank mask, coordinates, unit, and
history through CFITSIO. The explicit import methods agree with CASA.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

## A FITS attachment can report a different name after an environment change

In CASA 6.7.6.14, open `$IMAGE_DIR/source.fits` and change `IMAGE_DIR`
to another directory containing a different `source.fits`. The attached
image continues reading from the first file. `image.name()` reports the
second path. casa-rs keeps the attachment name and pixel reads tied to
the resolved input from the original open. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## Named image creation can overwrite a lexical destination without permission

With `left/alias` pointing to `right/nested`, create `left/product.im`
and leave `right/product.im` absent. In CASA 6.7.6.14,
`image.fromshape("left/alias/../product.im", shape=[2, 3])` can replace
`left/product.im` with the default `overwrite=False`. CASA checks the
path through the symlink for a collision, then creates the image at
the lexical path. casa-rs checks both paths and preserves the existing
product unless overwrite is enabled. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## An unbraced environment variable at the end of a path can duplicate the directory

With `IMAGE_FILE=source.fits`, CASA 6.7.6.14 can open `/dir/${IMAGE_FILE}`
while `/dir/$IMAGE_FILE` fails with a missing-file error. Casacore's path
expansion can repeat the preceding directory components when it finds the
unbraced variable in a later component. casa-rs opens the same `source.fits`
with either spelling. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## A bracketed FITS attachment can read a different file from the supplied path

Place distinct valid FITS images at `source.fits` and `source.fits[0]`.
In CASA 6.7.6.14, `image.open("source.fits[0]")` requires the bracketed
file to exist, then reports and reads `source.fits`. Both explicit import
methods read the literal bracketed file. casa-rs consistently treats
brackets as part of the filename and uses `whichhdu` for explicit HDU
selection. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## FITS import can abort on an empty primary HDU

CASA 6.7.6.14 aborts in `ia.fromfits(infile=path, whichhdu=0)` when the FITS
primary HDU has `NAXIS=0` and a later extension contains the image. The same
file imports successfully when `whichhdu=-1` selects the first image or
`whichhdu=1` selects its extension. casa-rs returns `RuntimeError` for the
empty HDU and accepts both ways to select the extension. See
[#1625](https://github.com/bglenden/casa-rs/issues/1625).

## `importfits` can remove an existing image before import succeeds

CASA 6.7.6.14 removes `imagename` as soon as it sees `overwrite=True`. A
subsequent validation error, such as a beam whose major axis is smaller
than its minor axis, leaves the old image gone. A beam position angle
given in time units can fail after publishing the replacement image.
The task and both explicit image methods (`fromfits` and `newimagefromfits`)
also delete the output path early when it contains the input FITS file.
casa-rs stages the native image and exchanges it only after the new
product is complete. It refuses output paths containing the FITS input
and nonempty directories that are not native images (#977, #1625).

## `exportfits` drops characters when wrapping long `HISTORY` cards

- Date noted: 2026-11-19
- Status: CASA silently loses history text; casa-rs wraps losslessly (#977).
- Affected code: casacore `ImageFITSConverter` FITS `HISTORY` card continuation writer.

In CASA 6.7.6.14, `exportfits(..., history=True)` wraps logtable lines across
80-byte FITS `HISTORY` cards by placing 72 characters on the first card and
`>` followed by 71 characters on continuation cards, but skips character index
143 on card 3 and subsequent continuation cards when the wrapped line spans
more than 143 characters. On `cube_i.im` (`crates/casa-images/tests/data/exportfits/cube_i.im`),
CASA drops `'y'` from `csys={...}` in the 202-character `ia.fromshape(...)`
message and drops the closing `')'` of the 144-character
`ia.setrestoringbeam(...)` message (emitting 47 `HISTORY` cards instead of 48);
on a synthetic 160-digit `"0123..."` history entry, CASA drops `'3'` at index
143 between `>...012` and `>456...`.

casa-rs (`crates/casa-images/src/analysis/exportfits.rs`, `wrap_fits_history_text`)
splits at 72 characters on the first card and `>` plus 71 characters on each
continuation card without skipping characters, emitting all 48 cards on
`cube_i.im` so both casa-rs and CASA 6.7.6.14 `ia.history(list=False)` read back
all 17 original history entries intact (#977).

## Duplicate `immoments` requests can leave a partial set of images

CASA 6.7.6.14 writes the first output directory for a repeated moment
number before the second occurrence reports that the same directory
already exists. For example, `moments=[0,0]` leaves the `.integrated`
directory after a failed task. casa-rs checks the destination names
before writing and refuses the duplicate without publishing a product
(#977).

## imsubimage aborts for an out-of-range keepaxes index

On a four-axis image, CASA 6.7.6.14 aborts the process with
`itsAxes(i) >= naxes` for `imsubimage(dropdeg=True, keepaxes=[4])`.
The check is in casacore's `AxesSpecifier::apply`. casa-rs returns an
out-of-range error before creating or replacing the output image. With
`dropdeg=False`, CASA and casa-rs ignore `keepaxes`.

The same abort occurs with the image tool's `ia.subimage(dropdeg=True,
keepaxes=[5])` on a three-axis image. The shared casa-rs selection returns a
Python `RuntimeError` and leaves any existing output intact.

## An image subimage can replace its source history table

CASA 6.7.6.14 can replace the source image's `logtable` when that path is
passed to `ia.subimage(overwrite=True)`. The source history then fails to
read because the table lacks the `TIME` column. If the history table is
already open, CASA reports that it is in the table cache. Casa-rs rejects
overwriting existing entries inside the source image before staging the
cutout. The same replacement through `ia.fromimage` or
`ia.newimagefromimage` succeeds in CASA and leaves the source history
unreadable. Casa-rs protects the source before staging either copy.

## Paged image concatenation can remove a source image or mask

CASA 6.7.6.14 accepts `imageconcat(overwrite=True)` with an existing output
directory that contains an input. It removes the nested image before failing
to read its pixels. Replacing an input's mask subtable as the output can also
succeed, leaving the input mask unreadable because the new table has no
`PagedArray` column. Casa-rs raises a Python error before either replacement.
A new output directory inside a source image is supported when no existing
subtable is replaced.

## imsubimage permutes coordinate labels after dropping transposed axes

In CASA 6.7.6.14, take a cube in RA, Dec, Stokes, frequency order and
apply `imtrans(order="3012")`. Then run
`imsubimage(box="1,2,4,5", chans="2", stokes="Q", dropdeg=True, keepaxes=[0])`.
The resulting shape is `[1, 4, 4]`, with pixels in frequency, RA, Dec order.
CASA labels the axes Dec, frequency, RA. In
`CoordinateUtil::dropRemovedAxes`, the permutation passed to
`CoordinateSystem::transpose` maps from creation order to pixel order,
while `transpose` expects the inverse. casa-rs keeps the coordinates on
their original pixel axes as it removes Stokes.

## plotbandpass passes a fractional platforming channel to range

CASA 6.7.6.14 raises `TypeError` while checking a wide bandpass for
platforming. The check computes the two interior channel offsets with
floating-point division, then passes those offsets to Python `range()`.
The paired 64-channel fixture enters this branch.

casa-rs evaluates the check at integer channel indices. In the fixture,
a step at channel 32 produces a platforming report at the 10 percent
threshold and a clean result at the 100 percent threshold (#1526).

## PlotMS disabling selection calls the selection setter with a missing argument

The installed CASA PlotMS wrapper raises `Exception("updateImmediately must
be a boolean")` for `selectdata=False`. The wrapper's no-selection call omits
a selector argument before the update flag. casa-rs applies the requested
unfiltered selection and leaves the caller's selector expressions inactive
(#1526).

## plotbandpass references an undefined name when WEATHER is absent

An atmospheric overlay on the paired bandpass with no associated WEATHER
table raises `NameError` in CASA 6.7.6.14 when it tries to read
`needToClose_mymsmd`. casa-rs evaluates the native atmospheric profile
with the station defaults and plots the original calibration spectra
with the resulting curve (#1526).


## Listing an incomplete antenna integration can terminate CASA

CASA 6.7.6.14's `listcal` reshapes each spectral window's solution
rows into complete antenna integrations. Removing the last antenna
row from an otherwise regular G Jones table terminated the paired
CASA subprocess after opening the requested listing file.

casa-rs validates the antenna blocks before publishing a listing. The
same input raises a recoverable error and leaves the destination absent.
Complete integrations retain their original order, even when two have
the same time and field (#1526).

## Beam removal can delete metadata before rejecting a record

In CASA 6.7.6.14, `ia.setrestoringbeam(remove=True, beam={'junk': 2})`
on a native image with a restoring beam raises `RuntimeError` because the
record has no `major` field. Reopening the image shows the beam was already
removed. The call validates the record while preparing history after deleting
the beam, even when history is disabled.

casa-rs validates the supplied record before it saves any changes, so a
rejected call preserves the restoring beam and history. See
[#1625](https://github.com/bglenden/casa-rs/issues/1625).

## Naming a removed coordinate axis can terminate CASA

CASA 6.7.6.14 can terminate when `coordsys.setnames` is called with
`type="linear"` and two names for a linear coordinate whose world and
pixel maps are `[0, -2]`. casa-rs raises `RuntimeError` before applying
either name. Passing the one visible axis name without a coordinate type
renames that axis and preserves the stored removed-axis name (#1626).

## Time averaging LAG_DATA and mixed FLOAT_DATA loses visibility information

CASA 6.7.6.14 leaves LAG_DATA undefined while time averaging, and a LAG_DATA-only
product can also have undefined FLAG cells and zero spectral weights. The
native average retains the lag axis. It averages defined lag values using
scalar weights, preferring rows with an unflagged frequency sample for the
same correlation. It computes the frequency flags and statistics from the
input frequency cells.

With `datacolumn="all"` on a MeasurementSet containing both FLOAT_DATA and
complex visibilities, `VbAvg::accumulateElementForCubes` can accumulate
calibrated and single-dish weights into the same buffer. The resulting
denominators attenuate CORRECTED_DATA, MODEL_DATA, and FLOAT_DATA. The same
layout can substitute FLOAT_DATA for complex DATA. Native averaging uses
the calibrated weights for corrected and model samples and the observed
variance for DATA and FLOAT_DATA. The oracle verifies the independent lanes
against CASA runs on the separate layouts and against an analytic lag sum.
[Owner decision](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7020003055)

## SPW-partitioned mstransform can drop a channel qualifier from a window range

With `createmms=True`, `separationaxis='spw'`, and `spw='0~1:1~5'`,
CASA 6.7.6.14 can retain every channel in both windows. Its partition helper
matches each numeric window to expressions with a literal window ID and
drops the channel qualifier on the range. casa-rs resolves the complete
selector and writes channels 1 through 5 for both windows.

## Automatic Multi-MS partitioning can keep flagged rows with keepflags disabled

CASA 6.7.6.14 can publish automatic or baseline Multi-MS members containing
rows with `FLAG_ROW=true` when the task uses `keepflags=False`. Its worker
prepends the flag predicate to an OR-separated partition expression without
parenthesizing the partition expression. Later alternatives can bypass the
flag predicate. casa-rs selects the partition and applies the flag filter to
every row. The paired Multi-MS probe includes an input with every row flagged.

## A nonzero mstransform DDI start can leave dangling row references

With CASA 6.7.6.14, a standalone `mstransform(ddistart=3)` of an MS
with one selected data description writes MAIN rows with
`DATA_DESC_ID=3` while the DATA_DESCRIPTION table contains one row.
That offset is intended for subsequent Multi-MS assembly. The native
task publishes the requested MAIN IDs and reserves the preceding
data-description entries as flagged rows with valid spectral window
and polarization references. The native output remains readable as a
standalone MeasurementSet (#1615).

## Channel-width argument variants can crash both MeasurementSet tasks

CASA 6.7.6.14 `split` and `mstransform` leave a partial MeasurementSet after
crashing on a zero-dimensional NumPy integer array or an active width list
with a zero or negative width. `mstransform` also crashes on an empty
`chanbin` list when `chanaverage=True`.

The native tasks interpret a zero-dimensional integer array as its scalar
value. They reject invalid active lists before creating output. The public
channel-averaging oracle runs the crash cases in separate processes and checks
the native outcomes. [Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7018325259)

## Channel averaging can leave the weights of FLOAT_DATA or LAG_DATA undefined

With only FLOAT_DATA or LAG_DATA selected, the CASA 6.7.6.14 channel-averaging
writer can leave required WEIGHT and SIGMA cells undefined. The spectral
statistics writer branches on DATA, CORRECTED_DATA and MODEL_DATA.

The native tasks derive defined weights and sigmas from calibrated input
weights and channel flags. LAG_DATA retains its independent lag axis. The
channel-averaging oracle checks the derived numbers against the input.
[Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7018429826)

The same spectral statistics writer is used with `mstransform(hanning=True)`.
The native task retains defined WEIGHT and SIGMA using the calibrated noise
and the Hanning coefficients. It smooths FLOAT_DATA on the frequency axis
and preserves LAG_DATA on its independent axis (#1618).

Regridding FLOAT_DATA likewise leaves those cells undefined in CASA. The
native regrid uses the selected calibrated noise spectrum and the output
flags for the scalar reductions (#1618).

## A terminal channel stride can leave an incomplete SPECTRAL_WINDOW

CASA 6.7.6.14 accepts `mstransform(spw="0:0~8^2", hanning=True)` on a
nine-channel MS, but its SPW writer attempts to use channel nine when
computing the last stride's footprint. It logs an error and leaves the SPW
metadata incomplete. The native transform uses the available final channel
as the last footprint and writes metadata for all five selected channels
(#1618).

## Spectral-window combination can ignore weights when exposures differ

For simultaneous overlapping windows with unequal EXPOSURE, CASA 6.7.6.14
`mstransform(combinespws=True)` reports that it will use spectral weights.
`MSTransformManager::combineCubeOfData` modifies a local coefficient during
its first pass but reloads the original geometric coefficient when it adds
the visibility. With equal bandwidth and observed weights of 4/9 and 4,
overlapping real samples 16 and 1000 yield 508. The native task applies
the matching observed weights in the sum and normalization, giving 901.6.
Both tasks record the mean input exposure (#1618).

## Combining spectral windows can copy the last row's FLAG_ROW

With simultaneous windows whose row flags disagree, CASA 6.7.6.14
`mstransform(combinespws=True)` can mark a usable combined row flagged
when the last input row is flagged. The loop in
`MSTransformManager::mapAndAverageVector` never increments its count,
so the branch that combines flags with AND is skipped. The native task
marks the row flagged only if all participating rows are flagged (#1618).

When spectral windows are combined, CASA also leaves the input's category
flags undefined and can leave an association to a deleted spectral window.
The native task preserves category flags through the channel mapping and
removes associations between windows that no longer exist separately. It
refuses to combine distinct LAG_DATA cells when requested, since a merged
frequency window has no mapping for their independent lag grids (#1618).

## Regridding can read beyond a final partial averaging bin

In CASA 6.7.6.14, an odd channel count with `chanaverage=True`, a
two-channel bin and `regridms=True` leaves a one-sample bin before
interpolation. `MSTransformManager::average` iterates over all output bins
using the full bin width, reaching past the input on the final bin.
With Hanning enabled, this can change the neighbouring channel's
visibility and noise. The native task averages only samples present in
the final bin. The paired regridding oracle also checks that result
against independent inverse-variance averaging, smoothing and linear
interpolation (#1618).

## Channel-bounds regridding can read past the upper band edge

CASA 6.7.6.14 can publish a fictitious 0 Hz channel after
`mstransform(regridms=True, mode='channel_b', start=7, nchan=3, width=1)`
on a nine-channel ascending input. Its upper-bound check admits index
nine even though the last channel is index eight. The native task clips
the bounds to the available channels. The oracle checks the native
result against a CASA request for the two valid channels (#1618).

## Frame conversion after time averaging can use an unrelated input epoch

CASA 6.7.6.14 `MSTransformManager::initFrequencyTransGrid` reads the
selected input's TIME column using the current visibility buffer's row
ID. After time averaging, `AveragingTvi2` generates output row IDs
sequentially, so those IDs no longer identify contributing input rows.
In the paired 20-second averaging case, the next group begins at input
time +20 seconds but CASA reads the integration at +10 seconds. The
native task uses the first contributing input row's epoch. The paired
case compares flags exactly and checks values at the project's 0.001
normalized RMS tolerance (#1618).

## Splitting a regridded spectrum can write oversized noise spectra

With nine channels and `mstransform(regridms=True, nspw=2)`, CASA
6.7.6.14 writes SPWs of five and four channels. The noise spectra in the
shorter window are padded to five with weight zero and sigma −1. When
all samples in that window are flagged, the padding also enters the
scalar statistic. The native task writes four values for every
channel-indexed column and computes statistics from those four values.
It rejects partitions that would leave an empty final window before
creating output. CASA can abort when attempting that final slice
(#1618).

## Selecting defined spectra or several channel ranges can crash channel averaging

Both CASA 6.7.6.14 tasks can terminate with SIGSEGV and leave an incomplete MS
when physical row zero has undefined weight spectra and the selection starts
with defined spectra. The output structure checks the physical first row for
spectral columns, and the writer checks the first selected row.

An active per-SPW width vector can also crash when an expression selects more
than one channel range in a window. The upstream data handler compares the
number of widths with the number of ranges, even when the vector covers every
selected window.

The native tasks allocate output columns for the selected spectra and apply
the requested width to each selected SPW. The oracle verifies both crashes in
subprocesses and compares native products against equivalent CASA selections.
[Owner approval](https://github.com/bglenden/casa-rs/issues/978#issuecomment-7019329916)

## Transforming an MS with FLOAT_DATA can replace the requested complex DATA

For an input with separately populated DATA and FLOAT_DATA, CASA 6.7.6.14
can fill output DATA from FLOAT_DATA, with zero imaginary components, when
the requested layout includes DATA. This occurs with `datacolumn="all"`,
`"float_data,data"`, and `"lag_data,data"` in the physical layout oracle.

The native tasks select each requested physical column and preserve its own
values through row, channel, and correlation selection (#1614).

## Transforming LAG_DATA leaves its cells undefined

CASA 6.7.6.14 creates the LAG_DATA descriptor for `split` and `mstransform`
but the visibility writer skips the column. Selecting only LAG_DATA can
also leave FLAG cells undefined.

The native tasks preserve the selected rows and correlations of LAG_DATA
with its independent lag axis. The frequency-channel selection applies to
the visibility and flag columns on the spectral axis (#1614).

## Transforming all columns can crash an MS with one scratch column

On an MS with a single physical visibility column, either CORRECTED_DATA
or MODEL_DATA, CASA 6.7.6.14 `split(datacolumn="all")` and
`mstransform(datacolumn="all")` terminate Python with SIGSEGV in the pinned
Linux build.

The native tasks keep the original visibility column and its weights in
these sparse layouts (#1614).

## Image removal can delete an open table through a symbolic link

In CASA 6.7.6.14, open a native image by its table path with `table.open()`.
`image.removefile()` refuses to delete that path while the table is open.
Calling it with a symbolic link to the same path can succeed and remove both
the link and the table. The open table handle then refers to a deleted table.

casa-rs resolves aliases before checking whether the table is in use and
preserves both names when it refuses removal. After the last handle closes,
removing through the link deletes the link and table as CASA does. See
[#1625](https://github.com/bglenden/casa-rs/issues/1625).

When `table.open()` holds the native image's `logtable`, CASA can also remove
the parent image and its log. casa-rs checks live subtables before deleting
the image.

An external directory move can also leave an attached tool pointing at a
pathname that now contains another image. CASA can delete that replacement
image through `ia.remove()`. casa-rs checks the directory identity captured
at open and refuses the removal if the pathname was rebound.

When a symbolic link lies inside a read-only directory, CASA can delete the
linked image and leave the link dangling. casa-rs checks that directory
before deleting the image and preserves both entries when it refuses.

## Image rename can move an open table before failing to reattach

In CASA 6.7.6.14, open one native image in two image tools in the same process
and call `rename()` on the first tool. CASA can move the table, then raise
`RuntimeError` while reopening it under the new name. The second tool keeps
the old name and can abort while closing that stale handle. casa-rs refuses
the move before changing either path while another tool holds the image.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

A lazy `imagecalc` result provides another reproduction. CASA 6.7.6.14
permits renaming the native operand while the result is attached. Its next
pixel read fails because the original tile path no longer exists. casa-rs
retains the operand while the lazy result is open and refuses the rename
before moving either path.

When a native table moves across filesystems, CASA 6.7.6.14 copies it and
then deletes the original entries in place. If a child directory is readable
but denies deletion of its contents, CASA raises after copying the image
and can leave a partial table at the original name. casa-rs moves the
original table to a private staging directory before copying, keeps the
published image readable, and reports the staging path if cleanup fails.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

With `parent/source.im` attached, calling
`ia.rename("parent", overwrite=True)` can delete the source table and
unrelated files in `parent` before CASA 6.7.6.14 reports an error. The same
loss occurs when `parent` is itself a native image table. casa-rs rejects
the relationship before deleting either path. The failed call closes the
attachment as CASA does. See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

When a rename destination contains a symbolic link followed by `..`,
CASA 6.7.6.14 can move a native table or FITS file into the directory
reached through the link, then remove `..` lexically and fail to reopen
the image from a different directory. casa-rs refuses the destination
before moving the source when the two directory paths resolve differently.
It also refuses a destination ending in `..` because the argument does
not identify a named replacement entry.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

### MIRIAD directory rename and replaced source paths

With `parent/source.mir` attached, `ia.rename("parent", overwrite=True)`
in CASA 6.7.6.14 can delete the source image and other entries in `parent`
before reporting an error. casa-rs rejects the ancestor destination before
moving or removing either path. The failed call detaches the tool, as in CASA.

Open `source.mir` without reading a pixel, move its directory to `saved.mir`,
and create a different image at `source.mir`. CASA can read the replacement
pixels through the original attachment's coordinates. `ia.rename("target.mir")`
can also move the replacement directory. casa-rs checks the image entry it
originally opened, so the first pixel read and the rename report that the
entry has changed. Both images stay at their existing names. A peer that has
already read a pixel keeps its original stream after a rename, as in CASA.

For moves across filesystems, casa-rs checks that it can remove the original
tree before publishing the copy. A directory with readable contents whose
permissions prevent removal causes a refusal with both public names preserved.
See [#1955](https://github.com/bglenden/casa-rs/issues/1955).

### Copying a MIRIAD image with incomplete coordinates can leave a partial table

Open a MIRIAD image whose header describes fewer coordinate axes than its
stored shape, for example two Stokes planes whose MIRIAD codes both map to
fractional linear polarization. CASA 6.7.6.14 reports the shape and decoded
coordinates, but a named `fromimage` call creates a partial native table
before raising `Failed AlwaysAssert setCoordinateInfo`. casa-rs reports
the shape and decoded coordinates and rejects the copy before publication.
See [#1955](https://github.com/bglenden/casa-rs/issues/1955).

### Moving a concatenation can rebind MIRIAD data links

Set `source/a.mir/image` to the relative symbolic link `../b.mir/image`.
Concatenate `source/a.mir` with a different `b.mir`, using `mode="move"`.
CASA 6.7.6.14 can resolve the link to the newly adjacent `b.mir` and return
its pixels in both sections without reporting an error. Casa-rs checks the
projected bindings of the linked payload and metadata before moving either
input. If a binding would change, the call preserves both inputs and any
previous output. `mode="copy"` preserves the linked file contents in the
owned MIRIAD copy. See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

### A failed nested concatenation lock can retain a table lock

Nest a virtual concatenation containing a MIRIAD image and a native table
inside another virtual concatenation, with `tempclose=False` at both levels.
Unlock the outer image and take a read lock on its final native member in
another process. CASA 6.7.6.14 returns `False` from `lock(writelock=True)`
but can retain the write lock on the earlier native table. Casa-rs restores
each member's physical lease independently of the MIRIAD advisory flags.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

### A virtual concatenation beam edit can abort when a member cannot publish

Concatenate two native images with `tempclose=False`. Remove `table.lock`
from the second image and make its directory read-only while keeping the
table files writable. CASA 6.7.6.14 reports success from
`setrestoringbeam`, then aborts on `done()` when casacore cannot create
`table.dat_tmp`. Casa-rs checks every native member's write access before
the beam edit, so the call returns an error and the earlier member keeps
its original beam. See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

### A null field in saved image-expression metadata can crash `image.open`

Create a saved Float expression with `"MiscInfo": {"unknown": null}` in
its `imageexpr.json` and open the directory. CASA 6.7.6.14 can segfault
while opening the manifest. Casa-rs rejects the null field with a
catchable error. An absent `MiscInfo` field still inherits the operand
record, and a present empty record still overrides it. See
[#1959](https://github.com/bglenden/casa-rs/issues/1959).

### A relative saved-expression name can point at another directory after `chdir`

Open a saved expression with a relative manifest path, then change the
working directory. CASA 6.7.6.14 continues reading the original operands,
but `image.name()` reports the manifest path relative to the new directory.
Casa-rs records the manifest's absolute name at attachment so the name
continues to identify the same image. See
[#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Cyclic saved expressions can crash `image.open`

Create two saved image-expression directories whose `ImageExpr` fields
refer to each other, then open either directory. CASA 6.7.6.14 recurses
until the Python process segfaults. Casa-rs resolves aliases to the same
directory and raises an error on the cycle before publishing an output.
The same active path check covers a saved expression reached through a
virtual concatenation.
See [#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Copying a saved expression can delete one of its manifests

Use a native table directory as a saved expression by adding
`imageexpr.json`, or put an expression directory inside a native table
directory. Use that expression in an outer saved expression and copy the
outer expression with `fromimage` or `newimagefromimage`, naming the
native table as the destination and setting `overwrite=True`. CASA
6.7.6.14 succeeds and deletes the inner manifest, breaking the original
outer expression. Casa-rs rejects an overwrite that overlaps any
captured operand and preserves the receiver and source files.
See [#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Updating saved-expression metadata can overwrite another manifest

Open `expression.im` by relative name, then change into a directory that
also contains `expression.im`. CASA 6.7.6.14 writes `setmiscinfo` to the
second directory while the tool still reads from the first expression. An
external replacement at the original pathname can also receive the write.
Casa-rs updates the original directory after a working-directory change.
It refuses a stale write when the directory or manifest was replaced,
even if the replacement contains the same expression.
See [#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Writing some saved-expression metadata can break reopen or lose values

CASA 6.7.6.14 accepts NaN, infinity, empty arrays, and multidimensional
arrays in `setmiscinfo` on an open saved image expression. Reopening can
segfault for the non-finite values or reject the arrays. Casa-rs refuses
these writes before changing the manifest or the live record. CASA can also
omit a NumPy `int64` scalar or truncate the elements of an `int64` vector.
Casa-rs stores wider integers without truncation in the same JSON fields,
which both implementations can read. See
[#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Renaming a saved expression can move a replacement or erase an operand

Open a named saved expression and move its directory outside the image
tool. If another expression is then created at the old pathname, CASA
6.7.6.14 renames and opens the replacement. A relative name can also
point at a different directory after `chdir`. Casa-rs preserves the
captured directory and rejects an external replacement.

Rename a saved expression with `overwrite=True` to the native table
it reads, or to its own containing directory. CASA can replace the
operand before segfaulting or delete the containing directory before
reporting a move error. Casa-rs refuses overlapping destinations and
moves that would remove a captured operand's pathname, including an
alias stored inside the expression directory. This includes an alias
reached through another alias to the expression directory. CASA can
move the directory and then fail to reopen its operand, leaving the old
pathname absent.

If `imageexpr.json` links to `../manifest.json`, moving the image under a
different parent can make the link resolve to a different manifest.
CASA accepts the move and attaches the replacement's metadata. Casa-rs
preserves the referent and restores the original directory entries if
the move would switch to a different manifest with the same expression.
See [#1959](https://github.com/bglenden/casa-rs/issues/1959).

### Linked saved-expression writes can follow a changed referent

Open a saved expression whose `imageexpr.json` is a symbolic link. Replacing
that link or the file it points to after attachment makes CASA 6.7.6.14
write to the replacement on `setmiscinfo`, even when the live graph still
uses the original expression. Casa-rs checks the captured link and file
identities, rejects a stale update, and preserves the live metadata on
failure. A successful update atomically replaces the file through its
captured parent directory and leaves the link in place.

On a move between two APFS filesystems, CASA copies the resolved file
into a regular `imageexpr.json`. Casa-rs copies the link, verifies its
referent at the final location, and keeps metadata writes directed to that
referent. An internal relative link moves with its file. An external
absolute link continues to share the existing file.
See [#1959](https://github.com/bglenden/casa-rs/issues/1959).

## Uncached component-list images can count a point source twice

Create a temporary component-list image of shape `[6, 8]` with a 2 Jy
point source at the default direction reference and `cache=False`.
In CASA 6.7.6.14, the first full read returns 2 Jy at `[3, 4]`. The next
read returns 4 Jy there. The cached case returns 2 Jy on both reads.
Casa-rs samples the same owned component model for each requested
section and returns 2 Jy in either cache mode.
See [#1691](https://github.com/bglenden/casa-rs/issues/1691).

## A scalar spectral index can give invalid polarized component-image pixels

In CASA 6.7.6.14, create a point component with Stokes flux
`[2, 1, 0, 0]` Jy, reference frequency 1.415 GHz and scalar spectral
index `-3`. Sample a component image with Stokes I and Q and frequencies
1 kHz to either side of the reference. The Q values can depend on other
allocations in the Python process. `SpectralIndex::sampleStokes` indexes
four parameters in its matrix overload when the scalar input supplied
one. The explicit vector `[-3, 0, 0, 0]` produces the expected power law
in both planes. Casa-rs returns that same power law for the scalar form.
See [#1691](https://github.com/bglenden/casa-rs/issues/1691).

## A renamed component-image mask can retain its previous table path

In CASA 6.7.6.14, create a native component-list image with a mask,
copy the mask, select the copy as the default and rename it.
`maskhandler("default")` returns the new name, but reading the mask
through that attachment can fail with a file error for the old
directory. Reopening the image reads the renamed table. Casa-rs
in the same attachment reads the mask from its new path when the
rename succeeds.
See [#1691](https://github.com/bglenden/casa-rs/issues/1691).

## An attached virtual concatenation can write to a retargeted input symlink

Create a `nomove` concatenation using a symlink as one input, then retarget
that symlink to a second image of the same shape while the tool is still
attached. CASA 6.7.6.14 reads the new image and sends `putchunk` writes to it
under both `TempClose` settings. casa-rs pins the physical input for the
attachment, so the write reaches the original image. A new open resolves the
manifest paths again. This prevents an existing handle from modifying an
unrelated image.

See [#1749](https://github.com/bglenden/casa-rs/issues/1749).

## A strided write to a virtual concatenation can abort at a skipped member

In CASA 6.7.6.14, concatenate three `[3, 4]` Float members along
axis zero with `mode="nomove"` and `relax=True`, with the first
member repeated in the third position. A `[2, 1]` write from
`blc=[0, 0]` with `inc=[6, 1]` aborts Python. The middle member
intersects the bounding interval but has no selected samples.
Casa-rs skips members with no requested samples and writes the two
samples in order to the single physical member.
See [#1749](https://github.com/bglenden/casa-rs/issues/1749).

## Failed virtual concatenation can leave inputs and a previous output displaced

In CASA 6.7.6.14, concatenate images with shapes `[2, 3]` and `[2, 4]`
along axis zero. With `mode="copy"` the shape error leaves partial copies
in the output. With `mode="move"` the error can leave both operands in that
directory, with the original paths gone. A pre-existing destination can be
removed before validation. casa-rs checks shape and path constraints first,
publishes the complete output, and restores original names if a later step
fails. Same-filesystem moves and rollback rename entries without duplicating
pixel data. Across filesystems the move makes the necessary destination copy
once and holds the original until publication succeeds.

Distinct operands from different directories can also have the same
basename. CASA's copy can replace the first copy and bind both sections
to the second operand. Move can remove both originals and retain only the
second. casa-rs refuses the conflict before transferring either image.

An input can itself be a virtual image whose manifest refers to sibling
images. Copying or moving it together with other images of those same
basenames can silently substitute the other images in the nested product.
casa-rs checks the manifest graph and symbolic links in the proposed
directory layout before transferring the inputs. A chain of relative symlinks
can move together when it continues to resolve to the same image.

With a saved expression and its referenced native image as operands,
`mode="move"` can displace both inputs and leave a product whose expression
still points to the old pathname. Using the expression's referenced table
as the output can also crash Python in `mode="nomove"`. Casa-rs checks the
transitive operands and the proposed storage layout before publication.
See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

See [#1749](https://github.com/bglenden/casa-rs/issues/1749).

## Renaming a sibling-relative virtual image can displace a readable image

Create a `nomove` concatenation of two sibling images in CASA 6.7.6.14
and rename it into a new parent directory. The operands remain in the old
parent, so reopen fails after CASA moves the virtual directory. casa-rs
checks operand resolution before rename and refuses a destination that
changes the bindings, including an owned member linked to an external
relative target. Renaming a self-contained tree preserves its nested
member paths across parent directories.

See [#1749](https://github.com/bglenden/casa-rs/issues/1749).

## `calcmask` can crash on decisive Boolean validity

In CASA 6.7.6.14, create `source.im` as a float image with values
`[[1, 2], [3, 4]]` and default mask `"source.im" > 2`. On another
image of shape `[2, 2]`, `calcmask('mask(("source.im" > 0) || T)')`
and `calcmask('mask(("source.im" > 0) && F)')` terminate Python with
SIGSEGV. Reversing the operands also crashes. An unmasked image predicate
can trigger the same failure, for example
`mask(("source.im" > 2) || (value("source.im") <= 2))`.

casa-rs evaluates validity per pixel. A known true operand makes OR valid,
and a known false operand makes AND valid. Where neither operand determines
the result, both operands must be valid. The resulting validity mask is
all true in these reproductions. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## `calc` can crash on a mixed-precision vector

With a temporary float image of shape `[3]`, CASA 6.7.6.14 terminates Python
with SIGSEGV when evaluating `calc("[1, 2.5, 4] + 1", False)` or
`calc("[-1.25, -2e1, +3.2]", False)`. The inputs mix single and double
precision scalars in an array literal.

casa-rs promotes those elements using the expression's numeric types and
stores the results `[2, 3.5, 5]` and `[-1.25, -20, 3.2]` as float pixels.
[#1625](https://github.com/bglenden/casa-rs/issues/1625)

## `imagecalc` can publish pixels before rejecting the metadata source

In CASA 6.7.6.14, evaluate
`imagecalc('out.im', '"source.im" * 3', True, 'missing.im')` with a valid
float `source.im` and a missing metadata image. CASA raises `RuntimeError`
after writing the calculated pixels to `out.im`. If `out.im` already held
an unrelated image, the same failed call replaces its original pixels.

casa-rs validates the metadata source and stages the product before
publication. The failed call preserves an existing output and leaves a new
output path absent. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

The same staging protects inputs of a saved metadata expression. For
`out.im` containing `leaf.im`, use a saved `imagemd` expression that reads
`out.im/leaf.im` and request `imagecalc(..., outfile='out.im', overwrite=True)`.
CASA deletes the nested input before failing to open the metadata. Casa-rs
rejects the overlap and keeps both tables. An exact reference to `out.im`
is valid and reads the new staged output.

## An analysis output can remove an entry needed by its mask expression

Use a saved expression as the `mask` operand of `subimage` and replace an
existing native `out.im`. In CASA 6.7.6.14, the replacement can delete a
symbolic link inside `out.im` used to reach the saved image or one of its
leaves. It can also delete the referent of a linked `imageexpr.json` inside
`out.im`. A leaf read can then fail after the original directory is gone.
Casa-rs checks the entries used by the mask before publication and keeps
the original table. A disjoint sibling reached through the same directory
link is still writable. See [#1627](https://github.com/bglenden/casa-rs/issues/1627).

## A saved `imagecalc` operand can return a stale cached section

With `imagecalc(pixels='"expr.im" + 1')`, where the saved `expr.im` contains
`"leaf.im" * 3`, read a one-pixel section, unlock the result, and add 50 to
the underlying `leaf.im` with another tool. CASA 6.7.6.14 can return its
previous value on the next read of the same section. Casa-rs returns the
updated value on that read. See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

## `imagecalc` can read a different FITS operand after a directory change

Create two directories with an `input.fits` of the same shape in each. Give
the first image pixel value 2 and the second image pixel value 21. In the
first directory, create a lazy CASA 6.7.6.14 result using
`imagecalc(pixels='"input.fits" * 3')`. Change to the second directory before
the first pixel read. CASA returns 63 while keeping the first file's
metadata. With a native image operand, CASA returns 6 in this sequence.

casa-rs resolves the operand paths when creating the result and returns 6
with the original FITS metadata. The FITS handle is opened on the first pixel
read and retained for subsequent reads. [#1625](https://github.com/bglenden/casa-rs/issues/1625)

## `image.addnoise` can erase pixels or fail to return on unusable input

Fill an image with nonzero pixels and call
`ia.addnoise("normal", [0, -1], zero=True, seeds=[1, 2])`. CASA 6.7.6.14
zeros the region before reporting an illegal variance. An unknown
distribution such as `"bogus"` has the same destructive order. casa-rs
validates the distribution before writing the existing image.
Passing `seeds=None` explicitly can also change the image before the Python
wrapper raises `SystemError`. casa-rs rejects that argument before changing
pixels.

CASA also fails to return for a one-pixel image with
`ia.addnoise("geometric", [0], seeds=[1, 2])` or
`ia.addnoise("poisson", [800], seeds=[1, 2])`. The geometric rejection loop
cannot draw a uniform zero with those seeds. The Poisson bound underflows to
zero, leaving its product loop true after the product reaches zero. casa-rs
returns a Python error for these inputs.

## PlotMS leaves calibration-table header values blank

CASA 6.7.6.14 with casaplotms 2.9.1 exports labels such as `File:` and
`Observation date:` when `headeritems` is set on a current bandpass table.
The corresponding values are blank even in a wide export with only
`headeritems='filename'`. Casa-rs reads available values from the table and
lays them out within the requested figure dimensions. Wildcard row IDs leave
the matching metadata values blank. [Calibration diagnostics #1526](https://github.com/bglenden/casa-rs/issues/1526)

## PlotMS can fail when a calibration-table transform is enabled

With CASA 6.7.6.14 and casaplotms 2.9.1, enabling `freqframe='BARY'` or
`freqframe='TOPO'` on the same bandpass table failed in the headless plotting
process before completing the export. `CalCache::loadIt` documents the
calibration-table behavior with its warning that transformations are ignored.
Casa-rs emits that warning and exports the stored spectral coordinates.
Both engines produced identical text when `transform=False` with nondefault
frame, rest frequency and velocity convention. [Calibration diagnostics #1526](https://github.com/bglenden/casa-rs/issues/1526)

## Mixed averaged flagdata can leave row flags clear

On the public `Four_ants_3C286.ms` fixture, CASA 6.7.6.14's stock mixed
Clip, TFCrop, RFlag, and antint list test starts from cleared flags. The
shared iterator averages pairs of integrations and groups channels in
bins of eight. The task flags 591,808 samples and leaves `FLAG_ROW` false
for 26 of the 486 rows whose samples are all flagged in the repeated
stock replay. The same 26 rows occur with the Clip, TFCrop, and RFlag
prefix alone. An interleaved timing replay returned another row mask
with identical sample flags. CASA's
`AveragingTvi2::writeFlag` derives the row flag from the propagated
sample mask. Casa-rs preserves that rule after projecting the final
shared mask to the input rows. The stock test passes and every sample
flag agrees with CASA.

## Calibration quack can flag the whole scan when times are out of order

With CASA 6.7.6.14, two Tsys time blocks in the same scan stored in reverse
order are both flagged by `quackinterval=1.0` with either `quackmode='beg'`
or `quackmode='endb'`, even though their timestamps are ten seconds apart.
The calibration handler uses the first and last eligible stored rows to
derive the scan boundaries. Casa-rs uses the eligible timestamps and flags
the chronological beginning or end as requested.

## Calibration flagdata ignores a channel stride

CASA 6.7.6.14 manually flags channels 1 through 5 on current CPARAM
bandpass and FPARAM Tsys tables for `spw='0:1~5^2'`. The channel stride
selects channels 1, 3, and 5. Casa-rs flags those channels and preserves
channels 2 and 4. The calibration flagdata oracle checks both outcomes.

## Calibration RFlag can extend flags outside the selected channels

In CASA 6.7.6.14, `flagdata(mode='rflag', spw='0:3~7')` with automatic
extension can flag channel 0 in both CPARAM bandpass and FPARAM Tsys
tables. The corresponding selected channels agree with casa-rs, which
preserves every flag outside the requested slice on both calibration tables
and MeasurementSets.

On a calibration table or MeasurementSet, `winsize=0` and `winsize=-1`
abort the CASA process with an out-of-range vector access. Casa-rs rejects
nonpositive window lengths before running the agent.

Using a positive explicit time or frequency threshold with a zero or
negative scale can also crash CASA while applying flags on either a
calibration table or a MeasurementSet. Casa-rs rejects these combinations
before writing flags. Calculate mode ignores the scale factors and reports
the unscaled calculated thresholds.

## Calibration flagdata summary can crash on a wildcard field

CASA 6.7.6.14 `flagdata(mode='summary')` can segfault on a delay table
created by `gencal(caltype='sbd')`, whose FIELD_ID is -1. Casa-rs counts
the flags and uses the literal `-1` key for the wildcard field. The
calibration flagdata oracle runs the CASA call in a separate process and
verifies the native report and unchanged input.

## Calibration command lists can abort or flag beyond an unmatched selector

CASA 6.7.6.14 aborts with a vector allocation error when an antenna
integration command names the physical CPARAM or FPARAM column in a
calibration flag list. Casa-rs raises a ValueError identifying the required
visibility input and preserves the table's flags. The single-task
`mode='antint'` call on these tables raises CASA's agent-configuration error.

In a multi-command list, an antenna or field name that matches no calibration
rows can cause CASA to flag the whole table. Casa-rs rejects the selector
before publishing staged edits. The calibration command-list oracle verifies
both the unmatched-antenna refusal and the antenna-integration crash case.

On an FPARAM table, a local `antenna='!A1' spw='0:1'` command followed by a
summary can flag only A1. Used alone, the same command can leave all flags
unchanged. Casa-rs flags the selected channel for antennas other than A1 in
both cases. The oracle checks each mask against the original table.

The direct `agentflagger` tool can abort when its antenna-integration agent
names a physical CPARAM or FPARAM column. In casa-rs, `run()` raises a
RuntimeError and leaves the stored flags intact, including when an earlier
agent staged a flag edit. An unmatched local antenna selector also raises
before publication. The direct calibration agent oracle checks both paths
and verifies that the next run starts without the consumed agents.

With a direct manual calibration agent configured with `array='9'` and
`antenna='A2'`, CASA cancels filtering and flags every antenna. Casa-rs keeps
the valid antenna restriction to avoid changing flags on unrelated rows. The
direct oracle checks both masks on CPARAM and FPARAM tables.

## PlotMS shows the excluded antenna on a GSPLINE plot

On a GSPLINE table with antennas A0 through A4, CASA 6.7.6.14 and
casaplotms 2.9.1 plot only A2 for `antenna="!A2"`. The legacy selector
excludes A2 from the source row query, then passes the excluded ID to the
plot cache as an antenna to display. The cache evaluates and plots that ID.
casa-rs applies the exclusion to the source rows and plots A0, A1, A3, and
A4. The legacy calibration PlotMS oracle checks both outcomes.

## PlotMS averages disjoint channel selections on a compressed sample grid

With CASA 6.7.6.14, a callibrary, `spw='0:0~2;5~7'` and
`avgchannel='2'`, PlotMS retains frequency bins for original
channels 0–1 and 5–6. It averages visibility samples from original
channels 0–1 and 2 and 5, in the order of its compressed channel
buffer. casa-rs preserves the paired text coordinates and sample
values so existing PlotMS diagnostics can be compared directly (#1526).

## `impv` can lose overlap on small rotated strips

In CASA 6.7.6.14, a 12-by-12 direction plane can report that the input and
rotated output do not overlap even when both endpoints are at least two
pixels from the edges. Examples are a reversed horizontal cut from
`[8,6]` to `[3,6]` and a vertical cut from `[5,3]` to `[5,8]`. Padding the
input direction plane before the same sky-coordinate cut restores the CASA
result. casa-rs interpolates only the requested strip and agrees with the
padded CASA reference without requiring callers to pad the input (#1024).

## `impv` masks early validation errors during cleanup

CASA 6.7.6.14 raises `UnboundLocalError` when `outfile` is empty, when
coordinate mode lacks an endpoint, or when length mode lacks a center,
length, or position angle. The task raises its validation error before
assigning `myia`, then the `finally` block references that local. casa-rs
identifies the missing parameter or mode-specific selection and leaves no
output. The upstream suite checks each implementation's error (#977).

## imregrid drops brightness units and the beam when broadcasting a channel

In CASA 6.7.6.14, `imregrid` with a one-channel input, an explicitly selected
spectral axis and a multi-channel output replicates the data and mask. During
that copy it loses the brightness unit and restoring beam. The `singleton`
case in `crates/casa-images/tests/data/imregrid/expected.json` records a
`Jy/beam` image with a 4 by 3 arcsec beam becoming a unitless image without a
beam. casa-rs matches the samples, mask and template frequency grid and retains
the original units and beam. Copying channels does not change the calibration
of the pixels being copied.

## `imregrid` can terminate when shifting a sliced direction

CASA 6.7.6.14 on macOS returns the original pixels and mask when regridding
the unchanged direction axis of the 3 × 2 × 5 `dropdeg_ra.image` fixture.
Increasing the retained axis's reference pixel by 0.25 in the same template
record makes the task terminate with SIGBUS. The casacore regrid path
treats a direction coordinate as a two-axis plane even when an axis has
been removed. Casa-rs preserves the unchanged slice directly. It refused
the shifted fixture with an all-masked result and published no output.

## `imregrid` can attach new linear coordinates to unshifted data

CASA 6.7.6.14 can copy pixels and masks unchanged when regridding a leading
axis of a three-axis linear coordinate at the same shape. The
`same_shape_linear_shift` case in
`crates/casa-images/tests/data/imregrid/expected.json` changes the first
axis's reference value by 0.25 m on a 7 × 6 × 5 image. CASA reports the new
reference value but retains the source samples. In casacore's
`LinearCoordinate::near`, the exclusion loop marks consecutive entries
instead of the indexes in the supplied exclusion list. The comparison can
therefore skip the axis being regridded.

Casa-rs samples the shifted grid and updates the mask. For unit-spaced
pixels, the first two samples with values 0 and 1 yield 0.25 at output pixel
zero. The last sample lies outside the input and is masked. The fixture
records both CASA's actual product and the independently calculated shift.

## Legacy MosaicFT rejects split voltage-pattern image records

The CASA 6.7.6.14 legacy `HetArrayConvFunc::findAntennaSizes` checks
for `realimage` and `imagimage` in a custom `IMAGE` record. It reads
the imaginary image from `imagim`. `vpmanager.setpbimage` saves the
standard field `imagimage`. The public `imager.ft` then reports that
`imagim` is unknown and returns false.

The same responses packaged as complex Jones images work through the
public legacy imager. Adding `imagim` as an alias in the split records
also works and gives the same CASA prediction. casa-rs reads the standard
field for native prediction. For virtual interoperation, the Fourier grid
owns a copy of the voltage-pattern table with the alias. A real-only
pattern uses a zero-valued imaginary image owned by the same grid.
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520) ·
[`imager_custom_mosaic_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_custom_mosaic_oracle.py)

## Image collapse can abort or shift a wrapped longitude by 180 degrees

CASA 6.7.6.14 aborts for negative or duplicate axes, for example
`ia.collapse("sum", [-1])` and `ia.collapse("sum", [0, 0])`. casa-rs
raises a Python error before replacing the output.

On a default 4 × 4 direction image, `ia.collapse("sum", [0, 1])` also
averages longitudes on opposite sides of the 0-degree meridian as ordinary
numbers. CASA reports a longitude near 180 degrees for a footprint centered
about half an arcminute from the meridian. casa-rs unwraps both endpoints around the interior sky position before
averaging, so the result remains on the selected footprint. This also
keeps the midpoint on fields broader than 180 degrees.

CASA 6.7.6.14 also segfaults collapsing a Stokes–Frequency image whose
native record puts Frequency at pixel axis 1 and world axis 0. casa-rs
resolves `axes="Frequency"` through the native world-to-pixel map and
collapses the corresponding pixel axis.

## Continuum subtraction leaves a partial product for aliased outputs

CASA 6.7.6.14 accepts the same initially absent path for `outline` and
`outcont` in `image.continuumsub`. It writes the continuum model before
raising an error while publishing the residual. With overwrite enabled,
CASA still reports an error after writing the model. casa-rs rejects
aliased destinations before fitting and verifies both destinations before
publishing either image.

## A zero-stride region aborts `image.modify`

CASA 6.7.6.14 terminates with SIGABRT and an uncaught
`casacore::ArraySlicerError` when `image.modify` uses an LCSlicer record
whose increment is zero. A scratch probe used a 7 × 7 float image and a
point component, with the first increment of a region covering [1, 1]
through [5, 5] set to zero. Casa-rs raises a Python error during region
validation and preserves the stored pixels and history (#1625). The same
zero-stride record also aborts CASA when passed to `ia.fromimage` or
`ia.newimagefromimage`. Casa-rs reports a Python error before copying.

## Legacy imager single-dish Gaussian settings and virtual recipes

CASA 6.7.6.14 constructs the default single-dish imager with an empty
`convertfirst` string. An explicit `setsdoptions(convertfirst='NEVER')`
allows `imager.ft` to proceed. casa-rs starts with that default.

The legacy `SDGrid` copy constructor omits the Gaussian widths and
cutoff that `CubeSkyEquation` needs when it initializes the live model.
On a smooth ALMA model with off-centre recorded pointings, pinned SF and
BOX produce nonzero scratch models. The pinned GAUSS control returns
zero even with an explicit positive cutoff. casa-rs evaluates GAUSS and
GJINC from their kernel equations. The paired oracle uses the pinned
pointing coordinates and independent Gaussian and Bessel evaluations.

CASA publishes an `SDGrid` record for `sd` without the kernel settings.
For `both`, `GridBoth::toRecord` writes its two transforms into the same
record, leaving the SDGrid name and image with some GridFT fields.
Pinned `uvsub` rejects the records for both modes. casa-rs saves the
single-dish settings with the complex SDGrid image and gives GridFT its
own entry for `both`. The CASA image tool opens both backing images, and
native `uvsub` reopens the models. An incomplete CASA recipe raises an
error that identifies the missing settings.

Standalone SDGrid uses the interpolator selected by `freqinterp` for
scratch prediction. Its native saved recipe retains that choice and the
original channel span on reopen, including the single-channel bypass.
GridBoth delegates prediction to its two child transforms. The legacy
frequency setter updates the parent, so both children use nearest
neighbour. In the pinned five-plane test, all four methods produce identical
mixed-grid outputs. The native mixed-grid prediction follows that behavior.

With `setvp(dovp=True)`, pinned scratch prediction applies the field's sky
voltage pattern as it advances through the visibility buffers. Its virtual
recipe stores the first apparent sky for the selected fields. The beam oracle
compares the stored sky pixels with CASA, then uses that pinned image as input
to a separate CASA scratch prediction to verify native readback. Standalone
SDGrid takes custom gridding beams. GridBoth keeps its default gridding beam
and can still apply a custom sky voltage pattern. The legacy voltage-pattern
lookup uses the first concrete table row for the telescope. A later row
marked as the VPManager default does not change that choice.

SDGrid can include a cross-correlation when all of its UVW components
are nonpositive. casa-rs applies the single-dish contribution to
autocorrelations. In `both`, the ordinary GridFT path predicts the
cross-correlations.

For fast scans, CASA normally fits a natural cubic through each antenna's
pointings. Its linear fallback can take neighboring rows from another
antenna when the short sequence is interleaved. casa-rs interpolates over
the recorded samples of the requested antenna. With an empty POINTING
table, casa-rs applies the first feed's offset to the field direction.
The paired test uses pinned measures to shift the field in its reference.
The pinned SDGrid copy also drops the requested conversion order and
disables the pointing cache. casa-rs retains both controls. In the
conversion-order test, pinned CASA measures convert each AZEL control
point at its recorded epoch. A second pinned run samples that converted
track in the image frame.
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520) ·
[`imager_single_dish_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_single_dish_oracle.py) ·
[`imager_single_dish_options_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_single_dish_options_oracle.py) ·
[`imager_single_dish_spectral_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_single_dish_spectral_oracle.py) ·
[`imager_single_dish_beam_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_single_dish_beam_oracle.py)

## Legacy `pbwproject` virtual recipes lack a machine name

CASA 6.7.6.14 predicts nonzero complex model visibilities in the legacy
VLA single-W-plane case with a new or existing convolution cache.
`nPBWProjectFT::toRecord` saves an image and
transform geometry. It omits the Fourier-machine name and the convolution
needed to repeat the prediction. Pinned `ms.uvsub` refuses that record
with `field name is unknown`. Its virtual-model reader also lacks an
`nPBWProjectFT` constructor.

Casa-rs shares the cache generator with the legacy EP workflow and
retains the Fourier image and its convolution in a native recipe.
The CASA image tool opens both images. Native `ms.uvsub` reopens the model
after the original cache is removed from its path. Both readers refuse
the incomplete pinned recipe before subtracting a visibility model.
[`imager_legacy_pbw_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_legacy_pbw_oracle.py) ·
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

## FITS export can change axis lengths or abort while removing axes

With CASA 6.7.6.14, export a float image of shape `[3, 4, 2, 1]` with a
two-plane Stokes axis and one spectral channel using `image.tofits` with
`deglast=True` and `stokeslast=True`. The output declares two spectral
channels and two Stokes planes even though the source has only 24 pixels.
The degenerate-axis pass uses the input extents after the Stokes pass has
already changed their order. casa-rs writes `[3, 4, 1, 2]` and preserves
all pixel values and their coordinates.

CASA also raises `Unknown error copying image to FITS file` when moving a
leading singleton axis in a linear image of shape `[1, 4, 2]` with
`deglast=True`. casa-rs writes `[4, 2, 1]` with the data in the matching
axis order, readable in CASA.

On the pinned Linux build, `dropdeg=True` on a `[1, 1, 1, 1]` float image
can terminate Python with SIGFPE. casa-rs raises `RuntimeError` because no
image axes would remain. See [#1625](https://github.com/bglenden/casa-rs/issues/1625).

## Image region collapse can select the first pixel or abort

With a 5×4×2 image whose coordinate is linear, CASA 6.7.6.14
`image.getregion(axes=[0])` can return shape `(1, 1, 2)` with the first
two input pixels. With the standard coordinates and double-complex
pixels, the same call can terminate the process.

casa-rs averages valid pixels over the requested axes and retains all
other axes, for every supported image pixel type (#1627).

## Empty integer selectors can abort MeasurementSet metadata queries

On an attached `msmetadata` tool, CASA 6.7.6.14 aborts the process when
`namesforfields`, `namesforspws`, `bandwidths`, `rxbands`, or `subwindows`
receives a zero-length NumPy integer array, such as
`numpy.array([], dtype=numpy.int64)`. Its integer-vector path computes a
minimum on the empty array and the resulting casacore exception escapes
the Python binding. An empty Python list selects all values through the
default path.

casa-rs treats an empty integer array as an empty selector and returns
the same result as the corresponding omitted argument (#1029).

## Explicit observation and array IDs can select an uninitialized scan key

On an attached CASA 6.7.6.14 `msmetadata` tool, get a valid scan number
from `scannumbers(0, 0)` and call
`fieldsforscans([scan], obsid=0, arrayid=0)` or
`timesforscans([scan], obsid=0, arrayid=0)`. The vector selection
initializes the scan number but leaves the observation and array IDs
uninitialized. CASA can report an unknown scan with unrelated IDs. The
single-scan and `asmap=True` variants do use the supplied IDs.

casa-rs uses the requested observation, array, and scan number for the
vector selection too, and reports an error for a missing scan (#1029).

## Scan queries can abort on an empty MeasurementSet

Create a valid MeasurementSet with no MAIN rows and open it with
`msmetadata`. In CASA 6.7.6.14, calling `narrays()` or `scannumbers()`
then terminates the process with an uncaught casacore exception. The
array-ID check calls `max` on the empty MAIN column.

casa-rs reports zero arrays and returns an empty scan-number array
(`numpy.int64`) for the default query, consistent with `nscans()`
returning zero on the same MeasurementSet (#1029).

## Unsupported scan-selector array types can select every scan

With CASA 6.7.6.14, pass a nonempty `numpy.float16` or `object` array to
`msmetadata.fieldsforscans`. CASA emits a conversion message, discards
the supplied scan numbers, and returns fields for every scan. casa-rs
raises `TypeError` so the unsupported array type cannot silently widen
the selection (#1029).

## Field time selection can include the first field after the time window

In CASA 6.7.6.14, open a MeasurementSet whose first MAIN time is after
zero and call `msmetadata.fieldsfortimes(0.0, 0.0)`. The result includes
the first observed field because the loop collects fields before testing
the upper bound. casa-rs includes only fields with an observation time
inside the requested inclusive interval (#1029).

## Empty or out-of-range time selectors can terminate CASA

For an attached CASA 6.7.6.14 `msmetadata` tool, passing
`numpy.array([], dtype=numpy.int64)` to `timesforscans` can segfault
because the binding dereferences the minimum of an empty vector.
Passing the same array to `timesforspws` can abort with an empty-array
exception. In `timesforspws`, a positive scalar outside the spectral
window table can also segfault because that path indexes without a
bounds check.

casa-rs interprets the empty integer vector as the full in-scope set
and raises `RuntimeError` for the out-of-range spectral window (#1029).

## An unsigned scalar can widen a spectral-window time selection

In CASA 6.7.6.14, open a MeasurementSet with more than one spectral
window. `msmetadata.timesforspws(numpy.uint32(0))` returns a dictionary
covering all windows. A Python integer, signed NumPy integer, or
one-element unsigned integer array returns the requested window.
casa-rs treats the unsigned scalar as the requested window too, so its
numeric value cannot be lost and the selection widened (#1029).

## Image coordinate frequency conversion can use the wrong physical direction

Create a CASA 6.7.6.14 image with shape `[20,20,20]` and the default J2000
direction `(0,0)` at the reference pixel. Set the direction conversion
to B1950 and the spectral conversion to CMB. The converted direction
coordinates then enter the frequency measures frame labeled as J2000.
The returned frequency is wrong regardless of the direction reporting
frame requested by the caller.

For the reference pixel's LSRK frequency of 1.415 GHz, the pinned
`image.coordmeasures` tool returns approximately 1,416,700,650.518 Hz in CMB,
1,414,142,155.337 Hz in LGROUP and 1,415,002,053.555 Hz in BARY. CASA's
independent measures tool reproduces these results when fed the mislabeled
coordinates. Both casa-rs and the independent conversion with the actual
J2000 direction agree within 0.001 Hz on 1,416,697,167.835 Hz,
1,414,138,758.484 Hz and 1,415,001,368.979 Hz respectively. The native
frequency context uses the physical position, independently of the
direction chosen for the returned record.

## The 45-degree image rotation fixture assumes prior floating-point rounding

The CASA 6.7.6.14 upstream `ia_rotate_test.test_basic` rotates `prerot.im`
by 45 degrees and compares every pixel with the archived `postrot.im`.
It requires a maximum absolute difference below 1e-21. Pinned CASA
reproduced the archive within 4.24e-22 on the validation host.

Both backends preserve the float32 image pixel type, 200-by-200 shape,
float64 Python chunk dtype, and exact 33,272-pixel validity mask. On
valid pixels, casa-rs differs from the archive by 9.52e-9 normalized
RMS and at most 5.96e-8. The largest difference is between adjacent
float32 values near 0.64. Returning those values in a float64 array
does not increase the accuracy of the stored pixels. The companion
test checks the image types, dimensions, mask, and numerical agreement
at float32 precision. The upstream assertion remains unchanged.

## Image statistics can report an extremum outside the sampled pixels

In CASA 6.7.6.14, `ia.statistics(region=...)` on a box starting at
`[1, 1, 0]` with stride `[2, 1, 1]` can report `maxpos=[2, 2, 1]`
for the maximum at image pixel `[3, 2, 1]`. The first coordinate omits
the region's sampling stride, so the reported position was never selected.
casa-rs maps extrema to absolute image pixels using the original stride.
The same reporting issue affects the plane indices written to a statistics
logfile. With a channel selection of `1, 3, 5`, CASA prints `1, 2, 3`.
casa-rs reports the actual selected channel indices next to their world values.

An unfiltered `ia.statistics()` on a 5×4×2 image created with
`linear=True` and pixel values 0 through 39 reports `npts=2` and `sum=1`,
even though `ia.getchunk()` reads the full image. Casa-rs measures all
40 pixels and reports `sum=780` and `trc=[4, 3, 1]`.

On a three-axis image, `ia.statistics(axes=[0, 0])` can also terminate the
process. casa-rs raises an error for the duplicate axis.

The pinned biweight algorithm can also terminate when every selected
pixel is masked. casa-rs returns a recoverable error for the empty sample.

## Image statistics can fail after dropping one direction axis

In CASA 6.7.6.14, create a Float image with shape `[9, 1, 1, 5]`, fill it
with `x + 10 * channel`, and call `subimage(dropdeg=True)` to retain one
direction axis and the frequency axis. `statistics` on an `LCSlicer` from
`[1, 1]` through `[7, 4]` with stride `[2, 2]` raises a WCSLIB `wcsp2s`
error while formatting the selected coordinates. The same region is
readable with `getregion`. Casa-rs returns the pixel statistics, including
the analytic sample count of 8 and sum of 192.

## Image statistics can use the wrong beam for a selected channel

On a direction, Stokes and spectral cube with distinct beams for I and Q,
select one channel and compute `ia.statistics(axes=[0, 1], region=...)`.
CASA 6.7.6.14 can divide both polarization sums by the beam area for I.
The selected channel is degenerate, and the beam lookup assigns the
polarization index to the channel dimension. casa-rs maps each result to
its original channel and Stokes beam.

## Image statistics can remove channels when selecting polarizations

In CASA 6.7.6.14, call `statistics(axes=[0, 1], region=...)` on a
`[3, 4, 8, 4]` direction, spectral and Stokes image. Select all eight
channels and only Stokes I and V with `regionmanager.frombcs`. CASA returns
arrays of shape `(6, 4)` because it removes channel planes 1 and 2 while
leaving the excluded polarizations in the result. Casa-rs returns shape
`(8, 2)` with the eight original channels and only I and V. It applies
each region exclusion to the corresponding display axis.

## Setting the pixels of a complex image can terminate CASA

On the pinned Linux build of CASA 6.7.6.14, `image.fromshape(shape=[3, 4],
type="c")` followed by `image.set(pixels=2)` terminates Python with SIGSEGV.
casa-rs raises `RuntimeError` and preserves the complex image. See
[#1625](https://github.com/bglenden/casa-rs/issues/1625).

## Legacy AW prediction needs a usable sensitivity image for PB correction

With CASA 6.7.6.14, the legacy `imager.ft` routes `awproject` and
`mawproject` predict the same visibilities on the original VLA fixture.
A `tclean` CFS cache works directly when `dopbgriddingcorrections=False`.
With correction enabled, the legacy imager fails in its sensitivity-image
lattice setup if the cache contains no `avgPB`. Supplying an `avgPB` image
with the model's axes permits the corrected prediction. The cold legacy
run on the same fixture fails while setting up its convolution cells.

The legacy writer emits the model image and the `AWProjectWBFT` name but
omits its convolution cache. Its own `ms.uvsub` reports `Unsupported
FTMachine`. Casa-rs retains the kernel images with the model. CASA can
open the backing images, and native `ms.uvsub` uses them after the source
cache has moved. The native reader reports an error if the saved recipe
has no retained kernel images.
[`imager_legacy_aw_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_legacy_aw_oracle.py) ·
[Calibration model outcome](https://github.com/bglenden/casa-rs/issues/1520)

## Legacy A/W can read beyond its channel-to-kernel map

With wideband legacy `awproject` or `mawproject` in CASA 6.7.6.14,
`selectvis(spw='0:0;3')` spans four visibility channels. `CFBuffer::initMaps`
overwrites each spectral window's map with the last range's entries. Here
the last range defines one entry. `AWVisResampler::GridToData` indexes the
map with the compact visibility channel number, leaving indices beyond
the first entry without a defined kernel selection.

Casa-rs follows the last range for indices within the map and looks up
the cell nearest the physical channel frequency for later indices.
Perturbing the two cache frequencies independently confirms that both
engines use the last range's kernel at index zero in either range order.
[`imager_legacy_aw_oracle.py`](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/imager_legacy_aw_oracle.py)

## `image.insert` aborts for a source that misses along one axis

CASA 6.7.6.14 terminates with SIGABRT and `Slicer error:end<start` for a
`[2, 2]` source inserted into a `[4, 4]` target at `locate=[10, 1]`
or `[-10, 1]`. One axis overlaps. The other yields an empty clipped interval.
The casa-rs tool raises a Python `RuntimeError` before writing pixels or mask
bits.

## `image.insert` can replace unread source pixels during a self-insert

Create a native float image from a 1510 by 1510 ramp in NumPy row order and
insert the source region `box([0, 0], [1508, 1508])` into the same image at
`locate=[1, 1]`. CASA 6.7.6.14 returns success and persists 27,063 wrong
pixels. For example, `(2, 152)` contains 150 instead of the original value
1661 from `(1, 151)`. The cursor writes overlap pixels that a subsequent
cursor reads. casa-rs snapshots the selected pixels and validity before
writing, using scratch storage when the overlap exceeds its memory budget.

## A non-finite scalar coordinate order can terminate CASA

Create a one-axis Linear coordinate system and pass a bare `float("inf")`
to `transpose` or `reorder`. Both calls terminate the CASA 6.7.6.14 process
on macOS arm64. With `[float("inf")]`, CASA raises its ordinary axis error.
Casa-rs applies the arm64 axis conversion to either form and rejects the
axis with an ordinary Python exception, preserving the coordinate system.
See [#1626](https://github.com/bglenden/casa-rs/issues/1626).

## `coordsys.setrestfrequency` can store an infinite frequency

CASA 6.7.6.14 accepts `cs.setrestfrequency(float('inf'))` and writes an
infinite active rest frequency. That value cannot produce meaningful Doppler
velocities. casa-rs raises `RuntimeError` and retains the previous frequency
list, active frequency and coordinate transform.

## `coordsys.setrestfrequency` can crash on a whitespace-only string

CASA 6.7.6.14 terminates with `SIGSEGV` when `cs.setrestfrequency('  ')`
tokenizes the string and tries to read the first quantity from an empty
list. casa-rs raises `RuntimeError` and retains the existing list.

## `coordsys.setreferencelocation` can crash when an axis is masked

On a two-axis linear coordinate, CASA 6.7.6.14 terminates with `SIGSEGV`
for `cs.setreferencelocation([2, 3], [8.0, 9.0], [True, False])`. The same
failure can occur with a Stokes axis because the method excludes Stokes
from the edit. casa-rs changes both reference values on each enabled
non-Stokes axis and preserves both values on the other axes. It publishes
the updated coordinate state only after the complete edit succeeds.

## `imager.ft` can crash after a refused duplicate image with a new component

With the same live CASA 6.7.6.14 imager, predict an image, then submit that
image again together with an unused component list. `ft` reports a duplicate
image. A subsequent call with just the component reports that the component
is already present, and a no-input `ft()` can terminate with `SIGSEGV`.
casa-rs refuses the mixed duplicate request before adding the component.
The same imager can repeat the original image prediction and can subsequently
add the component alone.

## Coordsys relative conversions can abort on missing values

On a two-axis coordinate system, CASA 6.7.6.14 aborts for
`cs.toabs(isworld=0)` and `cs.toabs(isworld=1)`. casa-rs interprets the
omitted values as zero offsets and returns the reference position in the
requested domain. CASA also aborts when `toabsmany` or `torelmany` receives
a matrix with fewer rows than the number of coordinate axes. casa-rs
raises `RuntimeError` for these matrices.

## Coordsys spectral velocity conversions can crash on missing input

In CASA 6.7.6.14, calling `frequencytovelocity` or `velocitytofrequency`
with an empty vector on a spectral coordinate exits with `SIGSEGV`.
Calling `velocitytofrequency([1.0])` without a spectral coordinate also
exits with `SIGSEGV` when it looks up the default frequency unit. casa-rs
returns an empty `float64` array for empty vectors on a spectral coordinate
and raises `RuntimeError` when the spectral coordinate is missing.

## Coordsys Doppler conversion can misread byte-swapped arrays

On a little-endian host, CASA 6.7.6.14 treats a big-endian `float64` NumPy
array such as `np.array([1.0, 2.0, 3.0], dtype='>f8')` as native-endian
bytes in `frequencytofrequency`. The values become tiny subnormal numbers
even at zero velocity. casa-rs respects the array dtype's byte order.

## Coordsys linear transform input can abort or make a record unreadable

CASA 6.7.6.14 can abort in `setlineartransform('linear', value)` on a
three-dimensional numeric array. It can also accept a zero transform for
a sampled tabular or spectral coordinate, then write constant lookup
values that its record reader refuses to reopen. casa-rs raises
`RuntimeError` before editing the coordinate in these cases. It also
rejects non-finite matrix coefficients and reads byte-swapped arrays
according to their declared byte order.

## Coordsys tabular samples can leave an unreadable coordinate

CASA 6.7.6.14 accepts `settabular([0, 0, 2], [5, 6, 7])` and then raises
`Interpolate1D::operator() data has repeated x values` in `torecord()`.
The interpolator also treats adjacent pixels within `1e-13` as repeated.
It also accepts nonfinite samples, which can turn even finite input
positions into `NaN`. casa-rs rejects these samples before changing the
coordinate. Finite one-sample tables still define constant mappings.

CASA also replaces the samples when either argument is a tuple, then raises
a Python `SystemError` after the replacement. casa-rs raises the Python
error before changing the coordinate.

## `image.statistics` can overwrite a logfile omitted from the current call

With CASA 6.7.6.14, call `ia.statistics(logfile='report.txt')`, replace
the file contents, then call `ia.statistics(append=False)` on the same
image tool. The second call truncates the previously selected file even
though `logfile` was omitted. casa-rs uses a logfile only on calls that
explicitly supply its path.

## `image.rebin` can raise after writing an output with tuple factors

CASA 6.7.6.14 writes the named output of `ia.rebin("output.im", (2, 2))`
and then raises a Python `SystemError`. A list with the same values succeeds.
casa-rs reads the factors as integers and returns the rebinned image for
either sequence.

## Coordinate setters and measure input with a removed world axis

CASA 6.7.6.14 accepts a two-axis linear coordinate record with
`worldmap0=[-1,0]` and both pixel axes retained. Its reference measure
correctly retains both local quantities. Passing that measure to
`coordsys.topixel` aborts the process. Giving the same record to
`coordsys.setreferencevalue` with `type="linear"` also aborts when the tool
addresses the removed world slot. The typed setter can abort for a
direction coordinate with the same map.

casa-rs decodes the complete measure and uses the coordinate record's
replacement when conversion needs the hidden world component. A typed
numeric edit accepts the visible components in local order or the full
local vector. Edits from either representation update visible axes and
retain the hidden value and its replacement. The mapped inspection and
conversion tests exercise these cases. See #1002 and #1819.

## Typed coordinate units with different pixel and world orders

CASA 6.7.6.14 indexes the world-unit vector with pixel positions in
`coordsys.units(type=...)`. In a record whose direction pixel axes are
`[1,0]` and world axes are `[0,2]`, CASA reports direction units
`["","rad"]` and Stokes units `["rad"]`. The empty unit belongs to Stokes.
casa-rs uses the coordinate's world axes and reports `["rad","rad"]`
for direction and `[""]` for Stokes. See #1002 and #1819.

## FITS import with independently placed spectral and Stokes axes

CASA 6.7.6.14 writes a two-axis image with Stokes followed by frequency
in pixel order and frequency followed by Stokes in world order. The
FITS PC matrix has zero diagonal and unit off-diagonal terms. The
same CASA build segfaults in `image.fromfits` when opening its output.

casa-rs pairs the world rows with their pixel columns using the PC
matrix before parsing the local coordinate models. Imported pixels
keep the FITS array order and the native record retains the original
world order. The regression reads files written by CASA and by casa-rs
and independently checks which pixel step moves each world coordinate.
See #1002 and #1823.

## Coordinate subimage with a sampled frequency axis in units other than Hz

CASA 6.7.6.14 resamples a tabulated spectral axis in the input coordinate's
display units, then uses those values to construct a coordinate in Hz. For
example, a sample at `1020 MHz` becomes `1020 Hz` in the returned record.
The rest frequency also loses its unit scale. casa-rs converts the samples
and rest frequency to Hz before building the subimage record.

An explicit negative length for a sampled spectral axis can abort CASA
when the signed length is used for an unsigned allocation. casa-rs raises
`RuntimeError` before allocating sample storage.

When the spectral axis is in different positions in pixel and world order,
CASA indexes the world increment vector with the spectral pixel position.
Cropping a `[10, 11, 13, 18] Hz` table to its final two pixels can change a
linear axis's increment from `2` to `5` and leave the spectral samples at
`[13, 15.6667] Hz`. casa-rs preserves the linear increment and stores the
sampled spectral values `[13, 18] Hz`.

CASA also reads byte-swapped NumPy origin arrays as native-endian bytes.
For example, `np.array(1.5, dtype=">f8")` applies a zero-pixel shift on a
little-endian host. casa-rs reads the declared byte order.

## FITS export shifts the zenithal polynomial coefficients

CASA 6.7.6.14 starts the `ZPN` coefficients at latitude-axis `PV i_1`
when it exports an image. FITS WCS defines the constant coefficient at
`PV i_0`. For a polynomial beginning `[0.02, 1, 0.1, -0.02]`, reopening the
export in CASA changes the sky positions. A zero constant can cause CASA
to reopen the file as a linear coordinate. casa-rs writes the coefficients
from `PV i_0`. Both readers recover the original sky positions from that
header.

## A rejected latitude edit can leave a projected coordinate unusable

In CASA 6.7.6.14, a CEA coordinate with zero reference latitude and
`LONPOLE=0°` cannot move its reference to `-0.5` radians with the same
pole. `coordsys.setreferencevalue` raises for this edit but leaves the new
latitude in the coordinate record. The same failure occurs for other
projections when the requested latitude cannot use the existing pole.
casa-rs checks the pole before committing the reference edit and preserves
the usable coordinate if the edit fails.

## A display frame can shift an adjusted native coordinate reference twice

CASA 6.7.6.14's `coordsys.setreferencecode` can pass the displayed
reference position into the conversion as if it were still in the native
frame. For a J2000 reference of `[24°, 32°]`, adjusting to ECLIPTIC
gives about `[0.597567, 0.356488]` radians. With a Galactic display layer
active, it instead gives about `[2.582935, -0.784427]` radians. An active
BARY display layer also changes the result of an LSRK-to-GEO adjustment
of the same spectral reference pixel. casa-rs uses the native direction
and native reference-pixel frequency, then resets the display layer on
the coordinate whose native code changed.

## A rejected combined conversion-frame request changes the direction frame

CASA 6.7.6.14 applies the direction conversion in `coordsys.setconversiontype`
before it validates the requested spectral frame. With `direction="GALACTIC"`
and `spectral="invalid"`, the call raises after changing the direction
conversion. casa-rs preserves both conversion layers when the call fails.

## Rebuilding a sampled spectral axis can relabel its rest frequency

CASA 6.7.6.14 passes the old rest-frequency number to the new `setspectral`
table in Hz. If the old coordinate declares MHz, a rest frequency of
`1420.405751786 MHz` becomes `1420.405751786 Hz`. casa-rs converts the active
rest frequency to Hz before building the new frequency or velocity table.

The same setter accepts a positive rest quantity with incompatible units,
such as `21cm`, and records it as `0.21 Hz`. It can also store an infinite
rest frequency. casa-rs rejects these inputs while preserving the prior
spectral coordinate.

## Reflected Quality pixels keep their original labels in CASA

For a Float image of shape `[3, 2]` with a linear axis and a Quality axis
ordered `DATA, ERROR`, CASA 6.7.6.14's `image.transpose(order="-10")` places
the error pixels first but labels them `DATA`. casa-rs reverses the labels
with the pixels and retains their association after reopening the image.

## FITS export can abort on an image with a native Quality coordinate

Exporting a Float native image with a two-plane Quality coordinate
(`DATA` and `ERROR`) with `image.tofits` terminates pinned CASA 6.7.6.14
with a floating-point exception. With the Quality axis removed from the
public layout and the `ERROR` plane fixed in the record, export terminates
with a segmentation fault. casa-rs raises a Python error and preserves the
image in both cases. Its single-HDU FITS writer cannot represent the data
and error semantics of a native Quality coordinate.

## Mixed conversion of a nested coordinate system can crash

CASA 6.7.6.14 restores a two-axis Linear coordinate system stored as an
outer `coordsys0` coordinate. With child units `m` and `s`, both
`coordsys.convert([6.0, 7.0], unitsin=['pix', 'pix'], unitsout=['cm', 'ms'])`
and the equivalent `convertmany([[6.0], [7.0]], ...)` segfault. Casa-rs
raises a Python error and leaves the coordinate record usable. Scalar and
batched `toworld` and `topixel` continue to use the nested transform.

The same CASA version can also segfault in `coordsys.summary(list=False)`
when a nested Linear system retains two pixel axes but its inner
`worldmap0` removes one world axis. Casa-rs reports the incompatible
axis counts and leaves the coordinate tools usable for transformation.

## An empty nested list crashes `coordsys.convertmany`

CASA 6.7.6.14 exits with a segmentation fault when a two-axis coordinate
system receives `convertmany([[], []])`. The equivalent NumPy array with
shape `(2, 0)` returns an empty double array of that shape. casa-rs returns
the same empty array for both representations.

## Invalid slice indices can terminate `image.getslice`

On a `[7, 8, 3]` image, CASA 6.7.6.14 aborts on `getslice([0, 2],
[0, 2], coord=[0, 0, 100])`. Its axis-vector reader also accesses the
second entry when an explicit list contains only the first axis. casa-rs
raises a Python error for an out-of-range fixed coordinate or a missing
second axis and preserves the attached image. An explicit `npts=None` also
aborts CASA. casa-rs raises a type error for that argument.

## `image.transpose` can change the metadata of a reflected sample

In CASA 6.7.6.14, reflecting an axis negates the corresponding world
increment. In a `[4, 6, 5]` image with a direction matrix
`[[1, 0.2], [-0.3, 1]]`, `transpose(order="2-01")` moves the sample at
`[1, 2, 3]` to `[3, 2, 2]` and changes its reported direction. casa-rs
reflects the matching pixel column of the coupled transform and preserves
the sample's world position.

For a `[4, 5, 2, 6]` image, `transpose(order="-30-21")` reverses the
spectral and Stokes samples but retains the original beam grid and Stokes
labels. casa-rs moves each sampled plane with its original label and
restoring beam.

For a six-pixel tabular axis with pixels `[0, 1, 3, 5]` and worlds
`[100, 101, 111, 120]`, the reflected pixel 1 represents the original
pixel 4. CASA reports 119 there. casa-rs reflects the lookup grid and
reports the original interpolated value of 115.5.

If an image has pixel axes `[Stokes, Frequency]` and world axes
`[Frequency, Stokes]`, CASA 6.7.6.14 reflects Stokes pixels for the named
order `["-Frequency", "Stokes"]`. casa-rs resolves the names through the
world-to-pixel mapping, so this order reflects the frequency planes.

## Unequal image-section strides can shift coupled sky coordinates

On a SIN direction plane with reference pixel `[0, 0]` and PC matrix
`[[0, -1], [1, 0]]`, CASA 6.7.6.14 applies a stride of `[2, 1]` by doubling
the first world increment. It reports about one arcminute of declination change
between output pixels `[0, 0]` and `[1, 0]`. The selected input pixels
`[0, 0]` and `[2, 0]` are about two arcminutes apart. This affects the
section geometry used by `image.subimage`, `image.pad`, and `image.pbcor`.
casa-rs scales the PC matrix with the increments so that each selected
pixel retains its sky position.

## Fluxscale dictionaries can select the wrong windows in `setjy`

CASA 6.7.6.14 documents `spw` as a selector for valid windows in a
`fluxdict`. Its Python wrapper calls `ms.msseltoindex` on the class when
`spw` is explicit, raising `AttributeError` before writing. With an implicit
selection, all `fluxd=-1` and a nonzero fitted flux, the wrapper passes an
empty window list. The imager interprets it as every window and can replace
an existing model. casa-rs applies the intersection of the selected window
IDs and the valid dictionary entries. An empty intersection leaves the
scratch and virtual models intact.

A `fluxscale(fitorder=0)` dictionary can also contain only the fitted
intercept in `spidx`. CASA's wrapper indexes the first element after
removing that intercept and can raise `IndexError`. casa-rs uses zero
spectral index for this constant fit. The local oracle compares the
corrected cases with equivalent CASA manual models and checks preservation
of the input model. [#1519](https://github.com/bglenden/casa-rs/issues/1519)

## Listing the Butler–JPL Horizons 2010 objects in setjy raises before displaying them

With `listmodels=True` and `standard="Butler-JPL-Horizons 2010"`, CASA
6.7.6.14 calls an unavailable `logsink.posta` method and raises
`AttributeError`. The object names are already defined in the task.
casa-rs displays that list and returns the empty listing result without
opening a MeasurementSet. [#1519](https://github.com/bglenden/casa-rs/issues/1519)

## A resolved setjy model uses the wrong manual spectral law

In CASA 6.7.6.14, `setjy(standard="manual", model="3C286_L.im",`
`fluxdensity=2.4, spix=-0.74, reffreq="1.41GHz")` writes zero
visibilities in the 1.413 GHz VLA band. With `spix=0`, `scalebychan=True`
and the default 1 GHz reference, CASA incorrectly gives the image a
frequency-dependent amplitude. The resolved-image branch evaluates the
spectral polynomial at a frequency in Hz and uses the result as a power
of the frequency ratio. casa-rs evaluates the documented polynomial in
`log10(f / f0)`, as used by the manual point-source model. It normalizes
the existing image morphology to that intensity per channel or at the
representative frequency for constant scaling. [#1519](https://github.com/bglenden/casa-rs/issues/1519)

## CASA uvsub drops the second feed basis of a resolved virtual model

In CASA 6.7.6.14, an MS with RR/LL on one window and XX/YY on the next can
hold two valid `SetJyGridFT` records for a resolved IQUV image. `ms.uvsub`
uses the first window's circular model and subtracts zero on the linear
window, even when CASA itself wrote both records. Scratch `setjy` predicts
nonzero visibilities on both windows. casa-rs reads either writer's
records and applies the matching Fourier grid on each window.
[#1519](https://github.com/bglenden/casa-rs/issues/1519)

## getcalmodvla rejects a documented calendar reference date

CASA 6.7.6.14 accepts a `YYYY-MM-DD` observation date but raises
`TypeError` for the same `refdate` format. It parses the date into MJD
and then tests the original string against zero. casa-rs uses the
parsed MJD to decide whether to include `refdate` in the request and
keeps the supplied calendar date in the component list provenance.
[#1519](https://github.com/bglenden/casa-rs/issues/1519)

## Convolution can produce isolated infinities in an otherwise undefined plane

CASA 6.7.6.14 spreads an unmasked NaN through the FFT axes in
`image.convolve`. The validity mask and planes along degenerate kernel
axes are independent. For an unmasked infinity, a four-pixel input
`[1, 1, 1, inf]` with kernel `[1, 0, 0]` and `scale=1` produces
`[nan, nan, inf, nan]`. The surviving infinity depends on the transform
and padding. Casa-rs returns NaN throughout the affected plane when
convolution mixes a valid nonfinite value, so the result is consistent
across direct accumulation, FFT blocks, and tile sizes. A kernel with no
active axes still applies the scalar coefficient with IEEE arithmetic.
Masked samples contribute zero and the output retains the selected input
validity.

## Solar System setjy drops a scan or time restriction

CASA 6.7.6.14 uses the requested scan to choose the epoch for
`setjy(standard="Butler-JPL-Horizons 2012")` but omits the scan when writing
`MODEL_DATA`. Even when one scan has been selected, the task replaces
existing models in other scans of that field and spectral window. With a
time range, it writes the correct rows but calculates the spectrum at the
first timestamp outside the requested range because it passes `timerange`
where the CASA tool expects `time`.

casa-rs uses the scan and time selection both for the model epoch and for
writing. A pinned comparison checks the selected scan against CASA and
verifies that unselected model cells retain their previous values. A
second comparison physically selects the requested time range in the CASA
reference MeasurementSet and checks both the returned flux and complex model
visibilities.

## Solar System setjy changes the integrated flux for reversed channel widths

CASA 6.7.6.14 passes the two channel edges in reverse order to its
brightness integrator when `CHAN_WIDTH` is negative. For a Titan channel
centered at 345.9 GHz with a 1 GHz bandwidth, the reported flux changes
by more than ten percent when only the width's sign is reversed. A
one-channel spectrum also changes its reference edge.

casa-rs integrates the physical band from its lower to its higher
frequency. A paired one-channel check compares both signed widths with
CASA's positive-width result, including the model published to the
MeasurementSet.

## setjy fails to read an observation selector supplied as a string

CASA 6.7.6.14 assigns its local observation selector only for non-string
values when `usescratch=True`. For example, `observation="1"` raises before
selecting a row, while `observation=1` reaches the same documented selector.
casa-rs resolves either form. The 2012 Solar System comparison uses the
integer form in CASA and the string form in casa-rs against an MS with two
observatory rows.

## imager.setjy can abort when the manual reference frequency is omitted

CASA 6.7.6.14 can terminate the Python process when
`imager.setjy(fluxdensity=[5, 0.6, 0.2, 0.1], standard="Perley-Butler 2017")`
uses the declared empty `reffreq` default. The binding constructs a spectrum
with a zero reference frequency, which the component library rejects.

casa-rs supplies the task's 1 GHz reference when the tool parameter is
omitted. The fixed-flux comparison uses that explicit frequency in CASA
and the default in casa-rs.

## An empty imager.setjy selection can widen the next ft

In CASA 6.7.6.14, `imager.setjy(field="0", scan="999", ...)` returns
a format-only record when the scan matches no data. The call also
rebuilds the visibility iterator over the full MeasurementSet. A subsequent
`ft` can then replace model cells outside the preceding `selectvis`.

casa-rs preserves the existing selection when the new scan matches no rows.
A paired test verifies the format-only return, unchanged model cells for the
empty call, and the scope of the following `ft`.

## Image decomposition counts masked emission and fits empty backgrounds

CASA 6.7.6.14 counts masked samples in `image.decompose`. On a 9 × 11
Float image with peaks 7 at [1, 2], 1 at [3, 4], and 4 at [5, 6],
`decompose(simple=True, threshold=1, fit=False)` returns three islands
after its default mask excludes [1, 2]. An expression mask accepting
only the 7 peak also leaves all three bounds. The native
decomposer reads raw pixels from the selected subimage. Casa-rs combines
the default and requested masks with the selected region before
thresholding so excluded samples cannot become sources.

On an 11 × 13 zero image, CASA reports zero regions with
`decompose(simple=True, fit=True, threshold=0.7)`, then starts fitting
143 components to the background. Casa-rs returns empty component and
bound matrices when the detection set is empty.

A fit can also be underdetermined. Three samples with values 9, 3, and 1
at [7, 7], [8, 7], and [8, 8] constrain fewer than the six parameters
returned for a 2D Gaussian. With `threshold=0.7`, `maxretry=0`, and
`maxrms=0.0001`, each implementation reconstructs all three samples to
within 0.00001. The independent solvers can select different centers,
widths, and angles from the valid solutions. Casa-rs also uses a fixed
retry sequence so fits with identical inputs are reproducible.

### predictcomp leaves requested figures unwritten

With CASA 6.7.6.14, a valid `antennalist`, `showplot=True` and a nonempty
`savefig` return a component list and `savedfig=None`. The task produces
no figure. This also leaves the documented marker, axis and legend controls
without an effect. casa-rs projects the specified array at the observation
epoch, evaluates the model's visibility amplitudes at the requested
frequencies, and renders the requested figure.

### predictcomp 2012 scales the disk position angle as arcseconds

The Horizons 2012 helper passes the ephemeris `NP_ang` model angle, in
degrees, to `componentlist` as arcseconds. For Jupiter at MJD 60000 this
stores an angle of about 0.0931 degrees instead of about 335.11 degrees.
casa-rs uses the ephemeris angle in degrees, as for the 2012 `setjy` model.
This preserves the projected disk orientation when the table is used to
predict visibility amplitudes.

### predictcomp can reuse stale task output for a rounded epoch

The CASA task removes a previous component list using a filename formed
from the full Python frequency and epoch strings. The imager generates
its filename with six significant digits. For repeated 3C286 predictions
at MJD 58000.25 with the same first frequency, changing the final
frequency can leave the old spectrum in place while the task reports the
new frequency grid. The casa-rs task replaces a prior component list at
the generated path after completing its new table.

## A circular deviation window with an all-true footprint can abort

On a 15 × 17 Float direction image, CASA 6.7.6.14 terminates the Python
process with a segmentation fault during
`image.deviation(xlength=3, ylength="", stattype="npts")`. The 1.5-pixel
radius includes all nine pixel centers of the bounding square, so there
is no footprint mask. The diagnostic assumes it exists when reporting
the circle size. Casa-rs computes the documented circular statistic
and clips the window at the image's edges, returning counts of 9 in
the interior, 6 at an edge, and 4 in a corner for an unmasked image.

## Image FFT can use the wrong explicit axis or abort on a coupled coordinate

CASA 6.7.6.14 indexes its array of requested FFT axes with the requested
axis value itself. On a 7 × 8 linear image, `image.fft(axes=[1], complex=...)`
can transform the first axis. The requested value 1 is outside the one-entry
list. Casa-rs transforms the requested second axis and rejects axes outside
the image's rank before creating outputs.

CASA can also terminate the process when only one axis of a coupled
linear coordinate is selected. A 7 × 8 image with linear transform
`[[1, 0.2], [0.5, 1]]` and `fft(axes=[0], complex=...)` reproduces it.
The linear-coordinate Fourier operation rejects the partial inversion,
after which the tool dereferences the absent coordinate. Casa-rs returns
the coordinate error and leaves the output destinations untouched.

For a Fourier transform of a linear frequency or time coordinate, CASA
6.7.6.14 labels the output in seconds or hertz and uses the input's
unconverted increment. A four-pixel axis with 2 MHz spacing produces
`0.125 s`, while the same axis expressed as 2,000,000 Hz produces
`1.25e-7 s`. A four-pixel axis spaced by 2 ms likewise produces
`0.125 Hz` instead of 125 Hz. Casa-rs converts the spacing to hertz or
seconds before taking the reciprocal so equivalent inputs give the same
Fourier coordinates.

### `image.findsources` can abort on a transposed plane or an invalid source count

On a Float image of shape `[21, 23]` with a compact off-center source,
CASA 6.7.6.14 finds the source in the original sky plane.
After `transpose(order="10")`, the default point search aborts with a heap
error in a separate process. `SkyComponentFactory::encodeSkyComponent`
swaps flux and the first position on a latitude-first sky plane and accesses
the sixth parameter even for a three-parameter point source. A transposed
four-axis image with two singleton axes can instead return an incorrect sky
position. Casa-rs maps both pixel positions and ellipse endpoints through the
direction coordinate's axis order. The search area still uses the selected
image's pixel order.

A search on a Float image with an unselected third nondegenerate axis aborts
with `Slicer error:IPosition-lengths differ` in CASA, which uses a
two-dimensional slice after dropping singleton axes. Casa-rs requires a
two-dimensional sky plane and reports a Python error before reading pixels if
more than two nondegenerate axes remain. Selecting one channel or polarization
before the search still works.

`nmax=0` crashes CASA because the search accesses an empty result matrix.
Casa-rs returns an empty component list. A negative count aborts on CASA's
negative matrix dimension and raises a Python error in casa-rs.

The first two mask rows initialize `inDone` from the mask instead of its
inverse. On a 16 × 17 Float image with a compact peak centered at row 1 or
2, CASA finds no source with an all-true mask. When the first two rows are
masked off, CASA reports the source anyway. Casa-rs preserves the regular
scan starting at row 3 for both masks, so rows excluded by the mask cannot
contribute to these detections.

For a native J2000 sky coordinate with its conversion type set to Galactic,
`findsources` returns Galactic longitude and latitude, approximately
`(1.68201, -1.05039)` radians for an off-center point in a synthetic image
centered at J2000 zero. CASA tags these values as J2000, so a component list
using that record interprets a different sky position. Casa-rs tags the
angles with the frame used to compute them. Gaussian directions and their
position angles are computed in that same frame.

### `image.fitcomponents` writes fitted estimates in the wrong pixel grid

In CASA 6.7.6.14, fitting a Gaussian centered at pixel `[14.2, 17.7]` in a
31 × 34 image with the default sky coordinates reports the correct
`pixelcoords` in the fit record. The same call writes `14.9998, 17.0002`
in the `newestimates` file. `ImageFitterResults::writeNewEstimatesFile`
inserts the component direction in radians into a coordinate system whose
world values use the visible direction units. Casa-rs writes the fitted
center in the original input pixel grid, which is what the estimates-file
reader expects.

The detailed fit record also inherits the direction-label mismatch described
for `findsources`. With a J2000 sky coordinate and a Galactic conversion
frame, CASA returns the fitted direction near `(1.68200, -1.05040)` radians
tagged J2000. Casa-rs labels the fitted direction with its conversion frame.

The optional summary labels the frequency column `GHz` but writes raw world
values. The default spectral axis reports channel 1 as `1.415e+09` under that
heading, while the component's frequency record correctly reports
`1.415 GHz`. Casa-rs converts the summary column to GHz.

A masked value of 100 next to a 7 Jy/pixel Gaussian gives a reported peak
uncertainty of 1.7604 Jy/pixel without a `residual` output in CASA 6.7.6.14.
Requesting that output gives about `3.4e-9 Jy/pixel` for the same fit. CASA
uses all entries in its anonymous residual image for the first RMS and
honors the source mask for the second. Casa-rs computes fit uncertainties
from valid pixels whether or not a residual image is written.

When fitting Stokes Q or U in a multi-Stokes cube, CASA 6.7.6.14 reports the
selected polarization's fitted flux but writes its model and residual pixels
to the first polarization plane. A rank-five image with an ancillary axis
selected at pixel 1 shows the same mismatch. The output write position is
initialized to zero and only the spectral position is updated. Casa-rs
writes the fitted result to the selected polarization and ancillary position.
The input grid and validity mask remain present in both requested images.

### `image.fitprofile` can mislabel or leave part of a fitted spectrum

In CASA 6.7.6.14, collapsing the spatial axes of a cube centered on
J2000 zero can place the averaged spectrum near the antipode. On a
5 × 7 × 27 synthetic line cube the direction record reports a right
ascension of 12 hours. With default detailed logging the inverse
projection can fail and the call raises even though the spectral fit
converged. Casa-rs uses the original direction coordinate evaluated
at the selected spatial pixel midpoint, including for its model and
residual products.

In the same process, fit an 11-channel spectrum with a model image and
then reject a fit to a 23-channel spectrum using an amplitude range.
CASA's failure buffer keeps the first call's 11-channel shape. Only the
first 11 channels of the rejected model are NaN and masked. A later
shorter fit can instead attempt a slice larger than its output.
Casa-rs uses the current spectrum's shape to mark every rejected
channel invalid. Profiles skipped by `minpts` still retain the input
pixels and mask in the products.

A log-transformed polynomial with every coefficient fixed crashes
CASA during fitting. Casa-rs evaluates the given coefficients in log
space and reports their zero parameter errors without trying to estimate
a covariance. Pixels outside the log function's domain remain masked.

For an explicit direction-axis fit on a spectral cube, CASA rejects
the fit with `The X vector must have some elements` even though the
box supplies 23 pixels on the requested axis. The spectral-axis branch
precomputes its abscissa and the alternate-axis branch fails to supply
one. Casa-rs derives the fit abscissa from the requested pixel axis and
reports its center and width in that axis's world units.

With a Galactic conversion frame on a J2000 image, the angles in the
`fitprofile` direction record are Galactic but CASA labels and formats
them as equatorial. Casa-rs reports the conversion frame and displays
its longitude and latitude in degrees.

CASA calculates the reported integral uncertainty by multiplying
relative errors by the signed line integral. A fitted absorption
line therefore has a negative uncertainty. Casa-rs preserves its
negative integral and reports the standard error as a nonnegative
magnitude.

For a Gaussian multiplet with tied peaks of 6 and 3 and an accepted
amplitude range of 4 to 7, CASA accepts both peaks because its
constraint loop repeatedly examines the first peak. Casa-rs checks
the range against every component of the multiplet.

A synthetic Gaussian centered at channel 15 with its peak and width
fixed gives a different center in CASA after its peak and sample values
are scaled from 1 to 10⁻¹⁵. The velocity center shifts by about
0.0175 km/s. Casa-rs scales the solver's damping to its curvature and
fits the same physical center at both intensities.

### `image.getprofile` can return values on a different axis than requested

On a 4 × 5 × 1 × 7 image, CASA 6.7.6.14 returns the right pixels for
a region selecting channels 1 through 5, but with `frame="BARY"` its
reported frequencies come from channels 0 through 4. The conversion
uses the original coordinate system with channel numbers starting at
zero. Casa-rs converts the actual selected channels and pairs their
coordinates with the corresponding reduced samples.

A declination profile on the same image can fail an
`AlwaysAssert newWorldOrder.nelements() == nWorldAxes()` when a world
unit is requested. Casa-rs keeps the selected declination axis and
evaluates its coordinates along the spatially collapsed image grid.

With `spectype="beta"` and `unit="m"`, CASA reports its unchanged
frequency values while labeling them as meters. Casa-rs rejects
relativistic velocity with a wavelength unit, as it does for radio
and optical velocity.

For a region retaining channels 1, 3 and 5, CASA returns samples at
those channels but numbers the `planes` array 1, 2 and 3. The same
contiguous numbers appear in `coords` when pixel units are requested.
Casa-rs reports the original sampled pixel numbers.

### `image.histograms` can abort on duplicate axes

On a 5 × 3 Float image with `axes=[1, 1]`, CASA 6.7.6.14 aborts
inside the lattice collapse. It checks each axis in isolation and
passes a duplicated axis to the tile iterator. Casa-rs raises a Python
error for duplicate axes and keeps the image attached and its pixels
intact.

### `image.maxfit` can select the wrong channel or crash on an all-masked image

On a direction cube with peaks of +6 in one channel and +2 and −12 in
another, CASA 6.7.6.14 returns the weaker positive source with
`negfind=False` and the +6 source with `negfind=True`. The
plane-selection branch applies the opposite sign policy to the one
documented, and the subsequent source search can miss a negative peak.
Casa-rs ranks positive peaks by value by default and ranks both signs
by magnitude with `negfind=True`, retaining the fitted peak's sign.
It ranks fit-eligible peaks by their sampled pixel values. A broad
source with a larger fitted height cannot replace a sharper source
whose sampled peak is stronger. The fit can resolve an extremum
between two equally bright adjacent pixels.

### `image.moments` can report an extreme channel in the wrong Doppler convention

On a spectral cube with nine monotonically increasing channel intensities,
a rest frequency of 1.1 GHz and observed frequencies near 1.415 GHz, CASA
6.7.6.14 uses optical velocities for the weighted coordinate with
`doppler="OPTICAL"`. The median, maximum and minimum coordinates still
use radio velocities. Casa-rs converts the selected channel to the
requested Doppler convention for every coordinate-valued moment.

On a one-axis tabular image with world coordinates `[3, 4, 10, 14, 64, 80]`,
a region retaining pixels 1, 3 and 5 should carry world coordinates
`[4, 14, 80]`. CASA 6.7.6.14 uses different coordinates for the
weighted and maximum-coordinate moments and the output reference
pixel. Casa-rs preserves the world coordinates evaluated at the
original selected pixels.

Collapsing the Stokes axis of a cube with distinct restoring beams
per polarization raises a restoring-beam error in both implementations.
CASA can create the named output directory before it raises. Casa-rs
checks the beam count before publishing any output image.

With a Gaussian smoothing width of zero and an inclusion interval,
CASA can produce a moment image with no valid samples because the
smoothing kernel is undefined. Casa-rs rejects the zero width before
allocating or publishing an output.

With automatic noise estimation and an all-zero spectrum, the CASA
noise histogram can fail to fit. Its caller then uses a noise variable
that the fit did not assign. The pinned build masks the zero spectrum
for `method=["fit"]` and `method=["window", "fit"]`. Casa-rs rejects a
spectrum with no signal before calculating its SNR. Plain clipping
still returns a valid zero.

On an entirely masked Float direction image, default `image.maxfit`
can dereference an absent maximum and terminate the CASA Python
process. Casa-rs raises a Python error and preserves the image.

### `image.pv` can reject a strip with reversed endpoints

In a `[24, 26, 2, 5]` Float cube with the default sky coordinate,
CASA 6.7.6.14 accepts `start=[4, 7], end=[16, 7]`. Interchanging the
endpoints enters the 180-degree rotation path and raises a no-overlap
error even though the strip lies entirely inside the image. Casa-rs
samples the strip in the specified order, yielding the same values
in reverse along the offset axis.

With the same input axes transposed to `[Spectral, Dec, RA, Stokes]`,
CASA raises a coordinate assertion at output creation. Casa-rs writes a
readable result with axes `[Spectral, Offset, Stokes]` and the same
sampled values projected into that order.

When the native world-axis map is permuted independently of the pixel
axes, CASA removes the world axis with the same *index* as the collapsed
pixel axis. Some permutations reorder the output, omit an unrelated
coordinate such as Stokes, or fail an assertion. Casa-rs removes the
world axis associated with the collapsed sky axis and retains the order
of the other world axes.

In a diagonal strip centered exactly on a half pixel, CASA rounds the
two transverse endpoints separately. On a default `[60, 52, 1, 3]`
cube, `[8, 9]` to `[45, 40]` includes both equidistant rows with
`width=1`. Changing the second image axis to length 54 changes the
selection to one row. Other half-pixel centers can construct an
inverted region and fail. Casa-rs includes both equidistant boundary
rows, making the cut independent of low-order rounding and image
extent outside the strip.

### `image.regrid` can extrapolate just outside an integer-aligned region

On a default `[31, 33, 1, 7]` Float cube, regridding the inclusive box
`[3, 4, 0, 1]` through `[14, 19, 0, 5]` onto the original coordinate
record can mark adjacent rows or columns valid in CASA 6.7.6.14. A
sky-coordinate round trip moves those samples across the linear
interpolator's boundary by a fraction of a pixel at floating-point
precision. Casa-rs uses the exact integer shift on the same tangent
plane, preserving the selected region's validity boundary.

### `image.rotate` can reject an overlapping right-angle sky rotation

For a default `[24, 26]` direction image, CASA 6.7.6.14 rejects
`rotate(pa="90deg", method="nearest")` with a no-overlap error.
The longitude and latitude grids still overlap after a right-angle
rotation about the reference pixel. Casa-rs samples the actual
overlapping pixels and masks the portions outside the source.

At `pa="180deg"` on the same grid, CASA's linear and cubic interpolators
can include pixels just outside the top or left edge. A sky-coordinate
round trip moves some positions back inside the interpolation cutoff.
Casa-rs checks the sky projection and reflects the pixel about the exact
reference pixel, so those positions remain outside the grid.

### `image.rotate` can permute the coordinate axes when dropping a singleton

For a Float `[24, 26, 1, 7]` cube transposed with `order="2301"`,
CASA 6.7.6.14 returns shape `[7, 24, 26]` from `rotate(pa="35deg",
dropdeg=True, replicate=True)` but assigns Direction, Direction and
Spectral coordinates in that order. With per-channel restoring beams,
it fails because the spectral dimension no longer agrees with the beam
record. Casa-rs retains Spectral, Direction and Direction in pixel order
and keeps each beam on its original channel.

On a default direction image of shape `[1, 26]`, CASA 6.7.6.14
can also terminate when `rotate(pa="35deg", dropdeg=True)` removes one
axis of the required two-axis plane. Casa-rs rejects the resulting
coordinate geometry with an error.

### `image.rotate` can replace the only pixel of a linear plane with zero

On a linear `[1, 1]` image whose pixel is 5, CASA 6.7.6.14 returns zero
from `rotate(pa="35deg", method="linear")`. Both singleton axes are
removed from the interpolation pass, leaving the initialized output pixel
instead of copying the input. Casa-rs preserves the original pixel, mask
and coordinates when the rotation has no sampled axis.

### `image.sepconvolve` can replace an explicitly selected kernel axis

On a `[13, 15]` Float image with an impulse at `[6, 7]`, the pinned
CASA 6.7.6.14 call `sepconvolve(axes=[1], types=["boxcar"], widths=[4])`
spreads the impulse along pixel axis 0. The parser assigns `-1` while
checking the sole axis, so the request takes the default branch.
Casa-rs respects the supplied axis and applies the kernel to axis 1.

The Python wrappers for `image.sepconvolve` and
`image.twopointcorrelation` also raise a `SystemError` for tuple-valued
axes, such as `axes=(0, 1)` when the rest of the call is valid. Casa-rs
reports a `TypeError` before creating or replacing an output.

### `image.sepconvolve` accepts undefined kernel coefficients

CASA 6.7.6.14 fills the processed profiles with NaN for a zero-pixel
Gaussian width. With width `[-2]` on a `[10, 12]` image it can terminate
while trying to allocate the kernel. Casa-rs rejects a nonpositive
Gaussian pixel width before publishing an output.

For an even profile length, the Hanning generator assigns symmetric
coefficients around the lower middle sample and leaves its trailing
coefficient uninitialized. Casa-rs assigns zero to the unused final
coefficient. The assigned coefficients retain CASA's normalization and
indexing.

### `image.twopointcorrelation` can terminate the Python process on invalid axes

For a `[5, 7]` Float image, CASA 6.7.6.14 can fault when a selected
axis is negative, such as `axes=[-2, 1]`. It can also abort on
`axes=[0, 0]` for a linear image. Casa-rs validates that the axes are
in range and strictly increasing before creating or replacing an output.

### `image.twopointcorrelation` can ignore a selected mask or region

On an unmasked `[11, 11]` Float image whose value is its first pixel
index, an external mask selecting values below 5 leaves 189 lags with
at least one valid pair. CASA 6.7.6.14 reports all 441 lags as valid. Its
correlation loop only checks the combined mask when the selected image
has a physical pixel mask. Adding an all-true pixel mask makes CASA
honor the selection. The same behavior occurs for a polygon region.
Casa-rs applies the combined validity to both endpoints of every pair.

For a direction image of shape `[1, 7]` with an all-false pixel mask,
repeated CASA 6.7.6.14 calls can also report valid samples near the zero
lag. Casa-rs reports every lag as masked because the input contains no
valid pairs.

## WVR global phase reversal can miss sparse spectral-window selections

CASA 6.7.6.14's `wvr.gcal(reverse=True, spw=[1])` uses the position of each selected window to build the reversal mask. Its writer then looks up that mask by spectral-window ID. A selection containing only window 1 therefore leaves that window's phase unchanged.

casa-rs applies the global reversal to each selected spectral-window ID. The compact ALMA comparison confirms that the corrected gain for window 1 is the complex conjugate of the ordinary gain, while the installed CASA result retains the ordinary sign. [Pinned CASA WVR tool source](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/tools/wvr/wvr_cmpt.cc?at=refs%2Ftags%2F6.7.6.14)

## Seeded WVR proposals vary across CASA C++ standard libraries

CASA 6.7.6.14 uses `std::normal_distribution` for LibAIR's proposal
sampler. With the same seed, Linux libstdc++ and macOS libc++ return each
Gaussian pair in opposite order. The projected draws change the accepted
fits. On the original ALMA `uid___A002_X8ca70c_X5_shortened.ms` fixture with
`segsource=False, nsol=5, rseed=27, refant='DA41'`, Linux CASA accepts three
fits and macOS CASA accepts four.

casa-rs uses a fixed draw order on both platforms. The Mac and Linux native
posterior rows agree, and the seeded fit agrees with the pinned Linux CASA
reference. This keeps native seeded calibration reproducible across hosts.
[Pinned CASA proposal sampler](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/bnmin1/src/metro_propose.h?at=9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2)

## WVR can move successful atmospheric fits into earlier segments

On the recorded ALMA test MeasurementSet `uid___A002_X8ca70c_X5_shortened.ms`,
CASA 6.7.6.14 and casa-rs recover the same two successful atmospheric fits.
The other two fits have zero Bayesian evidence. LibAIR's
`doALMAAbsRet` advances its segment-boundary index only when a fit succeeds.
`SimpleMultiple` consequently attaches those two successful fits to the
first and second segments even though they measured the second and fourth.

casa-rs keeps both successful fits at their original times. The oracle
reconstructs the CASA and corrected gain tables independently from the
coefficients logged by CASA and the recorded radiometer temperatures.
The native output agrees with the correctly assigned gains within
`2.5e-5` normalized RMS. The incorrect CASA assignment produces a gain
difference of approximately `0.123` normalized RMS.

## WVR can dereference a missing single retrieval

For the same recorded ALMA observation, CASA 6.7.6.14 with
`segsource=False, toffset=-1.0` retries after a zero-evidence fit. In that
retry the only selected measurement is flagged. LibAIR's
`ALMAAbsProcessor` returns a null pointer when no fits remain and the
tool dereferences it while calculating gains. The paired oracle
reproduces the segmentation fault in a subprocess.

casa-rs returns a failed retrieval with `rval=1` and publishes no table.
The nearby two-retrieval case with the same time offset has a valid fit
and produces the same gains in both engines.


## Replacing a native image alias can mix a peer's pixels and history

Open a Float native image through a symbolic link using one image tool.
Then rename a different image over that link using a second tool with
`overwrite=True`. In CASA 6.7.6.14, a first tool that already read pixels
keeps reading the original table's pixels, while `history()` reads the
replacement table's history. When its first pixel read happens after the
rename, it can also read the replacement's pixels. With the pixel reader
already open, the renaming tool can continue reading the peer's cached
pixels. Casa-rs binds the peer to its original table and the renaming tool
to the moved table. The peer retains the name supplied when it was opened.

CASA 6.7.6.14 also permits replacing a destination link inside a native
table that an image or table tool has open. Casa-rs applies its existing
containing-table ownership checks before changing a link. The call returns
a busy-table error and preserves both paths, since a link under a table
can participate in its live subtable layout.

## Invalid Jy/K factors can publish unflagged nonfinite gains

The pinned CASA 6.7.6.14 `gencal(caltype='jyperk')` and `getjyperkalma`
convert factors to antenna gains with `1/sqrt(factor)`. A zero factor writes
an unflagged infinite gain. A negative factor writes an unflagged NaN.
Casa-rs rejects a factor that cannot yield a finite positive single-precision
gain, and any accumulation that overflows or underflows to zero. If the caller supplied a calibration table
to update, the original table remains intact. The Jy/K oracle tests the two
invalid factors and checks the existing table after the rejection.

## A flagged F Jones coefficient can leave unflagged visibilities

In CASA 6.7.6.14, a flagged TEC coefficient for the last antenna in a circular-feed
`F Jones` table leaves that antenna's Jones matrix at its nominal identity with
both receptors marked valid. Applying the table leaves its baselines
unflagged even though a correction is missing. Casa-rs flags the affected
correlations when the apply mode writes flags and retains the valid antenna's
correction. The TEC oracle exercises this difference using a CASA-written table.

## An empty EOP result can delete an existing calibration table

With an existing `Fringe Jones` destination, CASA 6.7.6.14 starts by creating a
fresh destination. If an input EOP file contains no updates or cannot be read,
CASA removes the destination during cleanup. Casa-rs stages a replacement only
when it has finished generating a nonempty product. The existing table survives
both a missing file and a file with no matching updates.

## EOP interpolation can ignore later daily samples

CASA 6.7.6.14 bounds the interpolation index by the number of columns in
its Python EOP dictionary. The bound is always three, so an observation
after the fourth sample can use two earlier values even when the input
covers the observed time. Casa-rs brackets the epoch by the available
samples. The seven-epoch EOP oracle checks the resulting delay against an
independent ERFA matrix calculation and measures the difference from CASA.

## A zero single-dish sky reference can write an unflagged infinity

CASA 6.7.6.14 divides the sky-subtracted visibility by a zero reference
spectrum and can publish infinite corrected values with the original
channel flag still clear. Casa-rs retains the value entering the sky step when the
sky spectrum cannot provide a finite correction and flags the channel
in modes that write flags. The paired sky-application oracle checks the
zero reference alongside usable channels.

## Raster sky calibration can skip the last strip

CASA 6.7.6.14's raster gap detector tests the next element of an
uninitialized boundary array before adding the final integration count.
Repeated runs on the same 22-strip map can publish either 44 or 42
solutions. In the 42-solution result both references from the last strip
are missing. Casa-rs always includes the last strip and checks its
solutions against the selected integrations and the complete CASA runs.
[Pinned CASA raster source](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/synthesis/MeasurementComponents/SingleDishSkyCal.cc?at=refs%2Ftags%2F6.7.6.14)

## Double-circle gain calibration can abort on several selected channels

CASA 6.7.6.14 uses a scalar gain cell while copying the selected spectrum
into the solve buffer. On the paired total-power map, `sdgaincal` with
`spw='0:1~2'` terminates with `ArrayShapeError` because a one-channel
cell receives two channels. The same map with `spw='0:1'` produces scalar
solutions at the selected frequency. Casa-rs reports a catchable
single-channel requirement before publishing an output. The paired oracle
runs the CASA crash case in its own process.

With the field reference moved outside the mapped region, the same CASA
release can segfault when no integration passes the central-region filter.
Casa-rs returns a catchable solve error and preserves an existing
destination.

The filter interpolates pointings in time order and applies the resulting
selection mask in input order. This can shift the reference samples for
physically shuffled input rows. Casa-rs follows that selection and emits
solutions in the same spectral-window and time order as CASA.

## Single-dish smoothing can publish an incomplete MeasurementSet

CASA 6.7.6.14 creates the output of `sdsmooth` before it validates the
convolution. A partial channel selection whose width has no configured
convolver raises an error and can leave a MeasurementSet with zeroed spectra.
Casa-rs stages the output, reports the convolution error, and preserves an
existing MeasurementSet on a failed overwrite. A successful retry publishes
the completed replacement.

## Single-dish baseline tables can contain malformed singleton mask ranges

CASA 6.7.6.14 writes an isolated fitted channel as one endpoint when the
channel starts a spectrum's mask. Its own `apply_baseline_table` requires
start/end pairs and rejects an odd-length list. Casa-rs writes both
endpoints for singleton ranges. The paired baseline oracle applies the
resulting casa-rs table with CASA.

## Single-dish baseline weight updates can fail when removing two columns

When both spectral noise columns exist, CASA 6.7.6.14 runs the baseline
calculation, then passes the space-separated string
`WEIGHT_SPECTRUM SIGMA_SPECTRUM` as a single column name to
`table.removecols`. The task raises after writing the MeasurementSet.
Casa-rs's task publishes the recalculated scalar weights and leaves out
both spectral columns. The direct tool retains the undefined spectral
columns in its output schema.

## Single-dish baseline reports accumulate clipped-channel counts

CASA 6.7.6.14 carries the clipped-channel count from earlier spectra in a
visibility buffer into a spectrum's CSV and text report. With one rejected
channel per spectrum and two polarizations, successive records can show
`1,1,2,2,3,3`. Casa-rs reports the number rejected in each spectrum.

## Single-dish baseline fitting accepts negative iteration counts in CASA

The CASA 6.7.6.14 task passes `clipniter+1` to the tool, which converts it
to an unsigned 16-bit fit count for the solver. A count of zero leaves the
input real spectra in the output and can write coefficients and RMS values
without performing a fit. Smaller values can wrap to very large fit counts.
Casa-rs requires a nonnegative `clipniter` and a positive direct-tool
`num_fitting_max`.

## Dense single-dish cubic splines can lose numerical accuracy in CASA

CASA 6.7.6.14 solves a normal system for the spline coefficients. On a
96-channel input with seeded Gaussian samples, the fitted residuals differed
from an independent B-spline least-squares calculation by RMS 0.0557 with
47 pieces and 0.5401 with 93 pieces. Casa-rs solves the design matrix with
an SVD. Its fitted residuals agreed with the independent calculation
within RMS (2.2\times10^{-8}) on both inputs.

## Single-dish automatic line masks can access invalid channels in CASA

CASA's line finder can call its median helper with zero searchable bins.
Its wing extension can also decrement an unsigned index at channel zero.
Casa-rs stops an averaging level when it has fewer than two eligible bins
and stops the wing at the start of the spectrum.

## Single-dish line fitting can exceed its time bin on descending rows

CASA 6.7.6.14 iterates `fit_line` by data description and the uncombined
scan, state, and field keys. `SDMSManager::setIterationApproach` omits
`TIME` from this sort. The averaging iterator compares later time
stamps with its first buffer's time. With physical rows at 6, 4, 2, and
0 seconds and `timebin='1s', timespan='scan'`, CASA combines the four
spectra per hand into one. The same input in chronological order
produces four spectra per hand. casa-rs sorts the integrations by time
inside each grouping key so the requested interval holds for either row
order. The paired oracle verifies both CASA results and the native
chronological grouping.

## Single-dish polarization averaging can abort for one selected hand

In CASA 6.7.6.14, `PolAverageTVI::configurePolAverage` obtains its two
hand indices from the full POLARIZATION table setup. With an XX/YY MS
and `sdfit(pol='YY', polaverage='geometric', nfit=[1])`, CASA uses the
original pair to slice an array with just one selected hand and aborts
with `ArrayBase::operator()(b,e,i) - incorrectly specified`. casa-rs
resolves the two hands after correlation selection. When there is no
complete pair it uses the selected spectrum unchanged, as the
iterator's own skip message describes. The value agrees with fitting
the same selected hand without averaging.

## Single-dish line fitting aborts for negative component counts

CASA 6.7.6.14 uses `std::stoi` on the comma-separated list counts and
stores them in an unsigned vector. In list mode, `nfit=[-1]` reaches a
negative array shape and aborts outside the task exception handler.
casa-rs reports the invalid component count before allocating a fit and
keeps the auto-mode interpretation, where the line finder determines
component counts.

## A failed line fit can delete the previous report

CASA 6.7.6.14 removes an existing `sdfit` report at the beginning of an
overwrite. If a later selection or fit fails, the older report is
lost. casa-rs stages the text report and replaces the existing file
after successful processing.

## Atmospheric correction can remove an unrelated output directory

CASA 6.7.6.14 uses recursive directory removal for an existing
`sdatmcor` destination when `overwrite=True`. A nonempty directory
containing unrelated files is deleted before output is written.
casa-rs replaces a readable MeasurementSet or an empty placeholder
after preparing the new product. It preserves a nonempty directory
that contains no MeasurementSet.

## An empty atmospheric correction window selection raises a record-type error

CASA 6.7.6.14 resolves the windows to process separately from the windows
to output. With `spw='19', outputspw='23'` on the upstream total-power
fixture, the work list is empty. NumPy infers a floating-point array
for the empty list, and the `atmcor` record conversion rejects
`processspw` before it can write the requested data. casa-rs passes a
typed empty list and writes window 23 with its spectra unchanged.

## Atmospheric correction can index a cropped window from channel zero

On `X320b_sel2.ms`, `sdatmcor(spw='19', outputspw='19:370~407^2',
scan='7')` writes the requested channels. CASA 6.7.6.14 subtracts
corrections for original channels 0 through 18 from their data. The
paired oracle recovers that exact offset from the uncropped CASA
product. casa-rs uses the correction at each selected original
channel. Its cropped spectra equal the same channel slice of its
full-window correction, including reordered and overlapping ranges.

## Atmospheric correction can publish unflagged nonfinite spectra

With the recorded total-power fixture and CASA 6.7.6.14, a gain table
with zero amplitude for window 19 produces an infinite reciprocal-square
factor. A scalar factor of `1e40` also exceeds the Float32 range of the
corrected spectra. Both inputs write unflagged nonfinite samples.
casa-rs rejects either result before publishing the output, preserving
an existing product on overwrite.

## ASAP import writes observation times in days to a seconds column

CASA 6.7.6.14 copies the scan table's MJD day range into
`OBSERVATION.TIME_RANGE`, whose unit is seconds. The same import converts
the MAIN integration times to seconds. On the upstream ALMA ASAP test
fixture, the observation range starts at `56839.91554` and the first
integration is `4910968702.656` seconds from MJD zero. casa-rs converts
both tables to MJD seconds and preserves the original observation bounds.

## The single-dish filler can write inconsistent receptor metadata

CASA 6.7.6.14 writes two receptor names to `FEED.POLARIZATION_TYPE` with
`NUM_RECEPTORS=0`. It fills `POLARIZATION.CORR_PRODUCT` with zero pairs
even for YY, LL and cross-hands. The imported spectra themselves are in
the order indicated by `CORR_TYPE`. casa-rs writes the receptor count and
maps directly measured linear and circular products to the corresponding
ordered pairs. Derived Stokes products retain their placeholders.

## NOSTAR import writes FEED rows only for the first beam

On the upstream Orion KL NOSTAR file, CASA 6.7.6.14 writes MAIN spectra
for antenna IDs 0 through 3, but FEED rows only for antenna ID 0. The
other three beams reference feeds with no corresponding metadata.
casa-rs writes one FEED row for each beam and spectral window referenced
by MAIN.

## NOSTAR import discards the imaginary part of cross-hand spectra

In a four-plane NOSTAR integration, the third and fourth planes encode
the real and imaginary components of the cross-hands. CASA 6.7.6.14
always writes `FLOAT_DATA`. A test with the instrument planes
`[XX=1, YY=2, XY=3, YX=4]` produces `[1, 3, 3, 2]`.
casa-rs writes complex `DATA` with both components in the conjugate
products `[1, 3+4i, 3-4i, 2]`.

## Single-dish averaging can label two time-averaged hands as Stokes I

After `fit_line(timebin='8s', timespan='scan', polaverage='geometric')`
CASA 6.7.6.14 uses its time-averaging iterator for subsequent operations
on the same attachment. Smoothing, baseline subtraction and atmospheric
correction produce both XX and YY samples, but link the output data
description to a one-correlation Stokes I setup. casa-rs preserves the
XX/YY setup that describes the actual samples.

## Applying a single-dish baseline can abort after polarization averaging

On an XX/YY attachment, CASA 6.7.6.14 can save a one-correlation baseline
table after `fit_line(polaverage='stokes')` or
`fit_line(polaverage='geometric')`. Applying that table on the same
attachment terminates the process with a casacore matrix row error.
casa-rs applies the model to the projected correlation. The paired
oracle reads both saved tables through casacore and applies both in
casa-rs.

## Single-dish polarization averaging can clear a rejected integration's row flag

On an XX/YY spectrum with `FLAG_ROW=true` and clear per-channel `FLAG`
cells, `fit_line(polaverage='geometric')` followed by `atmcor` in CASA
6.7.6.14 produces a row with clear flags. casa-rs retains the rejection
in the output row and channels. The baseline fitter also excludes that
integration from its fit.

## GSPLINE append overwrites the previous field's fit

In CASA 6.7.6.14, `gaincal(gaintype='GSPLINE', field='FLUX')` stores the
FLUX fit under every FIELD_ID, including unselected fields. Solving
`field='PHASE', append=True` into that table then replaces the fit under
each existing FIELD_ID. With disjoint field time ranges, applying the
updated table to the original field loses its corrections.

casa-rs publishes rows for selected fields and updates only those fields on
append. Added fits can have different knot counts. The stored arrays share
a shape so CASA can read them, and each row keeps its actual count. The
[paired oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/gspline_oracle.py) compares the original
rows to a saved copy of the initial solve and the new rows to CASA. It then
applies the combined table through both implementations.

The direct `calibrater.setsolvegainspline` and `solve` workflow uses the same
selected-field publication policy. Its paired oracle checks the new field's
coefficients against CASA and verifies that append preserves the prior field.

## GSPLINE fit logs give the RMS for the wrong baseline

The `gaincal(gaintype='GSPLINE')` diagnostic log indexes residuals as if
only cross-correlations occupied the array. The solver's storage includes
autocorrelation slots. A noisy fit therefore displays a zero RMS for the
first baseline, then assigns its residual to a different baseline.
casa-rs reports the weighted fit RMS for the stated baseline. The paired
oracle recomputes it from the visibility data and saved coefficients.

## GSPLINE can abort when every channel has a rejected correlation

CASA 6.7.6.14 uses common channel flags for the time-spline solve. An
XX/YY input with all YY channels flagged therefore has no usable shared
samples. CASA can terminate while attempting this fit. Casa-rs returns
the insufficient-sampling result without publishing a calibration table.

## GSPLINE can publish nonfinite phases for a disconnected antenna

When all baselines to one antenna are flagged, CASA 6.7.6.14 attempts to
fit that antenna along with the connected array. The singular fit can
publish nonfinite coefficients, including for antennas that have data.
Casa-rs fits the connected array and omits antennas without accepted data.
Both legacy readers then flag visibilities with a missing antenna gain.
This rule also applies to generic `calibrater.setsolve(type='GSPLINE')`,
which publishes the fitted antennas across every attached field.

## `fringefit` can abort with one selected channel per spectral window

With `combine='spw'`, CASA 6.7.6.14 can abort if every selected window
contributes only one channel. In a test with two windows 48 MHz apart and
`spw='0:0,1:0'`, the concatenated FFT tried to address channel zero on an
array with a zero-length frequency axis. CASA raised `ArrayError` and
terminated before publishing the table. Casa-rs derives the grid spacing
from the separation of the selected frequencies and checks the FFT memory
requirement before allocating the grid. Its two-band regression exercises
both search modes.

## `accor` rejects TaQL selections because the wrapper uses the ANTENNA table name

CASA 6.7.6.14 adds `ANTENNA==ANTENNA2` when the caller supplies a nonempty
`msselect`. A valid predicate such as `ANTENNA1<3` fails because `ANTENNA`
is a subtable. The default task path already uses `ANTENNA1==ANTENNA2`.
Casa-rs intersects the caller's predicate with `ANTENNA1==ANTENNA2` in both
cases. Its test compares the result to CASA selecting those same antennas
through `antenna`.

## `pccor` can fail after creating an empty calibration table

Selecting no spectral windows in CASA 6.7.6.14, for example with
`spw='!0'` on a one-window MeasurementSet, reaches the table constructor
before the first attempt to index an empty set of solutions. The task
raises `IndexError`. Casa-rs checks the selected windows before
publication and returns the same exception.

CASA can also raise `UnboundLocalError` when the first selected reference
antenna has pulse data only outside the reference interval. Casa-rs
reports the missing reference data with a `RuntimeError`. The paired
PCCOR oracle covers both cases and checks destination preservation.

## `appendantab` attaches new subtable links to the child tables

CASA 6.7.6.14 creates `SYSCAL` and `GAIN_CURVE` under the copied
MeasurementSet and writes each keyword on the corresponding child table.
The embedded gain-curve consumer reads the `GAIN_CURVE` link on MAIN.
Casa-rs attaches both links on MAIN. The paired oracle compares the
subtable contents and gives the CASA reference copy the same MAIN links
before checking its independent `gencal` consumer.

With `overwrite=True` on a copy that has no gain rows, CASA also indexes
the first receptor of an empty sensitivity array and raises `IndexError`.
Casa-rs imports the gain records and publishes the completed copy. Its
test compares the output with the default CASA import.

CASA uses the position of an ANTAB gain record as a channel index when
overwriting a gain row. This raises `IndexError` when the second antenna
has a gain record for a one-channel window. Casa-rs tests the window's
frequency coverage against the gain record's range. The oracle checks
both antennas' replaced sensitivities.

CASA assembles Tsys temperatures in spectral-window number order, then
writes row identifiers in data-description order. The temperatures can
follow the wrong SPW when those orders differ. Casa-rs assembles each row
using its stated spectral-window identifier. It also ignores an unknown
station's GAIN record while preserving the following GAIN header. CASA
can consume that header while skipping to the next slash.

## Baseline Mueller calibration assumes ordered antennas

CASA 6.7.6.14 computes its `M Mueller` and `Mf Mueller` baseline index
assuming the first antenna ID is at most the second. For five antennas,
`(0,3)` uses slot 3, while `(3,0)` uses slot 9, the slot for `(2,2)`.
This can select or update the wrong baseline response. Exchanging antennas
also conjugates the parallel-hand visibilities. Casa-rs stores the response
for the ordered pair and conjugates it when solving or applying a reversed
row. The [paired BLCAL oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/blcal_oracle.py) checks
the reversed-input solve and apply against the known sky model.

When cross-hands carry weight and both parallel hands have zero weight,
CASA can attempt the interval and publish unflagged NaNs from a zero-weight
division. Casa-rs retains the interval and flags the unavailable
parallel-hand responses, with the same zero coefficients as other absent
solutions. The oracle compares the attempt and checks the output flags.

## Calibrater reports antenna IDs from the last visibility buffer

CASA 6.7.6.14 collects field, observation and scan IDs throughout the
`calibrater.returndict()` visibility iteration. It fetches the antennas
after iteration, from the last buffer. With antennas 0 and 1 in the first
selected scan and 2 and 3 in the second, the record lists both scans and
only antennas 2 and 3. Casa-rs reports the four antennas in the selected
rows. The [paired calibrater oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_apply_tool_oracle.py)
checks both records on the disjoint baselines.

## Calibrater can reuse inverse Jones terms when corrupting a model

CASA 6.7.6.14 applies a constant antenna gain of 1.2 correctly in a
fresh `calibrater.corrupt()`, multiplying the physical model by 1.44.
After `correct('trial')` on the same tool and selection, it divides the
model by 1.44 on each `corrupt()` call. `VisCal::corrupt2` requests forward
application even in this sequence. Casa-rs evaluates the forward Jones
terms on every model corruption. The [paired tool oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_apply_tool_oracle.py)
checks both sequences against the constant-gain product law. Both engines
retain the previous single-term activity counts across the corruption.

## Calibrater's legacy iterator can crash during a gain solve

With CASA 6.7.6.14, `setvi(True, True)` followed by a selected G Jones
solution can terminate Python with a segmentation fault in `solve()`.
The [direct solve oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_solve_tool_oracle.py)
reproduces the crash with the legacy gain case. The same legacy fixture
returns normally when `minblperant=99` blocks the fit. Casa-rs completes the
fit using its shared solver and checks the resulting table and activity
against the successful CASA modern-iterator solve. Legacy return records
retain their original layout without `solvestats`.

## Direct FRINGE can terminate Python with an empty mask and default windows

In CASA 6.7.6.14, use the [direct FRINGE fixture](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_fringe_tool_oracle.py), set a valid reference antenna, and call `setsolve(type='FRINGE')` with the default empty `paramactive`, `delaywindow`, and `ratewindow` lists. On unflagged selected data, `solve()` can terminate Python. The same empty mask with explicit search bounds raises `Error in Calibrater::solve.`. Casa-rs reports that error for a nonempty gather that requires global refinement with no active parameters. An empty gather still completes and reports zero attempts. [#2012](https://github.com/bglenden/casa-rs/pull/2012)

## Direct specifycal aborts for literal wildcard selectors

With CASA 6.7.6.14, call `calibrater.specifycal(caltable='gain.cal', caltype='amp', parameter=[1.5], antenna='*')` on the [direct fixture](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_specifycal_tool_oracle.py). CASA passes antenna index `-1` into a parameter-array slice and aborts. A literal `spw='*'` likewise reaches an invalid spectral-window index. Casa-rs broadcasts over the requested wildcard axes and verifies the resulting table against CASA's empty-selector form, which expresses the same prescription. [#2015](https://github.com/bglenden/casa-rs/pull/2015)

## `mstransform` silently skips requested continuum fitting

CASA 6.7.6.14 `mstransform` sends the public `fitspw` argument to its continuum
visibility iterator under the key `fitspw`. `UVContSubTVI::parseFitSpec` reads
`fitspec`, so the iterator receives no fit windows and copies each visibility
unchanged. Reproduce with `douvcontsub=True` and `fitspw="0:1~4"` on a dataset
with a varying spectrum. The same task with `douvcontsub=False` writes the
same visibility values.

casa-rs fits the requested polynomial on the unflagged line-free channels and
writes residuals, or the estimate when `want_cont=True`. The paired oracle in
`tools/casa-compat/ms_transform_continuum_oracle.py` reproduces the public
wrapper bug and independently compares the native result with CASA's compiled
fitter after correcting only the configuration key. The native task also
discards its staged product when fit selection fails, which allows the caller
to correct the selection and retry at the same path.

The compiled denoising fitter has a second defect in
`GslMultifitLinearBase::calcFitModelCore`. It replaces the model vector for
each coefficient instead of adding the polynomial terms. That helper
evaluates the stored float output and the residuals used to reweight both
the real and imaginary data during later iterations. For example, a linear
float spectrum with nonzero offset retains the full offset after CASA
subtraction. casa-rs evaluates the whole polynomial in both places. The
oracle compares those cases with a separate NumPy weighted fit.

When the compiled fitter follows time averaging, its line-channel mask is
written through a reference to the averaging iterator's cached visibility
flags. It can replace valid flags and change scalar weights and sigmas. The
native fit reads the flags and applies the mask only to the fit samples.
The oracle takes the averaged flags and noise from CASA's public task, where
the misnamed configuration key leaves the mask inactive.

The compiled path can also try to change the shape of an allocated
`WEIGHT_SPECTRUM` cell when the original task selects only part of a spectral
window. For the selection case, the oracle applies CASA's channel selection
first and fits using the equivalent local channel indices. The native task
computes the intersection on the selected grid in one transform.

## Direct fluxscale can terminate Python with an empty window map

In CASA 6.7.6.14, use the [direct fluxscale fixture](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_fluxscale_tool_oracle.py), attach the MS, and transfer from `FLUX` to `PHASE` with `refspwmap=[]`. The reference process can terminate with SIGSEGV. Casa-rs treats an empty map as no remapping and compares its returned record, caltable and HISTORY against CASA's default `refspwmap=[-1]`. [#2017](https://github.com/bglenden/casa-rs/pull/2017)

## Direct opacity solves can terminate Python

CASA 6.7.6.14 accepts `calibrater.setsolve` for `TOpac` and `TfOpac`. With usable data, the first fit in either visibility iterator terminates the process with SIGSEGV and leaves no calibration table. An otherwise equivalent VI2 `T` solve completes. If every selected sample is flagged, both opacity families return success and preserve the configured solver. They record zero attempted and successful intervals and publish no table.

Casa-rs preserves the setup, state and no-result behavior. When the selected data would reach a fit, it raises a recoverable solve error and clears the equation. The oracle runs the reference fits in child processes. [Direct opacity solve oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_opacity_solve_oracle.py)

## An additive polynomial window can keep a degenerate frequency basis

With a one-channel selection in each of two combined spectral windows, flagging one window throughout the first scan makes CASA 6.7.6.14 record the remaining channel's frequency as both domain endpoints. A later scan can have usable samples at both frequencies, but the saved basis still has coincident endpoints. Casa-rs derives the domain from both selected frequencies and retains a solvable linear fit in the later scan. [Additive Mueller polynomial oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_additive_polynomial_oracle.py)

## Generic BPOLY setup can terminate Python

CASA 6.7.6.14 can terminate with SIGSEGV during `calibrater.setsolve(type='BPOLY')` on an attached MeasurementSet in either visibility iterator. Casa-rs raises a recoverable setup error. The dedicated `setsolvebandpoly` entry point supplies the polynomial controls and supports the legacy fit. [Generic polynomial oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_generic_polynomial_oracle.py)

## Generic GSPLINE phase wrapping can vary between CASA processes

With the same data and generic `calibrater.setsolve(type='GSPLINE')` arguments, CASA 6.7.6.14 produced different phase coefficients in separate processes. One process was terminated during phase fitting under the local memory limit. The dedicated `setsolvegainspline` call uses explicit wrapping controls. Casa-rs uses 7,200-second spacing, two phase points, and a zero wrap threshold for the generic path. These controls reproduced the pinned phase coefficients in the dedicated reference and in three isolated generic preaveraged fits. [Generic polynomial oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_generic_polynomial_oracle.py)

## Competing VI1 callibrary slices can mix antenna gains in one buffer

In CASA 6.7.6.14, `calibrater.corrupt()` with the old iterator looks up a callibrary buffer with wildcard scan and intent. Competing scan-specific entries for one table can therefore resolve to the earlier selected scan for both scans of a field. On a fixture with two scan-specific mappings and an additional explicitly scoped field, CASA combined unity and calibrated antenna gains within the same scan. This produced distinct baseline factors of 1, 1.2, and 1.44 from a single intended antenna gain of 1.2. Casa-rs selects one slice consistently for both antennas in each legacy buffer. Scan-filtered input, other fields, correction, and modern corruption retain their applicable mappings. [Paired calibration-library oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_legacy_callibrary_priority_oracle.py)

The selected slice uses its registered calibration scan even when the current visibility belongs to a different scan. Mixed scan and intent wildcards can also give the CASA ordered index a cyclic comparison. The one-antenna probe with three distinct gains found an insertion-order-dependent result. The native lookup uses one ordered context throughout a buffer. The oracle records the two registration orders where the pinned CASA result differs.

## Some direct calibration factories gather data without a usable fitting stage

CASA 6.7.6.14 accepts generic `calibrater.setsolve` for J, Jf, A Noise, F Jones, B TSYS, gain curve, and legacy power curve. With all input flagged, the tool reports the expected intervals and does not produce a table. When four-correlation usable data reaches the fitting stage, the pinned legacy J/Jf and both B TSYS iterators terminate. The tested modern J/Jf, A Noise, F Jones and curve configurations return a solve error. Modern power curve returns a setup error. Casa-rs retains the available gather stage and uses a recoverable error for the unavailable fit. These limits concern solving. Existing table application and specification are separate operations. [Paired factory oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_gather_only_oracle.py)

## Direct KAntPos can abort when a solution combines multiple buffers

CASA 6.7.6.14 uses its antenna delay fitter for direct `calibrater.setsolve(type='KAntPos')`. Both iterators write the two fitted terms in a three-term `KAntPos Jones` table. The unused third term is zero, valid for the reference antenna and flagged for the others. Two overlapping windows with `combine='spw'` or two fields with `combine='field'` terminated in both iterators when the multi-buffer fitter assigned two terms to a three-term array. Casa-rs reports a recoverable error when this fit is reached. Windows separated into single-buffer scan or integration intervals produce tables. [Paired antenna-position oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_antenna_position_oracle.py)

## A Noise baseline indexing assumes ordered antenna columns

With eight antennas, both CASA 6.7.6.14 calibrater iterators applied the `A Noise` coefficients of `(0, 7)` to a deliberately reversed `(1, 0)` visibility. Casa-rs uses the coefficients of `(0, 1)`, conjugates them, and exchanges the cross hands to preserve the visibility's orientation. The ordinary ordered-baseline inputs agree with CASA. [Paired A Noise oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_anoise_oracle.py)

## Direct EVLASWP can gather data and fail at fitting

CASA 6.7.6.14 accepts direct `calibrater.setsolve(type='EVLASWP')` when the MeasurementSet links readable `SYSPOWER` and `CALDEVICE` tables. Both iterators record expected intervals and write no table for fully flagged input. Both terminate on the isolated usable visibility fit. Casa-rs retains the gather stage and returns a recoverable solve error for usable data. [Paired switched-power and KMBD oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_kmbd_oracle.py)

## Joint KMBD fitting can abort with single-correlation buffers

When `calibrater` combines spectral windows for a KMBD fit, both CASA 6.7.6.14 iterators refuse a mix of single- and dual-hand buffers. Two single-hand buffers instead terminate when the one-hand answer is assigned to the two-hand table array. Casa-rs reports a recoverable solve error in each case. [Paired KMBD oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_kmbd_oracle.py)

## A calibrater gain matrix can keep its correction direction during corruption

In CASA 6.7.6.14, a VI2 session that calls `correct` and then `corrupt` with the same single-slot gain table can apply the inverse gain to both visibility columns. Casa-rs applies the inverse gain to `CORRECTED_DATA` and the forward gain to `MODEL_DATA`. The same planted session with VI1 terminates during its direct amplitude G fit in an isolated CASA process. The native VI1 session completes the fit and both visibility operations. [Paired session oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calibrater_session_oracle.py)

## `calanalysis.get` can terminate on a restricted feed

In CASA 6.7.6.14, open one of the two-feed, multi-window tables in the [selected-series oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calanalysis_get_oracle.py) and call `get(feed='1')`. On both complex and real tables, the reference process terminates with SIGSEGV after opening the table successfully. Casa-rs filters the selected feed by its table index and verifies that the returned records equal the same feed's series from the unrestricted query.

## `calanalysis.fit` can terminate on an empty or singleton fit input

In CASA 6.7.6.14, use the [fit oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calanalysis_fit_oracle.py) to fit a fully flagged channel with `type='ROBUST'`. Both AVERAGE and LINEAR terminate when the robust estimator asks for a median of an empty array. With one timestamp and `axis='F'`, a successful AVERAGE fit can also terminate while calculating the variance of one residual. Casa-rs returns the input series with an invalid fit when every sample is flagged and reports zero residual variance for a successful singleton fit.

## `calanalysis.get` can invent phase wraps on descending frequencies

In CASA 6.7.6.14, the [group-delay oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calanalysis_get_oracle.py) supplies eight descending channels with constant complex gains. Default group-delay unwrapping searches a signed interval away from zero and inserts wraps into a zero-delay solution. Casa-rs searches a symmetric interval using positive spacing between distinct frequencies. It removes the common carrier from the power calculation and refines peaks at the bandwidth scale, preserving the original channel order. The oracle checks ascending and descending zero-delay and planted-delay series, including 1 kHz channels at 100 GHz.

## `calanalysis.fit` can overestimate a nearly exact fit's reduced chi-square

In CASA 6.7.6.14, the [fit oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/calanalysis_fit_oracle.py) supplies an almost exact linear series over eight channels near 100 GHz with uncertainty 0.001. CASA reports a reduced chi-square of about 0.0365. Its returned residuals and uncertainties imply about `2.33e-8`. The normal-equation calculation subtracts much larger quadratic terms. Casa-rs sums the squared, weighted residuals directly, after the existing flags and uncertainty exclusion, and uses that metric for reduced chi-square and unweighted covariance.

## Layer absorption queries can terminate Python on an invalid layer

With CASA 6.7.6.14, initialize a profile and spectral window, then call
`getAbsTotalDry(getNumLayers(), 0)`. The C++ wrapper checks the window and
channel but lets the layer's `std::out_of_range` escape, terminating Python.
casa-rs raises `RuntimeError` for an invalid layer and preserves the tool state.
The paired atmosphere oracle isolates the CASA call and checks all nine native
layer getters, including a valid query after each error.

## Intent wildcards can lose literal punctuation

In CASA 6.7.6.14, give a STATE row the intent
`OBSERVE+TARGET#ON_SOURCE` and use that row in MAIN.
`msmetadata.fieldsforintent("OBSERVE+*")` then returns no fields, although
the exact intent and `"OBSERVE*"` match. The shared wildcard helper uses
a replacement that refers to a nonexistent regex capture, dropping
characters such as `+`. casa-rs treats `*` as the wildcard and matches
all other characters literally in field, scan, and spectral-window
intent selectors (#1029).

## MSCAL predicates can crash or hang in `applycal`

On the synthetic MeasurementSet in the [applycal oracle](https://github.com/bglenden/casa-rs/blob/main/tools/casa-compat/applycal_oracle.py),
CASA 6.7.6.14 terminates with SIGSEGV when `msselect` calls `mscal.scan("2")`
or `mscal.time(">2019/12/31/21:53:15")`. An `mscal.uvdist("<40m")` predicate
returns rows in a direct table query but times out with the MeasurementSet
open for `applycal`. casa-rs evaluates these expressions through the shared
scan, time and UV-distance grammars. The oracle uses equivalent ordinary CASA
TaQL predicates to compare the calibrated and untouched rows and their flags.

## A metadata tool borrowed from `ms` can crash or follow a different attachment

In CASA 6.7.6.14, a child returned by `ms.metadata()` can terminate
the process when its `name()` is called. The borrowed constructor
initializes the metadata reader without initializing the table pointer
used by `name()`. For example, open the upstream `ngc5921_ut.ms`,
obtain a child with `cachesize=0`, and call `name()` on it.
The child can also start reading the rows of an unrelated MS after
the parent closes and reopens, as seen when reopening a seven-row
reference view. casa-rs gives the child the logical name of its
original attachment. The same child continues to see selections and
reset from that attachment and keeps its last selection after the
parent detaches. It also drops cached scan metadata when its selection
changes. In CASA, calling `scannumbers()` before selecting scans 3–5
in `ngc5921_ut.ms` can keep returning all seven scans even though
`nrows()` reports the selected row count. If the original directory
is removed or replaced, casa-rs reports an unavailable attachment on
queries that need its data (#1030).

## Out-of-range TaQL array indices can abort MS selection

In CASA 6.7.6.14, open an MS with ordinary visibility arrays and pass
`abs(DATA[999,1]) > 0` as `msselect`'s TaQL predicate.
`msseltoindex` and `msselect(onlyparse=True)` accept the predicate
without reading cells. Applying it can terminate the process with an
uncaught array-slicing exception. casa-rs validates the expression
without reading visibility cells and reports a failed selection when
evaluation reaches an invalid index. The previously selected rows
remain accessible (#1030).

## A Multi-MS metadata count can omit newly published member rows

Open a genuine CASA Multi-MS and obtain its metadata with
`cachesize=0`. Append a complete row to the last member and close its
writer. CASA 6.7.6.14's child count can stay at the original total and a
summary can abort. The casa-rs child counts the published member rows
and reports the same total in its summary (#1030).

## Concatenated FIELD tables can hide their ephemerides

Create a Multi-MS with two members carrying FIELD ephemerides and list
FIELD among the tables to concatenate. CASA 6.7.6.14 can search a
transient, nonexistent table directory for both members' ephemerides.
`ms.getfielddirmeas()` then returns an empty direction and
`ms.metadata().phasecenter()` raises an error. casa-rs finds each
ephemeris beside its physical FIELD table. Each combined direction
matches CASA's direction for the corresponding member opened on its
own (#1030).

## An unobserved antenna name can return every antenna station

With CASA 6.7.6.14, attach a MeasurementSet whose ANTENNA table includes
`A4` but whose observation 0 uses only `A1` through `A3`.
`msmetadata.antennastations("A4", obsid=0)` and
`antennastations(["A4"], obsid=0)` return every station. The name-selection
branch passes its empty intersection to an accessor where an omitted ID
list means every station. The numeric selection of the same antenna
returns an empty list. casa-rs returns an empty list for both forms
because no selected antenna participated in the observation (#1029).

## Unsupported antenna scalars can silently select unrelated antennas

With CASA 6.7.6.14, pass `numpy.uint32(1)` or `numpy.bool_(True)` as
the selector for `msmetadata.antennaids`, `antennastations`, or
`antennaposition`. CASA discards the scalar. It returns all antenna IDs,
all stations, or the position of antenna zero, respectively, just as for
an omitted selector. casa-rs uses the unsigned antenna ID in the two
methods that accept IDs and raises `RuntimeError` for unsupported selector
types. An unsupported value cannot silently widen the selection or return
the position of a different antenna (#1029).

## A cached cross-correlation row count can underflow

In CASA 6.7.6.14, call `msmetadata.summary()` and then
`nrows(autoc=False, flagged=True)` on an attached MeasurementSet
containing cross-correlations. CASA's cached row-statistics path subtracts
the total row count from the autocorrelation count in an unsigned type.
For example, a 30-row MeasurementSet with 18 cross-correlations can return
a float equal to 2⁶⁴. casa-rs counts the actual MAIN rows whose antenna
IDs differ, independently of which metadata method was called first (#1029).

## MeasurementSet row counts and effective exposure can ignore FLAG_ROW

In CASA 6.7.6.14, set `FLAG_ROW=True` on a MAIN cross-correlation row
while leaving that row's `FLAG` array clear. `msmetadata.nrows(flagged=False)`
counts the row and `effexposuretime()` includes its exposure. Setting the
entire `FLAG` cell instead excludes both. casa-rs treats the row flag as
marking all samples in the row bad, as the MeasurementSet defines it.
This leaves total row counts and the effective-exposure bandwidth
denominator unchanged (#1029).

## Summarizing an empty MeasurementSet can abort CASA

Open a valid zero-row MeasurementSet, for example a disk reference view
made with CASA's `table.selectrows` and an empty integer array.
In CASA 6.7.6.14, `msmetadata.summary()` computes minimum and maximum
times on an empty array and aborts the Python process. casa-rs raises
`RuntimeError` because the summary has no beginning or ending timestamp.
The tool remains attached and reports zero rows (#1029).

## Descending channels can produce negative unflagged counts and effective exposure

CASA 6.7.6.14 weights usable samples by signed `CHAN_WIDTH` in
`msmetadata.nrows(flagged=False)` and `effexposuretime()`. With four
1 MHz channels stored in descending order, negative channel widths,
positive `TOTAL_BANDWIDTH`, and one channel flagged in each correlation,
CASA reports −13.5 unflagged cross-correlation rows and −22.5 s on an
18-cross-row fixture. The ascending ordering gives +13.5 rows and +22.5 s.
casa-rs uses channel-width magnitudes, so either ordering gives the same
positive count and duration (#1029).

## MS selection can reopen subtables through a retargeted symlink

In CASA 6.7.6.14, open an MS through a directory symlink. Retarget the
symlink to a reference view without a local `ANTENNA` subdirectory, then
call `msselect` with a baseline present in the original MAIN. CASA tries
to open `ANTENNA/table.f0` through the new target and returns `False`.
casa-rs keeps the public link name and resolves reads and writes against
the MS captured at open, so changing the link cannot redirect an
attached operation (#1030).

## An invalid FIELD direction column can return the north celestial pole

In CASA 6.7.6.14, call `ms.getfielddirmeas("phase")` on a valid MS.
CASA logs an illegal column name and still returns a J2000 direction
with declination π/2. casa-rs logs the bad column and returns the empty
boolean array also used for other failed direction queries (#1030).

## Empty MS selections can fail the spectral-window summary

In CASA 6.7.6.14, select zero rows from a valid MS with nonempty
DATA_DESCRIPTION and SPECTRAL_WINDOW, then call `getspectralwindowinfo()`.
The scalar range returns an empty record and the summary indexes a
missing field, raising an internal assertion error. casa-rs returns an
empty dictionary because no data descriptions are used (#1030).

## An MS opened read-only can start writing through another handle

In CASA 6.7.6.14, open an MS with `ms.open(path, nomodify=True)` and
call `hanningsmooth("data")`. It initially returns `False` and asks for
`nomodify=False`. Open and close the same path for update with a second
`ms` or `table` tool. The first handle now reports `iswritable() == True`
and `hanningsmooth("data")` writes to MAIN, even after the writer closed.
casa-rs retains the requested access mode per handle. A read-only handle
continues to report `False` and refuses writes (#1030).

## PlotMS can lose its backend on the real axis of FLOAT_DATA

In CASA 6.7.6.14 with casaplotms 2.9.1, select real versus channel on a
float-only MeasurementSet using `ydatacolumn="float"`. The headless backend
can close its RPC stream during export, producing no file. The cache fills
the floating amplitude array. The real axis indexer reads a separate array
that the cache leaves empty. The explicit axis validation permits real-valued plots.
[Pinned cache](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/src/code/plotms/Data/MSCache.cc?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)
[Pinned indexer](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/src/code/plotms/Data/PlotMSIndexer.cc?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)

casa-rs reads the real values directly. The regression checks the same
selected samples against CASA's successful signed amplitude export, which
has the same coordinates for float cells (#1032).

## Clearcal releases a caller's explicit table lock

In CASA 6.7.6.14, open a writable MeasurementSet with
`table.open(vis, nomodify=False, lockoptions={'option': 'user'})` and take an
explicit write lock with `table.lock(write=True)`. Calling
`clearcal(vis, addmodel=True)` writes the scratch columns. The attached
table then reports `haslock(write=True) == False` even though its owner never
released the lock.

CASA also releases automatic locks on unrelated tables open in the same process.
casa-rs yields automatic locks on the input MeasurementSet before its
clearcal provider starts and keeps the caller's table attached. An explicit
or active lock still held by the caller produces a `RuntimeError` before
the task writes. The caller retains ownership of that lock (#1520).

## PlotMS time averaging can dilute complex visibilities when FLOAT_DATA is present

In CASA 6.7.6.14 with casaplotms 2.9.1, plot two Real ordinates from
`CORRECTED_DATA` and `MODEL_DATA` against channel on a MeasurementSet that
also stores `DATA` and `FLOAT_DATA`. With unit visibilities and unit
weights, averaging pairs of integrations produces 0.666667 for both
ordinates. Adding a two-channel average gives the same values.

The mixed-column transform enables time averaging for the float column.
Its accumulator adds observed float weights to the weight spectrum used
to normalize the corrected and model columns. casa-rs accumulates weights
for the requested complex columns and preserves the constant value 1. The
paired fixture checks the selected bins and metadata in both modes (#1032).
[Pinned transform setup](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/mstransform/MSTransform/MSTransformManager.cc?at=9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2)
[Pinned time averager](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/msvis/MSVis/AveragingTvi2.cc?at=9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2)

## PlotMS can stall while exporting phase-shifted coordinates to text

In CASA 6.7.6.14 with casaplotms 2.9.1, plot U in metres against observed
phase for selected XX channels on a complex MeasurementSet. Enable
`transform=True` and set `phasecenter="J2000 00h00m02.00s -29d59m40.00s"`.
The headless task writes the PNG and returns. A text export of the same
phase shift can stop after the selection header and fail to return.

casa-rs exports the selected coordinates and flags to text. The paired
test compares them with pinned PlotMS on a separate MS written by the
CASA `phaseshift` task. Both CASA paths use the same phase-shifting
visibility iterator. The test also compares PNG export using the original
MS and active transform in both public PlotMS tasks (#1032).
[Pinned PlotMS iterator setup](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/src/code/plotms/Data/MSCache.cc?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)
[Pinned phase-shifting iterator](https://open-bitbucket.nrao.edu/projects/CASA/repos/casa6/browse/casatools/src/code/msvis/MSVis/PhaseShiftingTVI.cc?at=9c42dc103aeed74a4a1af2d42f8ef00dfee4abd2)

## PlotMS SPW averaging can repeat the first window's channel selection

In CASA 6.7.6.14 with casaplotms 2.9.1, select `spw="0:0,1:2"`
with `avgspw=True` on an MS with two four-channel windows. The second
window contributes channel 0 to the average. With SPW averaging disabled,
the same selection correctly exports channel 2 from that window.

casa-rs pairs channels by their position within each selected window. The
public-task regression checks the original selection against unaveraged
CASA data, then moves the selected sample to the common index in a separate
CASA input. The aligned CASA average matches the native coordinates.
When the selected channel counts differ, CASA can repeat the first count as
well. casa-rs reports that the selected shapes cannot be averaged together
before writing the export (#1032).
[Pinned PlotMS reducer](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/src/code/plotms/Data/PlotMSVBAverager.cc?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)

## PlotMS can terminate on disjoint channel ranges in two averaged windows

With CASA 6.7.6.14 and casaplotms 2.9.1, set
`spw="0:0~2;5~7,1:0~2;5~7"`, `avgspw=True` and `avgchannel="2"` on
an MS with two eight-channel windows. The transform reports a mismatch
between the number of widths and the number of windows, then the PlotMS
backend terminates. Selecting the disjoint ranges in a single window
exports the two physical bins over channels 0–1 and 5–6.

casa-rs runs the same channel transform for both windows and averages
their output samples by bin position. The regression checks the shared
physical grid against the single-window CASA request and verifies the
two-window output against the weighted complex average (#1032).

With two four-channel windows at 90 and 100 GHz, a virtual model, and
`spw="0:1;200GHz,1:2"` with `avgspw=True`, the pinned PlotMS backend
terminates when it receives the unmatched 200 GHz slice alongside valid
channels. casa-rs skips that empty slice and retains the requested valid
channels. Their averaged coordinates match the CASA control using
`spw="0:1,1:2"` (#1032).

## Empty MS visibility strides can abort or change the attachment

On a nonempty MS, `getdata(increment=4294967297)` can admit a
stride using its low 32 bits and select zero rows using the full stride.
CASA 6.7.6.14 returns empty visibility and time arrays. `flag_sum` can
report an arbitrary second extent, and requesting `u` aborts with
`ArrayConformanceError`. An `axis_info` request raises on the missing
row zero and leaves the attachment with zero selected rows.

casa-rs preserves the empty stride result and returns typed empty UVW
components and flag counts of shape `(0, 0)`. The axis metadata query
raises while retaining the prior active row view (#1030).

With `average=True`, CASA also indexes the missing channel and row
dimensions of the rank-one empty visibility array. The pinned control
reported DATA shape `(0, 1)`. casa-rs returns deterministic averaged shapes
`(0, 0)` for data and `(0, 0, 1)` for flags, with typed empty weights.

On the 30-row control, `getdata(['data', 'DATA'], increment=3, average=True)`
raises because the first reduction consumed its temporary field. CASA then
leaves the 10-row stride selected. casa-rs reports the duplicate averaging
error and retains the original active rows.

## MS visibility averaging uses the wrong row and correlation weights

In CASA 6.7.6.14, `ms.getdata(['data', 'flag', 'weight'], average=True)`
advances a flattened WEIGHT pointer once per row and applies that value
to every correlation and channel. WEIGHT instead has a separate cell for
each correlation and row. For six rows with first-channel correlation 0
values `[1, 9, 17, 25, 33, 41]`, weights `[1, 3, 5, 7, 9, 11]`, and the
second value flagged, CASA returns 29.631578. The unflagged weighted mean
is 30.575758. The stock NGC 5921 `test_getdata_args` also records CASA's
flattened-weight result.

casa-rs applies each unflagged visibility's own row and correlation
weight before projecting its amplitude, phase, real, or imaginary part.
The original test stays verbatim, and an independent weighted regression
checks the corrected scientific result (#1030).
[Pinned casacore averaging loop](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/ms/MSSel/MSSelUtil2.tcc#L127-L147)

## Averaging the same MS sigma field twice can exhaust memory

For an ordinary MS with a nonempty SIGMA matrix, CASA 6.7.6.14
`ms.getdata(['avg_sigma', 'sigma'], average=True)` reduces SIGMA twice.
The second call supplies a vector to a routine that indexes dimensions
of a matrix or cube. On the 30-row control the process consumed one CPU
and grew past 5 GiB of resident memory before it was stopped.

casa-rs computes the root sum of squares over the row axis once and
retains the raw array returned for the literal `avg_sigma` item (#1030).

## MS baseline-axis gap metadata can terminate the process

In CASA 6.7.6.14, `ms.getdata(['axis_info'], ifraxis=True, ifraxisgap=1)`
on a view with several first-antenna groups uses the gap sentinel `-1`
as an ANTENNA row index. The pinned CASA process aborted in a casacore
array slice. casa-rs preserves the `-1` baseline number and puts empty
names and a zero length at each gap. The other axis coordinates remain
available (#1030).

The same invalid access occurs in a row-blocked iterator when
`axis_info` is requested with gaps. The baseline numbers and numeric
visibility record remain readable on pinned CASA when `axis_info` is omitted.

## MS baseline-axis singleton integrations can return invalid coordinates or hang

CASA 6.7.6.14 removes the time dimension of visibility arrays when exactly
one integration is available. Its baseline `flag_sum` still reads that
missing extent. The pinned process did not return from the two-row
control within 30 seconds and was terminated. The `UT` coordinate also
reads a second integration in this case. For a 5-second integration
its result was the absolute MJD seconds instead of seconds from midnight.

casa-rs retains the singleton visibility shape, counts the flags at each
baseline's one integration, and uses that integration to determine
midnight for `UT` (#1030).

## Duplicate MS baseline-axis fields can append spurious integrations

CASA 6.7.6.14 appends a baseline/time block each time an item occurs in
`ms.getdata(items, ifraxis=True)`. A two-integration view queried for
`['time', 'data', 'TIME']` reports four times for two data slices. casa-rs
uses one integration axis for each case-insensitive field name and leaves
the attachment selection intact (#1030).

## The legacy MS baseline number can identify two different antenna pairs

CASA 6.7.6.14 uses `1000 * ANTENNA1 + ANTENNA2` as both the displayed
baseline number and the key for placing data on the `ifraxis` grid.
On a valid MS with 1,002 antennas, baselines `(0, 1001)` and `(1, 1)`
at the same time yield one slot numbered 1001. CASA discards the earlier
pair's visibility and derives the label for both from `(1, 1)`.

casa-rs places and labels data by the actual antenna pair. Both
slots and their separate labels remain available even when their
legacy public baseline numbers coincide (#1030).

## MS baseline-axis averaging can weight the wrong cells and collapse baselines

With `ifraxis=True, average=True`, CASA 6.7.6.14 advances its flattened
WEIGHT pointer once per baseline and integration. The stored WEIGHT
has a separate value for each correlation, baseline and integration.
Zero-filled sparse slots also reach the averaging loop with their flag
clear. On the sparse eight-row control, one measured sample of 89 is
halved to 44.5, other measured samples disappear, and seven output flags
disagree with the independent weighted calculation.

With only one integration, the visibility cube has no final time axis.
CASA interprets its baseline dimension as ordinary rows and collapses
four distinct baselines into one result with no baseline dimension.

casa-rs reduces occupied slots within each antenna pair using each
unflagged sample's correlation weight. It retains the baseline axis
for a single integration and marks a sample with no unflagged
observations flagged. Unchanged upstream tests still record the pinned
CASA results (#1030).
[Pinned casacore averaging loop](https://github.com/casacore/casacore/blob/aaf72eb7edd5a5fecefa78ca04713233b7b6ffd6/ms/MSSel/MSSelUtil2.tcc#L127-L147)

## MS averaged sidereal time can fail when multiple fields are selected

CASA 6.7.6.14 derives the common FIELD_ID as `-1` when a time-averaged
selection spans fields. With `ifraxis=True, average=True`, a request for
`axis_info` and `last` then tries to open FIELD row
`18446744073709551615`. Local apparent sidereal time depends on the epoch
and observatory position. casa-rs computes it in the first antenna's
measure frame at the mean epoch, including when no single field describes
the selection (#1030).

## MS channel selection can accept an index one past the window

On a four-channel MS, CASA 6.7.6.14 accepts
`selectchannel(3, 0, 1, 2)`, which requests channels 0, 2 and 4.
A later `getdata` aborts while using the invalid channel. casa-rs
checks every selected bin against the active spectral windows,
returns false at selection time and retains the previous channel
selection (#1030).

## MS channel selection repeats channels when data descriptions share a window

With two active data descriptions for the same spectral window and four input
channels, CASA 6.7.6.14 returns four DATA and FLAG channels for either
`selectchannel(2, 0, 1, 2)` or `selectchannel(2, 0, 2, 2)`. It adds each
range to the iterator once per description. Both frequency and resolution
metadata still describe two selected channels. casa-rs applies each range
once per spectral window so DATA, FLAG and the reported frequencies have the
requested channel count. It preserves the selected rows and their order
(#1030).

## MS channel averaging mistakes data description IDs for visibilities

After `selectchannel(2, 0, 2, 2)`, CASA 6.7.6.14 raises an incorrect-data-type
error for `getdata('data_desc_id')`. Its flagged-visibility cleanup treats
the integer field as complex data because its name contains `data`. casa-rs
preserves the integer row IDs while averaging the visibility channels
(#1030).

## MS time averages use mismatched flags and weights after Stokes conversion

For an MS recording RR and LL, select `I` and call `getdata` with
`average=True`. CASA 6.7.6.14 passes the one-correlation visibility cube
to its averaging loop with the original two-correlation flags and weights.
The loop walks the flags by the converted data shape and does not select a
weight for each output correlation. With unequal recorded weights and flags,
one controlled MS differed from the propagated-variance calculation by 0.059
normalized RMS on ordinary reads and 0.250 when averaging by baseline.
casa-rs projects visibility flags and inverse variances to the selected
Stokes products for the reduction. Public raw weight and sigma fields retain
their recorded correlation axes, as in CASA (#1030).

## MS channel averaging can abort when Stokes conversion changes the axis size

On an MS with four channels and recorded RR and LL, call
`selectchannel(2, 0, 2, 2)`, `selectpolarization(['I'])` and then
`getdata(['data', 'flag', 'axis_info'])`. CASA 6.7.6.14 aborts with an
uncaught `ArrayConformanceError`. Its flagged-data cleanup reshapes the
one-correlation converted data using the two-correlation recorded flag cube.
casa-rs uses the converted flag axis to zero the masked Stokes values and
returns the recorded FLAG shape and the requested Stokes data shape (#1030).

## MS polarization reads can reuse the wrong recorded correlation order

Interleave two data descriptions for the same spectral window. The first
records `[RR, LL]`. The second records `[LL, RR]` with matching receptor
products. In CASA 6.7.6.14, both `selectpolarization(['RR'])` and conversion
to `['I', 'V']` can use the first description's order for a row in the
second description. For a second-description row with real samples
`[9, 10]`, CASA read RR as 9 and Stokes V as −0.5. casa-rs uses each row's
recorded correlation order, giving RR 10 and V +0.5 (#1030).

## MS iteration can reuse stale row blocking after a cursor is replaced

On an MS with ten rows per integration, call `iterinit(maxrows=7)`
and position the cursor. A subsequent `iterinit()` should restore ordinary
chunk traversal. CASA 6.7.6.14 leaves the previous maxrows flag set and
reads only the first integration of the new outer chunk. Ending the blocked
iterator and then requesting a normal read with selected channels or
correlations can also raise “Attempt to advance subchunk past end of
chunk.” casa-rs keeps row blocking with the cursor that requested it.
Reinitialization and ordinary reads use the requested layout (#1030).

## MS iterator argument errors can replace an active cursor

After starting a blocked cursor with `iterinit(maxrows=7)` and
`iterorigin()`, pass an invalid interval such as `None` or the string
`"15"`, or pass `maxrows=None`. CASA 6.7.6.14 reports a Python
`SystemError` but replaces the active cursor before reporting it.
The next `getdata('time')` raises “VisBuffer not positioned to a
subchunk.” casa-rs retains the positioned cursor when Python argument
conversion fails and returns the same exception class (#1030).

## MS row-blocked baseline reads can lose an integration or average across baselines

With ten baselines per integration, call `iterinit(maxrows=12)`,
`iterorigin()`, and `getdata(['data', 'time'], ifraxis=True)`. The first
block contains ten rows from the first integration and two from the second.
CASA 6.7.6.14 replaces the first two baseline samples with the later values
and reports only the first time. casa-rs returns both times on the integration
axis and retains all twelve samples. With `maxrows=3` and `average=True`,
CASA reduces the baseline axis and returns one value per correlation and
channel. casa-rs averages across time for each baseline and retains the
baseline axis, including flags for absent samples (#1030).

## MS iterator frequency metadata can refer to a different data description

On an MS with four channels in the first data description and three in the
second, start ordinary iteration over all rows before requesting any
visibility record. The first read initializes the ordinary attachment view
to the first description, while the iterator still traverses both. CASA
6.7.6.14 reads three-channel data in the second chunk but reports the first
description's four-channel frequencies. casa-rs reports the frequencies of
the iterator's data description while keeping the ordinary attachment
selection separate (#1030).

## A live Stokes conversion can use the wrong stored hand of an MS iterator

For a MeasurementSet recording RR and LL, select LL before starting and
positioning an iterator, then select I before reading the chunk. CASA
6.7.6.14 keeps the one-correlation flag and weight shapes but can read a
contiguous prefix of the original two-correlation data into the one-hand
shape. This mixes recorded correlations, channels, and rows. With RR
selected, the same test also changes the sample amplitudes. casa-rs
applies the live conversion to the captured data and its physical
correlation label, giving half the recorded parallel-hand visibility
for I when only that hand is present. The reduction uses the
corresponding flags and variances (#1030).

## MS visibility ranges can include a zero from an entirely flagged first block

For a reference view with a fully flagged first block and a later
unflagged sample with real values 9 through 16, CASA 6.7.6.14 reports a
real range of `[0, 16]` with `blocksize=1`. The same rows read with
`blocksize=10` report `[9, 16]`. CASA treats the earlier empty block's
zero-initialized extrema as valid measurements. casa-rs initializes the
range from the first included sample, independent of the block size.
An entirely flagged selection still yields CASA's zero extrema (#1030).

## An entirely flagged FLOAT_DATA range can abort CASA

Call `ms.range('float_data')` when every sample is flagged. CASA 6.7.6.14
aborts with an uncaught `ArrayError` from `MaskedArray::minMax`, which
requires an unmasked sample. casa-rs uses the same zero extrema as an
entirely flagged complex visibility range and keeps the tool attached.
Calling again with `useflags=False` returns the recorded extrema (#1030).

## MS writes can overrun a positioned max-row block

Position an MS cursor with `iterinit(['TIME'], 0.0, 10, False)`, advance
to the second block, and pass its ten-row `getdata('data')` array back to
`putdata`. CASA 6.7.6.14 restarts the inner traversal and attempts to
slice rows 10–19 from the ten-row array. The resulting `ArrayError`
aborts the Python process. casa-rs uses the rows of the positioned
block and writes no other rows (#1030).

## An invalid MS write can leave CASA using the previous selection

On a writable MS, pass a Float32 array for `FLAG_ROW` to `putdata`.
CASA 6.7.6.14 rejects its type after creating an internal iterator.
Selecting only row zero and writing one Boolean row plus a weight vector
on the same attachment can then abort. Closing and reopening before the
second selection succeeds. casa-rs validates the record before starting
a mutation. The error leaves the attachment usable for a new selection
and write (#1030).

## A legacy iterator can report an empty final row block

In CASA 6.7.6.14, select eight rows in the same legacy group and call
`ms.iterinitold(['TIME'], maxrows=4, adddefaultsortcolumns=False)`. After
origin, CASA exposes two four-row blocks and then reports one additional
successful advance with zero selected rows. Casa-rs advances through the
two nonempty blocks and restores the pre-iteration selection when the
cursor is exhausted (#1030).

## Legacy iterator sort output can overwrite an input or escape the MeasurementSet

In CASA 6.7.6.14, open a writable MultiMS and start `iterinitold(['TIME'],
adddefaultsortcolumns=False)`. The call can create a stray
`*SORTED_TABLE` reference in the process's working directory, outside the
MultiMS. On a plain MeasurementSet, opening its existing `SORTED_TABLE`
for update and requesting different legacy sort keys rewrites the saved
reference that was opened as input. Casa-rs iterates those views in
memory and leaves the working directory and input reference intact.
Writable plain MeasurementSets and independent writable references
publish the sorted cache in the physical MeasurementSet (#1030).

## The original MS visibility iterator can return the wrong antenna or abort

CASA 6.7.6.14 returns the `ANTENNA1` column for both antenna fields in
`ms.ngetdata(['antenna1', 'antenna2'])`. Casa-rs returns the recorded
second antenna from `ANTENNA2`. On an open MS before initialization,
`niterend()` and `niternext()` dereference the absent cursor in CASA.
`ngetdata('model_data')` and `ngetdata('corrected_data')` also abort when
the requested column is absent. Casa-rs raises a Python `RuntimeError`
in these cases and keeps the attachment available (#1030).

## MS writes can reorder values read from saved views

Read `getdata('data')` from a saved reference whose rows are out of order,
or from a multi-MS whose members cover the same integration times. With
no explicit iterator, CASA 6.7.6.14 can sort the rows for `putdata` even
though it returned the input in view order. Modified samples then go to
different physical rows or members. casa-rs writes each row to its saved
physical location in view order. Where a reference repeats one physical
row, its final supplied value wins (#1030).

For a single integration, `getdata(ifraxis=True)` drops the singleton
time dimension. On the saved row order `[2, 0, 3]`, CASA 6.7.6.14
returned the three baselines in order `[0, 2, 3]` and swapped physical
rows 0 and 2 when the array was written back unchanged. casa-rs resolves
an ambiguous trailing axis using the most recent read of writable
visibility data. A subsequent ordinary row-axis read restores row
ordering for such arrays (#1030).

## An MS write after baseline downsampling can overwrite omitted rows with zeros

On a writable MeasurementSet, read `getdata('data', ifraxis=True,
increment=2)` and pass its baseline-axis array to `putdata`. CASA
6.7.6.14 restores the full row selection after the read but retains the
sampled baseline axis. It writes zeros to rows on baselines absent from
that axis. In the control, CASA zeros the visibilities in 15 of 30 rows.
casa-rs rejects the sampled baseline-axis record before
mutation. A fresh read with `increment=1` supplies a complete writable
grid (#1030).

## Legacy baseline writes can modify omitted rows and discard the first row

Read a baseline-axis record with `ms.getdataold(..., ifraxis=True,
increment=2)` and write it with `ms.putdataold()`. CASA 6.7.6.14 can
write the first contiguous rows instead of the rows selected by the
stride. Its baseline-axis FLAG_ROW, WEIGHT, and SIGMA writeback also
skips the first row because the inverse mapping tests `row > 0`.
On the stock fixture the first physical WEIGHT and SIGMA cells become
zero even though the supplied baseline record contains nonzero values.
Casa-rs writes only the occupied slots of the captured row projection,
including physical row zero, and rejects a baseline record after that
projection changes (#1030).

## Legacy flag writes can abort on channel bins starting after channel zero

In CASA 6.7.6.14, configure `ms.selectchannelold(2, 1, 2, 3)` and pass
the resulting FLAG shape to `ms.putdataold()`. The legacy writer applies
the starting channel a second time to an already sliced buffer and
aborts. Casa-rs expands each bin into its selected physical channels
once and keeps the channels between bins and outside the selection
unchanged (#1030).

## Legacy Float-data writes can swap recorded correlations

On a MeasurementSet with recorded RR and LL, select `['LL', 'RR']` with
`ms.selectpolarizationold()`. CASA 6.7.6.14 returns FLOAT_DATA in the
requested order but writes an unchanged `getdataold('float_data')`
record in the recorded order. This swaps the two physical correlation
planes. Casa-rs writes each input plane to the recorded correlation
named by the selected order, preserving the original cells on a round
trip (#1030).

## Observed MS statistics take inverse squares of the stored weights

For weighted observed `DATA` and `FLOAT_DATA` statistics, CASA 6.7.6.14's
`Vi2StatsSigmasCubeIterator` computes `1 / sigma²`, but the
`VisBufferImpl2::getSigmaScaled(correlation, channel, row)` it calls reads
`WEIGHT_SPECTRUM` or `WEIGHT`. In a fixture with first-row sigma
`[2, 3]` and weight `[100, 200]`, CASA reports total weight 224.0004
after filtering channel flags. casa-rs reads `SIGMA_SPECTRUM` or `SIGMA`,
reports about 225.19444, and weights the moments by those inverse
variances, as the observed-visibility time-averaging path does (#1030).

## Observed MS statistics time averaging can ignore the flag and weight switches

With `timeaverage=True` in CASA 6.7.6.14, the observed-data accumulator
always reads the FLAG cube and weights data by inverse sigma squared,
regardless of the four `useflags` and `useweights` combinations selected
by the caller. In the controlled three-integration fixture all four
CASA results have mean 126.68421. casa-rs applies both switches before
reducing the averaged samples. With both enabled, it matches CASA.
With both disabled, it reports the analytic unweighted, unmasked
mean 119.5 (#1030).

## MS statistics time averaging can ignore the requested UVW displacement

In CASA 6.7.6.14, `ms.statistics('DATA', 'real', timeaverage=True,
timebin='30s', maxuvwdistance=1.0)` averages integrations whose UVW
positions move by 10 km. The call passes the numeric displacement, but
does not enable the averaging option that uses it. In the three
integration control, CASA reports 80 averaged samples with or without
the limit. casa-rs applies the stated metre limit and returns statistics
for all 240 separate samples (#1030).

## Legacy MS statistics can pad ID columns with invented zero samples

In CASA 6.7.6.14, `ms.statisticsold('FIELD_ID', useflags=False)` appends
one scalar per iterator buffer to an array sized for all selected rows.
The unused elements are zeros. On NGC 5921 the reported mean is about
0.00362 because only 60 buffer IDs contributed to 22,653 array elements.
The same append logic handles `ARRAY_ID` and `DATA_DESC_ID`. casa-rs reads
the physical ID for each selected row and applies the requested row
mask before reducing it (#1030).

## Invalid maximum UVW distance can raise a Python binding SystemError

CASA 6.7.6.14 raises a binding-level `SystemError` for
`ms.statistics('DATA', 'amp', maxuvwdistance='1')`. casa-rs rejects the
nonnumeric argument with `TypeError` and retains the attachment (#1030).

## MS record selection can misread non-native NumPy byte order

In CASA 6.7.6.14 on a little-endian host, `ms.select({'antenna1': v})`
selects antenna 0 from the big-endian Int64 array
`v = numpy.array([1], dtype='>i8')`. On NGC 5921 that gives 1,619 rows
instead of the 1,559 for antenna 1. An exact `times` selection using a
big-endian Float64 array for a known integration selects zero of its 351
rows. casa-rs reads the numeric values according to the array dtype's
byte order and selects the specified antenna and integration (#1030).

## A two-dimensional MS record selection can terminate CASA

In CASA 6.7.6.14, `ms.select({'rows': numpy.array([[1, 2], [3, 4]])})`
can abort with a casacore `ArrayNDimError` when it tries to construct a
vector. casa-rs raises a Python `RuntimeError` when a numeric selection
field has more than one non-singleton axis. It retains earlier successful
fields in the same ordered record. A vector with a singleton axis, such
as shape `(1, 4)`, is accepted (#1030).

## MS summary can return success without writing the report

On an attached MeasurementSet in CASA 6.7.6.14, call
`ms.summary(listfile="missing-directory/report.txt")` with a missing
parent. CASA returns a summary record even though it cannot create the
report. casa-rs raises an I/O error and preserves the attachment so the
caller can retry with a writable path (#1030).

## MS summary misreports a nonnumeric cache-size argument

In CASA 6.7.6.14, calling `ms.summary(cachesize="50")` on a detached
tool raises a Python `SystemError` because the binding returns with a
conversion exception pending. casa-rs reports the invalid argument
with `TypeError`. Numeric arguments, including NumPy floating-point
scalars, remain accepted (#1030).

## A failed visibility listing can leave an output file

In CASA 6.7.6.14, `ms.lister(datacolumn='model', listfile=...)` on a
MeasurementSet without a stored `MODEL_DATA` opens the requested output
before reporting the missing visibility product. The file remains after
the error. casa-rs writes the report to a temporary file beside the
destination and publishes it only after successful rendering, without
replacing an existing output (#1030).

## MS lister can report success for a missing float column or raise a binding error

In CASA 6.7.6.14 on a MeasurementSet without `FLOAT_DATA`,
`ms.lister(datacolumn='float_data', listfile=...)` logs a missing column,
returns true and leaves an empty report. casa-rs reports the missing
product as an error without publishing a report. Passing
`pagerows=numpy.bool_(True)` can also create a report before CASA raises
`SystemError`. casa-rs raises `TypeError` during argument conversion
before touching the output. Python integers, booleans and NumPy integers
retain the usual pagination semantics (#1030).

## MS lister can reject an all-negative float visibility buffer

CASA 6.7.6.14 computes the display amplitude range for
`ms.lister(datacolumn='float_data')` by taking the minimum after masking
negative values. If all samples in a buffer are negative the resulting
empty mask raises an exception. casa-rs prints the signed float samples
with enough space for the sign and magnitude. For example, a visibility
buffer containing only `-7.0` produces a report with `-7.000` for each
sample (#1030).

## MS sorting can overwrite an unrelated output directory

In CASA 6.7.6.14, `ms.sort(newmsname, ['TIME'])` can remove a nonempty
directory at the output name even when it contains unrelated files and
is not a table. casa-rs refuses that destination. It publishes a
complete stage atomically and can replace an otherwise eligible table
or an empty directory (#1030).

## MS sorting a transient selection checks a nonexistent table path

In CASA 6.7.6.14, after `ms.select({'rows': [21, 3, 8, 1]})`,
`ms.timesort('selected.ms')` raises `DOos::freeSpace` for the
unmaterialized table name. casa-rs creates the named copy of the
selected rows and preserves the source attachment. It requires a named
destination for the transient selection to protect unselected rows from
in-place replacement (#1030).

## MS sorting can leave finite keys out of order beside NaNs

In CASA 6.7.6.14, `ms.timesort('sorted.ms')` on TIME keys beginning
`[NaN, 3, NaN, 1, 0, 2]` with the remaining keys equal to `5` can place
`3` and `1` after the `5`s and intermingle them with the NaNs. The
casacore comparator cannot consistently order a NaN against another key.
casa-rs orders the finite keys numerically and places NaNs in deterministic
positions according to their floating-point representation (#1030).

## Legacy MS averaging can apply weights from a different row or baseline

CASA 6.7.6.14 `ms.getdataold('data', average=True)` uses one flat WEIGHT
index for all correlations at each row or baseline and time slot.
`MSSelUtil2::timeAverage` advances that index once per row or slot even
though WEIGHT has a leading correlation axis. For more than one
correlation it therefore uses weights belonging to other inputs. On the
stock `ngc5921_ut.ms`, both engines read identical DATA, FLAG and WEIGHT
arrays, and the error reaches 0.00226 normalized RMS for baseline
averaging. Casa-rs applies the weight belonging to each unflagged
correlation in each row, and matches the independently calculated weighted
average to about 3×10⁻⁸ on that workload (#1030).

For the same legacy baseline reduction, CASA puts the sum of the input
times into the averaged `axis_info.time_axis.MJDseconds`; the main `time`
field already contains their mean. Casa-rs reports the mean in both fields
so the averaged coordinate lies within the observation (#1030).

## Legacy MS polarization selection can abort after channel averaging

On CASA 6.7.6.14 with `ngc5921_ut.ms`, call `ms.selectchannelold(2,0,2,2)`
and then `ms.selectpolarizationold('LL')`. The next
`ms.getdataold(['data', 'axis_info'])` aborts because the legacy selector
retains a sliced channel shape while reinitializing its axis metadata.
Casa-rs clears the old channel slice when initializing polarization
selection and returns the full channel axis for the selected correlation
(#1030).

## A negative gap can crash legacy MS baseline-axis reads

The first call to `ms.getdataold(['data', 'time'], ifraxis=True, ifraxisgap=-2)`
on CASA 6.7.6.14 can segfault. `MSSelector::getData` skips building the
baseline coordinate array for a negative gap, then indexes that array.
Casa-rs treats a negative gap as zero and fills the baseline and
integration axes (#1030).

## Absent visibility columns can abort legacy MS reads

On CASA 6.7.6.14, `ms.getdataold('corrected_data', average=True)`
can segfault when CORRECTED_DATA is absent. Reading
`obs_residual_data` with DATA present and MODEL_DATA absent can also
abort because the subtraction uses incompatible shapes. Casa-rs returns
an empty array of the requested product's type, as CASA does for an
unaveraged read of an absent CORRECTED_DATA column (#1030).

## An unknown legacy polarization can leave empty visibility axes

On CASA 6.7.6.14, `ms.selectpolarizationold('bogus')` returns false but
leaves conversion enabled with an uninitialized converter. The next
visibility read can have no correlation samples while its labels still
identify the recorded correlations. Casa-rs returns false and keeps the
initialized visibility axes usable (#1030).

## A legacy FLOAT_DATA range can abort after polarization conversion

CASA 6.7.6.14 leaves FLOAT_DATA on the recorded correlation axis when
polarization conversion is requested. After `ms.selectpolarizationold('XX')`
on recorded RR and LL, `ms.rangeold('float_data', useflags=True)` aborts
because it tries to apply the converted single-correlation flags to the
two-correlation samples. Casa-rs uses flags on the recorded correlation
axis for these Float samples. It agrees with CASA for `useflags=False`
and returns the extrema of the unflagged recorded samples for
`useflags=True` (#1030).

## Replacing the legacy row selection can abort a complex range

In CASA 6.7.6.14, initialize `ms.selectinitold(1)` for a three-channel
window, replace the selection with a row from a four-channel window via
`ms.selectold({'rows': [0]})` and call `ms.rangeold('real')`. CASA
aborts when assigning the four-channel data to an array sized for three
channels. Casa-rs computes extrema from the actual selected cell.
Spectral metadata still describes the initialized window, as in CASA
(#1030).

## PlotMS exits when adding an atmospheric curve to two visibility ordinates

CASA 6.7.6.14 with casaplotms 2.9.1 terminates its PlotMS process with signal 11 for `xaxis='frequency'`, `yaxis=['real','imag']`, `ydatacolumn=['model','corrected']`, `spw='0:0;3;6'` and `showatm=True` on the paired weather fixture. The public task reports a closed RPC connection and leaves no TXT export. The same failure occurs with two stored ordinates. [Pinned overlay setup](https://open-bitbucket.nrao.edu/projects/CASA/repos/casaplotms/browse/src/code/plotms/Plots/PlotMSPlot.cc?at=e05a21b9b343f67f82b24ec1949ffd9d0367f362)

Casa-rs exports both visibility ordinates and the transmission curve. The paired check compares the visibility axes to CASA's two-ordinate export with transmission disabled and the curve to its single-ordinate transmission export on the same channels. Both checks preserve the virtual model reference and stored visibility cells. [Paired public PlotMS oracle](../tools/casa-compat/plotms_ms_atmosphere_oracle.py)

## The first MS history write can create HISTORY and then fail

With the HISTORY keyword and subtable missing, CASA 6.7.6.14's
`ms.writehistory("first", msname=path)` creates and links a standard HISTORY
table and then raises `NullTable::getColumn - Table object is empty`.
The out-of-date HISTORY handle prevents the first message from being saved.
casa-rs opens the linked table before appending the message. CASA can read
and append to the resulting subtable (#1030).

## An MS TaQL output can erase an unrelated directory

In CASA 6.7.6.14, `ms.selecttaqlold('True GIVING "output.ms"')`
overwrites an existing directory containing ordinary files. A mistyped
output path can therefore destroy unrelated data. casa-rs uses the table
query destination checks and leaves a nonempty non-table directory and the
legacy row selection intact when publication is refused (#1030).

## Legacy MS TaQL can collapse the order of duplicate row IDs

In CASA 6.7.6.14, selecting explicit rows `[3, 7, 8, 3]` with
`ms.selectold()` followed by `ms.selecttaqlold('True ORDERBY ROWID() DESC')`
reports original row numbers `[3, 3, 8, 7]`. Both occurrences of row 3
sort together even though they occupy separate positions in the selector.
casa-rs applies the same view-relative row IDs used by a WHERE predicate
and returns `[3, 8, 7, 3]` (#1030).
