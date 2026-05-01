# Wave 5 Simulation Parity Evidence

Truth class: current evidence
Last reality check: 2026-05-01
Verification:
- `/usr/bin/time -p target/release/simobserve --model /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits --out target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms --duration 3600 --integration 2 --overwrite`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s-trace`
- `/usr/bin/time -p target/release/casars-imager --ms target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms --imagename target/wave5-parity-full/images-trace-fast/ppdisk-rust-dirty --imsize 257 --cell-arcsec 0.00311 --dirty-only --weighting natural --no-preview-pngs`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s-trace-fast-images --model-image target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.skymodel --rust-image target/wave5-parity-full/images-trace-fast/ppdisk-rust-dirty.image --casa-image target/wave5-parity-full/images/ppdisk-casa-dirty.image`
- `scripts/run-wave5-issue125.sh target/wave5-issue125`
- `scripts/run-wave5-issue126.sh target/wave5-issue126`
- `scripts/run-wave5-issue126-panels.sh target/wave5-issue126-panels`

## Tutorial Command

The current Wave 5 parity gate targets the first noiseless VLA
protoplanetary-disk simulation tutorial run:

- model: `ppdisk672_GHz_50pc.fits`
- peak model brightness: `3e-5 Jy/pixel`
- center frequency: `44 GHz`
- bandwidth: `128 MHz`
- array: `vla.a.cfg`
- direction: `J2000 18h00m00.031s -22d59m59.6s`
- integration: `2s`
- total time: `3600s`
- thermal noise: disabled

The CASA reference is generated with CASA 6.7.5-9 `simobserve`. The casa-rs
run uses the `simobserve` task binary and the same model, spectral setup, VLA A
configuration, phase center, time sampling, and brightness scaling.

## Current Result

The current implementation is not yet review-complete for Wave 5 because the
generated products are structurally comparable but not numerically identical.

| Check | Result |
|---|---:|
| CASA rows | `631800` |
| casa-rs rows | `631800` |
| `TIME` max abs diff | `0.0 s` |
| `ANTENNA1` / `ANTENNA2` max abs diff | `0` |
| `WEIGHT` / `SIGMA` max abs diff | `0.0` |
| `FLAG` mismatches | `0` |
| `UVW` max abs diff | `0.000005175632395548746 m` |
| `UVW` p95 abs diff | `0.0000025352687771373843 m` |
| uv-distance max abs diff | `0.0000050816415750887245 m` |
| uv-distance p95 abs diff | `0.0000026658349270292084 m` |
| `DATA` max abs diff | `0.000002990868069109164 Jy` |
| `DATA` p95 abs diff | `0.00000001848952399365207 Jy` |
| `DATA` p99.9 abs diff | `0.00000002115352009497353 Jy` |
| `DATA` cells above `1e-6 Jy` | `2 / 1263600` |
| amplitude max abs diff | `0.000002981557732448432 Jy` |
| amplitude p95 abs diff | `0.0000000013969462114198434 Jy` |
| dirty image max abs diff | `0.0000024915789254009724 Jy/beam` |
| dirty image p95 abs diff | `0.0000006156158633530138 Jy/beam` |
| dirty image p99.9 abs diff | `0.0000018601072952151507 Jy/beam` |
| CASA `simobserve` ptgfile repro runtime | `6.45 s` |
| casa-rs release task runtime | `4.417 s internal / 5.54 s wall` |
| casa-rs dirty-image runtime | `2.05 s wall` |

Inspectable artifacts:

- `target/wave5-parity-full/report-3600s-trace/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s-trace/wave5-simulation-parity.png`
- `target/wave5-parity-full/report-3600s-trace-fast-images/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s-trace-fast-images/wave5-simulation-parity.png`
- `target/wave5-parity-full/report-3600s-trace-fast-images/wave5-simulation-image-panel.png`

The image-panel artifact shows the imported model, casa-rs dirty image, CASA
C++ dirty image, and `casa-rs - CASA C++` dirty-image residual on matched
257-pixel, `0.00311arcsec` natural-weight MFS products. It is generated from
the `casars-imager --dirty-only` product and a CASA `tclean(niter=0,
gridder="standard", usepointing=False)` product for the same reference run.

## Interpretation

The row schedule, antenna pairing, scalar weights, sigma, and flags now match
the CASA reference for the full first tutorial run. CASA source inspection and
casatools instrumentation showed that simulator UVW is not a sky-direction
conversion: `NewMSSimulator::calcAntUVW` converts `MBaseline` antenna vectors
through `ITRF -> HADEC -> TOPO -> APP -> JNAT -> J2000`, constructs
`MVuvw(baseline, phase_center)`, and subtracts per-antenna UVW coordinates for
each row. Matching that shape reduced component UVW from metre-level residuals
to sub-mm residuals.

Release-mode performance is again comparable with CASA because casa-rs now
computes per-antenna UVW once per integration instead of doing a measures
conversion per baseline row.

The CASA `.skymodel` pixels match the scaled FITS model. Source inspection shows
`Simulator::predict` drives `SkyEquation`, `GridFT`, and `VPSkyJones`; `GridFT`
negates `u`/`v` before degridding, pads the 257-pixel model with CASA's default
`1.3` GridFT padding, chooses a composite FFT size of 360, divides by
`ConvolveGridder::correctX1D`, and applies `LatticeFFT::cfft2d` before
degridding. Matching those details, plus the CASA simulator RA handedness,
`modifymodel` image-center phase offset, and VLA Q-band primary-beam screen,
reduced the full-tutorial complex `DATA` p95 residual to `1.85e-8 Jy`.

The current source-backed trace fixed the remaining order-of-magnitude UVW
error by matching casacore's legacy `MCEpoch::UT1_GAST`, polar-motion Euler
order, inverse apparent-to-J2000 direction shifts, and `MeasMath::rotateShift`
path. The simulator predictor also now rotates UVW from the field phase center
into the model-image tangent frame before degridding, matching
`FTMachine::rotateUVW` when the image center differs from the pointing center.

The CASA reference is reproducible from a clean `simobserve` run with an
explicit ptgfile and no `indirection`; that rerun matches the existing CASA MS
exactly for `DATA`, `UVW`, `TIME`, flags, weights, and sigma. A rerun with
`indirection` instead changes only the skymodel image center and produces a
global complex phase difference, so the reference semantics are now pinned.

Strict max-absolute `DATA` allclose at `1e-6 Jy` is still false because one
baseline row, both correlations, remains above `1e-6 Jy`. The remaining row is
row `54358` (`ANTENNA1=16`, `ANTENNA2=25`), where casa-rs predicts
`0.00043928137 + 0.00032850710i Jy` and CASA predicts
`0.00044181067 + 0.00033010333i Jy`. Instrumentation showed that using CASA's
reference UVW, CASA's tabulated VLA-Q Airy voltage-pattern lookup, and a
Fortran-`sectdgrid`-style combined weight normalization does not move this row.
The standalone C++ `ConvolveGridder` + `LatticeFFT` shim agrees with the Rust
predictor at float precision for this sample, while CASA production
`GridFT::get` remains `2.99e-6 Jy` higher. Wave 5 should stay in progress until
that final `GridFT`/`SkyEquation` production-path difference is either matched
or accepted with an explicit tutorial tolerance.

## Issue #125 Analysis and Imaging Evidence

The #125 harness runs the first VLA ppdisk tutorial analysis slice end-to-end
from generated MeasurementSets:

- casa-rs `simobserve` creates a fresh synthetic MS from the tutorial FITS
  model.
- CASA `tclean(niter=0, specmode="mfs", gridder="standard",
  weighting="natural")` images the CASA reference MS.
- `casars-imager --dirty-only` images the casa-rs synthetic MS with matching
  `257` pixel, `0.00311arcsec` natural-weight MFS parameters.
- CASA `imhead`/`imstat` and casa-rs `imexplore imhead`/`imexplore imstat`
  inspect the resulting images.
- CASA `plotms` is attempted for the amplitude-vs-uv-distance product. In this
  headless run it fails because `DISPLAY` is unset, so the script records that
  error and produces the CASA-side plot from CASA `casatools.table` data with
  matplotlib. The casa-rs side is rendered by `msexplore`.
- A missing-MS check confirms the imager fails before writing image products
  for invalid synthetic-MS input.

Artifacts:

- `target/wave5-issue125/wave5-issue125-analysis-summary.json`
- `target/wave5-issue125/wave5-issue125-image-panel.png`
- `target/wave5-issue125/wave5-issue125-plot-panel.png`
- `target/wave5-issue125/casa-imhead.json`
- `target/wave5-issue125/rust-imhead.json`
- `target/wave5-issue125/casa-imstat.json`
- `target/wave5-issue125/rust-imstat.json`

Measured result:

| Check | Result |
|---|---:|
| CASA image shape | `[257, 257, 1, 1]` |
| casa-rs image shape | `[257, 257, 1, 1]` |
| CASA image units | `Jy/beam` |
| casa-rs image units | `Jy/beam` |
| dirty image max abs diff | `2.4915789254009724e-6 Jy/beam` |
| dirty image RMS abs diff | `3.4122472299780043e-7 Jy/beam` |
| dirty image relative RMS diff | `6.51337996907232e-4` |
| `imstat` max abs diff | `3.3760443329811096e-9 Jy/beam` |
| `imstat` min abs diff | `1.1146767064929008e-6 Jy/beam` |
| `imstat` mean abs diff | `1.6597811675756044e-7 Jy/beam` |
| `imstat` RMS abs diff | `6.553763823671267e-8 Jy/beam` |
| `imstat` npts diff | `0` |
| invalid MS check | failed before image products, as expected |
| CASA `tclean` runtime | `2.146216791123152 s` |
| casa-rs `casars-imager` runtime | `2.897235166048631 s` |
| CASA plot export fallback runtime | `0.20892916596494615 s`; real `plotms` unavailable because `DISPLAY` is unset |
| casa-rs `msexplore` plot runtime | `6.284246832830831 s` |

The #125 image and statistics products match at the same residual scale as the
accepted #124 synthetic-MS residual. The visible panels show the same dirty
image morphology and the same amplitude-vs-uv-distance structure. The original
#125 run exposed a casa-rs imaging runtime of `23.14154116716236 s`; profiling
showed almost all of that time was repeated MFS frequency-frame conversion
during `prepare_plane_input/accumulate_rows`. Caching the MFS frequency scale by
row time and field reduced the same command to `2.897235166048631 s` without
changing the image or statistics residuals. The plot timing remains labeled as
a headless artifact comparison because CASA `plotms` cannot run in this local
environment without `DISPLAY`.

## Issue #126 Corruption Evidence

The #126 harness covers the simulator-tool corruption slice needed for tutorial
examples without trying to clone the open-ended simulator corruption catalog.
It runs the VLA ppdisk model with `120s`, `2s` integrations, and four channels
so channel-dependent effects are visible:

- casa-rs clean synthetic MS.
- casa-rs noise+gain synthetic MS with `--noise-stddev-jy 0.001`,
  `--gain-amplitude-stddev 0.05`, and `--gain-phase-stddev-rad 0.02`.
- casa-rs common-corruption synthetic MS with the same noise+gain plus
  bandpass, parallel-hand polarization leakage, and a global primary-beam
  pointing offset.
- CASA simulator reference made by copying the same clean MS and running
  `sm.setnoise(mode="simplenoise", simplenoise="0.001Jy")`,
  `sm.setgain(mode="fbm", amplitude=[0.05, 0.02])`, and `sm.corrupt()`.

Artifacts:

- `target/wave5-issue126/wave5-issue126-corruption-summary.json`
- `target/wave5-issue126/rust-clean-report.json`
- `target/wave5-issue126/rust-noise-gain-report.json`
- `target/wave5-issue126/rust-common-corruptions-report.json`
- `target/wave5-issue126/rust-clean-timing.json`
- `target/wave5-issue126/rust-noise-gain-timing.json`
- `target/wave5-issue126/rust-common-corruptions-timing.json`
- `target/wave5-issue126/casa-noise-gain-timing.json`
- `target/wave5-issue126-panels/wave5-issue126-noise-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-gain-phase-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-leakage-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-bandpass-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-pointing-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-panel-summary.json`
- `target/wave5-issue126-panels/wave5-issue126-noise-residual-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-gain-phase-time-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-bandpass-channel-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-leakage-visibility-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-pointing-impact-panel.png`
- `target/wave5-issue126-panels/wave5-issue126-tutorial-panel-summary.json`

Measured result:

| Check | Result |
|---|---:|
| clean rows | `21060` |
| rust noise+gain rows | `21060` |
| rust common-corruption rows | `21060` |
| CASA noise+gain rows | `21060` |
| data shape | `[2, 4, 21060]` |
| rust noise+gain component stddev delta | `0.0010213215136900544 Jy` |
| CASA noise+gain component stddev delta | `0.0012523955665528774 Jy` |
| rust common-corruption component stddev delta | `0.0010408269008621573 Jy` |
| rust noise+gain mean amplitude ratio | `1.258807897567749` |
| CASA noise+gain mean amplitude ratio | `1.2593351602554321` |
| rust common-corruption mean amplitude ratio | `1.25957453250885` |
| rust clean runtime | `1.2037884159944952 s` |
| rust noise+gain runtime | `0.18237316608428955 s` |
| rust common-corruption runtime | `0.1811619158834219 s` |
| CASA noise+gain corruption runtime | `0.12549133296124637 s` |

The rust and CASA noise+gain runs are not expected to be cell-identical because
CASA's simulator uses its own random generator and `setgain(mode="fbm")`
implementation. The comparison pins the same clean input MS, same seed value,
same simple-noise sigma, same gain RMS parameters, row/shape preservation, and
similar corrupted-data statistics. The broader casa-rs common-corruption run
demonstrates deterministic bandpass, polarization leakage, and primary-beam
pointing-offset controls. CASA `setbandpass` is documented as not implemented
in the simulator tool XML, and CASA pointing corruption requires an external
pointing-error table, so #126 keeps those to the practical native tutorial
surface rather than inventing a broad calibration-table workflow.

The per-effect panel harness renders dirty-image panels for each bounded
corruption type. Noise and gain/phase have direct CASA simulator comparison
panels. Leakage is rendered as a casa-rs impact panel because CASA's
`setleakage(mode="constant", amplitude=[0.01, 0.0])` fails on the two-correlation
tutorial MS with `JonesGenLin matrix apply (J::aR) incompatible with VisVector`.
Bandpass and pointing are also rendered as casa-rs impact panels because CASA
documents `setbandpass` as not implemented and `setpointingerror` requires an
external pointing-error table.

Per-effect summary:

| Effect | CASA direct panel | rust delta component stddev | CASA delta component stddev | rust/CASA RMS data diff |
|---|---|---:|---:|---:|
| noise | yes | `0.0010002946946769953 Jy` | `0.0010000548791140318 Jy` | `0.002000108826905489 Jy` |
| gain/phase | yes | `0.0002072835195576772 Jy` | `0.0007518390193581581 Jy` | `0.0011220132000744343 Jy` |
| leakage | no, CASA fails on two-correlation MS | `2.8077792535441404e-07 Jy` | n/a | n/a |
| bandpass | no, CASA `setbandpass` not implemented | `0.00017571824719198048 Jy` | n/a | n/a |
| pointing | no, CASA requires pointing-error table | `8.060932259468245e-07 Jy` | n/a | n/a |

Tutorial-style diagnostic panels are also generated because the CASA simulation
guides emphasize residual/fidelity images, image statistics, `plotms`-style
visibility plots, and channelized visual checks rather than only dirty-image
comparisons:

| Diagnostic | Artifact | Result |
|---|---|---:|
| noise residual image and residual histogram | `target/wave5-issue126-panels/wave5-issue126-noise-residual-panel.png` | rust residual RMS `2.360620283261362e-06 Jy/beam`; CASA residual RMS `2.48095804209798e-06 Jy/beam` |
| gain/phase amplitude ratio and phase offset vs time | `target/wave5-issue126-panels/wave5-issue126-gain-phase-time-panel.png` | direct CASA and casa-rs visibility-domain comparison |
| bandpass amplitude ratio and phase offset vs channel | `target/wave5-issue126-panels/wave5-issue126-bandpass-channel-panel.png` | native casa-rs channel-dependent signature |
| polarization-leakage visibility delta by correlation | `target/wave5-issue126-panels/wave5-issue126-leakage-visibility-panel.png` | native casa-rs visibility-domain signature; CASA direct path unavailable on this two-correlation MS |
| pointing primary-beam impact | `target/wave5-issue126-panels/wave5-issue126-pointing-impact-panel.png` | production `2/-1 arcsec` offset image RMS `1.5238555183271463e-07 Jy/beam`; visualization `20/-10 arcsec` offset image RMS `0.00017008024922316966 Jy/beam` |
