# Wave 5 Simulation Parity Evidence

Truth class: current evidence
Last reality check: 2026-05-01
Verification:
- `/usr/bin/time -p target/release/simobserve --model /Users/brianglendenning/SoftwareProjects/casa-tutorial-data/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits --out target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms --duration 3600 --integration 2 --overwrite`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s-trace`
- `/usr/bin/time -p target/release/casars-imager --ms target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms --imagename target/wave5-parity-full/images-trace/ppdisk-rust-dirty --imsize 257 --cell-arcsec 0.00311 --dirty-only --weighting natural --no-preview-pngs`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.trace.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s-trace-images --model-image target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.skymodel --rust-image target/wave5-parity-full/images-trace/ppdisk-rust-dirty.image --casa-image target/wave5-parity-full/images/ppdisk-casa-dirty.image`
- `scripts/run-wave5-issue125.sh target/wave5-issue125`

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
| casa-rs dirty-image runtime | `22.83 s wall` |

Inspectable artifacts:

- `target/wave5-parity-full/report-3600s-trace/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s-trace/wave5-simulation-parity.png`
- `target/wave5-parity-full/report-3600s-trace-images/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s-trace-images/wave5-simulation-parity.png`
- `target/wave5-parity-full/report-3600s-trace-images/wave5-simulation-image-panel.png`

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
| CASA `tclean` runtime | `2.1315181250683963 s` |
| casa-rs `casars-imager` runtime | `23.14154116716236 s` |
| CASA plot export runtime | `0.21042108279652894 s` with recorded `plotms` display fallback |
| casa-rs `msexplore` plot runtime | `5.899880249984562 s` |

The #125 image and statistics products match at the same residual scale as the
accepted #124 synthetic-MS residual. The visible panels show the same dirty
image morphology and the same amplitude-vs-uv-distance structure. Performance
is not yet competitive for imaging or plotting; that is recorded here as
evidence for Wave 7/#130 rather than hidden by the #125 closeout.
