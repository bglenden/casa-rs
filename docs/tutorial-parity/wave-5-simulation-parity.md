# Wave 5 Simulation Parity Evidence

Truth class: current evidence
Last reality check: 2026-05-01
Verification:
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s`
- `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python scripts/wave5-simulation-parity.py target/wave5-parity-full/ppdisk.rust.vla.a.3600s.release.ms target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms target/wave5-parity-full/report-3600s-release`

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
| `UVW` max abs diff | `0.00046243144424806815 m` |
| `UVW` p95 abs diff | `0.00022301637018244934 m` |
| uv-distance max abs diff | `0.0002631813404150307 m` |
| uv-distance p95 abs diff | `0.00016473680607305118 m` |
| `DATA` max abs diff | `0.000020302598486117852 Jy` |
| `DATA` p95 abs diff | `0.000000018524588085711002 Jy` |
| `DATA` p99.9 abs diff | `0.000000021139580825483745 Jy` |
| `DATA` cells above `1e-6 Jy` | `14 / 1263600` |
| amplitude max abs diff | `0.00002001160520101919 Jy` |
| amplitude p95 abs diff | `0.000000001565668994213572 Jy` |
| CASA `simobserve` runtime | `5.15946508385241 s` |
| casa-rs debug task runtime | `24.447 s` |
| casa-rs release task runtime | `4.024 s` |

Inspectable artifacts:

- `target/wave5-parity-full/report-3600s/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s/wave5-simulation-parity.png`
- `target/wave5-parity-full/report-3600s-release/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s-release/wave5-simulation-parity.png`

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

Strict max-absolute `DATA` allclose at `1e-6 Jy` is still false because 14 of
1263600 complex cells remain above `1e-6 Jy`, with the largest outlier
`2.03e-5 Jy` on a low-amplitude visibility near a model null. The normal row
population is now at numerical-noise scale, but Wave 5 should stay in progress
until the near-null outliers are either traced to the same source-backed
standard or explicitly accepted as a bounded tutorial tolerance.
