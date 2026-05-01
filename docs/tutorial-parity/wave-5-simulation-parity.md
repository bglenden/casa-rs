# Wave 5 Simulation Parity Evidence

Truth class: current evidence
Last reality check: 2026-04-30
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
| `UVW` max abs diff | `54.63720216499269 m` |
| `UVW` p95 abs diff | `25.084938265168862 m` |
| uv-distance max abs diff | `2.490580885023519 m` |
| uv-distance p95 abs diff | `1.3048159326861395 m` |
| `DATA` max abs diff | `0.0007076460214079025 Jy` |
| `DATA` p95 abs diff | `0.0004260575572766784 Jy` |
| CASA `simobserve` runtime | `5.15946508385241 s` |
| casa-rs debug task runtime | `24.447 s` |
| casa-rs release task runtime | `3.129 s` |

Inspectable artifacts:

- `target/wave5-parity-full/report-3600s/wave5-simulation-parity.json`
- `target/wave5-parity-full/report-3600s/wave5-simulation-parity.png`

## Interpretation

The row schedule, antenna pairing, scalar weights, sigma, and flags now match
the CASA reference for the full first tutorial run. The UV-distance agreement is
close after applying J2000-to-date precession for UVW projection, but component
UVW and predicted visibility values are still outside a closeout tolerance.
Release-mode performance is currently faster than CASA for this full noiseless
`simobserve` workload, while debug-mode runtime is not meaningful as a CASA C++
comparison.

Wave 5 should stay in progress until the UVW/DATA deltas are reduced to an
accepted tolerance, the analysis/imaging products are compared end-to-end, and
the performance result is either improved or explicitly accepted with a shaped
follow-up.
