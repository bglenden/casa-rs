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
| `DATA` max abs diff | `0.00012725180545661448 Jy` |
| `DATA` p95 abs diff | `0.00003876001790270493 Jy` |
| amplitude max abs diff | `0.0001270262737504222 Jy` |
| amplitude p95 abs diff | `0.00003824164161010807 Jy` |
| CASA `simobserve` runtime | `5.15946508385241 s` |
| casa-rs debug task runtime | `24.447 s` |
| casa-rs release task runtime | `4.654 s` |

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

The remaining correctness gap is in the image model prediction. The CASA
`.skymodel` pixels match the scaled FITS model. Applying the CASA simulator RA
handedness convention, CASA `modifymodel` image-center phase offset, and CASA's
composite padded FFT grid reduced the complex `DATA` p95 residual by an order of
magnitude from the first full-tutorial comparison. Complex visibility parity is
still outside a closeout tolerance, with the remaining residual now dominated by
amplitude rather than phase. Wave 5 should stay in progress until the remaining
`sm.predict`/`ConvolveGridder` amplitude convention is traced to the same
standard as the UVW path, or a narrower accepted tolerance is explicitly defined
for this tutorial slice.
