## Wave 34 - `bandpass` Combine-Scan Parity

### Goal

Extract the first `bandpass(..., combine='scan')` slice from the remaining
`12.6` follow-on work into the implemented calibration surface.

### Scope

- Extend the limited `B Jones` solve path to combine rows across scan
  boundaries when requested
- Keep the existing narrow solve surface:
  - `solint='inf'`
  - explicit refant
  - prior gain-table preapply
  - point-source `smodel=[I,0,0,0]`
- Revalidate the real CASA `bandpass` parity suite with the corrected helper
  script rather than trusting earlier false-green runs

### Implemented

- `BandpassSolveRequest` now carries `combine_scans`
- `solve-bandpass --combine scan` now maps onto that same library surface
- `build_bandpass_groups()` now keys on scan number only when scan combining is
  disabled
- The shared CASA `run_casa_bandpass()` helper was fixed so the slow parity
  suite actually executes CASA `bandpass` instead of skipping on a malformed
  Python snippet
- Added a new real-MS parity case for:
  - `bandpass(..., combine='scan', gaintable=[prior G])`
  - evaluated on the multi-scan `field=1, spw=0` subset of `ngc5921.ms`

### Validation

- `cargo test -p casa-calibration --test bandpass_solve -- --nocapture`
- `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity bandpass -- --nocapture`
- `cargo test -p casa-calibration`
- `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`

### Notes

- The accepted downstream parity contract for this combine-scan `B Jones` case
  uses a solver-specific tolerance on `ngc5921.ms field=1, spw=0`
- Sampled `CPARAM` values between CASA and Rust stay close; the remaining
  spread shows up only after downstream application on the harder multi-scan
  real-MS workload
- The broader slow `bandpass` parity surface was rerun after fixing the CASA
  helper, including:
  - baseline `B Jones`
  - `solnorm`
  - `parang`
  - `BPOLY`
