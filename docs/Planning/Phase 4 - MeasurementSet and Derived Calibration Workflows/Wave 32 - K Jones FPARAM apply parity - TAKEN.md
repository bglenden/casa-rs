## Wave 32 - `K Jones` `FPARAM` Apply Parity

### Goal

Extract the first float-parameter calibration-table slice from backlog item
`12.7` by supporting CASA `K Jones` delay tables in the public `calibrate`
apply path.

### Scope

- Extend the apply executor to read `FPARAM`-backed `K Jones` tables
- Materialize delay rows as diagonal complex Jones factors at data-channel
  frequencies using the caltable spectral-window pivot frequency
- Keep the scope narrow to apply support only; do not broaden into generic
  float-parameter solving
- Add synthetic planner/executor regressions and real CASA parity on
  `ngc5921.ms`

### Implemented

- `casa-calibration` apply execution now supports:
  - complex `CPARAM` tables
  - float `FPARAM` tables with `VisCal="K Jones"`
- Delay application follows CASA's `KJones::calcAllJones()` convention:
  per receptor and channel, apply
  `exp(i * 2pi * delay_ns * (freq_hz - ref_freq_hz) / 1e9)`
- Delay tables do not participate in `calwt`; the executor ignores `calwt`
  requests for delay-like tables, matching CASA's effective behavior
- Synthetic helpers can now write minimal `K Jones` caltables for fast tests
- Slow parity now covers:
  - CASA `gaincal(..., gaintype='K')`
  - CASA `applycal` on one MS copy
  - Rust `execute_apply_from_path()` on another copy
  - selected-row `CORRECTED_DATA` / flag comparison

### Validation

- `cargo test -p casa-calibration`
- `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_k_delay_matches_casa_applycal_on_ngc5921_subset -- --nocapture`

### Notes

- Real CASA `K Jones` tables expose `FPARAM` and `FLAG` as `[nReceptor, 1]`
  cells. The executor supports both 1-D and 2-D delay payloads.
- This wave intentionally does not attempt broader float-parameter families.
  Those remain deferred under backlog item `12.7`.
