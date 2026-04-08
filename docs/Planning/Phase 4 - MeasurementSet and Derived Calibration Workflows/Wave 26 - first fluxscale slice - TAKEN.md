# Wave 26 - First `fluxscale` Slice

## Origin

- Backlog item: 12.6 (partial).
- Follow-on from Wave 25.

## Goal

- Land the first CASA-compatible `fluxscale` workflow slice on top of the
  existing calibration-table substrate and gain-table writer.

## Non-goals

- CLI or schema/UI surfacing for `fluxscale`
- spectral fitting beyond the single-SPW case
- `solnorm`
- `combine=*`
- `BPOLY`

## Scope

### Library surface

- Add a library-first `fluxscale` API in `casa-calibration`.
- Support:
  - complex `CPARAM` gain tables (`G Jones`, `T Jones`)
  - reference and transfer field selectors by id, exact name, or simple `*`
    glob
  - optional `refspwmap`
  - optional `gainthreshold`
  - full scaled fluxtable output or incremental correction-factor output

### Output

- Copy the input gain table tree and mutate only the transfer-field `CPARAM`
  values in the output table.
- Emit a machine-readable report with per-field/per-SPW transfer fluxes and
  fitted single-SPW reference frequencies.

### Validation

- Add synthetic tests for non-incremental and incremental table writing.
- Add a slow CASA parity test on a real `ngc5921.ms` amplitude-gain case.

## Dependencies

- Existing complex gain-table reader/writer
- Existing slow CASA parity harness

## Files touched

- `crates/casa-calibration/src/fluxscale.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/fluxscale.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add the library-first `fluxscale` request/report/error types.
- [x] Implement field selection, SPW mapping, gain-threshold filtering, and
      output-table mutation.
- [x] Add synthetic regression tests for scaled and incremental outputs.
- [x] Add a slow CASA parity helper for `fluxscale`.
- [x] Close slow parity on a real `ngc5921.ms` case.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo test -p casa-calibration --test fluxscale`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity fluxscale_matches_casa_on_ngc5921_gain_table -- --nocapture`

## Interpretation

- This wave keeps the app layer thin and pushes all `fluxscale` behavior into a
  library API with explicit CASA parity coverage.
- The first cut intentionally stays inside the existing gain-table family and
  avoids broader spectral-fit semantics until `solnorm` / `combine=*` decisions
  are made.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo test -p casa-calibration --test fluxscale` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity fluxscale_matches_casa_on_ngc5921_gain_table -- --nocapture` -> PASS

## Follow-ups

- Decide whether to surface `fluxscale` through the public `calibrate` app
  before or after the remaining `12.6` solve semantics.
- Keep `solnorm`, `combine=*`, `BPOLY`, and richer UI/schema work explicit in
  the backlog rather than as code TODOs.
