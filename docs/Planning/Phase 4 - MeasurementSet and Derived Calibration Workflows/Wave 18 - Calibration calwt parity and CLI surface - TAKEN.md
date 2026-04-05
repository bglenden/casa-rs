# Wave 18 - Calibration calwt Parity and CLI Surface

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Close the remaining functional gap in the first public `calibrate`
  apply workflow by implementing `calwt` with real CASA parity coverage.

## Non-goals

- Apply-path performance optimization.
- Solver work (`gaincal`, `bandpass`, `fluxscale`).
- `gainfield='nearest'`, `callib`, or other deferred calibration surfaces.

## Scope

### Executor `calwt` support

- Remove the executor's hard rejection of `calwt`.
- Update `WEIGHT` for diagonal gain application when `calwt=true`.
- When `WEIGHT_SPECTRUM` exists, mirror CASA's apply path by seeding the
  working channelized weights from `WEIGHT`, then collapsing back to
  `WEIGHT` after per-channel scaling.

### Public CLI exposure

- Add a `--calwt BOOL[,BOOL...]` surface to the public `calibrate` app.
- Support one value for all gaintables or one value per gaintable.
- Preserve the same request surface in the developer `plan-apply` subcommand.

### Regression and parity coverage

- Add synthetic executor tests for:
  - direct `WEIGHT` updates
  - `WEIGHT_SPECTRUM` updates with `WEIGHT` reduction
- Extend the slow CASA parity harness to compare `WEIGHT` and
  `WEIGHT_SPECTRUM` where present.
- Add a real `applycal(..., calwt=True)` parity test on `ngc5921.ms`.

## Dependencies

- Wave 13 CASA apply parity harness.
- Wave 17 executor/planner/timing closeout.

## Files touched

- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/tests/apply_execute.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add red tests for `WEIGHT` and `WEIGHT_SPECTRUM` updates.
- [x] Expose `--calwt` in the public CLI and plan subcommand.
- [x] Implement executor-side `calwt` for the current diagonal apply path.
- [x] Align the `WEIGHT_SPECTRUM` behavior with CASA's reset-from-`WEIGHT`
      semantics on the tested MS.
- [x] Extend slow parity to compare weights and verify real CASA `calwt`.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Interpretation

- The synthetic `calwt` path was initially easy to implement, but the first
  real CASA parity run showed that using the stored `WEIGHT_SPECTRUM` as the
  starting point was wrong on `ngc5921.ms`.
- CASA's apply path effectively resets the working channelized weights from
  `WEIGHT` before calibration-weight updates, so matching CASA required
  mirroring that behavior instead of trusting the stored
  `WEIGHT_SPECTRUM` payload.
- With this wave complete, the remaining work under `12.4` is performance
  follow-up rather than functional correctness for the current v1 apply
  surface.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS

## Follow-ups

- Keep performance work for `12.4` focused on planning overhead and
  storage-layer main-table persistence.
- If later calibration families reveal different weight semantics
  (`bandpass`, `Tsys`, etc.), extend parity coverage before broadening the
  generic `calwt` claims.
