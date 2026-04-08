# Wave 29 - `gaincal combine='scan'` Parity

## Origin

- Backlog item: 12.5 (partial).
- Follow-on from Wave 28.

## Goal

- Land the first broader `gaincal` solve semantics slice by supporting
  `combine='scan'` through the existing limited `G` / `T` solver.

## Non-goals

- Other `combine=*` modes
- New solve families beyond the existing `G` / `T` surface
- Any solver UI expansion

## Scope

### Solver behavior

- Extend solve grouping so scan boundaries can be ignored when requested.
- Keep the numerical solve kernel, caltable writing, and downstream acceptance
  contract unchanged.

### Validation

- Add a synthetic regression showing that identical gains across two scans
  collapse to one solution group when `combine_scans=true`.
- Add a slow real-MS downstream parity test against CASA
  `gaincal(..., combine='scan')` followed by `applycal`.

## Dependencies

- Existing limited `gaincal` solver
- Existing slow CASA `gaincal` / `applycal` parity harness

## Files touched

- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/src/solve/grouping.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/gain_solve.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add the request surface for scan-combined solving.
- [x] Thread the flag into solve grouping.
- [x] Extend the CASA helper to run `gaincal(..., combine='scan')`.
- [x] Add a synthetic grouping/downstream regression.
- [x] Close slow real-MS downstream parity against CASA.

## Test plan

- [x] `cargo test -p casa-calibration --test gain_solve solve_gain_phase_g_combine_scans_writes_one_solution_group_across_scans -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_phase_gain_with_combine_scan_matches_casa_gaincal_downstream -- --nocapture`

## Interpretation

- This wave expands solve grouping semantics without changing the accepted
  output contract: downstream corrected-data parity after application remains
  the deciding signal.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration --test gain_solve solve_gain_phase_g_combine_scans_writes_one_solution_group_across_scans -- --nocapture` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_phase_gain_with_combine_scan_matches_casa_gaincal_downstream -- --nocapture` -> PASS

## Follow-ups

- Continue any broader `combine=*` work under `12.5` and `12.6` as separate
  waves rather than extending this one silently.
