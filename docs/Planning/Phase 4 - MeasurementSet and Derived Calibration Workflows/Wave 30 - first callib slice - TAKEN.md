# Wave 30 - First `callib` Slice

## Origin

- Backlog item: 12.7 (partial extraction).
- Follow-on from Wave 29.

## Goal

- Land a narrow, CASA-checked callibrary path on top of the existing apply
  planner/executor without inventing a separate application engine.

## Non-goals

- `intent`, `reach`, `obsmap`, or `antmap`
- Full CASA callibrary grammar
- `parang`

## Scope

### Library surface

- Add a callibrary parser that maps supported CASA directives directly onto
  `ApplyCalibrationTableSpec`.
- Support:
  - `caltable`
  - `calwt`
  - per-entry `field`, `spw`, `obs`
  - `fldmap`
  - `spwmap`
  - supported `tinterp` / `finterp` combinations

### Apply plumbing

- Add per-table applicability selection to the apply planner/executor so
  callibrary entries can target subsets of selected MS rows.
- Surface callibrary input through `calibrate apply` and `calibrate plan-apply`
  via `--callib`.

### Validation

- Add fast parser and relative-path tests.
- Add fast planner/executor tests for per-entry applicability.
- Add a slow CASA `docallib` parity case on a real phase-gain workload.

## Dependencies

- Existing apply planner/executor
- Existing phase-gain apply parity workload on `ngc5921.ms`

## Files touched

- `crates/casa-calibration/src/callib.rs`
- `crates/casa-calibration/src/plan.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/callib.rs`
- `crates/casa-calibration/tests/apply_plan.rs`
- `crates/casa-calibration/tests/apply_execute.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add a narrow callibrary parser to the library crate.
- [x] Add per-table applicability selection to the apply planner/executor.
- [x] Surface `--callib` through the public `calibrate` apply/plan commands.
- [x] Add fast parser/applicability tests.
- [x] Close a slow real-MS CASA `docallib` parity case.

## Test plan

- [x] `cargo test -p casa-calibration callib -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_phase_gain_via_callib_matches_casa_applycal_on_ngc5921_subset -- --nocapture`

## Interpretation

- This wave keeps callibrary support honest by mapping only the part that can be
  represented by the existing apply substrate and parity-checked against CASA.
- Unsupported directives stay explicit errors instead of silent no-ops.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration callib -- --nocapture` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_phase_gain_via_callib_matches_casa_applycal_on_ngc5921_subset -- --nocapture` -> PASS

## Follow-ups

- Keep `parang`, float-parameter table families, `BPOLY`, and broader plotting
  work explicit in the backlog rather than as code TODOs.
