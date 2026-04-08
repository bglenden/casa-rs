# Wave 28 - `gainfield='nearest'` Parity

## Origin

- Backlog item: 12.7 (partial extraction).
- Follow-on from Wave 27.

## Goal

- Promote `gainfield='nearest'` from deferred backlog into the supported apply
  surface with explicit CASA parity evidence.

## Non-goals

- `callib`
- `parang`
- Any new plotting or browser UI

## Scope

### Planner behavior

- Extend apply-table planning to resolve `gainfield='nearest'` per selected MS
  field using FIELD phase directions.
- Preserve the resolved nearest-field mapping in the apply plan for text/JSON
  inspection and later execution.

### Executor behavior

- Use the resolved nearest calibration field during per-row calibration-table
  lookup.

### Validation

- Add a synthetic planner regression.
- Add a synthetic executor regression.
- Add a slow real-MS parity test against CASA `applycal(..., gainfield='nearest')`.

## Dependencies

- Existing apply planner/executor
- Existing FIELD-direction access in MS and caltable subtables
- Existing slow applycal parity harness

## Files touched

- `crates/casa-calibration/src/plan.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/apply_plan.rs`
- `crates/casa-calibration/tests/apply_execute.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add a `Nearest` gainfield selector to the apply plan model.
- [x] Resolve nearest calibration fields from phase directions.
- [x] Thread the resolved mapping through execution.
- [x] Extend CLI text output to show nearest-field mappings.
- [x] Add synthetic planner/executor regressions.
- [x] Close slow real-MS parity against CASA.

## Test plan

- [x] `cargo test -p casa-calibration --test apply_plan plan_apply_resolves_gainfield_nearest_by_field_direction`
- [x] `cargo test -p casa-calibration --test apply_execute execute_apply_uses_nearest_gainfield_mapping`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_nearest_gainfield_matches_casa_on_real_ms -- --nocapture`

## Interpretation

- This wave keeps `gainfield='nearest'` as a planner policy layer, not a new
- calibration algebra feature.
- The important acceptance contract is still downstream MS mutation parity with
  CASA, not just agreement on the chosen nearest field ids.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration --test apply_plan plan_apply_resolves_gainfield_nearest_by_field_direction` -> PASS
  - `cargo test -p casa-calibration --test apply_execute execute_apply_uses_nearest_gainfield_mapping` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_nearest_gainfield_matches_casa_on_real_ms -- --nocapture` -> PASS

## Follow-ups

- Keep `callib`, `parang`, `FPARAM`, `BPOLY`, and broader plotting/UI work in
  backlog rather than as code TODOs.
