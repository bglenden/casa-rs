# Wave 12 - Public `calibrate` App Registration

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Promote `calibrate` from a developer-only calibration CLI to a public
  schema-backed one-shot `casars` app that can honestly run the existing apply
  workflow.

## Non-goals

- `calwt`.
- Custom structured `casars` renderer support for calibration output.
- Broader CASA `applycal` parity and performance closeout.

## Scope

### Public CLI/app surface

- Add a public default `calibrate` CLI mode that applies existing calibration
  tables to an MS.
- Keep `summary` and `plan-apply` as explicit developer subcommands.
- Add a `--ui-schema` contract for the public apply surface.
- Register `calibrate` in `casars` as a one-shot app.

### Public apply contract

- Require an MS path and one or more caltable paths.
- Support `apply_mode=calflag|calonly|trial`.
- Support a limited selection surface aligned with the current
  `MsSelection`-based executor inputs:
  - `field`
  - `spw`
  - `antenna`
  - `scan`
  - `observation`
  - `array`
  - `timerange`
  - `msselect`

### Tests/docs

- Add CLI schema/parse tests in `casa-calibration`.
- Add `casars` registry/launcher tests for the new app.
- Update Phase 4 tracking to record the public-app extraction.

## Dependencies

- Wave 11 apply executor core.
- Existing `casars` schema-backed one-shot app model.

## Ordering constraints

- This wave comes after the executor core so the public app is not just a
  summary or planning shell.

## Files likely touched

- `crates/casa-calibration/`
- `crates/casars/`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Executor entry point exists and mutates an MS for the supported workflow.
- [x] Public app naming settled on `calibrate`.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add a public default apply mode to `calibrate`.
- [x] Add `--ui-schema` support for the public apply surface.
- [x] Keep developer subcommands explicit instead of overloading the public app
      contract.
- [x] Register `calibrate` in `casars`.
- [x] Add launcher and schema regression tests.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration -p casars --all-targets -- -D warnings`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casars calibrate_load_schema_describes_public_apply_surface`
- [x] `cargo test -p casars launcher_lists_registered_apps_in_expected_order`
- [x] `cargo test -p casars launcher_screen_renders_available_apps`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`
      to confirm the unchanged CASA table-read parity path still passes after
      public-app registration.

## Performance plan

- No new execution kernels were added in this wave; keep the app layer thin and
  avoid copying calibration logic into `casars`.
- Broader parity/benchmark work remains in backlog item 12.4.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Public app registration matches actual capability.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration -p casars --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
  - `cargo test -p casars calibrate_load_schema_describes_public_apply_surface` -> PASS
  - `cargo test -p casars launcher_lists_registered_apps_in_expected_order` -> PASS
  - `cargo test -p casars launcher_screen_renders_available_apps` -> PASS
- Follow-ups:
  - `calwt` remains deferred inside backlog item 12.4.
  - CASA `applycal` parity and performance benchmarking remain the next major
    execution-focused slice.

## Lessons learned

- The `casars` schema-backed one-shot model is a good fit for calibration as
  long as the app stays thin and the library owns all workflow logic.
- The current schema model favors single-value form fields, so multi-caltable
  input is best represented as a single comma-separated field until a richer
  launcher field type exists.
