# Wave 11 - Calibration Apply Executor Core

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Add the first `applycal`-class execution core in `casa-calibration` so the
  library can mutate an MS using an existing apply plan, create
  `CORRECTED_DATA` when absent, and support `Trial`, `CalOnly`, and `CalFlag`
  behavior for the initial complex-diagonal workflow.

## Non-goals

- Public `casars` registration of `calibrate`.
- `calwt`.
- `gainfield='nearest'`, `callib`, `parang`, or solving.
- Full CASA executor parity and performance closeout.

## Scope

### Executor library

- Add an executor entry point that consumes an `ApplyPlanRequest` or pre-built
  `ApplyPlan`.
- Create `CORRECTED_DATA` on demand.
- Load complex `CPARAM` caltables and apply diagonal gains to selected MAIN
  rows.
- Support `ApplyMode::Trial`, `ApplyMode::CalOnly`, and `ApplyMode::CalFlag`.
- Treat missing antenna solutions as non-fatal execution conditions:
  - `CalFlag`: flag affected samples
  - `CalOnly`: leave data/flags unchanged for those samples

### Tests/docs

- Add synthetic executor regression coverage for:
  - `CORRECTED_DATA` creation
  - corrected-visibility writeback
  - `CalFlag` behavior when solutions are missing
- Record the partial extraction of backlog item 12.4.

## Dependencies

- Wave 9 caltable substrate.
- Wave 10 apply planner.

## Ordering constraints

- This wave must land before public `casars` registration so the app boundary
  represents real MS mutation rather than summary-only behavior.

## Files likely touched

- `crates/casa-calibration/`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Planner output reviewed for executor inputs.
- [x] Non-goals documented.
- [x] Synthetic MS/caltable fixture helpers available.

## Implementation checklist

- [x] Add executor entry points and outcome/error types.
- [x] Create `CORRECTED_DATA` when absent.
- [x] Load complex diagonal caltable rows into executor-friendly lookup/index
      structures.
- [x] Apply gains to selected rows for the supported correlation layouts.
- [x] Support `Trial`, `CalOnly`, and `CalFlag`.
- [x] Treat missing antenna solutions as flag/skip conditions instead of hard
      failures.
- [x] Add synthetic executor regression tests.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration --all-targets -- -D warnings`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`
- [ ] Executor parity against CASA `applycal` on a real MS copy.
- [ ] End-to-end performance benchmark against CASA apply paths.

## Performance plan

- Keep executor logic structured around planner output and row-level processing
  so chunked parallel execution can be added without reshaping the public API.
- Do not gate this wave on universal CASA outperformance; capture that work in
  the remaining 12.4 backlog.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo/CLI surface remains developer-only and accurate to capability.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
- Follow-ups:
  - Public `casars` registration remains deferred until the app can honestly be
    presented as calibration rather than a developer-only library/CLI surface.
  - `calwt`, CASA `applycal` parity, and benchmarks stay in backlog item 12.4.

## Lessons learned

- The planner/executor split paid off immediately: executor behavior for
  missing antenna solutions could be fixed without destabilizing the selection
  and mapping logic.
- Missing-solution handling is part of the apply contract, not just an error
  path, so it deserves explicit regression coverage before CASA parity work.
