# Wave 31 - `parang` Apply and Solve Parity

## Origin

- Backlog item: 12.7 (partial extraction).
- Follow-on from Wave 30.

## Goal

- Extract parallactic-angle handling from the deferred calibration backlog into
  the implemented apply and solve surface, while keeping the public app thin
  and checking the behavior against CASA on real MeasurementSets.

## Non-goals

- Linear-feed `P Jones` application
- `FPARAM`-family caltables
- `BPOLY`
- New plotting or stats UX

## Scope

### Derived support

- Extend the derived-calculation engine with mount-aware parallactic-angle
  behavior matching CASA for:
  - `ALT-AZ`
  - `EQUATORIAL`
  - `ORBITING`
  - `X-Y`
  - `ALT-AZ+NASMYTH-R`
  - `ALT-AZ+NASMYTH-L`
  - `ALT-AZ+BWG-R`
  - `ALT-AZ+BWG-L`
- Add feed-angle support by combining mount-aware parallactic angle with
  `FEED::RECEPTOR_ANGLE`.

### Apply path

- Add `parang` to apply planning and execution.
- Support circular-feed correlation layouts through the same diagonal
  calibration executor.
- Reject linear-feed `parang` use explicitly rather than silently applying the
  wrong algebra.

### Solve path

- Allow `gaincal` / `bandpass` preapply to use `parang` even when there are no
  prior calibration tables.
- Thread `parang` through the limited `gaincal` and `bandpass` request surfaces
  so solve parity can be checked through downstream application.

### Validation

- Add fast regression tests for circular-feed `P Jones` phase factors and the
  planner allowance for `parang`-only preapply.
- Add slow CASA parity for:
  - `applycal(..., parang=True)` on the standard `ngc5921.ms` workload
  - `applycal(..., parang=True)` after rewriting antenna mounts to BWG
  - `gaincal(..., parang=True)` through downstream CASA `applycal`
  - `bandpass(..., parang=True, gaintable=[prior G])` through downstream CASA
    `applycal`

## Dependencies

- Existing apply planner/executor and real-MS parity harness
- Existing limited `gaincal` and `bandpass` surfaces
- Existing derived hour-angle / parallactic-angle engine

## Files touched

- `crates/casa-ms/src/derived/engine.rs`
- `crates/casa-calibration/src/plan.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/src/solve/grouping.rs`
- `crates/casa-calibration/src/bandpass.rs`
- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/apply_plan.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add mount-aware and feed-angle-aware parallactic-angle support to the
  derived engine.
- [x] Add `parang` to apply planning and execution for circular-feed layouts.
- [x] Allow `parang`-only solve preapply without requiring a dummy caltable.
- [x] Add fast regression coverage for circular `P Jones` behavior.
- [x] Close the slow real-MS CASA parity cases for apply, `gaincal`, and
  `bandpass`.

## Test plan

- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_phase_gain_with_parang_matches_casa_applycal_on_ngc5921_subset -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity apply_phase_gain_with_parang_matches_casa_applycal_on_bwg_mounts -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_phase_gain_with_parang_matches_casa_gaincal_downstream_via_casa_applycal -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_bandpass_with_prior_gain_and_parang_matches_casa_bandpass_downstream -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Interpretation

- This wave keeps the public app thin while putting the new math and
  mount/feed semantics in library code.
- The accepted contract remains CASA-checked downstream behavior on real MS
  copies, not hand-waved analytic agreement.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS

## Follow-ups

- Keep linear-feed `parang`, `FPARAM`, `BPOLY`, and broader inspection work
  explicit in the backlog rather than as code TODOs.
