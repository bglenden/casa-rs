# Wave 21 - `gaincal` `solint` and Preapply Expansion

## Origin

- Backlog item: 12.5 (partial).

## Goal

- Extend the limited `gaincal` implementation beyond global solves by adding
  broader `solint` support and prior-caltable preapply while keeping the same
  acceptance contract: CASA-compatible caltable output and downstream parity
  checked via application.

## Non-goals

- `combine=*` semantics.
- Additional solve families beyond `G` / `T`.
- `bandpass`, `fluxscale`, or inspection work.
- Full solver-kernel factoring; note the cleanup need in backlog instead.

## Scope

### Public/library solve surface

- Extend `GainSolveRequest` with:
  - `GainSolveInterval::{Infinite, Integration, Seconds(f64)}`
  - prior-caltable preapply through existing apply-table specs
- Keep the solve family otherwise narrow:
  - `gaintype=G|T`
  - `calmode='p|ap'`
  - explicit refant
  - `smodel=[I,0,0,0]`

### Solve grouping and preapply

- Group solves by `(observation, field, spw, scan, solve bucket)` rather than a
  single global bucket.
- Support:
  - `solint='inf'`
  - `solint='int'`
  - explicit seconds-valued solve intervals
- Reuse the existing non-mutating apply planner/executor path to evaluate prior
  calibration tables before solving residual gains.

### Regression and parity coverage

- Add synthetic downstream tests for:
  - integration-bucket solves
  - fixed-seconds solve grouping
  - residual solves after prior `G` preapply
- Extend the slow CASA parity harness with real-MS downstream comparisons for:
  - `gaincal(..., solint='int')`
  - `gaintype='T'` phase solves with prior `G` preapply

## Dependencies

- Wave 20 `calmode='ap'` expansion.
- Existing downstream apply parity harness in `casa_calibration_parity.rs`.
- Existing apply planner/executor library surface for preapply reuse.

## Files touched

- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/tests/gain_solve.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add failing synthetic tests for integration and fixed-seconds solve
      grouping.
- [x] Add a failing synthetic residual-solve test using a prior caltable.
- [x] Extend the public request surface with broader `solint` and prior-caltable
      preapply.
- [x] Rework solve grouping around explicit solve buckets.
- [x] Reuse the non-mutating apply path to evaluate prior calibration tables for
      residual solving.
- [x] Add real CASA downstream parity tests for `solint='int'` and `T` with
      prior `G` preapply.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Interpretation

- Broader `solint` landed without widening the fundamental solve algebra or the
  on-disk compatibility contract.
- Prior-caltable preapply now reuses the same planner/executor stack as
  `calibrate` instead of growing a second bespoke evaluator inside the solver.
- The next leverage under `12.5` is now structural rather than functional:
  split solver math from grouping/IO/caltable writing so the numerical kernel
  can be improved or swapped more safely, then decide whether to widen
  semantics such as `combine=*`.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS

## Follow-ups

- Factor the solver into cleaner kernel/grouping/writer boundaries.
- Decide whether `combine=*` is worth adding before moving on to `bandpass`.
- Reevaluate whether any part of the handwritten solver should move onto a
  stronger reusable linear-algebra backend once the factoring cleanup lands.
