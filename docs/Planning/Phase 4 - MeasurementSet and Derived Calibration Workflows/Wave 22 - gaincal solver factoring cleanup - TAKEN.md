# Wave 22 - `gaincal` Solver Factoring Cleanup

## Origin

- Backlog item: 12.5 (partial).

## Goal

- Split the limited `gaincal` implementation into cleaner internal library
  boundaries so future solve work does not continue to accumulate in one
  monolithic handwritten module.

## Non-goals

- Broaden solve semantics such as `combine=*`.
- Change the public solve API.
- Change the downstream parity contract.
- Add new solver families beyond the existing `G` / `T` surface.

## Scope

### Internal library boundaries

- Keep `solve.rs` as the public orchestration layer and request/error surface.
- Move selection, bucketing, and accumulation into `solve/grouping.rs`.
- Move the numerical graph solver into `solve/kernel.rs`.
- Move caltable writing and output-root preparation into `solve/writer.rs`.

### Behavior preservation

- Keep the accepted solve surface unchanged:
  - `gaintype=G|T`
  - `calmode='p|ap'`
  - `solint='inf'|'int'|<seconds>`
  - explicit refant
  - prior-caltable preapply
- Preserve the existing synthetic downstream tests and real CASA downstream
  parity suite as the acceptance gate.

## Dependencies

- Wave 21 broader `solint` and prior-preapply support.
- Existing CASA parity harness in `casa_calibration_parity.rs`.

## Files touched

- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/src/solve/grouping.rs`
- `crates/casa-calibration/src/solve/kernel.rs`
- `crates/casa-calibration/src/solve/writer.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add internal solve submodules for grouping, kernel, and writer concerns.
- [x] Keep the public API exported from `solve.rs` unchanged.
- [x] Preserve all existing synthetic solve/apply tests.
- [x] Preserve the real CASA downstream parity suite unchanged.

## Test plan

- [x] `cargo test -p casa-calibration`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Interpretation

- The solver is now structurally easier to extend: solve grouping, the
  numerical kernel, and on-disk caltable emission can evolve more independently.
- This is a cleanup wave, not a new capability wave. The value is reduced
  coupling and lower risk for the next `gaincal` expansions.
- The next leverage under `12.5` is now semantic rather than structural:
  decide whether to widen solve semantics such as `combine=*`, or whether to
  improve the numerical backend behind the existing kernel.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS

## Follow-ups

- Decide whether `combine=*` is worth adding before moving to `bandpass`.
- Reevaluate whether the current graph-based kernel should eventually sit on a
  stronger reusable linear-algebra backend.
- Keep any broader solve semantics behind the same downstream CASA parity
  acceptance contract.
