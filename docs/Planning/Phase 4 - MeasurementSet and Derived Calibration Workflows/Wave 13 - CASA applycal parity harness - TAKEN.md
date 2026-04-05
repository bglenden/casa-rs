# Wave 13 - CASA `applycal` Parity Harness

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Add the first end-to-end parity test that compares Rust calibration
  application against actual CASA `applycal` on a copied real-world
  MeasurementSet.

## Non-goals

- `calwt`.
- Performance benchmarking.
- Solver parity.
- Full-manifest persistent digests including CASA-written HISTORY/auxiliary
  tables outside the current supported mutation surface.

## Scope

### Slow parity coverage

- Generate a CASA `phase.gcal` table from `ngc5921.ms`.
- Copy the shared fixture MS to separate CASA and Rust temp worktrees.
- Apply the CASA-generated caltable with CASA `applycal` to one copy.
- Apply the same caltable with Rust to the other copy using the supported
  `CalFlag` executor path.
- Compare selected-row MAIN state for:
  - `CORRECTED_DATA`
  - `FLAG`
  - `FLAG_ROW`

### Test helpers

- Add reusable slow-test helpers for:
  - copying MeasurementSet directory trees
  - generating a CASA phase-only gain table
  - running CASA `applycal` with the same limited selection surface used by the
    Rust executor test

## Dependencies

- Wave 11 executor core.
- Wave 12 public app registration.
- Existing CASA slow-test discovery conventions.

## Ordering constraints

- This wave should land before `calwt` or benchmark work so the hot path is
  anchored to a real CASA state comparison first.

## Files likely touched

- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Shared CASA python discovery helper exists.
- [x] Shared `ngc5921.ms` fixture path helper exists.
- [x] Executor can already mutate an MS for the supported apply mode.

## Implementation checklist

- [x] Add recursive MeasurementSet copy helper for slow parity tests.
- [x] Add CASA `phase.gcal` generation helper.
- [x] Add CASA `applycal` helper.
- [x] Add end-to-end Rust-vs-CASA apply parity test on `ngc5921.ms`.
- [x] Compare `CORRECTED_DATA`, `FLAG`, and `FLAG_ROW` on the selected rows.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Performance plan

- This wave intentionally stops at correctness parity.
- Benchmarking remains in backlog item 12.4 and should use the same real-MS
  copy/setup path so timing and correctness are measured against the same
  workload.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Real CASA `applycal` state comparison exists for the supported executor
      path.
- [x] Slow tests skip cleanly when CASA python or shared testdata is missing.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
- Follow-ups:
  - `calwt` remains in backlog item 12.4.
  - Performance benchmarking remains the next major execution-focused slice.

## Lessons learned

- Comparing mutated MS state is the right parity boundary for apply work;
  comparing raw caltable contents or command logs would miss the real user
  contract.
- Keeping the comparison focused on supported MAIN columns avoids false
  negatives from CASA-owned HISTORY and other workflow metadata that the Rust
  path does not claim yet.
