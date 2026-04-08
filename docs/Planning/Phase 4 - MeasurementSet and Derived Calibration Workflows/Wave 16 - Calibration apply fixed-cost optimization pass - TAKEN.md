# Wave 16 - Calibration Apply Fixed-Cost Optimization Pass

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Reduce the biggest fixed-cost overheads identified by Wave 15 without
  changing public behavior or relaxing CASA interoperability guarantees.

## Non-goals

- `calwt`.
- New calibration features.
- Storage-layer redesign for incremental main-table persistence.

## Scope

### Executor path cleanup

- Avoid opening the MeasurementSet twice in `execute_apply_from_path`.
- Plan against the already-open MeasurementSet instance, then execute against
  that same handle.

### Main-table-only save path

- Add a `MeasurementSet::save_main_table_only()` API for workflows that mutate
  only MAIN columns/keywords and leave subtables untouched.
- Use that API from the calibration apply executor instead of rewriting every
  subtable directory.

### Selection fast path experiment

- Add a direct structured-selection path in `MsSelection::apply` when there is
  no raw TaQL expression, so simple `field`/`spw`/`scan`-style filters can be
  evaluated without going through a view query.
- Validate with a regression test covering combined `field` + `spw` filtering.

## Dependencies

- Wave 11 executor core.
- Wave 15 timing instrumentation.

## Ordering constraints

- This wave should land before deeper storage work so the next optimization
  pass starts from the cleaned-up executor path rather than avoidable reopen and
  subtable rewrite overhead.

## Files touched

- `crates/casa-calibration/src/execute.rs`
- `crates/casa-ms/src/ms.rs`
- `crates/casa-ms/src/selection.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Implementation checklist

- [x] Reuse one MS open across planning and execution.
- [x] Add and test `save_main_table_only()`.
- [x] Route calibration apply through the main-table-only save path.
- [x] Add a structured-selection fast path and a regression test.
- [x] Re-run parity and benchmark checks.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-ms -p casa-calibration --all-targets -- -D warnings`
- [x] `cargo test -p casa-ms save_main_table_only_persists_main_mutations_without_rewriting_subtables`
- [x] `cargo test -p casa-ms apply_field_and_spw_selection_returns_only_intersection`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`
- [x] `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`

## Performance results

- Previous Wave 15 smoke benchmark:
  - Rust median: `0.860000s`
  - CASA median: `0.090487s`
  - Rust/CASA ratio: `9.504`
- Wave 16 smoke benchmark:
  - Rust median: `0.715000s`
  - CASA median: `0.092148s`
  - Rust/CASA ratio: `7.759`
  - Rust planning median: `0.223020s`
  - Rust `CORRECTED_DATA` setup median: `0.009564s`
  - Rust save median: `0.445879s`
  - Rust row compute median: `0.007450s`
  - Rust row writeback median: `0.001017s`

## Interpretation

- The main-table-only save path removed most of the old
  `CORRECTED_DATA` setup overhead and produced a meaningful end-to-end
  improvement.
- The direct structured-selection path did not materially move the planning
  number on the benchmarked workload, so the remaining planning cost is not
  primarily the TaQL query/view path.
- The next optimization target is still dominated by:
  - main-table save/rewrite cost
  - apply-plan construction beyond raw selection

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-ms -p casa-calibration --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-ms save_main_table_only_persists_main_mutations_without_rewriting_subtables` -> PASS
  - `cargo test -p casa-ms apply_field_and_spw_selection_returns_only_intersection` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh` -> PASS

## Follow-ups

- Keep `calwt` deferred within item 12.4.
- Investigate whether the remaining planning cost is row-plan construction,
  caltable spectral-window planning, or other fixed metadata work.
- Evaluate whether the main-table persistence substrate needs a more targeted
  write path for apply-style mutations.
