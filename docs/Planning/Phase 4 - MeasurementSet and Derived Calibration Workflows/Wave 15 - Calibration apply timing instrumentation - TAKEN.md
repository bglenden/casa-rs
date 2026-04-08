# Wave 15 - Calibration Apply Timing Instrumentation

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Add internal timing instrumentation to the Rust `applycal`-class path so
  follow-on optimization work is driven by measured cost centers rather than
  wall-clock guesses.

## Non-goals

- Performance optimization itself.
- `calwt`.
- New calibration features.

## Scope

### Executor timings

- Extend `ApplyExecutionReport` with a structured timing breakdown covering:
  - planning
  - MeasurementSet open
  - `CORRECTED_DATA` creation/seeding
  - correlation metadata lookup
  - calibration-table load/index
  - row compute
  - row writeback
  - MeasurementSet save
  - total wall-clock time represented by the report

### CLI output

- Surface the timing breakdown in the text report.
- Preserve JSON output so scripts can consume the structured timings.

### Benchmark script

- Update the benchmark harness to capture per-run Rust JSON reports.
- Report medians for the internal timing fields alongside the end-to-end median.

## Dependencies

- Wave 11 executor core.
- Wave 12 public `calibrate` CLI.
- Wave 14 benchmark harness.

## Ordering constraints

- This wave should land before any optimization wave so the first performance
  pass is anchored to measured fixed costs and compute costs.

## Files likely touched

- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/src/lib.rs`
- `scripts/bench-calibrate-vs-casa.sh`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Reproducible benchmark harness exists.
- [x] Apply execution already returns structured JSON.
- [x] Benchmark non-goals documented.

## Implementation checklist

- [x] Add structured timing fields to the execution report.
- [x] Measure the main fixed-cost phases separately from row compute/writeback.
- [x] Surface timings in text output and JSON output.
- [x] Extend the benchmark harness to aggregate timing medians.
- [x] Run a smoke benchmark and record the first breakdown.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-calibration --all-targets -- -D warnings`
- [x] `cargo test -p casa-calibration`
- [x] `bash -n scripts/bench-calibrate-vs-casa.sh`
- [x] `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`

## Performance results

- Command:
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`
- Workload:
  - `ngc5921.ms`
  - CASA-generated `phase.gcal`
  - `field=0`
  - `spw=0`
  - `apply_mode=calflag`
  - timing excludes MS copy and caltable generation
- Results:
  - Rust end-to-end median: `0.860000s`
  - Rust reported total median: `0.836244s`
  - CASA median: `0.090487s`
  - Rust/CASA ratio: `9.504`
  - Rust planning median: `0.219655s`
  - Rust `CORRECTED_DATA` seeding median: `0.202096s`
  - Rust save median: `0.400758s`
  - Rust row compute median: `0.007215s`
  - Rust row writeback median: `0.000903s`
- Interpretation:
  - The dominant overhead is fixed-cost work around planning, scratch-column
    setup, and save/write persistence, not the per-row correction kernel.
  - The next optimization wave should focus on avoiding full-table rewrites and
    reducing repeated metadata/planning overhead before tuning the row kernel.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Internal timing data is available in the public apply report.
- [x] Benchmark harness reports timing medians for the Rust path.
- [x] The dominant performance costs are documented explicitly.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `bash -n scripts/bench-calibrate-vs-casa.sh` -> PASS
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh` -> PASS
- Follow-ups:
  - Optimize planning, `CORRECTED_DATA` creation/seeding, and save/write
    overhead before broadening feature scope.
  - Keep `calwt` deferred within item 12.4.

## Lessons learned

- Wall-clock parity alone was not enough; the timing split changes the next
  optimization target from row math to table-level fixed costs.
- The Rust-reported total tracks the shell wall-clock closely enough to use as
  the main internal profiling signal in follow-on work.
