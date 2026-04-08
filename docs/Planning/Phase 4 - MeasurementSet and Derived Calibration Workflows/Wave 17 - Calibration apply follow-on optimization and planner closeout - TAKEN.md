# Wave 17 - Calibration Apply Follow-on Optimization and Planner Closeout

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Keep pushing the benchmarked `applycal`-class path toward CASA parity by
  attacking the dominant fixed costs left after Wave 16.

## Non-goals

- `calwt`.
- New calibration features.
- A storage-layer redesign for incremental or column-targeted main-table
  persistence.

## Scope

### Trusted main-table save path

- Add a `Table::save_assuming_valid()` entry point that reuses the borrowed-row
  save path but skips a redundant full-table validation pass.
- Add `MeasurementSet::save_main_table_only_assuming_valid()` and route the
  calibration apply executor through it.

### Planner timing breakdown

- Split the coarse `planning_ns` timing into:
  - selection
  - selected-row expansion
  - MS spectral-window loading
  - calibration-table planning
- Surface those timings through the JSON/text executor report and the benchmark
  harness.

### Selection follow-ups

- Try a column-wise structured selection path that preloads only the needed
  scalar columns.
- Try a slot-indexed single-pass row scan for the same structured selection
  workload.
- Probe the existing TaQL path using `--msselect` on the benchmarked
  `field=0, spw=0` selection.

## Dependencies

- Wave 16 fixed-cost optimization pass.

## Files touched

- `crates/casa-calibration/src/cli.rs`
- `crates/casa-calibration/src/execute.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/src/plan.rs`
- `crates/casa-ms/src/ms.rs`
- `crates/casa-ms/src/selection.rs`
- `crates/casa-tables/src/storage/mod.rs`
- `crates/casa-tables/src/table/io.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`
- `scripts/bench-calibrate-vs-casa.sh`

## Implementation checklist

- [x] Add a borrowed-row trusted save path at the table layer.
- [x] Use a trusted main-table-only save path from the calibration executor.
- [x] Add planner subphase timings to the executor report and benchmark script.
- [x] Re-benchmark to identify the dominant planner subphase.
- [x] Try the structured-selection follow-up ideas and keep only the best one.
- [x] Re-run parity and benchmark checks from the final kept state.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy -p casa-tables -p casa-ms -p casa-calibration --all-targets -- -D warnings`
- [x] `cargo test -p casa-tables flush_writes_changes_to_disk`
- [x] `cargo test -p casa-ms save_main_table_only_persists_main_mutations_without_rewriting_subtables`
- [x] `cargo test -p casa-ms apply_field_and_spw_selection_returns_only_intersection`
- [x] `cargo test -p casa-calibration`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`
- [x] `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`

## Performance results

- Previous Wave 16 smoke benchmark:
  - Rust median: `0.715000s`
  - CASA median: `0.092148s`
  - Rust/CASA ratio: `7.759`
- Early Wave 17 trusted-save benchmark:
  - Rust median: `0.580000s`
  - CASA median: `0.089377s`
  - Rust/CASA ratio: `6.489`
  - Rust planning median: `0.193349s`
  - Rust save median: `0.342060s`
- Final kept Wave 17 smoke benchmark:
  - Rust median: `0.600000s`
  - CASA median: `0.096214s`
  - Rust/CASA ratio: `6.236`
  - Rust planning median: `0.206561s`
  - Rust planning selection median: `0.202205s`
  - Rust selected-row expansion median: `0.001213s`
  - Rust MS spectral-window planning median: `0.000514s`
  - Rust per-caltable planning median: `0.002593s`
  - Rust save median: `0.349944s`

## Interpretation

- Skipping the duplicate full-table validation pass was a real improvement and
  should stay.
- The new planner timings show that `MsSelection::apply` dominates the planning
  cost on the benchmarked workload; row expansion and caltable planning are not
  material bottlenecks.
- The two plausible local follow-ups did not beat the current structured
  selection path:
  - the slot-indexed single-pass row scan regressed the benchmark
  - the TaQL `--msselect` path was slower than the current structured
    `--field/--spw` selection on the same workload
- With selection alternatives and row-kernel work largely exhausted, the
  remaining path to meaningful parity improvement is deeper main-table
  persistence work rather than another small planner tweak.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-tables -p casa-ms -p casa-calibration --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-tables flush_writes_changes_to_disk` -> PASS
  - `cargo test -p casa-ms save_main_table_only_persists_main_mutations_without_rewriting_subtables` -> PASS
  - `cargo test -p casa-ms apply_field_and_spw_selection_returns_only_intersection` -> PASS
  - `cargo test -p casa-calibration` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh` -> PASS

## Follow-ups

- Keep `calwt` deferred within item 12.4.
- If more apply-performance work is required in Phase 4, focus it on
  storage-layer main-table persistence rather than planner micro-optimizations.
- Any deeper persistence work should preserve the current RR/RC/CR/CC interop
  contract for on-disk CASA compatibility.
