# Wave 14 - taql command-line executor

## Origin

- Backlog item: 22.1.

## Goal

- Provide a `taql` CLI entry point for interactive and single-command TaQL
  execution.

## Non-goals

- `showtableinfo`/other CLI tools.
- Additional TaQL language features.

## Scope

### Read path

- Read commands from stdin or command argument.
- Execute queries through existing TaQL APIs.

### Write path

- Optional output table writes only via TaQL command semantics.

### API/docs/demo

- New executable and usage docs.

## Dependencies

- Stable `execute_taql` behavior and printable result formatting.

## Files likely touched

- `crates/casacore-tables/examples/taql.rs` or `src/bin/taql.rs`
- `crates/casacore-tables/src/table.rs` (if glue needed)
- CLI tests/snapshots

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop is CLI output comparison:
  1. Run same query via Rust `taql` CLI and C++ `taql` CLI, compare formatted output
  - Note: Output format differences are expected; compare data values, not exact formatting.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined — 100-query CLI throughput, threshold 2× C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add CLI binary with stdin/arg modes.
- [x] Add result pretty-printer.
- [x] Add basic integration tests.
- [ ] Add C++ TaQL interop CLI output comparison for this wave.
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Run command mode on fixture table.
- [x] Run stdin mode on fixture table.
- [x] Validate output and error paths.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) — CLI output comparison.
- [ ] Data values match between Rust and C++ CLI output.

## Performance plan

- Workload: 100 sequential queries via CLI, `SELECT col1 FROM $T WHERE col1 > N`.
- Rust command: `time for i in $(seq 1 100); do ./taql "$TABLE" "SELECT col1 FROM $T WHERE col1 > $i"; done`
- C++ command: `time for i in $(seq 1 100); do taql "$TABLE" "SELECT col1 FROM $T WHERE col1 > $i"; done`
- Alert threshold: 2× C++ runtime.

## Closeout criteria

- [ ] All Phase 2 closeout gates pass.
- [ ] Public docs updated at C++ doxygen-comparable detail.
- [ ] Demo added/updated if user-visible workflow changed.

## Results

- Date:
- Commit:
- Commands:
  - `` -> PASS/FAIL
- Interop matrix:
  - RR:
  - RC:
  - CR:
  - CC:
- Performance:
  - Rust:
  - C++:
  - Ratio:
- Skips/blockers/follow-ups:

## Lessons learned

-
