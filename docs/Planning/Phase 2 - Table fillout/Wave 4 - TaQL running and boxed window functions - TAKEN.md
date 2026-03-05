# Wave 4 - TaQL running and boxed window functions

## Origin

- Backlog items: 3.1, 3.2.

## Goal

- Add running and boxed window aggregate function infrastructure and core
  function support.

## Non-goals

- Group-by aggregate family (Wave 6).
- JOIN/HAVING/UDF work.

## Scope

### Read path

- Execution pass for running and sliding windows.
- Function dispatch for RUNNING* and BOXED* families.

### Write path

- N/A.

### API/docs/demo

- TaQL function coverage only.

## Dependencies

- Existing TaQL exec ordering semantics.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/ast.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Array fixture (10-row, 2-D arrays), interop queries:
  1. `SELECT RUNNINGMEAN(arrcol) FROM $T`
  2. `SELECT RUNNINGSUM(arrcol) FROM $T`
  3. `SELECT BOXEDMEAN(arrcol, 3) FROM $T`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined — 10k-element running mean, threshold 5× C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add window kind infrastructure.
- [x] Implement running aggregates.
- [x] Implement boxed/sliding aggregates.
- [x] Add NULL/boundary behavior tests.
- [ ] Add C++ TaQL interop queries for this wave (3 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] RUNNINGSUM/RUNNINGMEAN/RUNNINGMEDIAN coverage.
- [x] BOXEDMEAN/BOXEDMIN boundary coverage.
- [x] Existing TaQL tests remain green.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 3 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: 10k-element column, `SELECT RUNNINGMEAN(col) FROM $T`.
- Rust command: `cargo bench --bench taql_perf -- running_mean_10k`
- C++ command: `./casacore_cpp_taql_shim $TABLE "SELECT RUNNINGMEAN(col) FROM $T"`
- Alert threshold: 5× C++ runtime.

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
