# Wave 5 - TaQL partial-axis array reductions

## Origin

- Backlog item: 4.1.

## Goal

- Add partial-axis reduction functions (`SUMS`, `MEANS`, `MINS`, etc.) for
  array arguments.

## Non-goals

- Running/boxed window functions.
- Group aggregate families.

## Scope

### Read path

- Register and evaluate partial-axis reduction forms in function dispatch.

### Write path

- N/A.

### API/docs/demo

- TaQL function behavior only.

## Dependencies

- `ndarray` lane/axis reduction support.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Array fixture (10-row, 2-D arrays), interop queries:
  1. `SELECT SUMS(arrcol, 1) FROM $T`
  2. `SELECT MEANS(arrcol, 2) FROM $T`
  3. `SELECT MINS(arrcol, 1) FROM $T`
  4. `SELECT MAXS(arrcol, 2) FROM $T`
  5. `SELECT SUMS(arrcol, 1, 2) FROM $T`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined — 100k-element axis reduction, threshold 3× C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add function registration and arg validation.
- [x] Implement single- and multi-axis reductions.
- [x] Add shape/result consistency tests.
- [ ] Add C++ TaQL interop queries for this wave (5 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] 2-D and multi-axis reduction coverage.
- [x] 1-D reduction-to-scalar edge case.
- [x] Existing TaQL tests remain green.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 5 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: 100k-element array column, `SELECT SUMS(arrcol, 1) FROM $T`.
- Rust command: `cargo bench --bench taql_perf -- axis_reduction_100k`
- C++ command: `./casacore_cpp_taql_shim $TABLE "SELECT SUMS(arrcol, 1) FROM $T"`
- Alert threshold: 3× C++ runtime.

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
