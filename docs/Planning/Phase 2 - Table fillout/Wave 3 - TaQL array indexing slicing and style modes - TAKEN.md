# Wave 3 - TaQL array indexing slicing and style modes

## Origin

- Backlog items: 2.2, 18.1.

## Goal

- Implement TaQL array element/slice access and style switching
  (`USING STYLE GLISH|PYTHON`).

## Non-goals

- Other expression syntax additions from Wave 2.
- Window and group aggregate functions.

## Scope

### Read path

- Parse/eval `arr[i]`, `arr[i,j]`, `arr[s:e[:step]]`.
- Add style-aware index/axis handling (Glish default, Python override).

### Write path

- N/A.

### API/docs/demo

- TaQL behavior only.

## Dependencies

- Wave 2 AST/parser shape decisions for expressions.

## Files likely touched

- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`
- `crates/casacore-tables/src/taql/exec.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Array fixture (10-row, 2-D arrays), interop queries:
  1. `SELECT arrcol[1,2] FROM $T`
  2. `SELECT arrcol[2,3] FROM $T`
  3. `SELECT arrcol[1:3,] FROM $T`
  4. `SELECT arrcol[,2:4] FROM $T`
  5. `SELECT arrcol[1:2:1,1:3:1] FROM $T USING STYLE PYTHON`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (array access, not a hot path).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add array index/slice AST + parser handling.
- [x] Implement slicing/index extraction in eval.
- [x] Add style context and parser support for `USING STYLE`.
- [x] Add regression tests for GLISH and PYTHON behavior.
- [ ] Add C++ TaQL interop queries for this wave (5 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Array indexing/slicing positive and error cases.
- [x] Style mode tests for 1-based and 0-based behavior.
- [x] Existing TaQL tests remain green.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 5 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: N/A — array element access, not a hot path.
- Rust command: N/A.
- C++ command: N/A.
- Alert threshold: N/A.

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
