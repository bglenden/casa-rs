# Wave 9 - TaQL giving set operations and subqueries

## Origin

- Backlog items: 7.3, 7.4, 7.5.

## Goal

- Implement advanced query-shape clauses: `GIVING`, set operations, and
  subqueries.

## Non-goals

- DDL statements (`CREATE/DROP TABLE`).
- JOIN execution.

## Scope

### Read path

- Parse and execute `UNION`, `INTERSECT`, `EXCEPT`.
- Parse and execute subqueries in `IN` and scalar contexts.

### Write path

- Support `GIVING` output table materialization modes.

### API/docs/demo

- TaQL statement behavior only.

## Dependencies

- Stable select execution and schema compatibility checks.

## Files likely touched

- `crates/casacore-tables/src/taql/token.rs`
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop queries:
  1. `SELECT col1 FROM $T WHERE col1 > 10 UNION SELECT col1 FROM $T WHERE col1 < 5`
  2. `SELECT col1 FROM $T WHERE col1 > 10 INTERSECT SELECT col1 FROM $T WHERE col1 < 30`
  3. `SELECT col1 FROM $T WHERE col1 IN [SELECT col2 FROM $T WHERE col2 > 20]`
  4. `SELECT col1 FROM $T WHERE col1 > (SELECT MIN(col2) FROM $T)`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (query-shape semantics, not compute-bound).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add parser support for `GIVING` and set-op syntax.
- [x] Add executor paths for set-op row-set operations.
- [x] Add subquery execution and value extraction.
- [x] Add output materialization logic for `GIVING`.
- [ ] Add C++ TaQL interop queries for this wave (4 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] `GIVING` path/mode tests.
- [x] Set-operations correctness tests.
- [x] Subquery `IN` and scalar tests.
- [x] Schema mismatch error tests.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 4 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: N/A — query-shape semantics, not compute-bound.
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
