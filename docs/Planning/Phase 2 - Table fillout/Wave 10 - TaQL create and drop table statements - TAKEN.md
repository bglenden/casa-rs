# Wave 10 - TaQL create and drop table statements

## Origin

- Backlog items: 7.6, 7.7.

## Goal

- Add TaQL `CREATE TABLE` and `DROP TABLE` statements.

## Non-goals

- `GIVING`, set ops, and subqueries.
- JOIN or UDF work.

## Scope

### Read path

- Parse DDL statements and validate syntax.

### Write path

- Execute create/drop operations on table paths.

### API/docs/demo

- TaQL statement behavior only.

## Dependencies

- Existing table creation/open/delete primitives.

## Files likely touched

- `crates/casacore-tables/src/taql/token.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Filesystem fixture, interop is storage format compatibility:
  1. Rust CREATE TABLE → C++ open and verify schema
  2. C++ CREATE TABLE → Rust open and verify schema
  - Note: Interop is on-disk format compatibility for DDL-created tables, not query result comparison.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (DDL statements, I/O-bound not compute-bound).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add DDL AST/parser support.
- [x] Add create path execution.
- [x] Add drop path execution + error handling.
- [ ] Add C++ TaQL interop queries for this wave (2 format-compat tests above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Create table then open/verify schema.
- [x] Drop table then verify path removed.
- [x] Non-existent drop error handling.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) — format compatibility for DDL-created tables.
- [ ] Rust-created tables readable by C++; C++-created tables readable by Rust.

## Performance plan

- Workload: N/A — DDL statements are I/O-bound, not compute-bound.
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
