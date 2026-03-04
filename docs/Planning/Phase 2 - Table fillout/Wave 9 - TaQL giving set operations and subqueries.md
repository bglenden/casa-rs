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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add parser support for `GIVING` and set-op syntax.
- [ ] Add executor paths for set-op row-set operations.
- [ ] Add subquery execution and value extraction.
- [ ] Add output materialization logic for `GIVING`.

## Test plan

- [ ] `GIVING` path/mode tests.
- [ ] Set-operations correctness tests.
- [ ] Subquery `IN` and scalar tests.
- [ ] Schema mismatch error tests.

## Performance plan

- Workload: N/A.
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
