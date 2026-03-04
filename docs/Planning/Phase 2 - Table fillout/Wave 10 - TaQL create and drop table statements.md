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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add DDL AST/parser support.
- [ ] Add create path execution.
- [ ] Add drop path execution + error handling.

## Test plan

- [ ] Create table then open/verify schema.
- [ ] Drop table then verify path removed.
- [ ] Non-existent drop error handling.
- [ ] Clean skip when `pkg-config casacore` is unavailable.

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
