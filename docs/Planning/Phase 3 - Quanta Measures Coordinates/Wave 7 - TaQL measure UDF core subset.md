# Wave 7 - TaQL measure UDF core subset

## Origin

- Backlog items: 6.1, 6.2, 6.3.

## Goal

- Add core measure-aware TaQL UDF support for epoch/position/direction/
  frequency conversions used by common workflows.

## Non-goals

- Full casacore `meas` UDF catalog.
- Non-core astronomy-specialized helper functions.

## Scope

### Read path

- Parse and evaluate measure UDF calls in TaQL expressions.

### Write path

- N/A (query execution layer).

### API/docs/demo

- Document supported UDF set and unsupported-function behavior.

## Dependencies

- Wave 4 completed.
- Wave 6 completed.

## Ordering constraints

- Must run after Waves 4 and 6.
- Can run in parallel with Wave 8.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/eval.rs`
- `crates/casacore-tables/tests/taql.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement parser/evaluator wiring for core measure UDFs.
- [ ] Add deterministic behavior for missing frames/data tables.
- [ ] Add interop tests comparing C++ and Rust query outcomes.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: TaQL query batches with repeated measure UDF calls.
- Rust command: release benchmark over representative TaQL statements.
- C++ command: matching TaQL run with `meas` UDF functions.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 3 closeout gates pass.
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
