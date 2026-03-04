# Wave 11 - TaQL join execution

## Origin

- Backlog item: 8.1.

## Goal

- Implement TaQL JOIN execution (inner/left/cross) with correct row-context
  behavior.

## Non-goals

- UDF framework.
- Statement-level DDL and set operations.

## Scope

### Read path

- Execute join plans with ON-condition evaluation across table contexts.
- Baseline nested-loop implementation plus optional index acceleration.

### Write path

- N/A.

### API/docs/demo

- TaQL query execution behavior only.

## Dependencies

- Existing `JoinClause` parsing support and eval expression model.

## Files likely touched

- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/eval.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement nested-loop join executor.
- [ ] Add multi-table row expression context.
- [ ] Add optional index-assisted path.
- [ ] Add null-fill behavior for LEFT JOIN.

## Test plan

- [ ] INNER/LEFT/CROSS correctness tests.
- [ ] Nested join and ON-clause tests.
- [ ] Index-assisted join path tests.
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
