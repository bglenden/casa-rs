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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add window kind infrastructure.
- [ ] Implement running aggregates.
- [ ] Implement boxed/sliding aggregates.
- [ ] Add NULL/boundary behavior tests.

## Test plan

- [ ] RUNNINGSUM/RUNNINGMEAN/RUNNINGMEDIAN coverage.
- [ ] BOXEDMEAN/BOXEDMIN boundary coverage.
- [ ] Existing TaQL tests remain green.
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
