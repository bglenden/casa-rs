# Wave 6 - TaQL group array aggregates and lazy collectors

## Origin

- Backlog items: 6.1, 6.2.

## Goal

- Implement TaQL group array aggregate family and lazy group collectors
  (`GAGGR`/`GROWID`).

## Non-goals

- Window aggregates.
- JOIN execution.

## Scope

### Read path

- Add per-element group aggregate accumulation for arrays.
- Add lazy buffering accumulators for collected values/row ids.

### Write path

- N/A.

### API/docs/demo

- TaQL GROUP BY function behavior only.

## Dependencies

- Existing group-by execution pipeline.

## Files likely touched

- `crates/casacore-tables/src/taql/aggregate.rs`
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/functions.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add array accumulator types and validation.
- [ ] Add `GHIST` support.
- [ ] Add lazy `GAGGR`/`GROWID` support.
- [ ] Add shape mismatch and group-output tests.

## Test plan

- [ ] Group-by array aggregate correctness.
- [ ] Histogram and lazy collector coverage.
- [ ] Shape mismatch error coverage.
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
