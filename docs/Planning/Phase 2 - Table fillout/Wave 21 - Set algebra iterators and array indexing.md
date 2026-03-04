# Wave 21 - Set algebra iterators and array indexing

## Origin

- Backlog items: 10.5, 12.1, 12.2.

## Goal

- Implement table set algebra and iteration/indexing extensions.

## Non-goals

- Custom compare objects (`12.3`, deferred).

## Scope

### Read path

- Add iterator descending order and `NoSort` behavior.
- Add `ColumnsIndexArray` lookup support.

### Write path

- N/A.

### API/docs/demo

- Add set algebra operations and indexing API surface.

## Dependencies

- Existing `RefTable`, `sorting`, and `indexing` internals.

## Ordering constraints

- Recommended before Wave 24 (for richer structure/report outputs).

## Files likely touched

- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/ref_table.rs`
- `crates/casacore-tables/src/sorting.rs`
- `crates/casacore-tables/src/indexing.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add set algebra APIs over row-index sets.
- [ ] Add descending/NoSort iterator behavior.
- [ ] Add array-index lookup structure and tests.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC).
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary).
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
