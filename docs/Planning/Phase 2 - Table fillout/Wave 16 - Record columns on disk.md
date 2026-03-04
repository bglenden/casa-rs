# Wave 16 - Record columns on disk

## Origin

- Backlog item: 1.2.

## Goal

- Implement `TpRecord` on-disk interop for StManAipsIO, StandardStMan, and
  IncrementalStMan.

## Non-goals

- Mixed-schema mega-fixture (Wave 17).

## Scope

### Read path

- Detect/deserialize record cells from storage manager payloads.

### Write path

- Serialize record cells as indirect `Vector<uChar>` using AipsIO framing.

### API/docs/demo

- Storage + interop tests.

## Dependencies

- `casacore-aipsio` framing primitives.

## Ordering constraints

- Must finish before Wave 17.

## Files likely touched

- `crates/casacore-tables/src/storage/stman_aipsio.rs`
- `crates/casacore-tables/src/storage/standard_stman.rs`
- `crates/casacore-tables/src/storage/incremental_stman.rs`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add record encode/decode for all targeted managers.
- [ ] Add `ScalarRecord` fixture variants and shim functions.
- [ ] Add full interop tests including nested-record edge cases.

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
