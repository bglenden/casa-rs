# Wave 17 - Mixed schema interoperability fixture

## Origin

- Backlog item: 1.5.

## Goal

- Add a single mixed-schema fixture exercising scalar, fixed-array,
  variable-array, record, and keyword features together.

## Non-goals

- New storage encoding features.

## Scope

### Read path

- Verify mixed-schema read behavior under C++ and Rust for one manager.

### Write path

- Verify mixed-schema write behavior under full 2x2 interop.

### API/docs/demo

- Test/support layer only.

## Dependencies

- Wave 1 completed (`1.1` variable arrays).
- Wave 16 completed (`1.2` record columns).

## Ordering constraints

- Must run after Wave 16.

## Files likely touched

- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/table_interop.rs`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Define mixed schema fixture payload.
- [ ] Add C++ shim write/verify coverage.
- [ ] Add full 2x2 test for selected manager.

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
