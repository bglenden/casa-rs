# Wave 15 - Interop fixture hardening

## Origin

- Backlog items: 1.3, 1.4, 1.6.

## Goal

- Close interop test coverage gaps for undefined scalars, column keywords, and
  row-range stride behavior.

## Non-goals

- Record-column on-disk support (`TpRecord`) in storage layers.

## Scope

### Read path

- Verify undefined scalar decode behavior in all supported managers.
- Verify keyword and row-stride reads against C++ fixtures.

### Write path

- Add missing C++ shim write/verify fixtures for 2x2 interop.

### API/docs/demo

- Test/support layer only.

## Dependencies

- Existing C++ fixture harness in `casacore-test-support`.

## Ordering constraints

- Can run independently of Waves 16-24.

## Files likely touched

- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/lib.rs`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add missing fixture variants and shim entry points.
- [ ] Add 2x2 cross-matrix tests.
- [ ] Add stride interop assertions.

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
