# Wave 20 - Locking extension modes and external sync

## Origin

- Backlog items: 11.1, 11.2.

## Goal

- Add remaining lock modes and optional external lock synchronization hook.

## Non-goals

- Table lifecycle APIs from Wave 19.

## Scope

### Read path

- Add `AutoNoReadLocking`, `UserNoReadLocking`, and `DefaultLocking` semantics.

### Write path

- Integrate `ExternalLockSync` callbacks into lock acquire/release path.

### API/docs/demo

- Public lock mode and hook extensions.

## Dependencies

- Existing lock-file and table lock plumbing.

## Ordering constraints

- Can run independently of Waves 21-24.

## Files likely touched

- `crates/casacore-tables/src/lock/mod.rs`
- `crates/casacore-tables/src/lock/lock_file.rs`
- `crates/casacore-tables/src/table.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add new lock-mode enums and behavior.
- [ ] Add external sync trait and table hook.
- [ ] Add contention and callback-order tests.

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
