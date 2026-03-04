# Wave 19 - Table lifecycle operations

## Origin

- Backlog items: 10.1, 10.2, 10.3, 10.4.

## Goal

- Implement core table lifecycle controls: `flush`, `resync`, `reopen_rw`, and
  mark/unmark for delete.

## Non-goals

- Table set algebra and data manager info APIs.

## Scope

### Read path

- Add `resync` behavior and read-only to read-write reopening behavior.

### Write path

- Add `flush` and mark-for-delete behavior for on-disk tables.

### API/docs/demo

- Public table lifecycle API surface.

## Dependencies

- Locking and save/load internals in `table_impl`.

## Ordering constraints

- Can run independently of Waves 20-24.

## Files likely touched

- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/table_impl.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add API methods and state transitions.
- [ ] Integrate lock/permission checks.
- [ ] Add drop-path deletion semantics.

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
