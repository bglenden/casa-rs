# Wave 18 - Table metadata and keyword table references

## Origin

- Backlog items: 9.1, 9.2.

## Goal

- Implement `TableInfo` persistence and subtable-reference keyword support.

## Non-goals

- Introspection formatter methods (`show_*`) and CLI surface (Wave 24).

## Scope

### Read path

- Read `table.info` and keyword table-reference records.

### Write path

- Persist `TableInfo` and table-reference keyword values in C++-compatible form.

### API/docs/demo

- Add public `Table::info`, `Table::set_info`, and table keyword table accessor.

## Dependencies

- Storage table-control record parsing/writing paths.

## Ordering constraints

- Must finish before Wave 22 (`copy_info` dependency).
- Recommended before Wave 24 for richer introspection output.

## Files likely touched

- `crates/casacore-types/src/lib.rs`
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/table_impl.rs`
- `crates/casacore-tables/src/storage/table_control.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add `TableInfo` model + read/write integration.
- [ ] Add keyword table-reference value representation and I/O.
- [ ] Add API methods and interop coverage.

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
