# Wave 22 - Row and copy utility APIs

## Origin

- Backlog items: 13.1, 13.2, 14.1.

## Goal

- Add row-level copy helpers and selected table copy utility methods.

## Non-goals

- Subtable copy support (`copySubTables`, deferred in backlog item 14.1).

## Scope

### Read path

- Add row accessor/copy ergonomics aligned with current materialized model.

### Write path

- Add `copy_rows`, `copy_info`, and fill-column helpers.

### API/docs/demo

- Public convenience API additions.

## Dependencies

- Table core mutation APIs and schema validation.

## Ordering constraints

- Must run after Wave 18 (`copy_info` depends on `TableInfo`).

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

- [ ] Add row copier APIs and mappings.
- [ ] Add selected copy utility methods.
- [ ] Add type- and schema-safety tests.

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
