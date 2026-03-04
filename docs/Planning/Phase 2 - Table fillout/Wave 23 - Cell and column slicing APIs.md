# Wave 23 - Cell and column slicing APIs

## Origin

- Backlog items: 15.1, 15.2.

## Goal

- Implement `Slicer` and both cell-level and column-range slicing APIs.

## Non-goals

- Unrelated iterator/indexing changes.

## Scope

### Read path

- Add slice reads for cells and row-ranged column reads.

### Write path

- Add slice writes for array cell sub-regions.

### API/docs/demo

- Public slicing API surface in table/storage layers.

## Dependencies

- Existing array cell load/store behavior across managers.

## Ordering constraints

- This wave intentionally keeps 15.1 and 15.2 together; do not split.

## Files likely touched

- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/src/table_impl.rs`
- `crates/casacore-tables/src/storage/mod.rs`
- `crates/casacore-tables/src/storage/tiled_stman.rs`
- `crates/casacore-tables/src/storage/standard_stman.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add `Slicer` type and validation.
- [ ] Implement `get_cell_slice`/`put_cell_slice`.
- [ ] Implement column-range + array-slice combined path.

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
