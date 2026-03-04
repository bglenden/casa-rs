# Wave 6 - TableMeasures descriptors and column accessors

## Origin

- Backlog items: 5.1, 5.2, 5.3, 5.4, 5.5.

## Goal

- Implement `MEASINFO` descriptor persistence and typed scalar/array measure
  column access for fixed and variable refs/offsets.

## Non-goals

- Full coordinate projection stack.
- MeasurementSet typed wrappers.

## Scope

### Read path

- Reconstruct `MEASINFO` including `VarRefCol`, `RefOffCol`, and tab ref maps.

### Write path

- Persist `MEASINFO` and linked ref/offset metadata with C++-compatible keys.

### API/docs/demo

- Add typed table measure descriptor/accessor APIs and examples.

## Dependencies

- Wave 2 completed.
- Wave 4 completed.

## Ordering constraints

- Must run after Waves 2 and 4.
- Must run before Wave 7.

## Files likely touched

- `crates/casacore-tables/src/storage/table_control.rs`
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add read/write support for `MEASINFO` records and linked ref/offset cols.
- [ ] Implement scalar and array measure accessors with conversion hooks.
- [ ] Add fixture tests for fixed-ref, var-ref, fixed-offset, and var-offset.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: bulk read/write of measure-tagged columns.
- Rust command: release benchmark for measure column put/get throughput.
- C++ command: `tTableMeasures`/`dVarRefMdirCol` style workload.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 3 closeout gates pass.
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
