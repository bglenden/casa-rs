# Wave 2 - TableQuantum keyword semantics and APIs

## Origin

- Backlog items: 2.1, 2.2, 2.3, 2.4.

## Goal

- Implement casacore-compatible `QuantumUnits`/`VariableUnits` behavior for
  scalar and array table columns.

## Non-goals

- `MEASINFO`/TableMeasures descriptor handling.
- Measure frame conversions.

## Scope

### Read path

- Reconstruct fixed and variable unit metadata from column keywords.

### Write path

- Emit correct fixed/variable unit keywords and unit-column bindings.

### API/docs/demo

- Add explicit table API helpers for unit metadata inspection/reset.

## Dependencies

- Wave 1 completed.

## Ordering constraints

- Must run after Wave 1.
- Must run before Wave 6.

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

- [ ] Add read/write support for `QuantumUnits` and `VariableUnits`.
- [ ] Cover scalar + array columns and unit-column validation errors.
- [ ] Add 2x2 interop tests for fixed and variable units.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: read/write many quantum-tagged rows.
- Rust command: release benchmark over scalar and array quantum columns.
- C++ command: `tTableQuantum` equivalent benchmark path.
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
