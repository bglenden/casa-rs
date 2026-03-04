# Wave 7 - Derivedmscal core quantities and UDF integration

## Origin

- Backlog items: 7.1, 7.2, 7.3.

## Goal

- Implement core derived calibration quantities and integrate them with TaQL
  UDF hooks for common workflows.

## Non-goals

- Full `derivedmscal` catalog parity.
- Imaging/cube analysis workflows.

## Scope

### Read path

- Compute selected derived quantities from existing MS columns and measures.

### Write path

- N/A (computed values and query integration).

### API/docs/demo

- Expose derived-quantity APIs and matching TaQL UDF entrypoints.

## Dependencies

- Wave 4 completed.
- Wave 6 completed.

## Ordering constraints

- Must run after Waves 4 and 6.
- Required before Wave 8.

## Files likely touched

- `crates/casacore-ms/src/`
- `crates/casacore-ms/tests/`
- `crates/casacore-tables/src/taql/functions.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement core derived quantity computations.
- [ ] Expose UDF hooks and error handling for missing context.
- [ ] Add parity tests against C++ derived outputs for selected cases.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated derived-quantity evaluation in query loops.
- Rust command: release benchmark for derived/UDF path.
- C++ command: matching `derivedmscal` + TaQL UDF workload.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
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
