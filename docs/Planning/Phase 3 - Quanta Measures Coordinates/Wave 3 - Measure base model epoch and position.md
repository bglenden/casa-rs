# Wave 3 - Measure base model epoch and position

## Origin

- Backlog items: 3.1, 3.2, 3.3.

## Goal

- Establish measure base/reference model and first concrete types: `MEpoch`
  and `MPosition`.

## Non-goals

- Direction/frequency/doppler measure families.
- TableMeasures `MEASINFO` persistence.

## Scope

### Read path

- Parse measure records for epoch/position values and reference types.

### Write path

- Serialize epoch/position measures with casacore-compatible record layout.

### API/docs/demo

- Provide typed measure constructors, ref enums, and conversion entrypoints.

## Dependencies

- Wave 1 completed.

## Ordering constraints

- Must run after Wave 1.
- Must run before Waves 4-8.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-types/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement measure base/ref abstractions for typed measures.
- [ ] Add `MEpoch` and `MPosition` types with core ref enums.
- [ ] Add interop tests for record-encoded measure values.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: batch construction/conversion of epoch/position values.
- Rust command: release benchmark for core measure operations.
- C++ command: `tMeasure` subset benchmark.
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
