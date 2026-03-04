# Wave 3 - MeasurementSet open create and validate APIs

## Origin

- Backlog items: 3.1, 3.2, 3.3, 3.4.

## Goal

- Implement high-level MS lifecycle APIs: create/open/validate with required
  subtable links and metadata checks.

## Non-goals

- Data-column optimization work.
- Derived-calculation helpers.

## Scope

### Read path

- Validate required MS table structure and linked subtable references.

### Write path

- Create new MS skeletons with required table and subtable layout.

### API/docs/demo

- `MeasurementSet` top-level API with open/create/validate operations.

## Dependencies

- Wave 1 completed.
- Wave 2 completed.

## Ordering constraints

- Must run after Waves 1 and 2.
- Required before Waves 4-8.

## Files likely touched

- `crates/casacore-ms/src/`
- `crates/casacore-ms/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement `MeasurementSet::create/open/validate` core flows.
- [ ] Enforce required schema/keyword/subtable constraints.
- [ ] Add interop tests for valid and invalid MS layouts.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated open/validate of representative MS fixtures.
- Rust command: release benchmark over open/validate loops.
- C++ command: matching `MeasurementSet` open + basic validation sequence.
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
