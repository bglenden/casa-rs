# Wave 4 - Measure-aware ms column helpers

## Origin

- Backlog items: 4.1, 4.2, 4.3.

## Goal

- Add measure-aware helpers for key MS columns (`TIME`, `UVW`, direction and
  reference-frequency fields) using Phase 3 measure/tablemeasure behavior.

## Non-goals

- Full parity for every measure-bearing MS column.
- Full coordinate projection features.

## Scope

### Read path

- Decode measure-tagged column metadata and typed values for targeted columns.

### Write path

- Write measure-tagged values with correct `MEASINFO`/units/ref metadata.

### API/docs/demo

- Ergonomic typed getters/setters for targeted measure-bearing columns.

## Dependencies

- Wave 3 completed.
- Phase 3 Wave 6 completed.

## Ordering constraints

- Must run after Wave 3.
- Must run before Waves 5, 7, and 8.

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

- [ ] Map targeted MS columns to typed measure wrappers.
- [ ] Implement read/write conversions with ref handling.
- [ ] Add interop tests against C++ measure column behavior.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: bulk read/write of measure-bearing MS columns.
- Rust command: release benchmark for targeted column access.
- C++ command: matching `MSColumns` measure column benchmark.
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
