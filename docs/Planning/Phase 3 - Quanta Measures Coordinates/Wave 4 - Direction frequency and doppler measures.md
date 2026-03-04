# Wave 4 - Direction frequency and doppler measures

## Origin

- Backlog items: 3.4, 3.5, 3.6.

## Goal

- Add `MDirection`, `MFrequency`, and `MDoppler` with core frame conversion
  behavior used by table metadata and TaQL.

## Non-goals

- Full planet/comet catalog and advanced ephemeris routes.
- Coordinate projection/WCSLIB integration.

## Scope

### Read path

- Decode direction/frequency/doppler records and reference variants.

### Write path

- Encode records and conversion metadata for persisted measure values.

### API/docs/demo

- Expose typed conversions and frame descriptors for these measure families.

## Dependencies

- Wave 3 completed.

## Ordering constraints

- Must run after Wave 3.
- Must run before Waves 5, 7, and 8.

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

- [ ] Add direction/frequency/doppler type and ref enums.
- [ ] Implement core conversion routes used by C++ parity tests.
- [ ] Add regression tests for unsupported frame combinations.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated direction/frequency conversions.
- Rust command: release benchmark for conversion engine loops.
- C++ command: `tMDirection` and `tMFrequency` subset.
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
