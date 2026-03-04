# Wave 2 - Core subtable typed wrappers

## Origin

- Backlog items: 2.1, 2.2, 2.3, 2.4, 2.5.

## Goal

- Add typed wrappers for core subtables (`ANTENNA`, `FIELD`,
  `SPECTRAL_WINDOW`, `POLARIZATION`, `DATA_DESCRIPTION`).

## Non-goals

- Full `MSColumns` parity for every subtable.
- Selection and query APIs.

## Scope

### Read path

- Typed row/column accessors over existing table storage.

### Write path

- Typed row insertion/update helpers with schema validation.

### API/docs/demo

- Public typed structs and wrapper APIs for core subtables.

## Dependencies

- Wave 1 completed.

## Ordering constraints

- Must run after Wave 1.
- Required before Waves 3-7.

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

- [ ] Implement typed wrappers for selected subtables.
- [ ] Add schema and required-keyword validation.
- [ ] Add interop tests for typed read/write operations.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: typed row read/write loops across core subtables.
- Rust command: release benchmark for wrapper overhead.
- C++ command: matching `MSColumns` access pattern microbenchmark.
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
