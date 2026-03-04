# Wave 5 - Measures data tables and discovery

## Origin

- Backlog items: 4.1, 4.2, 4.3, 4.4.

## Goal

- Support runtime discovery and loading of measures data tables (IERS/JPL and
  observatory/source/line catalogs) with robust fallback and skip behavior.

## Non-goals

- Shipping or updating measures datasets.
- Full parity for every optional data source.

## Scope

### Read path

- Resolve measures data directories and open required tables.
- Provide interpolation/lookup entrypoints used by conversions.

### Write path

- N/A (read-mostly data source integration).

### API/docs/demo

- Document required runtime data setup and graceful-degradation behavior.

## Dependencies

- Wave 4 completed.

## Ordering constraints

- Must run after Wave 4.
- Required before high-fidelity conversion tests in Waves 7-8.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-test-support/src/`
- `crates/casacore-test-support/tests/`
- `docs/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement data directory discovery order and overrides.
- [ ] Add loaders for required IERS/JPL/lookup tables.
- [ ] Add tests for available-data and missing-data paths.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated table lookup and interpolation calls.
- Rust command: release benchmark for lookup throughput.
- C++ command: matching `MeasIERS`/`MeasJPL` lookup microbenchmark.
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
