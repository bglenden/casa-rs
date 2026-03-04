# Wave 8 - CoordinateSystem stokes and spectral core

## Origin

- Backlog items: 7.1, 7.2, 7.3.

## Goal

- Implement coordinate core subset: `CoordinateSystem` axis mapping operations,
  `StokesCoordinate`, and linear/tabular `SpectralCoordinate`.

## Non-goals

- Full `DirectionCoordinate` projection/WCSLIB parity (deferred).
- Full FITS coordinate utility coverage.

## Scope

### Read path

- Parse and evaluate coordinate metadata for spectral and stokes axes.

### Write path

- Persist coordinate metadata for supported subset with C++-compatible layout.

### API/docs/demo

- Add core coordinate APIs and examples for image/table coordinate metadata.

## Dependencies

- Wave 4 completed.

## Ordering constraints

- Must run after Wave 4.
- Can run in parallel with Wave 7.
- Must not include deferred Direction/WCSLIB parity scope.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-tables/src/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add supported coordinate types and axis mapping operations.
- [ ] Add spectral conversion subset without full direction projection stack.
- [ ] Add interop fixtures for coordinate metadata round-trips.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: bulk pixel/world conversion for spectral and stokes coordinates.
- Rust command: release benchmark for coordinate conversion loops.
- C++ command: `tCoordinateSystem`/`tSpectralCoordinate` subset benchmark.
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
