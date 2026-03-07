> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 5 - Complete coordinates and coordinate system parity

## Origin

- Backlog items: 5.1, 5.2, 5.3, 5.4.

## Goal

- Complete deferred coordinate parity: `DirectionCoordinate`,
  `SpectralCoordinate`, `StokesCoordinate`, and `CoordinateSystem` behavior.

## Non-goals

- Image storage and iteration APIs.
- Full nonlinear image reprojection workflows.

## Scope

### Read path

- Decode and apply full coordinate metadata and axis mapping behavior.

### Write path

- Persist full coordinate metadata with C++-compatible semantics.

### API/docs/demo

- Public coordinate APIs including full `CoordinateSystem` manipulation support.

## Dependencies

- Waves 1-4 completed.

## Ordering constraints

- Must run after Wave 4.
- Required before Waves 6-10.

## Files likely touched

- `crates/casacore-coordinates/`
- workspace wiring if new crate is introduced
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement full coordinate and coordinate-system type coverage.
- [ ] Add axis map/transpose/remove behavior parity tests.
- [ ] Add interop fixtures for coordinate metadata round-trips.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated pixel/world conversion and axis-map operations.
- Rust command: release benchmark for coordinate conversion throughput.
- C++ command: matching coordinate conversion benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 5 closeout gates pass.
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
- Iterator matrix:
  - full:
  - strided:
  - tiled/chunked:
  - region/mask-aware:
- Performance:
  - Rust:
  - C++:
  - Ratio:
- Skips/blockers/follow-ups:

## Lessons learned

-
