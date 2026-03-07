> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 6 - FITS and WCS coordinate interoperability

## Origin

- Backlog items: 6.1, 6.2, 6.3.

## Goal

- Implement FITS/WCS coordinate import/export parity and conversion utilities
  required for image interoperability.

## Non-goals

- Full imaging algorithm suite.
- Mosaic reprojection workflows.

## Scope

### Read path

- Parse FITS/WCS coordinate metadata into coordinate-system types.

### Write path

- Emit FITS/WCS metadata from coordinate-system definitions.

### API/docs/demo

- Utility APIs and demo for FITS/WCS coordinate round-trips.

## Dependencies

- Wave 5 completed.

## Ordering constraints

- Must run after Wave 5.
- Required before Waves 7-10.

## Files likely touched

- `crates/casacore-coordinates/src/`
- `crates/casacore-coordinates/tests/`
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

- [ ] Implement FITS/WCS parse/emit helpers.
- [ ] Add round-trip interoperability tests.
- [ ] Add error-path tests for unsupported/invalid metadata.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: batch FITS/WCS metadata parse/emit on representative headers.
- Rust command: release benchmark for conversion utility throughput.
- C++ command: matching FITS coordinate utility benchmark.
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
