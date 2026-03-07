> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 8 - Image iteration slicing and chunk APIs

## Origin

- Backlog items: 8.1, 8.2, 8.3.

## Goal

- Implement image-level iteration/slicing/chunk APIs with coordinate-aware
  axis-order helpers.

## Non-goals

- Full image math/expression framework.
- Reprojection/mosaicking.

## Scope

### Read path

- Iterate image data via full, strided, and chunk traversals.

### Write path

- Mutable chunk/slice updates through iterator APIs.

### API/docs/demo

- Image iterator API docs and examples for common scans.

## Dependencies

- Wave 3 completed.
- Wave 7 completed.

## Ordering constraints

- Must run after Waves 3 and 7.
- Required before Wave 10.

## Files likely touched

- `crates/casacore-images/src/`
- `crates/casacore-images/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement image iterators for full/strided/chunked traversal.
- [ ] Add slicing APIs with coordinate-axis helpers.
- [ ] Add iterator correctness tests across axis-order variants.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: image sweep and chunked plane extraction workloads.
- Rust command: release benchmark for iterator throughput.
- C++ command: matching image iterator benchmark.
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
