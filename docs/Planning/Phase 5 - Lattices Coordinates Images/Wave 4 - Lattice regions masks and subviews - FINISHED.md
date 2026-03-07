> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 4 - Lattice regions masks and subviews

## Origin

- Backlog items: 4.1, 4.2, 4.3, 4.4.

## Goal

- Add lattice regions, masks, and subview composition with region/mask-aware
  iteration semantics.

## Non-goals

- Full image expression engine.
- Coordinate reprojection workflows.

## Scope

### Read path

- Region and mask constrained reads and view materialization.

### Write path

- Mask-aware updates and region-scoped writes.

### API/docs/demo

- Region/mask/subview APIs and iteration examples.

## Dependencies

- Wave 3 completed.

## Ordering constraints

- Must run after Wave 3.
- Required before Waves 7, 8, 9, and 10.

## Files likely touched

- `crates/casacore-lattices/src/`
- `crates/casacore-lattices/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement region and mask model types.
- [ ] Integrate region/mask behavior with lattice iterators.
- [ ] Add correctness tests for nested and degenerate regions.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: masked and regioned scans with varied sparsity.
- Rust command: release benchmark for region/mask iteration overhead.
- C++ command: matching lattice region benchmark.
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
