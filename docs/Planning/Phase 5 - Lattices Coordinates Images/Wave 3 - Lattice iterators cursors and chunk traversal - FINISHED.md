> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 3 - Lattice iterators cursors and chunk traversal

## Origin

- Backlog items: 3.1, 3.2, 3.3.

## Goal

- Implement first-class lattice iteration APIs: full traversal, strided
  traversal, cursor-based access, and chunk/tile iteration.

## Non-goals

- Region/mask-aware iteration.
- Image-level wrappers.

## Scope

### Read path

- Read lattice values through iterator/cursor/chunk APIs.

### Write path

- Mutable iterator/cursor/chunk updates where safe.

### API/docs/demo

- Expose ergonomic iterator APIs with explicit axis-order behavior.

## Dependencies

- Wave 2 completed.

## Ordering constraints

- Must run after Wave 2.
- Required before Waves 4, 8, and 10.

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

- [ ] Implement iterator/cursor/chunk traversal primitives.
- [ ] Document traversal order and mutation semantics.
- [ ] Add correctness tests for full and strided traversal.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: linear, strided, and chunked scans over large lattices.
- Rust command: release iterator throughput benchmark suite.
- C++ command: matching lattice iterator benchmark.
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
