# Wave 2 - Lattice storage backends and paged tiling

## Origin

- Backlog items: 2.1, 2.2, 2.3, 2.4.

## Goal

- Implement lattice storage backends: memory-backed and paged/tiled disk-backed
  lattices with compatible metadata semantics.

## Non-goals

- Iterator APIs beyond minimal internal traversal.
- Region/mask algebra.

## Scope

### Read path

- Open and read lattice data from memory and paged/tiled storage.

### Write path

- Persist lattice data and storage metadata to disk.

### API/docs/demo

- Backend selection APIs and simple read/write demo.

## Dependencies

- Wave 1 completed.

## Ordering constraints

- Must run after Wave 1.
- Required before Waves 3, 4, 7, and 8.

## Files likely touched

- `crates/casacore-lattices/src/`
- `crates/casacore-lattices/tests/`
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

- [ ] Implement memory and paged/tiled lattice backends.
- [ ] Add metadata serialization/deserialization support.
- [ ] Add interop tests for persistence and reopen behavior.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: large array read/write with varied tile shapes.
- Rust command: release benchmark for backend throughput.
- C++ command: matching paged lattice storage benchmark.
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
