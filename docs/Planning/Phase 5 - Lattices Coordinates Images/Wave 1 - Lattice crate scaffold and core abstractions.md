# Wave 1 - Lattice crate scaffold and core abstractions

## Origin

- Backlog items: 1.1, 1.2, 1.3.

## Goal

- Create `casacore-lattices` crate and core lattice abstractions
  (shape, element access contract, coordinate attachment points).

## Non-goals

- Storage backend implementation.
- Iteration APIs.

## Scope

### Read path

- N/A (abstractions only).

### Write path

- N/A (abstractions only).

### API/docs/demo

- Public lattice trait/types and basic in-memory constructor stubs.

## Dependencies

- Phase 4 completed.

## Ordering constraints

- First wave in Phase 5.
- Required before Waves 2-10.

## Files likely touched

- `Cargo.toml`
- `crates/casacore-lattices/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add crate and workspace wiring.
- [ ] Define core lattice traits and shape/index types.
- [ ] Add baseline docs and compile tests.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: N/A.
- Rust command: N/A.
- C++ command: N/A.
- Alert threshold: N/A.

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
