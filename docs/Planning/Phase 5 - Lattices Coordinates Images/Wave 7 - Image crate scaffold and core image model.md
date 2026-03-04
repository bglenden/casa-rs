# Wave 7 - Image crate scaffold and core image model

## Origin

- Backlog items: 7.1, 7.2, 7.3.

## Goal

- Create `casacore-images` crate and core image model built on lattices plus
  attached coordinate systems and metadata.

## Non-goals

- Image iteration APIs.
- Image expression engine and advanced math.

## Scope

### Read path

- Open and inspect image metadata and core lattice payload.

### Write path

- Create and persist core image metadata/payload.

### API/docs/demo

- Public `Image` type with open/create/save lifecycle and metadata access.

## Dependencies

- Wave 4 completed.
- Wave 6 completed.

## Ordering constraints

- Must run after Waves 4 and 6.
- Required before Waves 8-10.

## Files likely touched

- `Cargo.toml`
- `crates/casacore-images/`
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

- [ ] Add crate and workspace wiring.
- [ ] Implement core image model and metadata integration.
- [ ] Add basic interop fixtures for open/create/save.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: image open/save on representative array sizes.
- Rust command: release benchmark for image lifecycle operations.
- C++ command: matching image open/save benchmark.
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
