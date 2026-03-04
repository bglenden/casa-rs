# Wave 10 - Lattice and image interop matrix demos and perf closeout

## Origin

- Backlog items: 10.1, 10.2, 10.3.

## Goal

- Close Phase 5 with end-to-end lattice/image interop fixtures, iterator
  correctness matrix, demos, and performance closeout evidence.

## Non-goals

- New core features.
- Advanced science algorithms.

## Scope

### Read path

- Validate complete read workflows for lattice/image fixtures authored by
  Rust and C++.

### Write path

- Validate complete write workflows with round-trip parity.

### API/docs/demo

- Add/update lattice/image demos and phase closeout documentation.

## Dependencies

- Waves 8 and 9 completed.

## Ordering constraints

- Final wave.
- Must run after Waves 8 and 9.

## Files likely touched

- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`
- `crates/casacore-lattices/examples/`
- `crates/casacore-images/examples/`
- `docs/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Build canonical lattice/image fixture matrix.
- [ ] Add iterator correctness matrix and perf evidence.
- [ ] Publish/update demos and closeout docs.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: end-to-end image/lattice workflows with iteration-heavy phases.
- Rust command: release benchmark pipeline for open/iterate/update/save.
- C++ command: matching lattice/image pipeline benchmark.
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
