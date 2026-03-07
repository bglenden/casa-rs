> **Closed Wave Record**
>
> This wave is finished and kept only as historical record.
> Do not plan or implement new work from this file.
> Use Phase 5 Waves 11a-15 for current Phase 5 parity work.

# Wave 9 - Image metadata masks history and basic expressions

## Origin

- Backlog items: 9.1, 9.2, 9.3.

## Goal

- Implement image metadata/mask/history handling and a minimal expression layer
  suitable for interoperability and basic workflows.

## Non-goals

- Advanced deconvolution/imaging algorithms.
- Full expression parser parity.

## Scope

### Read path

- Read metadata, history records, and mask state with image payload.

### Write path

- Persist metadata/history updates and mask changes.

### API/docs/demo

- APIs for metadata/history operations and minimal expression use cases.

## Dependencies

- Wave 4 completed.
- Wave 7 completed.

## Ordering constraints

- Must run after Waves 4 and 7.
- Required before Wave 10.

## Files likely touched

- `crates/casacore-images/src/`
- `crates/casacore-images/tests/`
- `crates/casacore-types/src/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement metadata/history persistence and retrieval.
- [ ] Implement mask state APIs and updates.
- [ ] Add minimal expression support and regression tests.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: metadata/mask-heavy open-update-save loop.
- Rust command: release benchmark for metadata and mask updates.
- C++ command: matching image metadata update benchmark.
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
