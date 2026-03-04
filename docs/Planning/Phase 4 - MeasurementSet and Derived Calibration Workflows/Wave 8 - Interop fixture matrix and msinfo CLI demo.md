# Wave 8 - Interop fixture matrix and msinfo CLI demo

## Origin

- Backlog items: 8.1, 8.2, 8.3.

## Goal

- Close Phase 4 with canonical MS 2x2 interop fixtures, regression matrix, and
  a small `msinfo` CLI/demo for operational validation.

## Non-goals

- New core data-model features.
- Full diagnostics parity with all casacore command-line tools.

## Scope

### Read path

- Verify end-to-end MS open/read/selection/derived flows from C++ and Rust
  authored fixtures.

### Write path

- Verify Rust-authored MS fixtures are consumed correctly by C++ tools/tests.

### API/docs/demo

- Add `msinfo`-style CLI/demo and closeout docs.

## Dependencies

- Waves 5, 6, and 7 completed.

## Ordering constraints

- Final wave.
- Must run after Waves 5-7.

## Files likely touched

- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`
- `crates/casacore-ms/examples/`
- `docs/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Build canonical MS fixture matrix for end-to-end coverage.
- [ ] Add full phase regression tests and skip gates.
- [ ] Add `msinfo` CLI/demo and usage docs.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: end-to-end workflow benchmark on representative MS fixtures.
- Rust command: release benchmark for open/select/derive/report pipeline.
- C++ command: matching pipeline with casacore tools and simple harness.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
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
- Performance:
  - Rust:
  - C++:
  - Ratio:
- Skips/blockers/follow-ups:

## Lessons learned

-
