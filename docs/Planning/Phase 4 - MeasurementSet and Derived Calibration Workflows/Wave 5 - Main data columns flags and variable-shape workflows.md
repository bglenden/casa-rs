# Wave 5 - Main data columns flags and variable-shape workflows

## Origin

- Backlog items: 5.1, 5.2, 5.3, 5.4.

## Goal

- Implement practical main-table data-column workflows for `DATA`/`FLAG`/
  `WEIGHT`/`SIGMA` including variable-shape array paths and bitflag engine use.

## Non-goals

- Alternate lossy storage manager implementations.
- Imaging or calibration math layers.

## Scope

### Read path

- Efficient typed read paths for targeted data and flag columns.

### Write path

- Correct write/update behavior for variable-shape visibility data and flags.

### API/docs/demo

- High-level helpers for common main-table data operations.

## Dependencies

- Wave 3 completed.
- Wave 4 completed.

## Ordering constraints

- Must run after Waves 3 and 4.
- Can run in parallel with Wave 6.
- Required before Wave 8.

## Files likely touched

- `crates/casacore-ms/src/`
- `crates/casacore-ms/tests/`
- `crates/casacore-tables/src/storage/virtual_bitflags.rs`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add typed workflows for core data and flag columns.
- [ ] Verify variable-shape + bitflag semantics under 2x2 interop.
- [ ] Add perf smoke tests for large-row workloads.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: representative MAIN table reads/writes with variable arrays.
- Rust command: release benchmark for 10k+ row visibility workloads.
- C++ command: matching `MSMainColumns` workload.
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
