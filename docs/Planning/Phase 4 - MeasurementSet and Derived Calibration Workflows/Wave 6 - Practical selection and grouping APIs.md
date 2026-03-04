# Wave 6 - Practical selection and grouping APIs

## Origin

- Backlog items: 6.1, 6.2, 6.3.

## Goal

- Provide practical MS selection APIs (time, baseline, spw, field) backed by
  existing table indexing/TaQL features.

## Non-goals

- Full C++ `MSSel` grammar and parser compatibility.
- CASA task-level selection DSLs.

## Scope

### Read path

- Typed selection builders and optimized row filtering/grouping.

### Write path

- N/A (selection/query layer).

### API/docs/demo

- User-facing selection builder APIs and examples.

## Dependencies

- Wave 3 completed.

## Ordering constraints

- Must run after Wave 3.
- Can run in parallel with Wave 5.
- Required before Waves 7 and 8.

## Files likely touched

- `crates/casacore-ms/src/`
- `crates/casacore-ms/tests/`
- `crates/casacore-tables/src/taql/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement typed selection builders for core dimensions.
- [ ] Add row-group and iterator integrations.
- [ ] Add parity tests versus equivalent C++ selection outcomes.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated selection/query workloads over medium MS fixtures.
- Rust command: release benchmark for selection latency and throughput.
- C++ command: matching `MSSelection`/TaQL workload.
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
