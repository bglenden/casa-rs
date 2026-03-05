# Wave 6 - TaQL group array aggregates and lazy collectors

## Origin

- Backlog items: 6.1, 6.2.

## Goal

- Implement TaQL group array aggregate family and lazy group collectors
  (`GAGGR`/`GROWID`).

## Non-goals

- Window aggregates.
- JOIN execution.

## Scope

### Read path

- Add per-element group aggregate accumulation for arrays.
- Add lazy buffering accumulators for collected values/row ids.

### Write path

- N/A.

### API/docs/demo

- TaQL GROUP BY function behavior only.

## Dependencies

- Existing group-by execution pipeline.

## Files likely touched

- `crates/casacore-tables/src/taql/aggregate.rs`
- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/functions.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop queries:
  1. `SELECT grp, GAGGR(val) FROM $T GROUP BY grp`
  2. `SELECT grp, GROWID() FROM $T GROUP BY grp`
  3. `SELECT grp, GHIST(val, 0, 10, 5) FROM $T GROUP BY grp`
  4. `SELECT grp, GCOUNT(), GMIN(val), GMAX(val) FROM $T GROUP BY grp`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined — 10k-row grouping, threshold 3× C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add array accumulator types and validation.
- [x] Add `GHIST` support.
- [x] Add lazy `GAGGR`/`GROWID` support.
- [x] Add shape mismatch and group-output tests.
- [ ] Add C++ TaQL interop queries for this wave (4 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Group-by array aggregate correctness.
- [x] Histogram and lazy collector coverage.
- [x] Shape mismatch error coverage.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 4 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: 10k-row table with 100 groups, `SELECT grp, GCOUNT() FROM $T GROUP BY grp`.
- Rust command: `cargo bench --bench taql_perf -- groupby_10k`
- C++ command: `./casacore_cpp_taql_shim $TABLE "SELECT grp, GCOUNT() FROM $T GROUP BY grp"`
- Alert threshold: 3× C++ runtime.

## Closeout criteria

- [ ] All Phase 2 closeout gates pass.
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
