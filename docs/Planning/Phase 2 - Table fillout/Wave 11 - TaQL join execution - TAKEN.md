# Wave 11 - TaQL join execution

## Origin

- Backlog item: 8.1.

## Goal

- Implement TaQL JOIN execution (inner/left/cross) with correct row-context
  behavior.

## Non-goals

- UDF framework.
- Statement-level DDL and set operations.

## Scope

### Read path

- Execute join plans with ON-condition evaluation across table contexts.
- Baseline nested-loop implementation plus optional index acceleration.

### Write path

- N/A.

### API/docs/demo

- TaQL query execution behavior only.

## Dependencies

- Existing `JoinClause` parsing support and eval expression model.

## Files likely touched

- `crates/casacore-tables/src/taql/exec.rs`
- `crates/casacore-tables/src/taql/eval.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture × 2 (two 50-row scalar tables), interop queries:
  1. `SELECT t1.col1, t2.col2 FROM $T1 INNER JOIN $T2 ON t1.key = t2.key`
  2. `SELECT t1.col1, t2.col2 FROM $T1 INNER JOIN $T2 ON t1.key = t2.key WHERE t1.col1 > 10`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined — 1k×1k join, threshold 5× C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement nested-loop join executor.
- [x] Add multi-table row expression context.
- [x] Add optional index-assisted path.
- [x] Add null-fill behavior for LEFT JOIN.
- [ ] Add C++ TaQL interop queries for this wave (2 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] INNER/LEFT/CROSS correctness tests.
- [x] Nested join and ON-clause tests.
- [x] Index-assisted join path tests.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 2 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: 1k×1k row join, `SELECT t1.col1, t2.col2 FROM $T1 INNER JOIN $T2 ON t1.key = t2.key`.
- Rust command: `cargo bench --bench taql_perf -- join_1k_1k`
- C++ command: `./casacore_cpp_taql_shim $TABLE "SELECT t1.col1, t2.col2 FROM $T1 JOIN $T2 ON t1.key=t2.key"`
- Alert threshold: 5× C++ runtime.

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
