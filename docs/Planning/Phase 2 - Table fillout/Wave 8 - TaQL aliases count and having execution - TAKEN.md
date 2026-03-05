# Wave 8 - TaQL aliases count and having execution

## Origin

- Backlog items: 7.1, 7.2, 8.2.

## Goal

- Complete low-risk execution semantics for aliases, statement-level `COUNT`,
  and `HAVING` filtering.

## Non-goals

- JOIN execution.
- Set operations and subqueries.

## Scope

### Read path

- Ensure alias names are propagated to result columns.
- Add statement-level `COUNT SELECT ...` parse/exec.
- Apply `HAVING` predicate after aggregation.

### Write path

- N/A.

### API/docs/demo

- TaQL statement behavior only.

## Dependencies

- Existing select/group execution path.

## Files likely touched

- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/exec.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop queries:
  1. `SELECT col1 AS alias1 FROM $T`
  2. `SELECT col1 AS a, col2 AS b FROM $T WHERE a > 5`
  3. `COUNT SELECT col1 FROM $T WHERE col1 > 10`
  4. `SELECT grp, COUNT(val) AS cnt FROM $T GROUP BY grp HAVING cnt > 2`
  5. `SELECT grp, SUM(val) FROM $T GROUP BY grp HAVING SUM(val) > 100`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (statement semantics, not compute-bound).
- [x] Non-goals documented.

## Implementation checklist

- [x] Alias propagation in result schema.
- [x] `COUNT SELECT` parser + executor support.
- [x] HAVING filtering in grouped results.
- [ ] Add C++ TaQL interop queries for this wave (5 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Alias naming tests.
- [x] `COUNT SELECT` statement tests.
- [x] HAVING aggregate threshold tests.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 5 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: N/A — statement-level semantics, not compute-bound.
- Rust command: N/A.
- C++ command: N/A.
- Alert threshold: N/A.

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
