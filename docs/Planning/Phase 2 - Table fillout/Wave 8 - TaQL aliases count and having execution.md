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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Alias propagation in result schema.
- [ ] `COUNT SELECT` parser + executor support.
- [ ] HAVING filtering in grouped results.

## Test plan

- [ ] Alias naming tests.
- [ ] `COUNT SELECT` statement tests.
- [ ] HAVING aggregate threshold tests.
- [ ] Clean skip when `pkg-config casacore` is unavailable.

## Performance plan

- Workload: N/A.
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
