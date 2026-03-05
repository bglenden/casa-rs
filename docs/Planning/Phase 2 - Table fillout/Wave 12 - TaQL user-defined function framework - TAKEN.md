# Wave 12 - TaQL user-defined function framework

## Origin

- Backlog item: 8.3.

## Goal

- Add static-registration UDF framework for TaQL functions.

## Non-goals

- Dynamic library loading.
- JOIN and statement semantics.

## Scope

### Read path

- Add function trait, registry, and lookup precedence over built-ins.

### Write path

- N/A.

### API/docs/demo

- Public registration API for custom TaQL functions.

## Dependencies

- Existing TaQL function dispatch path.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/mod.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop is regression-only:
  1. `SELECT sin(col1), cos(col1), sqrt(col1) FROM $T`
  - Note: UDFs are Rust-only; interop verifies built-in functions still work correctly after UDF framework integration.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (framework plumbing, not compute-bound).
- [x] Non-goals documented.

## Implementation checklist

- [x] Define trait and registration API.
- [x] Integrate registry lookup into eval dispatch.
- [x] Add collision/unknown-name behavior tests.
- [ ] Add C++ TaQL interop regression query (1 query above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Register and invoke custom UDF.
- [x] UDF-over-builtin precedence test.
- [x] Unregistered function error test.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) — regression: built-ins still work after UDF integration.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: N/A — framework plumbing, not compute-bound.
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
