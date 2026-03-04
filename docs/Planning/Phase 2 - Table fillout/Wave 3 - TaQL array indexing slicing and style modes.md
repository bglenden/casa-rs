# Wave 3 - TaQL array indexing slicing and style modes

## Origin

- Backlog items: 2.2, 18.1.

## Goal

- Implement TaQL array element/slice access and style switching
  (`USING STYLE GLISH|PYTHON`).

## Non-goals

- Other expression syntax additions from Wave 2.
- Window and group aggregate functions.

## Scope

### Read path

- Parse/eval `arr[i]`, `arr[i,j]`, `arr[s:e[:step]]`.
- Add style-aware index/axis handling (Glish default, Python override).

### Write path

- N/A.

### API/docs/demo

- TaQL behavior only.

## Dependencies

- Wave 2 AST/parser shape decisions for expressions.

## Files likely touched

- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`
- `crates/casacore-tables/src/taql/exec.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add array index/slice AST + parser handling.
- [ ] Implement slicing/index extraction in eval.
- [ ] Add style context and parser support for `USING STYLE`.
- [ ] Add regression tests for GLISH and PYTHON behavior.

## Test plan

- [ ] Array indexing/slicing positive and error cases.
- [ ] Style mode tests for 1-based and 0-based behavior.
- [ ] Existing TaQL tests remain green.
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
