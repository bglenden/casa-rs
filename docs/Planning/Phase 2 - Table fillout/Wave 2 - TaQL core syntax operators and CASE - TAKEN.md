# Wave 2 - TaQL core syntax operators and CASE

## Origin

- Backlog items: 2.1, 2.3, 2.4, 2.5.

## Goal

- Add missing core TaQL syntax in lexer/parser/eval for regex literals,
  bitwise operators, bracket `IN` ranges/sets, and `CASE` expressions.

## Non-goals

- Array indexing style switching (handled in Wave 3).
- Window, group, and JOIN execution changes.

## Scope

### Read path

- Parse and evaluate:
  - `p/.../flags` and `m/.../flags`
  - `& | ^ ~`
  - `IN [a,b,c]`, `IN [a:b[:s]]`
  - `CASE WHEN ... THEN ... ELSE ... END`

### Write path

- N/A.

### API/docs/demo

- Internal TaQL grammar/eval only.

## Dependencies

- Existing TaQL AST/parser/eval pipeline.

## Files likely touched

- `crates/casacore-tables/src/taql/token.rs`
- `crates/casacore-tables/src/taql/lexer.rs`
- `crates/casacore-tables/src/taql/ast.rs`
- `crates/casacore-tables/src/taql/parser.rs`
- `crates/casacore-tables/src/taql/eval.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop queries:
  1. `SELECT col1 FROM $T WHERE col1 =~ p/^A.*/i`
  2. `SELECT col1 FROM $T WHERE col1 =~ m/test/`
  3. `SELECT col1 & 0xFF FROM $T`
  4. `SELECT col1 | 0x10 FROM $T`
  5. `SELECT col1 ^ col2 FROM $T`
  6. `SELECT col1 FROM $T WHERE col1 IN [1,2,3]`
  7. `SELECT col1 FROM $T WHERE col1 IN [1:10]`
  8. `SELECT CASE WHEN col1>5 THEN 'big' ELSE 'small' END FROM $T`
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (pure expression eval, no hot path).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add tokens/lexer support.
- [x] Extend AST/parser.
- [x] Implement evaluator semantics and errors.
- [x] Add parser/eval regression tests.
- [ ] Add C++ TaQL interop queries for this wave (8 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Parser coverage for new syntax forms.
- [x] Eval coverage for valid/invalid expressions.
- [x] Existing TaQL tests remain green.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 8 queries.
- [ ] Results match between Rust and C++ within tolerance.

## Performance plan

- Workload: N/A — pure expression evaluation, no hot-path concern.
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
