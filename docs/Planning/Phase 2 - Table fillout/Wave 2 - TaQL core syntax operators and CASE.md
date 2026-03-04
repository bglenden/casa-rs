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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add tokens/lexer support.
- [ ] Extend AST/parser.
- [ ] Implement evaluator semantics and errors.
- [ ] Add parser/eval regression tests.

## Test plan

- [ ] Parser coverage for new syntax forms.
- [ ] Eval coverage for valid/invalid expressions.
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
