# Wave 13 - VirtualTaQLColumn engine

## Origin

- Backlog item: 16.1.

## Goal

- Implement `VirtualTaQLColumn` virtual engine with persistent expression
  spec and evaluation.

## Non-goals

- Other virtual engines (`MappedArrayEngine`, `RetypedArrayEngine`).
- Broader TaQL parser features outside engine needs.

## Scope

### Read path

- Parse/compile stored expression and evaluate per-row virtual values.

### Write path

- Persist expression in engine spec record.

### API/docs/demo

- Add binding API for `VirtualTaQLColumn`.

## Dependencies

- Existing virtual engine registry and TaQL evaluator.

## Files likely touched

- `crates/casacore-tables/src/storage/virtual_taql_column.rs`
- `crates/casacore-tables/src/storage/virtual_engine.rs`
- `crates/casacore-tables/src/storage/mod.rs`
- `crates/casacore-tables/src/table.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add engine implementation and registration.
- [ ] Add bind API and spec persistence.
- [ ] Add constant-expression cache path.
- [ ] Add reopen + interop tests.

## Test plan

- [ ] Expression evaluation correctness.
- [ ] Constant expression cache test.
- [ ] Save/reopen persistence test.
- [ ] C++ interop verification test.

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
