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

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Array fixture, interop is persistence format compatibility:
  1. Rust-written VirtualTaQLColumn table → C++ open and read virtual column values
  2. C++-written VirtualTaQLColumn table → Rust open and read virtual column values
  - Note: Interop is virtual engine persistence format, not TaQL query comparison.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (virtual engine plumbing, not compute-bound).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add engine implementation and registration.
- [x] Add bind API and spec persistence.
- [x] Add constant-expression cache path.
- [x] Add reopen + interop tests.
- [ ] Add C++ TaQL interop queries for this wave (2 persistence-compat tests above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] Expression evaluation correctness.
- [x] Constant expression cache test.
- [x] Save/reopen persistence test.
- [x] C++ interop verification test.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) — persistence format compatibility.
- [ ] Rust-written VirtualTaQLColumn readable by C++; C++-written readable by Rust.

## Performance plan

- Workload: N/A — virtual engine plumbing, not compute-bound.
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
