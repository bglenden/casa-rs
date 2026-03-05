# Wave 7 - TaQL missing utility functions

## Origin

- Backlog items: `pattern()`/`sqlpattern()`, extended `string()`,
  `iskeyword()`, and `rand()`.

## Goal

- Fill remaining TaQL function gaps in `functions.rs` plus eval context wiring
  for keyword lookup.

## Non-goals

- Group aggregate families.
- Statement/clause execution changes.

## Scope

### Read path

- Implement `pattern()` and `sqlpattern()` helpers.
- Add `string(value, format)` and width/precision forms.
- Replace `iskeyword()` stub with real keyword lookup.
- Replace deterministic `rand()` with proper RNG behavior.

### Write path

- N/A.

### API/docs/demo

- TaQL function behavior only.

## Dependencies

- Eval context access to table keyword records.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`
- `crates/casacore-tables/src/taql/eval.rs`

## Definition of Ready

- [x] C++ reference paths identified — `tables/TaQL/TableParse.h: tableCommand()`
- [x] 2x2 interop fixtures identified — Simple fixture (50-row scalar), interop queries:
  1. `SELECT pattern('test*', 'f') FROM $T`
  2. `SELECT sqlpattern('test%') FROM $T`
  3. `SELECT string(col1, '%06.2f') FROM $T`
  4. `SELECT iskeyword('EXISTING_KEY') FROM $T`
  - Note: `rand()` cannot be compared (different RNGs); verify bounds only (0 ≤ r < 1).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined or marked N/A — N/A (utility functions, no hot path).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add missing function implementations.
- [x] Add eval keyword context plumbing.
- [x] Add deterministic-range and behavior tests.
- [ ] Add C++ TaQL interop queries for this wave (4 queries above).
- [ ] Run cross-matrix and record results.

## Test plan

- [x] `pattern`/`sqlpattern` matching tests.
- [x] `string` formatted output tests.
- [x] `iskeyword` true/false tests.
- [x] `rand()` bounds and non-constant behavior tests.
- [ ] TaQL interop cross-matrix (RR/CC/RC/CR) for 4 queries.
- [ ] Results match between Rust and C++ within tolerance (except `rand()` — bounds-only check).

## Performance plan

- Workload: N/A — utility functions, no hot-path concern.
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
