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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add missing function implementations.
- [ ] Add eval keyword context plumbing.
- [ ] Add deterministic-range and behavior tests.

## Test plan

- [ ] `pattern`/`sqlpattern` matching tests.
- [ ] `string` formatted output tests.
- [ ] `iskeyword` true/false tests.
- [ ] `rand()` bounds and non-constant behavior tests.

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
