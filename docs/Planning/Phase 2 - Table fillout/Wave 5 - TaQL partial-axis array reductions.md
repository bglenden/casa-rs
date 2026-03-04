# Wave 5 - TaQL partial-axis array reductions

## Origin

- Backlog item: 4.1.

## Goal

- Add partial-axis reduction functions (`SUMS`, `MEANS`, `MINS`, etc.) for
  array arguments.

## Non-goals

- Running/boxed window functions.
- Group aggregate families.

## Scope

### Read path

- Register and evaluate partial-axis reduction forms in function dispatch.

### Write path

- N/A.

### API/docs/demo

- TaQL function behavior only.

## Dependencies

- `ndarray` lane/axis reduction support.

## Files likely touched

- `crates/casacore-tables/src/taql/functions.rs`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add function registration and arg validation.
- [ ] Implement single- and multi-axis reductions.
- [ ] Add shape/result consistency tests.

## Test plan

- [ ] 2-D and multi-axis reduction coverage.
- [ ] 1-D reduction-to-scalar edge case.
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
