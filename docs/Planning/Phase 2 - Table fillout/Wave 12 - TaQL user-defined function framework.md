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

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Define trait and registration API.
- [ ] Integrate registry lookup into eval dispatch.
- [ ] Add collision/unknown-name behavior tests.

## Test plan

- [ ] Register and invoke custom UDF.
- [ ] UDF-over-builtin precedence test.
- [ ] Unregistered function error test.
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
