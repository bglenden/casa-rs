# Wave 1 - casacore-ms crate scaffold and canonical schema constants

## Origin

- Backlog items: 1.1, 1.2, 1.3.

## Goal

- Create a dedicated `casacore-ms` crate and define canonical MS table/subtable
  schema constants and shared identifiers.

## Non-goals

- Row data read/write workflows.
- Derived calculations and selection logic.

## Scope

### Read path

- N/A (scaffold and schema constants only).

### Write path

- N/A (scaffold and schema constants only).

### API/docs/demo

- Expose shared schema descriptors and naming constants for MAIN and subtables.

## Dependencies

- Phase 3 Wave 6 completed.

## Ordering constraints

- First wave in Phase 4.
- Required before Waves 2-8.

## Files likely touched

- `Cargo.toml`
- `crates/casacore-ms/`
- workspace docs and crate-level docs

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Add new crate and workspace wiring.
- [ ] Define canonical schema constants for core MS tables.
- [ ] Add doc and compile tests for schema definitions.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: N/A.
- Rust command: N/A.
- C++ command: N/A.
- Alert threshold: N/A.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
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
