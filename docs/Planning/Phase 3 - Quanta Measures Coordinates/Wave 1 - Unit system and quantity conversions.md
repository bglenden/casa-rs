# Wave 1 - Unit system and quantity conversions

## Origin

- Backlog items: 1.1, 1.2, 1.3.

## Goal

- Add casacore-compatible unit parsing, dimensional conformance, and quantity
  conversion primitives.

## Non-goals

- Measure reference frames (`MEpoch`, `MDirection`, etc.).
- Table keyword integration (`QuantumUnits`, `MEASINFO`).

## Scope

### Read path

- Parse unit strings and validate dimensional compatibility.
- Normalize units for conversion and comparison.

### Write path

- Serialize quantity values with explicit unit text where required.

### API/docs/demo

- Public `Unit`/`Quantity`-style Rust API with conversion and conformance checks.

## Dependencies

- None.

## Ordering constraints

- First wave in Phase 3.
- Required before Waves 2-6.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-types/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement unit grammar + canonicalization.
- [ ] Implement quantity conversion + conform checks.
- [ ] Add regression tests for invalid/ambiguous unit inputs.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated parse/convert loops for representative units.
- Rust command: targeted criterion/release benchmark command.
- C++ command: matching casacore unit conversion microbenchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 3 closeout gates pass.
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
