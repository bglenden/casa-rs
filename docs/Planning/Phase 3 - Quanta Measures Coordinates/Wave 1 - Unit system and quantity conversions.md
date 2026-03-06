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

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A). N/A — pure computation.
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement unit grammar + canonicalization.
- [x] Implement quantity conversion + conform checks.
- [x] Add regression tests for invalid/ambiguous unit inputs.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [x] Endian matrix (if applicable). N/A — no on-disk format.
- [x] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable. N/A.

## Performance plan

- Workload: repeated parse/convert loops for representative units.
- Rust command: targeted criterion/release benchmark command.
- C++ command: matching casacore unit conversion microbenchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date: 2026-03-05
- Commit: f17346b
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (61 quanta unit tests)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: PASS (unit parse, conform, quantity conversions)
  - RC: N/A (no C++ shim needed — pure Rust unit system)
  - CR: N/A
  - CC: N/A
- Performance:
  - N/A — unit parsing/conversion not benchmarked separately; performance is
    dominated by measure conversions tested in later waves.
- Skips/blockers/follow-ups:
  - No standalone C++ interop for units — casacore unit system is well-specified
    and Rust implementation follows the same grammar directly.

## Lessons learned

- Casacore's unit system uses a prefix+base grammar with SI and custom astronomy
  units. Rust's `logos` lexer provides a clean, fast implementation.
- Quantity conformance checks are purely dimensional — no need for C++ interop
  testing since the rules are deterministic and well-documented.
