# Wave 3 - Measure base model epoch and position

## Origin

- Backlog items: 3.1, 3.2, 3.3.

## Goal

- Establish measure base/reference model and first concrete types: `MEpoch`
  and `MPosition`.

## Non-goals

- Direction/frequency/doppler measure families.
- TableMeasures `MEASINFO` persistence.

## Scope

### Read path

- Parse measure records for epoch/position values and reference types.

### Write path

- Serialize epoch/position measures with casacore-compatible record layout.

### API/docs/demo

- Provide typed measure constructors, ref enums, and conversion entrypoints.

## Dependencies

- Wave 1 completed.
- **SOFA/ERFA routines**: Use the pure-Rust `sofars` crate
  (<https://github.com/astro-xao/sofars>) instead of linking against the C
  `liberfa`/`libsofa_c` library. This avoids a C build dependency and keeps the
  crate tree pure Rust. The `sofars` crate provides the IAU SOFA algorithms
  needed for precession, nutation, Earth rotation, and related frame conversions.

## Ordering constraints

- Must run after Wave 1.
- Must run before Waves 4-8.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-types/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A). EOP tables needed for UT1.
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement measure base/ref abstractions for typed measures.
- [x] Add `MEpoch` and `MPosition` types with core ref enums.
- [x] Add interop tests for record-encoded measure values.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [x] Endian matrix (if applicable). N/A — measure records use existing table format.
- [x] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: batch construction/conversion of epoch/position values.
- Rust command: release benchmark for core measure operations.
- C++ command: `tMeasure` subset benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date: 2026-03-05
- Commit: 959f330
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (47 measures unit tests + 63 integration tests)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: PASS (epoch UTC↔TAI↔TT↔TDB↔UT1↔GAST↔LAST, position WGS84↔ITRF, record format)
  - RC: PASS (epoch conversions, position conversions, record encoding)
  - CR: PASS (epoch conversions, position conversions, record decoding)
  - CC: PASS (epoch roundtrips UTC↔TAI, UTC→TT, TT→TDB; position roundtrip)
- Performance (release, 10k conversions):
  - Epoch UTC→TAI: ratio 2.41x (Rust slower — SOFA overhead for leap-second lookup)
  - Epoch TT→TDB: ratio 3020x (known: SOFA `dtdb` uses full Fairhead & Bretagnon series)
  - Position WGS84→ITRF: ratio 0.05x (Rust 20x faster)
  - Position ITRF→WGS84: ratio 0.09x (Rust 11x faster)
- Skips/blockers/follow-ups:
  - Epoch TT→TDB performance is dominated by SOFA's `dtdb` full analytical series;
    casacore uses a simpler polynomial approximation. Not a correctness issue.
  - `MeasFrame` uses `sofars` (pure Rust SOFA) instead of casacore's bespoke algorithms.
    See Wave 4 lessons learned for deviation analysis.

## Lessons learned

- C++ `MEpoch` stores MJD as a pair (integer day + fractional day) for precision.
  Rust `MjdHighPrec` mirrors this with `(day, frac)` representation.
- C++ `MeasFrame` lazily computes derived quantities. Rust takes an immutable-builder
  approach instead, computing derived values on demand.
- EOP (Earth Orientation Parameter) data is required for UT1 and GAST/LAST
  conversions. The `casacore-measures-data` crate bundles IERS finals2000A.data
  to ensure tests always work without external data.
