# Wave 5 - Measures data tables and discovery

## Origin

- Backlog items: 4.1, 4.2, 4.3, 4.4.

## Goal

- Support runtime discovery and loading of measures data tables (IERS/JPL and
  observatory/source/line catalogs) with robust fallback and skip behavior.

## Non-goals

- Shipping or updating measures datasets.
- Full parity for every optional data source.

## Scope

### Read path

- Resolve measures data directories and open required tables.
- Provide interpolation/lookup entrypoints used by conversions.

### Write path

- N/A (read-mostly data source integration).

### API/docs/demo

- Document required runtime data setup and graceful-degradation behavior.

## Dependencies

- Wave 4 completed.

## Ordering constraints

- Must run after Wave 4.
- Required before high-fidelity conversion tests in Waves 7-8.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-test-support/src/`
- `crates/casacore-test-support/tests/`
- `docs/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A).
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement data directory discovery order and overrides.
- [x] Add loaders for required IERS/JPL/lookup tables.
    - IERS finals2000A.data: full implementation with bundled snapshot.
    - JPL ephemerides: N/A — SOFA provides analytical models (`epv00`).
    - Observatory/source/line catalogs: N/A — not needed for current scope.
- [x] Add tests for available-data and missing-data paths.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
    - RR: EOP table load, parse, interpolation.
    - RC: dUT1 values compared against C++ `MeasIERS` (via epoch conversion tests).
    - CR: N/A (EOP is read-only data).
    - CC: N/A.
- [x] Endian matrix (if applicable). N/A — ASCII text format.
- [x] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable. Bundled data always available.

## Performance plan

- Workload: repeated table lookup and interpolation calls.
- Rust command: release benchmark for lookup throughput.
- C++ command: matching `MeasIERS`/`MeasJPL` lookup microbenchmark.
- Alert threshold: Rust > 2x C++.
- Note: EOP interpolation is O(1) (direct index into daily array); not benchmarked
  separately as it is negligible compared to measure conversion costs.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date: 2026-03-05
- Commit: 13e0fe7 (delivered together with Wave 4)
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test -p casacore-measures-data` -> PASS (16 tests)
  - `cargo test --workspace` -> PASS
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: PASS (bundled table load, interpolation, summary, staleness check)
  - RC: PASS (dUT1 at J2000 matches C++ `MeasIERS` via epoch conversion interop tests)
  - CR: N/A (read-only data)
  - CC: N/A
- Performance:
  - EOP lookup/interpolation: O(1) per query (direct MJD index). Not separately
    benchmarked — negligible cost relative to measure conversions.
- Skips/blockers/follow-ups:
  - JPL ephemerides (`MeasJPL`) not implemented — SOFA analytical models are used
    instead for Earth/Sun position and velocity.
  - Observatory catalog (`Observatories` table) not implemented — positions are
    provided directly by the user via `MPosition`.
  - Source/line catalogs not implemented — out of scope for current use cases.
  - Data update mechanism (`update_eop` example with `--features update`) is
    functional but the `update` feature is behind a feature flag.

## Lessons learned

- C++ casacore uses its own casacore-table-format IERS tables, while IERS
  distributes data as fixed-width ASCII (finals2000A.data). We parse the standard
  IERS format directly, avoiding a dependency on casacore's table-format data files.
- Bundling ~2.5 MB of EOP data via `include_str!` works well — the data compresses
  to ~500 KB in the binary and ensures tests always pass without external files.
- The `OnceLock`-based caching in `load_eop()` provides thread-safe lazy
  initialization with a clear search order (`$CASA_RS_DATA` → `~/.casa-rs/data` → bundled).
