# Wave 4 - Direction frequency and doppler measures

## Origin

- Backlog items: 3.4, 3.5, 3.6.

## Goal

- Add `MDirection`, `MFrequency`, and `MDoppler` with core frame conversion
  behavior used by table metadata and TaQL.

## Non-goals

- Full planet/comet catalog and advanced ephemeris routes.
- Coordinate projection/WCSLIB integration.

## Scope

### Read path

- Decode direction/frequency/doppler records and reference variants.

### Write path

- Encode records and conversion metadata for persisted measure values.

### API/docs/demo

- Expose typed conversions and frame descriptors for these measure families.

## Dependencies

- Wave 3 completed.
- **SOFA/ERFA routines**: Uses `sofars` (pure Rust) — see Wave 3 dependency note.

## Ordering constraints

- Must run after Wave 3.
- Must run before Waves 5, 7, and 8.

## Files likely touched

- `crates/casacore-types/src/`
- `crates/casacore-types/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A). EOP for HADEC/AZEL/ITRF.
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add direction/frequency/doppler type and ref enums.
- [x] Implement core conversion routes used by C++ parity tests.
- [x] Add regression tests for unsupported frame combinations.
- [x] Add `MRadialVelocity` (originally Wave 5 scope, pulled forward).

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [x] Endian matrix (if applicable). N/A — measure records use existing table format.
- [x] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: repeated direction/frequency conversions.
- Rust command: release benchmark for conversion engine loops.
- C++ command: `tMDirection` and `tMFrequency` subset.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date: 2026-03-05
- Commit: 13e0fe7
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (107 interop tests, 19 perf tests)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: PASS (direction J2000↔B1950/GALACTIC/ECLIPTIC/SUPERGAL/JMEAN/ICRS/HADEC/AZEL/ITRF,
    frequency LSRK↔BARY/GEO/TOPO/LGROUP/CMB/LSRD, doppler RADIO↔Z/BETA/GAMMA,
    radvel LSRK↔BARY/GEO/LGROUP/CMB, IAU 2000A direction tests)
  - RC: PASS (all measure types — Rust converts, compared against C++ output)
  - CR: PASS (all measure types — C++ output fed to Rust for reverse conversion)
  - CC: PASS (roundtrip tests for direction, frequency, doppler, radvel)
- Performance (release, 10k conversions):
  - Direction J2000→GALACTIC: ratio 0.36x (Rust 2.8x faster)
  - Direction J2000→B1950: ratio 0.61x (Rust 1.6x faster)
  - Direction J2000→ITRF: ratio 102x (known: multi-hop chain with SOFA full series)
  - Frequency LSRK→BARY: ratio 1.67x
  - Doppler BETA→GAMMA: ratio 0.06x (Rust 17x faster)
  - RadVel LSRK→BARY: ratio 0.85x
  - RadVel BARY→GEO: ratio 18.76x (known: Earth velocity computation overhead)
- Skips/blockers/follow-ups:
  - Direction J2000→ITRF and BARY→GEO performance dominated by SOFA's full
    analytical series called per-conversion. C++ caches intermediate results
    across the conversion chain. Performance optimization is a follow-up.
  - SOFA vs casacore algorithmic deviation: ~1.5 mas (IAU 1976/1980), ~16 mas
    (IAU 2000A). Root cause: different precession/nutation parameterizations.
    Documented in `direction.rs` module docs and `misc/github_issue_draft.md`.
  - Planet/comet ephemeris directions not implemented (non-goal).

## Lessons learned

- C++ casacore does NOT use SOFA/ERFA internally — it has bespoke implementations
  of precession (Euler angle polynomials), nutation (custom series), and aberration
  (Stumpff polynomial series). SOFA is only an optional test dependency. This means
  our Rust implementation (using `sofars`) will never match C++ to machine precision.
- The ~16 mas IAU 2000A deviation comes from different precession parameterizations:
  casacore uses ζA/zA/θA Euler angles with frame bias in constant terms; SOFA uses
  ψA/ωA/χA (Lieske 1977) with IAU 2000 corrections and separate frame bias matrix.
- Direction conversion routing uses BFS over a graph of reference-frame edges,
  matching C++ `MCDirection::getConvert()`. The ITRF↔HADEC edge must go through
  HADEC (matching C++ `MeasMath::applyHADECtoITRF`), not directly from J2000.
- Angular separation on the sphere requires the Vincenty formula — coordinate
  differences in longitude are NOT angular distances (must account for cos(lat)).
- GEO↔TOPO frequency conversions require dUT1 in the frame (for Earth rotation
  angle needed by observatory velocity computation).
