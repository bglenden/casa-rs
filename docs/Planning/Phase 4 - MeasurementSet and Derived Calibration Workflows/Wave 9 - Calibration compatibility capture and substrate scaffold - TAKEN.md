# Wave 9 - Calibration Compatibility Capture and Substrate Scaffold

## Origin

- Backlog item: 12.1.

## Goal

- Freeze real CASA `G` / `T` / `B` exemplar evidence and land the first
  `casa-calibration` substrate able to open, validate, and summarize those
  tables through the Rust table stack.

## Non-goals

- Public `casars` registration of `calibrate`.
- MS mutation or `applycal` execution.
- Any solving beyond CASA-generated exemplars.

## Scope

### Read path

- Generate tiny CASA-produced calibration tables from `ngc5921.ms`.
- Ensure `casa-tables` can open those tables.
- Add `casa-calibration` summary/validation APIs over the opened tables.

### Write path

- Persist a minimal Rust-authored complex calibration-table fixture for fast
  regression coverage.
- Do not promise canonical writer parity yet beyond that synthetic fixture.

### API/docs/demo

- Add a developer CLI to summarize calibration tables.
- Add Phase 4 backlog and wave documentation for the calibration track.

## Dependencies

- Existing `casa-tables` storage stack.
- Existing `CASA_RS_CASA_PYTHON` and `CASA_RS_TESTDATA_ROOT` conventions.

## Ordering constraints

- This wave must complete before apply planning or execution work.

## Files likely touched

- `crates/casa-tables/`
- `crates/casa-calibration/`
- `scripts/`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (shared `ngc5921.ms` fixture).
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Fix the scalar-`Direct` table-schema compatibility bug exposed by CASA `gaincal`.
- [x] Add `casa-calibration` crate scaffold with summary/validation API and CLI.
- [x] Add fast synthetic summary coverage for a Rust-authored minimal complex caltable.
- [x] Add slow CASA exemplar generation and summary parity tests for `G`, `T`, and `B`.
- [x] Add a manual exemplar-capture script and Phase 4 backlog entries.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [x] Edge cases (empty/zero-length/undefined/boundary/schema validation).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable.

## Performance plan

- Workload: N/A for this wave; capture/read correctness only.
- Rust command: `cargo test -p casa-calibration`
- C++ command: CASA-generated exemplar capture through `CASA_RS_CASA_PYTHON`
- Alert threshold: N/A

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date:
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
- Interop matrix:
  - RR: synthetic Rust-authored minimal caltable summary fixture
  - RC: pending later canonical writer parity
  - CR: CASA-generated `G` / `T` / `B` exemplar summary tests
  - CC: delegated to CASA runtime oracle for exemplar generation
- Performance:
  - Rust: N/A
  - C++: N/A
  - Ratio: N/A
- Skips/blockers/follow-ups:
  - Canonical writer parity remains backlog item 12.2 follow-on work.

## Lessons learned

- Real CASA calibration tables exposed a table-schema compatibility bug before
  the calibration crate existed, which validates the decision to capture
  exemplars first instead of guessing at the shape.
