# Wave 10 - Calibration Apply Planner

## Origin

- Backlog item: 12.3.

## Goal

- Add a deterministic apply-planning layer in `casa-calibration` that resolves
  MS row selection, `CORRECTED_DATA` creation needs, explicit `gainfield`,
  `spwmap`, and interpolation metadata for complex CASA calibration tables.

## Non-goals

- Public `casars` registration of `calibrate`.
- MAIN-table mutation or `applycal` execution.
- `gainfield='nearest'`, `callib`, `parang`, or solver work.

## Scope

### Planner library

- Add plan request/response types for apply planning.
- Resolve selected MS rows through existing `casa-ms` selection APIs.
- Detect whether `CORRECTED_DATA` must be created.
- Resolve exact `gainfield` by FIELD_ID or exact FIELD.NAME.
- Resolve `spwmap` and attach spectral-window grids needed by later execution.

### CLI/demo

- Extend the developer `calibrate` CLI with a `plan-apply` mode that emits
  text or JSON plans without mutating the MeasurementSet.

### Tests/docs

- Add synthetic fixture coverage for:
  - `CORRECTED_DATA` creation planning
  - explicit `gainfield` resolution by name
  - `spwmap` handling with `nearest,linear`
- Update Phase 4 planning docs to record the new wave.

## Dependencies

- Wave 9 caltable summary/validation substrate.
- Existing `casa-ms` selection and typed-subtable APIs.

## Ordering constraints

- This wave must complete before `applycal`-class execution work.

## Files likely touched

- `crates/casa-calibration/`
- `scripts/test-slow.sh`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Existing MS selection APIs reviewed for planner reuse.
- [x] Existing caltable summary surface reviewed for planner inputs.
- [x] Non-goals documented.

## Implementation checklist

- [x] Rename the new calibration crate/package away from `casacore-*`.
- [x] Add apply plan request/response types and planner errors.
- [x] Implement row selection, DDID/SPW resolution, and scratch-column planning.
- [x] Implement explicit `gainfield` resolution by FIELD_ID / exact name.
- [x] Implement `spwmap` resolution and attach spectral-window grid metadata.
- [x] Extend the developer CLI with `plan-apply`.
- [x] Add synthetic planner regression tests.

## Test plan

- [x] Synthetic planner coverage for selected-row and `CORRECTED_DATA` planning.
- [x] Synthetic planner coverage for exact-name `gainfield`.
- [x] Synthetic planner coverage for `spwmap` + `nearest,linear`.
- [ ] CASA parity for planner manifests on real task examples.

## Performance plan

- Workload: planner-only path, not executor throughput.
- Rust command: `cargo test -p casa-calibration`
- Alert threshold: no obvious pathological planner blow-up on small synthetic MS fixtures.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.

## Results

- Date:
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy -p casa-calibration --all-targets -- -D warnings` -> PASS
  - `cargo test -p casa-calibration` -> PASS
- Follow-ups:
  - CASA parity for planner manifests remains for a later slow-suite expansion.
  - Executor work stays in backlog item 12.4.

## Lessons learned

- The planner boundary is useful immediately: it exposed the public naming
  mistake early and made the `CORRECTED_DATA` creation contract testable
  without touching MAIN-table mutation yet.
