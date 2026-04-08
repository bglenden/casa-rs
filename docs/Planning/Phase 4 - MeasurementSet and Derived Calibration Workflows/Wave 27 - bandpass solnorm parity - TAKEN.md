# Wave 27 - `bandpass solnorm` Parity

## Origin

- Backlog item: 12.6 (partial).
- Follow-on from Wave 26.

## Goal

- Close the first `solnorm` slice for the existing `B Jones` solver by matching
  CASA's actual normalization behavior, not just the high-level task wording.

## Non-goals

- `combine=*`
- `BPOLY`
- Any new `bandpass` CLI or schema/UI surfacing

## Scope

### Solver behavior

- Remove the temporary `solnorm=true` rejection from the first `bandpass`
  implementation.
- Match CASA's `BJones::normalize()` behavior:
  - per `(spw,time,antenna,pol)` row
  - coherent mean phase across channels
  - RMS amplitude normalization across channels

### Validation

- Add a synthetic regression that checks normalized bandpass rows directly.
- Add a slow real-MS parity test against CASA `bandpass(..., solnorm=True)`.

## Dependencies

- Existing `B Jones` solver
- Existing slow CASA bandpass/applycal parity harness

## Files touched

- `crates/casa-calibration/src/bandpass.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `crates/casa-calibration/tests/bandpass_solve.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Remove the temporary unsupported `solnorm` error.
- [x] Implement CASA-aligned complex normalization over solved bandpass rows.
- [x] Extend the CASA helper to run `bandpass(..., solnorm=True)`.
- [x] Add synthetic regression coverage.
- [x] Close slow downstream parity on `ngc5921.ms`.

## Test plan

- [x] `cargo test -p casa-calibration --test bandpass_solve solve_bandpass_with_solnorm_normalizes_per_receptor_average_amplitude`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_bandpass_with_solnorm_matches_casa_bandpass_downstream_via_casa_applycal -- --nocapture`

## Interpretation

- The important part of this wave is that the implementation is now tied to the
  CASA C++ normalization rule rather than an approximation based only on task
  documentation.
- `bandpass` remains limited, but one of the key remaining semantics under
  `12.6` is now closed with parity evidence.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration --test bandpass_solve solve_bandpass_with_solnorm_normalizes_per_receptor_average_amplitude` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity solve_bandpass_with_solnorm_matches_casa_bandpass_downstream_via_casa_applycal -- --nocapture` -> PASS

## Follow-ups

- Keep `combine=*` and `BPOLY` explicit backlog work under `12.6`.
- Reuse the same source-driven approach for any remaining solver semantics.
