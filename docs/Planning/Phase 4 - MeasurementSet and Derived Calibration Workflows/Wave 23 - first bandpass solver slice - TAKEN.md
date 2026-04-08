# Wave 23 - First `bandpass` Solver Slice

## Origin

- Backlog item: 12.6 (partial).

## Goal

- Land the first narrow `bandpass` implementation on top of the existing
  calibration substrate, apply executor, and prior-gain solve path.

## Non-goals

- `solnorm`
- `combine=*`
- `BPOLY`
- `calstat`, `fluxscale`, or inspection UI/workspaces

## Scope

### Public/library solve surface

- Add a library-first `bandpass` API with:
  - `bandtype='B'`
  - `solint='inf'`
  - explicit refant
  - prior gain-table preapply
  - point-source `smodel=[I,0,0,0]`
- Keep the first cut out of the CLI surface for now.

### Solver and writer behavior

- Solve per-channel complex diagonal `B Jones` terms on interferometric data.
- Reuse the existing apply planner/executor path for prior-calibration
  preapply.
- Reuse the existing graph-based solve kernel per channel.
- Emit CASA-compatible channelized `CPARAM` caltables with subtype `B Jones`.
- Preserve the full `SPECTRAL_WINDOW` channel grid in the output caltable.

### Regression and parity coverage

- Add a synthetic downstream-correction test for prior `G` plus solved `B`.
- Extend the slow CASA parity harness with a real-MS downstream comparison
  against CASA `bandpass(..., gaintable=[prior G])`.

## Dependencies

- Existing `applycal`-class executor and caltable substrate.
- Existing `gaincal` prior-solve support.
- Existing solve factoring from Wave 22.

## Files touched

- `crates/casa-calibration/src/bandpass.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/src/solve/kernel.rs`
- `crates/casa-calibration/src/solve/writer.rs`
- `crates/casa-calibration/tests/bandpass_solve.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add a library request/report/error surface for limited `bandpass`.
- [x] Reuse the shared selection and prior-preapply machinery.
- [x] Add a per-channel diagonal `B Jones` solver and channelized caltable writer.
- [x] Add a synthetic downstream-correction test.
- [x] Add a slow CASA downstream parity test using prior `G` plus `B`.

## Test plan

- [x] `cargo test -p casa-calibration`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

## Interpretation

- The first `bandpass` slice is useful without trying to absorb the whole CASA
  task surface. Prior gain-table preapply is part of the minimal practical
  workflow, so it is included from the start.
- The output contract is strong: CASA can consume the Rust-written `B` table in
  downstream `applycal` alongside the same prior gain table used by CASA.
- The remaining `12.6` leverage is now semantic and workflow-oriented rather
  than basic `B`-table persistence.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS

## Follow-ups

- Add `solnorm` if it proves necessary for real workflows.
- Decide whether `combine=*` belongs before `calstat` / `fluxscale`.
- Add `calstat`-class table statistics and summary/inspection support.
- Add `fluxscale` only after the broader solve chain is stable.
