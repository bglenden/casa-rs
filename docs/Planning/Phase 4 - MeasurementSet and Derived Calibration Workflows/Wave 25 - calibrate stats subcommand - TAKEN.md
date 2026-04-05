# Wave 25 - `calibrate stats` Subcommand

## Origin

- Backlog item: 12.6 (partial).
- Follow-on from Wave 24.

## Goal

- Surface the existing library-first `calstat` capability through the public
  `calibrate` binary so calibration-table statistics are part of the shipped
  app, not just a library API.

## Non-goals

- Any new statistics math beyond the Wave 24 library surface
- A schema-driven stats form in `casars`
- `fluxscale`
- `solnorm`
- `combine=*`
- `BPOLY`

## Scope

### CLI surface

- Add a `stats` subcommand to `calibrate`.
- Support:
  - one calibration-table path
  - `--axis`
  - `--datacolumn`
  - `--use-flags`
  - `--stats-format text|json`
  - `--stats-output`
  - `--overwrite`

### Output

- Render human-readable text summaries from the existing report type.
- Preserve pretty JSON output for scripting and machine-readable inspection.

### Validation

- Add CLI parsing coverage for the new subcommand.
- Re-run the existing slow CASA parity test for the shared stats engine.

## Dependencies

- Wave 24 library-first `calstat` implementation.

## Files touched

- `crates/casa-calibration/src/cli.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add `calibrate stats` argument parsing.
- [x] Route the subcommand through the existing library stats API.
- [x] Add text and JSON rendering.
- [x] Add CLI parsing regression coverage.
- [x] Re-run slow CASA parity coverage for the shared stats engine.

## Test plan

- [x] `cargo test -p casa-calibration`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test stats -- --nocapture`

## Interpretation

- This wave keeps the app layer thin: no new stats logic lives in the CLI.
- The public calibration app now includes a direct inspection entry point, but
  the schema-driven `casars` form is still centered on apply workflows.
- Any future stats-specific form/schema work should stay under `12.6` instead of
  becoming ad hoc CLI-only expansion.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test stats -- --nocapture` -> PASS

## Follow-ups

- Decide whether stats need a first-class schema-driven `casars` form or whether
  the direct CLI surface is sufficient.
- Keep `fluxscale`, `solnorm`, `combine=*`, and `BPOLY` as explicit follow-on
  backlog work.
