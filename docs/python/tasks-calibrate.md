# Calibrate Tasks

`casars.tasks.calibrate` keeps the standalone `calibrate` binary, the `casars` application, and the Python wrapper on the same conceptual contract.

The canonical unit is the calibration task request/result model in Rust. Python does not reconstruct flag lists. Every public wrapper:

1. resolves a `calibrate` binary
2. validates `--protocol-info`
3. sends one `CalibrationTaskRequest` through `--json-run -`
4. returns the canonical `CalibrationTaskResult` JSON envelope

## Public entry points

- `summary(...)`
- `stats(...)`
- `plan_apply(...)`
- `execute_apply(...)`
- `solve_gain(...)`
- `solve_bandpass(...)`
- `fluxscale(...)`

The wrappers use normal Python arguments and a few stdlib dataclasses for reused nested values such as `Selection`, `SolveCombine`, and `CalibrationTableSpec`, but the subprocess protocol itself remains the Rust request/result schema.
