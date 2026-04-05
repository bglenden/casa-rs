# Wave 20 - `gaincal` Amplitude-Phase Expansion

## Origin

- Backlog item: 12.5 (partial).

## Goal

- Extend the first limited `gaincal` implementation from phase-only solves to
  the first `calmode='ap'` slice while keeping the same downstream acceptance
  contract: CASA-compatible caltable output and parity checked via downstream
  application.

## Non-goals

- Broader `solint` support beyond `inf`.
- Prior-caltable preapply.
- `bandpass`, `fluxscale`, or inspection work.
- Full solver-kernel factoring; note the cleanup need in backlog instead.

## Scope

### Public/library solve surface

- Extend `GainSolveMode` with amplitude-plus-phase solving.
- Keep the request surface otherwise narrow:
  - `gaintype=G|T`
  - `solint='inf'`
  - explicit refant
  - `smodel=[I,0,0,0]`

### Solver kernel

- Reuse the existing graph-based solve path for both `p` and `ap`.
- Carry per-edge accumulated weights alongside the complex edge sums.
- Use a mode-dependent weighted iterative update:
  - unit-modulus updates for `p`
  - complex updates for `ap`
- Preserve the current CASA-compatible caltable writer and downstream apply
  harness unchanged.

### Regression and parity coverage

- Add synthetic downstream tests for:
  - `G` amplitude-plus-phase solve
  - `T` amplitude-plus-phase solve
- Extend the slow CASA parity harness with a real-MS
  `gaincal(..., calmode='ap')` downstream comparison.
- Close the real-MS `ap` parity case with a solver fix, not by hiding the gap
  behind a giant flat tolerance.

## Dependencies

- Wave 19 first limited `gaincal` phase-only cut.
- Existing downstream apply parity harness in `casa_calibration_parity.rs`.

## Files touched

- `crates/casa-calibration/src/solve.rs`
- `crates/casa-calibration/tests/gain_solve.rs`
- `crates/casa-calibration/tests/casa_calibration_parity.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add failing synthetic `ap` downstream tests for `G` and `T`.
- [x] Add a failing slow CASA downstream parity test for `calmode='ap'`.
- [x] Extend the public solver mode enum and request surface.
- [x] Teach the graph solver to accumulate edge weights and solve `ap`.
- [x] Keep the writer/apply compatibility surface unchanged.
- [x] Close the real-MS `ap` parity gap and record the result in backlog/docs.

## Test plan

- [x] `cargo test -p casa-calibration --test gain_solve -- --nocapture`
- [x] `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`

## Interpretation

- The synthetic `ap` path converges cleanly and round-trips through the Rust
  apply path for both `G` and `T`.
- On the real `ngc5921.ms` downstream parity case, the remaining mismatch turned
  out not to be a broad numerical instability. The dominant error was that the
  `ap` solver pinned the reference-antenna amplitude to `1.0` instead of only
  pinning its phase. Allowing the refant amplitude to float during iteration
  while re-anchoring only the phase brought the solved Rust caltable into
  line with CASA and closed the downstream parity gap.
- The next leverage under `12.5` is no longer table-compatibility plumbing; it
  is solver expansion (`solint`, preapply) and solver cleanup/factoring.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration --test gain_solve -- --nocapture` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS

## Follow-ups

- Add broader `solint` support on top of the same caltable writer and
  downstream acceptance path.
- Add prior-caltable preapply before widening the solve surface further.
- Split solver math from MS selection / grouping / caltable writing so the
  numerical kernel can be improved or swapped more safely later.
