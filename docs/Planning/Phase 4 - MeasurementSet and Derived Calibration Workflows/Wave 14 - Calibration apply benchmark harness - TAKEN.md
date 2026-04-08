# Wave 14 - Calibration Apply Benchmark Harness

## Origin

- Backlog item: 12.4 (partial).

## Goal

- Add a reproducible benchmark harness for the current `applycal`-class Rust
  workflow against actual CASA `applycal`, using the same real-MS copy/setup
  path as the parity tests.

## Non-goals

- Performance optimization itself.
- `calwt`.
- Solver benchmarks.
- CI gating on benchmark numbers.

## Scope

### Benchmark script

- Add a standalone script under `scripts/` that:
  - discovers `CASA_RS_CASA_PYTHON` and `CASA_RS_TESTDATA_ROOT`
  - uses `ngc5921.ms` by default
  - generates a CASA `phase.gcal` once
  - warms both Rust and CASA once
  - copies the MS fresh for each timed run
  - times only the apply step for Rust and CASA
  - reports per-run timings, medians, and a Rust/CASA ratio

### Benchmark defaults

- Field: `0`
- SPW: `0`
- Refant: `VA15`
- Apply mode: `calflag`

## Dependencies

- Wave 13 parity harness and helper conventions.
- Public `calibrate` CLI surface from Wave 12.

## Ordering constraints

- This wave should land before any optimization wave so performance work starts
  from a fixed, reproducible measurement path.

## Files likely touched

- `scripts/bench-calibrate-vs-casa.sh`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`

## Definition of Ready

- [x] Real CASA apply parity exists for the same workload shape.
- [x] Public release-mode `calibrate` CLI exists.
- [x] Benchmark non-goals documented.

## Implementation checklist

- [x] Add the benchmark script.
- [x] Exclude MS copying and caltable generation from timed sections.
- [x] Report medians and Rust/CASA ratio.
- [x] Run a smoke benchmark and record results.

## Test plan

- [x] `cargo fmt --all -- --check`
- [x] Script smoke run with `CAL_BENCH_REPEATS=2`

## Performance results

- Command:
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`
- Workload:
  - `ngc5921.ms`
  - CASA-generated `phase.gcal`
  - `field=0`
  - `spw=0`
  - `apply_mode=calflag`
  - timing excludes MS copy and caltable generation
- Results:
  - Rust median: `0.880000s`
  - CASA median: `0.091177s`
  - Rust/CASA ratio: `9.652`
- Interpretation:
  - Rust is well slower than the Phase 4 `2x` threshold, so this wave must
    trigger explicit follow-on optimization work rather than closing item 12.4.

## Closeout criteria

- [ ] All Phase 4 closeout gates pass.
- [x] Reproducible benchmark harness exists.
- [x] Follow-on optimization need is documented explicitly.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh` -> PASS
- Follow-ups:
  - Profile and reduce Rust apply overhead before item 12.4 can be closed.
  - `calwt` remains deferred within item 12.4.

## Lessons learned

- The benchmark needs to share the same setup path as parity; otherwise it is
  too easy to optimize a workload that does not reflect the real correctness
  path.
- The current bottleneck is large enough that the next wave should focus on
  profiling and architectural overhead, not marginal tuning.
