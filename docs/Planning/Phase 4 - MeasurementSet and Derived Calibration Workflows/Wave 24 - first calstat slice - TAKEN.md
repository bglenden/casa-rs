# Wave 24 - First `calstat` Slice

## Origin

- Backlog item: 12.6 (partial).

## Goal

- Land a library-first statistics/reporting surface over calibration tables so
  the tables produced by the existing apply/solve workflows can be inspected
  quantitatively without adding a browser/UI workflow yet.

## Non-goals

- `fluxscale`
- `solnorm`
- `combine=*`
- `BPOLY`
- any interactive inspection UI or plotting

## Scope

### Public/library stats surface

- Add a library-first `calstat`-class API over calibration tables.
- Support CASA-style complex axes:
  - `amp` / `amplitude`
  - `phase`
  - `real`
  - `imag` / `imaginary`
- Support real-valued column statistics for scalar or array columns.
- Normalize CASA-style `datacolumn='gain'` to `CPARAM`.

### Returned report shape

- Return global stats with:
  - `npts`
  - `sum`
  - `sumsq`
  - `mean`
  - `median`
  - `medabsdevmed`
  - quartiles
  - `min` / `max`
  - `var`
  - `stddev`
  - `rms`
- Report flagged and total value counts separately.
- Add grouped stats by:
  - `FIELD_ID`
  - `SPECTRAL_WINDOW_ID`
  - `ANTENNA1`
  - `OBSERVATION_ID`

### Regression and parity coverage

- Add synthetic tests for exact global and grouped amplitude stats.
- Add synthetic coverage for excluding flagged values.
- Add a slow CASA parity test comparing global `calstat` amplitude stats on a
  generated phase-gain table.

## Dependencies

- Existing calibration-table substrate and summary/open paths.
- Existing slow CASA calibration harness.

## Files touched

- `crates/casa-calibration/src/stats.rs`
- `crates/casa-calibration/src/lib.rs`
- `crates/casa-calibration/tests/stats.rs`
- `crates/casa-calibration/tests/common/mod.rs`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/BACKLOG.md`

## Implementation checklist

- [x] Add a public library stats API and report types.
- [x] Support complex-axis transforms over `CPARAM`.
- [x] Support real-valued column statistics.
- [x] Add grouped stats by field/SPW/antenna/observation.
- [x] Add synthetic exactness tests.
- [x] Add a slow CASA parity test for global amplitude stats.

## Test plan

- [x] `cargo test -p casa-calibration`
- [x] `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- [x] `cargo test -p casa-calibration --features slow-tests --test stats -- --nocapture`

## Interpretation

- The first `calstat` slice is intentionally library-first and machine-readable.
  It gives the calibration stack a real quantitative inspection hook without
  forcing a CLI or browser workflow decision yet.
- Global CASA parity is closed for the amplitude stats on a real generated
  `G` table, while the richer grouped report is currently a repo-specific
  extension beyond the CASA task return shape.
- The next leverage under `12.6` is still workflow-heavy rather than plumbing:
  `fluxscale`, `solnorm`, `combine=*`, or a decision about how to surface
  inspection results in `calibrate`.

## Results

- Date: 2026-04-04
- Commit:
- Commands:
  - `cargo test -p casa-calibration` -> PASS
  - `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings` -> PASS
  - `cargo test -p casa-calibration --features slow-tests --test stats -- --nocapture` -> PASS

## Follow-ups

- Decide whether to expose this stats surface through the `calibrate` CLI now or
  keep it library-only until broader inspection work lands.
- Add parity coverage for additional axes such as `phase` or real-valued
  columns if those become important.
- Keep `fluxscale`, `solnorm`, `combine=*`, and `BPOLY` as explicit follow-ons
  rather than code comments.
