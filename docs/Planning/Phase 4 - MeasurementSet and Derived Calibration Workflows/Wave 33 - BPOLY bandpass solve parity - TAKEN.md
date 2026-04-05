## Wave 33 - `BPOLY` Bandpass Solve Parity

### Goal

Extract the legacy `BPOLY` bandpass family from backlog item `12.7` into the
implemented calibration surface by fitting and writing CASA-readable `BPOLY`
tables from the existing narrow bandpass solve path.

### Scope

- Extend the limited `bandpass` solver to emit either:
  - channelized `B Jones` tables, or
  - legacy polynomial `BPOLY` tables
- Keep the solve scope narrow:
  - `solint='inf'`
  - explicit refant
  - prior gain-table preapply
  - point-source `smodel=[I,0,0,0]`
- Preserve the same acceptance contract as the rest of the crate:
  - synthetic downstream correction through Rust apply
  - real-MS downstream parity against CASA on `ngc5921.ms`

### Implemented

- `BandpassSolveRequest` now carries:
  - `band_type`
  - `amplitude_degree`
  - `phase_degree`
- `solve_bandpass_from_path()` supports `band_type='BPOLY'`
- The new BPOLY path:
  - reuses the existing solved per-channel bandpass rows
  - fits legacy Chebyshev amplitude/phase coefficients per antenna/receptor
  - writes a legacy `BPOLY` main table plus `CAL_DESC` / `CAL_HISTORY`
  - includes the legacy solvable-cal columns CASA expects when applying the
    table back through `applycal`
- The public CLI `solve-bandpass` surface now accepts:
  - `--bandtype b|bpoly`
  - `--degamp`
  - `--degphase`

### Validation

- `cargo test -p casa-calibration`
- `cargo clippy -p casa-calibration --all-targets --features slow-tests -- -D warnings`
- `cargo test -p casa-calibration --features slow-tests --test casa_calibration_parity -- --nocapture`

### Notes

- This first BPOLY slice closes two different contracts:
  - on-disk interop: CASA `applycal` accepts the Rust-written `BPOLY` table
  - downstream behavior: corrected data close against CASA on `ngc5921.ms`
- The downstream parity tolerance for `BPOLY` is intentionally wider than the
  channelized `B`-table path. The on-disk compatibility is exact enough for
  CASA reuse; the remaining gap is solver-fit fidelity rather than table shape.
