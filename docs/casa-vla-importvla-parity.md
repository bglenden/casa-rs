# CASA VLA `importvla` Parity

This note records the current command sequence for running the native Rust VLA
importer against CASA `importvla` on real export archives.

## Requirements

- A working CASA Python environment discoverable by `casa_test_support::discover_casa_python`
- Real VLA export archives available locally
- Enough free disk space for temporary MeasurementSets under `target/`

The real-data parity tests are intentionally `#[ignore]`d. Run them explicitly.

## Known-good commands

Run the fast local importer regressions first:

```bash
cargo test -p casa-vla --lib
```

Run a single archive through the shared CASA parity helper:

```bash
CASA_RS_IMPORTVLA_ARCHIVE=/Volumes/home/casatestdata/unittest/importvla/AS758_C030425.xp1 \
  cargo test -p casa-vla --test cpp_import_ms_parity \
  imported_measurement_set_matches_casa_task_when_configured -- --nocapture
```

Run the full known-real-archive parity matrix:

```bash
cargo test -p casa-vla --test cpp_import_ms_parity_matrix \
  known_real_archives_match_casa_when_available -- --ignored --nocapture
```

Run the broader real-data Rust-vs-CASA parity assertions:

```bash
cargo test -p casa-vla --test real_importvla_parity -- --ignored --nocapture
```

Inspect archive inventory and mode/revision coverage before choosing new matrix
cases:

```bash
CASA_RS_IMPORTVLA_ARCHIVES=/Users/brianglendenning/Desktop/AG189/observation.46182.7646759/AG189_1_46182.76468_46183.09488.exp,\
/Users/brianglendenning/Desktop/AG189/observation.46325.2302894/AG189_1_46325.23029_46325.80807.exp,\
/Users/brianglendenning/Desktop/AG189/observation.46673.4830671/AG189_1_46673.48307_46673.81374.exp \
  cargo test -p casa-vla --test real_archive_inventory \
  inventory_real_archives -- --ignored --nocapture
```

## Current matrix

The checked-in matrix currently targets these real archives when available:

- `AG189_1_46182.76468_46183.09488.exp` — revision 11 continuum, multi-band
- `AG189_1_46325.23029_46325.80807.exp` — revision 11 mixed spectral-line + continuum
- `AG189_1_46673.48307_46673.81374.exp` — revision 12 continuum
- `AS758_C030425.xp1` — revision 26 reference-pointing style case
- `AS758_C030426.xp5` — revision 26 tipping-curve style case

## Notes

- The parity helpers create temporary MeasurementSets under `target/` and clean
  them up automatically when the test process exits.
- `real_importvla_parity` checks both table rows and selected persisted metadata
  (`MEASINFO`, `QuantumUnits`, and `MS_VERSION`) against CASA output.
