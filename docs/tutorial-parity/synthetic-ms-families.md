# Synthetic MeasurementSet Families

Truth class: implementation note
Last reality check: 2026-07-04
Verification: final 2026-07-04 wave gate: `just verify`; earlier wave gates: `just verify`; `just docs-check`; `PYTHONPATH=crates/casars-python/python pytest -q crates/casars-python/python/tests/test_simobserve.py`; `python3 tools/perf/imager/test_bench_simobserve.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py`; targeted `swift test --package-path apps/casars-mac --filter WorkbenchStoreTests/testSimobserveFamilyRequestSavesReopensEditsCanonicalJSON --filter WorkbenchStoreTests/testProcessGenericTaskRunsSimobserveFamilyThroughSavedJsonRun --filter WorkbenchStoreTests/testProcessGenericTaskSurfacesSimobserveFamilyValidationFailure`; 2026-07-04 read-path gate: `just quick`; targeted 2026-07-04 read-path checks: `cargo fmt --all -- --check`; `cargo test -p casa-tables lazy_disk_open_reads_required_scalar_columns_owned_without_materializing_rows --lib -- --nocapture`; `cargo test -p casa-ms visibility_buffer::tests::select_visibility_rows_filters_fields_ddids_and_tracks_time --lib -- --nocapture`; `cargo test -p casars-imager read_columnar_prepared_source_uses_visibility_buffer_and_geometry_rows --lib -- --nocapture`; `cargo test -p casars-imager production_visibility_column_reads_are_source_stream_centralized --lib -- --nocapture`; `cargo clippy -p casa-tables --lib -- -D warnings`; `cargo clippy -p casa-ms --lib -- -D warnings`; `cargo clippy -p casars-imager --lib -- -D warnings`

Native `simobserve` is the primary generator for synthetic MeasurementSets used
to exercise single-field, mosaic, MFS, continuum, spectral cube, cubedata, and
MT-MFS imaging diagnostics. CASA remains the oracle for selected small parity
cases.

## Model Inputs

Task protocol v2 keeps legacy `model_image` FITS requests and adds
`request.model`.

Analytic component file:

```json
{
  "schema_version": 1,
  "name": "14pt-3gauss-v1",
  "components": [
    {
      "kind": "point",
      "name": "core_0",
      "l_rad": 0.0,
      "m_rad": 0.0,
      "spectrum": {
        "flux_jy": 1.0,
        "spectral_index": -0.7
      }
    },
    {
      "kind": "gaussian",
      "name": "line_cloud_0",
      "l_rad": 0.00012,
      "m_rad": -0.00008,
      "major_fwhm_rad": 0.00004,
      "minor_fwhm_rad": 0.00002,
      "position_angle_rad": 0.6,
      "spectrum": {
        "flux_jy": 0.25,
        "spectral_index": -0.2,
        "line_peak_jy": 1.5,
        "line_center_fraction": 0.35,
        "line_sigma_fraction": 0.06
      }
    }
  ]
}
```

Run that analytic model:

```json
{
  "kind": "run",
  "request": {
    "model": {
      "kind": "analytic_components",
      "path": "models/14pt-3gauss-v1.json"
    },
    "output_ms": "out/spectral-cube.ms",
    "overwrite": true,
    "telescope_name": "VLA",
    "spectral_setup": {
      "name": "Qband",
      "start_frequency_hz": 44000000000.0,
      "channel_width_hz": 128000000.0,
      "channel_count": 64
    },
    "worker_policy": "auto",
    "row_workers": 8,
    "channel_workers": 8
  }
}
```

Run a FITS image or cube:

```json
{
  "kind": "run",
  "request": {
    "model": {
      "kind": "fits_image",
      "path": "models/sky-cube.fits",
      "model_peak_jy_per_pixel": 0.00003
    },
    "output_ms": "out/fits-cube.ms",
    "overwrite": true
  }
}
```

Single-plane FITS inputs are valid sampled-continuum diagnostics. FITS cubes are
authoritative sampled spectral sky models. Analytic component models provide
exact point-source and Gaussian visibility predictions plus per-channel spectra.

## Family Inputs

The dialog-persistent shape is `kind: "family"`. It records the parameters
needed to size and regenerate a family member for an imaging mode:

```json
{
  "kind": "family",
  "request": {
    "source_model": {
      "kind": "analytic_components",
      "path": "models/14pt-3gauss-v1.json"
    },
    "telescope": "VLA",
    "array_config": "A",
    "band": "Q",
    "target_ms_size_gib": 8.0,
    "polarizations": 4,
    "ms_channels": 64,
    "image_channels": 16,
    "pointing_count": 7,
    "imaging_mode": "mosaic",
    "output_ms": "out/vla-mosaic.ms",
    "worker_policy": "auto",
    "row_workers": 8,
    "channel_workers": 8
  }
}
```

Run persisted requests with:

```bash
simobserve --json-run request.json
```

Family execution now expands the saved inputs into a concrete `kind: "run"`
request, writes the MeasurementSet, and persists a sibling manifest named like
`out/vla-mosaic.synthetic-family.json`. The manifest records the source model,
mode, telescope/config/band, resolved antenna-coordinate source, requested
pointings, MS and image channels, polarization count, target and actual sizes,
worker settings, generated run request, and generated run result.

Supported mode labels are `single_field`, `mfs`, `continuum_mfs`, `mosaic`,
`mosaic_mfs`, `spectral_cube`, `cube`, `cubedata`, `mt_mfs`, `simalma`, and
`aca`. Mosaic-like modes generate deterministic multi-field pointings from the
shared source model; non-mosaic modes reuse the central pointing.

Supported array configuration labels are deliberately split between real CASA
configuration files and generated diagnostics:

- Real VLA configs: `A` uses the embedded CASA `vla.a.cfg` coordinates, while
  `B`, `C`, `D`, `vla.b.cfg`, `vla.c.cfg`, and `vla.d.cfg` require a readable
  CASA `.cfg` file. Set `CASA_RS_SIMOBSERVE_CONFIG_ROOT` to a directory
  containing CASA simmos config files, set `CASADATA`, set `CASAPATH`, or pass
  an explicit `.cfg` path.
- Real ALMA/ACA configs: labels such as `alma.cycle10.5.cfg` and
  `aca.cycle10.cfg` likewise require a readable CASA `.cfg` file or explicit
  path.
- Generated diagnostic layouts are explicit: `synthetic-vla-b`,
  `synthetic-vla-c`, `synthetic-vla-d`, `synthetic-alma-compact`,
  `synthetic-aca`, and `synthetic-simalma`.

Supported bands remain real receiver bands: VLA `L`, `S`, `C`, `X`, `Ku`, `K`,
`Ka`, and `Q`; ALMA/ACA `Band 3`, `Band 6`, `Band 7`, and `Band 9`.

`polarizations` maps to actual POLARIZATION metadata and MAIN `DATA`, `FLAG`,
`WEIGHT`, and `SIGMA` shapes. Supported values are `1`, `2`, and `4`; VLA
defaults to circular receptor metadata, while ALMA/ACA defaults to linear
metadata.

`ms_channels` controls the generated MS spectral-window channel count.
`image_channels` is persisted planning metadata for downstream imaging
diagnostics and manifests; it does not change the MS channel count by itself.

Python callers can use the same contract without a second schema:

```python
from casars.tasks import simobserve

request = {
    "source_model": {"kind": "analytic_components", "path": "models/14pt-3gauss-v1.json"},
    "telescope": "ALMA",
    "array_config": "synthetic-aca",
    "band": "Band 3",
    "target_ms_size_gib": 0.25,
    "polarizations": 4,
    "ms_channels": 32,
    "image_channels": 8,
    "pointing_count": 7,
    "imaging_mode": "mosaic",
    "output_ms": "out/aca-mosaic.ms",
}
simobserve.save_request("out/aca-mosaic.json", kind="family", request=request)
result = simobserve.run_file("out/aca-mosaic.json")
```

## Spectral Diagnostics

Analytic spectra are component-local, so channels do not need to be scaled copies
of one plane. Use a mix of:

- continuum components with different `spectral_index` values;
- narrow emission lines with small `line_sigma_fraction`;
- broad or extended line Gaussians with larger `line_sigma_fraction`;
- absorption with `absorption_peak_jy`, `absorption_center_fraction`, and
  `absorption_sigma_fraction`.

## Performance Targets

Correctness gates come before speed claims.

- v1 analytic-model floor: at least 500 MB/s end-to-end on medium datasets on
  fast local storage. This is not a stopping point; benchmark closeout should
  keep reporting and improving the gap to the streamed column write path until
  disk or table-write bandwidth is the limiting stage.
- Existing write-path guard remains at least 700 MB/s native output throughput
  and 900 MB/s streamed column write throughput for prediction-disabled
  internal-disk checks.
- CPU multi-worker prediction is the required baseline. GPU or Metal support is
  optional and should be added only after stage timing shows prediction remains
  the limiting stage after CPU and write-path iteration.
- `tools/perf/imager/bench_simobserve.py` reports native output MB/s, streamed
  MAIN-column MB/s, explicit CASA-relative timing, stage timing fractions,
  analytic small/medium tier slots, and serial/auto/fixed CPU worker
  comparisons. Treat `stage_timing.gpu_candidate` as a profiling signal, not
  proof that a GPU path is required.
- If CASA Python is unavailable or `--skip-casa` is set, the benchmark writes a
  JSON artifact with `casa_oracle.status: "skipped"` and an oracle comparison
  gap rather than failing before evidence can be inspected. The oracle helper
  compares MeasurementSet rows, UVW, FLAG/FLAG_ROW, WEIGHT, SIGMA, and DATA
  samples. When matched CASA and casa-rs products have already been generated,
  `--casa-image-prefix` and `--native-image-prefix` also compare selected image
  products such as `.image`, `.residual`, `.psf`, `.model`, `.sumwt`, and `.pb`.

Current local evidence on 2026-07-04:

- A 1 GiB analytic synthetic VLA-D-style/Q-band mosaic family run with 64 MS
  channels, 16 image channels, 7 pointings, and 4 correlations measured
  `1,253 MB/s` by reported simulator time, with about `2,899 MB/s` through the
  streamed MAIN-column write path. The 500 MB/s floor is met, and the
  run-based `IncrementalStMan` scalar override path reduced save overhead enough
  that the remaining gap to streamed write bandwidth is dominated by prediction,
  enqueue, and tiled DATA write bandwidth rather than row materialization.
  Manifest:
  `target/synthetic-ms-families/analytic-1g-vla-mosaic-current.synthetic-family.json`.
- A 100 GiB analytic synthetic VLA-D-style/Q-band mosaic family run with 64 MS
  channels, 16 image channels, 7 pointings, and 4 correlations on
  `/Volumes/GLENDENNING` generated `46,603,674` MAIN rows and `100,582,228,345`
  bytes in `131.183 s`: `767 MB/s` end-to-end and `1088 MB/s` through the
  streamed MAIN-column write path. This reruns the high-row-count 64-channel
  case that previously died after producing an invalid partial 84 GiB MS; the
  generated-scalar MAIN path leaves `main_row_add_millis=0` and
  `scalar_column_millis=17`, and run-based ISM scalar generation lowered
  `save_millis` from `31.937 s` to `16.758 s`. Manifest:
  `/Volumes/GLENDENNING/casa-rs-benchmarks/synthetic-ms-families/analytic-100g-vla-mosaic.synthetic-family.json`.
- The same 100 GiB MS is readable through the bounded visibility path. After
  fixing `ms-read-probe` setup to avoid single-cell full-column materialization,
  a 1,000,000-row visibility read (`DATA`, `FLAG`, `WEIGHT`, `UVW`, all 64
  channels, 8,192-row blocks) completed in `1.465 s`, reporting `1.49 GiB/s`
  logical/physical read throughput and about `161 MB` peak RSS. The actual
  imager read probe over `FIELD_ID=0` reads `5,293,848` active rows through the
  bounded path. Before read-path cleanup this measured `33.48 s` total /
  `26.79 s` read time (`362 MiB/s` total, `452 MiB/s` read). After carrying
  `ANTENNA1/2` in selected-row metadata, parallelizing required scalar loads
  across independent data managers, and honoring `--imaging-row-block-rows`,
  the best row-shaped 2026-07-04 local probe used `393,216` rows/block and
  measured `28.55 s` total / `21.12 s` read time (`424 MiB/s` total,
  `574 MiB/s` read) while still adapting every row back into `Array2`
  payloads. The current reusable columnar-buffer probe avoids that legacy row
  adaptation, reads `11.56 GiB` of logical buffer payload with `1,048,576`
  rows/block, and measures `28.28 s` total / `21.01 s` read time
  (`419 MiB/s` total, `563 MiB/s` read). The 2,097,152-row block variant was
  slightly slower (`413 MiB/s` total, `552 MiB/s` read). A selected-row-only
  scalar loading experiment was rejected because it increased `select_main_rows`
  to about `20.7 s`; the retained path keeps full scalar-column selection and
  the remaining total-time gap is dominated by selected-row visibility payload
  reads plus about `7-10 s` of scalar selection/setup.
- Pre-review selected-row I/O refactor kept the typed packed selected-channel
  path as the only channel-range table API, removed the generic array-slicing
  fallback path, and moved imager row adaptation onto `VisibilityBuffer` layout
  helpers. A release smoke probe on the same 100 GiB
  MS read `1,000,000` rows with `8,192`-row blocks and explicit
  `DATA,FLAG,WEIGHT,UVW` columns at `2.796 GiB/s` logical/physical throughput
  with about `155 MB` peak RSS. The release imaging-sidecar variant with
  `65,536`-row blocks measured `0.754 GiB/s`; scalar sidecar loading dominated
  that run rather than the channelized typed selected-read path.
- A prior 100 GiB analytic synthetic VLA-D-style/Q-band mosaic family run with
  1024 MS channels on `/Volumes/GLENDENNING` generated `2,912,949` MAIN rows
  and `99,650,093,245` bytes in `123.561 s`: `806 MB/s` end-to-end and
  `912 MB/s` through the streamed MAIN-column write path. Manifest:
  `/Volumes/GLENDENNING/casa-rs-benchmarks/synthetic-ms-families/analytic-100g-vla-mosaic-1024ch.synthetic-family.json`.
- A prediction-disabled write-path run using the same concrete run request
  measured `1186 MB/s` by wall time and `1243 MB/s` by reported simulator time,
  with `4433 MB/s` through the streamed MAIN-column write path. Report:
  `target/synthetic-ms-families/analytic-1g-vla-mosaic-writeonly.report.json`.
- The final simalma CASA breadth artifact
  `target/imperformance-artifacts/simulation-breadth/aca-simalma/20260703T230505Z-simalma/aca-simalma-benchmark.json`
  passed its closeout gate. CASA generated `111.6 MB` in `27.3 s`
  (`4.1 MB/s`). Native generated the 12m, 7m, and two TP MSs at `9.1`,
  `8.2`, `4.5`, and `4.5 MB/s`; rows, FIELD centers, flags, weights, sigmas,
  and sampled DATA all passed against CASA. The direct multi-MS native imager
  also wrote the combined mosaic MFS product set.
- The final ACA CASA breadth artifact
  `target/imperformance-artifacts/simulation-breadth/aca-simalma/20260703T230145Z-aca/aca-simalma-benchmark.json`
  passed its closeout gate. CASA generated `109.4 MB` in `23.7 s`
  (`4.6 MB/s`). Native generated the 12m, 7m, and TP MSs at `21.3`, `9.1`,
  and `19.7 MB/s`; rows, FIELD centers, UVW, flags, weights, sigmas, and
  sampled DATA all passed against CASA. The native imager wrote 12m and 7m MFS
  product sets, plus a TP sampled-product diagnostic.
