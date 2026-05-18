# ImPerformance Wave 1 Stage Instrumentation

Truth class: current descriptive
Last reality check: 2026-05-15
Verification: `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/test_run_workload.py`; `python3 -m unittest tools/perf/imager/test_run_workload.py`; `tools/perf/imager/run_workload.py --dry-run --output-dir target/imperformance-wave1/stage-instrumentation-dry-run wave1-standard-mfs-dirty-smoke`; `CASA_RS_TESTDATA_ROOT=/Users/brianglendenning/SoftwareProjects/casatestdata CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python tools/perf/imager/run_workload.py --repeats 1 --output-dir target/imperformance-wave1/stage-instrumentation-smoke wave1-standard-mfs-dirty-smoke`

Wave issue: #246
Child issue: #249

This note records the Wave 1 stage-level timing surface used by the benchmark
harness. The implementation does not add public task-protocol fields or managed
output schema fields. It normalizes the existing Rust profiler and CASA phase
probe output into the local benchmark result JSON under
`results.stage_breakdown`.

## Contract Review

The structured timing object is local benchmark evidence:

- producer: `tools/perf/imager/run_workload.py`
- consumer: Wave 1 benchmark summaries and the #251 bottleneck ledger
- persisted public format: none
- provider protocol change: none
- managed-output contract change: none

If these fields are later promoted into `casars-imager` JSON task output,
managed output, or provider contracts, that will need a separate contract
review.

## Rust Categories

The normalized Rust timing categories are:

| Category | Source timing fields | Purpose |
|---|---|---|
| `frontend_ms_preparation` | `open_measurement_set`, `prepare_plane_input`, `extract_phase_center` | MS open, selection, row adaptation, and phase-center resolution. |
| `visibility_adaptation_and_chunking` | `prepare_plane_input` | Visibility adaptation before pure imaging. |
| `weighting_density_setup` | `weighting` | Imaging weights, density grids, and taper setup. |
| `projection_pb_cf_preparation` | none yet | Explicit non-zero-free placeholder; projection/PB setup currently lives inside lower-level selected-mode paths. |
| `gridding_degridding` | `psf_grid`, `residual_degrid_grid` | PSF gridding plus residual degrid/grid work. |
| `fft` | `psf_fft`, `model_fft`, `residual_fft` | PSF, model, and residual FFT work. |
| `normalization_pb_correction` | `psf_normalize`, `residual_normalize` | PSF and residual normalization; PB correction is included when the selected mode produces PB products. |
| `deconvolution_minor_cycle` | `minor_cycle_solve` | Minor-cycle component selection and subtraction. |
| `model_prediction_and_residual_refresh` | `major_cycle_refresh` | Major-cycle model prediction and residual refresh aggregate. |
| `restore_and_beam_fit` | `beam_fit`, `restore` | Restoring-beam fit and restored-image generation. |
| `coordinate_and_product_writeback` | `build_coordinate_system`, `write_products` | Output coordinate construction and image product writeback. |
| `preview_sidecar_generation` | none in harness | Explicitly skipped because benchmark runs pass `--no-preview-pngs`. |
| `frontend_total` | `frontend_total` | Total frontend wallclock from the Rust profiler. |
| `core_total` | `total` | Total pure imaging-core wallclock from the Rust profiler. |

Each category reports `status`, `reason`, `total_ms`, `components_ms`,
`source_fields`, and a short description. Clean-only categories are marked
`skipped` for dirty-only or `niter=0` workloads instead of publishing a
misleading zero-cost stage.

## CASA Categories

The CASA side is a comparison probe, not the source of the `casa-rs`
instrumentation contract. The normalized CASA categories are:

- `setup_and_tool_construction`
- `weighting_density_setup`
- `psf_and_primary_beam`
- `major_cycle_residual`
- `deconvolution_minor_cycle`
- `restore_and_cleanup`
- `total`

CASA cannot currently expose the same grid, FFT, normalization, and PB
sub-stages through this probe, so the Rust categories are the canonical #249
surface for choosing `casa-rs` optimization ownership.

## Smoke Evidence

The local one-repeat dirty-control smoke run completed with:

- Rust CLI median: `0.926705 s`
- CASA `tclean` median: `0.131690 s`
- `results.stage_breakdown.schema_version`: `1`
- Rust frontend/MS preparation: `38.485 ms`
- Rust visibility adaptation/chunking: `37.356 ms`
- Rust gridding/degridding: `0.600 ms`
- Rust FFT: `0.424 ms`
- Rust normalization/PB correction: `0.028 ms`
- Rust coordinate/product writeback: `6.280 ms`
- Rust deconvolution and model-prediction refresh: `skipped` for dirty-only
- Preview sidecars: `skipped` because previews are disabled in benchmark runs

This smoke workload is intentionally small and mostly measures harness shape.
The 1 GiB / 32 GiB / 100 GiB simulated datasets will provide the Wave 1
baseline matrix in #251.

## Overhead Boundary

The #249 implementation does not add timers to the hot imaging loops. It
reuses timings already produced by `profile_imager` and `casa_phase_bench.py`,
then performs a dictionary normalization pass in the Python harness after the
benchmark process completes. Instrumentation overhead on imaging execution is
therefore bounded to the existing profiler runs; the new category mapping is
outside the measured imaging commands.

## Issue #249 Acceptance Mapping

- Structured `casa-rs` timing data: `results.stage_breakdown.rust`.
- Frontend/MS preparation versus pure imaging core: separate frontend,
  visibility-adaptation, and core categories plus `frontend_total` /
  `core_total`.
- Grid/degrid, FFT, normalization/PB correction, deconvolution, and writeback:
  normalized Rust categories listed above.
- Disabled/skipped paths: dirty-only clean stages and preview generation are
  explicit `skipped` categories with reasons.
- Contract impact: local benchmark JSON only; no provider protocol or
  managed-output schema change.
- Overhead: no new hot-loop timers; normalization happens after each benchmark
  process completes.
