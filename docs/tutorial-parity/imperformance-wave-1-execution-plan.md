# ImPerformance Wave 1 Execution Plan Structure

Truth class: historical evidence snapshot
Last reality check: 2026-05-19
Verification: `cargo test -p casa-imaging standard_mfs`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks reserved_streaming_dirty_backend_fails_before_workspace_creation`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path`; `tools/perf/imager/run_workload.py --repeats 1 --run-label wave1-execution-plan-smoke --storage-label local-casatestdata --output-dir target/imperformance-wave1/issue250-execution-plan-smoke tools/perf/imager/workloads/wave1-standard-mfs-dirty-smoke.json`; `just docs-check`; `just quick`

Wave issue: #246
Child issue: #250

This note records the narrow execution-plan boundary added for the measured
Wave 1 optimization path. The first durable boundary is standard-gridder MFS
dirty imaging because #251 now has a full-shape correctness-green medium result
where the dominant measured owners are gridding/degridding and standard-MFS
preparation.

The implementation is intentionally internal. It does not add a public backend
selector, GPU support, worker scheduling, new dependencies, provider-contract
fields, or persisted-format changes.

## Boundary Shape

The selected path now has these internal layers:

| Layer | Type or owner | Responsibility |
|---|---|---|
| Backend selector | `StandardMfsBackend` | Chooses the internal standard-MFS backend. `Cpu` is the only implemented backend; reserved backend names fail before execution. |
| Borrowed batch plan | `StandardMfsVisibilityPlan` | Builds a per-row-block borrowed view of accepted visibility samples and precomputed standard-gridder taps. |
| CPU workspace | `StandardMfsWorkspace` | Owns reusable PSF and residual grids for the CPU backend. |
| Streaming dirty executor | `StandardMfsDirtyCpuExecutor` | Owns the standard gridder, workspace, and accumulated sample sums while consuming borrowed row-block plans. |
| Public compatibility facade | `StandardMfsDirtyAccumulator` | Preserves the frontend-facing dirty accumulator API used by `casars-imager` without exposing backend or workspace types. |

`casars-imager` still owns MeasurementSet selection, table column reads,
processing-buffer creation, and CASA image product writing. `casa-imaging`
owns the backend plan, grid workspace, accumulation math, FFT normalization,
beam fitting, and restored dirty-image construction.

## Measured Path

The #251 full medium standard-MFS dirty-control evidence is:

- dataset: `wave1-vla-single-medium`
- shape: MFS, standard gridder, dirty-only, `2048x2048`, 512 channels
- result JSON:
  `target/imperformance-wave1/issue251-medium-large-runs/20260519T012933Z-wave1-vla-single-medium-standard-mfs-dirty-control-cf33b8e5.json`
- wallclock: Rust `500.239 s`, CASA `503.295 s`, ratio `0.99x`
- correctness: sampled `.image`, `.residual`, and `.psf` products are green

The stage split that motivates this boundary is:

| Measured owner | Time |
|---|---:|
| gridding/degridding | `310184 ms` |
| `get_ms_values_into_processing_buffer` | `141913 ms` |
| `prepare_processing_buffer` | `61297 ms` |
| total standard-MFS preparation | `207834 ms` |

The new internal structure lets future work target gridding/backend structure
and MS preparation throughput independently. It also prevents full prepared
batch retention from being the structural coupling point for the dirty path.

The #250 execution-plan smoke run completed successfully at:
`target/imperformance-wave1/issue250-execution-plan-smoke/20260519T024055Z-wave1-standard-mfs-dirty-smoke-7b0260d9.json`.

## Non-Goals Preserved

- No public API or CLI behavior was changed for users.
- No provider protocol or managed-output schema was changed.
- No runtime model, worker-thread scheduling, async I/O, or CASA-like
  `parallel` / `chanchunks` controls were added.
- No GPU, CUDA, Kokkos, distributed execution, W-projection, or AW/widefield
  backend was implemented.
- Clean, cube, mosaic, and MT-MFS execution remain on their existing paths.

## Issue #250 Acceptance Mapping

- Equivalent products before and after refactor: the existing
  `standard_mfs_dirty_accumulator_matches_split_run_imaging` regression still
  compares split streaming accumulation against `run_imaging`.
- Explicit internal plan/execution boundary: `StandardMfsBackend`,
  `StandardMfsVisibilityPlan`, `StandardMfsWorkspace`, and
  `StandardMfsDirtyCpuExecutor`.
- CPU-only backend: `StandardMfsBackend::Cpu` is the only implemented backend.
- Borrowed or bucketized batch handling: each streaming row block builds a
  borrowed `StandardMfsVisibilityPlan`; accepted samples reference source
  batches by index rather than cloning a full-batch set.
- Unsupported future backend choices fail clearly:
  `reserved_streaming_dirty_backend_fails_before_workspace_creation`.
- Public contracts unchanged: the boundary is crate-internal except for the
  existing `StandardMfsDirtyAccumulator` facade added for the measured
  standard-MFS dirty path.
- Performance expectation: the boundary preserves the #251 streaming route and
  keeps the full medium standard-MFS dirty result as the comparison evidence.
