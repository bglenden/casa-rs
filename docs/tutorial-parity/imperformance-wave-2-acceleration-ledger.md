# ImPerformance Wave 2 Acceleration Ledger

Truth class: current descriptive
Last reality check: 2026-05-19
Verification: `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py tools/perf/imager/test_run_workload.py`; `cargo test -p casa-imaging paired_f64_product_grid_matches_separate_updates --lib`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks --lib`; `cargo test -p casa-imaging weighting --lib`; `cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager --example profile_imager`; `cargo build --release -p casars-imager --example profile_imager`; selected `tools/perf/imager/run_workload.py` and `profile_imager` runs listed below

Wave issue: #263
Child issues: #264, #265, #266, #267

This note records the first ImPerformance Wave 2 acceleration pass on the
correctness-green full-medium VLA standard-MFS target from Wave 1.

The measured dataset is:

```text
/Volumes/GLENDENNING/casa-rs-imperformance/wave1/vla/single/medium/ms/wave1-vla-single-medium.ms
```

Workloads were regenerated under:

```text
target/imperformance-wave2/medium-plan-current/workloads/
```

## Completed Dirty Target

The dirty target completed after two internal changes:

- the MeasurementSet frontend loads stable geometry columns once per standard
  MFS dirty run instead of reopening `ANTENNA1`, `ANTENNA2`, and optional
  pointing metadata for every row block;
- the streaming dirty standard-MFS accumulator now plans and grids samples
  directly into the reusable PSF and residual grids, avoiding the large
  per-block `StandardMfsPlannedSample` vector that dominated sampled CPU and
  memory traffic.

Result:

| Workload | Result JSON | Rust median | CASA median | Ratio | Correctness |
|---|---|---:|---:|---:|---|
| `wave1-vla-single-medium-standard-mfs-dirty-control` | `target/imperformance-wave2/after-direct-plan-dirty/20260519T052204Z-wave1-vla-single-medium-standard-mfs-dirty-control-bb5e3ae8.json` | `506.759 s` | `528.464 s` | `0.96x` | GREEN: `.image`, `.residual`, and `.psf` compared |

The sampled product deltas remained consistent with the Wave 1 full-medium
baseline:

| Product | `diff_abs_max` | `diff_rms` | `diff_abs_max_over_casa_peak` | `diff_rms_over_casa_rms` |
|---|---:|---:|---:|---:|
| `.image` | `1.960754e-3` | `2.062222e-5` | `2.857953e-5` | `2.847730e-5` |
| `.residual` | `1.960754e-3` | `2.062222e-5` | `2.857953e-5` | `2.847730e-5` |
| `.psf` | `4.678965e-5` | `6.608373e-7` | `7.825216e-5` | `5.617084e-4` |

## Stage Ledger

The completed direct-plan dirty run reported these Rust stage medians:

| Stage | Median |
|---|---:|
| `get_ms_values_into_processing_buffer` | `152845.705 ms` |
| `prepare_processing_buffer` | `62147.769 ms` |
| `prepare_plane_input` | `220444.918 ms` |
| `psf_grid` | `148295.170 ms` |
| `residual_degrid_grid` | `148295.170 ms` |
| `run_imaging` | `296849.504 ms` |
| `total` | `508066.368 ms` |

The direct-plan change was driven by a sample of the superseded paired-grid run:

```text
target/imperformance-wave2/after-paired-grid-dirty/casars-imager-77750.sample.txt
```

That sample showed the remaining Rust time in
`StandardMfsVisibilityPlan::new`, especially memmoves of planned sample
records. Direct planning removed that allocation path for the streaming dirty
accumulator.

## Clean Benchmark Status

The full-medium clean workload is now generated with `.model` in its comparison
set:

```text
target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-clean-current.json
```

The first clean run was intentionally stopped before accepting timing claims
because the sampled Rust process exposed a separate ownership bottleneck in the
non-streaming clean path:

```text
target/imperformance-wave2/full-medium-clean-tracefix/casars-imager-clean-tracefix-99251.sample.txt
```

The sample showed `apply_weighting_with_density_source` cloning full
`VisibilityBatch` values for Briggs weighting, with the process footprint at
about `81 GiB`.

The ownership fix now gives the frontend an owned standard-MFS entrypoint:
`run_imaging_owned()` moves prepared `VisibilityBatch` values into the imaging
core and applies Briggs/Uniform weighting by mutating only the weight vectors in
place after the shared density grid has been built. This preserves the borrowed
`run_imaging()` API for general callers while avoiding a full visibility-payload
clone in the high-volume frontend path. Standard MFS clean runs with no initial
model also start from the existing combined PSF+dirty-residual accumulator,
avoiding a separate full PSF gridding pass before the first minor cycle.

The frontend row-block sizing heuristic is now represented as a standard-MFS
memory plan. The same plan that sizes multi-row prepare buffers now records the
total budget, all named reserves, and the row-buffer budget before assigning
rows to prepare blocks. Known reserves include image working set and
Briggs/Uniform density grid. The planner also names future reserve classes for
gridded visibilities, output images, worker staging, and GPU staging so those
buffers have a central accounting point when later Wave 2 stages introduce
them. `CASA_RS_IMAGING_GPU_STAGING_MB` can reserve GPU staging memory without
changing row-buffer code. Because the standard-MFS prepare path is still
sequential, the planner now defaults to one prepare buffer instead of splitting
the budget across not-yet-scheduled worker buffers; `CASA_RS_IMAGING_PREPARE_WORKERS`
remains an override for explicit experiments. The experimental streaming
standard-MFS residual-grid workers are also represented in the same planner:
when `CASA_RS_STANDARD_MFS_GRID_THREADS` is greater than one, the worker staging
reserve accounts for thread-local residual grids before assigning prepare-row
buffers.

The Rust stage profile now separates clean-loop work beyond the previous
aggregate `major_cycle_refresh` bucket. New stage medians include
`clean_cycle_setup`, `deconvolver_setup`, `residual_refresh_overhead`, and
`multiscale_scale_refresh` alongside the existing `model_fft`,
`residual_degrid_grid`, `residual_fft`, `residual_normalize`, and
`minor_cycle_solve` fields. These fields are intended to identify whether the
next clean bottleneck is pure gridding/degridding, deconvolver setup,
controller overhead, or multiscale state rebuild before choosing CPU,
threading, or GPU work.

A diagnostic run after the owned-Briggs change stayed memory-stable instead of
repeating the `81 GiB` clone spike. It was interrupted before completion because
the remaining clean runtime is still dominated by full standard-MFS gridding
passes:

```text
target/imperformance-wave2/full-medium-clean-owned-briggs-diagnostics/20260519T144727Z-wave1-vla-single-medium-standard-mfs-clean-current-ab62a9d0.json
```

The captured log reached the end of frontend row preparation at
`169.595 s`, with the centralized memory plan reporting a `512.0 MiB` total
budget, `256.0 MiB` image reserve, `16.0 MiB` Briggs density reserve, and
`240.0 MiB` prepare-buffer budget. New runs will also report the named
gridded-visibility, output-image, worker-staging, and GPU-staging reserves.
The interrupted Rust child was around `7.5 GiB` RSS. This confirms the clone
fix but does not make the full clean workload performance-green.

A follow-up full-medium clean diagnostic with the expanded clean-loop stage
fields was interrupted before Rust medians were available:

```text
target/imperformance-wave2/full-medium-clean-stage-diagnostics/20260519T153033Z-wave1-vla-single-medium-standard-mfs-clean-current-15dac89b.json
```

Its log reached frontend row preparation at `176.500 s`; the memory plan
reported a `512.0 MiB` total budget, `272.0 MiB` reserved, and `240.0 MiB`
prepare-buffer budget for the 2048-pixel, 512-channel clean workload.

To make the next optimization decision without waiting for a full clean run,
the same algorithmic path was profiled on a bounded 64-channel, 1024-pixel,
`niter=2` slice. The diagnostic isolates setup and the first major-cycle
refresh while still exercising Briggs weighting, multiscale deconvolution, and
standard-MFS residual refresh.

| Diagnostic | Row block rows | Grid threads | Prepare plane input | Get MS values | Run imaging | Core total |
|---|---:|---:|---:|---:|---:|---:|
| pre-planner CPU fix | `8,192` | `1` | `40.164 s` | `27.394 s` | `69.837 s` | `69.781 s` |
| planner one-buffer default | `32,768` | `1` | `25.808 s` | `12.920 s` | `69.673 s` | `69.629 s` |
| streaming residual-grid worker prototype | `32,768` | `4` | `27.587 s` | `14.369 s` | `49.416 s` | `49.364 s` |

The one-buffer planner default reduced this diagnostic's prepare phase by
`14.356 s` (`35.7%`) and total frontend runtime by `14.616 s` (`13.3%`). The
streaming residual-grid worker prototype then cut the residual refresh from
`29.552 s` to `9.206 s`, reducing total frontend runtime from `95.506 s` to
`77.028 s` on the same bounded workload. It remains env-gated while the larger
full-medium run and deterministic product comparisons are still outstanding.
The latest clean-loop medians show that the remaining grid/degrid work is split
between the initial PSF/dirty pass and the now-threaded residual refresh:

| Core stage | Median |
|---|---:|
| `psf_grid` | `18.325 s` |
| `residual_degrid_grid` | `27.476 s` |
| `major_cycle_refresh` | `9.206 s` |
| `multiscale_scale_refresh` | `0.667 s` |
| `minor_cycle_solve` | `0.011 s` |

The next Wave 2 optimization target is therefore the remaining initial
standard-MFS PSF/dirty gridding pass and then full-medium validation of the
worker prototype, not minor-cycle execution.

The stopped clean attempts wrote failed result records only:

| Attempt | Result JSON | Status | Note |
|---|---|---|---|
| initial clean benchmark | `target/imperformance-wave2/full-medium-clean/20260519T055802Z-wave1-vla-single-medium-standard-mfs-clean-current-e78b0202.json` | failed | interrupted after a sample showed trace/env overhead plus full-batch clone pressure |
| trace-fixed clean benchmark | `target/imperformance-wave2/full-medium-clean-tracefix/20260519T061626Z-wave1-vla-single-medium-standard-mfs-clean-current-dbf36763.json` | failed | interrupted after a sample showed the remaining full-batch clone ownership bottleneck |
| owned-Briggs diagnostic clean benchmark | `target/imperformance-wave2/full-medium-clean-owned-briggs-diagnostics/20260519T144727Z-wave1-vla-single-medium-standard-mfs-clean-current-ab62a9d0.json` | failed | interrupted after memory stayed stable but the clean run remained in expensive full standard-MFS gridding work |
| expanded-stage clean benchmark | `target/imperformance-wave2/full-medium-clean-stage-diagnostics/20260519T153033Z-wave1-vla-single-medium-standard-mfs-clean-current-15dac89b.json` | failed | interrupted after the full-medium frontend completed, before Rust stage medians were available |

The clean benchmark should not be recorded as performance-green until the
remaining full-gridding clean-loop cost is reduced and a complete clean
comparison passes.

## Reproduction

Regenerate the Wave 2 medium manifests:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance \
  --dataset wave1-vla-single-medium \
  --materialize-workloads \
  --output-dir target/imperformance-wave2/medium-plan-current
```

Run the completed dirty target:

```sh
CASA_RS_BENCH_MS_STAGING=direct \
CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
tools/perf/imager/run_workload.py \
  --repeats 1 \
  --run-label wave2-after-direct-plan-dirty-direct \
  --storage-label external-ssd-wave2-medium-direct \
  --output-dir target/imperformance-wave2/after-direct-plan-dirty \
  target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-dirty-control.json
```
