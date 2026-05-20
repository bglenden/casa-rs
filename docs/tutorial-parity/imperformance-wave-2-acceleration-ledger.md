# ImPerformance Wave 2 Acceleration Ledger

Truth class: current descriptive
Last reality check: 2026-05-20
Verification: `bash -n scripts/bench-imager-vs-casa.sh`; `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/stage_wave1_datasets.py tools/perf/imager/test_run_workload.py tools/perf/imager/test_stage_wave1_datasets.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py tools/perf/imager/test_run_workload.py`; `cargo test -p casa-imaging paired_f64_product_grid_matches_separate_updates --lib`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks --lib`; `cargo test -p casa-imaging weighting --lib`; `cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casa-imaging degrid --lib`; `cargo test -p casa-imaging standard_mfs_thread_count_parser_accepts_numeric_and_auto_values --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager --example profile_imager`; `cargo build --release -p casars-imager --example profile_imager`; `just quick`; `just docs-check`; `git diff --check`; selected `tools/perf/imager/run_workload.py` and `profile_imager` runs listed below, including the positive compact tap paired profile on 2026-05-20

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
reserve accounts for thread-local density grids and the two local complex grids
needed by the combined PSF/dirty workers before assigning prepare-row buffers.
The core and planner now also accept `CASA_RS_STANDARD_MFS_GRID_THREADS=auto`,
which expands to the process available-parallelism count in both places so
worker staging and actual worker allocation remain aligned.

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
| weighting plus combined dirty-grid workers | `32,768` | `4` | `37.234 s` | `24.275 s` | `22.014 s` | `21.973 s` |
| bounded thread scaling | `32,768` | `8` | `36.280 s` | `21.451 s` | `18.526 s` | `18.477 s` |
| bounded thread scaling | `32,768` | `10` | `28.253 s` | `15.239 s` | `16.426 s` | `16.388 s` |
| bounded thread scaling | `32,768` | `auto` | `28.481 s` | `15.290 s` | `18.959 s` | `18.916 s` |
| positive-only standard gridder plan | `32,768` | `4` | `26.942 s` | `13.713 s` | `15.699 s` | `15.639 s` |
| compact product tap set | `32,768` | `4` | `27.166 s` | `13.674 s` | `19.083 s` | `19.017 s` |
| compact product tap set | planner-sized | `10` | `33.055 s` | `16.969 s` | `18.244 s` | `18.195 s` |

The one-buffer planner default reduced this diagnostic's prepare phase by
`14.356 s` (`35.7%`) and total frontend runtime by `14.616 s` (`13.3%`). The
streaming residual-grid worker prototype then cut the residual refresh from
`29.552 s` to `9.206 s`, reducing total frontend runtime from `95.506 s` to
`77.028 s` on the same bounded workload. It remains env-gated while the larger
full-medium run and deterministic product comparisons are still outstanding.
The latest clean-loop medians show that the remaining grid/degrid work is split
between the initial PSF/dirty pass and the now-threaded residual refresh. After
threading the owned Briggs density/reweighting path and the initial combined
PSF/dirty grid, the same bounded workload moved most remaining time back to
frontend row preparation. Raising the worker count above the first `4`-worker
checkpoint still helps, but not linearly; on the local 10-logical-CPU machine
the bounded `10`-worker run reduced `run_imaging` to `16.426 s`, while
`auto` landed at `18.959 s` in a later one-repeat run. The next local planner
change added a positive-only sample plan for standard-MFS streaming paths that
only ever use the positive UV tap products. That avoids computing and
flattening the conjugate negative-tap products in the initial PSF/dirty grid
and residual refresh loops. On the same bounded `4`-worker profile, it reduced
`run_imaging` from `22.014 s` to `15.699 s`; two broader experiments in the
same area, a per-worker grid merge helper and paired PSF/residual updates, were
discarded after regressing this profile. A standard-MFS row-block prepare
worker prototype using `CASA_RS_IMAGING_PREPARE_WORKERS=4` was also discarded:
it reduced no measured bottleneck on this dataset, moving prepare from about
`27.0 s` to `30.7 s` because smaller row blocks and parallel column reads cost
more than the row-adaptation overlap saved. The retained follow-up shrank
`ProductTapSet` to the flat index and weight arrays used by the hot contiguous
grids; fallback paths now derive row/column coordinates from the flat index
only when needed. Later one-repeat bounded runs were noisier than the first
positive-only profile, but the compact representation still kept the `4`-worker
core below the earlier `22.014 s` checkpoint. Raising the same run to `10`
grid workers improved core imaging to `18.244 s` but forced the centralized
planner to reserve more worker staging, reducing row-buffer size and moving
`prepare_plane_input` to `33.055 s`; for this bounded workload, `4` workers
remained the better end-to-end tradeoff.

| Core stage | Median |
|---|---:|
| `weighting` | `0.765 s` |
| `psf_grid` | `4.104 s` |
| `residual_degrid_grid` | `12.167 s` |
| `major_cycle_refresh` | `8.140 s` |
| `multiscale_scale_refresh` | `0.845 s` |
| `minor_cycle_solve` | `0.011 s` |

The full 2048-pixel, 512-channel, `niter=2` profile shows the same worker
changes carry to the full-medium shape:

| Full-shape diagnostic | Grid threads | Weighting | PSF grid | Residual grid total | Major refresh | Run imaging | Frontend total |
|---|---:|---:|---:|---:|---:|---:|---:|
| residual-grid workers only | `4` | `440.781 s` | `343.492 s` | `634.129 s` | `291.282 s` | `1546.453 s` | `1725.802 s` |
| weighting workers | `4` | `125.454 s` | `387.984 s` | `590.097 s` | `202.655 s` | `1238.520 s` | `1406.923 s` |
| weighting plus combined dirty-grid workers | `4` | `119.135 s` | `195.249 s` | `455.601 s` | `260.904 s` | `917.914 s` | `1088.750 s` |
| auto worker count | `auto` | `160.042 s` | `139.826 s` | `351.524 s` | `211.994 s` | `827.926 s` | `1004.912 s` |
| positive compact tap plan | `auto` | `104.285 s` | `91.790 s` | `250.545 s` | `159.118 s` | `589.248 s` | `758.891 s` |

The full-shape `auto` run was Rust-only, not paired with CASA, but it confirms
that increasing worker count still helps at the real shape. Compared with the
previous `4`-worker standalone profile, `run_imaging` improved by `89.988 s`
and frontend total improved by `83.838 s`. The benefit came from PSF and
residual grid/degrid traversal; weighting moved the other way in this
one-repeat run and still needs repeated measurement before changing the
recommended full-run setting. The later positive compact tap plan keeps the
same `auto` worker setting but removes unnecessary negative-tap planning from
streaming standard-MFS loops and shrinks each flattened product-tap plan to the
flat index and weight arrays used by contiguous grids. On the same full-shape
diagnostic it reduced `run_imaging` by `238.678 s` (`28.8%`) and frontend total
by `246.021 s` (`24.5%`) against the prior standalone `auto` row. The biggest
core movements were `weighting` (`160.042 s` to `104.285 s`), `psf_grid`
(`139.826 s` to `91.790 s`), and `residual_degrid_grid` (`351.524 s` to
`250.545 s`).

A paired Rust-vs-CASA run of the full 2048-pixel, 512-channel, `niter=2`
diagnostic then completed with product comparison:

| Workload | Result JSON | Rust median | CASA median | Ratio | Correctness |
|---|---|---:|---:|---:|---|
| `wave1-vla-single-medium-standard-mfs-clean-niter2` | `target/imperformance-wave2/threaded-clean-niter2-casa/20260519T190828Z-wave1-vla-single-medium-standard-mfs-clean-niter2-58852a06.json` | `1214.646 s` | `2138.483 s` | `0.57x` | GREEN: `.image`, `.residual`, `.psf`, and `.model` compared |
| `wave1-vla-single-medium-standard-mfs-clean-niter2` | `target/imperformance-wave2/positive-compact-clean-niter2-casa/20260520T030729Z-wave1-vla-single-medium-standard-mfs-clean-niter2-b693a634.json` | `719.611 s` | `2017.326 s` | `0.36x` | GREEN: `.image`, `.residual`, `.psf`, and `.model` compared |

The first paired clean comparison made the Wave 2 Rust path materially faster
than CASA on the same diagnostic, about `1.76x` faster by wall time. The later
positive compact tap plan moved the paired CLI timing to `719.611 s`, about
`2.80x` faster than CASA's `2017.326 s` direct `tclean` wall time. It is still a
shallow clean diagnostic: `niter=2` produced two Rust minor iterations and
should not be read as a science-depth deconvolution run.

The latest sampled product deltas were:

| Product | `diff_abs_max` | `diff_rms` | `diff_abs_max_over_casa_peak` | `diff_rms_over_casa_rms` |
|---|---:|---:|---:|---:|
| `.image` | `3.566742e-4` | `1.085070e-5` | `1.854747e-5` | `7.027464e-5` |
| `.residual` | `4.953146e-5` | `1.074190e-5` | `3.262572e-6` | `7.479129e-5` |
| `.psf` | `8.560910e-7` | `5.215558e-8` | `2.095782e-6` | `1.026938e-4` |
| `.model` | `3.576279e-7` | `1.172043e-9` | `2.996346e-7` | `2.321833e-7` |

The paired harness run reported slower Rust stage timings than the immediately
preceding standalone profile, so the conservative evidence to compare against
CASA is the paired wall-clock result above, not standalone frontend profiles.
In the latest paired run the Rust core remained CPU dominated:
`weighting=98.431 s`, `psf_grid=89.829 s`, `residual_degrid_grid=266.098 s`,
`major_cycle_refresh=176.525 s`, and `prepare_plane_input=138.915 s`. The CASA
phase probe, which is diagnostic and not the headline CASA wall-clock
comparison, was dominated by `make_psf=608.218 s`,
`calcres_major_cycle=669.309 s`, and `clean_major_cycle=785.696 s`, with
`set_weighting=60.419 s`. The probe itself took `2129.522 s`, confirming that
CASA phase diagnostics cost roughly another CASA imaging pass on this shape and
must be opt-in rather than part of routine paired timing.

The next Wave 2 optimization target remains the standard-MFS grid/degrid
traversal inside the full-shape clean path. Minor-cycle execution is still not
material at `niter=2`; the large buckets are CPU gridding/degridding, weighting,
and frontend row preparation.

## GPU Feasibility Checkpoint

The local profiler host reports an Apple M4 GPU with 10 cores and Metal
support. The repo does not currently carry a Metal, wgpu, CUDA, or other GPU
runtime dependency. The standard-MFS execution layer already has a reserved
backend marker that fails before execution for names such as `gpu`, and the
central memory planner already exposes a named GPU staging reserve through
`CASA_RS_IMAGING_GPU_STAGING_MB`.

The Wave 2 conclusion so far is that a GPU implementation should plug in behind
the existing standard-MFS backend/planner boundary, not inside task routing or
frontend row preparation. Adding a real Metal/wgpu dependency is a substantial
runtime/dependency decision and was not done silently in this CPU optimization
pass. The current retained CPU work keeps the GPU path prepared at the resource
planning seam while deferring the dependency choice for explicit review.

Earlier stopped clean attempts wrote failed result records only:

| Attempt | Result JSON | Status | Note |
|---|---|---|---|
| initial clean benchmark | `target/imperformance-wave2/full-medium-clean/20260519T055802Z-wave1-vla-single-medium-standard-mfs-clean-current-e78b0202.json` | failed | interrupted after a sample showed trace/env overhead plus full-batch clone pressure |
| trace-fixed clean benchmark | `target/imperformance-wave2/full-medium-clean-tracefix/20260519T061626Z-wave1-vla-single-medium-standard-mfs-clean-current-dbf36763.json` | failed | interrupted after a sample showed the remaining full-batch clone ownership bottleneck |
| owned-Briggs diagnostic clean benchmark | `target/imperformance-wave2/full-medium-clean-owned-briggs-diagnostics/20260519T144727Z-wave1-vla-single-medium-standard-mfs-clean-current-ab62a9d0.json` | failed | interrupted after memory stayed stable but the clean run remained in expensive full standard-MFS gridding work |
| expanded-stage clean benchmark | `target/imperformance-wave2/full-medium-clean-stage-diagnostics/20260519T153033Z-wave1-vla-single-medium-standard-mfs-clean-current-15dac89b.json` | failed | interrupted after the full-medium frontend completed, before Rust stage medians were available |

The completed `niter=2` clean diagnostic is performance-green against CASA, but
the full Wave 2 acceleration ladder remains open until the remaining CPU
grid/degrid work is reduced further, GPU feasibility is settled, and broader
clean validation is recorded.

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

Run the completed full-shape `niter=2` clean comparison:

```sh
CASA_RS_STANDARD_MFS_GRID_THREADS=auto \
CASA_RS_BENCH_MS_STAGING=direct \
CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
tools/perf/imager/run_workload.py \
  --repeats 1 \
  --run-label wave2-positive-compact-clean-niter2 \
  --storage-label external-ssd-wave2-medium-direct \
  --output-dir target/imperformance-wave2/positive-compact-clean-niter2-casa \
  target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-clean-niter2.json
```

Set `CASA_RS_BENCH_PHASE_PROBE=1` on that command only when CASA
`PySynthesisImager` phase medians are needed. The default paired run records
Rust/CASA wall times and product comparisons without paying for a second CASA
diagnostic pass.
