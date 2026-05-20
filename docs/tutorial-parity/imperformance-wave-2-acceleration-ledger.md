# ImPerformance Wave 2 Acceleration Ledger

Truth class: current descriptive
Last reality check: 2026-05-20
Verification: `bash -n scripts/bench-imager-vs-casa.sh`; `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/stage_wave1_datasets.py tools/perf/imager/test_run_workload.py tools/perf/imager/test_stage_wave1_datasets.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py tools/perf/imager/test_run_workload.py`; `cargo test -p casa-imaging paired_f64_product_grid_matches_separate_updates --lib`; `cargo test -p casa-imaging streaming_dirty_executor_accumulates_borrowed_row_blocks --lib`; `cargo test -p casa-imaging weighting --lib`; `cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_briggs_weighting_matches_borrowed_weighting --lib`; `cargo test -p casa-imaging positive_tap_span_reconstructs_legacy_positive_taps --lib`; `cargo test -p casa-imaging compact_positive_tap_grid_and_degrid_match_product_taps --lib`; `cargo test -p casa-imaging fused_residual_refresh_matches_separate_degrid_grid --lib`; `cargo test -p casa-imaging standard_mfs_plan_buckets_gridder_accepted_samples --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=4 cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `CASA_RS_STANDARD_MFS_GRID_THREADS=auto cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casa-imaging degrid --lib`; `cargo test -p casa-imaging standard_mfs_thread_count_parser_accepts_numeric_and_auto_values --lib`; `cargo test -p casa-tables tiled_selected_row_reads_reuse_shared_tile_cache --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager --example profile_imager`; `cargo build --release -p casars-imager --example profile_imager`; `just quick`; `just docs-check`; `git diff --check`; selected `tools/perf/imager/run_workload.py` and `profile_imager` runs listed below, including the positive compact tap paired profile, bounded serial attribution probes, and final full-shape one-worker profiles on 2026-05-20

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

A 2026-05-20 Rust-only profiler check with
`CASA_RS_STANDARD_MFS_GRID_THREADS=1` on the same full-shape `niter=2`
diagnostic reported `frontend_total=1327.510 s`, `prepare_plane_input=126.145 s`,
`run_imaging=1201.220 s`, and `core_total=1198.218 s`. Against the paired CASA
wall-clock baseline of `2017.326 s`, that is still about `1.52x` faster than
CASA, but only because this was a Rust profile compared to the existing paired
CASA wall time rather than a fresh product-writing paired harness run. Compared
with the latest `auto` Rust profiler row from the paired run, the one-worker
profile was `1.81x` slower overall and `2.03x` slower in the imaging core. The
largest one-worker-to-auto regressions were `weighting` (`312.485 s` vs
`98.431 s`), `psf_grid` (`196.063 s` vs `89.829 s`),
`residual_degrid_grid` (`596.189 s` vs `266.098 s`), and
`major_cycle_refresh` (`400.458 s` vs `176.525 s`). This keeps serial
standard-MFS weighting and grid/degrid traversal in scope for Wave 2: worker
scaling wins the headline comparison, but it is masking substantial
single-thread cost.

The first serial-kernel pass precomputes normalized standard-gridder tap weights
per oversampling offset, precomputes density-cell scale constants, removes a
temporary density-position iterator from Briggs density construction, specializes
the owned no-trace Briggs reweight path, and updates complex grid cells by
their real/imaginary fields in the product-tap loop. On the same one-worker
full-shape diagnostic, that moved `frontend_total` to `1286.448 s` and
`core_total` to `1156.535 s`, a `41.063 s` (`3.1%`) frontend reduction and
`41.684 s` (`3.5%`) core reduction from the first one-worker profile. The
clearest movement was residual refresh/grid planning:
`residual_degrid_grid=560.749 s` and `major_cycle_refresh=362.298 s`, down from
`596.189 s` and `400.458 s`. This is useful but not sufficient; it confirms
that the remaining serial high nail is algorithmic residual refresh and
per-sample traversal, not only scalar-loop overhead.

Fresh call-tree samples on the bounded one-worker clean diagnostic changed the
next target. The first 2026-05-20 core sample was dominated by
`accumulate_streaming_standard_mfs_residual_grid_serial`, `flatten_tap_products`,
`sample_taps`, `degrid_sample_product_planned`, and the f64 product-grid update
helpers; FFT and minor-cycle solve were not material. Replacing streaming-path
product-tap flattening with direct positive-axis tap traversal kept the reusable
executor path unchanged, but removed the temporary 49-product tap set from the
large streaming path. On the same bounded 64-channel, 1024-pixel,
`niter=50`, `minor_cycle_length=50`, one-worker run, this moved
`core_total=31.373 s` to `23.094 s`, `psf_grid=6.595 s` to `4.171 s`, and
`residual_degrid_grid=21.526 s` to `15.790 s`. A post-change sample no longer
showed `flatten_tap_products` in the hot stack; the remaining stack was direct
tap traversal plus `sample_taps`.

The next serial pass attacked API overhead rather than adding more workers.
The shared tiled-storage cache now maps table paths to compact table IDs once
per table and uses those IDs in cache keys, so repeated tile lookups no longer
hash long table paths. The standard-MFS frontend row adapter also avoids
dynamic `ndarray` indexing in the hot DATA/FLAG accessors: selected channels
are converted to local channel offsets once, and array-backed row values use
cached shape/stride metadata for direct local indexing. On the same bounded
one-worker run this moved `prepare_processing_buffer` from the
`8.4-8.8 s` range to `5.3-5.5 s`, with the best retained frontend result at
`43.704 s`. The current sample no longer shows the earlier dynamic
`IxDyn`/`memmove` row-access stack as the dominant frontend cost.

The reusable executor path was brought onto the same compact positive-tap
representation as the streaming path so standard-MFS planning uses one exact
tap representation instead of retaining 49-product tap arrays in some paths.
The f64 grid/degrid slice fast paths now use unchecked indexing only after
`sample_taps` has proved the tap coordinates are inside the grid. The initial
dirty PSF/residual pair update also has a real-PSF plus complex-residual helper
so it no longer updates the PSF imaginary lane with zero. The best retained
bounded one-worker probe after those changes was:

```text
target/imperformance-wave2/calltree-probe/bounded-one-worker-real-complex-pair.log
frontend_total=43.704 s
prepare_plane_input=20.147 s
prepare_processing_buffer=5.288 s
core_total=23.476 s
psf_grid=4.122 s
residual_degrid_grid=16.219 s
major_cycle_refresh=12.149 s
major_cycles=2
minor_iterations=50
```

This is a valid serial improvement over the pre-direct-tap bounded baseline
(`frontend_total=57.788 s`, `core_total=31.373 s`) and over the first
post-direct-tap retained run (`frontend_total=49.524 s`,
`core_total=24.136 s`). It is still not the factor-of-two serial break loose:
the remaining high nail is exact visibility-domain residual refresh.

A deliberately tempting image-domain residual-refresh shortcut was tested and
rejected. It reduced the bounded run to about `34.996 s` and made
`major_cycle_refresh` nearly disappear, but the equivalence test
`trace_residual_refresh_matches_fft_residual_and_prediction_order` caught a
structured-model mismatch (`rms=3.6932018e-2`). That path is not part of Wave 2
unless a correct CASA-equivalent derivation is found.

The next Wave 2 optimization target remains the standard-MFS grid/degrid
traversal inside the full-shape clean path. Minor-cycle execution is still not
material at `niter=2`; the large buckets are CPU gridding/degridding, weighting,
and frontend row preparation.

To avoid chasing the wrong high nail, Wave 2 now keeps two clean benchmark
controls explicit. The generated `standard-mfs-clean-niter2` workload remains a
shallow major-cycle diagnostic with `minor_cycle_length=2`. The normal generated
clean workloads now set `minor_cycle_length=niter`, so medium-tier
`standard-mfs-clean-current` runs `niter=100` without forcing a residual refresh
after every two minor iterations.

A bounded 64-channel, 1024-pixel Rust-only check showed why this matters:

| Workload controls | Frontend | Run imaging | Major refresh | Residual grid | Minor solve | Major cycles | Minor iterations |
|---|---:|---:|---:|---:|---:|---:|---:|
| `niter=50`, `minor_cycle_length=2` | `125.223 s` | `96.820 s` | `75.085 s` | `75.336 s` | `0.265 s` | `26` | `50` |
| `niter=50`, `minor_cycle_length=50` | `34.815 s` | `8.063 s` | `2.963 s` | `4.392 s` | `0.263 s` | `2` | `50` |
| `niter=50`, `minor_cycle_length=50`, planner/reserve update | `33.933 s` | `8.123 s` | `3.031 s` | `4.446 s` | `0.261 s` | `2` | `50` |

The deeper-clean conclusion is that minor-cycle solving is still not a dominant
cost for this multiscale slice. With realistic cycle budgeting, bounded
deconvolution depth shifts attention back to frontend row preparation and the
same standard-MFS grid/degrid traversal; with `cycleniter=2`, the benchmark is a
deliberate stress test of repeated major-cycle refresh, not a representative
science-depth clean.

The planner/reserve update keeps imaging worker-local grid staging in the
central plan but no longer subtracts that future imaging-phase reserve from the
transient MS-read row-block buffer. On the bounded realistic profile this moved
the auto row block from `16,384` to the conservative cap of `32,768` rows and
moved `prepare_plane_input` from `27.038 s` in the progress-probed baseline to
`25.784 s` in the no-progress retained timing. The MFS row adapter now reserves
the row-block sample capacity once per block instead of issuing per-row vector
reservations.

## Serial Attribution Pass

The reprogrammed Wave 2 serial pass fixed the bounded workload at:

```text
64 channels, 1024 pixels, Briggs, multiscale, niter=50,
minor_cycle_length=50, CASA_RS_STANDARD_MFS_GRID_THREADS=1
```

Each row below is a one-repeat release-profile run unless marked as a census or
rejected candidate. Deltas are against the previous retained optimization step;
the cumulative column is against the fresh 2026-05-20 baseline.

| Step | Commit or state | Profile artifact | Frontend | Core | Prepare | PSF grid | Residual grid | Major refresh | Delta vs previous retained | Cumulative vs baseline | Correctness |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|---|
| fresh serial baseline | pre-step local state | `target/imperformance-wave2/serial-attribution/baseline-one-worker-20260520/bounded-one-worker-baseline.log` | `45.180 s` | `23.694 s` | `21.391 s` | `4.137 s` | `16.372 s` | `12.288 s` | baseline | baseline | existing green path |
| tap census counters | instrumentation-only local state | `target/imperformance-wave2/serial-attribution/tap-census-20260520/bounded-one-worker-tap-census.log` | timing invalid | timing invalid | timing invalid | timing invalid | timing invalid | timing invalid | not used for timing | not used for timing | counters only |
| compact positive tap spans | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/compact-span-one-worker-20260520/bounded-one-worker-compact-span.log` | `40.788 s` | `19.164 s` | `21.555 s` | `3.632 s` | `12.431 s` | `8.851 s` | frontend `-4.392 s`, core `-4.529 s` | frontend `-4.392 s`, core `-4.529 s` | GREEN: compact span tests plus required residual/Briggs tests |
| compact product table | rejected candidate, removed | `target/imperformance-wave2/serial-attribution/product-table-one-worker-20260520/bounded-one-worker-product-table-rerun.log` | `40.362 s` | `20.758 s` | `19.538 s` | `4.104 s` | `13.546 s` | `9.495 s` | core regressed `+1.594 s`; residual regressed `+1.115 s` | not retained | GREEN but rejected after two core regressions |
| fused exact residual refresh | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/fused-refresh-one-worker-20260520/bounded-one-worker-fused-refresh-rerun.log` | `38.292 s` | `18.662 s` | `19.559 s` | `3.502 s` | `12.028 s` | `8.578 s` | frontend `-2.495 s`, core `-0.502 s` | frontend `-6.888 s`, core `-5.032 s` | GREEN: fused helper test plus required residual/Briggs tests |
| compact prepared executor samples | retained in this Wave 2 commit | `target/imperformance-wave2/serial-attribution/compact-prepared-one-worker-20260520/bounded-one-worker-compact-prepared-rerun.log` | `38.113 s` | `18.496 s` | `19.549 s` | `3.497 s` | `11.861 s` | `8.416 s` | frontend `-0.179 s`, core `-0.166 s` | frontend `-7.067 s`, core `-5.198 s` | GREEN: compact plan test plus required residual/Briggs tests |

The exact tap census found `197,519,040` accepted samples,
`176,471,366` unique tap keys, and `21,047,674` duplicate samples. Adjacent
duplicate runs were not useful: p50, p90, p99, and max adjacent run size were
all `1`. The estimate also showed why compact spans were the right first
memory move: current planned-sample bytes were estimated at `39.504 GiB`,
versus `7.901 GiB` for the compact span representation.

Aggregation is therefore not enabled in this pass. Adjacent-run aggregation is
rejected by the max-run result of `1`; global sort/reduce aggregation is also
rejected for now because the duplicate rate is only about `10.7%` and would
add a large global sort or hash scratch structure to save a minority of tap
visits. If a later workload shows materially higher duplicate density, the
census gate can be rerun with `CASA_RS_STANDARD_MFS_TAP_CENSUS=exact`.

The compact product-weight table was also rejected. It used a named central
planner reserve while under test, but two bounded runs regressed the core path
relative to compact spans (`20.613 s` and `20.758 s` core versus `19.164 s`),
so the reserve and lookup table were removed. The retained centralized planner
change is the worker-staging reserve for already-existing standard-MFS worker
local grids.

The compact prepared-sample step is deliberately scoped to the reusable
standard-MFS executor. Once the executor has planned tap spans, it stores only
the weighted visibility, grid weight, and compact tap span. It no longer keeps
batch/sample indexes just to reach back into `u_lambda` and `v_lambda`, and the
Cotton-Schwab controller reuses that executor for exact residual refreshes
instead of rebuilding the tap plan from full batches on every major cycle.
The full frontend still carries `VisibilityBatch` values for warning metadata,
fallback streaming paths, W-projection, and trace-heavy modes; dropping those
buffers entirely is a later ownership step, not hidden in this serial pass.

The final full-shape one-worker gate used the generated
`standard-mfs-clean-niter2` shape: `512` channels, `2048` pixels, Briggs,
multiscale, `niter=2`, and `minor_cycle_length=2`. The first run was noisy in
unrelated weighting and PSF buckets, so it was repeated before accepting a
timing claim.

| Full-shape one-worker run | Artifact | Frontend | Core | Prepare | Weighting | PSF grid | Residual grid | Major refresh | Interpretation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| previous one-worker reference | earlier profiler note | `1327.510 s` | `1198.218 s` | `126.145 s` | `312.485 s` | `196.063 s` | `596.189 s` | `400.458 s` | pre-attribution reference |
| final run 1 | `target/imperformance-wave2/serial-attribution/final-full-shape-one-worker-20260520/full-shape-one-worker-final.log` | `1471.488 s` | `1378.809 s` | `89.164 s` | `402.099 s` | `250.722 s` | `611.908 s` | `361.518 s` | rejected as noisy/conflicting |
| final rerun | `target/imperformance-wave2/serial-attribution/final-full-shape-one-worker-20260520/full-shape-one-worker-final-rerun.log` | `1167.428 s` | `1075.047 s` | `88.518 s` | `292.866 s` | `177.577 s` | `508.367 s` | `331.164 s` | retained full-shape timing |

Against the previous one-worker full-shape reference, the retained final rerun
improved frontend total by `160.082 s` (`12.1%`), core total by `123.171 s`
(`10.3%`), residual grid/degrid by `87.822 s`, and major-cycle refresh by
`69.294 s`. This full-shape gate was Rust-only timing; product correctness for
the full-shape `niter=2` diagnostic remains covered by the earlier paired
Rust-vs-CASA comparison, while the new serial kernels are covered by the
targeted tests listed in the verification line.

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
