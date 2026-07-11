# `casars-imager` Performance Profiling

This directory documents the performance harness for the MeasurementSet-backed
imager.

## Entry points

- `tools/perf/imager/run_workload.py`
  - runs one JSON workload manifest, preflights support, delegates supported
    workloads to `scripts/bench-imager-vs-casa.sh`, and writes a normalized
    machine-readable result JSON
- `tools/perf/imager/stage_wave1_datasets.py`
  - validates the ImPerformance Wave 1 simulated-dataset registry, enforces the
    explicit data-root policy, and can materialize deterministic source models,
    spectral profiles, simulation request plans, and generated workload
    manifests
- `tools/perf/imager/bench_simobserve.py`
  - compares native `simobserve` with CASA on selected datasets, records native
    timing reports, and can enforce native throughput floors for internal-disk
    storage-manager regression checks
- `tools/perf/imager/measure_progress_overhead.py`
  - runs the same `casars-imager --json-run` request with progress disabled and
    enabled, then reports median wall time, event count, and payload bytes for
    the live-progress `<1%` overhead check
- `tools/perf/imager/wave1_dataset_registry.json`
  - records the VLA/ALMA, single-field/mosaic, small/medium, and one large
    ALMA mosaic/cube simulated-dataset plan for #248
- `tools/perf/imager/wave3_single_plane_matrix.json`
  - records the all-single-plane mode matrix for #274, including smoke,
    medium, and stress rows plus the review evidence contract used by #273
- `tools/perf/imager/wave3_matrix.py`
  - validates and enumerates the Wave 3 matrix without requiring local datasets
- `tools/perf/imager/imaging_performance_ledger.json`
  - records accepted, guarded, neutral, and rejected imaging-performance runs,
    including exact workload/artifact handles, correctness gates, stage metrics,
    and speedup or slowdown fractions
- `tools/perf/imager/imaging_performance_ledger.py`
  - validates formulas, role semantics, checked-in evidence identities and
    SHA-256 digests, then summarizes the performance ledger
- `tools/perf/imager/evidence/`
  - retains compact final run/comparison JSON used by the ledger so CI can
    verify evidence without workstation-local benchmark paths
- `crates/casars-imager/examples/profile_imager.rs`
  - runs repeated Rust imaging passes and reports median stage timings from the
    pure `casa-imaging` core
- `scripts/bench-imager-vs-casa.sh`
  - compares Rust CLI wall-clock timings and Rust stage medians against CASA
    `tclean` on the same MeasurementSet selection, and can preserve final-run
    products for harness-level comparison

## Artifact policy

Generated benchmark data does not default to the repository `target/`
directory. When `/Volumes/GLENDENNING` is mounted, perf-imager tools write
large artifacts under:

```text
/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/
```

That root contains `README_SAFE_TO_DELETE.txt`; its contents are generated and
safe to remove when no benchmark is actively using them. Override the root with
`CASA_RS_IMPERF_ARTIFACT_ROOT` or `run_workload.py --artifact-root` when a run
needs a different external scratch area. Small JSON/log result files may still
be directed with `--output-dir`, but image products, comparison panels, and
benchmark temp copies default to the safe-to-delete external root.

## Typical usage

```sh
scripts/bench-imager-vs-casa.sh
```

To run the Wave 1 manifest harness in validation mode:

```sh
tools/perf/imager/run_workload.py --dry-run wave1-standard-mfs-dirty-smoke
```

The command writes a JSON plan under the external artifact root without
requiring CASA Python or a local MeasurementSet.

To validate the Wave 1 simulated-dataset plan:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --dry-run \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance
```

Medium and large datasets are expected to live on the external drive on this
system. The staging tool requires those tiers under `/Volumes/GLENDENNING`
unless `--allow-non-external-large-root` is passed explicitly. The large tier
is intentionally one `wave1-alma-mosaic-large` dataset; standard, cube, mosaic,
and sentinel large workloads are generated as logical selections from that one
staged MeasurementSet.

For Wave 1, native `simobserve` is the primary benchmark dataset generator.
CASA C++ generation remains the small-case oracle for selected parity and
performance checks.

To enumerate the Wave 3 all-single-plane matrix:

```sh
python3 tools/perf/imager/wave3_matrix.py --format text
```

The Wave 3 matrix intentionally includes modes that are not yet executable by
`scripts/bench-imager-vs-casa.sh`. `run_workload.py --dry-run` accepts those
manifests and marks them `dry_run_only` in `run_support`; real execution remains
limited to rows the delegated benchmark script supports. Every result carries a
`human_review` gate, and completed comparison runs write per-product review
panels with identical color limits for the casa-rs and CASA images plus a
separate difference panel.

The #276 standard-MFS smoke row is available as:

```sh
tools/perf/imager/run_workload.py --dry-run wave3-standard-mfs-single-term-smoke
```

The bounded large mosaic MT-MFS sentinel is available as:

```sh
tools/perf/imager/run_workload.py --dry-run \
  tools/perf/imager/workloads/wave3-mosaic-mtmfs-alma-large-bounded.json
```

## Imaging runtime controls and telemetry

`casars-imager` task protocol v3 carries the performance controls used by the
current workload harness:

- `parallel` selects normal planned local execution or the serial CPU
  comparison surface. `parallel=false` forces CPU acceleration, one grid and
  prepare worker, one live source block, RustFFT product transforms, and no
  Metal grouped-input cache.
- `chanchunks` is the CASA-like top-level spectral channel chunk count. The
  requested count establishes the minimum slab shape; it is not an exact worker
  cap or a switch for shared-source concurrency. For every cube plan, the
  planner derives active planes and workers from plane/channel geometry,
  hardware capacity, exact source-cache bytes, per-plane working state, and the
  memory target. It uses the ordinary route when all planes fit one slab. Any
  selected multi-slab shape can reuse a bounded resident source cache when the
  same formula proves that cache resident, independent of dataset identity or a
  particular `chanchunks` value.
- `imaging_memory_target_mb`, `imaging_prepare_buffer_mb`,
  `imaging_row_block_rows`, and `imaging_prepare_workers` control the shared
  source-stream plan.
- `imaging_read_ahead_blocks` is the maximum number of live row blocks, not
  queue capacity. It is currently capped at two. The two-block configuration
  accounts for one producer-owned block and one consumer-owned block and uses
  a zero-capacity rendezvous channel (`queue_capacity = max_live - 2`), so no
  third block can wait in the queue. One block is synchronous. Full-slab
  spectral modes default to one and reject the overlap plan when it would cost
  modeled plane residency or row locality. Consumer failure cancels the
  producer after any current bounded read, wakes a blocked rendezvous send, and
  preserves the original consumer error.
- `imaging_fft_precision` and `imaging_fft_backend` select dirty/PSF/residual
  product transform policy independently from visibility-grid acceleration.

The shared source read-ahead path is used by standard MFS, mosaic MFS replay,
the supported mosaic MT-MFS replay path, standard and mosaic cube slabs,
cubedata preparation, and trace preparation. Its summary line reports mode,
enabled state, max-live count, queue capacity, observed handoff high water,
row blocks and rows per block, producer/consumer blocked time, measured overlap,
source read/route/consumer time, source bytes, effective read bandwidth, and
streamed samples. Protocol-v3 diagnostic progress also reports planned and
tracked memory, worker/queue states, stage timings, GPU eligibility/selection,
host/device bytes, command/kernel time, and CPU fallback reasons. The task
protocol version is 3; its newline-delimited progress event schema is version 1
and the embedded observability snapshot schema is version 2.

## Apple GPU product finishing

On Apple platforms, eligible f32 standard and single-term mosaic dirty-product
batches can keep PSF/residual/weight grids resident through MPSGraph FFT, grid
correction, normalization, and peak reduction. Large mosaic MFS and MT-MFS also
accumulate directly into a Metal-shared FFT input; MT-MFS returns transformed
Taylor planes to the CPU for image correction and PB normalization. Output-owned
tiles are disjoint, so CPU workers can convolve exact-plan records in parallel without
atomics or full-grid worker replicas. MFS groups identical convolution plans;
MT-MFS additionally stores f64 PSF moments and Complex64 dirty moments per plan,
then forms the `2*nterms-1` PSF and `nterms` residual values before one projector
traversal. Only the bounded tile accumulator narrows to f32. No full host grid,
pack, or host-to-device grid copy is retained.

MT-MFS compaction and tile routing retain only one metadata group and bounded
raw-sample chunk at a time. The frontend scratch request is computed from image
cells, Taylor plane count, and planned workers, and is capped by the run-level
memory target after fixed products, caches, and one source row block. The core
derives the tile edge and route-copy bound from grid geometry and kernel support,
reduces worker count when required, and converts the remaining bytes into a raw
sample limit using the actual record layouts. Reusable standard-MFS tap plans
also use an exact record-byte reservation instead of a sample cutoff. These
formulas do not identify or tune for a dataset.

An explicit `metal-mpsgraph` request uses the resident path when supported.
`auto` selects it when the exact f32 recipe is Metal-shared and therefore avoids
an input-boundary materialization; host-resident and f64 products use the CPU
finisher independent of shape or batch. Unsupported shapes, unavailable devices,
and resident command failures also use the CPU finisher under `auto`. Standard
and mosaic MFS recover retained shared grids without rereading the source. If
`auto` must recover an MT-MFS Metal attempt, it replays the bounded stream to
rebuild equivalent host grids; the normal MT-MFS route remains direct
Metal-shared accumulation. Explicit Metal requests fail closed. Fallback
preserves the same product set and is emitted in detailed backend telemetry.

The performance ledger is authoritative for acceptance. In the current wave,
the explicit Metal standard dirty-product stage improved from `45.552 ms` to
`30.544 ms` (`1.49x`, `33.0%` lower), but the paired medium end-to-end run was
still `8.4%` slower than CPU. Auto therefore keeps host-resident grids on CPU
for every shape and batch; only Metal-shared recipes avoid the input boundary copy
and remain on Metal. This decision is derived from exact recipe bytes rather
than a workload-tuned image-size crossover.
A separate proposed large-mosaic command fusion was `8.7%` slower in the
product FFT stage and was rejected.

The retained path wins at the large tier. On the `107 GiB` GLENDENNING ALMA
mosaic dataset, the seven-field row selected `6,060,670` rows and `96,970,720`
channel samples into a `1280x1280` dirty product. Exact-plan grouping plus the
disjoint direct grid reduced single-term MFS wall from the prior `57.179 s` CPU
result to `23.241 s` (`2.46x`, `59.4%` lower). A matched current CPU run was
also `23.241 s`, so the retained Metal path removed the earlier slowdown rather
than trading total time for a faster FFT.

The final counterbalanced MFS run passes the zero-tolerance no-slowdown gate,
but it does not support a release speedup claim. Its median paired block was
`17.4%` lower, while the three block deltas ranged from `38.3%` lower to `48.9%`
higher with a `43.6` percentage-point IQR. The retained adjacent semantic pair
still shows the Metal PSF-product stage falling from `75.685 ms` to `26.616 ms`.
The comparison remains `investigate` for `.image`, `.image.pbcor`, and
`.residual` because the small f32 accumulation-order differences are spatially
coherent; `.pb`, `.psf`, `.sumwt`, and `.weight` are `good`.

For mosaic MT-MFS with `nterms=2`, the final counterbalanced estimate is a
`3.71x` speedup, or `73.1%` lower wall. Its three paired-block reductions were
`69.7%`, `73.1%`, and `78.8%`, with a `4.58` percentage-point IQR. An earlier
matched diagnostic showed the initial-dirty replay falling from `119.777 s` to
`22.935 s`.
Exact-plan compaction retained about `7.4%` to `7.8%` of accepted records and
tile routing duplicated those records only about `1.06x` to `1.12x`. Four
workers used at most `10 MiB` aggregate host tile scratch and the five-plane
Metal input occupied `75 MiB`. The same-run CASA wall was `508.642 s`.

Replacing the fixed scratch clamp, four-tile assumption, route-copy count, and
sample cutoffs with grid/support/term/worker/memory formulas produced a further
single-run reduction on the same large Metal workload: `30.708 s` to `19.154 s`
(`1.60x`, `37.6%` lower). In that historical run, the shape-derived planner
selected a 453-pixel tile edge, nine disjoint tiles, four workers, and a
`262,144,000`-byte scratch budget; those values describe the selected example,
not fixed MT-MFS policy. All seven products in the previous-versus-current
comparison are `good`, with maximum normalized RMS `9.13e-7`. This additional
pair is not counterbalanced; the earlier `3.71x` CPU-to-Metal result remains the
formal release claim.

The retained CPU-versus-Metal products are all `good`, correlate at effectively
`1.0`, and have maximum normalized RMS `8.53e-7`. The current `1280x1280` CASA
oracle is also green on primary products: normalized RMS is at most `3.78e-7`,
and Rust wall is `12.116 s` versus CASA `14.981 s`. Full-product review remains
`investigate` only for cancellation-scale PSF/residual TT1 and sumwt TT1; model
TT0/TT1 and sumwt TT0/TT2 are exact, and PB TT1 is zero in both implementations.
Kernel/stage wins are promoted only when a retained-path comparison also clears
end-to-end and product-equivalence gates.

Read-ahead is also mode- and workload-sensitive. The standard MFS medium
workload improved from `129.408 s` to `108.572 s` (`1.19x`, `16.1%` lower)
with `39.041 s` of measured producer/consumer overlap. The bounded large
mosaic MT-MFS sentinel improved from `119.203 s` to `111.821 s` (`1.066x`,
`6.19%` lower) with `4.384 s` of overlap. Both final pairs enforce an exact
two-live-block ceiling and compare read-ahead-disabled versus enabled products
as `good`; standard-MFS products are bit-identical.

Cube shared-source concurrency is not a `chanchunks=4` or dataset-specific fast
path. Every selected multi-slab shape is eligible when its geometry-derived
source ranges and exact cache/per-plane memory model fit the target. As
historical evidence, the medium seven-field mosaic cube workload's original
exact `chanchunks=4` worker cap regressed to `42.34 s` versus `27.29 s` for one
slab. The formula-derived planner allowed all eight planes at a modeled
`3.529 GiB` active set under the `16 GiB` target and bypassed the unnecessary
cache variant. Three counterbalanced blocks then measured that configuration at
`8.92%` lower paired wall (`1.098x`) with zero slowdown tolerance; all seven
products were bit-identical. Other geometries and memory targets may select a
different one-slab or bounded multi-slab shape, with guarded shared-source reuse
instead of repeated full-MS reads whenever the formula admits it.

To compare native `simobserve` with CASA on a selected dataset:

```sh
python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-small \
  --disable-noise \
  --strict-values
```

The strict comparison samples matching rows by time, field, data description,
and baseline, then checks UVW, flags, weights, sigmas, and DATA. Its default
DATA tolerance is absolute `0.05 Jy` plus relative `1e-2`, which is tight
enough to catch model scaling/channel-order mistakes while avoiding false
failures from small CASA/native numerical differences in low-amplitude cells.
When CASA Python is unavailable, the run records
`casa_oracle.status: "skipped"` and leaves the MeasurementSet oracle comparison
marked skipped instead of aborting before writing the benchmark JSON. Add
`--fixed-channel-workers N` when a single artifact should compare serial,
auto-worker, and fixed-worker native CPU runs.
When matched CASA and casa-rs image products have already been generated, add
`--casa-image-prefix PREFIX --native-image-prefix PREFIX` to compare products
such as `.image`, `.residual`, `.psf`, `.model`, `.sumwt`, and `.pb` in the same
oracle artifact.

To check that the streamed MeasurementSet writer has not regressed, run a
native-only write-path benchmark on a fast local disk, not on
`/Volumes/GLENDENNING`:

```sh
cargo build --release --bin simobserve

python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-medium \
  --output-dir /path/to/fast-local-disk/internal-io-check \
  --skip-casa \
  --skip-serial-check \
  --disable-prediction \
  --require-native-throughput-mb-s 700 \
  --require-data-io-throughput-mb-s 900
```

`--disable-prediction` removes model prediction and corruption so the run is
dominated by MeasurementSet creation and streamed tiled-column writes. On this
machine, the internal-disk medium write-only run measured about `955 MB/s`
end-to-end and the full medium run showed only `67 ms` of producer blocking on
the writer. The same external-drive write pattern measured far lower, so
internal-disk checks are the meaningful guard for storage-manager regressions;
external-drive runs remain useful for capacity and end-to-end staging checks.

To run the same workload for real:

```sh
CASA_RS_TESTDATA_ROOT=/path/to/casatestdata \
CASA_RS_CASA_PYTHON=/path/to/casa-python \
tools/perf/imager/run_workload.py wave1-standard-mfs-dirty-smoke
```

To force a different dataset:

```sh
scripts/bench-imager-vs-casa.sh /path/to.ms
```

The manifest runner intentionally resolves data only from an explicit manifest
path or from the manifest's `dataset.root_env` plus `dataset.relative_path`.
It does not add personal workstation data fallbacks.

## Environment variables

- `CASA_RS_TESTDATA_ROOT`
  - defaults to `/Volumes/home/casatestdata` when available
- `CASA_RS_CASA_PYTHON`
  - CASA-capable Python used for the `tclean` side of the comparison
- `BENCH_REPEATS`
  - number of repeated Rust/CASA wall-clock runs
- `IMAGER_BENCH_MODE`
  - `dirty` or `clean`
- `IMAGER_BENCH_SPECMODE`
  - `mfs` or `cube`
- `IMAGER_BENCH_GRIDDER`
  - `standard` or `mosaic`
- `IMAGER_BENCH_INTERPOLATION`
  - cube spectral interpolation mode: `nearest` or `linear`
- `IMAGER_BENCH_FIELD`
- `IMAGER_BENCH_SPW`
- `IMAGER_BENCH_CHANNEL_START`
- `IMAGER_BENCH_CHANNEL_COUNT`
- `IMAGER_BENCH_IMSIZE`
- `IMAGER_BENCH_CELL_ARCSEC`
- `IMAGER_BENCH_WEIGHTING`
  - `natural`, `uniform`, or `briggs`
- `IMAGER_BENCH_ROBUST`
  - Briggs robustness parameter passed to both Rust and CASA when weighting is `briggs`
- `IMAGER_BENCH_NITER`
- `IMAGER_BENCH_HOGBOM_ITERATION_MODE`
  - `strict` uses casa-rs' corrected Hogbom component accounting
  - `casa` reproduces CASA's documented inclusive Hogbom off-by-one behavior;
    use this for Rust-vs-CASA Hogbom product comparisons
- `IMAGER_BENCH_GAIN`
- `IMAGER_BENCH_THRESHOLD_JY`
- `IMAGER_BENCH_NSIGMA`
- `IMAGER_BENCH_PSFCUTOFF`
- `IMAGER_BENCH_MINOR_CYCLE_LENGTH`
- `IMAGER_BENCH_WTERM`
  - currently only `none` is supported in the Rust-vs-CASA benchmark script because the Rust-only `direct` mode has no matching `tclean` configuration in this harness
- `IMAGER_BENCH_MS_STAGING`
  - `copy` copies the MeasurementSet into the script temp directory before
    timing; this is the default for small workloads
  - `direct` benchmarks the manifest MeasurementSet path in place and is the
    required mode for about-memory or larger-than-memory datasets
- `IMAGER_BENCH_TMP_ROOT`
  - parent directory for script scratch space; defaults to `${TMPDIR:-/tmp}`

The manifest runner also honors `CASA_RS_BENCH_MS_STAGING=direct` and records
the resulting `run.ms_staging` value in the result JSON. Use this for medium
and large Wave 1 manifests so the benchmark does not first copy a 32 GiB or
100 GiB MeasurementSet into local `/var/folders` scratch.

## Manifest fields

Workload manifests live in `tools/perf/imager/workloads/`. The first Wave 1
manifest is `wave1-standard-mfs-dirty-smoke.json`.

Required top-level fields:

- `id`: stable workload id used in result filenames
- `mode_id`: selected Wave 1 mode id, such as `standard-mfs-dirty-control`
- `dataset`: `key`, plus either `path` or `root_env` and `relative_path`
- `imaging`: CASA-like mode parameters

Supported `imaging` values for executable #252-era benchmark rows:

- `mode`: `dirty` or `clean`
- `specmode`: `mfs` or `cube`
- `gridder`: `standard` or `mosaic`
- `interpolation`: `nearest` or `linear`
- `hogbom_iteration_mode`: `strict` or `casa`; Wave 3 Hogbom CASA-comparison
  rows use `casa`, while the imager application default remains `strict`
- `wterm`: `none`

Wave 3 dry runs additionally accept `specmode=cubedata`, `gridder=wproject`,
and the known AW/widefield aliases (`widefield`, `awproject`, `awp2`,
`awphpg`) so the full matrix can be reviewed before each mode ticket adds real
execution support. Those rows are marked `run_support.status=dry_run_only` and
fail before timing claims if requested as real runs.

## Result JSON

`run_workload.py` writes one JSON file per run with:

- `schema_version: 1`
- `run_id`, manifest path, git branch/commit, CASA Python path, benchmark script
  hash, and the exact delegated command/env
- dataset key/path, selected mode, image shape, channel count, weighting,
  deconvolver, Hogbom iteration mode, `niter`, run label, storage label, and
  repeat count
- Rust CLI per-run wallclock and median wallclock
- CASA `tclean` per-run wallclock and median wallclock when CASA ran
- parsed Rust and CASA stage medians when present
- normalized `stage_breakdown` categories that distinguish frontend/MS
  preparation, visibility adaptation, weighting, gridding/degridding, FFT,
  normalization/PB correction, deconvolution, model refresh, and product
  writeback
- preserved product prefixes when a real run is executed
- CASA-backed product-comparison metrics for configured product suffixes
- review panels for compared products when CASA Python has matplotlib
- `human_review`, which remains `pending` until Brian accepts the numeric
  table and panels for the mode ticket
- a clear `dry_run`, `completed`, or `failed` status

The workload result's `schema_version: 1` is the benchmark artifact schema. It
is independent of `casars-imager` task protocol v3 and the progress/
observability schema versions emitted by the application.

### Failure semantics

Unsupported modes, missing dataset roots or paths, missing CASA Python, invalid
CASA Python paths, and invalid repeat counts fail during preflight before the
benchmark script is invoked. Those failures exit without writing partial timing
claims.

If the delegated benchmark command exits non-zero, the result JSON is written
with top-level `status: failed`, the benchmark log path, the command exit code,
Rust timing status `not_run`, CASA timing status `blocked`, and the shared block
reason. Product comparison is skipped.

If a completed benchmark log omits one timing section, the corresponding side is
reported as `status: missing` with an explanatory reason instead of `ran`; only a
side with a median wallclock is reported as `ran`.

The historical Wave 8 clean cube gate can be reproduced directly through the
same harness by setting, for example:

```sh
BENCH_REPEATS=1 \
IMAGER_BENCH_MODE=clean \
IMAGER_BENCH_SPECMODE=cube \
IMAGER_BENCH_FIELD=0 \
IMAGER_BENCH_SPW=0 \
IMAGER_BENCH_CHANNEL_START=0 \
IMAGER_BENCH_CHANNEL_COUNT=20 \
IMAGER_BENCH_INTERPOLATION=linear \
IMAGER_BENCH_IMSIZE=100 \
IMAGER_BENCH_CELL_ARCSEC=8.0 \
IMAGER_BENCH_WEIGHTING=natural \
IMAGER_BENCH_DECONVOLVER=hogbom \
IMAGER_BENCH_NITER=1000000 \
IMAGER_BENCH_GAIN=0.5 \
IMAGER_BENCH_THRESHOLD_JY=0.000001 \
IMAGER_BENCH_NSIGMA=10 \
IMAGER_BENCH_PSFCUTOFF=0.35 \
IMAGER_BENCH_MINOR_CYCLE_LENGTH=10 \
IMAGER_BENCH_CYCLEFACTOR=1.0 \
IMAGER_BENCH_MIN_PSFFRACTION=0.1 \
IMAGER_BENCH_MAX_PSFFRACTION=0.8 \
scripts/bench-imager-vs-casa.sh /Volumes/home/casatestdata/measurementset/vla/refim_point_withline.ms
```

## Stage timing fields

The Rust profiler reports medians for:

- `open_measurement_set`
- `prepare_plane_input`
- `get_ms_values_into_processing_buffer`
- `prepare_processing_buffer`
- `extract_phase_center`
- `run_imaging`
- `build_coordinate_system`
- `write_products`
- `frontend_total`
- `controller_overhead`
- `weighting`
- `psf_grid`
- `psf_fft`
- `psf_normalize`
- `model_fft`
- `residual_degrid_grid`
- `residual_fft`
- `residual_normalize`
- `major_cycle_refresh`
- `minor_cycle`
- `minor_cycle_solve`
- `beam_fit`
- `restore`
- `total`

Detailed runs additionally emit `imaging_source_read_ahead_summary`,
`dirty_product_fft_timing`, `dirty_product_gpu_resident`,
`mosaic_dirty_product_gpu_resident`, and
`dirty_product_gpu_resident_fallback` records. Keep source overlap/bandwidth and
GPU fallback metrics with the wall-clock result when adding a ledger entry; do
not infer a speedup from stage timing alone.
