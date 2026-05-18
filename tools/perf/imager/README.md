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
- `tools/perf/imager/wave1_dataset_registry.json`
  - records the VLA/ALMA, single-field/mosaic, small/medium, and one large
    ALMA mosaic/cube simulated-dataset plan for #248
- `crates/casars-imager/examples/profile_imager.rs`
  - runs repeated Rust imaging passes and reports median stage timings from the
    pure `casa-imaging` core
- `scripts/bench-imager-vs-casa.sh`
  - compares Rust CLI wall-clock timings and Rust stage medians against CASA
    `tclean` on the same MeasurementSet selection, and can preserve final-run
    products for harness-level comparison

## Typical usage

```sh
scripts/bench-imager-vs-casa.sh
```

To run the Wave 1 manifest harness in validation mode:

```sh
tools/perf/imager/run_workload.py --dry-run wave1-standard-mfs-dirty-smoke
```

The command writes a JSON plan under `target/imperformance-wave1/` without
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

To compare native `simobserve` with CASA on a selected dataset:

```sh
python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-small \
  --disable-noise \
  --strict-values
```

The strict comparison samples matching rows by time, field, data description,
and baseline, then checks UVW, flags, weights, sigmas, and DATA. Its default
DATA tolerance is absolute `0.05 Jy` plus relative `5e-3`, which is tight
enough to catch model scaling/channel-order mistakes while avoiding false
failures from small CASA/native numerical differences in low-amplitude cells.

To check that the streamed MeasurementSet writer has not regressed, run a
native-only write-path benchmark on a fast local disk, not on
`/Volumes/GLENDENNING`:

```sh
cargo build --release --bin simobserve

python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-medium \
  --output-dir target/imperformance-wave1/internal-io-check \
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

Supported `imaging` values for the #252 harness slice:

- `mode`: `dirty` or `clean`
- `specmode`: `mfs` or `cube`
- `gridder`: `standard` or `mosaic`
- `interpolation`: `nearest` or `linear`
- `wterm`: `none`

Unsupported modes fail before timing claims are written. In particular,
W-projection and AW/widefield manifests should be rejected by this ticket until
their benchmark support is added or delegated to the owning follow-up.

## Result JSON

`run_workload.py` writes one JSON file per run with:

- `schema_version: 1`
- `run_id`, manifest path, git branch/commit, CASA Python path, benchmark script
  hash, and the exact delegated command/env
- dataset key/path, selected mode, image shape, channel count, weighting,
  deconvolver, `niter`, run label, storage label, and repeat count
- Rust CLI per-run wallclock and median wallclock
- CASA `tclean` per-run wallclock and median wallclock when CASA ran
- parsed Rust and CASA stage medians when present
- normalized `stage_breakdown` categories that distinguish frontend/MS
  preparation, visibility adaptation, weighting, gridding/degridding, FFT,
  normalization/PB correction, deconvolution, model refresh, and product
  writeback
- preserved product prefixes when a real run is executed
- CASA-backed product-comparison metrics for configured product suffixes
- a clear `dry_run`, `completed`, or `failed` status

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

The active Wave 8 clean cube gate can now be reproduced directly through the
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
