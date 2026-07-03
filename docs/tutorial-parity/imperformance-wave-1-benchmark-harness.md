# ImPerformance Wave 1 Benchmark Harness

Truth class: historical evidence snapshot
Last reality check: 2026-05-20
Verification: `bash -n scripts/bench-imager-vs-casa.sh`; `python3 -m py_compile tools/perf/imager/run_workload.py tools/perf/imager/test_run_workload.py`; `python3 -m unittest tools/perf/imager/test_run_workload.py`; `CASA_RS_BENCH_MS_STAGING=direct CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python tools/perf/imager/run_workload.py --dry-run --repeats 1 --run-label wave2-positive-compact-clean-niter2 --storage-label external-ssd-wave2-medium-direct --output-dir target/imperformance-wave2/positive-compact-clean-niter2-dry-run target/imperformance-wave2/medium-plan-current/workloads/wave1-vla-single-medium-standard-mfs-clean-niter2.json`; `just docs-check`

Wave issue: #246
Child issue: #252

This note records the reusable CASA C++ versus `casa-rs` imaging benchmark
harness for ImPerformance Wave 1. The harness is intentionally manifest driven:
the same workload JSON chooses the MeasurementSet, imaging mode, gridding mode,
weighting, deconvolver, image geometry, channel selection, repeat count, and
product comparison settings for both implementations.

## Entry Point

Run one workload manifest with:

```sh
tools/perf/imager/run_workload.py wave1-standard-mfs-dirty-smoke
```

The manifest can be a stable workload id under
`tools/perf/imager/workloads/` or a generated JSON path from
`tools/perf/imager/stage_wave1_datasets.py --materialize-workloads`.

For real CASA C++ comparisons, set:

```sh
CASA_RS_TESTDATA_ROOT=/path/to/casatestdata
CASA_RS_CASA_PYTHON=/path/to/casa-python
```

Wave 1 generated datasets should instead use `CASA_RS_IMPERF_DATA_ROOT` in the
generated manifest.

## Harness Contract

`run_workload.py` performs the reusable outer harness work:

- validates that the requested workload is inside the supported Wave 1 slice;
- resolves datasets only through an explicit manifest path or root environment
  variable plus relative path;
- records git branch/commit, CASA Python path, benchmark script hash, command
  argv, and delegated environment;
- delegates to `scripts/bench-imager-vs-casa.sh` for the Rust CLI, Rust core
  stage profiler, CASA `tclean`, and, when explicitly enabled, CASA
  `PySynthesisImager` stage probing;
- preserves the final Rust and CASA product prefixes under the run output
  directory;
- compares configured image products with CASA `casatools.image`;
- writes one machine-readable JSON result per run.

The delegated shell script remains the only place that knows how to invoke the
current `casars-imager` CLI and CASA `tclean` parameter sets. That keeps the
manifest runner stable while the lower-level commands evolve.

## MeasurementSet Staging

The benchmark script supports two MeasurementSet staging modes:

- `copy`: copy the input MS into the script temp directory before timing. This
  is appropriate only for small, intentionally disposable workloads.
- `direct`: read the manifest MS path in place. This is the default. It can
  also be set explicitly through `CASA_RS_BENCH_MS_STAGING=direct` when
  invoking `run_workload.py`, or through `run.ms_staging: "direct"` in a
  generated/ad hoc manifest.

Medium and large Wave 1 datasets must use `direct`. Copy staging consumed a
32 GiB local `/var/folders` temp copy for the VLA medium MS before timing and
left too little headroom for reliable benchmark execution. Use
`IMAGER_BENCH_MS_STAGING=copy` or `CASA_RS_BENCH_MS_STAGING=copy` only when the
run size and cleanup cost are known in advance.

CASA phase probing is similarly opt-in. Set
`CASA_RS_BENCH_PHASE_PROBE=1` for manifest-driven runs, or
`IMAGER_BENCH_PHASE_PROBE=1` when invoking `scripts/bench-imager-vs-casa.sh`
directly, only when a CASA stage breakdown is needed. The probe drives
`PySynthesisImager` directly and can cost about as much as another CASA imaging
run on medium and larger shapes; routine paired timing should use the direct
`tclean` wall time plus product comparison without the extra probe.

## Supported Slice

The #252 harness slice supports:

| Field | Supported values |
|---|---|
| `imaging.mode` | `dirty`, `clean` |
| `imaging.specmode` | `mfs`, `cube` |
| `imaging.gridder` | `standard`, `mosaic` |
| `imaging.interpolation` | `nearest`, `linear` |
| `imaging.wterm` | `none` |

Unsupported W-projection and AW/widefield workloads fail before timing claims
are written. Those modes remain deferred to their owning tickets.

## Result JSON

Each completed run records:

- run identity, manifest path, selected workload, dataset path, storage label,
  repeat count, MS staging mode, and mode parameters;
- exact delegated command and environment variables;
- Rust CLI per-run wallclock and median;
- CASA `tclean` per-run wallclock and median;
- Rust and CASA stage medians when available;
- normalized `stage_breakdown` categories for the Wave 1 bottleneck ledger;
- preserved product root, Rust prefix, and CASA prefix;
- product comparison metrics for configured suffixes.

The default product comparison suffixes are:

- `.image`
- `.residual`
- `.psf`

Manifest authors can override them with:

```json
{
  "comparison": {
    "products": [".image", ".residual", ".psf"],
    "max_elements_per_product": 1000000
  }
}
```

The product comparison reports shape, sampling stride, finite overlap count,
Rust/CASA min/max/RMS, absolute-difference maximum, RMS difference,
`diff_rms_over_casa_rms`, and `diff_abs_max_over_casa_peak`.

For large products, `max_elements_per_product` bounds comparison cost by using
CASA image chunk strides rather than forcing a full product read.

## Smoke Evidence

The local #252 smoke run used:

```sh
CASA_RS_TESTDATA_ROOT=/Users/brianglendenning/SoftwareProjects/casatestdata \
CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
tools/perf/imager/run_workload.py \
  --repeats 1 \
  --output-dir target/imperformance-wave1/harness-smoke \
  wave1-standard-mfs-dirty-smoke
```

Result:

- status: completed
- Rust CLI median: `0.882409 s`
- CASA `tclean` median: `0.137461 s`
- `.image` comparison: `diff_rms_over_casa_rms = 0.0412297`,
  `diff_abs_max_over_casa_peak = 0.00942697`
- `.residual` comparison: same as `.image` for dirty imaging
- `.psf` comparison: `diff_rms_over_casa_rms = 0.0258262`,
  `diff_abs_max_over_casa_peak = 0.00257105`

This is harness evidence, not a Wave 1 performance conclusion. The smoke
dataset is a small existing testdata MeasurementSet, not one of the staged
1 GiB / 32 GiB / 100 GiB simulated benchmark datasets.

## Issue #252 Acceptance Mapping

- Reusable manifest-driven runner: `tools/perf/imager/run_workload.py`.
- Same manifest drives CASA C++ and `casa-rs`: `run_workload.py` translates
  manifest fields into the delegated command environment used for both sides.
- Command, inputs, products, wallclock, exit status, and stage timings:
  recorded in each result JSON.
- Product deltas: CASA-backed comparison of preserved `.image`, `.residual`,
  and `.psf` products, with manifest-configurable product suffixes.
- Dry-run support: `--dry-run` validates support and writes the planned command
  without requiring CASA Python or a local MeasurementSet.
- Unsupported modes: fail before timing claims are written.

## Next Tickets

#249 should move stage timing from the current script-parsed probe into a more
complete structured stage/resource report. #251 should consume these result
JSON files to build the baseline matrix and bottleneck ledger for the selected
Wave 1 workloads.
