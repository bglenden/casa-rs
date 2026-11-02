# `casars-imager` Performance Tools

These tools time casa-rs imaging against CASA `tclean` on MeasurementSets and
keep the evidence. What a performance claim needs is set by `TESTING.md`
("Imaging tiers and evidence") and `.agents/skills/casa-rs-imaging-performance`.
Their tests run with `scripts/test-perf-tools.sh`.

## Entry points

- `run_workload.py`: runs one JSON workload manifest (from `workloads/`, by id
  or path). It preflights each target, delegates ordinary paired workloads to
  `scripts/bench-imager-vs-casa.sh`, runs checked-in recipe-backed CASA oracles
  directly, and writes a result JSON.
- `run_alternating_comparison.py`: runs a baseline and a candidate workload,
  optionally from two casa-rs checkouts, in counterbalanced order and writes
  one comparison receipt (see [Two-build comparison](#two-build-comparison)).
- `scripts/bench-imager-vs-casa.sh`: compares Rust CLI wall times and stage
  medians with CASA `tclean` on the same MeasurementSet selection, and can keep
  the final run's products for comparison. It sets
  `CASA_RS_IMAGER_RUN_SUMMARY=1` so each run writes its
  `<imagename>.summary.json`.
- `stage_wave1_datasets.py` with `wave1_dataset_registry.json`: validates the
  simulated benchmark datasets, enforces the data-root policy, and writes
  source models, simulation requests and generated workload manifests.
- `stage_vlass_fragment.py`: verifies the frozen VLASS archive's SHA-256 and
  gzip stream, extracts only the expected MS and recipe, hashes the tree, and
  promotes it atomically into the evidence root.
- `bench_simobserve.py`: compares the native synthetic-MS generator with CASA
  `simobserve` on selected datasets and can enforce throughput floors.
- `wave3_matrix.py` with `wave3_single_plane_matrix.json`: validates and lists
  the single-plane mode matrix without local datasets.
- `imaging_performance_ledger.py` with `imaging_performance_ledger.json`:
  validates the ledger of accepted, guarded, neutral and rejected runs (formulas,
  roles, checked-in evidence identities and SHA-256 digests) and summarizes
  it. `evidence/` holds the compact run and comparison JSON the ledger cites,
  so CI can check it without workstation paths.

All of them use the `perf_harness` package for result schemas, subprocesses,
artifact writing, provenance, stage normalization and the MeasurementSet and
image comparators; the scripts only orchestrate. CASA-side programs are
checked-in Python files run with request and result JSON files; no command
generates a Python program at runtime.

## Artifact policy

Generated benchmark data does not default to the repository `target/`. When
`/Volumes/GLENDENNING` is mounted, large artifacts go under
`/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/`,
which is disposable scratch and never the only copy of anything needed later.
Override it with `CASA_RS_IMPERF_ARTIFACT_ROOT` or `--artifact-root`; small
JSON and log results can go elsewhere with `--output-dir`. Retained evidence
goes where `docs/agent-reference.md` puts it.

## Usage

Validate a manifest without CASA or a MeasurementSet (the plan is written under
the artifact root):

```sh
tools/perf/imager/run_workload.py --dry-run wave1-standard-mfs-dirty-smoke
```

Run it:

```sh
CASA_RS_TESTDATA_ROOT=/path/to/casatestdata \
CASA_RS_CASA_PYTHON=/path/to/casa-python \
tools/perf/imager/run_workload.py wave1-standard-mfs-dirty-smoke
```

The runner resolves data only from an explicit manifest path or from the
manifest's `dataset.root_env` plus `dataset.relative_path`; it has no
workstation fallbacks. `CASA_RS_BENCH_MS_STAGING=direct` reads the
MeasurementSet in place rather than copying it; medium and large datasets
need it. Matrix rows the bench script cannot run yet are accepted by
`--dry-run`, marked `dry_run_only`, and refused as real runs.

Validate the simulated-dataset plan (medium and large tiers must be under
`/Volumes/GLENDENNING` unless `--allow-non-external-large-root` is given):

```sh
tools/perf/imager/stage_wave1_datasets.py --dry-run \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance
```

List the single-plane mode matrix:

```sh
python3 tools/perf/imager/wave3_matrix.py --format text
```

Run the paired benchmark directly on another MeasurementSet:

```sh
scripts/bench-imager-vs-casa.sh /path/to.ms
```

The script's `IMAGER_BENCH_*` variables, read at its top, select the data,
mode and controls. Two need care:

- `IMAGER_BENCH_MS_STAGING`: `direct` (the default) benchmarks the
  MeasurementSet in place and is required for about-memory or larger data;
  `copy` gives Rust its own copy, for small correctness workloads whose
  model-column writes must not touch the source.
- `IMAGER_BENCH_HOGBOM_ITERATION_MODE`: `strict` (the default) uses casa-rs's
  Högbom component accounting; `casa` reproduces
  [CASA's off-by-one](../../../docs/CASA%20(C%2B%2B)%20bugs.md#casa-hogbom-niter-off-by-one-bug)
  and is the mode for Rust-versus-CASA Högbom product comparisons.

## Recipe-backed VLASS manifests

The `vlass-fragment-*` manifests run CASA from the archived VLASS recipe on the
staged fragment. Point them at the staged data; a real run also needs
explicit `--output-dir`, `--artifact-root` and `--cf-cache-root`:

```sh
export CASA_RS_VLASS_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-vlass/issue-446/data/<archive-sha256>
export CASA_RS_CASA_PYTHON=/path/to/casa/bin/python
tools/perf/imager/run_workload.py --dry-run vlass-fragment-single-field \
  --artifact-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/artifacts \
  --cf-cache-root /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache
```

- Outputs, artifacts, CF caches and the MeasurementSet (after resolving
  symlinks) must all be under `/Volumes/GLENDENNING/casa-rs-vlass/issue-446`,
  on the dataset's device, outside any `_tmp_safe_to_delete` directory.
- The manifests are immutable evidence: `--set-imaging` and a nonempty
  `run.env` are refused. A parameter experiment gets its own manifest.
- Each names one frozen dataset selection, and the run checks fields, SPWs,
  channels, correlations, UV range, intent and row count against it.
- Run a `-cold` manifest once to build the CF cache and its receipt, then the
  unsuffixed warm manifest to measure reuse. A warm run without its cold
  cache fails before CASA starts.
- A run writes everything under `<run-id>.partial` and renames it to
  `<run-id>` only after every required comparison passes its frozen
  tolerances; a failed or out-of-tolerance run keeps the partial tree.
- If CASA and the comparator finished but publishing the receipt failed,
  `run_workload.py --recover-receipt /absolute/path/to/run.json` revalidates
  and promotes the bundle without rerunning either.

The protocol's `wall_seconds` is the opaque `tclean()` call; these runs do not
split CASA's internal phases.

## Imaging runtime controls and telemetry

`casars-imager` takes the `tclean` catalog parameters, by name with
`--json-run` or as command-line flags (`docs/reference/task-parameters.md`).
The execution controls are `parallel`, `backend` (`auto`, `cpu` or `gpu`,
which is Metal on macOS) and `gridprecision` (`auto`, `f32` or `f64`); a timing
must name the backend and precision it ran with to be repeatable, and
`bench-imager-vs-casa.sh` always passes them.

With `CASA_RS_IMAGER_RUN_SUMMARY=1` in its environment (the benchmark scripts
set it; CASA's tclean writes nothing beside its products, so by default neither
does `casars-imager`), each run writes `<imagename>.summary.json` however it
ends: its status, the
memory capacity, workers, backend and resolved precision, the stop reason, the
minor-cycle totals and the products published, and for each phase its wall
time, resident high-water mark and `peak_reserved_bytes`.

Three diagnostic parameters log structured events at INFO (add
`--log-stderr-priority INFO` to see them on stderr):

- `stage_timing`: one `casa_imaging::stage_timing` event per phase. The bench
  script turns it on and reports the medians of each phase kind
  (`preparation`, `setup`, `major_cycles`, `minor_cycle_preparations`,
  `minor_cycles`, `model_prediction`, `products`, `total`) as the Rust stage
  medians.
- `metal_stage_timing`: per Metal gridding owner or prediction batch and wave,
  the host's preparation and wait times and the device's time; plus a
  `source stream` event per wave whose `consumer_wait_seconds` says whether
  reading the MeasurementSet is on the critical path.
- `science_trace`: the residual statistics, controls and record of each cycle.

On the CASA side, `CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES=1` makes the checked-in
`tclean` protocol log the duration of each `runMajorCycle` and its nested
`runMajorCycleCore` (`perf_harness/casa_major_timing.py`). Neither is a
gridding or kernel timer.

## Two-build comparison

`run_alternating_comparison.py` runs two workloads, or one twice, in
counterbalanced order: A B / B A warmup blocks, then A B B A / B A A B measured
blocks (`--warmup-pairs 2`, `--measured-pairs 3` by default), each a one-repeat
`run_workload.py` call. It writes one receipt, `<output-root>/<comparison-id>.json`,
beside the runs under `<output-root>/<comparison-id>/runs/`.

**Two builds.** `--baseline-repo` and `--candidate-repo` name the checkouts
each role runs; both default to this one. Each role runs its own checkout's
`run_workload.py` and `target/release/casars-imager`, so two checkouts need
their own `target/`: the harness refuses to start when `CARGO_TARGET_DIR` is
set or both resolve to one directory. Build both with the same FFTW
(`docs/agent-reference.md`) before alternating, and set
`IMAGER_BENCH_PREBUILT=1` so no Cargo check runs just before a timed run (it
perturbs the cache state). Remove the baseline worktree's `target/`
afterwards; a release build tree is tens of GB.

**Gates.** `--gate slowdown` (the default) passes when the median paired
relative wall delta is at most `--slowdown-tolerance` (default 0).
`--gate blocking` judges three things:

- *wall time*: the median paired relative delta must be at most
  `max(--wall-tolerance, --noise-sigmas × σ)`, defaults 0.02 and 2, where σ is
  the standard deviation of the baseline's measured walls over their median;
- *peak RSS*: the candidate's median must be at most `1 + --rss-tolerance`
  (default 0.05) times the baseline's, from the `wait4` high-water mark of each
  `casars-imager` process;
- *products*: the first measured run of each role keeps its products (later
  runs' are deleted once read), and `perf_harness.image_compare` compares every
  product in the baseline's `<imagename>.summary.json` in full mode; each NRMS,
  RMS(candidate − baseline) / RMS(baseline), must be at most `--product-nrms`
  (default 1e-6).

The comparison passes only when every verdict passes. A verdict without its
evidence (a failed run, a missing peak RSS, a comparator that did not run) is
inconclusive, and the command exits non-zero. The receipt records the
configuration and thresholds, each role's commit and dirty flag, the verdicts,
and for each run its summary, binary SHA-256 and FFTW codelet count.

**CASA and workers.** `run_workload.py` compares every run with CASA. Run the
workload once, then set `CASA_RS_BENCH_REUSE_CASA_PREFIX` to that run's CASA
prefix (`products/<run-id>/casa/casa`) so CASA does not rerun.
`CASA_RS_CASA_PYTHON` is still needed: the product comparator runs under CASA's
Python. Options after `--` go to every `run_workload.py` call;
`--set-imaging parallel=false` runs one worker and `parallel=true` the balanced
policy. Check `measured_summaries.<role>.workers`.

An A/A run of one build against itself records the baseline's noise band:

```sh
export CASA_RS_IMPERF_DATA_ROOT=/path/to/staged/casa-rs-imperformance
export CASA_RS_CASA_PYTHON=/Applications/CASA.app/Contents/MacOS/python3
export CASA_RS_BENCH_REUSE_CASA_PREFIX=/path/to/products/<run-id>/casa/casa
for parallel in false true; do
  tools/perf/imager/run_alternating_comparison.py \
    issue607-standard-mfs-dirty-representative \
    issue607-standard-mfs-dirty-representative \
    --gate blocking \
    --output-root ~/SoftwareProjects/casa-rs-evidence/imaging-port \
    -- --set-imaging parallel=$parallel
done
```

A baseline-versus-candidate run takes the baseline from a worktree:

```sh
git worktree add ../casa-rs-baseline <baseline-commit>
unset CARGO_TARGET_DIR
tools/perf/imager/run_alternating_comparison.py \
  issue607-standard-mfs-dirty-representative \
  issue607-standard-mfs-dirty-representative \
  --gate blocking \
  --baseline-repo ../casa-rs-baseline --candidate-repo . \
  --output-root ~/SoftwareProjects/casa-rs-evidence/imaging-port \
  -- --set-imaging parallel=true
```

## Manifests and results

Workload manifests (`schema_version: 1`) and the result envelope
(`schema_version: 3`) are defined and validated by `perf_harness/schema.py`;
unknown fields, wrong types and older result schemas are refused. A result's
`status` is one of `dry_run`, `completed`, `recovered_publication`,
`failed_execution`, `failed_comparison`, `out_of_tolerance` or `unavailable`,
and failed states carry a typed `results.failure` record. Preflight failures
(an unsupported mode, a missing dataset or CASA Python, an invalid repeat
count) exit before the benchmark runs and write no timing claims.
`migrate_evidence_v1_to_v2.py` and `migrate_evidence_v2_to_v3.py` are one-time
migrations of checked-in evidence, not runtime fallbacks.

## Synthetic-MS benchmarks

The benchmark datasets are generated by casa-ms's `synthetic-ms` developer
binary (`cargo build --release -p casa-ms --bin synthetic-ms`; it runs
`synthetic-ms --json-run request.json`, whose requests are documented in
`casa_ms::simulation_task`). `models/14pt-3gauss-v1.json` is the analytic
component model the datasets use.

Compare it with CASA on a dataset of the staged plan:

```sh
python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-small \
  --disable-noise \
  --strict-values
```

The strict comparison matches rows by time, field, data description and
baseline, then checks UVW, flags, weights, sigmas and DATA (absolute
`0.05 Jy` plus relative `1e-2` by default). Without CASA Python the run
records `casa_oracle.status: "skipped"` rather than failing.
`--fixed-channel-workers N` adds a fixed-worker run beside the serial and
automatic ones; `--casa-image-prefix` and `--native-image-prefix` also compare
already generated image products.

The MeasurementSet write path is guarded on a fast internal disk, not on
`/Volumes/GLENDENNING`, whose write speed is far lower:

```sh
cargo build --release -p casa-ms --bin synthetic-ms

python3 tools/perf/imager/bench_simobserve.py target/imperformance-wave1/plan/wave1-dataset-plan.json \
  --dataset wave1-vla-single-medium \
  --output-dir /path/to/fast-local-disk/internal-io-check \
  --skip-casa \
  --skip-serial-check \
  --disable-prediction \
  --require-native-throughput-mb-s 700 \
  --require-data-io-throughput-mb-s 900
```

`--disable-prediction` removes model prediction and corruption, so the run
measures MeasurementSet creation and the streamed tiled-column writes.
