# ImPerformance Wave 1 Simulated Dataset Plan

Truth class: current descriptive
Last reality check: 2026-05-16
Verification: `python3 -m py_compile tools/perf/imager/stage_wave1_datasets.py tools/perf/imager/test_stage_wave1_datasets.py`; `python3 -m unittest tools/perf/imager/test_stage_wave1_datasets.py`; `tools/perf/imager/stage_wave1_datasets.py --dry-run --data-root /Volumes/GLENDENNING/casa-rs-imperformance --output-dir target/imperformance-wave1/dataset-dry-run`; `tools/perf/imager/stage_wave1_datasets.py --dry-run --data-root /tmp/casa-rs-imperformance --dataset wave1-vla-single-small --output-dir target/imperformance-wave1/dataset-small-dry-run`; `tools/perf/imager/stage_wave1_datasets.py --data-root /tmp/casa-rs-imperformance --dataset wave1-vla-single-small --materialize-models --materialize-workloads --output-dir target/imperformance-wave1/dataset-small-materialized`; `tools/perf/imager/stage_wave1_datasets.py --data-root /tmp/casa-rs-imperformance --dataset wave1-alma-shared-large --allow-non-external-large-root --materialize-workloads --output-dir target/imperformance-wave1/dataset-large-workloads`; `tools/perf/imager/run_workload.py --dry-run --output-dir target/imperformance-wave1/generated-workload-dry-run target/imperformance-wave1/dataset-small-materialized/workloads/wave1-vla-single-small-standard-mfs-dirty-control.json`; `just docs-check`; `just quick`

Wave issue: #246
Child issue: #248

This note defines the deterministic simulated data inputs for ImPerformance
Wave 1. The goal is to benchmark imaging behavior, not calibration behavior.
The datasets therefore use visually interesting deterministic sky models, a
realistic but simple noise term, and explicit CASA C++ generation plans.

For Wave 1, CASA C++ is allowed to be the dataset generator of record. Native
`casa-rs` simulation gaps discovered by this plan are tracked as backlog issues
instead of blocking benchmark dataset staging.

## Size Tiers

The first performance wave uses three memory-pressure tiers:

| Tier | Approximate staged MS size | Storage intent | Default policy |
|---|---:|---|---|
| small | `1 GiB` | much smaller than memory | may live under any explicit `CASA_RS_IMPERF_DATA_ROOT` |
| medium | `32 GiB` | about memory on this workstation | stage under `/Volumes/GLENDENNING` unless explicitly overridden |
| large | `100 GiB` total | larger than memory on this workstation | exactly one shared dataset, staged under `/Volumes/GLENDENNING` unless explicitly overridden |

The large tier is intentionally **not** one 100 GiB dataset per mode. The
external drive budget allows one large dataset, so the registry contains a
single shared ALMA mosaic/cube superset:

- dataset id: `wave1-alma-shared-large`;
- instrument/family: ALMA shared-large mosaic/cube superset;
- observing shape: 7 pointings, 256 channels, 24 h, 2 s integrations;
- logical workloads select from this one MS:
  - standard modes use field `0` with `gridder='standard'`;
  - mosaic modes use all fields with `gridder='mosaic'`;
  - bounded mosaic cube uses a 32-channel subset;
  - MFS modes use the available channel range through `specmode='mfs'`.

Generated MeasurementSets are not committed to git. The checked-in artifact is
the registry and staging tool:

- registry: `tools/perf/imager/wave1_dataset_registry.json`
- staging/preflight tool: `tools/perf/imager/stage_wave1_datasets.py`

The staging root is always explicit. Set `CASA_RS_IMPERF_DATA_ROOT` or pass
`--data-root`. The tool does not add a personal workstation fallback.

## Instruments And Families

The registry includes both VLA and ALMA simulated datasets. CASA C++ generation
is the current path for all Wave 1 benchmark datasets.

| Instrument | Single-field datasets | Mosaic datasets | Native `casa-rs` simulation status |
|---|---:|---:|---|
| VLA | small, medium | small, medium | VLA single-field single-plane is supported by the existing native `simobserve` path; native cube spectral structure and true mosaic generation are backlog |
| ALMA | small, medium | small, medium, plus one shared large superset | request-plan-only until ALMA simulation parity is checked |

The registry also includes both single-field and mosaic families:

| Family | Wave 1 modes | Status |
|---|---|---|
| single | standard MFS dirty, standard MFS clean, standard cube, MT-MFS sentinel | CASA C++ generated datasets are usable now; native spectral cube model prediction is backlog #255 |
| mosaic | mosaic MFS clean, mosaic cube bounded | CASA C++ generated datasets are usable now; native multi-field mosaic generation is backlog #254 |
| shared-large | all selected Wave 1 modes | one ALMA large-tier superset backs all logical large workloads |

The native blocked/request-plan statuses are intentional. They prevent the
benchmark program from claiming native simulation capability while allowing the
performance wave to proceed from CASA C++ generated datasets.

## Source Model

The continuum model is deterministic and deliberately nontrivial:

- bright compact core;
- faint offset compact sources;
- two extended spiral arms;
- partial ring;
- broad diffuse halo.

The cube profile is also deterministic and includes frequency structure:

- continuum baseline with spectral index;
- central broad emission line;
- offset narrow emission line;
- absorption notch against the core;
- weak velocity-gradient metadata for the extended arms.

The current native simulator reads a single FITS plane. The staging tool
therefore writes a continuum FITS model plus a spectral-profile JSON file.
CASA C++ dataset generation must apply the spectral profile for cube benchmark
datasets. Native use of the spectral profile is backlog #255.

## Simulation Parity And Performance Checks

Each staged dataset plan includes:

- a `casa-simulation-plan.json` describing the CASA C++ simulation work to run
  for the Wave 1 benchmark dataset;
- a `casars-simobserve.json` request for native capability comparison where
  supported;
- model/source provenance, shape, noise, storage, and selected-mode metadata.

The simulation comparison is part of #248/#251 evidence because generated data
becomes benchmark input. The comparison must record:

- CASA C++ simulation status: ran, skipped, or blocked with reason;
- `casa-rs` simulation status: ran, skipped, or blocked with reason;
- simulation wallclock for each side when it ran;
- row count, channel count, UVW/time/data sanity statistics;
- model/source checksums.

This is not a calibration benchmark. The noise term is simple deterministic
complex visibility noise, tuned to make images realistic enough for repeated
inspection without adding calibration-solver scope.

## Native Simulation Follow-Ups

Native simulation is not on the critical path for Wave 1 dataset generation.
The missing capabilities are tracked separately:

- #254: native multi-field mosaic simulation generation.
- #255: native spectral cube model simulation.
- #180: ALMA protoplanetary-disk simulation breadth.
- #181: simalma workflow parity.
- #182: ACA simulation parity.

## Commands

Dry-run the full registry using the external drive root:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --dry-run \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance \
  --output-dir target/imperformance-wave1/dataset-dry-run
```

Dry-run only the small VLA single-field dataset on a temporary root:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --dry-run \
  --data-root /tmp/casa-rs-imperformance \
  --dataset wave1-vla-single-small \
  --output-dir target/imperformance-wave1/dataset-small-dry-run
```

Materialize source models and generated benchmark workload manifests for one
dataset:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --data-root "$CASA_RS_IMPERF_DATA_ROOT" \
  --dataset wave1-vla-single-small \
  --materialize-models \
  --materialize-workloads
```

Medium and large tiers require a root under `/Volumes/GLENDENNING` by default:

```sh
CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
tools/perf/imager/stage_wave1_datasets.py --dry-run
```

Use `--allow-non-external-large-root` only for a deliberate one-off override.

Generate logical workload manifests for the single shared large dataset without
materializing the 100 GiB MeasurementSet:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --data-root "$CASA_RS_IMPERF_DATA_ROOT" \
  --dataset wave1-alma-shared-large \
  --materialize-workloads
```

## Issue #248 Acceptance Mapping

- Deterministic generation path or registry entry for selected mode/tier
  combinations: `wave1_dataset_registry.json` plus CASA C++ simulation plans;
  the large tier intentionally maps all selected modes to
  `wave1-alma-shared-large`.
- Metadata needed to reproduce benchmark workloads: generated dataset plan,
  source model files, spectral profile, and simulation request plans.
- Provenance, size, checksum, path, and tier policy: this document plus the
  generated `wave1-dataset-plan.json`.
- Clear preflight failures: missing `CASA_RS_IMPERF_DATA_ROOT`, medium/large
  root outside `/Volumes/GLENDENNING`, and native simulation gaps are explicit
  statuses with follow-up issues.
- Shared-data policy: explicit root only; no bulky generated data in git.
- Performance tier intent: small, memory-sized, and one shared
  larger-than-memory staged dataset.

## Stop Conditions Preserved

The current tool does not widen native simulation semantics. It records native
gaps while allowing CASA C++ generation for Wave 1 datasets. Implementing
multi-field native simulation, CASA `simalma` parity, ALMA/ACA native parity,
or a native cube-model predictor should be separate approved scope.
