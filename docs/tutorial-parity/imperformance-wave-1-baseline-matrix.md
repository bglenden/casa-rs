# ImPerformance Wave 1 Baseline Matrix

Truth class: current descriptive
Last reality check: 2026-05-18
Verification: `python3 -m unittest tools/perf/imager/test_run_workload.py tools/perf/imager/test_stage_wave1_datasets.py`; `bash -n scripts/bench-imager-vs-casa.sh`; `tools/perf/imager/stage_wave1_datasets.py --data-root /Volumes/GLENDENNING/casa-rs-imperformance --materialize-workloads --output-dir target/imperformance-wave1/issue251-plan`; selected `tools/perf/imager/run_workload.py` runs listed below; `just docs-check`; `just quick`

Wave issue: #246
Child issue: #251

This note records the first `casa-rs` versus CASA C++ baseline matrix for
ImPerformance Wave 1. The benchmark runs are manifest-driven through
`tools/perf/imager/run_workload.py` and use the deterministic Wave 1
MeasurementSets staged under:

```text
/Volumes/GLENDENNING/casa-rs-imperformance
```

The matrix below is a first small-tier baseline. The medium and large staged
datasets are present on the external volume and are listed in this note, but
they were not benchmarked in this pass because the 32 GiB and 100 GiB runs are
capacity/runtime evidence, not normal iteration-loop checks.

## Staged Dataset Status

The following generated MeasurementSets were present under
`/Volumes/GLENDENNING/casa-rs-imperformance/wave1` during this pass:

| Dataset | Tier | Path | Generation status |
|---|---|---|---|
| `wave1-vla-single-small` | small | `wave1/vla/single/small/ms/wave1-vla-single-small.ms` | present |
| `wave1-vla-single-medium` | medium | `wave1/vla/single/medium/ms/wave1-vla-single-medium.ms` | present |
| `wave1-vla-mosaic-small` | small | `wave1/vla/mosaic/small/ms/wave1-vla-mosaic-small.ms` | present |
| `wave1-alma-single-small` | small | `wave1/alma/single/small/ms/wave1-alma-single-small.ms` | present |
| `wave1-alma-single-medium` | medium | `wave1/alma/single/medium/ms/wave1-alma-single-medium.ms` | present |
| `wave1-alma-mosaic-small` | small | `wave1/alma/mosaic/small/ms/wave1-alma-mosaic-small.ms` | present |
| `wave1-alma-mosaic-large` | large | `wave1/alma/mosaic-large/large/ms/wave1-alma-mosaic-large.ms` | present |

The generated workload manifests for this pass were materialized under:

```text
target/imperformance-wave1/issue251-plan/workloads/
```

## Matrix

Ratios are `casa-rs median / CASA median`; values below `1.0x` mean `casa-rs`
was faster for the measured workload. All final rows use three warm repeats
and the storage label `external-ssd-wave1`.

| Mode | Dataset | Shape | Products | Result JSON | Rust median | CASA median | Ratio | Correctness status | Dominant `casa-rs` stage |
|---|---|---|---|---|---:|---:|---:|---|---|
| `standard-mfs-dirty-control` | `wave1-vla-single-small` | MFS, standard, dirty, `512x512`, 24 channels, natural, `niter=0` | `.image`, `.residual`, `.psf` | `target/imperformance-wave1/issue251-final-runs/20260518T143929Z-wave1-vla-single-small-standard-mfs-dirty-control-b6da5434.json` | `2.824 s` | `1.192 s` | `2.37x` | GREEN: product deltas near floating noise (`image diff_rms_over_casa_rms=7.33e-7`) | frontend/MS preparation (`2331 ms`), then gridding/degridding (`318 ms`) |
| `standard-mfs-clean-current` | `wave1-vla-single-small` | MFS, standard, multiscale clean, `512x512`, 24 channels, Briggs, `niter=25` | `.image`, `.residual`, `.psf` | `target/imperformance-wave1/issue251-final-runs/20260518T144020Z-wave1-vla-single-small-standard-mfs-clean-current-d03f4bef.json` | `9.672 s` | `17.240 s` | `0.56x` | GREEN: small product deltas (`image diff_rms_over_casa_rms=7.22e-5`) | gridding/degridding (`4898 ms`) and model refresh (`4457 ms`) |
| `standard-cube-line` | `wave1-alma-single-small` | cube, standard, dirty, `512x512`, 8 channels, natural, `niter=0` | `.image`, `.residual`, `.psf` | `target/imperformance-wave1/issue251-final-runs/20260518T144336Z-wave1-alma-single-small-standard-cube-line-8b4690e9.json` | `3.901 s` | `1.949 s` | `2.00x` | GREEN: product deltas near floating noise (`image diff_rms_over_casa_rms=7.25e-7`) | frontend/MS preparation (`3264 ms`), then gridding/degridding (`300 ms`) |
| `mosaic-mfs-clean-primary` | `wave1-alma-mosaic-small` | MFS, mosaic, multiscale clean, `512x512`, 8 channels, Briggs, `niter=25` | `.image`, `.residual`, `.psf` | `target/imperformance-wave1/issue251-final-runs/20260518T144439Z-wave1-alma-mosaic-small-mosaic-mfs-clean-primary-868672a0.json` | `3.784 s` | `5.481 s` | `0.69x` | RED: products are not correctness-comparable (`image diff_rms_over_casa_rms=0.741`) | model refresh (`695 ms`), gridding/degridding/PB work, and frontend preparation (`866 ms`) |
| `mosaic-cube-bounded` | `wave1-alma-mosaic-small` | cube, mosaic, dirty, `512x512`, 8 channels, natural, `niter=0` | `.image`, `.residual`, `.psf` | `target/imperformance-wave1/issue251-final-runs/20260518T144602Z-wave1-alma-mosaic-small-mosaic-cube-bounded-f6d83157.json` | `1.247 s` | `1.112 s` | `1.12x` | RED: products are not correctness-comparable (`image diff_rms_over_casa_rms=0.960`) | frontend/MS preparation (`961 ms`), then FFT/gridding/writeback |
| `mtmfs-wideband-sentinel` | `wave1-alma-single-small` | MFS, standard, MT-MFS, `512x512`, 8 channels, Briggs, `nterms=2`, `niter=25` | `.image.tt0`, `.residual.tt0`, `.psf.tt0` | `target/imperformance-wave1/issue251-final-runs/20260518T151913Z-wave1-alma-single-small-mtmfs-wideband-sentinel-869a0934.json` | `12.242 s` | `26.858 s` | `0.46x` | GREEN: small Taylor-term product deltas (`image.tt0 diff_rms_over_casa_rms=1.97e-4`, `residual.tt0 diff_rms_over_casa_rms=1.99e-4`) | gridding/degridding (`7943 ms`), especially major-cycle residual refresh (`7146 ms`) |

## Blocked Or Skipped Evidence

- Medium and large datasets are staged but not benchmarked in this pass. They
  should be used for explicit capacity/runtime runs after the small matrix
  settles the correctness and ownership shape.
- MT-MFS with the standard gridder now runs as the Wave 1 wideband sentinel.
  MT-MFS with `gridder='mosaic'` is intentionally out of this ticket and is
  tracked by #262.
- Mosaic MFS and mosaic cube produce timings, but their products are
  correctness-red against CASA on the generated ALMA mosaic-small dataset.
  These timings are useful for stage ownership only; they must not be used as
  correctness-comparable performance claims.
- W-projection and AW/widefield work remain outside this Wave 1 baseline per
  the mode-selection note and the existing #52 ownership.
- CASA-like `parallel` and `chanchunks` remain outside this Wave 1 baseline per
  #56.

## Bottleneck Ledger

| Workload family | Evidence | Current bottleneck owner | Follow-up direction |
|---|---|---|---|
| Standard MFS dirty | Correctness-green, Rust `2.37x` CASA | frontend/MS preparation dominates (`2331 ms` of `2676 ms` frontend total) | Optimize row selection/adaptation and prepared-batch construction before changing gridding algorithms for this control case. |
| Standard MFS clean | Correctness-green, Rust `0.56x` CASA | imaging core dominates, especially gridding/degridding and model refresh | Keep as green baseline; later 10x work needs grid/degrid and residual-refresh backend structure, not urgent correctness repair. |
| Standard cube dirty | Correctness-green, Rust `2.00x` CASA | frontend/MS preparation dominates (`3264 ms`) | Cube follow-up should start with per-channel preparation/dataflow and only then look at core cube gridding. |
| Mosaic MFS clean | Timing runs and is faster than CASA, but correctness-red | product/correctness parity is the blocker; timing stage owner is model refresh plus mosaic gridding/PB work | Fix generated-mosaic CASA/Rust comparability before using timing as optimization evidence. If parity turns green, this remains a high-leverage optimization path. |
| Mosaic cube bounded | Timing runs and is close to CASA, but correctness-red | product/correctness parity is the blocker; frontend dominates the small dirty path | Treat as a parity/comparability issue before optimizing cube/PB performance. |
| MT-MFS sentinel | Correctness-green, Rust `0.46x` CASA | imaging core dominates, especially gridding/degridding and major-cycle residual refresh | Keep as a green standard-gridder wideband sentinel. Track mosaic/PB-aware MT-MFS separately in #262. |

## Follow-On Ranking

1. Fix mosaic generated-data comparability before treating mosaic MFS as the
   first 10x optimization target. The mode still matters, but #251 evidence
   shows the current generated ALMA mosaic rows are correctness-red, so timing
   claims would be misleading.
2. Open or use a narrow follow-up for frontend/MS preparation throughput on
   correctness-green standard MFS dirty and standard cube dirty. Those rows are
   slower than CASA and dominated by `prepare_plane_input`.
3. Keep standard MFS clean as a green benchmark sentinel. It is already faster
   than CASA on the small generated workload, but the dominant measured stages
   are exactly the future backend/resource boundaries from the dataflow note:
   grid/degrid and model refresh.
4. Keep MT-MFS standard-gridder coverage as a green sentinel. Mosaic/PB-aware
   MT-MFS is backlog #262 and should not be hidden inside #251.

## Harness Fixes Made During This Pass

- `scripts/bench-imager-vs-casa.sh` no longer expands an empty
  `phasecenter_args` array under `set -u`; it appends Rust
  `--phasecenter-field` only when the manifest sets one.
- The CASA side now passes numeric `phasecenter` field ids instead of the
  string `FIELD_ID <n>`, which CASA rejected for mosaic `tclean`.
- Generated MT-MFS workload manifests now use `deconvolver='mtmfs'`,
  `nterms=2`, and Taylor-term product suffixes instead of silently measuring a
  multiscale workload under the MT-MFS mode id.
- Explicit CLI `--gridder standard` now forces the standard prepared-gridder
  path for `casars-imager` and the profiler helper, so the standard-gridder
  MT-MFS sentinel is not accidentally routed through mosaic/PB preparation.

## Reproduction

Regenerate the manifest set:

```sh
tools/perf/imager/stage_wave1_datasets.py \
  --data-root /Volumes/GLENDENNING/casa-rs-imperformance \
  --materialize-workloads \
  --output-dir target/imperformance-wave1/issue251-plan
```

Run the final small representative matrix:

```sh
for workload in \
  target/imperformance-wave1/issue251-plan/workloads/wave1-vla-single-small-standard-mfs-dirty-control.json \
  target/imperformance-wave1/issue251-plan/workloads/wave1-vla-single-small-standard-mfs-clean-current.json \
  target/imperformance-wave1/issue251-plan/workloads/wave1-alma-single-small-standard-cube-line.json \
  target/imperformance-wave1/issue251-plan/workloads/wave1-alma-mosaic-small-mosaic-mfs-clean-primary.json \
  target/imperformance-wave1/issue251-plan/workloads/wave1-alma-mosaic-small-mosaic-cube-bounded.json \
  target/imperformance-wave1/issue251-plan/workloads/wave1-alma-single-small-mtmfs-wideband-sentinel.json; do
  CASA_RS_IMPERF_DATA_ROOT=/Volumes/GLENDENNING/casa-rs-imperformance \
  CASA_RS_CASA_PYTHON=/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python \
  tools/perf/imager/run_workload.py \
    --repeats 3 \
    --run-label warm-final \
    --storage-label external-ssd-wave1 \
    --output-dir target/imperformance-wave1/issue251-final-runs \
    "$workload" || true
done
```

## Issue #251 Acceptance Mapping

- Baseline matrix with dataset, mode, manifest, image shape, channel count,
  weighting, deconvolver, `niter`, products, wallclock medians, run count, and
  storage label: sections "Matrix" and "Reproduction".
- Correctness evidence or blocked reason: sections "Matrix" and "Blocked Or
  Skipped Evidence".
- Stage timings and dominant subsystems: sections "Matrix" and "Bottleneck
  Ledger".
- Generated JSON artifacts and reproduction commands: sections "Matrix" and
  "Reproduction".
- Follow-on optimization ranking: section "Follow-On Ranking".
