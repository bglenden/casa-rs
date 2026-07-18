# Wave 3 Issue 95 - Imager usepointing Performance

Truth class: current descriptive
Last reality check: 2026-04-27
Verification: focused `casars-imager` tests; VLASS and TW Hydra timing commands below; macOS `sample` against local CASA C++ run

Wave issue: #140
Child issue: #95

Issue #95 adds an explicit CASA-style `usepointing` control to the
`casars-imager` CLI, task contract, launcher schema, and Python wrapper. The
default is `false`, matching CASA task defaults for standard-MFS work. With the
default, geometry preparation uses FIELD phase centers and does not build a
POINTING-table direction resolver just to infer that the run is standard MFS.

## Public Control

The public surfaces are:

- CLI: `--usepointing` and `--use-pointing`
- JSON task contract: `use_pointing: bool`, default `false`
- UI schema: `usepointing` advanced toggle, default `false`
- Python helper: `casars.tasks.imager(..., usepointing=False)`

When `usepointing=true`, casa-rs preserves the previous POINTING-based antenna
direction resolution. When it is `false`, POINTING data is ignored for row
geometry and the antenna directions fall back to FIELD phase centers.

Future explicit `gridder` controls should stay orthogonal to this flag:
`gridder=standard` should not imply POINTING resolution, while future
mosaic/AWProjection paths may choose `usepointing=true` only when the user or a
documented gridder default explicitly requests POINTING-based directions.

## VLASS Benchmark

Dataset:

```text
/Users/brianglendenning/SoftwareProjects/casatestdata/measurementset/vla/ref_vlass_wtsp_creation.ms
```

Rust command shape:

```bash
target/release/casars-imager \
  --ms /Users/brianglendenning/SoftwareProjects/casatestdata/measurementset/vla/ref_vlass_wtsp_creation.ms \
  --imagename target/wdad-wave3-95/rust-default-run-1 \
  --imsize 128 \
  --cell-arcsec 2.5 \
  --field 0 \
  --ddid 0 \
  --spw 0 \
  --corr RR \
  --datacolumn DATA \
  --weighting natural \
  --deconvolver hogbom \
  --dirty-only \
  --no-preview-pngs
```

The before-equivalent command adds `--usepointing`, which preserves the old
implicit POINTING resolver path.

| Case | Wall-clock median | `prepare_plane_input` median | Resolver evidence |
|---|---:|---:|---|
| Rust default `usepointing=false` | `0.02 s` | `12.580 ms` | `build_pointing_resolver=0.000 s` |
| Rust `--usepointing` | `2.36 s` | `2066.971 ms` | `build_pointing_resolver=2.122 s` |
| CASA `tclean` standard dirty RR | `0.109 s` | not measured | `gridder="standard"` |

The default Rust path is now faster than the comparable CASA wall-clock median
on this small warmed benchmark and avoids the large POINTING resolver tax. This
case grids only `152` samples into a 128x128 dirty image and writes roughly
`408 KB` of CASA image-table output with preview PNGs disabled, so it is a
targeted regression benchmark for the POINTING resolver tax rather than a
general imaging-throughput claim.

## Full Wave 3 Tutorial Benchmark

The Wave 3 #117-sized workload uses the ALMA First Look / TW Hydra calibrated
MS staged at:

```text
target/wdad-wave3-117/twhya_calibrated.ms
```

Shared command shape:

```text
field=5 spw=0 specmode=mfs datacolumn=DATA weighting=briggs robust=0.5
deconvolver=hogbom niter=0 dirty-only imsize=250 cell=0.1arcsec
```

CASA was run with `gridder="standard"` and `usepointing=False` explicitly.
casa-rs used the default `usepointing=false`; progress logs show
`build_pointing_resolver=0.000 s`.

| Case | Wall-clock median | Internal / staged median | Notes |
|---|---:|---:|---|
| Rust before trace-free fast path | not rerun after cleanup | `15.722 s` frontend total | `prepare_plane_input=9.945 s`, `run_imaging=5.589 s` |
| Rust after trace-free fast path | `7.80 s` | `7.608 s` frontend total | `prepare_plane_input=2.497 s`, `run_imaging=5.105 s` |
| Rust after scalar-row and row-local access work | not rerun as CLI wall | `6.755 s` frontend total | `prepare_plane_input=1.667 s`, `run_imaging=5.081 s` |
| Rust after combined dirty PSF/residual gridding | not rerun as CLI wall | `4.777 s` frontend total | `prepare_plane_input=1.676 s`, `run_imaging=3.084 s` |
| CASA `tclean(..., usepointing=False)` | `5.246 s` | not measured in this rerun | standard gridder, no POINTING |

The trace-free standard-MFS path removes the per-sample oracle trace allocation
from ordinary imaging runs while preserving trace generation for explicit
oracle APIs. Additional scalar-column selection, direct standard-MFS collapse,
row-local typed array access, and `FLAG_ROW` pre-filtering reduce frontend
overhead. The remaining dirty-imaging gap was in the core: for `niter=0`,
casa-rs previously gridded PSF weights and residual visibilities in two full
passes over the same weighted samples. The combined dirty standard-MFS path
grids both in one sample pass and keeps the output equivalent to the previous
separate-pass implementation. The current full Rust run is about `0.91x` CASA
wall-clock on this benchmark, satisfying the Wave 3 parity target for this
standard dirty-MFS case.

The current progress instrumentation shows the remaining Rust cost split:

```text
select_main_rows=0.043 s
load_FLAG_ROW=0.021 s
load_DATA=0.479 s
load_FLAG=0.371 s
load_WEIGHT=0.017 s
build_prepared_geometry_rows=0.062 s
accumulate_rows=0.618 s
finish_standard_mfs_without_trace=0.050 s
run_imaging=3.162 s
```

For this dataset, `FLAG_ROW` does not explain the gap:

```text
rows_seen=44772 rows_flagged=0 rows_skipped_by_flag_row=0
adapt_samples_ms=609.370
```

## CASA C++ Sampling

The comparable CASA run was sampled with macOS `sample` against the local CASA
Python/C++ build. The sampled run elapsed `5.499973 s`, close to the benchmarked
CASA median above.

The sample shows CASA spending the active imaging path in:

```text
SynthesisImager::executeMajorCycle
SynthesisImagerVi2::runMajorCycle
SIMapperCollection::grid
GridFT::put
sectggridd_
```

The same stack also shows CASA consulting flags in the imaging path:

```text
VisBufferImpl2::fillImagingWeight
VisBufferImpl2::flagCube
VisibilityIteratorImpl2::flag
FTMachine::setSpectralFlag
arrayCompareAny<bool>
```

CASA reads visibility and flag data through `VisibilityIteratorImpl2` and table
column cell access during weighting/gridding. Storage profiling confirmed that
casa-rs still spends roughly `0.9 s` in DATA/FLAG tiled-array reads on this
dataset, but the parity-closing issue was the separate PSF and dirty-residual
gridding passes. The combined dirty path keeps the same prepared data contract
and avoids widening the change into a larger MeasurementSet streaming rewrite.

Captured artifacts:

```text
target/wdad-wave3-95/vlass-default-profile.txt
target/wdad-wave3-95/vlass-usepointing-profile.txt
target/wdad-wave3-95/vlass-rust-default-wall.txt
target/wdad-wave3-95/vlass-rust-usepointing-wall.txt
target/wdad-wave3-95/vlass-casa-tclean.txt
target/wdad-wave3-95/vlass-default-progress.stderr
target/wdad-wave3-95/vlass-usepointing-progress.stderr
target/wdad-wave3-95/twhya-field5-default-profile-final.txt
target/wdad-wave3-95/twhya-field5-rust-default-wall-final.txt
target/wdad-wave3-95/twhya-casa-tclean-usepointing-false.txt
target/wdad-wave3-95/twhya-final-progress.stderr
target/wdad-wave3-95/twhya-current-profile.txt
target/wdad-wave3-95/twhya-current-progress.stderr
target/wdad-wave3-95/twhya-combined-dirty-profile.txt
target/wdad-wave3-95/twhya-combined-dirty-progress.stderr
target/wdad-wave3-95/twhya-storage-current-profile.txt
target/wdad-wave3-95/twhya-storage-current.stderr
target/wdad-wave3-95/casa-sample.stdout
target/wdad-wave3-95/casa-sample.stderr
target/wdad-wave3-95/casa-sample-process.txt
```
