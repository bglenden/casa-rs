# Wave 3 Issue 117 - ALMA First Look TW Hydra MFS Foundation

Truth class: current descriptive
Last reality check: 2026-04-27
Verification: `scripts/test-python-package.sh`; `casatestdata-preflight --tier tutorial-parity --require-registry-key alma/first-look/twhya/calibrated-ms`; release imager/CASA timing and parity commands below

Wave issue: #140
Child issue: #117

This note records the CASA-to-casa-rs mapping for the first ALMA First Look /
TW Hydra imaging segment. It does not add a new public imaging API; the
standalone `msexplore` and `casars-imager` executables remain the canonical task
owners, with Python exposing thin wrappers over their existing JSON protocols.

## Dataset

The calibrated TW Hydra input is the tutorial-registry artifact:

- key: `alma/first-look/twhya/calibrated-ms`
- source artifact: `twhya_calibrated.ms.tar`
- expected SHA-256:
  `f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2`
- local policy:
  `${CASA_RS_TUTORIAL_DATA_ROOT}/tutorial-parity/alma/first-look/twhya/twhya_calibrated.ms.tar`

The wave check used:

```bash
cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier tutorial-parity \
  --require-registry-key alma/first-look/twhya/calibrated-ms
```

Observed roots:

```text
casatestdata preflight: tier=tutorial-parity root=/Users/brianglendenning/SoftwareProjects/casatestdata
casatestdata preflight: tutorial root=/Users/brianglendenning/SoftwareProjects/casa-tutorial-data
```

## Tutorial Mapping

| CASA tutorial operation | casa-rs owner | Wave 3 #117 mapping |
|---|---|---|
| `listobs(vis="twhya_calibrated.ms")` | `casa-ms` / `msexplore` | `msexplore --format json --field 5 ...` emits structured `ListObsSummary` for the target field. |
| `plotms` visibility inspection | `casa-ms` / `msexplore` | `msexplore --preset amplitude_vs_uv_distance --field 5 ...` writes a PNG plot artifact. |
| MFS continuum `tclean` | `casa-imaging` / `casars-imager` | `casars-imager --specmode mfs --field 5 --spw 0 --imsize 250 --cell-arcsec 0.1 ...` writes CASA-compatible `.psf`, `.residual`, `.model`, and `.image` products plus preview PNGs. |
| calibrated-data `split` | future child issue | Not implemented in #117; #118 owns corrected-data split handoff for the self-calibration loop. |

The target field resolved from the real tutorial MS is `FIELD_ID=5`, name
`TW Hya`, with `44772` selected rows, `SPW_ID=0`, and `384` channels.

## Executable Evidence

The real tutorial MS was staged under `target/wdad-wave3-117/twhya_calibrated.ms`
from the registry tarball. The following commands completed successfully:

```bash
cargo run -q -p casa-ms --bin msexplore -- \
  --format json \
  --field 5 \
  --output target/wdad-wave3-117/twhya-field5-listobs.json \
  --overwrite \
  target/wdad-wave3-117/twhya_calibrated.ms

cargo run -q -p casa-ms --bin msexplore -- \
  --preset amplitude_vs_uv_distance \
  --field 5 \
  --plot-output target/wdad-wave3-117/twhya-field5-amp-uv.png \
  --plot-width 1000 \
  --plot-height 700 \
  target/wdad-wave3-117/twhya_calibrated.ms

target/release/casars-imager \
  --ms target/wdad-wave3-117/twhya_calibrated.ms \
  --imagename target/wdad-wave3-117/release-twhya-field5-mfs \
  --imsize 250 \
  --cell-arcsec 0.1 \
  --field 5 \
  --spw 0 \
  --specmode mfs \
  --datacolumn DATA \
  --weighting briggs \
  --robust 0.5 \
  --deconvolver hogbom \
  --niter 0 \
  --threshold-jy 0 \
  --dirty-only \
  --managed-output true
```

The release imager smoke wrote existing artifacts for `.psf`, `.residual`,
`.model`, `.image`, and all four preview PNGs. It reported `17192448` gridded
samples, no warnings, and `12.386 s` internal frontend total time on this
machine.

## Correctness Evidence

The visibility-inspection parity check rendered `msexplore` and CASA `plotms`
from the same TW Hydra selection: `field=5`, `spw=0`, `scan=12`.

```bash
scripts/render-msexplore-side-by-side.sh \
  --ms "$PWD/target/wdad-wave3-117/twhya_calibrated.ms" \
  --output "$PWD/target/wdad-wave3-117/twhya-field5-scan12-amplitude-uvdist-side-by-side-readable.png" \
  --plot-width 2400 \
  --plot-height 1350 \
  --rust-symbolsize 1 \
  --casa-xaxis uvdist \
  --casa-yaxis amp \
  --casa-kw field=5 \
  --casa-kw spw=0 \
  --casa-kw scan=12 \
  --rust-label "casa-rs msexplore amplitude vs uvdist" \
  --casa-label "CASA plotms amplitude vs uvdist" \
  -- --preset amplitude_vs_uv_distance --field 5 --spw 0 --scan 12

scripts/render-msexplore-page-side-by-side.sh \
  --ms "$PWD/target/wdad-wave3-117/twhya_calibrated.ms" \
  --output "$PWD/target/wdad-wave3-117/twhya-field5-scan12-amplitude-phase-time-side-by-side-readable.png" \
  --plot-width 2400 \
  --plot-height 1350 \
  --rust-symbolsize 1 \
  --field 5 \
  --spw 0 \
  --scan 12 \
  --rust-label "casa-rs msexplore amplitude / phase vs time" \
  --casa-label "CASA plotms amplitude / phase vs time"
```

These side-by-side helpers use `msexplore --symbolsize 1` and larger CASA
`plotms` axis/title fonts to keep dense tutorial plots visually comparable.

The MFS dirty image parity check rendered the `casars-imager` `.image` product
and a CASA `tclean` `.image` product with a shared display scale, plus a
`casa-rs - CASA` difference panel:

```text
target/wdad-wave3-117/twhya-field5-mfs-image-casa-rs-vs-casa.png
```

Numerical image comparison for the 250x250 `.image` arrays:

| Metric | casa-rs | CASA |
|---|---:|---:|
| min | `-0.0399281606` | `-0.0398871973` |
| max | `0.2548154294` | `0.2548294365` |
| mean | `0.0003378017` | `0.0003362949` |
| stddev | `0.0162907603` | `0.0162881185` |

The direct difference had `rms=4.548259971625769e-05 Jy/beam`,
`max_abs=0.00031748320907354355 Jy/beam`, and image correlation
`0.9999961193712577`.

## Timing Evidence

The comparable one-repeat release timing command was:

```bash
BENCH_REPEATS=1 \
IMAGER_BENCH_FIELD=5 \
IMAGER_BENCH_SPW=0 \
IMAGER_BENCH_CHANNEL_START=0 \
IMAGER_BENCH_CHANNEL_COUNT=384 \
IMAGER_BENCH_SPECMODE=mfs \
IMAGER_BENCH_IMSIZE=250 \
IMAGER_BENCH_CELL_ARCSEC=0.1 \
IMAGER_BENCH_WEIGHTING=briggs \
IMAGER_BENCH_ROBUST=0.5 \
IMAGER_BENCH_MODE=dirty \
scripts/bench-imager-vs-casa.sh target/wdad-wave3-117/twhya_calibrated.ms
```

Observed timing on 2026-04-27:

| Path | Wall time | Internal total | Notes |
|---|---:|---:|---|
| Rust before #95 performance work | `13.110 s` | `11.660 s` | `prepare_plane_input=6455 ms`, `run_imaging=5193 ms` |
| Rust after #95 performance work | not rerun as CLI wall | `4.777 s` | `prepare_plane_input=1676 ms`, `run_imaging=3084 ms` |
| CASA `tclean` | `5.246 s` | not measured in final rerun | CASA 6.7.5-9 local Python; `gridder="standard"`, `usepointing=False` |

Rust is now about `0.91x` CASA by internal total for this dirty MFS case. Issue
#95 remains the Wave 3 performance evidence trail for the default standard-MFS
frontend overhead, explicit `usepointing` control, and combined dirty
PSF/residual gridding optimization.

## Python Projection

`casars-python` now includes task wrappers for:

- `casars.tasks.msexplore`: protocol info, schema retrieval, raw JSON run, and
  summary helper.
- `casars.tasks.imager`: protocol info, schema retrieval, raw JSON run, and MFS
  helper.

Both wrappers delegate to the existing executables through `--json-run`; they do
not move task ownership into Python.
