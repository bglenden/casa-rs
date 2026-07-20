#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  outdir="$1"
else
  outdir="target/wave6-issue175-vla-imaging-$(date -u +%Y%m%dT%H%M%SZ)"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
casa_python="${CASA_RS_CASA_PYTHON:-$HOME/SoftwareProjects/casa-build/venv/bin/python}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
archive="$tutorial_root/tutorial-parity/vla/imaging/SNR_G55_10s.calib.tar.gz"
cases="${CASA_RS_WAVE6_ISSUE175_CASES:-smoke}"

if [[ ! -x "$casa_python" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 1
fi

has_case() {
  local needle="$1"
  [[ " $cases " == *" $needle "* ]]
}

mkdir -p "$outdir"/{data,rust,casa}

echo "wave6 issue175: outdir=$outdir"
echo "wave6 issue175: CASA_RS_TUTORIAL_DATA_ROOT=$tutorial_root"
echo "wave6 issue175: CASA_RS_CASA_PYTHON=$casa_python"
echo "wave6 issue175: cases=$cases"

cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier tutorial-parity \
  --require-registry-key vla/imaging/calibrated-ms

if [[ ! -d "$outdir/data/SNR_G55_10s.calib.ms" ]]; then
  tar -xzf "$archive" -C "$outdir/data"
fi
ms="$outdir/data/SNR_G55_10s.calib.ms"

cargo build --release \
  -p casars-imager --bin casars-imager \
  -p casa-images --bin immath \
  -p casars --bin imexplore

cat >"$outdir/outliers.txt" <<'EOF'
imagename=Outlier1
imsize=[320,320]
phasecenter = J2000 19:23:27.693 22.37.37.180
imagename=Outlier2
imsize=[320,320]
phasecenter = J2000 19:25:46.888 21.22.03.365
EOF

cat >"$outdir/tutorial-sequence.json" <<'JSON'
{
  "guide": "VLA CASA Imaging",
  "guide_version": "CASA 6.5.4 scripted source",
  "issue": 175,
  "source_oldid": 36701,
  "script_sha256": "8c15e776ca6f8f6bd4a6a3c67044ed8f258c77571550c34b39f5427bf758f4a2",
  "dataset_sha256": "b79a63d1142674c89c4c3ae702a28625867728a420a3c156e0ec44078200bf6a",
  "non_goals": ["deconvolver='asp'", "gridder='wproject' with deconvolver='asp'"],
  "covered_tasks": [
    "tclean niter=0",
    "tclean niter=1000 and niter=10000 regular clean",
    "tclean nmajor=4",
    "tclean weighting natural/uniform/briggs",
    "tclean deconvolver='multiscale'",
    "tclean gridder='wproject' plus multiscale",
    "tclean deconvolver='mtmfs' nterms=2",
    "tclean mtmfs plus wproject",
    "tclean outlierfile multiscale modelcolumn",
    "impbcor equivalent image/PB division",
    "widebandpbcor equivalent MTMFS PB-corrected products",
    "imhead summary/list and mode='put' bunit",
    "immath scalar expression"
  ]
}
JSON

run_rust_smoke() {
  local prefix="$outdir/rust/SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1"
  /usr/bin/time -p "$repo_root/target/release/casars-imager" \
    --ms "$ms" \
    --imagename "$prefix" \
    --imsize 128 \
    --cell-arcsec 8 \
    --pblimit -0.01 \
    --spw 0 \
    --deconvolver mtmfs \
    --nterms 2 \
    --scales 0,6,10,30,60 \
    --smallscalebias 0.9 \
    --wterm wproject \
    --niter 1 \
    --threshold-jy 0.00015 \
    --weighting briggs \
    --robust 0 \
    --savemodel modelcolumn \
    --pbcor \
    --no-preview-pngs \
    >"$outdir/rust-smoke.log" 2>&1
}

run_casa_smoke() {
  local casa_ms="$outdir/data/casa-smoke/SNR_G55_10s.calib.ms"
  if [[ ! -d "$casa_ms" ]]; then
    mkdir -p "$outdir/data/casa-smoke"
    tar -xzf "$archive" -C "$outdir/data/casa-smoke"
  fi
  "$casa_python" - "$outdir" "$casa_ms" <<'PY'
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

from casatasks import imstat, tclean

out = Path(sys.argv[1])
ms = sys.argv[2]
prefix = out / "casa" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1"
for suffix in [
    ".image.tt0",
    ".image.tt1",
    ".model.tt0",
    ".model.tt1",
    ".residual.tt0",
    ".residual.tt1",
    ".psf.tt0",
    ".psf.tt1",
    ".psf.tt2",
    ".alpha",
    ".alpha.error",
    ".pb.tt0",
    ".sumwt.tt0",
    ".sumwt.tt1",
    ".sumwt.tt2",
]:
    path = str(prefix) + suffix
    if os.path.exists(path):
        shutil.rmtree(path)

start = time.monotonic()
tclean(
    vis=ms,
    imagename=str(prefix),
    spw="0",
    gridder="wproject",
    wprojplanes=-1,
    pblimit=-0.01,
    imsize=128,
    cell="8arcsec",
    specmode="mfs",
    deconvolver="mtmfs",
    nterms=2,
    scales=[0, 6, 10, 30, 60],
    smallscalebias=0.9,
    interactive=False,
    niter=1,
    weighting="briggs",
    robust=0.0,
    stokes="I",
    threshold="0.15mJy",
    savemodel="modelcolumn",
)
summary = {"elapsed_s": time.monotonic() - start}
for suffix in [".image.tt0", ".residual.tt0", ".model.tt0", ".alpha"]:
    path = str(prefix) + suffix
    if not os.path.exists(path):
        continue
    stats = imstat(path)
    summary[suffix] = {
        key: float(stats[key][0]) if key in stats and len(stats[key]) else None
        for key in ["max", "min", "rms", "sum"]
    }
    summary[suffix]["maxpos"] = [int(value) for value in stats.get("maxpos", [])]
    summary[suffix]["minpos"] = [int(value) for value in stats.get("minpos", [])]

(out / "casa-smoke-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
PY
}

compare_smoke() {
  "$casa_python" - "$outdir" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from casatools import image as image_tool

out = Path(sys.argv[1])
pairs = {
    "image.tt0": (
        out / "rust" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.image.tt0",
        out / "casa" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.image.tt0",
    ),
    "residual.tt0": (
        out / "rust" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.residual.tt0",
        out / "casa" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.residual.tt0",
    ),
    "model.tt0": (
        out / "rust" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.model.tt0",
        out / "casa" / "SNR_G55_10s.ms.MTMFS.wProj.spw0.niter1.model.tt0",
    ),
}
ia = image_tool()
summary = {}
for name, (rust_path, casa_path) in pairs.items():
    ia.open(str(rust_path))
    rust = np.array(ia.getchunk()).astype(float)
    ia.close()
    ia.open(str(casa_path))
    casa = np.array(ia.getchunk()).astype(float)
    ia.close()
    diff = rust - casa
    summary[name] = {
        "rust_max": float(np.nanmax(rust)),
        "casa_max": float(np.nanmax(casa)),
        "diff_rms": float(np.sqrt(np.nanmean(diff * diff))),
        "diff_max_abs": float(np.nanmax(np.abs(diff))),
        "corr": float(np.corrcoef(rust.ravel(), casa.ravel())[0, 1])
        if np.nanstd(rust) and np.nanstd(casa)
        else None,
    }
(out / "smoke-comparison.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
PY
}

run_rust_official() {
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.dirty" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --niter 0 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.Reg.Clean.niter1K" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --niter 1000 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.Reg.Clean.niter10K" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --niter 10000 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.Reg.Clean.nmajor4" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --niter 10000 --nmajor 4 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.natural" --imsize 540 --cell-arcsec 8 --pblimit -0.01 --niter 1000 --threshold-jy 0.00015 --weighting natural --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.uniform" --imsize 540 --cell-arcsec 8 --pblimit -0.01 --niter 1000 --threshold-jy 0.00015 --weighting uniform --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.briggs" --imsize 540 --cell-arcsec 8 --pblimit -0.01 --niter 1000 --threshold-jy 0.00015 --weighting briggs --robust 0 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.MultiScale" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --deconvolver multiscale --scales 0,6,10,30,60 --smallscalebias 0.9 --niter 1000 --threshold-jy 0.00012 --weighting briggs --robust 0 --savemodel modelcolumn --pbcor --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.wProj" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --wterm wproject --deconvolver multiscale --scales 0,6,10,30,60 --smallscalebias 0.9 --niter 1000 --threshold-jy 0.00015 --weighting briggs --robust 0 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.ms.MTMFS" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --deconvolver mtmfs --nterms 2 --scales 0,6,10,30,60 --smallscalebias 0.9 --niter 1000 --threshold-jy 0.00015 --weighting briggs --robust 0 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR_G55_10s.ms.MTMFS.wProj" --imsize 1280 --cell-arcsec 8 --pblimit -0.01 --wterm wproject --deconvolver mtmfs --nterms 2 --scales 0,6,10,30,60 --smallscalebias 0.9 --niter 1000 --threshold-jy 0.00015 --weighting briggs --robust 0 --savemodel modelcolumn --pbcor --no-preview-pngs
  "$repo_root/target/release/casars-imager" --ms "$ms" --imagename "$outdir/rust/SNR.MS.MFS-Main" --outlierfile "$outdir/outliers.txt" --imsize 640 --cell-arcsec 8 --pblimit -0.01 --deconvolver multiscale --scales 0,6,10,30,60 --smallscalebias 0.9 --niter 1000 --threshold-jy 0.00015 --weighting briggs --robust 0 --savemodel modelcolumn --no-preview-pngs
  "$repo_root/target/release/immath" --imagename "$outdir/rust/SNR_G55_10s.MultiScale.image" --imagename "$outdir/rust/SNR_G55_10s.MultiScale.pb" --expr "IM0 / IM1" --outfile "$outdir/rust/SNR_G55_10s.MS.pbcorr.image" --overwrite
  "$repo_root/target/release/immath" --imagename "$outdir/rust/SNR_G55_10s.ms.MTMFS.wProj.image.tt0" --expr "1.222e6*IM0/1.579^2/(29.30*29.03)" --outfile "$outdir/rust/SNR_G55_10s.ms.MTMFS.wProj.image.tt0-Tb" --overwrite
  "$repo_root/target/release/imexplore" imhead "$outdir/rust/SNR_G55_10s.ms.MTMFS.wProj.image.tt0-Tb" --mode put --hdkey bunit --hdvalue K --json >"$outdir/rust/imhead-put-bunit.json"
}

if has_case smoke; then
  run_rust_smoke
  run_casa_smoke
  compare_smoke
fi

if has_case official-rust; then
  run_rust_official
fi

echo "Wrote $outdir"
