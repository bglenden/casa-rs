#!/usr/bin/env bash
set -euo pipefail

outdir="${1:-target/wave6-issue53-mosaic-panels}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
casa_py="${CASA_RS_CASA_PYTHON:-$HOME/SoftwareProjects/casa-build/venv/bin/python}"
fetch="${CASA_RS_FETCH_TUTORIAL_DATA:-0}"
dataset="${CASA_RS_WAVE6_DATASET:-all}"

case "$dataset" in
  all|alma|vla) ;;
  *)
    echo "CASA_RS_WAVE6_DATASET must be all, alma, or vla" >&2
    exit 2
    ;;
esac

mkdir -p "$outdir"

want_dataset() {
  [[ "$dataset" == "all" || "$dataset" == "$1" ]]
}

download_if_requested() {
  local url="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    if [[ "$path" == *.tgz || "$path" == *.tar.gz ]]; then
      if tar -tzf "$path" >/dev/null 2>&1; then
        return
      fi
      if [[ "$fetch" != "1" ]]; then
        echo "incomplete archive at $path; set CASA_RS_FETCH_TUTORIAL_DATA=1 to resume $url" >&2
        return 2
      fi
      echo "resuming incomplete archive at $path" >&2
    else
      return
    fi
  fi
  if [[ "$fetch" != "1" ]]; then
    echo "missing $path; set CASA_RS_FETCH_TUTORIAL_DATA=1 to download $url" >&2
    return 2
  fi
  mkdir -p "$(dirname "$path")"
  curl --http1.1 --retry 5 --retry-delay 5 --retry-connrefused -L --fail --continue-at - --output "$path" "$url"
}

extract_if_needed() {
  local archive="$1"
  local directory="$2"
  local parent="$3"
  if [[ -d "$directory" ]]; then
    return
  fi
  mkdir -p "$parent"
  tar -xzf "$archive" -C "$parent"
}

run_casars_imager() {
  if [[ -n "${CASA_RS_IMAGER:-}" ]]; then
    "$CASA_RS_IMAGER" "$@"
  else
    cargo run -p casars-imager --release --quiet -- "$@"
  fi
}

alma_dir="$tutorial_root/tutorial-parity/alma/antennae/band7"
alma_archive="$alma_dir/Antennae_Band7_CalibratedData.tgz"
alma_data="$alma_dir/Antennae_Band7_CalibratedData"
if want_dataset alma; then
  download_if_requested \
    "https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_CalibratedData.tgz" \
    "$alma_archive"
  extract_if_needed "$alma_archive" "$alma_data" "$alma_dir"
fi

vla_dir="$tutorial_root/tutorial-parity/vla/3c391"
vla_archive="$vla_dir/EVLA_3C391_FinalCalibratedMosaicMS.tgz"
vla_ms_full="$vla_dir/EVLA_3C391_FinalCalibratedMosaicMS/3c391_ctm_mosaic_spw0.ms"
if want_dataset vla; then
  download_if_requested \
    "https://casa.nrao.edu/Data/EVLA/3C391/EVLA_3C391_FinalCalibratedMosaicMS.tgz" \
    "$vla_archive"
  extract_if_needed "$vla_archive" "$vla_ms_full" "$vla_dir"
fi

alma_ms="$alma_data/Antennae_North.cal.ms"
if want_dataset alma && [[ ! -d "$alma_ms" ]]; then
  echo "missing Antennae North MS at $alma_ms" >&2
  exit 2
fi
if want_dataset vla && [[ ! -d "$vla_ms_full" ]]; then
  echo "missing 3C391 MS at $vla_ms_full" >&2
  exit 2
fi

alma_run="$outdir/alma-antennae-north"
vla_run="$outdir/vla-3c391"
mkdir -p "$alma_run" "$vla_run"

if want_dataset alma; then
"$casa_py" - "$alma_ms" "$alma_run" <<'PY'
import os
import shutil
import sys
from pathlib import Path

from casatasks import tclean

vis = sys.argv[1]
outdir = Path(sys.argv[2])
imagename = outdir / "casa-antennae-north-cont-clean"
for suffix in [".image", ".model", ".image.pbcor", ".psf", ".residual", ".pb", ".sumwt", ".weight", ".mask"]:
    shutil.rmtree(str(imagename) + suffix, ignore_errors=True)
tclean(
    vis=vis,
    imagename=str(imagename),
    field="",
    phasecenter=12,
    deconvolver="hogbom",
    specmode="mfs",
    restfreq="345.79599GHz",
    spw="0:1~50;120~164",
    gridder="mosaic",
    datacolumn="data",
    mosweight=True,
    imsize=500,
    cell="0.13arcsec",
    interactive=False,
    niter=32,
    threshold="0.4mJy",
    pbcor=True,
)
PY

run_casars_imager \
  --ms "$alma_ms" \
  --imagename "$alma_run/rust-antennae-north-cont-clean" \
  --imsize 500 \
  --cell-arcsec 0.13 \
  --phasecenter-field 12 \
  --spw "0:1~50;120~164" \
  --datacolumn DATA \
  --deconvolver hogbom \
  --niter 32 \
  --gain 0.1 \
  --threshold-jy 0.0004 \
  --pbcor
fi

if want_dataset vla; then
"$casa_py" - "$vla_ms_full" "$vla_run" <<'PY'
import os
import shutil
import sys
from pathlib import Path

from casatasks import tclean

vis = sys.argv[1]
outdir = Path(sys.argv[2])
imagename = outdir / "casa-3c391-ctm-spw0-multiscale"
for suffix in [".image", ".model", ".image.pbcor", ".pbcorimage", ".psf", ".residual", ".pb", ".sumwt", ".weight", ".mask"]:
    shutil.rmtree(str(imagename) + suffix, ignore_errors=True)
tclean(
    vis=vis,
    imagename=str(imagename),
    field="",
    spw="",
    specmode="mfs",
    niter=500,
    gain=0.1,
    threshold="1.0mJy",
    gridder="mosaic",
    deconvolver="multiscale",
    scales=[0, 5, 15, 45],
    smallscalebias=0.9,
    interactive=False,
    imsize=[480, 480],
    cell="2.5arcsec",
    stokes="I",
    weighting="briggs",
    robust=0.5,
    pbcor=True,
)
PY

run_casars_imager \
  --ms "$vla_ms_full" \
  --imagename "$vla_run/rust-3c391-ctm-spw0-multiscale" \
  --imsize 480 \
  --cell-arcsec 2.5 \
  --phasecenter-field 0 \
  --deconvolver multiscale \
  --scales 0,5,15,45 \
  --smallscalebias 0.9 \
  --weighting briggs \
  --robust 0.5 \
  --niter 500 \
  --gain 0.1 \
  --threshold-jy 0.001 \
  --pbcor
fi

"$casa_py" - "$outdir" "$dataset" <<'PY'
import json
import math
import sys
from pathlib import Path
from urllib.request import urlretrieve

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image as image_tool

outdir = Path(sys.argv[1])
dataset = sys.argv[2]

def want_dataset(name):
    return dataset == "all" or dataset == name

originals = {}
if want_dataset("alma"):
    originals["alma_residual"] = ("https://casaguides.nrao.edu/images/9/98/Antennae_North.Cont.Clean.residual.tclean.png", outdir / "original-antennae-north-cont-clean-residual.png")
    originals["alma_image"] = ("https://casaguides.nrao.edu/images/4/44/Antennae_North.Cont.Clean.image-Antennae_South.Cont.Clean.image.png", outdir / "original-antennae-north-south-cont-clean-image.png")
if want_dataset("vla"):
    originals["vla_image"] = ("https://casaguides.nrao.edu/images/9/9e/3c391-tclean-residuals-CASA6.4.1.png", outdir / "original-3c391-final-restored.png")
for url, path in originals.values():
    if not path.exists():
        urlretrieve(url, path)

def read_image(path):
    ia = image_tool()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=float)
    finally:
        ia.close()
    while data.ndim > 2:
        data = data[:, :, 0]
    return data.T

def crop_common(a, b):
    ny = min(a.shape[0], b.shape[0])
    nx = min(a.shape[1], b.shape[1])
    def center_crop(x):
        y0 = (x.shape[0] - ny) // 2
        x0 = (x.shape[1] - nx) // 2
        return x[y0:y0+ny, x0:x0+nx]
    return center_crop(a), center_crop(b)

def robust_limits(data):
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return -1.0, 1.0
    lo, hi = np.nanpercentile(finite, [1, 99])
    if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
        peak = float(np.nanmax(np.abs(finite))) if finite.size else 1.0
        return -peak, peak
    return float(lo), float(hi)

def write_panel(name, title, original_path, casa_prefix, rust_prefix, product):
    casa = read_image(str(casa_prefix) + product)
    rust = read_image(str(rust_prefix) + product)
    casa, rust = crop_common(casa, rust)
    diff = rust - casa
    vmin, vmax = robust_limits(np.concatenate([casa.ravel(), rust.ravel()]))
    dpeak = np.nanpercentile(np.abs(diff[np.isfinite(diff)]), 99) if np.isfinite(diff).any() else 1.0
    dpeak = float(max(dpeak, 1.0e-12))
    fig, axes = plt.subplots(1, 4, figsize=(16, 4.5), constrained_layout=True)
    axes[0].imshow(plt.imread(original_path))
    axes[0].set_title("CASA Guide figure")
    axes[1].imshow(casa, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax)
    axes[1].set_title("CASA C++ " + product)
    axes[2].imshow(rust, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax)
    axes[2].set_title("casa-rs " + product)
    axes[3].imshow(diff, origin="lower", cmap="coolwarm", vmin=-dpeak, vmax=dpeak)
    axes[3].set_title("casa-rs - CASA")
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    fig.suptitle(title)
    panel = outdir / f"{name}-{product.strip('.')}-panel.png"
    fig.savefig(panel, dpi=150)
    plt.close(fig)
    return {
        "panel_png": str(panel),
        "casa_rms": float(np.sqrt(np.nanmean(casa * casa))),
        "rust_rms": float(np.sqrt(np.nanmean(rust * rust))),
        "diff_rms": float(np.sqrt(np.nanmean(diff * diff))),
        "diff_max_abs": float(np.nanmax(np.abs(diff))),
    }

summary_path = outdir / "wave6-issue53-mosaic-panel-summary.json"
if summary_path.exists() and dataset != "all":
    with open(summary_path, "r", encoding="utf-8") as handle:
        summary = json.load(handle)
else:
    summary = {}
if want_dataset("alma"):
    summary["alma_residual"] = write_panel(
        "alma-antennae-north-cont-clean",
        "Antennae North continuum mosaic residual after 32 CLEAN iterations",
        originals["alma_residual"][1],
        outdir / "alma-antennae-north" / "casa-antennae-north-cont-clean",
        outdir / "alma-antennae-north" / "rust-antennae-north-cont-clean",
        ".residual",
    )
    summary["alma_image_pbcor"] = write_panel(
        "alma-antennae-north-cont-clean",
        "Antennae North continuum mosaic PB-corrected restored image",
        originals["alma_image"][1],
        outdir / "alma-antennae-north" / "casa-antennae-north-cont-clean",
        outdir / "alma-antennae-north" / "rust-antennae-north-cont-clean",
        ".image.pbcor",
    )
if want_dataset("vla"):
    summary["vla_image"] = write_panel(
        "vla-3c391-multiscale",
        "3C391 multiscale continuum mosaic restored image with niter=500",
        originals["vla_image"][1],
        outdir / "vla-3c391" / "casa-3c391-ctm-spw0-multiscale",
        outdir / "vla-3c391" / "rust-3c391-ctm-spw0-multiscale",
        ".image",
    )
    summary["vla_image_pbcor"] = write_panel(
        "vla-3c391-multiscale",
        "3C391 multiscale continuum mosaic PB-corrected restored image with niter=500",
        originals["vla_image"][1],
        outdir / "vla-3c391" / "casa-3c391-ctm-spw0-multiscale",
        outdir / "vla-3c391" / "rust-3c391-ctm-spw0-multiscale",
        ".image.pbcor",
    )
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

cat > "$outdir/README.md" <<EOF
# Wave 6 Issue 53 Mosaic Panels

Generated by \`scripts/run-wave6-issue53-mosaic-panels.sh\`.

Dataset selector: \`$dataset\`.

Panels place the CASA Guide figure, CASA C++ product, casa-rs product, and casa-rs minus CASA difference image side by side.

Summary JSON: \`wave6-issue53-mosaic-panel-summary.json\`
EOF

echo "Wrote Wave 6 #53 mosaic panels under $outdir"
