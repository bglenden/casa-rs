#!/usr/bin/env bash
set -euo pipefail

outdir="${1:-target/wave6-issue53-mosaic-panels}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
casa_py="${CASA_RS_CASA_PYTHON:-$HOME/SoftwareProjects/casa-build/venv/bin/python}"
fetch="${CASA_RS_FETCH_TUTORIAL_DATA:-0}"
dataset="${CASA_RS_WAVE6_DATASET:-all}"
skip_casa="${CASA_RS_WAVE6_SKIP_CASA:-0}"

case "$dataset" in
  all|alma|vla) ;;
  *)
    echo "CASA_RS_WAVE6_DATASET must be all, alma, or vla" >&2
    exit 2
    ;;
esac

mkdir -p "$outdir"
timings_file="$outdir/wave6-issue53-mosaic-timings.tsv"
if [[ "$dataset" == "all" && "$skip_casa" != "1" ]]; then
  rm -f "$timings_file"
fi

now_seconds() {
  python - <<'PY'
import time
print(f"{time.time():.9f}")
PY
}

record_timing() {
  local key="$1"
  local start="$2"
  python - "$timings_file" "$key" "$start" <<'PY'
import sys
import time

path, key, start = sys.argv[1], sys.argv[2], float(sys.argv[3])
elapsed = time.time() - start
with open(path, "a", encoding="utf-8") as handle:
    handle.write(f"{key}\t{elapsed:.3f}\n")
print(f"{key}={elapsed:.3f}s")
PY
}

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
if [[ "$skip_casa" != "1" ]]; then
alma_casa_start="$(now_seconds)"
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
record_timing alma_casa_seconds "$alma_casa_start"
else
  echo "Skipping ALMA CASA C++ run because CASA_RS_WAVE6_SKIP_CASA=1"
fi

alma_rust_start="$(now_seconds)"
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
record_timing alma_rust_seconds "$alma_rust_start"
fi

if want_dataset vla; then
if [[ "$skip_casa" != "1" ]]; then
vla_casa_start="$(now_seconds)"
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
record_timing vla_casa_seconds "$vla_casa_start"
else
  echo "Skipping VLA CASA C++ run because CASA_RS_WAVE6_SKIP_CASA=1"
fi

vla_rust_start="$(now_seconds)"
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
record_timing vla_rust_seconds "$vla_rust_start"
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
timings_path = outdir / "wave6-issue53-mosaic-timings.tsv"

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

def read_timings():
    timings = {}
    if not timings_path.exists():
        return timings
    with open(timings_path, "r", encoding="utf-8") as handle:
        for line in handle:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                try:
                    timings[parts[0]] = float(parts[1])
                except ValueError:
                    pass
    return timings

def read_image(path):
    ia = image_tool()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=float)
        mask = np.asarray(ia.getchunk(getmask=True), dtype=bool)
    finally:
        ia.close()
    while data.ndim > 2:
        data = data[:, :, 0]
    while mask.ndim > 2:
        mask = mask[:, :, 0]
    return data.T, mask.T

def crop_common(*arrays):
    ny = min(array.shape[0] for array in arrays)
    nx = min(array.shape[1] for array in arrays)
    def center_crop(x):
        y0 = (x.shape[0] - ny) // 2
        x0 = (x.shape[1] - nx) // 2
        return x[y0:y0+ny, x0:x0+nx]
    return tuple(center_crop(array) for array in arrays)

def robust_limits(data):
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        return -1.0, 1.0
    lo, hi = np.nanpercentile(finite, [1, 99])
    if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
        peak = float(np.nanmax(np.abs(finite))) if finite.size else 1.0
        return -peak, peak
    return float(lo), float(hi)

def sci(value):
    if value is None or not np.isfinite(value):
        return "n/a"
    return f"{value:.3e}"

def seconds(value):
    if value is None:
        return "n/a"
    return f"{value:.1f}s"

def write_panel(name, title, original_path, casa_prefix, rust_prefix, product, timing_key):
    casa, casa_mask = read_image(str(casa_prefix) + product)
    rust, rust_mask = read_image(str(rust_prefix) + product)
    casa, rust, casa_mask, rust_mask = crop_common(casa, rust, casa_mask, rust_mask)
    shared_valid = casa_mask & rust_mask & np.isfinite(casa) & np.isfinite(rust)
    diff = rust - casa
    valid_values = np.concatenate([casa[shared_valid], rust[shared_valid]]) if shared_valid.any() else np.concatenate([casa.ravel(), rust.ravel()])
    vmin, vmax = robust_limits(valid_values)
    valid_diff = diff[shared_valid]
    dpeak = np.nanpercentile(np.abs(valid_diff), 99) if valid_diff.size else 1.0
    dpeak = float(max(dpeak, 1.0e-12))
    casa_display = np.ma.array(casa, mask=~shared_valid)
    rust_display = np.ma.array(rust, mask=~shared_valid)
    diff_display = np.ma.array(diff, mask=~shared_valid)
    image_cmap = plt.get_cmap("inferno").copy()
    image_cmap.set_bad("#d9d9d9")
    diff_cmap = plt.get_cmap("coolwarm").copy()
    diff_cmap.set_bad("#d9d9d9")
    fig, axes = plt.subplots(1, 4, figsize=(17.5, 5.4), constrained_layout=True)
    axes[0].imshow(plt.imread(original_path))
    axes[0].set_title("CASA Guide figure")
    casa_artist = axes[1].imshow(casa_display, origin="lower", cmap=image_cmap, vmin=vmin, vmax=vmax)
    axes[1].set_title("CASA C++ " + product)
    rust_artist = axes[2].imshow(rust_display, origin="lower", cmap=image_cmap, vmin=vmin, vmax=vmax)
    axes[2].set_title("casa-rs " + product)
    diff_artist = axes[3].imshow(diff_display, origin="lower", cmap=diff_cmap, vmin=-dpeak, vmax=dpeak)
    axes[3].set_title("casa-rs - CASA")
    for ax, artist, label in [
        (axes[1], casa_artist, "Jy/beam"),
        (axes[2], rust_artist, "Jy/beam"),
        (axes[3], diff_artist, "Jy/beam"),
    ]:
        colorbar = fig.colorbar(artist, ax=ax, fraction=0.046, pad=0.02)
        colorbar.set_label(label)
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    fig.suptitle(title)
    valid_count = int(np.count_nonzero(shared_valid))
    total_count = int(shared_valid.size)
    casa_rms = float(np.sqrt(np.nanmean(casa[shared_valid] * casa[shared_valid]))) if valid_count else float("nan")
    rust_rms = float(np.sqrt(np.nanmean(rust[shared_valid] * rust[shared_valid]))) if valid_count else float("nan")
    diff_rms = float(np.sqrt(np.nanmean(valid_diff * valid_diff))) if valid_count else float("nan")
    diff_max_abs = float(np.nanmax(np.abs(valid_diff))) if valid_count else float("nan")
    rel_diff_rms = float(diff_rms / abs(casa_rms)) if np.isfinite(casa_rms) and abs(casa_rms) > 0 else float("nan")
    casa_seconds = timings.get(f"{timing_key}_casa_seconds")
    rust_seconds = timings.get(f"{timing_key}_rust_seconds")
    speed_ratio = float(rust_seconds / casa_seconds) if casa_seconds and rust_seconds else None
    metrics = (
        f"stats on shared valid PB/mask support: {valid_count}/{total_count} pixels "
        f"({100.0 * valid_count / max(total_count, 1):.1f}%); "
        f"RMS CASA={sci(casa_rms)}, casa-rs={sci(rust_rms)}, diff={sci(diff_rms)}, "
        f"diff/CASA={rel_diff_rms:.2f}x, max|diff|={sci(diff_max_abs)}; "
        f"wall CASA={seconds(casa_seconds)}, casa-rs={seconds(rust_seconds)}"
    )
    if speed_ratio is not None:
        metrics += f", casa-rs/CASA={speed_ratio:.2f}x"
    fig.text(0.5, 0.01, metrics, ha="center", va="bottom", fontsize=9)
    panel = outdir / f"{name}-{product.strip('.')}-panel.png"
    fig.savefig(panel, dpi=150)
    plt.close(fig)
    return {
        "panel_png": str(panel),
        "casa_rms": casa_rms,
        "rust_rms": rust_rms,
        "diff_rms": diff_rms,
        "diff_max_abs": diff_max_abs,
        "diff_rms_over_casa_rms": rel_diff_rms,
        "shared_valid_pixels": valid_count,
        "total_pixels": total_count,
        "casa_valid_pixels": int(np.count_nonzero(casa_mask)),
        "rust_valid_pixels": int(np.count_nonzero(rust_mask)),
        "casa_seconds": casa_seconds,
        "rust_seconds": rust_seconds,
        "rust_over_casa_wall_time": speed_ratio,
    }

timings = read_timings()
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
        "alma",
    )
    summary["alma_image_pbcor"] = write_panel(
        "alma-antennae-north-cont-clean",
        "Antennae North continuum mosaic PB-corrected restored image",
        originals["alma_image"][1],
        outdir / "alma-antennae-north" / "casa-antennae-north-cont-clean",
        outdir / "alma-antennae-north" / "rust-antennae-north-cont-clean",
        ".image.pbcor",
        "alma",
    )
if want_dataset("vla"):
    summary["vla_image"] = write_panel(
        "vla-3c391-multiscale",
        "3C391 multiscale continuum mosaic restored image with niter=500",
        originals["vla_image"][1],
        outdir / "vla-3c391" / "casa-3c391-ctm-spw0-multiscale",
        outdir / "vla-3c391" / "rust-3c391-ctm-spw0-multiscale",
        ".image",
        "vla",
    )
    summary["vla_image_pbcor"] = write_panel(
        "vla-3c391-multiscale",
        "3C391 multiscale continuum mosaic PB-corrected restored image with niter=500",
        originals["vla_image"][1],
        outdir / "vla-3c391" / "casa-3c391-ctm-spw0-multiscale",
        outdir / "vla-3c391" / "rust-3c391-ctm-spw0-multiscale",
        ".image.pbcor",
        "vla",
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
