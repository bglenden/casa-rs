#!/usr/bin/env bash
set -euo pipefail

outdir="${1:-target/wave6-issue167-automasking}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
casa_py="${CASA_RS_CASA_PYTHON:-$HOME/SoftwareProjects/casa-build/venv/bin/python}"
skip_casa="${CASA_RS_WAVE6_ISSUE167_SKIP_CASA:-0}"
skip_rust="${CASA_RS_WAVE6_ISSUE167_SKIP_RUST:-0}"
skip_imaging="${CASA_RS_WAVE6_ISSUE167_SKIP_IMAGING:-0}"
run_stamp="${CASA_RS_WAVE6_RUN_STAMP:-$(date '+%Y%m%dT%H%M%S%z')}"
generated_at="${CASA_RS_WAVE6_GENERATED_AT:-$(date '+%Y-%m-%d %H:%M:%S %z')}"

archive="$tutorial_root/tutorial-parity/alma/automasking/twhya_selfcal.ms.contsub.tar"
data_dir="$outdir/data"
ms="$data_dir/twhya_selfcal.ms.contsub"
timings_file="$outdir/wave6-issue167-timings.tsv"

mkdir -p "$outdir"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"
mkdir -p "$MPLCONFIGDIR"
if [[ "$skip_imaging" != "1" ]]; then
  rm -f "$timings_file"
fi

if [[ ! -x "$casa_py" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 2
fi
if [[ ! -f "$archive" ]]; then
  echo "missing Automasking Guide archive: $archive" >&2
  exit 2
fi

now_seconds() {
  python3 - <<'PY'
import time
print(f"{time.time():.9f}")
PY
}

record_timing() {
  local key="$1"
  local start="$2"
  python3 - "$timings_file" "$key" "$start" <<'PY'
import sys
import time

path, key, start = sys.argv[1], sys.argv[2], float(sys.argv[3])
elapsed = time.time() - start
with open(path, "a", encoding="utf-8") as handle:
    handle.write(f"{key}\t{elapsed:.3f}\n")
print(f"{key}={elapsed:.3f}s")
PY
}

clear_products() {
  local prefix="$1"
  local suffix
  for suffix in .image .model .psf .residual .mask .pb .sumwt .weight .image.pbcor; do
    rm -rf "${prefix}${suffix}"
  done
}

run_casars_imager() {
  if [[ -n "${CASA_RS_IMAGER:-}" ]]; then
    "$CASA_RS_IMAGER" "$@"
  else
    target/release/casars-imager "$@"
  fi
}

echo "preflight: tutorial root=$tutorial_root"
cargo run -p casa-test-support --bin casatestdata-preflight --quiet -- \
  --tier tutorial-parity \
  --require-registry-key alma/automasking/contsub-ms

if [[ ! -d "$ms" ]]; then
  mkdir -p "$data_dir"
  tar -xf "$archive" -C "$data_dir"
fi
if [[ ! -d "$ms" ]]; then
  echo "missing extracted MeasurementSet: $ms" >&2
  exit 2
fi

casa_dir="$outdir/casa"
rust_dir="$outdir/rust"
mkdir -p "$casa_dir" "$rust_dir"

if [[ "$skip_imaging" == "1" ]]; then
  echo "Skipping imaging because CASA_RS_WAVE6_ISSUE167_SKIP_IMAGING=1"
else
  if [[ "$skip_rust" != "1" && -z "${CASA_RS_IMAGER:-}" ]]; then
    cargo build --release -p casars-imager --bin casars-imager
  fi

  if [[ "$skip_casa" != "1" ]]; then
    "$casa_py" - "$ms" "$casa_dir" "$timings_file" <<'PY'
import shutil
import sys
import time
from pathlib import Path

from casatasks import tclean

ms = sys.argv[1]
outdir = Path(sys.argv[2])
timings_path = Path(sys.argv[3])

def clear_products(prefix: Path) -> None:
    for suffix in [".image", ".model", ".psf", ".residual", ".mask", ".pb", ".sumwt", ".weight", ".image.pbcor"]:
        shutil.rmtree(str(prefix) + suffix, ignore_errors=True)

def record_timing(key: str, start: float) -> None:
    elapsed = time.time() - start
    with timings_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{key}\t{elapsed:.3f}\n")
    print(f"{key}={elapsed:.3f}s")

def common_kwargs(prefix: Path) -> dict:
    return {
        "vis": ms,
        "imagename": str(prefix),
        "field": "5",
        "spw": "0",
        "specmode": "cube",
        "nchan": 15,
        "start": "0.0km/s",
        "width": "0.5km/s",
        "outframe": "LSRK",
        "restfreq": "372.67249GHz",
        "gridder": "standard",
        "deconvolver": "hogbom",
        "weighting": "briggsbwtaper",
        "robust": 0.5,
        "perchanweightdensity": True,
        "restoringbeam": "common",
        "imsize": [250, 250],
        "cell": "0.1arcsec",
        "phasecenter": 5,
        "datacolumn": "data",
        "interactive": False,
    }

dirty = outdir / "twhya_dirtycube"
clear_products(dirty)
dirty_start = time.time()
tclean(**common_kwargs(dirty), niter=0, threshold="0Jy")
record_timing("casa_dirty_seconds", dirty_start)

base = outdir / "twhya_base_params"
clear_products(base)
base_start = time.time()
tclean(
    **common_kwargs(base),
    niter=100000,
    threshold="87mJy",
    usemask="auto-multithresh",
    noisethreshold=4.25,
    sidelobethreshold=2.0,
    lownoisethreshold=1.5,
    minbeamfrac=0.3,
    negativethreshold=15.0,
    verbose=True,
    fastnoise=False,
)
record_timing("casa_auto_seconds", base_start)
PY
  fi

  if [[ "$skip_rust" != "1" ]]; then
    clear_products "$rust_dir/twhya_dirtycube"
    rust_dirty_start="$(now_seconds)"
    run_casars_imager \
      --ms "$ms" \
      --imagename "$rust_dir/twhya_dirtycube" \
      --field 5 \
      --spw 0 \
      --specmode cube \
      --channel-count 15 \
      --start 0.0km/s \
      --width 0.5km/s \
      --outframe LSRK \
      --restfreq 372.67249GHz \
      --deconvolver hogbom \
      --weighting briggsbwtaper \
      --robust 0.5 \
      --perchanweightdensity \
      --restoringbeam common \
      --imsize 250 \
      --cell-arcsec 0.1 \
      --phasecenter-field 5 \
      --niter 0 \
      --threshold-jy 0 \
      --datacolumn DATA \
      --no-preview-pngs
    record_timing rust_dirty_seconds "$rust_dirty_start"

    clear_products "$rust_dir/twhya_base_params"
    rust_auto_start="$(now_seconds)"
    run_casars_imager \
      --ms "$ms" \
      --imagename "$rust_dir/twhya_base_params" \
      --field 5 \
      --spw 0 \
      --specmode cube \
      --channel-count 15 \
      --start 0.0km/s \
      --width 0.5km/s \
      --outframe LSRK \
      --restfreq 372.67249GHz \
      --deconvolver hogbom \
      --weighting briggsbwtaper \
      --robust 0.5 \
      --perchanweightdensity \
      --restoringbeam common \
      --imsize 250 \
      --cell-arcsec 0.1 \
      --phasecenter-field 5 \
      --niter 100000 \
      --threshold-jy 0.087 \
      --usemask auto-multithresh \
      --noisethreshold 4.25 \
      --sidelobethreshold 2.0 \
      --lownoisethreshold 1.5 \
      --minbeamfrac 0.3 \
      --negativethreshold 15.0 \
      --no-fastnoise \
      --datacolumn DATA \
      --no-preview-pngs
    record_timing rust_auto_seconds "$rust_auto_start"
  fi
fi

"$casa_py" - "$outdir" "$run_stamp" "$generated_at" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image as image_tool

outdir = Path(sys.argv[1])
run_stamp = sys.argv[2]
generated_at = sys.argv[3]
timings_path = outdir / "wave6-issue167-timings.tsv"

PRODUCTS = [
    ("dirty_image", "twhya_dirtycube", ".image", "Jy/beam"),
    ("dirty_residual", "twhya_dirtycube", ".residual", "Jy/beam"),
    ("base_image", "twhya_base_params", ".image", "Jy/beam"),
    ("base_residual", "twhya_base_params", ".residual", "Jy/beam"),
    ("base_mask", "twhya_base_params", ".mask", "mask"),
]
PANEL_CHANNELS = [0, 7, 14]

def read_timings() -> dict[str, float]:
    timings: dict[str, float] = {}
    if timings_path.exists():
        for line in timings_path.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                try:
                    timings[parts[0]] = float(parts[1])
                except ValueError:
                    pass
    return timings

def read_image(path: Path) -> tuple[np.ndarray, np.ndarray]:
    ia = image_tool()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=np.float64)
        mask = np.asarray(ia.getchunk(getmask=True), dtype=bool)
    finally:
        ia.close()
    return data, mask

def channel_count(data: np.ndarray) -> int:
    if data.ndim >= 4:
        return data.shape[3]
    if data.ndim == 3:
        return data.shape[2]
    return 1

def channel_plane(data: np.ndarray, chan: int) -> np.ndarray:
    if data.ndim >= 4:
        return np.asarray(data[:, :, 0, chan])
    if data.ndim == 3:
        return np.asarray(data[:, :, chan])
    if data.ndim == 2:
        return np.asarray(data)
    return np.asarray([[float(np.squeeze(data))]])

def metric_block(casa_plane: np.ndarray, casa_valid: np.ndarray, rust_plane: np.ndarray, rust_valid: np.ndarray) -> dict[str, object]:
    shared = casa_valid & rust_valid & np.isfinite(casa_plane) & np.isfinite(rust_plane)
    if not np.any(shared):
        return {
            "shared_pixels": 0,
            "casa_rms": None,
            "rust_rms": None,
            "diff_rms": None,
            "diff_rms_over_casa_rms": None,
            "max_abs_diff": None,
            "p99_abs_diff": None,
            "casa_peak_abs": None,
            "max_abs_diff_over_casa_peak": None,
        }
    casa = casa_plane[shared]
    rust = rust_plane[shared]
    diff = rust - casa
    abs_diff = np.abs(diff)
    casa_rms = float(np.sqrt(np.nanmean(casa * casa)))
    rust_rms = float(np.sqrt(np.nanmean(rust * rust)))
    diff_rms = float(np.sqrt(np.nanmean(diff * diff)))
    casa_peak = max(float(np.nanmax(np.abs(casa))), 1.0e-30)
    return {
        "shared_pixels": int(np.count_nonzero(shared)),
        "casa_valid_pixels": int(np.count_nonzero(casa_valid)),
        "rust_valid_pixels": int(np.count_nonzero(rust_valid)),
        "mask_mismatch_pixels": int(np.count_nonzero(casa_valid != rust_valid)),
        "casa_rms": casa_rms,
        "rust_rms": rust_rms,
        "diff_rms": diff_rms,
        "diff_rms_over_casa_rms": float(diff_rms / abs(casa_rms)) if abs(casa_rms) > 0 else None,
        "max_abs_diff": float(np.nanmax(abs_diff)),
        "p99_abs_diff": float(np.nanpercentile(abs_diff, 99.0)),
        "casa_peak_abs": casa_peak,
        "max_abs_diff_over_casa_peak": float(np.nanmax(abs_diff) / casa_peak),
    }

def mask_metric_block(casa_plane: np.ndarray, casa_valid: np.ndarray, rust_plane: np.ndarray, rust_valid: np.ndarray) -> dict[str, object]:
    casa_bool = (casa_plane > 0.5) & casa_valid & np.isfinite(casa_plane)
    rust_bool = (rust_plane > 0.5) & rust_valid & np.isfinite(rust_plane)
    union = casa_bool | rust_bool
    intersection = casa_bool & rust_bool
    mismatch = casa_bool != rust_bool
    return {
        "casa_masked_pixels": int(np.count_nonzero(casa_bool)),
        "rust_masked_pixels": int(np.count_nonzero(rust_bool)),
        "mask_union_pixels": int(np.count_nonzero(union)),
        "mask_intersection_pixels": int(np.count_nonzero(intersection)),
        "mask_mismatch_pixels": int(np.count_nonzero(mismatch)),
        "mask_jaccard": (
            float(np.count_nonzero(intersection) / np.count_nonzero(union))
            if np.count_nonzero(union)
            else None
        ),
        "rust_only_mask_pixels": int(np.count_nonzero(rust_bool & ~casa_bool)),
        "casa_only_mask_pixels": int(np.count_nonzero(casa_bool & ~rust_bool)),
    }

def write_panel(
    key: str,
    prefix: str,
    suffix: str,
    chan: int,
    casa_plane: np.ndarray,
    casa_valid: np.ndarray,
    rust_plane: np.ndarray,
    rust_valid: np.ndarray,
    units: str,
) -> str:
    shared = casa_valid & rust_valid & np.isfinite(casa_plane) & np.isfinite(rust_plane)
    if suffix == ".mask":
        casa_plot = np.ma.array(casa_plane > 0.5, mask=~casa_valid)
        rust_plot = np.ma.array(rust_plane > 0.5, mask=~rust_valid)
        diff_plot = np.ma.array((rust_plane > 0.5).astype(float) - (casa_plane > 0.5).astype(float), mask=~(casa_valid | rust_valid))
        vmin, vmax, dmax = 0.0, 1.0, 1.0
        cmap = "gray_r"
    else:
        casa_plot = np.ma.array(casa_plane, mask=~(casa_valid & np.isfinite(casa_plane)))
        rust_plot = np.ma.array(rust_plane, mask=~(rust_valid & np.isfinite(rust_plane)))
        diff_plot = np.ma.array(rust_plane - casa_plane, mask=~shared)
        values = np.concatenate([casa_plot.compressed(), rust_plot.compressed()])
        if values.size:
            vmin = float(np.nanpercentile(values, 1.0))
            vmax = float(np.nanpercentile(values, 99.0))
            if vmin == vmax:
                peak = max(abs(vmin), 1.0)
                vmin, vmax = -peak, peak
        else:
            vmin, vmax = -1.0, 1.0
        diff_values = np.abs(diff_plot.compressed())
        dmax = max(float(np.nanpercentile(diff_values, 99.0)), 1.0e-12) if diff_values.size else 1.0
        cmap = "inferno"
    fig, axes = plt.subplots(1, 3, figsize=(13.2, 4.2), constrained_layout=True)
    casa_artist = axes[0].imshow(casa_plot.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
    rust_artist = axes[1].imshow(rust_plot.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
    diff_artist = axes[2].imshow(diff_plot.T, origin="lower", cmap="RdBu_r", vmin=-dmax, vmax=dmax)
    axes[0].set_title("CASA C++")
    axes[1].set_title("casa-rs")
    axes[2].set_title("casa-rs - CASA")
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    label = units or "pixel value"
    fig.colorbar(casa_artist, ax=axes[0], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(rust_artist, ax=axes[1], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(diff_artist, ax=axes[2], fraction=0.046, pad=0.04, label=f"delta {label}")
    fig.suptitle(f"{prefix}{suffix} channel {chan}")
    panel = outdir / f"{key}-chan{chan}-panel.png"
    fig.savefig(panel, dpi=140)
    plt.close(fig)
    return str(panel)

summary: dict[str, object] = {
    "issue": 167,
    "dataset": "alma/automasking/contsub-ms",
    "generated_stamp": run_stamp,
    "generated_at": generated_at,
    "timings": read_timings(),
    "products": {},
}

for key, prefix, suffix, units in PRODUCTS:
    casa_path = outdir / "casa" / f"{prefix}{suffix}"
    rust_path = outdir / "rust" / f"{prefix}{suffix}"
    casa_data, casa_mask = read_image(casa_path)
    rust_data, rust_mask = read_image(rust_path)
    if casa_data.shape != rust_data.shape:
        raise RuntimeError(f"{key} shape mismatch: CASA {casa_data.shape} vs casa-rs {rust_data.shape}")
    product: dict[str, object] = {
        "shape": list(casa_data.shape),
        "valid_casa": int(np.count_nonzero(casa_mask)),
        "valid_rust": int(np.count_nonzero(rust_mask)),
        "valid_mask_mismatch_pixels": int(np.count_nonzero(casa_mask != rust_mask)),
        "channels": {},
        "units": units,
    }
    for chan in range(channel_count(casa_data)):
        casa_plane = channel_plane(casa_data, chan)
        rust_plane = channel_plane(rust_data, chan)
        casa_valid = channel_plane(casa_mask, chan).astype(bool)
        rust_valid = channel_plane(rust_mask, chan).astype(bool)
        metrics = (
            mask_metric_block(casa_plane, casa_valid, rust_plane, rust_valid)
            if suffix == ".mask"
            else metric_block(casa_plane, casa_valid, rust_plane, rust_valid)
        )
        if chan in PANEL_CHANNELS:
            metrics["panel"] = write_panel(
                key, prefix, suffix, chan, casa_plane, casa_valid, rust_plane, rust_valid, units
            )
        product["channels"][str(chan)] = metrics
    summary["products"][key] = product

summary_path = outdir / "wave6-issue167-summary.json"
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
(outdir / "README.md").write_text(
    "# Wave 6 #167 Automasking Evidence\n\n"
    "Generated by `scripts/run-wave6-issue167-automasking.sh`.\n\n"
    f"Generated at: `{generated_at}`\n\n"
    f"Run stamp: `{run_stamp}`\n\n"
    f"Summary JSON: `{summary_path.name}`\n\n"
    "Panels compare CASA C++ and casa-rs products from the same Automasking Guide MeasurementSet. "
    "Each panel includes CASA C++, casa-rs, and difference images for channels 0, 7, and 14.\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
