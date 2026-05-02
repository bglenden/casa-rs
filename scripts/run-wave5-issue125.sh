#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi
if [[ -z "${CASA_RS_CASA_PYTHON:-}" || ! -x "$CASA_RS_CASA_PYTHON" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 2
fi

outdir="${1:-target/wave5-issue125}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"
mkdir -p "$MPLCONFIGDIR"

tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
model="${CASA_RS_WAVE5_MODEL:-$tutorial_root/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits}"
casa_ms="${CASA_RS_WAVE5_CASA_MS:-target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.ms}"
casa_model="${CASA_RS_WAVE5_CASA_MODEL:-target/wave5-parity-full/psimvla1_casa/psimvla1_casa.vla.a.skymodel}"

if [[ ! -f "$model" ]]; then
  echo "missing VLA ppdisk model FITS: $model" >&2
  exit 2
fi
if [[ ! -d "$casa_ms" ]]; then
  echo "missing CASA reference MS: $casa_ms" >&2
  echo "Run the Wave 5 synthetic-observation foundation parity setup first, or set CASA_RS_WAVE5_CASA_MS." >&2
  exit 2
fi
casa_ms="$(cd "$(dirname "$casa_ms")" && pwd)/$(basename "$casa_ms")"
if [[ -d "$casa_model" ]]; then
  casa_model="$(cd "$(dirname "$casa_model")" && pwd)/$(basename "$casa_model")"
else
  casa_model=""
fi

cargo build --release -q \
  -p casa-ms --bin simobserve --bin msexplore \
  -p casars-imager --bin casars-imager \
  -p casa-images --bin imexplore

rust_ms="$outdir/ppdisk-rust-analysis.ms"
casa_image="$outdir/casa-ppdisk-analysis"
rust_image="$outdir/rust-ppdisk-analysis"
casa_plot="$outdir/casa-plotms-amplitude-vs-uvdist.png"
rust_plot="$outdir/rust-msexplore-amplitude-vs-uvdist.png"

rm -rf \
  "$rust_ms" \
  "$casa_image".image "$casa_image".model "$casa_image".psf "$casa_image".residual "$casa_image".sumwt "$casa_image".pb \
  "$rust_image".image "$rust_image".model "$rust_image".psf "$rust_image".residual "$rust_image".sumwt \
  "$casa_plot" "$rust_plot"

time_json_stdout() {
  local label="$1"
  local outfile="$2"
  local stdout_file="$3"
  shift 3
  python3 - "$label" "$outfile" "$stdout_file" "$@" <<'PY'
import json
import subprocess
import sys
import time

label = sys.argv[1]
outfile = sys.argv[2]
stdout_file = sys.argv[3]
cmd = sys.argv[4:]
started = time.perf_counter()
with open(stdout_file, "w", encoding="utf-8") as stdout:
    completed = subprocess.run(cmd, check=True, stdout=stdout)
elapsed = time.perf_counter() - started
with open(outfile, "w", encoding="utf-8") as handle:
    json.dump({"label": label, "elapsed_seconds": elapsed, "returncode": completed.returncode}, handle, indent=2)
PY
}

time_json_stdout rust-simobserve "$outdir/rust-simobserve-wall-timing.json" "$outdir/rust-simobserve-report.json" \
  target/release/simobserve \
  --model "$model" \
  --out "$rust_ms" \
  --duration 3600 \
  --integration 2 \
  --overwrite

time_json_stdout rust-tclean "$outdir/rust-tclean-wall-timing.json" "$outdir/rust-tclean-report.json" \
  target/release/casars-imager \
  --ms "$rust_ms" \
  --imagename "$rust_image" \
  --imsize 257 \
  --cell-arcsec 0.00311 \
  --dirty-only \
  --weighting natural \
  --niter 0 \
  --threshold-jy 0 \
  --datacolumn DATA \
  --no-preview-pngs

target/release/imexplore imhead "$rust_image.image" --json > "$outdir/rust-imhead.json"
target/release/imexplore imstat "$rust_image.image" --json > "$outdir/rust-imstat.json"

time_json_stdout rust-msexplore-plot "$outdir/rust-plot-timing.json" "$outdir/rust-msexplore-report.json" \
  target/release/msexplore "$rust_ms" \
  --xaxis uvdist \
  --yaxis amp \
  --data-column data \
  --plot-output "$rust_plot" \
  --plot-width 1200 \
  --plot-height 800 \
  --title "casa-rs VLA ppdisk amplitude vs uv distance" \
  --overwrite

CASA_RS_OUTDIR="$outdir" \
CASA_RS_CASA_MS="$casa_ms" \
CASA_RS_CASA_IMAGE="$casa_image" \
CASA_RS_CASA_PLOT="$casa_plot" \
"$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatasks import imhead, imstat, tclean
from casatools import table

try:
    from casatasks import plotms
except Exception:
    from casaplotms import plotms

outdir = Path(os.environ["CASA_RS_OUTDIR"])
casa_ms = os.environ["CASA_RS_CASA_MS"]
casa_image = os.environ["CASA_RS_CASA_IMAGE"]
casa_plot = os.environ["CASA_RS_CASA_PLOT"]

started = time.perf_counter()
tclean(
    vis=casa_ms,
    imagename=casa_image,
    specmode="mfs",
    gridder="standard",
    deconvolver="hogbom",
    weighting="natural",
    imsize=257,
    cell="0.00311arcsec",
    niter=0,
    threshold="0Jy",
    datacolumn="data",
    interactive=False,
)
with open(outdir / "casa-tclean-timing.json", "w", encoding="utf-8") as handle:
    json.dump({"label": "casa-tclean", "elapsed_seconds": time.perf_counter() - started}, handle, indent=2)

with open(outdir / "casa-imhead.json", "w", encoding="utf-8") as handle:
    json.dump(imhead(imagename=f"{casa_image}.image", mode="summary"), handle, indent=2, default=str)
with open(outdir / "casa-imstat.json", "w", encoding="utf-8") as handle:
    stats = imstat(imagename=f"{casa_image}.image")
    json.dump({k: (v.tolist() if hasattr(v, "tolist") else v) for k, v in stats.items()}, handle, indent=2, default=str)

started = time.perf_counter()
plot_status = {"backend": "plotms", "fallback": False}
try:
    plotms(
        vis=casa_ms,
        xaxis="uvdist",
        yaxis="amp",
        plotfile=casa_plot,
        expformat="png",
        showgui=False,
        clearplots=True,
        width=1200,
        height=800,
        dpi=72,
        overwrite=True,
        title="CASA C++ VLA ppdisk amplitude vs uv distance",
    )
except Exception as exc:
    plot_status = {
        "backend": "casatools.table + matplotlib",
        "fallback": True,
        "plotms_error": str(exc),
    }
    tb = table()
    tb.open(casa_ms)
    try:
        uvw = tb.getcol("UVW")
        data = tb.getcol("DATA")
    finally:
        tb.close()
    uvdist = np.sqrt(uvw[0] ** 2 + uvw[1] ** 2)
    amp = np.abs(data[0, 0, :])
    fig, ax = plt.subplots(figsize=(12, 8), constrained_layout=True)
    ax.scatter(uvdist, amp, s=5, alpha=0.6)
    ax.set_title("CASA C++ VLA ppdisk amplitude vs uv distance")
    ax.set_xlabel("uv distance (m)")
    ax.set_ylabel("amplitude (Jy)")
    fig.savefig(casa_plot, dpi=100)
    plt.close(fig)
with open(outdir / "casa-plot-timing.json", "w", encoding="utf-8") as handle:
    json.dump({"label": "casa-plotms", "elapsed_seconds": time.perf_counter() - started}, handle, indent=2)
with open(outdir / "casa-plot-status.json", "w", encoding="utf-8") as handle:
    json.dump(plot_status, handle, indent=2)
PY

"$CASA_RS_CASA_PYTHON" - "$outdir" "$casa_ms" "$rust_ms" "$casa_image.image" "$rust_image.image" "$casa_plot" "$rust_plot" "$casa_model" <<'PY'
import json
import math
import subprocess
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
from casatools import image

outdir = Path(sys.argv[1])
casa_ms = Path(sys.argv[2])
rust_ms = Path(sys.argv[3])
casa_image = Path(sys.argv[4])
rust_image = Path(sys.argv[5])
casa_plot = Path(sys.argv[6])
rust_plot = Path(sys.argv[7])
casa_model = Path(sys.argv[8]) if sys.argv[8] else None

def read_json(name):
    return json.loads((outdir / name).read_text(encoding="utf-8"))

def read_image(path):
    ia = image()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=np.float64)
        shape = list(data.shape)
        coords = ia.coordsys().torecord()
        units = ia.brightnessunit()
    finally:
        ia.close()
    while data.ndim > 2:
        data = data[..., 0]
    return data, {"shape": shape, "units": units, "coordsys": coords}

def finite(value):
    value = float(value)
    return value if math.isfinite(value) else str(value)

def compare_array(rust, casa):
    result = {
        "rust_shape": list(rust.shape),
        "casa_shape": list(casa.shape),
        "same_shape": rust.shape == casa.shape,
    }
    if rust.shape != casa.shape:
        return result
    diff = rust - casa
    mag = np.abs(diff)
    denom = np.sqrt(np.mean(np.asarray(casa) * np.asarray(casa))) + 1.0e-30
    result.update({
        "max_abs_diff": finite(np.max(mag)) if mag.size else 0.0,
        "mean_abs_diff": finite(np.mean(mag)) if mag.size else 0.0,
        "p95_abs_diff": finite(np.percentile(mag, 95)) if mag.size else 0.0,
        "p99_9_abs_diff": finite(np.percentile(mag, 99.9)) if mag.size else 0.0,
        "rms_abs_diff": finite(np.sqrt(np.mean(mag * mag))) if mag.size else 0.0,
        "relative_rms_diff": finite(np.sqrt(np.mean(mag * mag)) / denom) if mag.size else 0.0,
    })
    return result

def scalar_from_casa_stat(stats, key):
    value = stats[key]
    if isinstance(value, list):
        return float(value[0])
    return float(value)

def compare_imstat(rust_stats, casa_stats):
    compared = {}
    for key in ["npts", "min", "max", "sum", "mean", "rms", "sigma"]:
        if key not in rust_stats or key not in casa_stats:
            continue
        rust = float(rust_stats[key])
        casa = scalar_from_casa_stat(casa_stats, key)
        compared[key] = {
            "rust": rust,
            "casa": casa,
            "abs_diff": abs(rust - casa),
        }
    return compared

casa_data, casa_header = read_image(casa_image)
rust_data, rust_header = read_image(rust_image)
model_data = None
if casa_model is not None and casa_model.exists():
    model_data, _ = read_image(casa_model)

image_compare = compare_array(rust_data, casa_data)
image_diff = rust_data - casa_data

image_vmin = float(np.percentile(np.concatenate([rust_data.ravel(), casa_data.ravel()]), 0.5))
image_vmax = float(np.percentile(np.concatenate([rust_data.ravel(), casa_data.ravel()]), 99.5))
diff_abs = float(np.percentile(np.abs(image_diff), 99.9)) or float(np.max(np.abs(image_diff))) or 1.0
fig, axes = plt.subplots(2, 2, figsize=(12, 10), constrained_layout=True)
panels = [
    (axes[0, 0], model_data if model_data is not None else casa_data, "Input model image" if model_data is not None else "CASA C++ dirty image", None, None, "viridis"),
    (axes[0, 1], rust_data, "casa-rs dirty image", image_vmin, image_vmax, "viridis"),
    (axes[1, 0], casa_data, "CASA C++ dirty image", image_vmin, image_vmax, "viridis"),
    (axes[1, 1], image_diff, "casa-rs minus CASA C++", -diff_abs, diff_abs, "coolwarm"),
]
for ax, data, title, vmin, vmax, cmap in panels:
    if vmin is None:
        vmin = float(np.percentile(data, 0.5))
        vmax = float(np.percentile(data, 99.5))
    im = ax.imshow(data.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
    ax.set_title(title)
    ax.set_xlabel("x pixel")
    ax.set_ylabel("y pixel")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
fig.savefig(outdir / "wave5-issue125-image-panel.png", dpi=150)
plt.close(fig)

fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)
for ax, path, title in [
    (axes[0], casa_plot, "CASA C++ plotms"),
    (axes[1], rust_plot, "casa-rs msexplore"),
]:
    ax.imshow(mpimg.imread(path))
    ax.set_title(title)
    ax.axis("off")
fig.savefig(outdir / "wave5-issue125-plot-panel.png", dpi=150)
plt.close(fig)

invalid_prefix = outdir / "invalid-ms-check"
invalid = subprocess.run(
    [
        "target/release/casars-imager",
        "--ms",
        str(outdir / "does-not-exist.ms"),
        "--imagename",
        str(invalid_prefix),
        "--imsize",
        "32",
        "--cell-arcsec",
        "1",
        "--dirty-only",
        "--no-preview-pngs",
    ],
    text=True,
    capture_output=True,
)

timings = {}
for name in [
    "rust-simobserve-wall",
    "rust-tclean-wall",
    "casa-tclean",
    "rust-plot",
    "casa-plot",
]:
    path = outdir / f"{name}-timing.json"
    if path.exists():
        timings[name] = read_json(path.name)

rust_imhead = read_json("rust-imhead.json")
casa_imhead = read_json("casa-imhead.json")
rust_imstat = read_json("rust-imstat.json")
casa_imstat = read_json("casa-imstat.json")
summary = {
    "inputs": {
        "casa_ms": str(casa_ms),
        "rust_ms": str(rust_ms),
        "casa_image": str(casa_image),
        "rust_image": str(rust_image),
    },
    "image_products": {
        "casa_header": {"shape": casa_header["shape"], "units": casa_header["units"]},
        "rust_header": {"shape": rust_header["shape"], "units": rust_header["units"]},
        "rust_minus_casa": image_compare,
    },
    "imhead": {
        "casa_shape": casa_imhead.get("shape"),
        "rust_shape": rust_imhead.get("shape"),
        "rust_units": rust_imhead.get("units"),
    },
    "imstat": compare_imstat(rust_imstat, casa_imstat),
    "plots": {
        "casa_plotms_png": str(casa_plot),
        "rust_msexplore_png": str(rust_plot),
        "side_by_side_png": str(outdir / "wave5-issue125-plot-panel.png"),
        "casa_plot_status": read_json("casa-plot-status.json"),
    },
    "panels": {
        "image_panel_png": str(outdir / "wave5-issue125-image-panel.png"),
    },
    "timings": timings,
    "invalid_ms_check": {
        "returncode": invalid.returncode,
        "stderr": invalid.stderr.strip().splitlines()[-5:],
        "passed": invalid.returncode != 0 and not invalid_prefix.with_suffix(".image").exists(),
    },
}
(outdir / "wave5-issue125-analysis-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY

echo "Wave 5 issue #125 artifacts written to $outdir"
