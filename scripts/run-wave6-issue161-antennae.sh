#!/usr/bin/env bash
set -euo pipefail

outdir="${1:-target/wave6-issue161-antennae}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
casa_py="${CASA_RS_CASA_PYTHON:-$HOME/SoftwareProjects/casa-build/venv/bin/python}"
fetch="${CASA_RS_FETCH_TUTORIAL_DATA:-0}"
skip_casa="${CASA_RS_WAVE6_ISSUE161_SKIP_CASA:-0}"
skip_imaging="${CASA_RS_WAVE6_ISSUE161_SKIP_IMAGING:-0}"
run_stamp="${CASA_RS_WAVE6_RUN_STAMP:-$(date '+%Y%m%dT%H%M%S%z')}"
generated_at="${CASA_RS_WAVE6_GENERATED_AT:-$(date '+%Y-%m-%d %H:%M:%S %z')}"

mkdir -p "$outdir"
timings_file="$outdir/wave6-issue161-timings.tsv"
if [[ "$skip_casa" != "1" && "$skip_imaging" != "1" ]]; then
  rm -f "$timings_file"
fi

if [[ ! -x "$casa_py" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
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

download_if_requested() {
  local url="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    if tar -tzf "$path" >/dev/null 2>&1; then
      return
    fi
    if [[ "$fetch" != "1" ]]; then
      echo "incomplete archive at $path; set CASA_RS_FETCH_TUTORIAL_DATA=1 to resume $url" >&2
      return 2
    fi
    echo "resuming incomplete archive at $path" >&2
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

antennae_dir="$tutorial_root/tutorial-parity/alma/antennae/band7"
calibrated_archive="$antennae_dir/Antennae_Band7_CalibratedData.tgz"
reference_archive="$antennae_dir/Antennae_Band7_ReferenceImages.tgz"
calibrated_data="$antennae_dir/Antennae_Band7_CalibratedData"
reference_data="$antennae_dir/Antennae_Band7_ReferenceImages"

download_if_requested \
  "https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_CalibratedData.tgz" \
  "$calibrated_archive"
download_if_requested \
  "https://bulk.cv.nrao.edu/almadata/public/casaguides/Antennae_Band7_6.6.6/Antennae_Band7_ReferenceImages.tgz" \
  "$reference_archive"
extract_if_needed "$calibrated_archive" "$calibrated_data" "$antennae_dir"
extract_if_needed "$reference_archive" "$reference_data" "$antennae_dir"

north_ms="$calibrated_data/Antennae_North.cal.ms"
south_ms="$calibrated_data/Antennae_South.cal.ms"
if [[ ! -d "$north_ms" || ! -d "$south_ms" ]]; then
  echo "missing Antennae calibrated MeasurementSets under $calibrated_data" >&2
  exit 2
fi

run_dir="$outdir/continuum"
line_dir="$outdir/line-cube"
mkdir -p "$run_dir"
mkdir -p "$line_dir"

if [[ "$skip_imaging" == "1" ]]; then
  echo "Skipping imaging because CASA_RS_WAVE6_ISSUE161_SKIP_IMAGING=1"
else
  if [[ "$skip_casa" != "1" ]]; then
    casa_start="$(now_seconds)"
    "$casa_py" - "$north_ms" "$south_ms" "$run_dir" <<'PY'
import shutil
import sys
from pathlib import Path

from casatasks import tclean

north_ms = sys.argv[1]
south_ms = sys.argv[2]
outdir = Path(sys.argv[3])

def clear_products(prefix):
    for suffix in [".image", ".model", ".image.pbcor", ".psf", ".residual", ".pb", ".sumwt", ".weight", ".mask"]:
        shutil.rmtree(str(prefix) + suffix, ignore_errors=True)

jobs = [
    {
        "vis": north_ms,
        "prefix": outdir / "casa-antennae-north-cont-dirty",
        "phasecenter": 12,
        "spw": "0:1~50;120~164",
        "imsize": 300,
        "cell": "0.2arcsec",
        "niter": 0,
        "threshold": "0Jy",
    },
    {
        "vis": north_ms,
        "prefix": outdir / "casa-antennae-north-cont-clean",
        "phasecenter": 12,
        "spw": "0:1~50;120~164",
        "imsize": 500,
        "cell": "0.13arcsec",
        "niter": 32,
        "threshold": "0.4mJy",
    },
    {
        "vis": south_ms,
        "prefix": outdir / "casa-antennae-south-cont-clean",
        "phasecenter": 15,
        "spw": "0:1~30;120~164",
        "imsize": 750,
        "cell": "0.13arcsec",
        "niter": 32,
        "threshold": "0.4mJy",
    },
]

for job in jobs:
    clear_products(job["prefix"])
    tclean(
        vis=job["vis"],
        imagename=str(job["prefix"]),
        field="",
        phasecenter=job["phasecenter"],
        deconvolver="hogbom",
        specmode="mfs",
        restfreq="345.79599GHz",
        spw=job["spw"],
        gridder="mosaic",
        datacolumn="data",
        mosweight=True,
        imsize=job["imsize"],
        cell=job["cell"],
        interactive=False,
        niter=job["niter"],
        threshold=job["threshold"],
    )
PY
    record_timing casa_continuum_seconds "$casa_start"

    casa_line_start="$(now_seconds)"
    "$casa_py" - "$north_ms" "$line_dir" <<'PY'
import shutil
import sys
from pathlib import Path

from casatasks import tclean

north_ms = sys.argv[1]
outdir = Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)
prefix = outdir / "casa-antennae-north-line-dirty-probe"

for suffix in [".image", ".model", ".image.pbcor", ".psf", ".residual", ".pb", ".sumwt", ".weight", ".mask"]:
    shutil.rmtree(str(prefix) + suffix, ignore_errors=True)

tclean(
    vis=north_ms,
    imagename=str(prefix),
    field="",
    phasecenter=12,
    deconvolver="hogbom",
    specmode="cube",
    spw="0",
    nchan=2,
    start=120,
    width=1,
    gridder="mosaic",
    datacolumn="data",
    mosweight=True,
    perchanweightdensity=True,
    imsize=64,
    cell="0.13arcsec",
    interactive=False,
    niter=0,
    threshold="0Jy",
    pblimit=0.2,
)
PY
    record_timing casa_line_cube_probe_seconds "$casa_line_start"
  else
    echo "Skipping CASA C++ continuum run because CASA_RS_WAVE6_ISSUE161_SKIP_CASA=1"
  fi

  rust_start="$(now_seconds)"
  rm -rf \
    "$run_dir/rust-antennae-north-cont-dirty".* \
    "$run_dir/rust-antennae-north-cont-clean".* \
    "$run_dir/rust-antennae-south-cont-clean".*
  run_casars_imager \
    --ms "$north_ms" \
    --imagename "$run_dir/rust-antennae-north-cont-dirty" \
    --imsize 300 \
    --cell-arcsec 0.2 \
    --phasecenter-field 12 \
    --spw "0:1~50;120~164" \
    --datacolumn DATA \
    --deconvolver hogbom \
    --niter 0 \
    --threshold-jy 0 \
    --dirty-only \
    --no-preview-pngs
  run_casars_imager \
    --ms "$north_ms" \
    --imagename "$run_dir/rust-antennae-north-cont-clean" \
    --imsize 500 \
    --cell-arcsec 0.13 \
    --phasecenter-field 12 \
    --spw "0:1~50;120~164" \
    --datacolumn DATA \
    --deconvolver hogbom \
    --niter 32 \
    --minor-cycle-length 32 \
    --casa-hogbom-iterations \
    --gain 0.1 \
    --threshold-jy 0.0004 \
    --no-preview-pngs
  run_casars_imager \
    --ms "$south_ms" \
    --imagename "$run_dir/rust-antennae-south-cont-clean" \
    --imsize 750 \
    --cell-arcsec 0.13 \
    --phasecenter-field 15 \
    --spw "0:1~30;120~164" \
    --datacolumn DATA \
    --deconvolver hogbom \
    --niter 32 \
    --minor-cycle-length 32 \
    --casa-hogbom-iterations \
    --gain 0.1 \
    --threshold-jy 0.0004 \
    --no-preview-pngs
  record_timing rust_continuum_seconds "$rust_start"

  rust_line_start="$(now_seconds)"
  rm -rf "$line_dir/rust-antennae-north-line-dirty-probe".*
  run_casars_imager \
    --ms "$north_ms" \
    --imagename "$line_dir/rust-antennae-north-line-dirty-probe" \
    --imsize 64 \
    --cell-arcsec 0.13 \
    --phasecenter-field 12 \
    --spw "0" \
    --channel-start 120 \
    --channel-count 2 \
    --datacolumn DATA \
    --specmode cube \
    --deconvolver hogbom \
    --niter 0 \
    --dirty-only \
    --threshold-jy 0 \
    --perchanweightdensity \
    --pblimit 0.2 \
    --no-preview-pngs
  record_timing rust_line_cube_probe_seconds "$rust_line_start"
fi

"$casa_py" - "$outdir" "$run_stamp" "$generated_at" <<'PY'
import json
import sys
from pathlib import Path
from urllib.request import urlretrieve

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image as image_tool

outdir = Path(sys.argv[1])
run_stamp = sys.argv[2]
generated_at = sys.argv[3]
run_dir = outdir / "continuum"
line_dir = outdir / "line-cube"
timings_path = outdir / "wave6-issue161-timings.tsv"

originals = {
    "north_dirty": (
        "https://casaguides.nrao.edu/images/a/ae/Antennae_North.Cont.Dirty.image.CARTA.png",
        outdir / "original-antennae-north-cont-dirty-image.png",
    ),
    "north_residual": (
        "https://casaguides.nrao.edu/images/9/98/Antennae_North.Cont.Clean.residual.tclean.png",
        outdir / "original-antennae-north-cont-clean-residual.png",
    ),
    "south_residual": (
        "https://casaguides.nrao.edu/images/8/8b/Antennae_South.Cont.Clean.residual.tclean.png",
        outdir / "original-antennae-south-cont-clean-residual.png",
    ),
    "continuum_image": (
        "https://casaguides.nrao.edu/images/4/44/Antennae_North.Cont.Clean.image-Antennae_South.Cont.Clean.image.png",
        outdir / "original-antennae-north-south-cont-clean-image.png",
    ),
    "north_line_clean": (
        "https://casaguides.nrao.edu/images/b/bd/Antennae_North.CO3_2Line.Clean.image.png",
        outdir / "original-antennae-north-co32-line-clean-image.png",
    ),
}
for url, path in originals.values():
    if not path.exists():
        urlretrieve(url, path)

def read_timings():
    timings = {}
    if timings_path.exists():
        for line in timings_path.read_text(encoding="utf-8").splitlines():
            parts = line.split("\t")
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

def pct(value):
    return "n/a" if value is None or not np.isfinite(value) else f"{100.0 * value:.3f}%"

def sci(value):
    return "n/a" if value is None or not np.isfinite(value) else f"{value:.3e}"

def compare_panel(key, title, original_key, casa_prefix, rust_prefix, product, colorbar_label="Jy/beam"):
    casa, casa_mask = read_image(str(casa_prefix) + product)
    rust, rust_mask = read_image(str(rust_prefix) + product)
    casa, rust, casa_mask, rust_mask = crop_common(casa, rust, casa_mask, rust_mask)
    shared = casa_mask & rust_mask & np.isfinite(casa) & np.isfinite(rust)
    diff = rust - casa
    valid_values = np.concatenate([casa[shared], rust[shared]]) if shared.any() else np.concatenate([casa.ravel(), rust.ravel()])
    vmin, vmax = robust_limits(valid_values)
    valid_diff = diff[shared]
    dpeak = np.nanpercentile(np.abs(valid_diff), 99) if valid_diff.size else 1.0
    dpeak = float(max(dpeak, 1.0e-12))
    image_cmap = plt.get_cmap("inferno").copy()
    image_cmap.set_bad("#d9d9d9")
    diff_cmap = plt.get_cmap("coolwarm").copy()
    diff_cmap.set_bad("#d9d9d9")
    fig, axes = plt.subplots(1, 4, figsize=(17.5, 5.4), constrained_layout=True)
    axes[0].imshow(plt.imread(originals[original_key][1]))
    axes[0].set_title("CASA Guide figure")
    artists = [
        axes[1].imshow(np.ma.array(casa, mask=~(casa_mask & np.isfinite(casa))), origin="lower", cmap=image_cmap, vmin=vmin, vmax=vmax),
        axes[2].imshow(np.ma.array(rust, mask=~(rust_mask & np.isfinite(rust))), origin="lower", cmap=image_cmap, vmin=vmin, vmax=vmax),
        axes[3].imshow(np.ma.array(diff, mask=~shared), origin="lower", cmap=diff_cmap, vmin=-dpeak, vmax=dpeak),
    ]
    axes[1].set_title("CASA C++ " + product)
    axes[2].set_title("casa-rs " + product)
    axes[3].set_title("casa-rs - CASA")
    for ax, artist in zip(axes[1:], artists):
        colorbar = fig.colorbar(artist, ax=ax, fraction=0.046, pad=0.02)
        colorbar.set_label(colorbar_label)
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    valid_count = int(np.count_nonzero(shared))
    total_count = int(shared.size)
    casa_rms = float(np.sqrt(np.nanmean(casa[shared] * casa[shared]))) if valid_count else float("nan")
    diff_rms = float(np.sqrt(np.nanmean(valid_diff * valid_diff))) if valid_count else float("nan")
    casa_peak = float(np.nanmax(np.abs(casa[shared]))) if valid_count else float("nan")
    source = shared & (np.abs(casa) >= 0.25 * casa_peak) if valid_count else shared
    source_abs = np.abs(diff[source])
    source_p90 = float(np.nanpercentile(source_abs, 90) / casa_peak) if source_abs.size and casa_peak > 0 else float("nan")
    source_max = float(np.nanmax(source_abs) / casa_peak) if source_abs.size and casa_peak > 0 else float("nan")
    rust_only = int(np.count_nonzero(rust_mask & ~casa_mask))
    casa_only = int(np.count_nonzero(casa_mask & ~rust_mask))
    fig.suptitle(title)
    fig.text(
        0.5,
        0.01,
        f"generated={run_stamp}; shared valid={valid_count}/{total_count}; native mask mismatch rust-only={rust_only}, casa-only={casa_only}\n"
        f"CASA RMS={sci(casa_rms)}, diff RMS={sci(diff_rms)}, RMS diff/CASA={pct(diff_rms / abs(casa_rms) if abs(casa_rms) > 0 else float('nan'))}; "
        f"source p90|diff|/peak={pct(source_p90)}, max|diff|/peak={pct(source_max)}",
        ha="center",
        va="bottom",
        fontsize=7.8,
    )
    panel = outdir / f"{key}-{product.strip('.')}-{run_stamp}-panel.png"
    latest = outdir / f"{key}-{product.strip('.')}-panel.png"
    fig.savefig(panel, dpi=150)
    fig.savefig(latest, dpi=150)
    plt.close(fig)
    return {
        "panel_png": str(panel),
        "latest_panel_png": str(latest),
        "generated_stamp": run_stamp,
        "generated_at": generated_at,
        "shared_valid_pixels": valid_count,
        "total_pixels": total_count,
        "casa_valid_pixels": int(np.count_nonzero(casa_mask)),
        "rust_valid_pixels": int(np.count_nonzero(rust_mask)),
        "rust_only_valid_pixels": rust_only,
        "casa_only_valid_pixels": casa_only,
        "casa_rms": casa_rms,
        "diff_rms": diff_rms,
        "diff_rms_over_casa_rms": float(diff_rms / abs(casa_rms)) if abs(casa_rms) > 0 else None,
        "casa_peak_abs": casa_peak,
        "source25_diff_p90_over_casa_peak": source_p90,
        "source25_diff_max_over_casa_peak": source_max,
    }

summary = {
    "generated_stamp": run_stamp,
    "generated_at": generated_at,
    "timings": read_timings(),
    "continuum": {},
    "line_cube": {},
    "line_cube_status": {
        "status": "partial_mosaic_dirty_probe",
        "reason": "This run now includes a bounded North two-channel dirty line-cube probe with specmode='cube', gridder='mosaic', per-channel weight density, PB products, and CASA C++ / casa-rs image panels. It is not the full tutorial line-clean/selfcal workflow because restoringbeam='common', savemodel='modelcolumn', interactive clean masks, selfcal, moments, contours, and FITS exports are still not claimed.",
    },
}
summary["continuum"]["north_dirty_image"] = compare_panel(
    "alma-antennae-north-cont-dirty",
    "Antennae North continuum dirty mosaic image",
    "north_dirty",
    run_dir / "casa-antennae-north-cont-dirty",
    run_dir / "rust-antennae-north-cont-dirty",
    ".image",
)
summary["continuum"]["north_clean_residual"] = compare_panel(
    "alma-antennae-north-cont-clean",
    "Antennae North continuum clean residual",
    "north_residual",
    run_dir / "casa-antennae-north-cont-clean",
    run_dir / "rust-antennae-north-cont-clean",
    ".residual",
)
summary["continuum"]["north_clean_image"] = compare_panel(
    "alma-antennae-north-cont-clean",
    "Antennae North continuum clean restored image",
    "continuum_image",
    run_dir / "casa-antennae-north-cont-clean",
    run_dir / "rust-antennae-north-cont-clean",
    ".image",
)
summary["continuum"]["south_clean_residual"] = compare_panel(
    "alma-antennae-south-cont-clean",
    "Antennae South continuum clean residual",
    "south_residual",
    run_dir / "casa-antennae-south-cont-clean",
    run_dir / "rust-antennae-south-cont-clean",
    ".residual",
)
summary["continuum"]["south_clean_image"] = compare_panel(
    "alma-antennae-south-cont-clean",
    "Antennae South continuum clean restored image",
    "continuum_image",
    run_dir / "casa-antennae-south-cont-clean",
    run_dir / "rust-antennae-south-cont-clean",
    ".image",
)
summary["line_cube"]["north_dirty_probe_image_channel0"] = compare_panel(
    "alma-antennae-north-line-dirty-probe",
    "Antennae North CO(3-2) line cube dirty mosaic probe, channel 0",
    "north_line_clean",
    line_dir / "casa-antennae-north-line-dirty-probe",
    line_dir / "rust-antennae-north-line-dirty-probe",
    ".image",
)
summary["line_cube"]["north_dirty_probe_pb_channel0"] = compare_panel(
    "alma-antennae-north-line-dirty-probe",
    "Antennae North CO(3-2) line cube primary beam probe, channel 0",
    "north_line_clean",
    line_dir / "casa-antennae-north-line-dirty-probe",
    line_dir / "rust-antennae-north-line-dirty-probe",
    ".pb",
    "primary beam",
)
(outdir / "wave6-issue161-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY

cat > "$outdir/README.md" <<EOF
# Wave 6 Issue 161 Antennae Band 7 Artifacts

Generated by \`scripts/run-wave6-issue161-antennae.sh\`.

Generated at: \`$generated_at\`

Run stamp: \`$run_stamp\`

This artifact set is bounded to #161. It regenerates same-input CASA C++ and
casa-rs continuum mosaic products for the Antennae North/South tutorial data,
then writes stamped CASA Guide / CASA C++ / casa-rs / difference panels.

This run also includes a bounded North two-channel dirty line-cube mosaic probe
with PB products. Full line CLEAN, model-column selfcal, moment, contour, and
FITS products are inventoried but not claimed yet.

Summary JSON: \`wave6-issue161-summary.json\`
EOF

echo "Wrote Wave 6 #161 Antennae artifacts under $outdir"
