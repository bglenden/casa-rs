#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  outdir="$1"
else
  outdir="target/wave6-issue163-m100-combine-$(date -u +%Y%m%dT%H%M%SZ)"
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
casa_python="${CASA_RS_CASA_PYTHON:-/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
dataset_dir="$tutorial_root/tutorial-parity/alma/m100/band3-combine"
archive="$dataset_dir/M100_Band3_DataComb_ReferenceImages_5.1.tgz"
extract_root="$dataset_dir/extracted"
input_root="$extract_root/M100_Band3_DataComb_ReferenceImages_5.1"
raw_extract_root="$dataset_dir/raw/extracted"
tp_image="$(find "$raw_extract_root" -name M100_TP_CO_cube.spw3.image.bl -print -quit 2>/dev/null || true)"

if [[ ! -x "$casa_python" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 1
fi

mkdir -p "$outdir" "$extract_root"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"

if [[ ! -f "$archive" ]]; then
  if [[ "${CASA_RS_FETCH_TUTORIAL_DATA:-0}" != "1" ]]; then
    cat >&2 <<EOF
Missing $archive
Set CASA_RS_FETCH_TUTORIAL_DATA=1 to fetch the M100 reference-image archive,
or stage it under CASA_RS_TUTORIAL_DATA_ROOT using the registry layout.
EOF
    exit 1
  fi
  mkdir -p "$dataset_dir"
  curl --fail --location --continue-at - \
    --output "$archive" \
    "https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_DataComb_ReferenceImages_5.1.tgz"
fi

expected_sha="04e3e88f1393e93c18eab7fd4a9ae5c57e768dbb8be85259c3006ae9d4c7634b"
actual_sha="$(shasum -a 256 "$archive" | awk '{print $1}')"
if [[ "$actual_sha" != "$expected_sha" ]]; then
  echo "Archive checksum mismatch for $archive" >&2
  echo "expected $expected_sha" >&2
  echo "actual   $actual_sha" >&2
  exit 1
fi

if [[ ! -d "$input_root/M100_combine_CO_cube.image" ]]; then
  tar -xzf "$archive" -C "$extract_root"
fi
if [[ -z "$tp_image" || ! -d "$tp_image" ]]; then
  cat >&2 <<EOF
Missing TP image M100_TP_CO_cube.spw3.image.bl under $raw_extract_root
Stage the M100 ACA reference/TP raw products before running native imregrid+feather parity.
EOF
  exit 1
fi

cargo build -p casa-images --bin imsubimage --bin immath --bin imregrid --bin feather --bin immoments --bin exportfits

rust_bin="$repo_root/target/debug"
rm -rf "$outdir/casa" "$outdir/rust"
mkdir -p "$outdir/casa" "$outdir/rust"

"$casa_python" - "$input_root" "$tp_image" "$outdir/casa" <<'PY'
from pathlib import Path
import sys
import json
from casatasks import exportfits, feather, immath, immoments, imregrid, imstat, imsubimage

root = Path(sys.argv[1])
tp_image = Path(sys.argv[2])
out = Path(sys.argv[3])

imsubimage(
    imagename=str(root / "M100_combine_CO_cube.pb"),
    outfile=str(out / "M100_combine_CO_cube.pb.subim"),
    box="219,148,612,579",
    overwrite=True,
)
imsubimage(
    imagename=str(root / "M100_combine_CO_cube.pb"),
    outfile=str(out / "M100_combine_CO_cube.pb.1ch"),
    chans="35",
    overwrite=True,
)
imregrid(
    imagename=str(tp_image),
    template=str(root / "M100_combine_CO_cube.image"),
    output=str(out / "M100_TP_CO_cube.regrid"),
    axes=[0, 1, 3],
    interpolation="linear",
    overwrite=True,
)
imsubimage(
    imagename=str(out / "M100_TP_CO_cube.regrid"),
    outfile=str(out / "M100_TP_CO_cube.regrid.subim"),
    box="219,148,612,579",
    overwrite=True,
)
imsubimage(
    imagename=str(root / "M100_combine_CO_cube.image"),
    outfile=str(out / "M100_combine_CO_cube.image.subim"),
    box="219,148,612,579",
    overwrite=True,
)
immath(
    imagename=[
        str(out / "M100_TP_CO_cube.regrid.subim"),
        str(out / "M100_combine_CO_cube.pb.subim"),
    ],
    expr="IM0 * IM1",
    outfile=str(out / "M100_TP_CO_cube.regrid.subim.pbweighted"),
)
feather(
    imagename=str(out / "M100_Feather_CO.image"),
    highres=str(out / "M100_combine_CO_cube.image.subim"),
    lowres=str(out / "M100_TP_CO_cube.regrid.subim.pbweighted"),
)
chanstat = imstat(imagename=str(root / "M100_combine_CO_cube.image"), chans="4")
rms1 = chanstat["rms"][0]
chanstat = imstat(imagename=str(root / "M100_combine_CO_cube.image"), chans="66")
rms2 = chanstat["rms"][0]
rms = 0.5 * (rms1 + rms2)
mask_expr = f'"{root / "M100_combine_CO_cube.pb"}">0.3'
immoments(
    imagename=str(root / "M100_combine_CO_cube.image"),
    moments=[0],
    axis="spectral",
    chans="9~61",
    mask=mask_expr,
    includepix=[rms * 2.0, 100.0],
    outfile=str(out / "M100_combine_CO_cube.image.mom0"),
)
immoments(
    imagename=str(root / "M100_combine_CO_cube.image"),
    moments=[1],
    axis="spectral",
    chans="9~61",
    mask=mask_expr,
    includepix=[rms * 5.5, 100.0],
    outfile=str(out / "M100_combine_CO_cube.image.mom1"),
)
immath(
    imagename=[
        str(out / "M100_combine_CO_cube.image.mom0"),
        str(out / "M100_combine_CO_cube.pb.1ch"),
    ],
    expr="IM0 / IM1",
    outfile=str(out / "M100_combine_CO_cube.image.mom0.pbcor"),
)
immath(
    imagename=[
        str(out / "M100_Feather_CO.image"),
        str(out / "M100_combine_CO_cube.pb.subim"),
    ],
    expr="IM0 / IM1",
    outfile=str(out / "M100_Feather_CO.image.pbcor"),
)
immoments(
    imagename=str(out / "M100_Feather_CO.image"),
    moments=[0],
    chans="10~61",
    outfile=str(out / "M100_Feather_CO.image.mom0"),
)
immoments(
    imagename=str(out / "M100_Feather_CO.image"),
    moments=[1],
    chans="10~61",
    outfile=str(out / "M100_Feather_CO.image.mom1"),
)
exportfits(
    imagename=str(out / "M100_Feather_CO.image.pbcor"),
    fitsimage=str(out / "M100_Feather_CO.image.pbcor.fits"),
    overwrite=True,
)
(out / "thresholds.json").write_text(
    json.dumps({"combine_rms": rms, "combine_rms_ch4": rms1, "combine_rms_ch66": rms2}, indent=2)
)
PY

combine_rms="$("$casa_python" - "$outdir/casa/thresholds.json" <<'PY'
import json
import sys
from pathlib import Path
print(json.loads(Path(sys.argv[1]).read_text())["combine_rms"])
PY
)"

"$rust_bin/imsubimage" \
  "$input_root/M100_combine_CO_cube.pb" \
  "$outdir/rust/M100_combine_CO_cube.pb.subim" \
  --box 219,148,612,579 \
  --overwrite >"$outdir/rust/imsubimage-result.json"

"$rust_bin/imsubimage" \
  "$input_root/M100_combine_CO_cube.pb" \
  "$outdir/rust/M100_combine_CO_cube.pb.1ch" \
  --chans 35 \
  --overwrite >"$outdir/rust/imsubimage-pb1ch-result.json"

"$rust_bin/imregrid" \
  --imagename "$tp_image" \
  --template "$input_root/M100_combine_CO_cube.image" \
  --output "$outdir/rust/M100_TP_CO_cube.regrid" \
  --interpolation linear \
  --overwrite >"$outdir/rust/imregrid-tp-result.json"

"$rust_bin/imsubimage" \
  "$outdir/rust/M100_TP_CO_cube.regrid" \
  "$outdir/rust/M100_TP_CO_cube.regrid.subim" \
  --box 219,148,612,579 \
  --overwrite >"$outdir/rust/imsubimage-tp-result.json"

"$rust_bin/imsubimage" \
  "$input_root/M100_combine_CO_cube.image" \
  "$outdir/rust/M100_combine_CO_cube.image.subim" \
  --box 219,148,612,579 \
  --overwrite >"$outdir/rust/imsubimage-combine-result.json"

"$rust_bin/immath" \
  --imagename "$outdir/rust/M100_TP_CO_cube.regrid.subim" \
  --imagename "$outdir/rust/M100_combine_CO_cube.pb.subim" \
  --expr "IM0 * IM1" \
  --outfile "$outdir/rust/M100_TP_CO_cube.regrid.subim.pbweighted" \
  --overwrite >"$outdir/rust/immath-tp-pbweighted-result.json"

"$rust_bin/feather" \
  --imagename "$outdir/rust/M100_Feather_CO.image" \
  --highres "$outdir/rust/M100_combine_CO_cube.image.subim" \
  --lowres "$outdir/rust/M100_TP_CO_cube.regrid.subim.pbweighted" \
  --overwrite >"$outdir/rust/feather-result.json"

"$rust_bin/immoments" \
  "$input_root/M100_combine_CO_cube.image" \
  --outfile "$outdir/rust/M100_combine_CO_cube.image.mom0" \
  --moments 0 \
  --chans 9~61 \
  --mask "\"$input_root/M100_combine_CO_cube.pb\">0.3" \
  --includepix "$(python3 - <<PY
print(float("$combine_rms") * 2.0)
PY
),100.0" \
  --overwrite >"$outdir/rust/immoments-combine-mom0-result.json"

"$rust_bin/immoments" \
  "$input_root/M100_combine_CO_cube.image" \
  --outfile "$outdir/rust/M100_combine_CO_cube.image.mom1" \
  --moments 1 \
  --chans 9~61 \
  --mask "\"$input_root/M100_combine_CO_cube.pb\">0.3" \
  --includepix "$(python3 - <<PY
print(float("$combine_rms") * 5.5)
PY
),100.0" \
  --overwrite >"$outdir/rust/immoments-combine-mom1-result.json"

"$rust_bin/immath" \
  --imagename "$outdir/rust/M100_combine_CO_cube.image.mom0" \
  --imagename "$outdir/rust/M100_combine_CO_cube.pb.1ch" \
  --expr "IM0 / IM1" \
  --outfile "$outdir/rust/M100_combine_CO_cube.image.mom0.pbcor" \
  --overwrite >"$outdir/rust/immath-combine-mom0-pbcor-result.json"

"$rust_bin/immath" \
  --imagename "$outdir/rust/M100_Feather_CO.image" \
  --imagename "$outdir/rust/M100_combine_CO_cube.pb.subim" \
  --expr "IM0 / IM1" \
  --outfile "$outdir/rust/M100_Feather_CO.image.pbcor" \
  --overwrite >"$outdir/rust/immath-result.json"

"$rust_bin/immoments" \
  "$outdir/rust/M100_Feather_CO.image" \
  --outfile "$outdir/rust/M100_Feather_CO.image.mom0" \
  --moments 0 \
  --chans 10~61 \
  --overwrite >"$outdir/rust/immoments-mom0-result.json"

"$rust_bin/immoments" \
  "$outdir/rust/M100_Feather_CO.image" \
  --outfile "$outdir/rust/M100_Feather_CO.image.mom1" \
  --moments 1 \
  --chans 10~61 \
  --overwrite >"$outdir/rust/immoments-mom1-result.json"

"$rust_bin/exportfits" \
  "$outdir/rust/M100_Feather_CO.image.pbcor" \
  "$outdir/rust/M100_Feather_CO.image.pbcor.fits" \
  --overwrite >"$outdir/rust/exportfits-result.json"

"$casa_python" - "$input_root" "$outdir" <<'PY'
from __future__ import annotations

import json
import math
import sys
import urllib.request
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image as image_tool

root = Path(sys.argv[1])
outdir = Path(sys.argv[2])
casa_dir = outdir / "casa"
rust_dir = outdir / "rust"
guide_dir = outdir / "guide-originals"
guide_dir.mkdir(parents=True, exist_ok=True)

guide_images = {
    "pb_subim": "https://casaguides.nrao.edu/images/5/57/M100_combine_CO_cube.pb.subim.png",
    "combine_mom0_pbcor": "https://casaguides.nrao.edu/index.php/Special:Redirect/file/M100_combine_CO_cube.image.mom0.pbcor_6.5.4.png",
    "combine_mom1": "https://casaguides.nrao.edu/index.php/Special:Redirect/file/M100_combine_CO_cube.image.mom1_6.5.4.png",
    "feather_chan26": "https://casaguides.nrao.edu/images/d/dd/M100_Feather_CO_chan26.png",
    "feather_mom0": "https://casaguides.nrao.edu/images/4/43/M100_Feather_CO.image.mom0.pbcor_6.5.4.png",
    "feather_mom1": "https://casaguides.nrao.edu/images/e/e8/M100_Feather_CO.image.mom1_6.5.4.png",
}
for key, url in guide_images.items():
    target = guide_dir / f"{key}.png"
    if not target.exists():
        cached = next(outdir.parent.glob(f"wave6-issue163-*/guide-originals/{key}.png"), None)
        if cached is not None:
            target.write_bytes(cached.read_bytes())
            continue
        try:
            urllib.request.urlretrieve(url, target)
        except Exception as error:
            print(f"warning: could not fetch CASA Guide image {key}: {error}", file=sys.stderr)

def read_image(path: Path):
    ia = image_tool()
    ia.open(str(path))
    try:
        data = ia.getchunk()
        mask = ia.getchunk(getmask=True)
    finally:
        ia.close()
    return data, mask

def plane(data: np.ndarray, selector: tuple[int, ...] | None):
    if selector is None:
        squeezed = np.squeeze(data)
        return squeezed
    return np.squeeze(data[selector])

def compare_product(name, title, original_key, product, selector=None, units=""):
    casa_data, casa_mask = read_image(casa_dir / product)
    rust_data, rust_mask = read_image(rust_dir / product)
    shared = casa_mask & rust_mask & np.isfinite(casa_data) & np.isfinite(rust_data)
    diff = rust_data[shared] - casa_data[shared]
    peak = max(float(np.nanmax(np.abs(casa_data[shared]))), 1e-30)
    abs_diff = np.abs(diff)
    metrics = {
        "product": product,
        "panel": str(outdir / f"{name}-panel.png"),
        "shape_casa": list(casa_data.shape),
        "shape_rust": list(rust_data.shape),
        "valid_casa": int(casa_mask.sum()),
        "valid_rust": int(rust_mask.sum()),
        "shared_pixels": int(shared.sum()),
        "mask_mismatch_pixels": int(np.count_nonzero(casa_mask != rust_mask)),
        "max_abs": float(abs_diff.max()) if abs_diff.size else None,
        "p99_abs": float(np.percentile(abs_diff, 99)) if abs_diff.size else None,
        "max_frac_peak": float(abs_diff.max() / peak) if abs_diff.size else None,
        "p99_frac_peak": float(np.percentile(abs_diff, 99) / peak) if abs_diff.size else None,
        "peak_abs_casa": peak,
        "units": units,
    }

    casa_plane = np.ma.array(
        plane(casa_data, selector),
        mask=~plane(casa_mask, selector).astype(bool),
    )
    rust_plane = np.ma.array(
        plane(rust_data, selector),
        mask=~plane(rust_mask, selector).astype(bool),
    )
    diff_plane = np.ma.array(rust_plane - casa_plane, mask=casa_plane.mask | rust_plane.mask)
    vmin = float(np.nanpercentile(casa_plane.compressed(), 1))
    vmax = float(np.nanpercentile(casa_plane.compressed(), 99))
    dmax = max(float(np.nanpercentile(np.abs(diff_plane.compressed()), 99)), 1e-12)

    fig, axes = plt.subplots(1, 4, figsize=(16, 4.6), constrained_layout=True)
    axes[0].imshow(plt.imread(guide_dir / f"{original_key}.png"))
    axes[0].set_title("CASA Guide figure")
    image_artist = axes[1].imshow(casa_plane.T, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax)
    axes[1].set_title("CASA C++ same input")
    rust_artist = axes[2].imshow(rust_plane.T, origin="lower", cmap="inferno", vmin=vmin, vmax=vmax)
    axes[2].set_title("casa-rs same input")
    diff_artist = axes[3].imshow(diff_plane.T, origin="lower", cmap="RdBu_r", vmin=-dmax, vmax=dmax)
    axes[3].set_title("casa-rs - CASA")
    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    label = units or "pixel value"
    fig.colorbar(image_artist, ax=axes[1], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(rust_artist, ax=axes[2], fraction=0.046, pad=0.04, label=label)
    fig.colorbar(diff_artist, ax=axes[3], fraction=0.046, pad=0.04, label=f"delta {label}")
    fig.suptitle(title)
    fig.savefig(metrics["panel"], dpi=140)
    plt.close(fig)
    return metrics

summary = {
    "issue": 163,
    "dataset": "alma/m100/band3-combine/reference-images",
    "source_page": "https://casaguides.nrao.edu/index.php/M100_Band3_Combine",
    "archive": str(root.parent.parent / "M100_Band3_DataComb_ReferenceImages_5.1.tgz"),
    "products": {},
    "deferred_products": {
        "raw_12m_7m_tclean": "Requires closing the native cube imaging path from split 12m+7m calibrated MeasurementSets; this runner now exercises native TP imregrid and feather from the official tutorial image products.",
    },
}

summary["products"]["pb_subim"] = compare_product(
    "pb-subim",
    "M100 PB subimage from tutorial box 219,148,612,579",
    "pb_subim",
    "M100_combine_CO_cube.pb.subim",
    selector=(slice(None), slice(None), 0, 35),
)
summary["products"]["combine_mom0_pbcor"] = compare_product(
    "combine-mom0-pbcor",
    "M100 12m+7m moment 0 PB-corrected, chans 9~61, PB>0.3",
    "combine_mom0_pbcor",
    "M100_combine_CO_cube.image.mom0.pbcor",
    units="Jy/beam km/s",
)
summary["products"]["combine_mom1"] = compare_product(
    "combine-mom1",
    "M100 12m+7m moment 1, chans 9~61, PB>0.3",
    "combine_mom1",
    "M100_combine_CO_cube.image.mom1",
    units="km/s",
)
summary["products"]["feather_pbcor_chan26"] = compare_product(
    "feather-pbcor-chan26",
    "M100 feathered PB-corrected cube, channel 26",
    "feather_chan26",
    "M100_Feather_CO.image.pbcor",
    selector=(slice(None), slice(None), 0, 26),
    units="Jy/beam",
)
summary["products"]["tp_regrid"] = compare_product(
    "tp-regrid",
    "M100 TP cube regridded to the combined cube template",
    "feather_chan26",
    "M100_TP_CO_cube.regrid",
    selector=(slice(None), slice(None), 0, 26),
    units="K",
)
summary["products"]["feather_cube"] = compare_product(
    "feather-cube",
    "M100 native feathered cube, channel 26",
    "feather_chan26",
    "M100_Feather_CO.image",
    selector=(slice(None), slice(None), 0, 26),
    units="Jy/beam",
)
summary["products"]["feather_mom0"] = compare_product(
    "feather-mom0",
    "M100 feathered moment 0, chans 10~61",
    "feather_mom0",
    "M100_Feather_CO.image.mom0",
    selector=(slice(None), slice(None), 0, 0),
    units="Jy/beam km/s",
)
summary["products"]["feather_mom1"] = compare_product(
    "feather-mom1",
    "M100 feathered moment 1, chans 10~61",
    "feather_mom1",
    "M100_Feather_CO.image.mom1",
    selector=(slice(None), slice(None), 0, 0),
    units="km/s",
)

for side in ("casa", "rust"):
    fits_path = outdir / side / "M100_Feather_CO.image.pbcor.fits"
    summary[f"{side}_exportfits"] = {
        "path": str(fits_path),
        "bytes": fits_path.stat().st_size,
    }

(outdir / "wave6-issue163-summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

cat >"$outdir/README.md" <<EOF
# Wave 6 #163 M100 Band 3 Combine Evidence

Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

This run uses the official M100 Band 3 data-combination reference-image archive
from the ALMA science-verification site and writes side-by-side CASA Guide /
CASA C++ / casa-rs / difference panels.

Summary JSON: \`wave6-issue163-summary.json\`
EOF

echo "Wrote Wave 6 #163 M100 evidence under $outdir"
