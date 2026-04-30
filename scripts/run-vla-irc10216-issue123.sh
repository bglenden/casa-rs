#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

outdir="${1:-target/wdad-wave4-123}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"
mkdir -p "$MPLCONFIGDIR"

if [[ -z "${CASA_RS_CASA_PYTHON:-}" || ! -x "$CASA_RS_CASA_PYTHON" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 2
fi

issue122_dir="${CASA_RS_ISSUE122_ARTIFACTS:-target/wdad-wave4-122-middlefreq}"
if [[ ! -d "$issue122_dir/TDRW0001_10s.ms" || ! -d "$issue122_dir/casa-priorcal" || ! -d "$issue122_dir/rust-priorcal" ]]; then
  scripts/run-vla-irc10216-issue122.sh "$issue122_dir"
fi
issue122_dir="$(cd "$issue122_dir" && pwd)"

cargo build --release -q \
  -p casa-calibration --bin calibrate \
  -p casa-ms --bin mstransform \
  -p casars-imager --bin casars-imager \
  -p casa-images --bin imexplore --bin immoments --bin impv

rm -rf \
  "$outdir/casa-target-prior.ms" \
  "$outdir/rust-target-prior.ms" \
  "$outdir/casa-transform.ms" \
  "$outdir/rust-transform.ms" \
  "$outdir/casa-contsub.ms" \
  "$outdir/rust-contsub.ms" \
  "$outdir/casa-HC3N-natural.image" \
  "$outdir/casa-HC3N-natural.model" \
  "$outdir/casa-HC3N-natural.psf" \
  "$outdir/casa-HC3N-natural.residual" \
  "$outdir/casa-HC3N-natural.sumwt" \
  "$outdir/rust-HC3N-natural.image" \
  "$outdir/rust-HC3N-natural.model" \
  "$outdir/rust-HC3N-natural.psf" \
  "$outdir/rust-HC3N-natural.residual" \
  "$outdir/rust-HC3N-natural.sumwt" \
  "$outdir/casa-HC3N-natural.mom0" \
  "$outdir/rust-HC3N-natural.mom0" \
  "$outdir/casa-HC3N-natural.pv" \
  "$outdir/rust-HC3N-natural.pv"

cp -R "$issue122_dir/TDRW0001_10s.ms" "$outdir/casa-target-prior.ms"
cp -R "$issue122_dir/TDRW0001_10s.ms" "$outdir/rust-target-prior.ms"

time_json() {
  local label="$1"
  local outfile="$2"
  shift 2
  python3 - "$label" "$outfile" "$@" <<'PY'
import json
import subprocess
import sys
import time

label = sys.argv[1]
outfile = sys.argv[2]
cmd = sys.argv[3:]
started = time.perf_counter()
completed = subprocess.run(cmd, check=True)
elapsed = time.perf_counter() - started
with open(outfile, "w") as handle:
    json.dump({"label": label, "elapsed_seconds": elapsed, "returncode": completed.returncode}, handle, indent=2)
PY
}

CASA_RS_MS_PATH="$outdir/casa-target-prior.ms" \
CASA_RS_OUT_JSON="$outdir/casa-apply-timing.json" \
CASA_RS_PRIOR="$issue122_dir/casa-priorcal" \
"$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import time
from casatasks import applycal

prior = os.environ["CASA_RS_PRIOR"]
started = time.perf_counter()
applycal(
    vis=os.environ["CASA_RS_MS_PATH"],
    field="1",
    gaintable=[f"{prior}/cal.ant", f"{prior}/cal.gc", f"{prior}/cal.tau"],
    interp=["", "nearest", "nearest"],
    calwt=False,
    applymode="calonly",
)
with open(os.environ["CASA_RS_OUT_JSON"], "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)
PY

target/release/calibrate apply \
  --ms "$outdir/rust-target-prior.ms" \
  --field 1 \
  --apply-mode calonly \
  --interp "nearest;nearest;nearest" \
  --gaintables "$issue122_dir/rust-priorcal/cal.ant,$issue122_dir/rust-priorcal/cal.gc,$issue122_dir/rust-priorcal/cal.tau" \
  --format json \
  --output "$outdir/rust-apply-report.json" \
  --overwrite
python3 - "$outdir/rust-apply-report.json" "$outdir/rust-apply-timing.json" <<'PY'
import json
import sys
report = json.loads(open(sys.argv[1]).read())
with open(sys.argv[2], "w") as handle:
    json.dump({"elapsed_seconds": report["timings"]["total_ns"] / 1.0e9}, handle, indent=2)
PY

time_json rust-transform "$outdir/rust-transform-timing.json" \
  target/release/mstransform \
  --ms "$outdir/rust-target-prior.ms" \
  --out "$outdir/rust-transform.ms" \
  --field 1 \
  --spw "0:7~58" \
  --datacolumn CORRECTED_DATA

CASA_RS_OUTDIR="$outdir" "$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import time
from casatasks import mstransform, uvcontsub, tclean, imstat, immoments, impv

outdir = os.environ["CASA_RS_OUTDIR"]
started = time.perf_counter()
mstransform(
    vis=f"{outdir}/casa-target-prior.ms",
    outputvis=f"{outdir}/casa-transform.ms",
    field="1",
    spw="0:7~58",
    datacolumn="corrected",
    reindex=False,
)
with open(f"{outdir}/casa-transform-timing.json", "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)

started = time.perf_counter()
uvcontsub(
    vis=f"{outdir}/casa-transform.ms",
    outputvis=f"{outdir}/casa-contsub.ms",
    fitspec="0:0~7;44~51",
    fitorder=0,
    datacolumn="data",
    fitmethod="casacore",
)
with open(f"{outdir}/casa-uvcontsub-timing.json", "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)

started = time.perf_counter()
tclean(
    vis=f"{outdir}/casa-contsub.ms",
    imagename=f"{outdir}/casa-HC3N-natural",
    field="1",
    spw="0",
    specmode="cube",
    nchan=20,
    start="0",
    width="1",
    outframe="LSRK",
    restfreq="36.39232GHz",
    gridder="standard",
    deconvolver="hogbom",
    weighting="natural",
    imsize=128,
    cell="0.4arcsec",
    phasecenter=1,
    niter=0,
    threshold="0Jy",
    datacolumn="data",
    interactive=False,
)
with open(f"{outdir}/casa-tclean-timing.json", "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)

stats = imstat(imagename=f"{outdir}/casa-HC3N-natural.image", box="48,48,80,80", chans="5~15")
with open(f"{outdir}/casa-imstat.json", "w") as handle:
    json.dump({k: (v.tolist() if hasattr(v, "tolist") else v) for k, v in stats.items()}, handle, indent=2, default=str)

started = time.perf_counter()
immoments(imagename=f"{outdir}/casa-HC3N-natural.image", outfile=f"{outdir}/casa-HC3N-natural.mom0", moments=[0], chans="5~15")
with open(f"{outdir}/casa-immoments-timing.json", "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)

started = time.perf_counter()
impv(imagename=f"{outdir}/casa-HC3N-natural.image", outfile=f"{outdir}/casa-HC3N-natural.pv", mode="coords", start=[32,64], end=[96,64], width=3, unit="arcsec", chans="5~15", overwrite=True)
with open(f"{outdir}/casa-impv-timing.json", "w") as handle:
    json.dump({"elapsed_seconds": time.perf_counter() - started}, handle, indent=2)
PY

time_json rust-uvcontsub "$outdir/rust-uvcontsub-wall-timing.json" \
  target/release/calibrate uvcontsub \
  --ms "$outdir/rust-transform.ms" \
  --out "$outdir/rust-contsub.ms" \
  --fitspw "0:0~7;44~51" \
  --fitorder 0 \
  --datacolumn DATA \
  --format json \
  --output "$outdir/rust-uvcontsub-report.json" \
  --overwrite
python3 - "$outdir/rust-uvcontsub-report.json" "$outdir/rust-uvcontsub-timing.json" <<'PY'
import json
import sys
report = json.loads(open(sys.argv[1]).read())
with open(sys.argv[2], "w") as handle:
    json.dump({"elapsed_seconds": report["elapsed_ns"] / 1.0e9}, handle, indent=2)
PY

time_json rust-tclean "$outdir/rust-tclean-timing.json" \
  target/release/casars-imager \
  --ms "$outdir/rust-contsub.ms" \
  --imagename "$outdir/rust-HC3N-natural" \
  --field 1 \
  --spw 0 \
  --specmode cube \
  --channel-count 20 \
  --start 0 \
  --width 1 \
  --outframe LSRK \
  --restfreq 36.39232GHz \
  --weighting natural \
  --imsize 128 \
  --cell-arcsec 0.4 \
  --phasecenter-field 1 \
  --niter 0 \
  --threshold-jy 0 \
  --datacolumn DATA \
  --no-preview-pngs \
  --dirty-only

target/release/imexplore imstat "$outdir/rust-HC3N-natural.image" --box 48,48,80,80 --chans 5~15 --json > "$outdir/rust-imstat.json"
time_json rust-immoments "$outdir/rust-immoments-timing.json" \
  target/release/immoments "$outdir/rust-HC3N-natural.image" \
  --outfile "$outdir/rust-HC3N-natural.mom0" \
  --moments 0 \
  --chans 5~15 \
  --overwrite
time_json rust-impv "$outdir/rust-impv-timing.json" \
  target/release/impv "$outdir/rust-HC3N-natural.image" \
  --outfile "$outdir/rust-HC3N-natural.pv" \
  --start 32,64 \
  --end 96,64 \
  --width 3 \
  --chans 5~15 \
  --overwrite

CASA_RS_OUTDIR="$outdir" "$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image, table

outdir = Path(os.environ["CASA_RS_OUTDIR"])

def read_ms_data(path):
    tb = table(); tb.open(str(path))
    data = tb.getcol("DATA")
    tb.close()
    return data

def image_chunk(path):
    ia = image(); ia.open(str(path)); data = ia.getchunk(); ia.close()
    return data

def compare_array(name, casa, rust):
    casa_cmp = np.squeeze(casa)
    rust_cmp = np.squeeze(rust)
    entry = {"casa_shape": list(casa.shape), "rust_shape": list(rust.shape)}
    if casa_cmp.shape == rust_cmp.shape:
        diff = rust_cmp - casa_cmp
        entry.update({
            "rms": float(np.sqrt(np.mean(np.abs(diff) ** 2))),
            "max_abs": float(np.max(np.abs(diff))),
            "relative_rms": float(np.sqrt(np.mean(np.abs(diff) ** 2)) / (np.sqrt(np.mean(np.abs(casa_cmp) ** 2)) + 1.0e-30)),
        })
    return entry

summary = {}
summary["transform_data"] = compare_array("transform", read_ms_data(outdir / "casa-transform.ms"), read_ms_data(outdir / "rust-transform.ms"))
summary["contsub_data"] = compare_array("contsub", read_ms_data(outdir / "casa-contsub.ms"), read_ms_data(outdir / "rust-contsub.ms"))
for product in ["image", "residual", "psf", "sumwt"]:
    summary[f"cube_{product}"] = compare_array(
        product,
        image_chunk(outdir / f"casa-HC3N-natural.{product}"),
        image_chunk(outdir / f"rust-HC3N-natural.{product}"),
    )
summary["mom0"] = compare_array("mom0", image_chunk(outdir / "casa-HC3N-natural.mom0"), image_chunk(outdir / "rust-HC3N-natural.mom0"))
summary["pv"] = compare_array("pv", image_chunk(outdir / "casa-HC3N-natural.pv"), image_chunk(outdir / "rust-HC3N-natural.pv"))

timings = {}
for name in ["apply", "transform", "uvcontsub", "tclean", "immoments", "impv"]:
    casa_path = outdir / f"casa-{name}-timing.json"
    rust_path = outdir / f"rust-{name}-timing.json"
    if casa_path.exists() and rust_path.exists():
        casa = json.loads(casa_path.read_text())["elapsed_seconds"]
        rust = json.loads(rust_path.read_text())["elapsed_seconds"]
        timings[name] = {"casa_seconds": casa, "rust_seconds": rust, "speedup_casa_over_rust": casa / rust if rust else None}
summary["timings"] = timings
(outdir / "issue123-summary.json").write_text(json.dumps(summary, indent=2))

casa_cube = image_chunk(outdir / "casa-HC3N-natural.image")
rust_cube = image_chunk(outdir / "rust-HC3N-natural.image")
chan = min(10, casa_cube.shape[3] - 1)
vmax = max(float(np.nanmax(np.abs(casa_cube[:, :, 0, chan]))), float(np.nanmax(np.abs(rust_cube[:, :, 0, chan]))), 1.0e-12)
fig, axes = plt.subplots(1, 3, figsize=(13, 4), dpi=150)
for ax, title, data in [
    (axes[0], "CASA cube channel 10", casa_cube[:, :, 0, chan]),
    (axes[1], "casa-rs cube channel 10", rust_cube[:, :, 0, chan]),
    (axes[2], "casa-rs - CASA", rust_cube[:, :, 0, chan] - casa_cube[:, :, 0, chan]),
]:
    im = ax.imshow(data.T, origin="lower", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_title(title)
    ax.set_xticks([]); ax.set_yticks([])
fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.8)
fig.savefig(outdir / "cube-channel10-casa-vs-rust.png", bbox_inches="tight")
plt.close(fig)

casa_mom = np.squeeze(image_chunk(outdir / "casa-HC3N-natural.mom0"))
rust_mom = np.squeeze(image_chunk(outdir / "rust-HC3N-natural.mom0"))
vmax = max(float(np.nanmax(np.abs(casa_mom))), float(np.nanmax(np.abs(rust_mom))), 1.0e-12)
fig, axes = plt.subplots(1, 3, figsize=(13, 4), dpi=150)
for ax, title, data in [
    (axes[0], "CASA moment 0", casa_mom),
    (axes[1], "casa-rs moment 0", rust_mom),
    (axes[2], "casa-rs - CASA", rust_mom - casa_mom),
]:
    im = ax.imshow(data.T, origin="lower", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_title(title)
    ax.set_xticks([]); ax.set_yticks([])
fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.8)
fig.savefig(outdir / "moment0-casa-vs-rust.png", bbox_inches="tight")
plt.close(fig)

casa_pv = np.squeeze(image_chunk(outdir / "casa-HC3N-natural.pv"))
rust_pv = np.squeeze(image_chunk(outdir / "rust-HC3N-natural.pv"))
vmax = max(float(np.nanmax(np.abs(casa_pv))), float(np.nanmax(np.abs(rust_pv))), 1.0e-12)
fig, axes = plt.subplots(1, 3, figsize=(13, 4), dpi=150)
for ax, title, data in [
    (axes[0], "CASA PV", casa_pv),
    (axes[1], "casa-rs PV", rust_pv),
    (axes[2], "casa-rs - CASA", rust_pv - casa_pv),
]:
    im = ax.imshow(data.T, origin="lower", aspect="auto", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_title(title)
    ax.set_xlabel("offset pixel")
    ax.set_ylabel("channel")
fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.8)
fig.savefig(outdir / "pv-casa-vs-rust.png", bbox_inches="tight")
plt.close(fig)
PY

echo "Issue #123 artifacts written to $outdir"
