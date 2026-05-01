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

outdir="${1:-target/wave5-issue126-panels}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"

tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
model="${CASA_RS_WAVE5_MODEL:-$tutorial_root/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits}"
if [[ ! -f "$model" ]]; then
  echo "missing VLA ppdisk model FITS: $model" >&2
  exit 2
fi

cargo build --release -q -p casa-ms --bin simobserve -p casars-imager

duration="${CASA_RS_WAVE5_ISSUE126_PANEL_DURATION:-120}"
clean_ms="$outdir/clean.ms"
rm -rf "$clean_ms" "$outdir"/rust-*.ms "$outdir"/casa-*.ms "$outdir"/images
mkdir -p "$outdir/images"

common_args=(
  --model "$model"
  --duration "$duration"
  --integration 2
  --channels 4
  --overwrite
)

target/release/simobserve "${common_args[@]}" --out "$clean_ms" > "$outdir/clean-report.json"

run_rust_case() {
  local slug="$1"
  shift
  target/release/simobserve "${common_args[@]}" --out "$outdir/rust-$slug.ms" \
    --corruption-seed 12345 "$@" > "$outdir/rust-$slug-report.json"
}

run_rust_case noise \
  --noise-stddev-jy 0.001
run_rust_case gain-phase \
  --gain-amplitude-stddev 0.05 \
  --gain-phase-stddev-rad 0.02
run_rust_case leakage \
  --polarization-leakage 0.01
run_rust_case bandpass \
  --bandpass-amplitude-stddev 0.03 \
  --bandpass-phase-stddev-rad 0.04
run_rust_case pointing \
  --pointing-offset-ra-arcsec 2.0 \
  --pointing-offset-dec-arcsec -1.0
run_rust_case pointing-visual \
  --pointing-offset-ra-arcsec 20.0 \
  --pointing-offset-dec-arcsec -10.0

CASA_RS_OUTDIR="$outdir" CASA_RS_CLEAN_MS="$clean_ms" "$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import shutil
import time
from pathlib import Path

from casatools import simulator

outdir = Path(os.environ["CASA_RS_OUTDIR"])
clean_ms = Path(os.environ["CASA_RS_CLEAN_MS"])

cases = [
    {
        "slug": "noise",
        "description": "CASA setnoise(mode='simplenoise', simplenoise='0.001Jy')",
        "calls": [("setnoise", {"mode": "simplenoise", "simplenoise": "0.001Jy"})],
    },
    {
        "slug": "gain-phase",
        "description": "CASA setgain(mode='fbm', amplitude=[0.05, 0.02])",
        "calls": [("setgain", {"mode": "fbm", "amplitude": [0.05, 0.02]})],
    },
    {
        "slug": "leakage",
        "description": "CASA setleakage(mode='constant', amplitude=[0.01, 0.0])",
        "calls": [("setleakage", {"mode": "constant", "amplitude": [0.01, 0.0]})],
    },
]

status = {}
for case in cases:
    slug = case["slug"]
    casa_ms = outdir / f"casa-{slug}.ms"
    if casa_ms.exists():
        shutil.rmtree(casa_ms)
    shutil.copytree(clean_ms, casa_ms)
    sm = simulator()
    started = time.perf_counter()
    try:
        sm.openfromms(str(casa_ms))
        sm.setdata(fieldid=[])
        sm.setseed(12345)
        for name, kwargs in case["calls"]:
            getattr(sm, name)(**kwargs)
        sm.corrupt()
        status[slug] = {
            "available": True,
            "description": case["description"],
            "elapsed_seconds": time.perf_counter() - started,
            "ms": str(casa_ms),
        }
    except Exception as exc:
        status[slug] = {
            "available": False,
            "description": case["description"],
            "error": str(exc),
            "elapsed_seconds": time.perf_counter() - started,
        }
    finally:
        try:
            sm.done()
        except Exception:
            pass

status["bandpass"] = {
    "available": False,
    "description": "CASA simulator setbandpass XML documents this method as not implemented.",
}
status["pointing"] = {
    "available": False,
    "description": "CASA simulator setpointingerror requires an external pointing-error table; #126 implements the tutorial native offset directly.",
}
with open(outdir / "casa-corruption-status.json", "w", encoding="utf-8") as handle:
    json.dump(status, handle, indent=2, sort_keys=True)
PY

image_ms() {
  local label="$1"
  local ms="$2"
  local prefix="$outdir/images/$label"
  rm -rf "$prefix".*
  target/release/casars-imager \
    --ms "$ms" \
    --imagename "$prefix" \
    --imsize 257 \
    --cell-arcsec 0.00311 \
    --dirty-only \
    --weighting natural \
    --no-preview-pngs > "$outdir/images/$label-imager-report.json"
}

image_ms clean "$clean_ms"
for slug in noise gain-phase leakage bandpass pointing pointing-visual; do
  image_ms "rust-$slug" "$outdir/rust-$slug.ms"
  if [[ -d "$outdir/casa-$slug.ms" ]]; then
    image_ms "casa-$slug" "$outdir/casa-$slug.ms"
  fi
done

"$CASA_RS_CASA_PYTHON" - "$outdir" "$model" <<'PY'
import json
import math
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import image, table

outdir = Path(sys.argv[1])
model_path = Path(sys.argv[2])
images = outdir / "images"

def read_casa_image(path):
    ia = image()
    ia.open(str(path))
    try:
        data = np.asarray(ia.getchunk(), dtype=np.float64)
    finally:
        ia.close()
    while data.ndim > 2:
        data = data[..., 0]
    if data.ndim != 2:
        raise ValueError(f"expected 2-D image plane for {path}, got {data.shape}")
    return data

def read_ms_data(path):
    tb = table()
    tb.open(str(path))
    try:
        data = np.asarray(tb.getcol("DATA"), dtype=np.complex64)
    finally:
        tb.close()
    return data

def read_ms_columns(path):
    tb = table()
    tb.open(str(path))
    try:
        data = np.asarray(tb.getcol("DATA"), dtype=np.complex64)
        time = np.asarray(tb.getcol("TIME"), dtype=np.float64)
    finally:
        tb.close()
    return data, time

def read_fits_image(path):
    raw = path.read_bytes()
    cards = []
    offset = 0
    while True:
        block = raw[offset:offset + 2880]
        offset += 2880
        for index in range(0, len(block), 80):
            card = block[index:index + 80].decode("ascii", errors="ignore")
            cards.append(card)
            if card.startswith("END"):
                header = {}
                for item in cards:
                    if "=" not in item:
                        continue
                    key = item[:8].strip()
                    value = item[10:30].split("/")[0].strip()
                    header[key] = value
                bitpix = int(header["BITPIX"])
                naxis = int(header["NAXIS"])
                shape = [int(header[f"NAXIS{i}"]) for i in range(1, naxis + 1)]
                dtype = {8: ">u1", 16: ">i2", 32: ">i4", -32: ">f4", -64: ">f8"}[bitpix]
                count = int(np.prod(shape))
                data = np.frombuffer(raw, dtype=np.dtype(dtype), count=count, offset=offset)
                data = data.reshape(tuple(reversed(shape))).astype(np.float64)
                while data.ndim > 2:
                    data = data[0]
                return data.T

def percentile_limits(data):
    finite = np.asarray(data)[np.isfinite(data)]
    if finite.size == 0:
        return -1.0, 1.0
    vmin = float(np.percentile(finite, 0.5))
    vmax = float(np.percentile(finite, 99.5))
    if vmin == vmax:
        pad = abs(vmin) * 0.01 or 1.0
        return vmin - pad, vmax + pad
    return vmin, vmax

def summarize_delta(clean, data):
    delta = data - clean
    component = np.concatenate([delta.real.reshape(-1), delta.imag.reshape(-1)])
    return {
        "max_abs_delta_jy": float(np.max(np.abs(delta))),
        "rms_abs_delta_jy": float(np.sqrt(np.mean(np.abs(delta) ** 2))),
        "component_stddev_delta_jy": float(np.std(component)),
        "mean_amplitude_ratio": float(np.mean(np.abs(data)) / np.mean(np.abs(clean))),
    }

def compare_delta(rust, casa):
    diff = rust - casa
    return {
        "max_abs_diff_jy": float(np.max(np.abs(diff))),
        "rms_abs_diff_jy": float(np.sqrt(np.mean(np.abs(diff) ** 2))),
        "mean_abs_diff_jy": float(np.mean(np.abs(diff))),
    }

def rms(data):
    return float(np.sqrt(np.mean(np.asarray(data, dtype=np.float64) ** 2)))

def phase_delta_deg(clean, data, axis=None):
    cross = data * np.conj(clean)
    if axis is None:
        cross = np.mean(cross)
    else:
        cross = np.mean(cross, axis=axis)
    return np.degrees(np.angle(cross))

def time_series(clean, data, time):
    unique_time = np.unique(time)
    t_min = (unique_time - unique_time[0]) / 60.0
    amp_ratio = []
    phase_deg = []
    for value in unique_time:
        mask = time == value
        clean_slice = clean[:, :, mask]
        data_slice = data[:, :, mask]
        amp_ratio.append(float(np.mean(np.abs(data_slice)) / np.mean(np.abs(clean_slice))))
        phase_deg.append(float(phase_delta_deg(clean_slice, data_slice)))
    return t_min, np.asarray(amp_ratio), np.asarray(phase_deg)

def channel_series(clean, data):
    clean_amp = np.mean(np.abs(clean), axis=(0, 2))
    data_amp = np.mean(np.abs(data), axis=(0, 2))
    ratio = data_amp / clean_amp
    phase = phase_delta_deg(clean, data, axis=(0, 2))
    channel = np.arange(clean.shape[1])
    return channel, ratio, np.asarray(phase)

def plot_noise_residual_panel(clean_image, rust_image, casa_image, outdir):
    rust_resid = rust_image - clean_image
    casa_resid = casa_image - clean_image
    residual_stack = np.concatenate([rust_resid.ravel(), casa_resid.ravel()])
    resid_abs = float(np.percentile(np.abs(residual_stack), 99.5)) or 1.0
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), constrained_layout=True)
    panels = [
        (axes[0, 0], clean_image, "Clean dirty image", *percentile_limits(clean_image), "viridis"),
        (axes[0, 1], rust_resid, f"casa-rs noise residual; RMS={rms(rust_resid):.3e}", -resid_abs, resid_abs, "coolwarm"),
        (axes[1, 0], casa_resid, f"CASA noise residual; RMS={rms(casa_resid):.3e}", -resid_abs, resid_abs, "coolwarm"),
    ]
    for ax, data, label, vmin, vmax, cmap in panels:
        im = ax.imshow(data.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(label)
        ax.set_xlabel("x pixel")
        ax.set_ylabel("y pixel")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    axes[1, 1].hist(rust_resid.ravel(), bins=80, histtype="step", density=True, label="casa-rs")
    axes[1, 1].hist(casa_resid.ravel(), bins=80, histtype="step", density=True, label="CASA")
    axes[1, 1].set_title("Residual pixel distribution")
    axes[1, 1].set_xlabel("Jy/beam")
    axes[1, 1].set_ylabel("density")
    axes[1, 1].legend()
    panel_path = outdir / "wave5-issue126-noise-residual-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)
    return panel_path, {"rust_residual_rms": rms(rust_resid), "casa_residual_rms": rms(casa_resid)}

def plot_gain_time_panel(clean_ms, rust_ms, casa_ms, time, outdir):
    rust_t, rust_amp, rust_phase = time_series(clean_ms, rust_ms, time)
    casa_t, casa_amp, casa_phase = time_series(clean_ms, casa_ms, time)
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), constrained_layout=True, sharex=True)
    axes[0].plot(rust_t, rust_amp, ".", label="casa-rs")
    axes[0].plot(casa_t, casa_amp, ".", label="CASA")
    axes[0].axhline(1.0, color="black", linewidth=0.8)
    axes[0].set_title("Gain corruption amplitude ratio vs time")
    axes[0].set_ylabel("mean |corrupted| / |clean|")
    axes[0].legend()
    axes[1].plot(rust_t, rust_phase, ".", label="casa-rs")
    axes[1].plot(casa_t, casa_phase, ".", label="CASA")
    axes[1].axhline(0.0, color="black", linewidth=0.8)
    axes[1].set_title("Gain corruption phase offset vs time")
    axes[1].set_xlabel("minutes from start")
    axes[1].set_ylabel("degrees")
    axes[1].legend()
    panel_path = outdir / "wave5-issue126-gain-phase-time-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)
    return panel_path

def plot_bandpass_channel_panel(clean_ms, rust_ms, outdir):
    channel, amp_ratio, phase = channel_series(clean_ms, rust_ms)
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), constrained_layout=True, sharex=True)
    axes[0].plot(channel, amp_ratio, "o-")
    axes[0].axhline(1.0, color="black", linewidth=0.8)
    axes[0].set_title("Bandpass amplitude ratio vs channel")
    axes[0].set_ylabel("mean |corrupted| / |clean|")
    axes[1].plot(channel, phase, "o-")
    axes[1].axhline(0.0, color="black", linewidth=0.8)
    axes[1].set_title("Bandpass phase offset vs channel")
    axes[1].set_xlabel("channel")
    axes[1].set_ylabel("degrees")
    panel_path = outdir / "wave5-issue126-bandpass-channel-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)
    return panel_path

def plot_leakage_visibility_panel(clean_ms, rust_ms, time, outdir):
    t_min, amp_ratio, phase = time_series(clean_ms, rust_ms, time)
    delta = rust_ms - clean_ms
    corr0_delta = np.mean(np.abs(delta[0, :, :]), axis=0)
    corr1_delta = np.mean(np.abs(delta[1, :, :]), axis=0)
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), constrained_layout=True, sharex=True)
    axes[0].plot(t_min, amp_ratio, ".")
    axes[0].axhline(1.0, color="black", linewidth=0.8)
    axes[0].set_title("Polarization leakage mean amplitude ratio vs time")
    axes[0].set_ylabel("mean |corrupted| / |clean|")
    axes[1].scatter((time - time.min()) / 60.0, corr0_delta, s=4, alpha=0.35, label="corr 0")
    axes[1].scatter((time - time.min()) / 60.0, corr1_delta, s=4, alpha=0.35, label="corr 1")
    axes[1].set_title("Leakage visibility delta by correlation")
    axes[1].set_xlabel("minutes from start")
    axes[1].set_ylabel("mean channel |delta| Jy")
    axes[1].legend()
    panel_path = outdir / "wave5-issue126-leakage-visibility-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)
    return panel_path

def plot_pointing_impact_panel(clean_image, pointing_image, visual_image, outdir):
    standard_diff = pointing_image - clean_image
    visual_diff = visual_image - clean_image
    diff_abs = float(np.percentile(np.abs(visual_diff), 99.5)) or 1.0
    ratio = np.divide(
        visual_image,
        clean_image,
        out=np.ones_like(visual_image),
        where=np.abs(clean_image) > max(float(np.max(np.abs(clean_image))) * 1e-4, 1e-12),
    )
    ratio_vmin, ratio_vmax = percentile_limits(ratio[np.isfinite(ratio)])
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), constrained_layout=True)
    panels = [
        (axes[0, 0], clean_image, "Clean dirty image", *percentile_limits(clean_image), "viridis"),
        (axes[0, 1], visual_image, "20/-10 arcsec pointing diagnostic", *percentile_limits(visual_image), "viridis"),
        (axes[1, 0], visual_diff, f"visual offset minus clean; RMS={rms(visual_diff):.3e}", -diff_abs, diff_abs, "coolwarm"),
        (axes[1, 1], ratio, "visual offset / clean", ratio_vmin, ratio_vmax, "magma"),
    ]
    for ax, data, label, vmin, vmax, cmap in panels:
        im = ax.imshow(data.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(label)
        ax.set_xlabel("x pixel")
        ax.set_ylabel("y pixel")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    panel_path = outdir / "wave5-issue126-pointing-impact-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)
    return panel_path, {"standard_offset_rms": rms(standard_diff), "visual_offset_rms": rms(visual_diff)}

model = read_fits_image(model_path)
clean_image = read_casa_image(images / "clean.image")
clean_ms, clean_time = read_ms_columns(outdir / "clean.ms")
with open(outdir / "casa-corruption-status.json", encoding="utf-8") as handle:
    casa_status = json.load(handle)

cases = [
    ("noise", "Noise"),
    ("gain-phase", "Gain/phase"),
    ("leakage", "Polarization leakage"),
    ("bandpass", "Bandpass"),
    ("pointing", "Pointing offset"),
]

summary = {}
for slug, title in cases:
    rust_image = read_casa_image(images / f"rust-{slug}.image")
    rust_ms = read_ms_data(outdir / f"rust-{slug}.ms")
    casa_available = bool(casa_status.get(slug, {}).get("available"))
    if casa_available:
        casa_image = read_casa_image(images / f"casa-{slug}.image")
        casa_ms = read_ms_data(outdir / f"casa-{slug}.ms")
        diff = rust_image - casa_image
        diff_title = "casa-rs minus CASA C++"
        casa_title = "CASA C++ dirty image"
        image_stack = np.concatenate([rust_image.ravel(), casa_image.ravel()])
        data_comparison = compare_delta(rust_ms, casa_ms)
        casa_delta = summarize_delta(clean_ms, casa_ms)
    else:
        casa_image = clean_image
        diff = rust_image - clean_image
        diff_title = "casa-rs minus clean"
        casa_title = "CASA reference unavailable; clean image shown"
        image_stack = np.concatenate([rust_image.ravel(), clean_image.ravel()])
        data_comparison = None
        casa_delta = None

    image_vmin, image_vmax = percentile_limits(image_stack)
    diff_abs = float(np.percentile(np.abs(diff), 99.9)) if diff.size else 0.0
    if diff_abs == 0.0 or not math.isfinite(diff_abs):
        diff_abs = float(np.max(np.abs(diff))) if diff.size else 1.0
    if diff_abs == 0.0 or not math.isfinite(diff_abs):
        diff_abs = 1.0

    fig, axes = plt.subplots(2, 2, figsize=(12, 10), constrained_layout=True)
    panels = [
        (axes[0, 0], model, "Input model image", *percentile_limits(model), "viridis"),
        (axes[0, 1], rust_image, "casa-rs corrupted dirty image", image_vmin, image_vmax, "viridis"),
        (axes[1, 0], casa_image, casa_title, image_vmin, image_vmax, "viridis"),
        (axes[1, 1], diff, diff_title, -diff_abs, diff_abs, "coolwarm"),
    ]
    fig.suptitle(f"Wave 5 #126 corruption panel: {title}")
    for ax, data, label, vmin, vmax, cmap in panels:
        im = ax.imshow(data.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
        ax.set_title(label)
        ax.set_xlabel("x pixel")
        ax.set_ylabel("y pixel")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    panel_path = outdir / f"wave5-issue126-{slug}-panel.png"
    fig.savefig(panel_path, dpi=150)
    plt.close(fig)

    summary[slug] = {
        "panel_png": str(panel_path),
        "casa": casa_status.get(slug, {}),
        "rust_delta_from_clean": summarize_delta(clean_ms, rust_ms),
        "casa_delta_from_clean": casa_delta,
        "rust_vs_casa_data": data_comparison,
    }

with open(outdir / "wave5-issue126-panel-summary.json", "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
print(json.dumps(summary, indent=2, sort_keys=True))

tutorial_panels = {}
if casa_status.get("noise", {}).get("available"):
    path, stats = plot_noise_residual_panel(
        clean_image,
        read_casa_image(images / "rust-noise.image"),
        read_casa_image(images / "casa-noise.image"),
        outdir,
    )
    tutorial_panels["noise_residual"] = {"panel_png": str(path), **stats}
if casa_status.get("gain-phase", {}).get("available"):
    path = plot_gain_time_panel(
        clean_ms,
        read_ms_data(outdir / "rust-gain-phase.ms"),
        read_ms_data(outdir / "casa-gain-phase.ms"),
        clean_time,
        outdir,
    )
    tutorial_panels["gain_phase_time"] = {"panel_png": str(path)}
tutorial_panels["bandpass_channel"] = {
    "panel_png": str(plot_bandpass_channel_panel(clean_ms, read_ms_data(outdir / "rust-bandpass.ms"), outdir))
}
tutorial_panels["leakage_visibility"] = {
    "panel_png": str(plot_leakage_visibility_panel(clean_ms, read_ms_data(outdir / "rust-leakage.ms"), clean_time, outdir))
}
path, stats = plot_pointing_impact_panel(
    clean_image,
    read_casa_image(images / "rust-pointing.image"),
    read_casa_image(images / "rust-pointing-visual.image"),
    outdir,
)
tutorial_panels["pointing_impact"] = {"panel_png": str(path), **stats}

with open(outdir / "wave5-issue126-tutorial-panel-summary.json", "w", encoding="utf-8") as handle:
    json.dump(tutorial_panels, handle, indent=2, sort_keys=True)
print(json.dumps(tutorial_panels, indent=2, sort_keys=True))
PY

echo "Wrote corruption panels and summary under $outdir"
