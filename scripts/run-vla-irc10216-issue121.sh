#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_TUTORIAL_DATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casa-tutorial-data" ]]; then
  export CASA_RS_TUTORIAL_DATA_ROOT="$HOME/SoftwareProjects/casa-tutorial-data"
fi
if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

outdir="${1:-target/wdad-wave4-121}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"
mkdir -p "$MPLCONFIGDIR"

ms_archive="${CASA_RS_TUTORIAL_DATA_ROOT:-}/tutorial-parity/vla/irc10216/TDRW0001_10s.ms.tgz"
fits_path="${CASA_RS_TUTORIAL_DATA_ROOT:-}/tutorial-parity/vla/irc10216/irc_fors1_dec_header.fits"
ms_path="$outdir/TDRW0001_10s.ms"

if [[ ! -f "$ms_archive" ]]; then
  echo "missing IRC+10216 MS archive: $ms_archive" >&2
  exit 2
fi
if [[ ! -f "$fits_path" ]]; then
  echo "missing IRC+10216 FITS input: $fits_path" >&2
  exit 2
fi
if [[ ! -d "$ms_path" ]]; then
  tar -xzf "$ms_archive" -C "$outdir"
fi

cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier slow-parity \
  --require-registry-key vla/irc10216/ms-10s
cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier tutorial-parity \
  --require-registry-key vla/irc10216/fors1-fits

cargo run -q -p casa-ms --bin msexplore -- \
  --format json \
  --output "$outdir/irc10216-listobs.json" \
  --overwrite \
  "$ms_path"

render_casatools_side_by_side() {
  local mode="$1"
  local rust_png="$2"
  local casa_png="$3"
  local combined_png="$4"
  shift 4

  cargo run -q -p casa-ms --bin msexplore -- \
    --plot-output "$rust_png" \
    --plot-format png \
    --plot-width 2400 \
    --plot-height 1350 \
    --symbolsize 1 \
    "$@" \
    "$ms_path"

  CASA_RS_PLOT_MODE="$mode" \
  CASA_RS_MS_PATH="$ms_path" \
  CASA_RS_CASA_PNG="$casa_png" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from casatools import table

mode = os.environ["CASA_RS_PLOT_MODE"]
ms_path = os.environ["CASA_RS_MS_PATH"]
out = os.environ["CASA_RS_CASA_PNG"]

tb = table()
tb.open(ms_path)
query = {
    "target_time": "FIELD_ID==1 && SCAN_NUMBER==6 && DATA_DESC_ID==0",
    "bandpass_channel": "FIELD_ID==2 && SCAN_NUMBER==56 && DATA_DESC_ID==0",
}[mode]
qt = tb.query(query)
data = qt.getcol("DATA")
time = qt.getcol("TIME")
qt.close()
tb.close()

fig, ax = plt.subplots(figsize=(16, 9), dpi=150)
if mode == "target_time":
    amp_by_chan_row = np.abs(data[0, :, :])
    amp = amp_by_chan_row.reshape(-1, order="F")
    x_offset = round(((time.min() + time.max()) / 2.0) / 10.0) * 10.0
    x = np.repeat(time - x_offset, amp_by_chan_row.shape[0])
    ax.scatter(x, amp, s=2, alpha=0.65)
    ax.set_title("CASA casatools IRC+10216 scan 6 RR amplitude vs time")
    ax.set_xlabel(f"Time (MJD seconds - {x_offset:.0f})")
    ax.set_ylabel("Amplitude")
else:
    amp_by_chan_row = np.abs(data[0, :, :])
    width = 4
    trim = (amp_by_chan_row.shape[0] // width) * width
    binned = amp_by_chan_row[:trim, :].reshape(-1, width, amp_by_chan_row.shape[1]).mean(axis=1)
    y = binned.reshape(-1, order="C")
    x = np.repeat(np.arange(binned.shape[0]), binned.shape[1])
    ax.scatter(x, y, s=14, alpha=0.75)
    ax.set_title("CASA casatools J1229+0203 scan 56 RR amplitude vs 4-channel bin")
    ax.set_xlabel("Channel bin")
    ax.set_ylabel("Amplitude")
ax.grid(True, alpha=0.25)
fig.tight_layout()
fig.savefig(out)
PY

  python3 - "$rust_png" "$casa_png" "$combined_png" <<'PY'
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

rust_path, casa_path, out_path = map(Path, sys.argv[1:4])
rust = Image.open(rust_path).convert("RGB")
casa = Image.open(casa_path).convert("RGB").resize(rust.size)
banner = 42
canvas = Image.new("RGB", (rust.width + casa.width, rust.height + banner), "white")
canvas.paste(rust, (0, banner))
canvas.paste(casa, (rust.width, banner))
draw = ImageDraw.Draw(canvas)
font = ImageFont.load_default()
draw.rectangle((0, 0, rust.width, banner), fill="#f2f6ff")
draw.rectangle((rust.width, 0, rust.width + casa.width, banner), fill="#fff4e8")
draw.text((12, 14), "casa-rs msexplore", fill="black", font=font)
draw.text((rust.width + 12, 14), "CASA casatools", fill="black", font=font)
canvas.save(out_path)
PY
}

render_casatools_side_by_side \
  target_time \
  "$outdir/irc10216-target-scan6-amplitude-time.rust.png" \
  "$outdir/irc10216-target-scan6-amplitude-time.casa.png" \
  "$outdir/irc10216-target-scan6-amplitude-time-side-by-side.png" \
  --preset amplitude_vs_time --field 1 --scan 6 --spw 0 --correlation RR

render_casatools_side_by_side \
  bandpass_channel \
  "$outdir/irc10216-bandpass-scan56-amplitude-channel.rust.png" \
  "$outdir/irc10216-bandpass-scan56-amplitude-channel.casa.png" \
  "$outdir/irc10216-bandpass-scan56-amplitude-channel-side-by-side.png" \
  --xaxis chan --yaxis amp --field 2 --scan 56 --spw 0 --correlation RR --avgchannel 4

cargo run -q -p casa-ms --bin msexplore -- \
  --format json \
  --output "$outdir/flag-j1229-selection-summary.json" \
  --field 2 \
  --spw 0 \
  --timerange "2018/11/07/13:30:27~2018/11/07/13:30:30" \
  --flag-action flag \
  --flag-selected \
  --flag-apply \
  --overwrite \
  "$ms_path"

cargo run -q -p casa-ms --bin msexplore -- \
  --format json \
  --output "$outdir/flag-3c286-selection-summary.json" \
  --field 3 \
  --spw 0 \
  --timerange "2018/11/07/13:38:54~2018/11/07/13:39:00" \
  --flag-action flag \
  --flag-selected \
  --flag-apply \
  --overwrite \
  "$ms_path"

echo "IRC+10216 #121 artifacts written under $outdir"
