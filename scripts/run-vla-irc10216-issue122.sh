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

outdir="${1:-target/wdad-wave4-122}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$outdir/matplotlib}"
mkdir -p "$MPLCONFIGDIR"

if [[ -z "${CASA_RS_CASA_PYTHON:-}" || ! -x "$CASA_RS_CASA_PYTHON" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 2
fi

rm -rf \
  "$outdir/TDRW0001_10s.ms" \
  "$outdir/casa-priorcal" \
  "$outdir/casa-priorcal-cold" \
  "$outdir/rust-priorcal"
scripts/run-vla-irc10216-issue121.sh "$outdir"
cargo build -q -p casa-calibration --bin calibrate

base_ms="$outdir/TDRW0001_10s.ms"
casa_prior="$outdir/casa-priorcal"
rust_prior="$outdir/rust-priorcal"

run_apply_pair() {
  local label="$1"
  local casa_tables="$2"
  local rust_tables="$3"
  local casa_interp="$4"
  local rust_interp="$5"
  local casa_ms="$outdir/casa-${label}.ms"
  local rust_ms="$outdir/rust-${label}.ms"

  rm -rf "$casa_ms" "$rust_ms"
  cp -R "$base_ms" "$casa_ms"
  cp -R "$base_ms" "$rust_ms"

  CASA_RS_MS_PATH="$casa_ms" \
  CASA_RS_GAINTABLES="$casa_tables" \
  CASA_RS_INTERP="$casa_interp" \
  CASA_RS_OUT_JSON="$outdir/casa-${label}-timing.json" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import time
from casatasks import applycal

tables = [item for item in os.environ["CASA_RS_GAINTABLES"].split(",") if item]
interp = [item for item in os.environ["CASA_RS_INTERP"].split(",")]
start = time.perf_counter()
applycal(
    vis=os.environ["CASA_RS_MS_PATH"],
    field="2",
    gaintable=tables,
    interp=interp,
    calwt=False,
    applymode="calonly",
)
elapsed = time.perf_counter() - start
with open(os.environ["CASA_RS_OUT_JSON"], "w") as handle:
    json.dump({"elapsed_seconds": elapsed}, handle, indent=2)
PY

  start_ns="$(python3 - <<'PY'
import time
print(time.perf_counter_ns())
PY
)"
  target/debug/calibrate apply \
    --ms "$rust_ms" \
    --field 2 \
    --apply-mode calonly \
    --interp "$rust_interp" \
    --gaintables "$rust_tables" \
    --format json \
    --output "$outdir/rust-${label}-report.json" \
    --overwrite
  end_ns="$(python3 - <<'PY'
import time
print(time.perf_counter_ns())
PY
)"
  python3 - "$start_ns" "$end_ns" "$outdir/rust-${label}-timing.json" <<'PY'
import json
import sys

start_ns = int(sys.argv[1])
end_ns = int(sys.argv[2])
with open(sys.argv[3], "w") as handle:
    json.dump({"elapsed_seconds": (end_ns - start_ns) / 1.0e9}, handle, indent=2)
PY

  CASA_RS_CASA_MS="$casa_ms" \
  CASA_RS_RUST_MS="$rust_ms" \
  CASA_RS_LABEL="$label" \
  CASA_RS_OUTDIR="$outdir" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from casatools import table

label = os.environ["CASA_RS_LABEL"]
outdir = Path(os.environ["CASA_RS_OUTDIR"])

def read_corrected(path):
    tb = table()
    tb.open(path)
    qt = tb.query("FIELD_ID==2 && DATA_DESC_ID==0")
    corrected = qt.getcol("CORRECTED_DATA")
    times = qt.getcol("TIME")
    scans = qt.getcol("SCAN_NUMBER")
    qt.close()
    tb.close()
    return corrected, times, scans

casa, times, scans = read_corrected(os.environ["CASA_RS_CASA_MS"])
rust, _, _ = read_corrected(os.environ["CASA_RS_RUST_MS"])
diff = rust - casa
amp_casa = np.abs(casa[0, :, :])
amp_rust = np.abs(rust[0, :, :])
amp_diff = amp_rust - amp_casa

summary = {
    "shape": list(casa.shape),
    "max_abs_diff": float(np.max(np.abs(diff))),
    "median_abs_diff": float(np.median(np.abs(diff))),
    "max_rel_diff": float(np.max(np.abs(diff) / np.maximum(np.abs(casa), 1.0e-12))),
    "median_rel_diff": float(np.median(np.abs(diff) / np.maximum(np.abs(casa), 1.0e-12))),
    "max_amp_diff": float(np.max(np.abs(amp_diff))),
    "median_amp_diff": float(np.median(np.abs(amp_diff))),
}
(outdir / f"{label}-corrected-comparison.json").write_text(json.dumps(summary, indent=2))

row_mask = scans == 56
if not np.any(row_mask):
    row_mask = np.ones_like(scans, dtype=bool)
plot_casa = amp_casa[:, row_mask]
plot_rust = amp_rust[:, row_mask]
plot_diff = plot_rust - plot_casa
channel = np.repeat(np.arange(plot_casa.shape[0]), plot_casa.shape[1])

fig, axes = plt.subplots(2, 1, figsize=(12, 9), dpi=150, sharex=True)
axes[0].scatter(channel, plot_casa.reshape(-1, order="F"), s=10, alpha=0.45, label="CASA")
axes[0].scatter(channel, plot_rust.reshape(-1, order="F"), s=18, marker="x", alpha=0.65, label="casa-rs")
axes[0].set_ylabel("RR amplitude")
axes[0].set_title(f"IRC+10216 #122 {label}: corrected DATA, field 2 scan 56")
axes[0].legend(loc="best")
axes[0].grid(True, alpha=0.25)
axes[1].scatter(channel, plot_diff.reshape(-1, order="F"), s=10, alpha=0.6, color="black")
axes[1].axhline(0.0, color="tab:red", linewidth=1)
axes[1].set_xlabel("Channel")
axes[1].set_ylabel("casa-rs - CASA")
axes[1].grid(True, alpha=0.25)
fig.tight_layout()
fig.savefig(outdir / f"{label}-corrected-casa-vs-rust.png")
PY
}

run_apply_pair \
  "prior-gctau" \
  "$casa_prior/cal.gc,$casa_prior/cal.tau" \
  "$rust_prior/cal.gc,$rust_prior/cal.tau" \
  "nearest,nearest" \
  "nearest;nearest"

run_apply_pair \
  "prior-full" \
  "$casa_prior/cal.ant,$casa_prior/cal.gc,$casa_prior/cal.tau" \
  "$rust_prior/cal.ant,$rust_prior/cal.gc,$rust_prior/cal.tau" \
  ",nearest,nearest" \
  "nearest;nearest;nearest"

python3 - "$outdir" <<'PY'
import json
import sys
from pathlib import Path

outdir = Path(sys.argv[1])
summary = {}
for label in ["prior-gctau", "prior-full"]:
    casa = json.loads((outdir / f"casa-{label}-timing.json").read_text())
    rust = json.loads((outdir / f"rust-{label}-timing.json").read_text())
    comparison = json.loads((outdir / f"{label}-corrected-comparison.json").read_text())
    summary[label] = {
        "casa_elapsed_seconds": casa["elapsed_seconds"],
        "rust_elapsed_seconds": rust["elapsed_seconds"],
        "speedup_casa_over_rust": casa["elapsed_seconds"] / rust["elapsed_seconds"],
        **comparison,
    }
(outdir / "issue122-summary.json").write_text(json.dumps(summary, indent=2))
PY

echo "Issue #122 artifacts written to $outdir"
