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

outdir="${1:-target/wave5-issue126}"
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"

tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
model="${CASA_RS_WAVE5_MODEL:-$tutorial_root/tutorial-parity/simulation/vla-ppdisk/ppdisk672_GHz_50pc.fits}"
if [[ ! -f "$model" ]]; then
  echo "missing VLA ppdisk model FITS: $model" >&2
  exit 2
fi

cargo build --release -q -p casa-ms --bin simobserve

clean_ms="$outdir/rust-clean.ms"
rust_noise_gain_ms="$outdir/rust-noise-gain.ms"
rust_common_ms="$outdir/rust-common-corruptions.ms"
casa_noise_gain_ms="$outdir/casa-noise-gain.ms"
rm -rf "$clean_ms" "$rust_noise_gain_ms" "$rust_common_ms" "$casa_noise_gain_ms"

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

common_args=(
  --model "$model"
  --duration "${CASA_RS_WAVE5_ISSUE126_DURATION:-120}"
  --integration 2
  --channels 4
  --overwrite
)

time_json_stdout rust-clean "$outdir/rust-clean-timing.json" "$outdir/rust-clean-report.json" \
  target/release/simobserve "${common_args[@]}" --out "$clean_ms"

time_json_stdout rust-noise-gain "$outdir/rust-noise-gain-timing.json" "$outdir/rust-noise-gain-report.json" \
  target/release/simobserve "${common_args[@]}" --out "$rust_noise_gain_ms" \
  --corruption-seed 12345 \
  --noise-simplenoise-jy 0.001 \
  --gain-mode fbm \
  --gain-interval-seconds 10 \
  --gain-amplitude 0.05,0.02

time_json_stdout rust-common-corruptions "$outdir/rust-common-corruptions-timing.json" "$outdir/rust-common-corruptions-report.json" \
  target/release/simobserve "${common_args[@]}" --out "$rust_common_ms" \
  --corruption-seed 12345 \
  --noise-simplenoise-jy 0.001 \
  --gain-mode fbm \
  --gain-interval-seconds 10 \
  --gain-amplitude 0.05,0.02 \
  --bandpass-mode calculate \
  --bandpass-interval-seconds 3600 \
  --bandpass-amplitude 0.03,0.04 \
  --leakage-amplitude 0.01,0.0 \
  --pointing-offset-ra-arcsec 2.0 \
  --pointing-offset-dec-arcsec -1.0

CASA_RS_OUTDIR="$outdir" \
CASA_RS_CLEAN_MS="$clean_ms" \
CASA_RS_CASA_NOISE_GAIN_MS="$casa_noise_gain_ms" \
"$CASA_RS_CASA_PYTHON" - <<'PY'
import json
import os
import shutil
import time
from pathlib import Path

from casatools import simulator

outdir = Path(os.environ["CASA_RS_OUTDIR"])
clean_ms = Path(os.environ["CASA_RS_CLEAN_MS"])
casa_ms = Path(os.environ["CASA_RS_CASA_NOISE_GAIN_MS"])

if casa_ms.exists():
    shutil.rmtree(casa_ms)
shutil.copytree(clean_ms, casa_ms)

sm = simulator()
started = time.perf_counter()
sm.openfromms(str(casa_ms))
try:
    sm.setdata(fieldid=[])
    sm.setseed(12345)
    sm.setnoise(mode="simplenoise", simplenoise="0.001Jy")
    sm.setgain(mode="fbm", amplitude=[0.05, 0.02])
    sm.corrupt()
finally:
    sm.done()
with open(outdir / "casa-noise-gain-timing.json", "w", encoding="utf-8") as handle:
    json.dump({"label": "casa-noise-gain-corrupt", "elapsed_seconds": time.perf_counter() - started}, handle, indent=2)
PY

"$CASA_RS_CASA_PYTHON" - "$outdir" "$clean_ms" "$rust_noise_gain_ms" "$rust_common_ms" "$casa_noise_gain_ms" <<'PY'
import json
import sys
from pathlib import Path

import numpy as np
from casatools import table

outdir = Path(sys.argv[1])
clean_ms = sys.argv[2]
rust_noise_gain_ms = sys.argv[3]
rust_common_ms = sys.argv[4]
casa_noise_gain_ms = sys.argv[5]

def read_data(path):
    tb = table()
    tb.open(path)
    try:
        data = np.array(tb.getcol("DATA"), dtype=np.complex64)
        rows = int(tb.nrows())
    finally:
        tb.close()
    return data, rows

def stats(label, clean, corrupted):
    delta = corrupted - clean
    component_delta = np.concatenate([delta.real.reshape(-1), delta.imag.reshape(-1)])
    amp = np.abs(corrupted).reshape(-1)
    clean_amp = np.abs(clean).reshape(-1)
    return {
        "label": label,
        "max_abs_delta_jy": float(np.max(np.abs(delta))),
        "rms_abs_delta_jy": float(np.sqrt(np.mean(np.abs(delta) ** 2))),
        "component_stddev_delta_jy": float(np.std(component_delta)),
        "mean_amplitude_jy": float(np.mean(amp)),
        "mean_clean_amplitude_jy": float(np.mean(clean_amp)),
        "mean_amplitude_ratio": float(np.mean(amp) / np.mean(clean_amp)),
        "nonzero_delta_count": int(np.count_nonzero(np.abs(delta) > 0.0)),
    }

clean, rows = read_data(clean_ms)
rust_noise_gain, rust_rows = read_data(rust_noise_gain_ms)
rust_common, rust_common_rows = read_data(rust_common_ms)
casa_noise_gain, casa_rows = read_data(casa_noise_gain_ms)

summary = {
    "rows": {
        "clean": rows,
        "rust_noise_gain": rust_rows,
        "rust_common": rust_common_rows,
        "casa_noise_gain": casa_rows,
    },
    "shape": list(clean.shape),
    "comparisons": [
        stats("rust_noise_gain_minus_clean", clean, rust_noise_gain),
        stats("casa_noise_gain_minus_clean", clean, casa_noise_gain),
        stats("rust_common_minus_clean", clean, rust_common),
    ],
    "acceptance": {
        "casa_noise_gain_reference": "CASA simulator setnoise(mode='simplenoise', simplenoise='0.001Jy') plus setgain(mode='fbm', amplitude=[0.05, 0.02]) on the same clean MS",
        "rust_noise_gain_reference": "casa-rs deterministic --noise-simplenoise-jy plus CASA-like --gain-mode fbm --gain-amplitude real,imag on the same model setup",
        "rust_common_extra_effects": [
            "bandpass",
            "leakage",
            "pointing",
        ],
    },
}
with open(outdir / "wave5-issue126-corruption-summary.json", "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
print(json.dumps(summary, indent=2, sort_keys=True))
PY

echo "Wrote $outdir/wave5-issue126-corruption-summary.json"
