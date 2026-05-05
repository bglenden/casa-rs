#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  outdir="$1"
else
  outdir="target/wave6-issue163-m100-raw-preflight-$(date -u +%Y%m%dT%H%M%SZ)"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
casa_python="${CASA_RS_CASA_PYTHON:-/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
raw_dir="$tutorial_root/tutorial-parity/alma/m100/band3-combine/raw"
extract_dir="$raw_dir/extracted"

if [[ ! -x "$casa_python" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 1
fi

mkdir -p "$outdir"

"$repo_root/scripts/stage-wave6-issue163-m100-raw.sh" status >"$outdir/stage-status.txt"

find_first() {
  local root="$1"
  local name="$2"
  if [[ ! -e "$root" ]]; then
    return 0
  fi
  find "$root" -name "$name" -print -quit 2>/dev/null || true
}

ms12="$(find_first "$extract_dir/M100_Band3_12m_CalibratedData" "M100_Band3_12m_CalibratedData.ms")"
ms7="$(find_first "$extract_dir/M100_Band3_7m_CalibratedData" "M100_Band3_7m_CalibratedData.ms")"
tp_image="$(find_first "$extract_dir" "M100_TP_CO_cube.spw3.image.bl")"
aca_root="$(find_first "$extract_dir/M100_Band3_ACA_ReferenceImages_5.1" "M100_Band3_ACA_ReferenceImages_5.1")"

cat >"$outdir/tutorial-sequence.json" <<'JSON'
{
  "guide": "M100 Band 3 Combine",
  "guide_url": "https://casaguides.nrao.edu/index.php/M100_Band3_Combine_6.6.6",
  "issue": 163,
  "wave_issue": 143,
  "required_inputs": [
    "M100_Band3_12m_CalibratedData.ms",
    "M100_Band3_7m_CalibratedData.ms",
    "M100_TP_CO_cube.spw3.image.bl"
  ],
  "casa_sequence": [
    "listobs 12m and 7m calibrated MS",
    "split 12m SPW 0 field M100 to M100_12m_CO.ms",
    "split 7m SPWs 3,5 field M100 to M100_7m_CO.ms",
    "tclean dirty 12m+7m cube with gridder=mosaic, specmode=cube, niter=0",
    "imstat dirty cube line-free channels 4 and 66",
    "tclean clean 12m+7m cube with automultithresh, mosweight=True, pbcor=True",
    "immoments combined cube moment 0 and moment 1 with PB mask and includepix thresholds",
    "imsubimage PB channel 35 and impbcor combined moment 0",
    "exportfits combined cube, PB, moment maps",
    "imhead restfreq checks for TP and combined cube",
    "imregrid TP cube to combined cube direction axes",
    "imsubimage TP, combined cube, and combined PB to guide box 219,148,612,579",
    "immath TP subimage multiplied by combined PB subimage",
    "feather combined cube subimage with PB-weighted TP subimage",
    "immoments TP and feathered cube moment maps",
    "immath feathered cube and moment 0 PB correction",
    "imstat total flux checks"
  ]
}
JSON

"$casa_python" - "$outdir" "${ms12:-}" "${ms7:-}" "${tp_image:-}" "${aca_root:-}" <<'PY'
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from casatasks import imhead, imstat, listobs
from casatools import table as table_tool

out = Path(sys.argv[1])
inputs = {
    "m100_12m_ms": Path(sys.argv[2]) if sys.argv[2] else None,
    "m100_7m_ms": Path(sys.argv[3]) if sys.argv[3] else None,
    "m100_tp_image": Path(sys.argv[4]) if sys.argv[4] else None,
    "m100_aca_reference_root": Path(sys.argv[5]) if sys.argv[5] else None,
}

def table_rows(path: Path) -> int:
    tb = table_tool()
    tb.open(str(path))
    try:
        return int(tb.nrows())
    finally:
        tb.close()

def table_column_names(path: Path) -> list[str]:
    tb = table_tool()
    tb.open(str(path))
    try:
        return list(tb.colnames())
    finally:
        tb.close()

def first_data_shape(path: Path) -> list[int] | None:
    tb = table_tool()
    tb.open(str(path))
    try:
        if "DATA" not in tb.colnames() or tb.nrows() == 0:
            return None
        cell = tb.getcell("DATA", 0)
        return list(cell.shape)
    finally:
        tb.close()

def ms_summary(path: Path) -> dict:
    start = time.monotonic()
    summary = {
        "path": str(path),
        "main_rows": table_rows(path),
        "main_columns": table_column_names(path),
        "first_data_shape": first_data_shape(path),
        "subtables": {},
    }
    for subtable in [
        "FIELD",
        "SPECTRAL_WINDOW",
        "DATA_DESCRIPTION",
        "POLARIZATION",
        "ANTENNA",
        "OBSERVATION",
        "STATE",
    ]:
        subpath = path / subtable
        if subpath.exists():
            summary["subtables"][subtable] = {
                "rows": table_rows(subpath),
                "columns": table_column_names(subpath),
            }
    summary["elapsed_s"] = time.monotonic() - start
    return summary

def json_safe(value):
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value

summary = {
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "inputs": {key: str(value) if value else None for key, value in inputs.items()},
    "measurement_sets": {},
    "images": {},
    "blockers": [],
}

for key in ["m100_12m_ms", "m100_7m_ms"]:
    path = inputs[key]
    if path and path.exists():
        listobs_path = out / f"{key}.listobs"
        listobs(str(path), listfile=str(listobs_path), overwrite=True)
        summary["measurement_sets"][key] = ms_summary(path)
        summary["measurement_sets"][key]["listobs"] = str(listobs_path)
    else:
        summary["blockers"].append(f"missing extracted input: {key}")

tp_path = inputs["m100_tp_image"]
if tp_path and tp_path.exists():
    summary["images"]["m100_tp_image"] = {
        "path": str(tp_path),
        "shape": json_safe(imhead(str(tp_path), mode="get", hdkey="shape")),
        "restfreq": json_safe(imhead(str(tp_path), mode="get", hdkey="restfreq")),
        "channel_4_rms": float(imstat(str(tp_path), chans="4")["rms"][0]),
        "channel_66_rms": float(imstat(str(tp_path), chans="66")["rms"][0]),
    }
else:
    summary["blockers"].append("missing extracted input: m100_tp_image")

(out / "raw-preflight-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))

lines = [
    "# Wave 6 Issue 163 Raw Preflight",
    "",
    f"Created UTC: `{summary['created_utc']}`",
    "",
    "## Inputs",
]
for key, value in summary["inputs"].items():
    lines.append(f"- `{key}`: `{value}`")
lines.extend(["", "## Measurement Sets"])
for key, item in summary["measurement_sets"].items():
    lines.append(
        f"- `{key}`: rows={item['main_rows']}, fields={item['subtables'].get('FIELD', {}).get('rows')}, "
        f"spws={item['subtables'].get('SPECTRAL_WINDOW', {}).get('rows')}, "
        f"antennas={item['subtables'].get('ANTENNA', {}).get('rows')}, "
        f"first DATA shape={item['first_data_shape']}, elapsed_s={item['elapsed_s']:.3f}"
    )
lines.extend(["", "## Images"])
for key, item in summary["images"].items():
    lines.append(
        f"- `{key}`: shape={item['shape']}, restfreq={item['restfreq']}, "
        f"rms(ch4)={item['channel_4_rms']:.9g}, rms(ch66)={item['channel_66_rms']:.9g}"
    )
lines.extend(["", "## Blockers"])
if summary["blockers"]:
    for blocker in summary["blockers"]:
        lines.append(f"- {blocker}")
else:
    lines.append("- none")
(out / "raw-preflight.md").write_text("\n".join(lines) + "\n")
PY

echo "Wrote $outdir"
