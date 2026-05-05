#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  outdir="$1"
else
  outdir="target/wave6-issue163-m100-split-parity-$(date -u +%Y%m%dT%H%M%SZ)"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
casa_python="${CASA_RS_CASA_PYTHON:-/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python}"
tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
raw_extract="$tutorial_root/tutorial-parity/alma/m100/band3-combine/raw/extracted"

if [[ ! -x "$casa_python" ]]; then
  echo "CASA_RS_CASA_PYTHON must point at a Python with casatasks/casatools" >&2
  exit 1
fi

find_first() {
  local root="$1"
  local name="$2"
  if [[ ! -e "$root" ]]; then
    return 0
  fi
  find "$root" -name "$name" -print -quit 2>/dev/null || true
}

ms12="$(find_first "$raw_extract/M100_Band3_12m_CalibratedData" "M100_Band3_12m_CalibratedData.ms")"
ms7="$(find_first "$raw_extract/M100_Band3_7m_CalibratedData" "M100_Band3_7m_CalibratedData.ms")"

if [[ -z "$ms12" || -z "$ms7" ]]; then
  echo "Missing extracted 12m or 7m M100 MeasurementSet under $raw_extract" >&2
  echo "12m: ${ms12:-missing}" >&2
  echo "7m:  ${ms7:-missing}" >&2
  exit 1
fi

mkdir -p "$outdir/casa" "$outdir/rust" "$outdir/logs"

cargo build -p casa-ms --bin mstransform

"$casa_python" - "$ms12" "$ms7" "$outdir/casa" <<'PY' >"$outdir/logs/casa-split.log" 2>&1
from pathlib import Path
import sys
from casatasks import split

ms12 = Path(sys.argv[1])
ms7 = Path(sys.argv[2])
out = Path(sys.argv[3])

split(
    vis=str(ms12),
    outputvis=str(out / "M100_12m_CO.ms"),
    spw="0",
    field="M100",
    datacolumn="data",
    keepflags=False,
)
split(
    vis=str(ms7),
    outputvis=str(out / "M100_7m_CO.ms"),
    spw="3,5",
    field="M100",
    datacolumn="data",
    keepflags=False,
)
PY

rust_bin="$repo_root/target/debug/mstransform"
CASA_RS_MSTRANSFORM_PROGRESS=1 "$rust_bin" \
  --ms "$ms12" \
  --out "$outdir/rust/M100_12m_CO.ms" \
  --spw "0" \
  --field "1~47" \
  --datacolumn DATA \
  --no-keepflags >"$outdir/rust/M100_12m_CO.mstransform.json" 2>"$outdir/logs/rust-12m-mstransform.log"

CASA_RS_MSTRANSFORM_PROGRESS=1 "$rust_bin" \
  --ms "$ms7" \
  --out "$outdir/rust/M100_7m_CO.ms" \
  --spw "3,5" \
  --field "1~23" \
  --datacolumn DATA \
  --no-keepflags >"$outdir/rust/M100_7m_CO.mstransform.json" 2>"$outdir/logs/rust-7m-mstransform.log"

"$casa_python" - "$outdir" <<'PY'
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from casatasks import listobs
from casatools import table as table_tool

out = Path(sys.argv[1])

def table_rows(path: Path) -> int:
    tb = table_tool()
    tb.open(str(path))
    try:
        return int(tb.nrows())
    finally:
        tb.close()

def colnames(path: Path) -> list[str]:
    tb = table_tool()
    tb.open(str(path))
    try:
        return list(tb.colnames())
    finally:
        tb.close()

def scalar_counts(path: Path, column: str) -> dict[str, int]:
    tb = table_tool()
    tb.open(str(path))
    try:
        values = tb.getcol(column)
        counts = {}
        for value in values.tolist():
            key = str(int(value)) if isinstance(value, (int, float)) else str(value)
            counts[key] = counts.get(key, 0) + 1
        return dict(sorted(counts.items(), key=lambda item: int(item[0]) if item[0].lstrip("-").isdigit() else item[0]))
    finally:
        tb.close()

def first_data_shape(path: Path) -> list[int] | None:
    tb = table_tool()
    tb.open(str(path))
    try:
        if tb.nrows() == 0 or "DATA" not in tb.colnames():
            return None
        return list(tb.getcell("DATA", 0).shape)
    finally:
        tb.close()

def ms_summary(path: Path, listobs_name: str) -> dict:
    start = time.monotonic()
    listobs_path = out / listobs_name
    listobs(str(path), listfile=str(listobs_path), overwrite=True)
    item = {
        "path": str(path),
        "main_rows": table_rows(path),
        "main_columns": colnames(path),
        "first_data_shape": first_data_shape(path),
        "field_id_counts": scalar_counts(path, "FIELD_ID"),
        "data_desc_id_counts": scalar_counts(path, "DATA_DESC_ID"),
        "listobs": str(listobs_path),
        "subtables": {},
    }
    for subtable in ["FIELD", "SPECTRAL_WINDOW", "DATA_DESCRIPTION", "ANTENNA", "POLARIZATION"]:
        subpath = path / subtable
        if subpath.exists():
            item["subtables"][subtable] = {"rows": table_rows(subpath)}
    item["elapsed_s"] = time.monotonic() - start
    return item

products = {
    "12m": {
        "casa": out / "casa" / "M100_12m_CO.ms",
        "rust": out / "rust" / "M100_12m_CO.ms",
    },
    "7m": {
        "casa": out / "casa" / "M100_7m_CO.ms",
        "rust": out / "rust" / "M100_7m_CO.ms",
    },
}

summary = {
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "products": {},
}

for label, paths in products.items():
    casa = ms_summary(paths["casa"], f"casa-{label}.listobs")
    rust = ms_summary(paths["rust"], f"rust-{label}.listobs")
    summary["products"][label] = {
        "casa": casa,
        "rust": rust,
        "deltas": {
            "main_rows": rust["main_rows"] - casa["main_rows"],
            "field_subtable_rows": rust["subtables"].get("FIELD", {}).get("rows", 0) - casa["subtables"].get("FIELD", {}).get("rows", 0),
            "spw_subtable_rows": rust["subtables"].get("SPECTRAL_WINDOW", {}).get("rows", 0) - casa["subtables"].get("SPECTRAL_WINDOW", {}).get("rows", 0),
            "data_description_rows": rust["subtables"].get("DATA_DESCRIPTION", {}).get("rows", 0) - casa["subtables"].get("DATA_DESCRIPTION", {}).get("rows", 0),
        },
    }

(out / "split-parity-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))

lines = [
    "# Wave 6 Issue 163 M100 Split Parity",
    "",
    f"Created UTC: `{summary['created_utc']}`",
    "",
    "| Product | CASA rows | casa-rs rows | Row delta | CASA first DATA shape | casa-rs first DATA shape | CASA DDIDs | casa-rs DDIDs |",
    "|---|---:|---:|---:|---|---|---|---|",
]
for label, product in summary["products"].items():
    casa = product["casa"]
    rust = product["rust"]
    lines.append(
        f"| `{label}` | {casa['main_rows']} | {rust['main_rows']} | {product['deltas']['main_rows']} | "
        f"`{casa['first_data_shape']}` | `{rust['first_data_shape']}` | "
        f"`{casa['data_desc_id_counts']}` | `{rust['data_desc_id_counts']}` |"
    )
lines.extend(["", "## Notes", ""])
lines.append("- CASA oracle uses tutorial `split(..., field='M100', keepflags=False)`.")
lines.append("- casa-rs uses `mstransform --no-keepflags` with equivalent numeric M100 target field ranges: 12m `1~47`, 7m `1~23`.")
lines.append("- This report checks MAIN row counts, first DATA shape, DDID distribution, and key subtable row counts. Byte-level MS equality is not asserted because storage-manager layout and non-selected metadata ordering can differ.")
(out / "split-parity.md").write_text("\n".join(lines) + "\n")
PY

echo "Wrote $outdir"
