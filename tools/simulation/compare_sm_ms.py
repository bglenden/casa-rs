#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare casa-rs simulator MSs with CASA's (simulation Phase 1c, #841).

Run with CASA's Python, which reads both. For every scenario in the CASA
oracle's manifest, the comparison opens `<casa>/<name>.ms` and
`<casars>/<name>.ms` and checks:

- **the tables:** which subtables exist, and their row counts;
- **the descriptors:** each table's TableInfo (type and subType), its
  non-table keywords, and every column's value type, dimensionality,
  fixed-shape option and keywords (units and MEASINFO);
- **the cells:** every column, row by row. DATA must be zero in both. Floats
  pass within a per-column tolerance, and UVW's largest difference is
  reported.

Data managers are not compared; the plan lets them differ. A scenario's
`ignore` entries (`TABLE.COLUMN`) are skipped. The exit status is 1 if any
scenario differs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np
from casatools import table

# Absolute tolerances by column; other float columns use DEFAULT_TOLERANCE.
TOLERANCES = {
    "UVW": 1e-3,
    "TIME": 1e-6,
    "TIME_CENTROID": 1e-6,
    "TIME_RANGE": 1e-6,
    "TIME_ORIGIN": 1e-6,
    "POSITION": 1e-6,
    "DIRECTION": 1e-12,
    "TARGET": 1e-12,
    "PHASE_DIR": 1e-12,
    "DELAY_DIR": 1e-12,
    "REFERENCE_DIR": 1e-12,
}
DEFAULT_TOLERANCE = 1e-9


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa", required=True, help="CASA oracle output (with manifest.json)")
    parser.add_argument("--casars", required=True, help="casa-rs output directory")
    parser.add_argument("--only", nargs="*", help="compare only these scenarios")
    parser.add_argument("--uvw-tolerance-m", type=float, default=TOLERANCES["UVW"])
    parser.add_argument("--json", help="write the report here as JSON")
    return parser.parse_args()


def subtables(tb, path: str) -> dict[str, str]:
    tb.open(path)
    keywords = tb.getkeywords()
    tb.close()
    return {
        name: value.split("Table: ", 1)[1]
        for name, value in keywords.items()
        if isinstance(value, str) and value.startswith("Table: ")
    }


def describe(tb, path: str) -> dict:
    tb.open(path)
    info = tb.info()
    keywords = {
        key: value
        for key, value in tb.getkeywords().items()
        if not (isinstance(value, str) and value.startswith("Table: "))
    }
    columns = {}
    for name in tb.colnames():
        desc = tb.getcoldesc(name)
        columns[name] = {
            "valueType": desc.get("valueType"),
            "ndim": desc.get("ndim", 0),
            "shape": list(desc.get("shape", [])),
            "option": desc.get("option", 0),
            "keywords": desc.get("keywords", {}),
        }
    rows = tb.nrows()
    tb.close()
    return {"info": info, "keywords": keywords, "columns": columns, "rows": rows}


def cells(tb, path: str, column: str, rows: int):
    tb.open(path)
    try:
        if rows == 0:
            return []
        values = []
        for row in range(rows):
            values.append(tb.getcell(column, row) if tb.iscelldefined(column, row) else None)
        return values
    finally:
        tb.close()


def compare_values(name: str, a, b, tolerance: float) -> tuple[bool, float | None, str]:
    """Whether two cell lists agree, the largest numeric difference, and a note."""
    if len(a) != len(b):
        return False, None, f"{len(a)} against {len(b)} cells"
    largest = 0.0
    numeric = False
    for row, (x, y) in enumerate(zip(a, b)):
        if (x is None) != (y is None):
            return False, None, f"row {row}: defined {x is not None} against {y is not None}"
        if x is None:
            continue
        xa, ya = np.asarray(x), np.asarray(y)
        if xa.shape != ya.shape:
            return False, None, f"row {row}: shape {xa.shape} against {ya.shape}"
        if xa.dtype.kind in "fc" or ya.dtype.kind in "fc":
            numeric = True
            if xa.size:
                difference = float(np.max(np.abs(xa.astype(complex) - ya.astype(complex))))
                if not np.isfinite(difference):
                    return False, None, f"row {row}: non-finite"
                largest = max(largest, difference)
        elif not np.array_equal(xa, ya):
            return False, None, f"row {row}: {x!r} against {y!r}"
    if numeric and largest > tolerance:
        return False, largest, f"largest difference {largest:.3g} over {tolerance:.3g}"
    return True, (largest if numeric else None), ""


def normalise_keywords(value):
    """Keywords as JSON-comparable values (NumPy arrays become lists)."""
    if isinstance(value, dict):
        return {key: normalise_keywords(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.generic,)):
        return value.item()
    return value


def compare_scenario(name: str, casa_ms: str, casars_ms: str, ignore: set[str], uvw_tol: float) -> dict:
    tb = table()
    report = {"name": name, "differences": [], "largest": {}}
    differ = report["differences"].append
    tables = {"MAIN": ("", "")}
    casa_sub = subtables(tb, casa_ms)
    casars_sub = subtables(tb, casars_ms)
    for missing in sorted(set(casa_sub) - set(casars_sub)):
        differ(f"subtable {missing} missing from casa-rs")
    for extra in sorted(set(casars_sub) - set(casa_sub)):
        differ(f"subtable {extra} only in casa-rs")
    for sub in sorted(set(casa_sub) & set(casars_sub)):
        tables[sub] = (sub, sub)
    for label, (casa_rel, casars_rel) in tables.items():
        casa_path = os.path.join(casa_ms, casa_rel) if casa_rel else casa_ms
        casars_path = os.path.join(casars_ms, casars_rel) if casars_rel else casars_ms
        a = describe(tb, casa_path)
        b = describe(tb, casars_path)
        if a["rows"] != b["rows"]:
            differ(f"{label}: {a['rows']} rows in CASA, {b['rows']} in casa-rs")
        for key in ("type", "subType"):
            if a["info"].get(key) != b["info"].get(key):
                differ(f"{label}: TableInfo {key} {a['info'].get(key)!r} against {b['info'].get(key)!r}")
        if normalise_keywords(a["keywords"]) != normalise_keywords(b["keywords"]):
            differ(f"{label}: keywords {normalise_keywords(a['keywords'])} against {normalise_keywords(b['keywords'])}")
        for column in sorted(set(a["columns"]) - set(b["columns"])):
            differ(f"{label}.{column}: missing from casa-rs")
        for column in sorted(set(b["columns"]) - set(a["columns"])):
            differ(f"{label}.{column}: only in casa-rs")
        for column in sorted(set(a["columns"]) & set(b["columns"])):
            qualified = f"{label}.{column}"
            if qualified in ignore:
                continue
            da, db = a["columns"][column], b["columns"][column]
            for key in ("valueType", "ndim", "shape"):
                if da[key] != db[key]:
                    differ(f"{qualified}: descriptor {key} {da[key]!r} against {db[key]!r}")
            if (da["option"] & 4) != (db["option"] & 4):
                differ(f"{qualified}: fixed-shape option {da['option']} against {db['option']}")
            ka, kb = normalise_keywords(da["keywords"]), normalise_keywords(db["keywords"])
            if ka != kb:
                differ(f"{qualified}: column keywords {ka} against {kb}")
            if a["rows"] != b["rows"]:
                continue
            values_a = cells(tb, casa_path, column, a["rows"])
            values_b = cells(tb, casars_path, column, b["rows"])
            if label == "MAIN" and column == "DATA":
                for side, values in (("CASA", values_a), ("casa-rs", values_b)):
                    if any(v is not None and np.any(np.asarray(v) != 0) for v in values):
                        differ(f"MAIN.DATA: {side} has nonzero data")
                continue
            tolerance = uvw_tol if column == "UVW" else TOLERANCES.get(column, DEFAULT_TOLERANCE)
            ok, largest, note = compare_values(qualified, values_a, values_b, tolerance)
            if largest is not None:
                report["largest"][qualified] = largest
            if not ok:
                differ(f"{qualified}: {note}")
    return report


def main() -> int:
    args = parse_args()
    with open(os.path.join(args.casa, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    reports = []
    for scenario in manifest["scenarios"]:
        name = scenario["name"]
        if args.only and name not in args.only:
            continue
        casars_ms = os.path.join(args.casars, name + ".ms")
        if not os.path.isdir(casars_ms):
            reports.append({"name": name, "differences": ["casa-rs MS missing"], "largest": {}})
            continue
        reports.append(
            compare_scenario(name, scenario["ms"], casars_ms, set(scenario.get("ignore", [])), args.uvw_tolerance_m)
        )
    failed = 0
    for report in reports:
        uvw = report["largest"].get("MAIN.UVW")
        status = "OK" if not report["differences"] else f"{len(report['differences'])} differences"
        print(f"{report['name']}: {status}" + (f" (UVW within {uvw:.3g} m)" if uvw is not None else ""))
        for line in report["differences"][:40]:
            print("   ", line)
        if len(report["differences"]) > 40:
            print(f"    ... {len(report['differences']) - 40} more")
        failed += bool(report["differences"])
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(reports, handle, indent=2, default=str)
            handle.write("\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
