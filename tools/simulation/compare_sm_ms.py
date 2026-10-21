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
- **the cells:** every column, row by row: which cells are defined, their
  shapes and their values. DATA must also be zero in both. Floats pass
  within a per-column tolerance, and UVW's largest difference is reported.

Data managers are not compared; the plan lets them differ. A scenario's
`ignore` entries (`TABLE.COLUMN`) are skipped. The exit status is 1 if any
scenario differs.

`test_compare_sm_ms.py` tests the cell comparison; it needs only NumPy, as
`casatools` is imported only to read tables.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np

# Absolute tolerances by column, added to RELATIVE_TOLERANCE times the
# value; other float columns use DEFAULT_TOLERANCE. Times are seconds near
# 5e9, where a double's spacing is about 1e-6 s.
TOLERANCES = {
    "UVW": 1e-3,
    "TIME": 2e-6,
    "TIME_CENTROID": 2e-6,
    "TIME_RANGE": 2e-6,
    "TIME_ORIGIN": 2e-6,
    "POSITION": 1e-6,
    "DIRECTION": 1e-12,
    "TARGET": 1e-12,
    "PHASE_DIR": 1e-12,
    "DELAY_DIR": 1e-12,
    "REFERENCE_DIR": 1e-12,
}
DEFAULT_TOLERANCE = 1e-9
RELATIVE_TOLERANCE = 1e-14

# Differences that are expected, with the reason. A difference whose key
# (`TABLE.COLUMN:aspect`) is listed is reported as known and does not fail.
KNOWN = {
    "MAIN.MODEL_DATA:missing": "plan decision 6: DATA only",
    "MAIN.CORRECTED_DATA:missing": "plan decision 6: DATA only",
    "MAIN.UVW:keywords": "casa-ms's UVW MEASINFO lacks casacore's default Ref ITRF (#847)",
    "POINTING.TARGET:ndim": "casa-ms's POINTING.TARGET has ndim 2, casacore's -1 (#847)",
    "SOURCE.DIRECTION:ndim": "CASA's simulator makes SOURCE arrays variable-shaped; casa-ms follows casacore's MSSource (fixed [2])",
    "SOURCE.DIRECTION:shape": "as SOURCE.DIRECTION:ndim",
    "SOURCE.DIRECTION:option": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:ndim": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:shape": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:option": "as SOURCE.DIRECTION:ndim",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa", required=True, help="CASA oracle output (with manifest.json)")
    parser.add_argument("--casars", required=True, help="casa-rs output directory")
    parser.add_argument("--only", nargs="*", help="compare only these scenarios")
    parser.add_argument("--uvw-tolerance-m", type=float, default=TOLERANCES["UVW"])
    parser.add_argument("--json", help="write the report here as JSON")
    parser.add_argument("--verbose", action="store_true", help="also list the known differences")
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
            if not tb.iscelldefined(column, row):
                values.append(None)
                continue
            try:
                values.append(tb.getcell(column, row))
            except RuntimeError as error:
                values.append(f"<unreadable: {error}>")
        return values
    finally:
        tb.close()


def compare_values(a, b, tolerance: float) -> tuple[str | None, float | None, str]:
    """How two cell lists differ (None if they agree, else the aspect), the
    largest numeric difference, and a note. Floats agree within `tolerance`
    plus RELATIVE_TOLERANCE of their size."""
    if len(a) != len(b):
        return "cells", None, f"{len(a)} against {len(b)} cells"
    largest = 0.0
    numeric = False
    for row, (x, y) in enumerate(zip(a, b)):
        if (x is None) != (y is None) or isinstance(x, str) != isinstance(y, str):
            return "defined", None, f"row {row}: CASA {'undefined' if x is None else repr(x)[:40]}, casa-rs {'undefined' if y is None else repr(y)[:40]}"
        if x is None:
            continue
        xa, ya = np.asarray(x), np.asarray(y)
        if xa.shape != ya.shape:
            return "shape", None, f"row {row}: shape {xa.shape} against {ya.shape}"
        if xa.dtype.kind in "fc" or ya.dtype.kind in "fc":
            numeric = True
            if xa.size:
                xc, yc = xa.astype(complex), ya.astype(complex)
                difference = np.abs(xc - yc)
                if not np.all(np.isfinite(difference)):
                    return "values", None, f"row {row}: non-finite"
                largest = max(largest, float(np.max(difference)))
                allowed = tolerance + RELATIVE_TOLERANCE * np.maximum(np.abs(xc), np.abs(yc))
                if np.any(difference > allowed):
                    index = int(np.argmax(difference - allowed))
                    return "values", largest, (
                        f"row {row}: difference {float(difference.flat[index]):.3g} over "
                        f"{float(allowed.flat[index]):.3g} ({xc.flat[index]} against {yc.flat[index]})"
                    )
        elif not np.array_equal(xa, ya):
            return "values", None, f"row {row}: {x!r} against {y!r}"
    return None, (largest if numeric else None), ""


def data_differences(casa: list, casars: list) -> list[tuple[str, str]]:
    """MAIN.DATA's differences, as (`TABLE.COLUMN:aspect`, message) pairs:
    nonzero data on either side, then the comparison every column gets
    (which cells are defined, their shapes and their values)."""
    found = []
    for side, values in (("CASA", casa), ("casa-rs", casars)):
        if any(value is not None and np.any(np.asarray(value) != 0) for value in values):
            found.append(("MAIN.DATA:nonzero", f"MAIN.DATA: {side} has nonzero data"))
    aspect, _, note = compare_values(casa, casars, TOLERANCES.get("DATA", DEFAULT_TOLERANCE))
    if aspect is not None:
        found.append((f"MAIN.DATA:{aspect}", f"MAIN.DATA: {note}"))
    return found


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
    from casatools import table

    tb = table()
    report = {"name": name, "differences": [], "known": [], "largest": {}}

    def differ(key: str, message: str) -> None:
        if key in KNOWN:
            report["known"].append(f"{message} [known: {KNOWN[key]}]")
        else:
            report["differences"].append(message)

    tables = {"MAIN": ("", "")}
    casa_sub = subtables(tb, casa_ms)
    casars_sub = subtables(tb, casars_ms)
    for missing in sorted(set(casa_sub) - set(casars_sub)):
        differ(f"{missing}:missing", f"subtable {missing} missing from casa-rs")
    for extra in sorted(set(casars_sub) - set(casa_sub)):
        differ(f"{extra}:extra", f"subtable {extra} only in casa-rs")
    for sub in sorted(set(casa_sub) & set(casars_sub)):
        tables[sub] = (sub, sub)
    for label, (casa_rel, casars_rel) in tables.items():
        casa_path = os.path.join(casa_ms, casa_rel) if casa_rel else casa_ms
        casars_path = os.path.join(casars_ms, casars_rel) if casars_rel else casars_ms
        a = describe(tb, casa_path)
        b = describe(tb, casars_path)
        if a["rows"] != b["rows"]:
            differ(f"{label}:rows", f"{label}: {a['rows']} rows in CASA, {b['rows']} in casa-rs")
        for key in ("type", "subType", "readme"):
            if a["info"].get(key) != b["info"].get(key):
                differ(f"{label}:info", f"{label}: TableInfo {key} {a['info'].get(key)!r} against {b['info'].get(key)!r}")
        if normalise_keywords(a["keywords"]) != normalise_keywords(b["keywords"]):
            differ(f"{label}:keywords", f"{label}: keywords {normalise_keywords(a['keywords'])} against {normalise_keywords(b['keywords'])}")
        for column in sorted(set(a["columns"]) - set(b["columns"])):
            differ(f"{label}.{column}:missing", f"{label}.{column}: missing from casa-rs")
        for column in sorted(set(b["columns"]) - set(a["columns"])):
            differ(f"{label}.{column}:extra", f"{label}.{column}: only in casa-rs")
        for column in sorted(set(a["columns"]) & set(b["columns"])):
            qualified = f"{label}.{column}"
            if qualified in ignore:
                continue
            da, db = a["columns"][column], b["columns"][column]
            for key in ("valueType", "ndim", "shape"):
                if da[key] != db[key]:
                    differ(f"{qualified}:{key}", f"{qualified}: descriptor {key} {da[key]!r} against {db[key]!r}")
            if (da["option"] & 4) != (db["option"] & 4):
                differ(f"{qualified}:option", f"{qualified}: fixed-shape option {da['option']} against {db['option']}")
            ka, kb = normalise_keywords(da["keywords"]), normalise_keywords(db["keywords"])
            if ka != kb:
                differ(f"{qualified}:keywords", f"{qualified}: column keywords {ka} against {kb}")
            if a["rows"] != b["rows"]:
                continue
            values_a = cells(tb, casa_path, column, a["rows"])
            values_b = cells(tb, casars_path, column, b["rows"])
            if label == "MAIN" and column == "DATA":
                for key, message in data_differences(values_a, values_b):
                    differ(key, message)
                continue
            tolerance = uvw_tol if column == "UVW" else TOLERANCES.get(column, DEFAULT_TOLERANCE)
            aspect, largest, note = compare_values(values_a, values_b, tolerance)
            if largest is not None:
                report["largest"][qualified] = largest
            if aspect is not None:
                differ(f"{qualified}:{aspect}", f"{qualified}: {note}")
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
            reports.append({"name": name, "differences": ["casa-rs MS missing"], "known": [], "largest": {}})
            continue
        reports.append(
            compare_scenario(name, scenario["ms"], casars_ms, set(scenario.get("ignore", [])), args.uvw_tolerance_m)
        )
    failed = 0
    for report in reports:
        uvw = report["largest"].get("MAIN.UVW")
        status = "OK" if not report["differences"] else f"{len(report['differences'])} differences"
        known = f", {len(report['known'])} known" if report["known"] else ""
        print(f"{report['name']}: {status}{known}" + (f" (UVW within {uvw:.3g} m)" if uvw is not None else ""))
        for line in report["differences"][:40]:
            print("   ", line)
        if len(report["differences"]) > 40:
            print(f"    ... {len(report['differences']) - 40} more")
        if args.verbose:
            for line in report["known"]:
                print("    known:", line)
        failed += bool(report["differences"])
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(reports, handle, indent=2, default=str)
            handle.write("\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
