#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare casa-rs simulator MSs with CASA's (simulation Phases 1c and 1d, #841).

Run with CASA's Python, which reads both. For every scenario in the CASA
oracle's manifest, the comparison opens `<casa>/<name>.ms` and
`<casars>/<name>.ms` and checks:

- **the tables:** which subtables exist, and their row counts;
- **the descriptors:** each table's TableInfo (type and subType), its
  non-table keywords, and every column's value type, dimensionality,
  fixed-shape option and keywords (units and MEASINFO);
- **the cells:** every column, row by row. Floats pass within a per-column
  tolerance, and UVW's largest difference is reported.
- **DATA**, by the scenario's `data` (default `zero`):
  - `zero` (observe only): DATA is zero in both.
  - `model` (predicted, no noise): the normalized RMS of casa-rs against
    CASA, NRMS = sqrt(sum |casa-rs - CASA|^2 / sum |CASA|^2) over every
    complex sample of the rows FLAG_ROW leaves unflagged in both MSs (the
    convention of Phase 1a's `predict_vs_casa`), must be at most
    `--data-nrms` (1e-3, plan Phase 1 acceptance). It is also reported per
    correlation and for cross- and autocorrelation rows apart. A
    correlation with no reference power (the cross-hands of a Stokes I
    model) is checked by its largest difference instead, which must be at
    most 1e-6 of CASA's peak; a scenario with no reference power at all
    fails. Rows flagged in both are reported by their largest value.
  - `noisy` (predicted and corrupted): the draws differ, so each
    implementation's noise is its DATA minus its noise-free `twin`
    scenario's. Per correlation, the variance of the noise's real and
    imaginary parts on cross-correlations and of its real part on
    autocorrelations must be the scenario's `noise_variance` within five
    standard errors in both, the autocorrelations' imaginary noise must be
    zero, and a two-sample Kolmogorov-Smirnov test compares the two
    implementations' normalized cross-correlation noise.

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

# DATA of predicted scenarios: the plan's Phase 1 acceptance.
DATA_NRMS = 1e-3
# A correlation without reference power passes when its largest difference is
# at most this fraction of CASA's peak.
ZERO_POWER_FRACTION = 1e-6
# Noise variances pass within this many standard errors.
NOISE_STANDARD_ERRORS = 5.0

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
    parser.add_argument("--data-nrms", type=float, default=DATA_NRMS, help="largest DATA NRMS of a model scenario")
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


def normalise_keywords(value):
    """Keywords as JSON-comparable values (NumPy arrays become lists)."""
    if isinstance(value, dict):
        return {key: normalise_keywords(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.generic,)):
        return value.item()
    return value


def main_columns(tb, path: str) -> dict:
    """MAIN's DATA as [row, corr, chan], FLAG_ROW, and which rows are
    autocorrelations."""
    tb.open(path)
    try:
        data = np.transpose(tb.getcol("DATA"), (2, 0, 1))
        flag_row = np.asarray(tb.getcol("FLAG_ROW"), dtype=bool)
        auto = np.asarray(tb.getcol("ANTENNA1")) == np.asarray(tb.getcol("ANTENNA2"))
    finally:
        tb.close()
    return {"data": data.astype(np.complex128), "flag_row": flag_row, "auto": auto}


def nrms(ours: np.ndarray, casa: np.ndarray) -> float | None:
    """sqrt(sum |ours - casa|^2 / sum |casa|^2), or None without reference power."""
    reference = float(np.sum(np.abs(casa) ** 2))
    if reference == 0.0:
        return None
    return float(np.sqrt(np.sum(np.abs(ours - casa) ** 2) / reference))


def compare_model_data(casa: dict, ours: dict, limit: float) -> tuple[dict, list[str]]:
    """The NRMS of casa-rs's DATA against CASA's over rows unflagged in both,
    and the failures."""
    failures = []
    if casa["data"].shape != ours["data"].shape:
        return {}, [f"MAIN.DATA: shape {casa['data'].shape} against {ours['data'].shape}"]
    unflagged = ~casa["flag_row"] & ~ours["flag_row"]
    both_flagged = casa["flag_row"] & ours["flag_row"]
    a, b = casa["data"][unflagged], ours["data"][unflagged]
    stats = {
        "rows_compared": int(unflagged.sum()),
        "samples_compared": int(a.size),
        "rows_flagged_in_one": int((casa["flag_row"] ^ ours["flag_row"]).sum()),
        "rows_flagged_in_both": int(both_flagged.sum()),
        "nrms": nrms(b, a),
        "casa_peak": float(np.abs(a).max()) if a.size else 0.0,
        "max_difference": float(np.abs(b - a).max()) if a.size else 0.0,
    }
    if both_flagged.any():
        stats["flagged_rows_max_abs"] = {
            "casa": float(np.abs(casa["data"][both_flagged]).max()),
            "casa-rs": float(np.abs(ours["data"][both_flagged]).max()),
        }
    if stats["nrms"] is None:
        failures.append("MAIN.DATA: CASA's DATA has no power on the rows compared")
    elif stats["nrms"] > limit:
        failures.append(f"MAIN.DATA: NRMS {stats['nrms']:.3e} over the {limit:g} limit")
    per_correlation = []
    for corr in range(a.shape[1]):
        value = nrms(b[:, corr, :], a[:, corr, :])
        difference = float(np.abs(b[:, corr, :] - a[:, corr, :]).max()) if a.size else 0.0
        per_correlation.append({"nrms": value, "max_difference": difference})
        if value is None and difference > ZERO_POWER_FRACTION * max(stats["casa_peak"], 1e-30):
            failures.append(
                f"MAIN.DATA: correlation {corr} has no CASA power and differs by {difference:.3g}"
            )
    stats["per_correlation"] = per_correlation
    auto = casa["auto"][unflagged]
    stats["cross_nrms"] = nrms(b[~auto], a[~auto])
    stats["auto_nrms"] = nrms(b[auto], a[auto]) if auto.any() else None
    return stats, failures


def ks_two_sample(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """The two-sample Kolmogorov-Smirnov statistic and its asymptotic p-value."""
    x, y = np.sort(x), np.sort(y)
    values = np.concatenate([x, y])
    cdf_x = np.searchsorted(x, values, side="right") / x.size
    cdf_y = np.searchsorted(y, values, side="right") / y.size
    statistic = float(np.max(np.abs(cdf_x - cdf_y)))
    effective = np.sqrt(x.size * y.size / (x.size + y.size))
    lam = (effective + 0.12 + 0.11 / effective) * statistic
    terms = [2.0 * (-1.0) ** (k - 1) * np.exp(-2.0 * (k * lam) ** 2) for k in range(1, 101)]
    return statistic, float(min(1.0, max(0.0, sum(terms))))


def noise_classes(noise: np.ndarray, auto: np.ndarray, expected: float) -> tuple[dict, list[str]]:
    """Per-correlation noise variances by class, and their failures."""
    classes, failures = {}, []

    def check(label: str, values: np.ndarray) -> float:
        variance = float(np.var(values))
        tolerance = NOISE_STANDARD_ERRORS * expected * np.sqrt(2.0 / max(values.size, 1))
        if abs(variance - expected) > tolerance:
            failures.append(f"{label}: variance {variance:.5f}, expected {expected:.5f} +- {tolerance:.5f}")
        return variance

    for corr in range(noise.shape[1]):
        cross = noise[~auto, corr, :].ravel()
        autos = noise[auto, corr, :].ravel()
        entry = {
            "cross_re_var": check(f"correlation {corr} cross real", cross.real),
            "cross_im_var": check(f"correlation {corr} cross imaginary", cross.imag),
            "cross_re_im_corr": float(np.corrcoef(cross.real, cross.imag)[0, 1]),
            "n_cross": int(cross.size),
        }
        if autos.size:
            entry["auto_re_var"] = check(f"correlation {corr} auto real", autos.real)
            entry["auto_im_max"] = float(np.abs(autos.imag).max())
            if entry["auto_im_max"] != 0.0:
                failures.append(f"correlation {corr}: autocorrelation imaginary noise {entry['auto_im_max']:.3g}")
        classes[f"corr{corr}"] = entry
    return classes, failures


def compare_noisy_data(paths: dict, expected: float | None) -> tuple[dict, list[str]]:
    """Noise statistics of both implementations against their twins."""
    tb = table()
    if expected is None:
        return {}, ["MAIN.DATA: a noisy scenario needs noise_variance"]
    stats, failures, normalized = {}, [], {}
    for side in ("casa", "casa-rs"):
        noisy = main_columns(tb, paths[side])
        clean = main_columns(tb, paths[side + "-twin"])
        if noisy["data"].shape != clean["data"].shape:
            failures.append(f"{side}: the twin's DATA has another shape")
            continue
        noise = noisy["data"] - clean["data"]
        classes, side_failures = noise_classes(noise, noisy["auto"], expected)
        classes["flagged_rows"] = int(noisy["flag_row"].sum())
        stats[side] = classes
        failures += [f"{side} noise: {failure}" for failure in side_failures]
        cross = noise[~noisy["auto"]].ravel()
        normalized[side] = np.concatenate([cross.real, cross.imag]) / np.sqrt(expected)
    if len(normalized) == 2:
        statistic, p_value = ks_two_sample(normalized["casa"], normalized["casa-rs"])
        stats["ks_cross"] = {"statistic": statistic, "p_value": p_value}
        if p_value < 1e-3:
            failures.append(f"noise: two-sample KS p-value {p_value:.2g} (statistic {statistic:.4f})")
    return stats, failures


def compare_data(tb, scenario: dict, paths: dict, limit: float) -> tuple[dict, list[str]]:
    """DATA by the scenario's kind: zero, model or noisy."""
    kind = scenario.get("data") or "zero"
    if kind == "zero":
        failures = []
        for side in ("casa", "casa-rs"):
            data = main_columns(tb, paths[side])["data"]
            if np.any(data != 0):
                failures.append(f"MAIN.DATA: {side} has nonzero data")
        return {"kind": kind}, failures
    if kind == "model":
        stats, failures = compare_model_data(
            main_columns(tb, paths["casa"]), main_columns(tb, paths["casa-rs"]), limit
        )
        return dict(stats, kind=kind), failures
    if kind == "noisy":
        stats, failures = compare_noisy_data(paths, scenario.get("noise_variance"))
        return dict(stats, kind=kind), failures
    return {"kind": kind}, [f"unknown data kind {kind!r}"]


def compare_scenario(
    scenario: dict, casa_ms: str, casars_ms: str, args: argparse.Namespace, twins: dict | None = None
) -> dict:
    name = scenario["name"]
    ignore = set(scenario.get("ignore", []))
    uvw_tol = args.uvw_tolerance_m
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
            if label == "MAIN" and column == "DATA":
                paths = {"casa": casa_ms, "casa-rs": casars_ms, **(twins or {})}
                report["data"], failures = compare_data(tb, scenario, paths, args.data_nrms)
                for failure in failures:
                    differ("MAIN.DATA:values", failure)
                continue
            values_a = cells(tb, casa_path, column, a["rows"])
            values_b = cells(tb, casars_path, column, b["rows"])
            tolerance = uvw_tol if column == "UVW" else TOLERANCES.get(column, DEFAULT_TOLERANCE)
            aspect, largest, note = compare_values(values_a, values_b, tolerance)
            if largest is not None:
                report["largest"][qualified] = largest
            if aspect is not None:
                differ(f"{qualified}:{aspect}", f"{qualified}: {note}")
    return report


def describe_data(data: dict) -> str:
    """A one-line summary of a scenario's DATA comparison."""
    kind = data.get("kind")
    if kind == "model" and data.get("nrms") is not None:
        auto = data.get("auto_nrms")
        return (
            f"; DATA NRMS {data['nrms']:.2e} over {data['samples_compared']} samples"
            f" (cross {data['cross_nrms']:.2e}" + (f", auto {auto:.2e}" if auto is not None else "") + ")"
        )
    if kind == "noisy" and "casa" in data and "casa-rs" in data:
        variances = lambda side: ", ".join(  # noqa: E731
            f"{entry['cross_re_var']:.5f}/{entry['cross_im_var']:.5f}"
            for key, entry in data[side].items()
            if key.startswith("corr")
        )
        ks = data.get("ks_cross", {})
        return (
            f"; noise cross re/im variance CASA [{variances('casa')}], casa-rs [{variances('casa-rs')}]"
            + (f", KS p {ks['p_value']:.2g}" if ks else "")
        )
    return ""


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
        twins = None
        if scenario.get("twin"):
            twin = next(other for other in manifest["scenarios"] if other["name"] == scenario["twin"])
            twins = {"casa-twin": twin["ms"], "casa-rs-twin": os.path.join(args.casars, twin["name"] + ".ms")}
        reports.append(compare_scenario(scenario, scenario["ms"], casars_ms, args, twins))
    failed = 0
    for report in reports:
        uvw = report["largest"].get("MAIN.UVW")
        status = "OK" if not report["differences"] else f"{len(report['differences'])} differences"
        known = f", {len(report['known'])} known" if report["known"] else ""
        print(
            f"{report['name']}: {status}{known}"
            + (f" (UVW within {uvw:.3g} m)" if uvw is not None else "")
            + describe_data(report.get("data", {}))
        )
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
