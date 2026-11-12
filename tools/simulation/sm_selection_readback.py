#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""C++ readback of the complete exported selection calculation matrix (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib

import numpy as np


def complex_values(values):
    values = np.asarray(values)
    return values[..., 0] + 1j * values[..., 1]


def moments(values):
    n = len(values)
    assert n >= 64 and np.isfinite(values).all()
    mean, variance = float(np.mean(values)), float(np.var(values))
    assert abs(mean) <= 5 * 0.1 / np.sqrt(n), (n, mean)
    assert abs(variance - 0.01) <= 5 * 0.01 * np.sqrt(2 / n), (n, variance)
    return {"samples": n, "mean": mean, "variance": variance}


def read(path):
    from casatools import ms, table

    tool = ms()
    assert tool.open(str(path))
    tool.close()
    tb = table()
    tb.open(str(path))
    try:
        result = {name: np.asarray(tb.getcol(name)) for name in (
            "ANTENNA1", "ANTENNA2", "TIME", "FIELD_ID", "DATA_DESC_ID", "SCAN_NUMBER",
            "DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA")}
        for column in ("MODEL_DATA", "CORRECTED_DATA"):
            if column in tb.colnames():
                result[column] = np.asarray(tb.getcol(column))
        return result
    finally:
        tb.close()


def verify(run, case, actual, cases, native_baseline=None):
    rows = case["rows"]
    assert actual["DATA"].shape == (2, 4, len(rows))
    data = actual["DATA"].transpose(2, 1, 0).reshape(len(rows), 8)
    reference = complex_values([row["data"] for row in rows])
    assert data.shape == reference.shape and np.isfinite(data).all()
    for column, key in (("ANTENNA1", "antennas"), ("ANTENNA2", "antennas")):
        index = int(column[-1]) - 1
        np.testing.assert_array_equal(actual[column], [row[key][index] for row in rows])
    for column, key in (("FIELD_ID", "field"), ("DATA_DESC_ID", "spw"), ("SCAN_NUMBER", "scan"),
                        ("TIME", "time_s"), ("FLAG_ROW", "flag_row")):
        np.testing.assert_array_equal(actual[column], [row[key] for row in rows])
    np.testing.assert_array_equal(actual["FLAG"].transpose(2, 1, 0).reshape(len(rows), 8),
                                  [row["flags"] for row in rows])
    for column in ("WEIGHT", "SIGMA"):
        np.testing.assert_array_equal(actual[column].T, [row[column.lower()] for row in rows])
    if not case.get("fresh"):
        model = actual["MODEL_DATA"].transpose(2, 1, 0).reshape(len(rows), 8)
        corrected = actual["CORRECTED_DATA"].transpose(2, 1, 0).reshape(len(rows), 8)
        np.testing.assert_array_equal(model, complex_values([row["model_data"] for row in rows]))
        untouched = complex_values([row["corrected_data"] for row in rows]) == -5 + 6j
        np.testing.assert_array_equal(corrected[untouched], np.full(np.count_nonzero(untouched), -5 + 6j))
        np.testing.assert_array_equal(corrected[~untouched], data[~untouched])
    if case.get("noise"):
        baseline = (complex_values([row["data"] for row in cases["all_replace"]["rows"]])
                    if case["name"] == "selected_prediction_noise" else np.full_like(data, 3 + 4j))
        selected = np.any(reference != baseline, axis=1)
        # CASA and native models have already passed the independent 1e-3
        # NRMS gate. Preservation is exact against the same native clean
        # model, so model rounding cannot be mistaken for changed rows.
        clean = native_baseline if native_baseline is not None else baseline
        np.testing.assert_array_equal(data[~selected], clean[~selected])
        residual = data - clean
        auto = actual["ANTENNA1"] == actual["ANTENNA2"]
        np.testing.assert_allclose(residual[selected & auto].imag, 0, rtol=0, atol=1e-6)
        cross = residual[selected & ~auto]
        statistics = {"cross_real": moments(cross.real.ravel()),
                      "cross_imaginary": moments(cross.imag.ravel())}
        if np.any(selected & auto):
            statistics["auto_real"] = moments(residual[selected & auto].real.ravel())
        return {**run, "noise": statistics}
    sentinel = 0 if case.get("fresh") else 3 + 4j
    unchanged = reference == sentinel
    np.testing.assert_array_equal(data[unchanged], reference[unchanged])
    changed = ~unchanged
    nrms = float(np.sqrt(np.sum(np.abs(data[changed] - reference[changed]) ** 2) /
                         np.sum(np.abs(reference[changed]) ** 2)))
    assert nrms <= 1e-3, (run, nrms)
    return {**run, "data_nrms": nrms, "rows": len(rows)}


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    with gzip.open(args.fixture, "rt") as handle:
        fixture = json.load(handle)
    assert fixture["casa_version"] == casatasks.version_string()
    cases = {case["name"]: case for case in fixture["cases"]}
    assert len(cases) == 40
    expected = {(name, backend, workers) for name in cases
                for backend, workers in (("Cpu", 1), ("Cpu", 3), ("Metal", 3))}
    seen, reports = set(), []
    for manifest in sorted(args.root.glob("*/case.json")):
        run = json.loads(manifest.read_text())
        key = (run["name"], run["backend"], run["workers"])
        assert key in expected and key not in seen, key
        seen.add(key)
        case = cases[run["name"]]
        path = manifest.parent / ("selection.ms" if case.get("fresh") else "existing.ms")
        native_baseline = None
        if case["name"] == "selected_prediction_noise":
            control = args.root / f"all_replace-{run['backend']}-w{run['workers']}" / "existing.ms"
            native_baseline = read(control)["DATA"].transpose(2, 1, 0).reshape(len(case["rows"]), 8)
        reports.append(verify(run, case, read(path), cases, native_baseline))
    assert seen == expected, ("missing", sorted(expected - seen))
    args.out.write_text(json.dumps({"casa_version": casatasks.version_string(), "runs": reports},
                                   indent=2, allow_nan=False) + "\n")
    print("C++ readback:", len(reports), "native selection outputs passed", flush=True)


if __name__ == "__main__":
    main()
