#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Flagged/missing G/D application, retaining CASA's invalid-leakage defect (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import subprocess
import sys

import numpy as np

import sm_apply_parameters_oracle as parameters

PROBES = ("flag_first_x", "flag_all_x", "flag_first_y", "flag_last_y",
          "flag_all_both", "missing_antenna")
CASES = [{"name": f"{kind}_{interp}_{probe}", "kind": kind, "interp": interp,
          "options": {}, "probe": probe}
         for kind in ("G", "D") for interp in ("nearest", "linear") for probe in PROBES]


def calibration(sm, path, kind, bounds, probe, neutral):
    from casatools import table

    info = parameters.calibration(sm, path, kind, bounds)
    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        antennas, times = tb.getcol("ANTENNA1"), tb.getcol("TIME")
        missing = probe == "missing_antenna"
        bad_antenna = 7 if missing else 0
        selected = antennas == bad_antenna
        if neutral:
            values = tb.getcol("CPARAM")
            values[:, :, selected] = 0
            tb.putcol("CPARAM", values)
        elif missing:
            tb.removerows(np.flatnonzero(selected).tolist())
        else:
            if "first" in probe:
                selected &= times == bounds[0]
            elif "last" in probe:
                selected &= times == bounds[1]
            receptors = [0, 1] if probe == "flag_all_both" else [int(probe.endswith("y"))]
            flags = tb.getcol("FLAG")
            for receptor in receptors:
                flags[receptor, 0, selected] = True
            tb.putcol("FLAG", flags)
        return {**info, "rows": tb.nrows(), "bad_antenna": bad_antenna}
    finally:
        tb.close()


def affected(case, row):
    if case["calibration"]["bad_antenna"] not in row["antennas"]:
        return False
    first, last = (case["calibration"][key] for key in ("first_time_s", "last_time_s"))
    if "first" in case["probe"]:
        return row["time_s"] < last if case["interp"] == "linear" else row["time_s"] <= (first + last) / 2
    if "last" in case["probe"]:
        return row["time_s"] > first if case["interp"] == "linear" else row["time_s"] > (first + last) / 2
    return True


def save(path, cases):
    import casatasks

    record = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sm_apply_flags_oracle.py", "cases": cases}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture, neutral):
    original = parameters.calibration
    # The wrapper retains the ordinary independent input recipe.
    def build(sm, path, kind, bounds):
        parameters.calibration = original
        try:
            return calibration(sm, path, kind, bounds, spec["probe"], neutral)
        finally:
            parameters.calibration = build
    parameters.calibration = build
    parameters.worker(spec, out, fixture)


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--controls", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=[case["name"] for case in CASES])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--neutral", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    args.out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES if not args.only or case["name"] == args.only]
    if args.worker:
        assert len(selected) == 1
        worker(selected[0], args.out, args.fixture, args.neutral)
        return
    with gzip.open(args.controls, "rt") as handle:
        controls = {case["name"]: case for case in json.load(handle)["cases"]}
    results = []
    for spec in selected:
        runs = []
        for neutral in ([False, True] if spec["kind"] == "D" else [False]):
            out = args.out / (spec["name"] + ("_neutral" if neutral else ""))
            out.mkdir()
            fixture = out / "result.json.gz"
            command = [sys.executable, __file__, "--out", str(out), "--fixture", str(fixture),
                       "--controls", str(args.controls), "--only", spec["name"], "--worker"]
            if neutral:
                command.append("--neutral")
            with (out / "worker.log").open("w") as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
            with gzip.open(fixture, "rt") as handle:
                runs.append(json.load(handle)["cases"][0])
        case = runs[0]
        if spec["kind"] == "D":
            case["casa_unmodified_rows"] = case["rows"]
            case["neutral_control_rows"] = runs[1]["rows"]
            control = controls[f"D_{spec['interp']}_control"]["rows"]
            case["rows"] = [neutral if affected(case, raw) else valid
                            for raw, neutral, valid in zip(case["casa_unmodified_rows"], runs[1]["rows"], control)]
            case["exception"] = "Invalid D coefficients are zero (neutral leakage), not CASA's one."
            assert any(a["data"] != b["data"] for a, b in zip(case["rows"], case["casa_unmodified_rows"]))
        results.append(case)
        save(args.fixture, results)
        print(spec["name"], "passed", flush=True)


if __name__ == "__main__":
    main()
