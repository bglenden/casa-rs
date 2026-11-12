#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Scalar T-table corruption across correlations, interpolation and flags (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import subprocess
import sys

import numpy as np

import sm_apply_parameters_oracle as parameters
from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session

LAYOUTS = {"one": "RR", "parallel": "RR LL", "full": "RR RL LR LL"}
PROBES = ("control", "flag_first_x", "flag_all_x", "missing_antenna")
CASES = [{"name": f"T_{interp}_{layout}_{probe}", "kind": "T", "interp": interp,
          "stokes": stokes, "probe": probe, "options": {}}
         for interp in ("nearest", "linear") for layout, stokes in LAYOUTS.items()
         for probe in PROBES]
CASES += [{"name": f"T_{interp}_full_{name}", "kind": "T", "interp": interp,
           "stokes": LAYOUTS["full"], "probe": "control", "options": options}
          for interp in ("nearest", "linear")
          for name, options in {**parameters.OPTIONS, "explicit_type": {"type": "T"}}.items()
          if name != "control"]


def calibration(sm, path, bounds, probe):
    from casatools import table

    info = parameters.calibration(sm, path, "G", bounds)
    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        # T has one scalar complex parameter; G's second receptor is removed.
        # CASA's optional WEIGHT cells are undefined and must stay undefined.
        for column in ("CPARAM", "FLAG", "PARAMERR", "SNR", "WEIGHT"):
            if column in tb.colnames() and tb.iscelldefined(column, 0):
                tb.putcol(column, tb.getcol(column)[:1, :, :])
        tb.putkeyword("VisCal", "T Jones")
        information = tb.info()
        information["subType"] = "T Jones"
        tb.putinfo(information)
        antennas, times = tb.getcol("ANTENNA1"), tb.getcol("TIME")
        if probe == "missing_antenna":
            tb.removerows(np.flatnonzero(antennas == 7).tolist())
        elif probe.startswith("flag"):
            selected = antennas == 0
            if "first" in probe:
                selected &= times == bounds[0]
            flags = tb.getcol("FLAG")
            flags[0, 0, selected] = True
            tb.putcol("FLAG", flags)
        return {**info, "rows": tb.nrows(), "parameters": 1}
    finally:
        tb.close()


def initialize(path, correlations):
    from casatools import table

    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        for row in range(tb.nrows()):
            for column, value in (("DATA", 3 + 4j), ("MODEL_DATA", 7 - 2j),
                                  ("CORRECTED_DATA", -5 + 6j)):
                tb.putcell(column, row, np.full((correlations, 4), value, dtype=np.complex64))
            tb.putcell("FLAG", row, np.zeros((correlations, 4), dtype=bool))
            tb.putcell("FLAG_ROW", row, False)
            tb.putcell("WEIGHT", row, np.array([2, 3, 5, 7][:correlations], dtype=np.float32))
            tb.putcell("SIGMA", row, np.array([0.5, 0.75, 1.25, 2][:correlations], dtype=np.float32))
    finally:
        tb.close()


REFUSALS = [{"name": f"G_{interp}_malformed_scalar", "kind": "G", "interp": interp,
             "stokes": LAYOUTS["full"], "probe": "control", "options": {}}
            for interp in ("nearest", "linear")]


def save(path, cases, refusals=()):
    import casatasks

    record = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sm_apply_scalar_oracle.py", "cases": cases,
              "refusals": refusals}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as stream:
        stream.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture):
    from casatools import simulator, table

    path = out / "applied.ms"
    sm = make_session(path, stokes=spec["stokes"])
    sm.close()
    initialize(path, len(spec["stokes"].split()))
    sm = simulator()
    assert sm.openfromms(str(path))
    tb = table()
    tb.open(str(path))
    times = tb.getcol("TIME")
    bounds = float(min(times)), float(max(times))
    tb.close()
    info = calibration(sm, out / "terms.cal", bounds, spec["probe"])
    if spec["kind"] == "G":
        tb.open(str(out / "terms.cal"), nomodify=False)
        tb.putkeyword("VisCal", "G Jones")
        information = tb.info()
        information["subType"] = "G Jones"
        tb.putinfo(information)
        tb.close()
    options = {"calwt": False, "t": 0.0, "opacity": 0.0, **spec["options"]}
    assert sm.setapply(table=str(out / "terms.cal"), interp=spec["interp"], **options)
    if spec["kind"] == "G":
        try:
            sm.corrupt()
        except RuntimeError as error:
            save(fixture, [], [{**spec, "calibration": info, "outcome": "raises",
                                "error": str(error)}])
            sm.close()
            return
        raise AssertionError("CASA accepted a malformed one-parameter G table")
    assert sm.corrupt()
    sm.close()
    rows = read_rows(path)
    tb.open(str(path))
    for index, row in enumerate(rows):
        row["time_s"] = float(tb.getcell("TIME", index))
    tb.close()
    save(fixture, [{**spec, "calibration": info, "rows": rows, "cleanup": "returned"}])


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=[case["name"] for case in CASES + REFUSALS])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    args.out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES + REFUSALS if not args.only or case["name"] == args.only]
    if args.worker:
        assert len(selected) == 1
        worker(selected[0], args.out, args.fixture)
        return
    results, refusals = [], []
    for case in selected:
        out = args.out / case["name"]
        out.mkdir()
        fixture = out / "result.json.gz"
        with (out / "worker.log").open("w") as log:
            subprocess.run([sys.executable, __file__, "--out", str(out), "--fixture", str(fixture),
                            "--only", case["name"], "--worker"], stdout=log, stderr=subprocess.STDOUT,
                           check=True)
        with gzip.open(fixture, "rt") as handle:
            record = json.load(handle)
            results.extend(record["cases"])
            refusals.extend(record.get("refusals", []))
        save(args.fixture, results, refusals)
        print(case["name"], "passed", flush=True)


if __name__ == "__main__":
    main()
