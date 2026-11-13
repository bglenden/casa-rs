#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA G/D setapply argument controls with time-varying receptor terms (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import pathlib
import subprocess
import sys

import numpy as np

from sm_flagged_samples_oracle import read_rows
from sm_multiple_models_oracle import make_session


OPTIONS = {
    "control": {}, "calwt": {"calwt": True}, "short_interval": {"t": 1.0},
    "long_interval": {"t": 100.0}, "negative_interval": {"t": -1.0},
    "opacity": {"opacity": 0.25}, "combined": {"calwt": True, "t": 25.0, "opacity": 0.25},
}
CASES = [{"name": f"{kind}_{interp}_{name}", "kind": kind, "interp": interp, "options": options}
         for kind in ("G", "D") for interp in ("nearest", "linear")
         for name, options in OPTIONS.items()]


def initialize(path):
    from casatools import table

    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        for row in range(tb.nrows()):
            for column, value in (("DATA", 3 + 4j), ("MODEL_DATA", 7 - 2j),
                                  ("CORRECTED_DATA", -5 + 6j)):
                tb.putcell(column, row, np.full((4, 4), value, dtype=np.complex64))
            tb.putcell("FLAG", row, np.zeros((4, 4), dtype=bool))
            tb.putcell("FLAG_ROW", row, False)
            tb.putcell("WEIGHT", row, np.array([2, 3, 5, 7], dtype=np.float32))
            tb.putcell("SIGMA", row, np.array([0.5, 0.75, 1.25, 2], dtype=np.float32))
    finally:
        tb.close()


def receptor_values(kind, antenna, elapsed):
    if kind == "G":
        return (1.1 + 0.05 * antenna + 0.002 * elapsed + 1j * (0.2 + 0.003 * elapsed),
                0.8 + 0.03 * antenna + 0.001 * elapsed - 1j * (0.1 + 0.002 * elapsed))
    return (0.005 * (antenna + 1) + 1j * 0.001 * elapsed,
            -0.004 * (antenna + 1) + 1j * 0.0005 * elapsed)


def calibration(sm, path, kind, bounds):
    from casatools import table

    if kind == "G":
        assert sm.setgain(mode="fbm", table=str(path), amplitude=[0.0, 0.0])
    else:
        assert sm.setleakage(table=str(path), amplitude=[0.0], offset=[0.0])
    sm.reset()
    tb = table()
    tb.open(str(path), nomodify=False)
    try:
        first, last = bounds
        times = tb.getcol("TIME")
        if kind == "D":
            # CASA's leakage simulator emits a single constant solution.
            # Add an explicit second endpoint so D interpolation is tested.
            templates = [{name: tb.getcell(name, row) for name in tb.colnames()
                          if tb.iscelldefined(name, row)} for row in range(tb.nrows())]
            count = len(templates)
            tb.addrows(count)
            for row, record in enumerate(templates):
                for name, value in record.items():
                    tb.putcell(name, count + row, value)
                tb.putcell("TIME", count + row, last)
                tb.putcell("FIELD_ID", count + row, 1)
        else:
            # Only endpoints: inner integrations must interpolate.
            tb.removerows(np.flatnonzero((times != first) & (times != last)).tolist())
        tb.putcol("INTERVAL", np.zeros(tb.nrows()))
        times, antennas = tb.getcol("TIME"), tb.getcol("ANTENNA1")
        values = tb.getcol("CPARAM")
        for row, (time, antenna) in enumerate(zip(times, antennas)):
            values[:, 0, row] = receptor_values(kind, int(antenna), float(time - first))
        tb.putcol("CPARAM", values)
        return {"first_time_s": first, "last_time_s": last, "rows": tb.nrows()}
    finally:
        tb.close()


def save(path, cases):
    import casatasks

    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sm_apply_parameters_oracle.py", "cases": cases}
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
        zipped.write((json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n").encode())


def worker(spec, out, fixture):
    from casatools import simulator, table

    path = out / "applied.ms"
    sm = make_session(path, stokes="XX XY YX YY")
    sm.close()
    initialize(path)
    sm = simulator()
    assert sm.openfromms(str(path))
    tb = table()
    tb.open(str(path))
    times = tb.getcol("TIME")
    bounds = float(np.min(times)), float(np.max(times))
    tb.close()
    info = calibration(sm, out / "terms.cal", spec["kind"], bounds)
    parameters = {"calwt": False, "t": 0.0, "opacity": 0.0, **spec["options"]}
    assert sm.setapply(table=str(out / "terms.cal"), interp=spec["interp"], **parameters)
    assert sm.corrupt()
    sm.close()
    rows = read_rows(path)
    tb = table()
    tb.open(str(path))
    try:
        for row, record in enumerate(rows):
            record["time_s"] = float(tb.getcell("TIME", row))
    finally:
        tb.close()
    save(fixture, [{**spec, "calibration": info, "rows": rows, "cleanup": "returned"}])
    print(spec["name"], "rows", len(rows), flush=True)


def main():
    import casatasks

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--fixture", type=pathlib.Path, required=True)
    parser.add_argument("--only", nargs="+", choices=[case["name"] for case in CASES])
    parser.add_argument("--kind", choices=["G", "D"])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    assert casatasks.version_string() == "6.7.6.14"
    args.out.mkdir(parents=True, exist_ok=True)
    selected = [case for case in CASES if (not args.only or case["name"] in args.only)
                and (not args.kind or case["kind"] == args.kind)]
    if args.worker:
        assert len(selected) == 1
        worker(selected[0], args.out, args.fixture)
        return
    results = []
    for case in selected:
        directory = args.out / case["name"]
        directory.mkdir()
        fixture = directory / "result.json.gz"
        with (args.out / (case["name"] + ".log")).open("w") as log:
            child = subprocess.run([sys.executable, __file__, "--out", str(directory),
                                    "--fixture", str(fixture), "--only", case["name"], "--worker"],
                                   stdout=log, stderr=subprocess.STDOUT, check=False)
        if child.returncode:
            raise RuntimeError(f"{case['name']}: exit {child.returncode}; see its log")
        with gzip.open(fixture, "rt") as handle:
            results.extend(json.load(handle)["cases"])
        save(args.fixture, results)
        print(case["name"], "rows", len(results[-1]["rows"]), flush=True)


if __name__ == "__main__":
    main()
