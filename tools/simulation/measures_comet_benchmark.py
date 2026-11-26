#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare warm release comet operations with CASA 6.7.6.14."""
import argparse
import gc
import json
import os
from pathlib import Path
import statistics
import subprocess
from tempfile import TemporaryDirectory
import time

import numpy as np
from casaconfig import config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--backend", choices=["casa", "casars"], required=True)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--calls", type=int, default=1000)
parser.add_argument("--samples", type=int, default=7)
parser.add_argument("--direction-cases", action="store_true")
args = parser.parse_args()

config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
os.environ["CASA_RS_MEASURESPATH"] = config.measurespath

from casatools import logsink, measures, table, version_string

assert version_string() == "6.7.6-14"
if args.backend == "casars":
    from casars.tools import measures

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "frontends/casars-python/python/tests/data/casa_measures_comet.json"
GANYMEDE = "JPL-Horizons/Ganymede_55437-56293dUTC.tab"
ABSOLUTE = str(args.measures_data.resolve() / "ephemerides" / GANYMEDE)


def make_topocentric(path):
    fixture = json.loads(FIXTURE.read_text())["synthetic"]
    columns = fixture["columns"]
    tool = table()
    desc = {name: {"valueType": "double"} for name in columns}
    assert tool.create(path, desc, nrow=len(columns["MJD"]))
    for name, values in columns.items():
        tool.putcol(name, np.asarray(values))
    tool.putkeywords(fixture["keywords"])
    tool.putinfo({"type": "IERS"})
    tool.close()


def run(method, table_path, reference=None):
    tool = measures()
    assert tool.done()
    assert tool.doframe(tool.epoch("UTC", "2011/01/03/17:00:00"))
    assert tool.framecomet(table_path)
    operation = getattr(tool, method)
    if reference is not None or method in ("showframe", "rise", "riseset"):
        direction = tool.direction("COMET")
        assert tool.doframe(tool.observatory("ALMA"))
        if method == "showframe":
            assert tool.doframe(direction)
            values = ()
        elif reference is not None:
            values = (direction, reference)
        else:
            values = (direction,)
        first = operation(*values)
        assert first, (method, reference, table_path)
    elif method == "framecomet":
        values = (table_path,)
        assert operation(*values)
    else:
        values = ()
        operation()
    measurements = []
    gc.disable()
    try:
        for sample in range(args.samples + 1):
            start = time.perf_counter_ns()
            for _ in range(args.calls):
                operation(*values)
            if sample:
                measurements.append((time.perf_counter_ns() - start) / args.calls)
    finally:
        gc.enable()
        tool.done()
    return {"median_ns": statistics.median(measurements), "samples_ns": measurements}


with TemporaryDirectory() as temp:
    topo = str(Path(temp) / "topocentre.tab")
    make_topocentric(topo)
    cases = [
        ("load_absolute", "framecomet", ABSOLUTE),
        ("load_ephemerides_relative", "framecomet", GANYMEDE),
        ("load_topocentric", "framecomet", topo),
        ("name", "cometname", ABSOLUTE),
        ("type", "comettype", ABSOLUTE),
        ("topocentre_m", "comettopo", topo),
        ("distance", "cometdist", ABSOLUTE),
        ("angular_diameter", "cometangdiam", ABSOLUTE),
        ("topocentric_distance", "cometdist", topo),
        ("topocentric_angular_diameter", "cometangdiam", topo),
    ]
    results = {name: run(method, path) for name, method, path in cases}
    if args.direction_cases:
        logger = logsink()
        logger.filter("SEVERE")
        j2000 = str(args.measures_data / "ephemerides/JPL-Horizons/Ganymede_55197-59214dUTC_J2000.tab")
        for name, path in (("app", ABSOLUTE), ("j2000", j2000), ("topo", topo)):
            for reference in ("APP", "J2000", "TOPO"):
                results[f"measure_{name}_to_{reference.lower()}"] = run("measure", path, reference)
            results[f"showframe_{name}"] = run("showframe", path)
        for method in ("rise", "riseset"):
            results[method] = run(method, ABSOLUTE)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


args.out.write_text(json.dumps({
    "backend": args.backend, "casa_version": version_string(),
    "revision": git("rev-parse", "HEAD"),
    "source_dirty": bool(git("status", "--porcelain")),
    "calls_per_sample": args.calls,
    "cases": results,
}, indent=2) + "\n")
print(f"Measured {len(results)} comet cases")
