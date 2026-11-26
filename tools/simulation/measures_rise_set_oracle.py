#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture and benchmark rise and riseset with CASA 6.7.6.14."""

import argparse
import gc
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from casaconfig import config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--benchmark", choices=["casa", "casars"])
parser.add_argument("--calls", type=int, default=400)
parser.add_argument("--samples", type=int, default=7)
args = parser.parse_args()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
os.environ["CASA_RS_MEASURESPATH"] = config.measurespath

from casatools import logsink, measures, version_string

assert version_string() == "6.7.6-14"

METHODS = ("rise", "riseset")
ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "frontends/casars-python/python/tests/data/casa_measures_rise_set.json"


def set_frame(tool, frame):
    assert tool.done()
    assert all(tool.doframe(element) for element in frame)


def capture():
    tool = measures()
    south = tool.observatory("ALMA")
    north = tool.observatory("VLA")
    epoch = tool.epoch("UTC", "2011/01/03/17:00:00")
    later = tool.epoch("UTC", "2013/06/13/03:20:00")
    tai = tool.epoch("TAI", "2011/01/03/17:00:00")
    source = tool.direction("J2000", "10deg", "20deg")
    vector = dict(
        source, m0={"value": [10., 90.], "unit": "deg"},
        m1={"value": [20., -50.], "unit": "deg"},
    )
    southern = [epoch, south]
    northern = [later, north]
    cases = [
        ("horizon", southern, [source]),
        ("positive", southern, [source, "5deg"]),
        ("negative", southern, [source, "-5deg"]),
        ("time_as_angle", southern, [source, "5s"]),
        ("bare_elevation", southern, [source, 5]),
        ("length_elevation", southern, [source, "5km"]),
        ("record_elevation", southern, [source, {"value": 3., "unit": "deg"}]),
        ("empty_elevation", southern, [source, ""]),
        ("turn_boundary", southern, [tool.direction("J2000", "250deg", "20deg")]),
        ("above", southern, [tool.direction("J2000", "10deg", "-89deg")]),
        ("below", southern, [tool.direction("J2000", "10deg", "89deg")]),
        ("north", northern, [source, "10deg"]),
        ("tai", [tai, south], [source, "15deg"]),
        ("array_first", southern, [vector]),
        ("angles_text", southern, ["10deg 20deg"]),
        ("ref_text", southern, ["J2000 10deg 20deg"]),
        ("text_list", southern, [["J2000", "10deg", "20deg"]]),
        ("app", southern, [tool.direction("APP", "10deg", "20deg")]),
        ("b1950_tangent", southern,
         [tool.direction("B1950", "10deg", "20deg"), "46.7775deg"]),
        ("b1950_crossings", southern, [tool.direction("B1950", "10deg", "20deg")]),
        ("hadec", southern, [tool.direction("HADEC", "10deg", "20deg")]),
        ("azel", northern, [tool.direction("AZEL", "120deg", "30deg")]),
        ("sun", southern, ["SUN", "-0.833deg"]),
        ("moon", northern, [tool.direction("MOON"), "1deg"]),
        ("same_reference_offset", southern,
         [dict(source, offset=tool.direction("J2000", "47deg", "-25deg"))]),
        ("fixed_reference_offset", southern,
         [dict(source, offset=tool.direction("GALACTIC", "47deg", "-25deg"))]),
        ("frame_dependent_offset", southern,
         [dict(source, offset=tool.direction("APP", "47deg", "-25deg"))]),
        ("no_coordinates", southern, []),
        ("unknown_coordinates", southern, ["nonsense"]),
        ("bad_record", southern, [{}]),
        ("epoch_as_source", southern, [epoch]),
        ("invalid_elevation", southern, [source, "nonsense"]),
    ]
    results = []
    for name, frame, arguments in cases:
        set_frame(tool, frame)
        values = {
            method: getattr(tool, method)(*arguments)
            for method in METHODS
        }
        case = {"name": name, "frame": frame, "args": arguments, "results": values}
        if name == "moon":
            case["direction_controls"] = {
                ref: tool.measure(arguments[0], ref)
                for ref in ("APP", "TOPO", "HADEC", "AZEL")
            }
        results.append(case)
    tool.done()
    return {"casa_version": version_string(), "cases": results}


def benchmark(backend):
    logger = logsink()
    logger.filter("SEVERE")
    tool_factory = measures
    if backend == "casars":
        from casars.tools import measures as native_measures
        tool_factory = native_measures

    names = {
        "horizon", "positive", "above", "below", "north", "turn_boundary",
        "angles_text", "sun", "moon", "b1950_crossings", "same_reference_offset",
    }
    cases = json.loads(FIXTURE.read_text())["cases"]
    rows = {}
    for case in cases:
        if case["name"] not in names:
            continue
        tool = tool_factory()
        set_frame(tool, case["frame"])
        for method in METHODS:
            operation = getattr(tool, method)
            arguments = case["args"]
            assert {"rise", "set"} == set(operation(*arguments))
            samples = []
            gc.disable()
            try:
                for sample in range(args.samples + 1):
                    start = time.perf_counter_ns()
                    for _ in range(args.calls):
                        operation(*arguments)
                    if sample:
                        samples.append((time.perf_counter_ns() - start) / args.calls)
            finally:
                gc.enable()
            rows[f"{method}_{case['name']}"] = {
                "median_ns": statistics.median(samples),
                "samples_ns": samples,
            }
        tool.done()

    def git(*arguments):
        return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True).strip()

    return {
        "backend": backend,
        "casa_version": version_string(),
        "revision": git("rev-parse", "HEAD"),
        "source_dirty": bool(git("status", "--porcelain")),
        "calls_per_sample": args.calls,
        "cases": rows,
    }


output = benchmark(args.benchmark) if args.benchmark else capture()
args.out.write_text(json.dumps(output, indent=2) + "\n")
print(f"{len(output['cases'])} {'timed' if args.benchmark else 'recorded'} cases")
