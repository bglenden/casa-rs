#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native geometry-tool call latency against CASA 6.7.6.14."""
import argparse
import gc
import json
import os
import statistics
import subprocess
import time
from pathlib import Path

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--backend", choices=["casa", "casars"], required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--repeat", type=int, default=1000)
parser.add_argument("--samples", type=int, default=7)
args = parser.parse_args()
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
os.environ["CASA_RS_MEASURESPATH"] = config.measurespath
from casatools import measures as casa_measures, version_string
assert version_string() == "6.7.6-14"

factory = casa_measures
if args.backend == "casars":
    from casars.tools import measures as factory

root = Path(__file__).resolve().parents[2]
fixture = root / "frontends/casars-python/python/tests/data/casa_measures_geometry.json"
cases = {case["name"]: case for case in json.loads(fixture.read_text())["cases"]}
names = [
    "baseline_values_0", "baseline_vector_1", "baseline_vector_3", "baseline_offset_0",
    "uvw_values_0", "uvw_vector_3", "uvw_offset_0", "earthmagnetic_ref_ITRF",
    "earthmagnetic_measure_0", "asbaseline_0", "asbaseline_1",
    "baseline_touvw_0", "touvw_baseline_vector_4", "baseline_expand_0",
    "expand_baseline_vector_4", "baseline_convert_HADEC", "baseline_convert_J2000",
    "uvw_convert_B1950", "uvw_convert_J2000", "baseline_inspect_1_addxvalue",
    "earthmagnetic_convert_HADEC", "earthmagnetic_convert_IGRF",
    "earthmagnetic_fk4_B1950_APP_1", "earthmagnetic_fk4_APP_B1950_1",
]
report = {}
gc.disable()
try:
    for name in names:
        case = cases[name]
        tool = factory()
        tool.done()
        for element in case.get("frame", []):
            assert tool.doframe(element)
        operation = getattr(tool, case["method"])
        values = case["args"]
        keywords = case["kwargs"]
        operation(*values, **keywords)
        elapsed = []
        for sample in range(args.samples + 1):
            start = time.perf_counter_ns()
            for _ in range(args.repeat):
                operation(*values, **keywords)
            if sample:
                elapsed.append((time.perf_counter_ns() - start) / args.repeat)
        tool.done()
        report[name] = {"median_ns": statistics.median(elapsed), "samples_ns": elapsed}
finally:
    gc.enable()

def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()

args.out.write_text(json.dumps({
    "backend": args.backend,
    "casa_version": version_string(),
    "revision": git("rev-parse", "HEAD"),
    "source_dirty": bool(git("status", "--porcelain")),
    "calls_per_sample": args.repeat,
    "cases": report,
}, indent=2) + "\n")
print(f"Measured {len(report)} cases")
