#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Benchmark recorded direction shifts against CASA 6.7.6.14."""
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
parser.add_argument("--backend", choices=["casa", "casars"], required=True)
parser.add_argument("--measures-data", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--calls", type=int, default=1000)
parser.add_argument("--samples", type=int, default=7)
args = parser.parse_args()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
os.environ["CASA_RS_MEASURESPATH"] = config.measurespath

from casatools import measures, version_string

assert version_string() == "6.7.6-14"
if args.backend == "casars":
    from casars.tools import measures

root = Path(__file__).resolve().parents[2]
fixture = root / "frontends/casars-python/python/tests/data/casa_measures_shift.json"
cases = {case["name"]: case for case in json.loads(fixture.read_text())["cases"]}
names = [
    "upstream_north", "upstream_east", "upstream_south", "galactic",
    "vector", "nested_offset", "large_east", "pole_crossing",
]

tool = measures()
assert tool.done()
results = {}
gc.disable()
try:
    for name in names:
        values = cases[name]["args"]
        operation = tool.shift
        assert operation(*values)
        samples = []
        for sample in range(args.samples + 1):
            start = time.perf_counter_ns()
            for _ in range(args.calls):
                operation(*values)
            if sample:
                samples.append((time.perf_counter_ns() - start) / args.calls)
        results[name] = {"median_ns": statistics.median(samples), "samples_ns": samples}
finally:
    gc.enable()
    tool.done()


def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()


args.out.write_text(json.dumps({
    "backend": args.backend, "casa_version": version_string(),
    "revision": git("rev-parse", "HEAD"),
    "source_dirty": bool(git("status", "--porcelain")),
    "calls_per_sample": args.calls, "cases": results,
}, indent=2) + "\n")
print(f"Measured {len(results)} shift cases")
