#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare release measure reference offsets with CASA 6.7.6.14."""

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
parser.add_argument("--backend", choices=("casa", "casars"), required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--calls", type=int, default=400)
parser.add_argument("--samples", type=int, default=7)
args = parser.parse_args()
config.data_auto_update = config.measures_auto_update = False
config.measurespath = str(args.measures_data.resolve())
config.datapath = [config.measurespath, *config.datapath]
os.environ["CASA_RS_MEASURESPATH"] = config.measurespath

from casatools import logsink, measures, version_string

assert version_string() == "6.7.6-14"
logger = logsink()
logger.filter("SEVERE")
if args.backend == "casars":
    from casars.tools import measures

root = Path(__file__).resolve().parents[2]
fixture = root / "frontends/casars-python/python/tests/data/casa_measures_offsets.json"
oracle = json.loads(fixture.read_text())
results = {}
for case in oracle["cases"]:
    if case["name"].endswith(("_malformed", "_mismatch")) or not case["result"]:
        continue
    tool = measures()
    try:
        assert tool.done()
        assert all(tool.doframe(item) for item in case.get("frame", oracle["frame"]))
        operation = getattr(tool, case["method"])
        arguments = case["args"]
        assert operation(*arguments), case["name"]
        samples = []
        gc.disable()
        for sample in range(args.samples + 1):
            start = time.perf_counter_ns()
            for _ in range(args.calls):
                operation(*arguments)
            if sample:
                samples.append((time.perf_counter_ns() - start) / args.calls)
        results[case["name"]] = {
            "median_ns": statistics.median(samples), "samples_ns": samples,
        }
    finally:
        gc.enable()
        tool.done()


def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()


args.out.write_text(json.dumps({
    "backend": args.backend,
    "casa_version": version_string(),
    "revision": git("rev-parse", "HEAD"),
    "source_dirty": bool(git("status", "--porcelain")),
    "calls_per_sample": args.calls,
    "cases": results,
}, indent=2) + "\n")
print(f"Measured {len(results)} reference-offset cases")
