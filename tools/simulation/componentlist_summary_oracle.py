#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture the visible lines of CASA 6.7.6.14 component summaries."""
import argparse
import json
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--out", type=Path)
parser.add_argument("--worker", action="store_true")
args = parser.parse_args()
if not args.worker:
    if args.out is None:
        parser.error("--out is required")
    result = subprocess.run([sys.executable, __file__, "--worker"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True)
    summaries = {}
    name = None
    for line in result.stdout.splitlines():
        if line.startswith("CASE "):
            name = line[5:]
            summaries[name] = []
        elif "\tINFO\tcomponentlist::summarize" in line:
            summaries[name].append(line.split("\t", 3)[3])
    assert len(summaries) == 6 and all(summaries.values()), result.stdout
    args.out.write_text(json.dumps({"casatools": "6.7.6-14", "summaries": {name: "\n".join(lines).rstrip("\n") for name, lines in summaries.items()}}, indent=2) + "\n")
    raise SystemExit(0)

from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string
assert version_string() == "6.7.6-14"
for name, shape, major, minor, pa in [("point", "Point", "4arcsec", "2arcsec", "30deg"), ("gaussian", "Gaussian", "4arcsec", "2arcsec", "30deg"), ("disk", "Disk", "1arcmin", "0.5arcmin", "-30deg"), ("errors", "Gaussian", "4arcsec", "2arcsec", "30deg"), ("precision_boundary", "Gaussian", "4arcsec", "2arcsec", "30deg"), ("limbdisk", "Gaussian", "4arcsec", "2arcsec", "30deg")]:
    print("CASE", name, flush=True)
    cl = componentlist()
    cl.addcomponent(flux=[3, 1, 0.5, 0.25], dir="J2000 10deg -20deg", label="example", shape=shape, majoraxis=major, minoraxis=minor, positionangle=pa)
    if name in ["errors", "precision_boundary", "limbdisk"]:
        record = cl.getcomponent(0)
        if name in ["errors", "precision_boundary"]:
            record["shape"]["direction"]["error"] = {"longitude": {"value": 1.50015 if name == "precision_boundary" else 0.02, "unit": "arcsec"}, "latitude": {"value": 3.2 if name == "precision_boundary" else 0.03, "unit": "arcsec"}}
            for key, value in [("majoraxiserror", 0.2), ("minoraxiserror", 0.1), ("positionangleerror", 0.5)]:
                record["shape"][key] = {"value": value, "unit": "deg" if key == "positionangleerror" else "arcsec"}
        else:
            record["shape"]["type"] = "Limbdarkeneddisk"
            record["optionalParameters"] = [0.5]
        cl.done()
        cl.add(record)
    cl.summarize(0)
    cl.done()
