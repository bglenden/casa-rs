#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's scalar measure extra values."""
import json
import resource
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import measures, version_string


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if hasattr(value, "tolist"):
        return clean(value.tolist())
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    return value


me = measures()
cases = []
for refer in ("J2000", "B1950", "GALACTIC", "AZEL", "SUN"):
    for lon, lat in (("10deg", "20deg"), ("-120deg", "-45deg"), ("0deg", "90deg")):
        cases.append((refer + lon + lat, me.direction(refer, lon, lat)))
for refer, args in (("ITRF", ("100m", "200m", "300m")),
                    ("ITRF", ("0m", "0m", "0m")),
                    ("WGS84", ("10deg", "20deg", "100m")),
                    ("WGS84", ("-100deg", "40deg", "0m"))):
    cases.append((refer + str(args), me.position(refer, *args)))
direction = me.direction("J2000", "10deg", "20deg")
cases.extend([
    ("one-element quantity", dict(direction, m0={"value": [0.1], "unit": "rad"})),
    ("unknown reference", dict(direction, refer="bad")),
    ("bad unit", dict(direction, m0={"value": 3, "unit": "m"})),
    ("epoch", me.epoch("UTC", "2020/01/01/00:00:00")),
    ("frequency", me.frequency("LSRK", "1GHz")),
    ("empty", {}), ("unknown type", {"type": "unknown", "refer": "J2000"}),
    ("not a record", 4), ("none", None),
])
results = []
for name, value in cases:
    item = {"case": name, "input": clean(value)}
    try:
        item["result"] = clean(me.addxvalue(value))
    except Exception as error:
        item["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(item)
cycle_probe = (
    "from casaconfig import config;config.data_auto_update=False;"
    "config.measures_auto_update=False;from casatools import measures;"
    "x={};x['self']=x;measures().addxvalue(x)"
)
probe = subprocess.run(
    [sys.executable, "-c", cycle_probe], capture_output=True,
    preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)),
)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
                                   "cycle_returncode": probe.returncode}))
