#!/usr/bin/env python3
"""Capture CASA 6.7.6-14 getoffset behavior for scalar measures."""
import json
import resource
import subprocess
import sys

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import measures


def clean(value):
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return clean(value.tolist())
    if isinstance(value, (tuple, list)):
        return [clean(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


m = measures()
base = {
    "direction": m.direction("J2000", "10deg", "20deg"),
    "epoch": m.epoch("UTC", "2020/01/01/00:00:00"),
    "frequency": m.frequency("LSRK", "1.4GHz"),
    "position": m.position("ITRF", "100m", "200m", "300m"),
}
offsets = {
    "direction": m.direction("J2000", "1deg", "2deg"),
    "epoch": m.epoch("UTC", "2020/01/02/00:00:00"),
    "frequency": m.frequency("LSRK", "10MHz"),
    "position": m.position("ITRF", "1m", "2m", "3m"),
}
cases = [
    ("empty", {}),
    ("unknown", {"type": "unknown", "refer": "J2000"}),
    ("nonrecord", 42),
    ("none", None),
    ("direction_bad_empty", dict(base["direction"], offset={})),
    ("direction_bad_type", dict(base["direction"], offset=offsets["frequency"])),
    ("direction_bad_scalar", dict(base["direction"], offset=42)),
]
for kind in base:
    cases.append((kind + "_none", base[kind]))
    cases.append((kind + "_offset", dict(base[kind], offset=offsets[kind])))
extra = dict(offsets["direction"], extra="marker")
cases.append(("direction_extra", dict(base["direction"], offset=extra)))

results = []
for name, value in cases:
    try:
        result = {"result": clean(m.getoffset(value))}
    except Exception as error:
        result = {"error": {"type": type(error).__name__, "message": str(error)}}
    results.append({"case": name, "input": clean(value), **result})

large = dict(offsets["direction"], extra={"large": 2**53 + 1, "flag": True, "name": "safe"})
large_result = clean(m.getoffset(dict(base["direction"], offset=large)))
cycle_probe = (
    "from casaconfig import config;config.data_auto_update=False;"
    "config.measures_auto_update=False;from casatools import measures;"
    "x={};x['self']=x;measures().getoffset(x)"
)
probe = subprocess.run(
    [sys.executable, "-c", cycle_probe], capture_output=True,
    preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)),
)
print(json.dumps({
    "casatools": "6.7.6-14", "cases": results, "large_input": 2**53 + 1,
    "large_result": large_result["extra"]["large"], "cycle_returncode": probe.returncode,
}, indent=2))
