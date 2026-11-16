#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's componentlist ASCII-conversion placeholder."""
import json
import resource
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string

CALLS = [
    (["in.cl", "in.txt"], {}), (["", ""], {}),
    ([{"_bytes": [255]}, "in.txt"], {}), (["a\u0000b", "in.txt"], {}),
    (["in.cl", "in.txt"], {"refer": "bad", "format": "bad", "direction": {"x": 1}, "spectrum": {}, "flux": {}, "log": False}),
    ([1, "in.txt"], {}), (["in.cl", 2], {}), ([None, "in.txt"], {}),
    (["in.cl", "in.txt"], {"refer": 1}), (["in.cl", "in.txt"], {"refer": None}),
    (["in.cl", "in.txt"], {"format": 1}), (["in.cl", "in.txt"], {"format": None}),
    (["in.cl", "in.txt"], {"direction": 1}), (["in.cl", "in.txt"], {"direction": None}),
    (["in.cl", "in.txt"], {"spectrum": []}), (["in.cl", "in.txt"], {"flux": "bad"}),
    (["in.cl", "in.txt"], {"log": 1}), (["in.cl", "in.txt"], {"log": None}),
    (["in.cl", "in.txt"], {"direction": {"large": 2**100}}),
]


def convert(value):
    if isinstance(value, dict) and set(value) == {"_bytes"}:
        return bytes(value["_bytes"])
    return value


results = []
for state in ("empty", "populated", "closed"):
    cl = componentlist()
    if state != "empty":
        cl.addcomponent(flux=1, dir="J2000 00h00m00s +00d00m00s")
    if state == "closed":
        cl.done()
    for args, kwargs in CALLS:
        item = {"state": state, "args": args, "kwargs": kwargs}
        try:
            item["result"] = cl.asciitocomponentlist(
                *(convert(value) for value in args),
                **{key: convert(value) for key, value in kwargs.items()},
            )
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        item["length"] = cl.length()
        results.append(item)
    cl.done()
cycle_probe = (
    "from casaconfig import config;config.data_auto_update=False;"
    "config.measures_auto_update=False;from casatools import componentlist;"
    "x={};x['self']=x;componentlist().asciitocomponentlist('a','b',direction=x)"
)
probe = subprocess.run(
    [sys.executable, "-c", cycle_probe], capture_output=True,
    preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)),
)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
                                   "cycle_returncode": probe.returncode}))
