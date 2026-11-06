#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe unsafe CASA setrefdir inputs in isolated processes."""

import json
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import version_string  # noqa: E402

CHILD = r'''
import json
import math
import resource
import sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from casaconfig import config
config.data_auto_update = config.measures_auto_update = False
from casatools import componentlist
cl = componentlist()
name = sys.argv[1]
if name == "empty_all":
    result = cl.setrefdir(-1, "1rad", "0.5rad")
elif name == "cycle":
    cl.addcomponent(flux=1)
    value = {}
    value["self"] = value
    result = cl.setrefdir(0, value, "0deg")
else:
    cl.addcomponent(flux=1)
    result = cl.setrefdir(0, {"value": float(name), "unit": "deg"}, "0deg")
    result = {"result": result, "longitude_is_nan": math.isnan(cl.getrefdir(0)["m0"]["value"])}
print("CASA_CHILD=" + json.dumps(result), flush=True)
'''
results = []
for name in ("empty_all", "cycle", "nan", "inf", "-inf"):
    try:
        child = subprocess.run([sys.executable, "-u", "-c", CHILD, name],
                               capture_output=True, text=True, timeout=15)
        output = next((line.removeprefix("CASA_CHILD=") for line in child.stdout.splitlines()
                       if line.startswith("CASA_CHILD=")), "")
        results.append({"case": name, "returncode": child.returncode,
                        "result": json.loads(output) if output else None})
    except subprocess.TimeoutExpired:
        results.append({"case": name, "timeout": True})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
