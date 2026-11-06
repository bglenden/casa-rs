#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe CASA replace crash cases in isolated processes without core dumps."""

import json
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import version_string  # noqa: E402

CHILD = r'''
import resource
import sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from casaconfig import config
config.data_auto_update = config.measures_auto_update = False
from casatools import componentlist
c = componentlist()
name = sys.argv[1]
if name == "record_cycle":
    value = {}
    value["self"] = value
    result = c.replace(0, value)
elif name == "indices_cycle":
    value = []
    value.append(value)
    result = c.replace(0, {}, value)
else:
    result = c.replace(0, {}, float(name))
print("CASA_CHILD=" + repr(result), flush=True)
'''
results = []
for name in ("record_cycle", "indices_cycle", "nan", "inf", "-inf"):
    try:
        child = subprocess.run([sys.executable, "-u", "-c", CHILD, name],
                               capture_output=True, text=True, timeout=15)
        output = next((line for line in child.stdout.splitlines() if line.startswith("CASA_CHILD=")), "")
        results.append({"case": name, "returncode": child.returncode, "output": output})
    except subprocess.TimeoutExpired:
        results.append({"case": name, "timeout": True})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
