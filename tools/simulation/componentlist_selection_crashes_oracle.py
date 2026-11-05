#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe CASA selection crash cases in separate disposable processes."""
import json
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import version_string  # noqa: E402

CHILD = r'''
import sys
from casaconfig import config
config.data_auto_update = config.measures_auto_update = False
from casatools import componentlist
c = componentlist()
for _ in range(3):
    c.addcomponent(flux=1, dir="J2000 0deg 0deg")
name = sys.argv[1]
if name == "cycle":
    value = []
    value.append(value)
elif name == "depth":
    value = 1
    for _ in range(100):
        value = [value]
elif name == "nan":
    value = float("nan")
else:
    value = float("inf")
try:
    c.select(value)
    print("CASA_CHILD=" + repr(c.selected().tolist()), flush=True)
except BaseException as error:
    print("CASA_CHILD_ERROR=" + type(error).__name__ + ": " + str(error), flush=True)
'''
results = []
for name in ("nan", "inf", "cycle", "depth"):
    try:
        child = subprocess.run([sys.executable, "-u", "-c", CHILD, name], capture_output=True, text=True, timeout=15)
        output = next((line for line in child.stdout.splitlines() if line.startswith("CASA_CHILD")), "")
        results.append({"case": name, "returncode": child.returncode, "output": output})
    except subprocess.TimeoutExpired:
        results.append({"case": name, "timeout": True})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
