#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Probe invalid CASA direction-frame names in isolated processes."""

import json
import subprocess
import sys
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import version_string  # noqa: E402

CHILD = r'''
import json
import resource
import sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from casaconfig import config
config.data_auto_update = config.measures_auto_update = False
from casatools import componentlist
cl = componentlist()
cl.addcomponent(flux=1, dir="J2000 10deg 20deg")
before = cl.getrefdir(0)
frame = bytes.fromhex(sys.argv[1][4:]) if sys.argv[1].startswith("hex:") else sys.argv[1]
try:
    result = {"result": cl.setrefdirframe(0, frame)}
except Exception as error:
    result = {"error": type(error).__name__, "message": str(error)}
result["before"] = before
result["after"] = cl.getrefdir(0)
print("CASA_CHILD=" + json.dumps(result), flush=True)
'''
results = []
for name in ("bad", "", "unknown", "B", "hex:ff"):
    try:
        child = subprocess.run([sys.executable, "-u", "-c", CHILD, name],
                               capture_output=True, text=True, timeout=15)
        output = next((line.removeprefix("CASA_CHILD=") for line in child.stdout.splitlines()
                       if line.startswith("CASA_CHILD=")), "")
        results.append({"frame": name, "returncode": child.returncode,
                        "result": json.loads(output) if output else None})
    except subprocess.TimeoutExpired:
        results.append({"frame": name, "timeout": True})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
