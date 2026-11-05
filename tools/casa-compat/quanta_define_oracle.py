#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's process-wide user-unit definitions and conversions."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

q, other = quanta(), quanta()
steps = [
    ("define", ["casaUserLen", "2m"]),
    ("convert", ["1casaUserLen", "m"]),
    ("convert", ["1casaUserLen.casaUserLen", "m2"]),
    ("other_convert", ["1casaUserLen", "m"]),
    ("define", ["casaUserLen", "3cm"]),
    ("convert", ["1casaUserLen", "m"]),
    ("convert", ["1casaUserLen.casaUserLen", "m2"]),
    ("other_convert", ["1casaUserLen", "m"]),
    ("define", ["casaUserLen", "badquantity"]),
    ("convert", ["1casaUserLen", "m"]),
    ("define", ["casaUserTime", {"value": 4.0, "unit": "s"}]),
    ("convert", ["1casaUserTime", "s"]),
    ("define", ["casaUserDefault"]),
    ("define", ["casaUserNone", None]),
    ("define", ["casaUserNumber", 2.0]),
    ("define", ["casaUserList", [1.0, 2.0]]),
    ("define", ["m", "2s"]),
    ("convert", ["1m", "m"]),
    ("convert", ["1m", "s"]),
    ("fits", []),
    ("convert", ["1M", "s"]),
    ("convert", ["1METERS", "s"]),
    ("define", [None, "1m"]),
]
cases = []
for method, args in steps:
    case = {"method": method, "args": args}
    try:
        target = other.convert if method == "other_convert" else getattr(q, method)
        case["result"] = target(*args)
    except Exception as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
