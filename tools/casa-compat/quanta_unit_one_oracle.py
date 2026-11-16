#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's literal dimensionless unit spelling."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

CASES = [
    ("quantity", [1, "1"]), ("quantity", [0.5, "1"]),
    ("quantity", ["2 1"]), ("quantity", ["0 1"]),
    ("quantity", [{"value": 0.5, "unit": "1"}]),
    ("getunit", [{"value": 0.5, "unit": "1"}]),
    ("add", [{"value": 0.5, "unit": "1"}, 1.0]),
    ("check", ["2 1"]), ("quantity", ["1.4.5GHz"]),
    ("quantity", [1, "1.m"]), ("quantity", [1, "m.1"]),
    ("quantity", [1, "1/m"]), ("quantity", [1, "(1)"]),
    ("quantity", [1, "1m"]), ("quantity", [1, "2.m"]),
    ("mul", ["2 1", "3m"]), ("mul", ["3m", "2 1"]),
    ("div", ["2 1", "3m"]), ("pow", ["2 1", 2]),
]
q = quanta()
results = []
for method, args in CASES:
    case = {"method": method, "args": args}
    try:
        case["result"] = getattr(q, method)(*args)
    except Exception as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
