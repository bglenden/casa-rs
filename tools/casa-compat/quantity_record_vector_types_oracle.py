#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA quantity-record numeric vector types and errors."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = [
    [0], [-1], [2**31], [-(2**31)-1], [2**53+1], [2**63-1],
    [1, 2], [1.0], [1.0, 2.0], [1, 2.0], [True], [],
    {"array": [2**31], "dtype": "int64"},
    {"array": [2**63], "dtype": "uint64"},
    {"array": [1, 2], "dtype": "int32"},
    {"array": [1], "dtype": "float64"},
    {"array": [1, 2], "dtype": "float64"},
    {"array": [True], "dtype": "bool"},
    {"array": [], "dtype": "int64"}, {"array": [], "dtype": "bool"},
    {"tuple": [2**31]}, {"tuple": [1, 2]},
]
CASES = [(value, "m") for value in VALUES] + [(VALUES[i], "not_a_unit") for i in (0, 6, 7, 8, 10, 12, 14, 15)]
q = quanta()
results = []
for method in ("quantity", "getunit", "add"):
    for value, unit in CASES:
        item = {"method": method, "value": value, "unit": unit}
        if isinstance(value, dict):
            actual = tuple(value["tuple"]) if "tuple" in value else np.array(value["array"], dtype=value["dtype"])
        else:
            actual = value
        args = ({"value": actual, "unit": unit},) + (("1m",) if method == "add" else ())
        try:
            result = getattr(q, method)(*args)
            if isinstance(result, dict) and hasattr(result["value"], "tolist"):
                result["value"] = result["value"].tolist()
            item["result"] = result
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
