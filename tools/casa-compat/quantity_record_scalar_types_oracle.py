#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity-record scalar integer types and errors."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = [0, -1, 2**31-1, 2**31, -(2**31)-1, 2**53+1, 2**63-1, -(2**63),
          {"numpy_scalar": 1, "dtype": "int32"}, {"numpy_scalar": 1, "dtype": "int64"},
          {"numpy_scalar": 1, "dtype": "uint64"}, {"numpy_scalar": 1.25, "dtype": "float64"},
          {"numpy_scalar": True, "dtype": "bool"}, {"numpy_scalar": 1.25, "dtype": "float16"},
          {"numpy_scalar": 1.25, "dtype": "float32"}]
CASES = [(value, "m") for value in VALUES] + [(VALUES[i], "not_a_unit") for i in (0, 8, 11, 12, 13, 14)]
q = quanta()
results = []
for method in ("quantity", "getunit", "add"):
    for value, unit in CASES:
        item = {"method": method, "value": value, "unit": unit}
        actual = value
        if isinstance(value, dict):
            if "numpy_scalar" in value:
                actual = np.dtype(value["dtype"]).type(value["numpy_scalar"])
            else:
                actual = np.array(value["numpy_array"], dtype=value["dtype"])
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
