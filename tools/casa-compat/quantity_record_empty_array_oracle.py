#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's handling of empty quantity-record vectors."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

q = quanta()
cases = []
for dtype in ("float16", "float32", "float64", "int32", "bool", "complex64"):
    for unit in ("m", "not_a_unit"):
        for method in ("quantity", "getunit", "getvalue", "add", "convert"):
            if method == "convert" and dtype in ("int32", "complex64"):
                continue
            record = {"value": np.array([], dtype=dtype), "unit": unit}
            args = (record,) + (("1m",) if method == "add" else (("cm",) if method == "convert" else ()))
            case = {"dtype": dtype, "unit": unit, "method": method}
            try:
                result = getattr(q, method)(*args)
                if isinstance(result, dict) and isinstance(result.get("value"), np.ndarray):
                    result = {"unit": result["unit"], "value": result["value"].tolist(), "shape": list(result["value"].shape)}
                elif isinstance(result, np.ndarray):
                    result = {"array": result.tolist(), "shape": list(result.shape)}
                case["result"] = result
            except Exception as error:
                case["error"] = {"type": type(error).__name__, "message": str(error)}
            cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
