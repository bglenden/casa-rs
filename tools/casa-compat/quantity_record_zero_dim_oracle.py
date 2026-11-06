#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's zero-dimensional floating quantity inputs."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

qa = quanta()
cases = []
for dtype in ("float32", "float64"):
    for value in (0.0, 1.25, -2.5):
        for unit in ("m", "not_a_unit"):
            for method in ("quantity", "getunit", "getvalue", "add", "convert"):
                record = {"unit": unit, "value": np.array(value, dtype=dtype)}
                args = (record,) + (("1m",) if method == "add" else (("cm",) if method == "convert" else ()))
                case = {"dtype": dtype, "value": value, "unit": unit, "method": method}
                try:
                    result = getattr(qa, method)(*args)
                    if isinstance(result, dict) and isinstance(result.get("value"), np.ndarray):
                        result = {"unit": result["unit"], "value": result["value"].tolist(), "shape": list(result["value"].shape)}
                    case["result"] = result
                except Exception as error:
                    case["error"] = {"type": type(error).__name__, "message": str(error)}
                cases.append(case)
for dtype in ("float32", "float64"):
    for keepshape in (False, True):
        result = qa.quantity(np.array(1.25, dtype=dtype), keepshape=keepshape)
        cases.append({"dtype": dtype, "value": 1.25, "method": "bare_quantity", "keepshape": keepshape,
                      "result": {"unit": result["unit"], "value": result["value"].tolist(), "shape": list(result["value"].shape)}})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
