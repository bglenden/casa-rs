#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14's direct simutil arithmetic and result types."""

import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatasks.private.simutil import simutil  # noqa: E402


CASES = [
    ("isEven", [-3]), ("isEven", [0]), ("isEven", [2**53 + 1]),
    ("isEven", [2**100]), ("isEven", [-2.0]), ("isEven", [1.5]),
    ("isEven", [{"array": [-3, 0, 2, 2**53 + 1], "dtype": "int64"}]),
    ("isEven", [{"array": [[-2, -1], [0, 1]], "dtype": "int32"}]),
    ("isEven", [{"scalar": 7, "dtype": "int64"}]),
    ("isEven", [[1, 2]]),
    ("applyRotate", [3, 4, 1, 0]), ("applyRotate", [1.5, -2, 0.6, 0.8]),
    ("applyRotate", [{"array": [1., 2.], "dtype": "float64"},
                     {"array": [3., 4.], "dtype": "float64"}, 0.6, 0.8]),
    ("applyRotate", [1, 2, None, 0]),
    ("needsFiller", [4, 1, 1, 4]), ("needsFiller", [4.01, 1, 1, 4]),
    ("needsFiller", [3.99, 1, 1, 4]), ("needsFiller", [0, 1, 0, 0]),
    ("needsFiller", [{"array": [0, 4], "dtype": "int64"}, 1, 1, 4]),
]


def value(spec):
    if isinstance(spec, dict) and "array" in spec:
        return np.array(spec["array"], dtype=spec["dtype"])
    if isinstance(spec, dict) and "scalar" in spec:
        return np.dtype(spec["dtype"]).type(spec["scalar"])
    return spec


def describe(result):
    if isinstance(result, tuple):
        return {"type": "tuple", "items": [describe(item) for item in result]}
    if isinstance(result, np.ndarray):
        return {"type": "ndarray", "dtype": str(result.dtype), "value": result.tolist()}
    if isinstance(result, np.generic):
        return {"type": type(result).__name__, "value": result.item()}
    return {"type": type(result).__name__, "value": result}


u = simutil()
results = []
for method, args in CASES:
    case = {"method": method, "args": args}
    try:
        case["result"] = describe(getattr(u, method)(*(value(arg) for arg in args)))
    except Exception as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(case)
print("CASA_RESULT=" + json.dumps(results))
