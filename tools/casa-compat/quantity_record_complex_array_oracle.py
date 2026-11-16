#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity-record complex NumPy array diagnostics."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

SHAPES = [[], [0], [1], [2], [1, 1]]
q = quanta()
cases = []
bare = []
for dtype in ("complex64", "complex128"):
    for shape in SHAPES:
        count = int(np.prod(shape)) if shape else 1
        value = np.array([complex(i + 1, i + 2) for i in range(count)], dtype=dtype).reshape(shape)
        bare.append({"dtype": dtype, "shape": shape, "result": q.quantity(value)})
for method in ("quantity", "getunit", "add"):
    for dtype in ("complex64", "complex128"):
        for shape in SHAPES:
            count = int(np.prod(shape)) if shape else 1
            value = np.array([complex(i + 1, i + 2) for i in range(count)], dtype=dtype).reshape(shape)
            for unit in ("m", "not_a_unit"):
                case = {"method": method, "dtype": dtype, "shape": shape, "unit": unit}
                args = ({"value": value, "unit": unit},) + (("1m",) if method == "add" else ())
                try:
                    case["result"] = getattr(q, method)(*args)
                except Exception as error:
                    case["error"] = {"type": type(error).__name__, "message": str(error)}
                cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "bare": bare, "cases": cases}))
