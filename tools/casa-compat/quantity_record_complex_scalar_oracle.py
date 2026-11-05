#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity-record complex scalar type diagnostics."""
import json
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = [
    {"dtype": "complex64", "real": 1.25, "imag": 2.5},
    {"dtype": "complex64", "real": -3.0, "imag": 0.0},
    {"dtype": "complex64", "real": 0.0, "imag": 0.0},
    {"dtype": "complex64", "real": 1e-10, "imag": 1e10},
    {"dtype": "complex128", "real": 1.25, "imag": 2.5},
    {"dtype": "python_complex", "real": 1.25, "imag": 2.5},
]
q = quanta()
cases = []
for method in ("quantity", "getunit", "add"):
    for value in VALUES:
        for unit in ("m", "not_a_unit"):
            number = complex(value["real"], value["imag"])
            actual = number if value["dtype"] == "python_complex" else np.dtype(value["dtype"]).type(number)
            args = ({"value": actual, "unit": unit},) + (("1m",) if method == "add" else ())
            case = {"method": method, "value": value, "unit": unit}
            try:
                case["result"] = getattr(q, method)(*args)
            except Exception as error:
                case["error"] = {"type": type(error).__name__, "message": str(error)}
            cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
