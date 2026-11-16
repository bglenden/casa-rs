#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta exponential and logarithm results and errors."""

import json
import math
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402


METHODS = ["exp", "log", "log10"]
VALUES = [
    "1", "50%", "1_", "1rad", "1m", "4m2", 0.5, 0.0, -1.0, 1.0, 10.0, 1000.0,
    None, [], [1, 2], "nonsense", {}, {"value": 0.5, "unit": ""},
    {"value": [0.5, 1.0], "unit": ""},
]
q = quanta()
results = []
for method in METHODS:
    for value in VALUES:
        item = {"method": method, "input": value}
        try:
            result = getattr(q, method)(value)
            number = result["value"]
            if not math.isfinite(number):
                result["value"] = "NaN" if math.isnan(number) else ("Infinity" if number > 0 else "-Infinity")
            item["result"] = result
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
