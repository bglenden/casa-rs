#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta trigonometric values, units and errors."""

import json
import math
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402


METHODS = ["sin", "cos", "tan", "asin", "acos", "atan"]
VALUES = [
    "30deg", "1rad", "-0.5rad", "0deg", "1arcsec", "1s", "1m", "1_", "50%",
    0.5, -1.0, 0.0, 1.0, 2.0, None, "nonsense",
    {"value": 0.5, "unit": ""}, {"value": [0.5, 1.0], "unit": "rad"},
]
q = quanta()
results = []
for method in METHODS:
    for value in VALUES:
        item = {"method": method, "input": value}
        try:
            result = getattr(q, method)(value)
            if math.isnan(result["value"]):
                result["value"] = "NaN"
            item["result"] = result
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
