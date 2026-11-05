#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA scalar quantity integer-power results and errors."""
import json
import math
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = ["2m", "2km", "4m2", "3m/s", "0m", "-2m", "2%", "2_", "4rad", None, "bad", 0.1, -0.0, "NaN", "Infinity", "-Infinity"]
CASES = [(value, power) for value in VALUES for power in (-2, -1, 0, 1, 2, 3)] + [
    ("1m", -100), ("1m", -99), ("1m", 99), ("1m", 100), ("1m10", 99),
    ("2m", None), ("2m", True), ("2m", False), ("2m", 1.0), ("2m", "2"),
    ("2m", [2]), ("2m", 2**32), ("2m", 2**32+1), ("2m", 2**63),
    ("bad", None), ("bad", 100), ({"value": [1.0, 2.0], "unit": "m"}, None),
]
SPECIAL = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
q = quanta()
results = []
for value, power in CASES:
    item = {"input": value, "power": power}
    value_arg = SPECIAL.get(value, value) if isinstance(value, str) else value
    try:
        result = q.pow(value_arg, power)
        number = result["value"]
        if not math.isfinite(number):
            result["value"] = "NaN" if math.isnan(number) else ("Infinity" if number > 0 else "-Infinity")
        item["result"] = result
    except Exception as error:
        item["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
