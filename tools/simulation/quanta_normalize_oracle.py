#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quantity angle normalization and conversion errors."""
import json
import math
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

VALUES = ["370deg", "-190deg", "180deg", "-180deg", "360deg", "0deg", "2rad", "12h", "-6h", "1m", "10", "1_", "bad", None, {"value": -0.0, "unit": "deg"}, {"special": "NaN", "unit": "deg"}, {"special": "Infinity", "unit": "deg"}, {"special": "-Infinity", "unit": "deg"}]
ORIGINS = [-0.5, 0.0, 0.25, 1.0]
CASES = [(value, origin) for value in VALUES for origin in ORIGINS] + [
    ("1deg", {"special": "NaN"}), ("1deg", {"special": "Infinity"}),
    ("1deg", {"special": "-Infinity"}), ("10deg", None), ("10deg", True),
    ("10deg", "0.5"), ("10deg", [0.5]), ("bad", None),
    ({"value": [1.0, 2.0], "unit": "deg"}, -0.5),
]
SPECIAL = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
q = quanta()
results = []
for value, origin in CASES:
    item = {"input": value, "origin": origin}
    if isinstance(value, dict) and "special" in value:
        value = {"value": SPECIAL[value["special"]], "unit": value["unit"]}
    if isinstance(origin, dict) and "special" in origin:
        origin = SPECIAL[origin["special"]]
    try:
        result = q.norm(value, origin)
        number = result["value"]
        if not math.isfinite(number):
            result["value"] = "NaN" if math.isnan(number) else ("Infinity" if number > 0 else "-Infinity")
        item["result"] = result
    except Exception as error:
        item["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
