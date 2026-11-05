#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta scalar relational results and errors."""

import json
import math
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402

METHODS = ["eq", "ne", "lt", "le", "gt", "ge"]
PAIRS = [
    ("1m", "100cm"), ("0.1m", "10cm"), ("2m", "1m"), ("1m", "2m"),
    ("1km", "1001m"), ("1%", "0.01"), ("12h", "180deg"), ("1_", "1"),
    ("1m", "1s"), ("1s", "1m"), ("0", "0m"), (None, None), ([], []),
    (1, 1), (0.0, -0.0), ([1, 2], 1), ("bad", "1"), ("1", "bad"),
    ("badleft", "badright"), ({"value": [1.0, 2.0], "unit": "m"}, "badright"),
    ({"value": 1.0, "unit": "m"}, "100cm"),
    ({"value": [1.0], "unit": "m"}, "1m"),
    ({"value": [1.0, 2.0], "unit": "m"}, "1m"),
    ("1m", {"value": [1.0, 2.0], "unit": "m"}),
    ("NaN", 1.0), (1.0, "NaN"), ("Infinity", "Infinity"),
    ("-Infinity", "Infinity"),
]
SPECIAL = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
q = quanta()
results = []
for method in METHODS:
    for left, right in PAIRS:
        item = {"method": method, "left": left, "right": right}
        args = [SPECIAL.get(v, v) if isinstance(v, str) else v for v in (left, right)]
        try:
            item["result"] = getattr(q, method)(*args)
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
