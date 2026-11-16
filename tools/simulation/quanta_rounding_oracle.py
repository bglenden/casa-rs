#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's quanta.ceil and quanta.floor results and errors."""

import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402


VALUES = [
    "1.6m", "-1.6m", "-0.1m", "0m", "1.0m", "1.2deg", "-3.25s", "2.5Hz",
    "1e20km", 2.5, -2.5, 0.0, True, None, [], [1.2, 2.3], "nonsense", {},
    {"value": 1.6, "unit": "km"}, {"value": [-1.5, 1.5], "unit": "deg"},
]
q = quanta()
results = []
for method in ("ceil", "floor"):
    for value in VALUES:
        item = {"method": method, "input": value}
        try:
            item["result"] = getattr(q, method)(value)
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
