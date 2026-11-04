#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta.isangle values and errors."""

import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402


VALUES = [
    "1rad", "-12deg", "1arcsec", "1h", "1s", "1d", "1m", "1Hz", "1m/s", "1", "nonsense",
    2, 0.0, True, None, [], [1, 2], {},
    {"value": 1.0, "unit": "rad"}, {"value": 2.0, "unit": "s"},
    {"value": 1.0, "unit": "m"}, {"value": [1.0, 2.0], "unit": "deg"},
]
q = quanta()
results = []
for value in VALUES:
    item = {"input": value}
    try:
        item["result"] = q.isangle(value)
    except Exception as error:
        item["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
