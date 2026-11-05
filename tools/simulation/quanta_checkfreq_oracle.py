#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta.checkfreq dimensions and conversion errors."""

import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import quanta, version_string  # noqa: E402


VALUES = [
    "1Hz", "0Hz", "1kHz", "1/s", "1rad/s", "1deg/s", "1s", "1h", "1m", "1km",
    "1/m", "1J", "1eV", "1kg.m", "1deg", "1d", "1m/s", "1kg.m/s", "1W", "1K", "1",
    "nonsense", 2, True, None, [], [1, 2], {},
    {"value": 1.0, "unit": "Hz"}, {"value": 2.0, "unit": "s"},
    {"value": 1.0, "unit": "rad"}, {"value": [1.0, 2.0], "unit": "Hz"},
]
q = quanta()
results = []
for value in VALUES:
    item = {"input": value}
    try:
        item["result"] = q.checkfreq(value)
    except Exception as error:
        item["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(item)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
