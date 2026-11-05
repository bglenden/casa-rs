#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA quanta frequency and Doppler conversions."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

CASES = []
for value in ("1GHz", "21cm", "2s", "1J", "0Hz", "0m", "0s", "0J", "-1GHz", "1km/s", "0.5", "2rad", "bad quantity"):
    for unit in ("Hz", "m", "s", "J", "km/s", "", "badunit"):
        CASES.append(("convertfreq", value, unit))
for value in ("1km/s", "3e8m/s", "0.5", "0%", "-2km/s", "1Hz", "2rad", "bad quantity"):
    for unit in ("km/s", "m/s", "", "1", "%", "Hz", "badunit"):
        CASES.append(("convertdop", value, unit))
for value in ("1rad/s", "1/m", "1kg.m", "0rad/s", "0/m", "0kg.m"):
    for unit in ("Hz", "rad/s", "/m", "kg.m", "m", "s", "J"):
        CASES.append(("convertfreq", value, unit))
for method in ("convertfreq", "convertdop"):
    for value in ("1GHz", "1km/s", 0, {"value": float("nan"), "unit": ""}, {"value": float("inf"), "unit": "m/s"}):
        CASES.append((method, value, None))
q = quanta()
results = []
for method, value, unit in CASES:
    case = {"method": method, "value": value, "unit": unit}
    try:
        case["result"] = getattr(q, method)(value) if unit is None else getattr(q, method)(value, unit)
    except Exception as error:
        case["error"] = {"type": type(error).__name__, "message": str(error)}
    results.append(case)
argument_cases = []
for method in ("convertfreq", "convertdop"):
    for args in ([], [None], ["1GHz", None], ["1GHz", 3], ["1GHz", []],
                 ["1GHz", {"bytes": "Hz"}], [[1, 2]], [{"value": [1.0, 2.0], "unit": "m"}]):
        case = {"method": method, "args": args}
        actual = [v["bytes"].encode() if isinstance(v, dict) and "bytes" in v else v for v in args]
        try:
            case["result"] = getattr(q, method)(*actual)
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        argument_cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results, "argument_cases": argument_cases}))
