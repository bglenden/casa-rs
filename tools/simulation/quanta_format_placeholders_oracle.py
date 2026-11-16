#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture CASA's format getter and setter placeholders."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

ARGS = [
    [], [""], ["prec"], ["unknown"], [{"bytes": [112, 114, 101, 99]}],
    [{"bytes": [255]}], [None], [1], [True], [[]], [{}],
]
SET_ARGS = ARGS + [["prec", "4"], ["", ""], ["prec", {"bytes": [255]}],
                   ["prec", None], ["prec", 4], ["prec", []]]
q = quanta()
cases = []
for method, inputs in (("getformat", ARGS), ("setformat", SET_ARGS)):
    for args in inputs:
        actual = [bytes(x["bytes"]) if isinstance(x, dict) and "bytes" in x else x for x in args]
        case = {"method": method, "args": args}
        try:
            case["result"] = getattr(q, method)(*actual)
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
