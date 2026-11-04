#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record direct simutil tilings with CASA 6.7.6.14 as JSON on stdout."""

import json

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatasks.private.simutil import simutil  # noqa: E402


CASES = {
    "getTrianglePoints": [
        [4.0, 2.0, 0.0, 1.0],
        [2.0, 4.0, 33.0, 1.0],
        [2.0, 2.0, -45.0, 1.0],
        [0.1, 0.1, 15.0, 2.0],
        [4.0, 2.0, 0.0, 0.0],
    ],
    "getTriangularTiling": [
        [4.0, 2.0, 0.0, 1.0, 1.0],
        [2.0, 4.0, 33.0, 1.0, 1.3],
        [2.0, 2.0, -45.0, 1.0, 0.8],
        [4.0, 1.0, 15.0, 1.0, 1.5],
        [0.1, 0.1, 15.0, 2.0, 3.0],
        [4.0, 2.0, 0.0, 0.0, 1.0],
    ],
}

u = simutil()
result = []
for method, cases in CASES.items():
    for args in cases:
        case = {"method": method, "args": args}
        try:
            case["result"] = getattr(u, method)(*args)
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        result.append(case)
print(json.dumps(result, indent=2))
