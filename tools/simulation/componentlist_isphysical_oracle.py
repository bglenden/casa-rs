#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record populated-component physicality and index behavior in CASA."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

FLUXES = [1.0, -1.0, 0.0, [1.0, 1.0, 0.0, 0.0], [1.0, 2.0, 0.0, 0.0], [1.0, 0.5, 0.5, 0.5]]
INDICES = [[0], [1], [2], [3], [4], [5], [-1], [], [0, 1], [0, 4], [4, 0], [99], [-2], [-1, 0], 0, 1.9, "0", None]
cl = componentlist()
empty_vector = cl.isphysical([])
for flux in FLUXES:
    cl.addcomponent(dir="J2000 0deg 0deg", flux=flux)
cases = []
for which in INDICES:
    case = {"which": which}
    try:
        case["result"] = cl.isphysical(which)
    except Exception as error:
        case["error"] = {"type": type(error).__name__}
        if "Index out of range." in str(error):
            case["error"]["message"] = "Index out of range."
        else:
            case["error"]["message"] = str(error)
    cases.append(case)
cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "empty_vector": empty_vector, "fluxes": FLUXES, "cases": cases}))
