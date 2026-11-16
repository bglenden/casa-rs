#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record component shapes before and after a CASA table round trip."""
import json
import os
import tempfile
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

COMPONENTS = [
    {"shape": "point", "majoraxis": "2arcsec", "minoraxis": "1arcsec", "positionangle": "0deg"},
    {"shape": "Gaussian", "majoraxis": "2arcsec", "minoraxis": "1arcsec", "positionangle": "30deg"},
    {"shape": "Disk", "majoraxis": "0.5deg", "minoraxis": "1arcmin", "positionangle": "0.5rad"},
    {"shape": "Gaussian", "majoraxis": "2arcmin", "minoraxis": "30arcsec", "positionangle": "-30deg"},
]
INDICES = [-1, 0, 1, 2, 3, 4, 2**32, 2**31, None, 1.5, "0"]
cl = componentlist()
cases = []
def capture(stage):
    for which in INDICES:
        case = {"stage": stage, "which": which}
        try:
            case["result"] = cl.getshape(which)
        except Exception as error:
            message = str(error)
            if "Failed AlwaysAssert index < nelements()" in message:
                message = "Failed AlwaysAssert index < nelements()"
            case["error"] = {"type": type(error).__name__, "message": message}
        cases.append(case)

with tempfile.TemporaryDirectory() as tmp:
    capture("empty")
    for component in COMPONENTS:
        cl.addcomponent(dir="J2000 0deg 0deg", flux=1.0, **component)
    capture("added")
    filename = os.path.join(tmp, "components")
    cl.rename(filename)
    capture("renamed")
    cl.close()
    cl.open(filename)
    capture("reopened")
    cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "components": COMPONENTS, "cases": cases}))
