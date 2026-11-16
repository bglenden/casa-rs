#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component shape units across a table round trip."""
import json
import os
import tempfile
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

COMPONENTS = [
    {"shape": "point"},
    {"shape": "Gaussian", "majoraxis": "2arcsec", "minoraxis": "1arcsec", "positionangle": "30deg"},
    {"shape": "disk", "majoraxis": "0.5deg", "minoraxis": "1arcmin", "positionangle": "0.5rad"},
    {"shape": "Gaussian", "majoraxis": "2arcmin", "minoraxis": "30arcsec", "positionangle": "-30deg"},
]
FIELDS = ("type", "majoraxis", "minoraxis", "positionangle", "majoraxiserror", "minoraxiserror", "positionangleerror")
cl = componentlist()
cases = []

def capture(stage):
    record = cl.torecord()
    shapes = []
    for index in range(len(COMPONENTS)):
        shape = record[f"component{index}"]["shape"]
        shapes.append({name: shape[name] for name in FIELDS if name in shape})
    cases.append({"stage": stage, "shapes": shapes})

with tempfile.TemporaryDirectory() as tmp:
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
