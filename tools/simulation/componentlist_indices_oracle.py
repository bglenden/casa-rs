#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's componentlist.indices placeholder and array type."""
import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

cases = []
for count, select in [(0, []), (1, []), (3, []), (3, [0, 2])]:
    cl = componentlist()
    for _ in range(count):
        cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    if select:
        cl.select(select)
    result = cl.indices()
    cases.append({"count": count, "select": select, "result": result.tolist(),
                  "dtype": str(result.dtype), "shape": list(result.shape)})
    cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
