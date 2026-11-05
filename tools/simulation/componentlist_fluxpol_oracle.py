#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's unimplemented componentlist polarization getter."""

import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import componentlist, version_string  # noqa: E402


METHODS = ["getfluxpol"]
VALUES = [0, -1, 2**40, True, 1.5, "0", None, [0], 2**100]
c = componentlist()
results = []
for method in METHODS:
    for value in VALUES:
        item = {"method": method, "input": value}
        try:
            item["result"] = getattr(c, method)(value)
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
c.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
