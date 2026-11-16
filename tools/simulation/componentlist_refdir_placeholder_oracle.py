#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's reference-direction string getter placeholders."""

import json
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatools import componentlist, version_string  # noqa: E402

METHODS = ["getrefdirra", "getrefdirdec"]
CALLS = [
    ([0], {}), ([-1], {}), ([2**40], {}), ([True], {}),
    ([1.5], {}), (["0"], {}), ([None], {}), ([[0]], {}), ([2**100], {}),
    ([0, "rad", 0], {}), ([0, "bad", -1], {}), ([0, "", 2**40], {}),
    ([0], {"unit": None}), ([0], {"unit": 5}), ([0], {"unit": ["deg"]}),
    ([0], {"precision": None}), ([0], {"precision": 1.5}),
    ([0], {"precision": "3"}), ([0], {"precision": True}),
    ([0], {"precision": 2**100}), ([None], {"unit": None}),
    ([0], {"unit": None, "precision": None}),
]
results = []
for populated in (False, True):
    c = componentlist()
    if populated:
        c.addcomponent(flux=1, freq="1.4 GHz")
    for method in METHODS:
        for args, kwargs in CALLS:
            item = {"populated": populated, "method": method, "args": args, "kwargs": kwargs}
            try:
                item["result"] = getattr(c, method)(*args, **kwargs)
            except Exception as error:
                item["error"] = {"type": type(error).__name__, "message": str(error)}
            results.append(item)
    c.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results}))
