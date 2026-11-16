#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's component-list convertshape placeholder and its arguments."""

import json

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def value(item):
    if isinstance(item, dict) and "bytes" in item:
        return bytes(item["bytes"])
    return item


CALLS = [
    ([0], {}), ([-1], {}), ([2**40], {}), ([True], {}),
    ([None], {}), ([2**100], {}), ([1.5], {}), (["0"], {}), ([[0]], {}),
    ([0, "rad", "deg", "arcsec"], {}), ([0, "", "bad", "bad"], {}),
    ([0, {"bytes": [255]}], {}), ([0, "a\u0000b"], {}),
    ([0], {"majoraxis": None}), ([0], {"minoraxis": None}),
    ([0], {"positionangle": None}), ([0, 5], {}), ([0, "m", []], {}),
    ([0, "m", "m", True], {}), ([1.5, None], {}), ([None, None], {}),
    ([2**100, None], {}), ([None, "rad"], {}),
    ([0], {"majoraxis": "rad", "positionangle": "bad"}),
]

results = []
for state in ("empty", "populated", "closed"):
    cl = componentlist()
    if state != "empty":
        cl.addcomponent(flux=1, dir="J2000 00h00m00s +00d00m00s", shape="Gaussian",
                        majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
    if state == "closed":
        cl.done()
    for args, kwargs in CALLS:
        item = {"state": state, "args": args, "kwargs": kwargs}
        try:
            item["result"] = cl.convertshape(*(value(x) for x in args),
                                               **{k: value(v) for k, v in kwargs.items()})
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(item)
    cl.done()


class CountingIndex:
    calls = 0

    def __index__(self):
        self.calls += 1
        return 0


cl = componentlist()
index = CountingIndex()
index_result = {"result": cl.convertshape(index), "calls": index.calls}
cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
                                   "index_once": index_result}))
