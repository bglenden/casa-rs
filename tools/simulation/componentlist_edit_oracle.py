#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's component-list edit placeholder and its arguments."""

import json

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def value(item):
    if isinstance(item, dict) and "numpy" in item:
        return getattr(np, item["numpy"])(item["value"])
    return item


CALLS = [
    ([0], {}), ([-1], {}), ([2**40], {}), ([True], {}),
    ([None], {}), ([2**100], {}), ([1.5], {}), (["0"], {}), ([[0]], {}),
    ([{"numpy": "int64", "value": 2}], {}),
    ([0, True], {}), ([0, False], {}), ([0, 0], {}), ([0, 1], {}),
    ([0, 2], {}), ([0, None], {}), ([0, 1.0], {}), ([0, ""], {}),
    ([0, []], {}), ([0, {}], {}), ([None, None], {}), ([2**100, None], {}),
    ([1.5, None], {}), ([0], {"log": False}), ([0], {"log": True}),
    ([0], {"log": {"numpy": "bool_", "value": True}}),
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
            item["result"] = cl.edit(*(value(x) for x in args),
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
index_result = {"result": cl.edit(index), "calls": index.calls}
cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
                                   "index_once": index_result}))
