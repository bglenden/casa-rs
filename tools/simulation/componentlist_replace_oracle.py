#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's component-list replace placeholder and argument handling."""

import json

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def value(item):
    if isinstance(item, dict) and "__numpy__" in item:
        if item["__numpy__"] == "array":
            return np.array(item["value"], dtype=item.get("dtype"))
        return getattr(np, item["__numpy__"])(value(item["value"]))
    if isinstance(item, dict) and "__bytes__" in item:
        return bytes(item["__bytes__"])
    if isinstance(item, dict) and "__complex__" in item:
        return complex(*item["__complex__"])
    if isinstance(item, dict) and "__object__" in item:
        return object()
    if isinstance(item, dict):
        return {k: value(v) for k, v in item.items()}
    if isinstance(item, list):
        return [value(v) for v in item]
    return item


CALLS = [
    ([0, {}], {}), ([-1, {}], {}), ([2**40, {}], {}), ([True, {}], {}),
    ([None, {}], {}), ([2**100, {}], {}), ([1.5, {}], {}), (["0", {}], {}),
    ([[0], {}], {}), ([0, None], {}), ([0, []], {}), ([0, 1], {}),
    ([0, "x"], {}), ([0, {"foo": 1}], {}),
    ([0, {"x": 2**100}], {}), ([0, {"x": [2**100]}], {}),
    ([0, {"x": [{"n": 2**100}]}], {}),
    ([0, {}], {"whichones": None}), ([0, {}, []], {}), ([0, {}, [-1]], {}),
    ([0, {}, [1.5]], {}), ([0, {}, [None]], {}), ([0, {}, ["1"]], {}),
    ([0, {}, 0], {}), ([0, {}, 1.5], {}), ([0, {}, "a"], {}),
    ([0, {}, {"__bytes__": [97]}], {}), ([0, {}, [2**100]], {}),
    ([0, {}, {"x": 2**100}], {}), ([0, {}, {}], {}),
    ([0, {}, [{"x": 2**100}]], {}), ([0, {}, [[1, 2], [3]]], {}),
    ([0, {}, {"__numpy__": "int64", "value": 2}], {}),
    ([0, {}, {"__numpy__": "uint64", "value": 2}], {}),
    ([0, {}, {"__numpy__": "bool_", "value": True}], {}),
    ([0, {}, {"__numpy__": "float16", "value": 2.0}], {}),
    ([0, {}, {"__numpy__": "float32", "value": 2.0}], {}),
    ([0, {}, {"__numpy__": "float64", "value": 2.0}], {}),
    ([0, {}, {"__complex__": [1, 2]}], {}),
    ([0, {}, {"__numpy__": "complex64", "value": {"__complex__": [1, 2]}}], {}),
    ([0, {}, [{"__numpy__": "complex64", "value": {"__complex__": [1, 2]}}]], {}),
    ([0, {}, {"__object__": True}], {}),
    ([0, {}, [{"__numpy__": "int64", "value": 2}]], {}),
    ([0, {}, {"__numpy__": "array", "value": 2}], {}),
    ([0, {}, {"__numpy__": "array", "value": [1, 2]}], {}),
    ([0, {}, {"__numpy__": "array", "value": [], "dtype": "float64"}], {}),
    ([0, {}, {"__numpy__": "array", "value": [2**100], "dtype": "object"}], {}),
    ([None, None, None], {}), ([1.5, None, "bad"], {}),
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
            item["result"] = cl.replace(*(value(x) for x in args),
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
index_result = {"result": cl.replace(index, {}), "calls": index.calls}
index_whichones = CountingIndex()
try:
    cl.replace(0, {}, index_whichones)
    whichones_result = {"result": False, "calls": index_whichones.calls}
except Exception as error:
    whichones_result = {"error": {"type": type(error).__name__, "message": str(error)},
                        "calls": index_whichones.calls}
cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
                                   "index_once": index_result, "index_whichones": whichones_result}))
