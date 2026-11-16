#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's component removal, recovery, purge and crash behavior."""
import json
import resource
import subprocess
import sys

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def snapshot(cl):
    result = {"labels": [], "selected": [int(x) for x in cl.selected()], "shape_units": [], "shape_values": [], "fluxes": []}
    for index in range(cl.length()):
        result["labels"].append(cl.getlabel(index))
        result["fluxes"].append(float(cl.getfluxvalue(index)[0].real))
        shape = cl.getshape(index)
        result["shape_units"].append(
            [shape[k]["unit"] for k in ("majoraxis", "minoraxis", "positionangle")]
            if shape else None
        )
        result["shape_values"].append(
            [float(shape[k]["value"]) for k in ("majoraxis", "minoraxis", "positionangle")]
            if shape else None
        )
    return result


scenarios = [
    {"name": "single", "select": [0, 1, 3], "actions": [["remove", [[1]]], ["recover", []], ["recover", []]]},
    {"name": "reverse_order", "select": [0, 1, 3], "actions": [["remove", [[3, 1]]], ["recover", []]]},
    {"name": "forward_order", "actions": [["remove", [[1, 3]]], ["recover", []]]},
    {"name": "remove_all", "actions": [["remove", []], ["recover", []]]},
    {"name": "nothing", "actions": [["remove", [[]]], ["recover", []], ["purge", []]]},
    {"name": "separate", "actions": [["remove", [[1]]], ["remove", [[1]]], ["recover", []]]},
    {"name": "purged", "actions": [["remove", [[1, 3]]], ["purge", []], ["recover", []]]},
    {"name": "released", "actions": [["remove", [[1]]], ["done", []], ["recover", []]]},
    {"name": "invalid", "select": [0, 1, 3], "actions": [["remove", [[1, -1]]], ["remove", [[1, 7]]], ["recover", []]]},
    {"name": "error_preserves_bin", "actions": [["remove", [[1]]], ["remove", [[7]]], ["recover", []]]},
    {"name": "bindings", "actions": [["remove", [None]], ["remove", ["1"]], ["remove", [0, None]], ["remove", [1, False]], ["recover", []]]},
    {"name": "duplicate_corrupts", "select": [0, 1, 3], "actions": [["remove", [[1, 1]]], ["recover", []]]},
]
for scenario in scenarios:
    cl = componentlist()
    for i in range(4):
        if i == 1:
            cl.addcomponent(label=str(i), flux=i + 1, shape="Gaussian", majoraxis="2arcmin", minoraxis="45arcsec", positionangle="15deg")
        elif i == 3:
            cl.addcomponent(label=str(i), flux=i + 1, shape="Gaussian", majoraxis="0.05deg", minoraxis="1arcmin", positionangle="0.5rad")
        else:
            cl.addcomponent(label=str(i), flux=i + 1)
    if "select" in scenario:
        cl.select(scenario["select"])
    scenario["before"] = snapshot(cl)
    scenario["steps"] = []
    for method, args in scenario["actions"]:
        step = {}
        try:
            step["result"] = getattr(cl, method)(*args)
        except Exception as error:
            message = str(error)
            if "Index out of range." in message:
                message = "Index out of range.\nA component number is less than one or greater than the list length\nNo components removed"
            step["error"] = {"type": type(error).__name__, "message": message}
        step["after"] = snapshot(cl)
        scenario["steps"].append(step)
    cl.done()


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


crashes = {}
for expression in ("c.remove()", "c.remove([-1])"):
    code = ("from casaconfig import config; config.data_auto_update=False; "
            "config.measures_auto_update=False; from casatools import componentlist; "
            "c=componentlist(); " + expression)
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, preexec_fn=no_core)
    crashes[expression] = result.returncode

print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "scenarios": scenarios, "empty_crashes": crashes}))
