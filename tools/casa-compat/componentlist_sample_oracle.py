#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's componentlist.sample placeholder and argument handling."""
import json
import resource
import subprocess
import sys

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def inputs():
    return {
        "default": (), "none": (None,), "empty": ([],), "integer": (1,),
        "text": ("x",), "boolean": (True,), "record": ({"a": 1},),
        "array": (np.array([1, 2]),), "array0": (np.array(1.2),),
        "nested": ([[1, 2]],), "bytes": (b"x",), "complex": (1 + 2j,),
        "object": (object(),), "set": ({1, 2},), "u64": (np.uint64(2**63),),
        "huge": (2**63,), "negative_huge": (-2**63 - 1,),
        "huge_list": ([2**80],), "huge_tuple": ((2**80,),),
        "huge_record": ({"a": [2**80]},), "huge_key": ({2**80: 1},),
        "object_array": (np.array([2**80], dtype=object),),
        "realistic": ("J2000 0deg 0deg", "1arcsec", "1arcsec", "1GHz"),
        "huge_second": (None, 2**63), "huge_third": (None, None, 2**63),
        "huge_fourth": (None, None, None, 2**63),
    }


cases = []
cl = componentlist()
for state in ("empty", "populated", "done"):
    if state == "populated":
        cl.addcomponent(label="unchanged")
    elif state == "done":
        cl.done()
    for name, args in inputs().items():
        case = {"state": state, "input": name}
        try:
            result = cl.sample(*args)
            case["result"] = {"value": result.tolist(), "shape": list(result.shape), "dtype": str(result.dtype)}
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        case["length"] = cl.length()
        if cl.length():
            case["label"] = cl.getlabel(0)
        cases.append(case)


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


code = ("from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist; "
        "v=[]; v.append(v); componentlist().sample(v)")
cyclic_exit = subprocess.run([sys.executable, "-c", code], capture_output=True, preexec_fn=no_core).returncode
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases, "cyclic_exit": cyclic_exit}))
