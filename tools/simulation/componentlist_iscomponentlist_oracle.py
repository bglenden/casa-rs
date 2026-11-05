#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's componentlist.iscomponentlist placeholder behavior."""
import json
import resource
import subprocess
import sys

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402


def values():
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
        "huge_after_float": ([1.0, 2**80],), "huge_before_float": ([2**80, 1.0],),
        "huge_after_text": (["x", 2**80],), "huge_after_bool": ([True, 2**80],),
        "huge_after_none": ([None, 2**80],), "huge_nested_float": ([[1.0, 2**80]],),
        "huge_record_float": ({"v": [1.0, 2**80]},),
    }


def disable_core_dumps():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


child = subprocess.run(
    [sys.executable, "-c", (
        "from casaconfig import config\n"
        "config.data_auto_update=config.measures_auto_update=False\n"
        "from casatools import componentlist\n"
        "value=[]; value.append(value)\n"
        "print(componentlist().iscomponentlist(value))\n"
    )],
    capture_output=True, text=True, timeout=15, preexec_fn=disable_core_dumps,
)
cl = componentlist()
cases = []
for state in ("empty", "populated", "done"):
    if state == "populated":
        cl.addcomponent()
    elif state == "done":
        cl.done()
    for name, args in values().items():
        case = {"state": state, "input": name}
        try:
            case["result"] = cl.iscomponentlist(*args)
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cyclic_exit": child.returncode, "cases": cases}))
