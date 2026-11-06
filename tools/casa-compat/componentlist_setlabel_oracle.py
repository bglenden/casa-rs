#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA componentlist.setlabel behavior, including failed updates."""
import json
import tempfile
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

inputs = [
    ("empty", 0, "x", True),
    ("populated", 0, "new", True),
    ("populated", 1, "αβ 🛰", False),
    ("populated", 1, "", True),
    ("populated", -1, "x", True),
    ("populated", 2, "x", True),
    ("populated", 2**32, "wrapped", True),
    ("populated", True, "boolean index", True),
    ("populated", None, "x", True),
    ("populated", 2**63, "x", True),
    ("populated", 0.0, "x", True),
    ("populated", "0", "x", True),
    ("populated", 0, None, True),
    ("populated", 0, 42, True),
    ("populated", 0, {"bytes": "bytes"}, True),
    ("populated", 0, {"hex": "6100ff"}, True),
    ("populated", 0, "abc\u0000def", True),
    ("populated", 0, "x", None),
    ("populated", 0, "x", 1),
    ("populated", 0, "x", "false"),
]
cases = []
for state, which, value, log in inputs:
    cl = componentlist()
    if state == "populated":
        cl.addcomponent(label="first")
        cl.addcomponent(label="second")
    case = {"state": state, "which": which, "value": value, "log": log}
    arg = (value["bytes"].encode() if "bytes" in value else bytes.fromhex(value["hex"])) if isinstance(value, dict) else value
    try:
        case["result"] = cl.setlabel(which, arg, log)
    except Exception as error:
        message = str(error)
        if ") Failed AlwaysAssert " in message:
            message = "Failed AlwaysAssert " + message.split(") Failed AlwaysAssert ", 1)[1]
        case["error"] = {"type": type(error).__name__, "message": message}
    case["labels"] = [cl.getlabel(i) for i in range(cl.length())]
    cases.append(case)
with tempfile.TemporaryDirectory(prefix="casa-setlabel-") as directory:
    filename = directory + "/components"
    cl = componentlist()
    cl.addcomponent(label="first")
    cl.addcomponent(label="second")
    cl.rename(filename)
    cl.setlabel(1, "changed")
    cl.close()
    cl.open(filename, nomodify=True)
    reopened = [cl.getlabel(i) for i in range(2)]
    cl.setlabel(0, "temporary")
    readonly_memory = [cl.getlabel(i) for i in range(2)]
    cl.close()
    cl.open(filename)
    after_readonly = [cl.getlabel(i) for i in range(2)]
    cl.done()
cl = componentlist()
cl.addcomponent(label="preserved")
invalid_set = cl.setlabel(0, b"\xff")
try:
    cl.getlabel(0)
except Exception as error:
    invalid_read = type(error).__name__
print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "cases": cases,
    "table": {"reopened": reopened, "readonly_memory": readonly_memory, "after_readonly": after_readonly},
    "invalid_utf8": {"set": invalid_set, "read_error": invalid_read},
}, ensure_ascii=False))
