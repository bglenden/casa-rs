#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA componentlist selection, including conversion side effects."""
import json
import os
import tempfile
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

# Each case starts with three components and a selection of index 1.
CASES = [
    ("select", []), ("select", [0, 2]), ("select", [2, 0, 2]),
    ("deselect", [1]), ("deselect", [0]), ("select", [-1]), ("deselect", [-1]),
    ("select", [-2]), ("select", [0, 3]), ("deselect", [0, 3]),
    ("select", [-1, 1]), ("select", [1.9]), ("select", ["1.9"]),
    ("select", ["bad"]), ("select", [None]), ("select", [[0, 2]]),
    ("select", 0), ("select", True), ("select", "1"), ("select", None),
    ("select", 2**32), ("select", [2**63]),
    ("select", {"numpy": [2**63], "dtype": "uint64"}),
    ("select", {"numpy": [[0, 2]], "dtype": "int64"}),
    ("select", {"numpy": 2, "dtype": "int64"}),
    ("select", {"scalar_numpy": 2, "dtype": "int64"}),
    ("select", {"scalar_numpy": 1.9, "dtype": "float64"}),
]
results = []
for method, value in CASES:
    cl = componentlist()
    for _ in range(3):
        cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    cl.select([1])
    item = {"method": method, "input": value}
    if isinstance(value, dict):
        argument = np.dtype(value["dtype"]).type(value["scalar_numpy"]) if "scalar_numpy" in value else np.array(value["numpy"], dtype=value["dtype"])
    else:
        argument = value
    try:
        item["result"] = getattr(cl, method)(argument)
    except Exception as error:
        message = str(error)
        # CASA embeds the current time in out-of-range diagnostics.
        if "Index out of range." in message:
            message = "Index out of range."
        item["error"] = {"type": type(error).__name__, "message": message}
    item["selected"] = cl.selected().tolist()
    item["dtype"] = str(cl.selected().dtype)
    results.append(item)
    cl.done()
with tempfile.TemporaryDirectory(prefix="casa-selection-") as tmp:
    cl = componentlist()
    cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    cl.select([-1])
    cl.addcomponent(flux=1, dir="J2000 0deg 0deg")
    lifecycle = {"after_add": cl.selected().tolist()}
    path = os.path.join(tmp, "selection.cl")
    cl.rename(path)
    lifecycle["after_rename"] = cl.selected().tolist()
    cl.done()
    cl.open(path)
    lifecycle["after_reopen"] = cl.selected().tolist()
    cl.done()
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results, "lifecycle": lifecycle}))
