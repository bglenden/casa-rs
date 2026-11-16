#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component reference-direction setters and a table fixture."""

import base64
import io
import json
import tarfile
import tempfile
from pathlib import Path

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


def components(cl):
    cl.addcomponent(flux=1, dir="J2000 10deg 20deg", label="first", shape="Gaussian",
                    majoraxis="2arcmin", minoraxis="1arcmin", positionangle="20deg")
    cl.addcomponent(flux=2, dir="GALACTIC 30deg -15deg", label="second")


def snapshot(cl):
    return [cl.getrefdir(i) for i in range(cl.length())]


CALLS = [
    ("populated", [0, "20deg", "30deg"], {}),
    ("populated", [1, "1rad", "0.5rad"], {}),
    ("populated", [-1, "1rad", "0.5rad"], {}),
    ("populated", [0, "361deg", "100deg"], {}),
    ("populated", [0, "-20deg", "-100deg"], {}),
    ("populated", [0, "60arcmin", "30arcmin"], {}),
    ("populated", [0, {"value": 2, "unit": "deg"}, {"value": 3, "unit": "deg"}], {}),
    ("populated", [], {"ra": "1deg", "dec": "2deg"}),
    ("populated", [2**32, "1rad", "0.5rad"], {}),
    ("populated", [True, "1rad", "0.5rad"], {}),
    ("populated", [None, "1rad", "0.5rad"], {}),
    ("populated", [2**100, "1rad", "0.5rad"], {}),
    ("populated", [2, "1rad", "0.5rad"], {}),
    ("populated", [2, "bad", "1deg"], {}),
    ("populated", [None, "bad", "1deg"], {}),
    ("populated", [2, "1deg", "2s"], {}),
    ("populated", [-2, "1rad", "0.5rad"], {}),
    ("populated", [1.5, "1rad", "0.5rad"], {}),
    ("populated", [0, 1, 2], {}), ("populated", [0, "1", "2"], {}),
    ("populated", [0, "1Jy", "2Jy"], {}),
    ("populated", [0, "2Jy", "3s"], {}),
    ("populated", [0, "2deg", "3s"], {}),
    ("populated", [0, "2s", "3deg"], {}),
    ("populated", [0, "bad", "3s"], {}),
    ("populated", [0, "2s", "bad"], {}),
    ("populated", [0, None, None], {}), ("populated", [0, [], []], {}),
    ("populated", [0, "12h", "30deg"], {}),
    ("populated", [0, "1deg", "2deg"], {"log": False}),
    ("populated", [0, "1deg", "2deg"], {"log": None}),
    ("populated", [0, "1deg", "2deg"], {"log": 1}),
    ("empty", [0, "1rad", "0.5rad"], {}),
    ("closed", [0, "1rad", "0.5rad"], {}),
]
results = []
for state, args, kwargs in CALLS:
    cl = componentlist()
    if state != "empty":
        components(cl)
    if state == "closed":
        cl.done()
    item = {"state": state, "args": args, "kwargs": kwargs, "before": snapshot(cl)}
    try:
        item["result"] = cl.setrefdir(*args, **kwargs)
    except Exception as error:
        message = str(error)
        if "Index out of range." in message:
            message = "Index out of range."
        item["error"] = {"type": type(error).__name__, "message": message}
    item["after"] = snapshot(cl)
    results.append(item)
    cl.done()

with tempfile.TemporaryDirectory() as directory:
    path = str(Path(directory) / "directions.cl")
    cl = componentlist()
    components(cl)
    cl.rename(path)
    cl.done()
    tb = table()
    tb.open(path, nomodify=False)
    tb.putkeyword("stable", 17)
    tb.putcell("Direction_Error", 0, np.array([0.001, 0.002]))
    tb.close()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        archive.add(path, arcname="directions.cl")
    fixture = base64.b64encode(buffer.getvalue()).decode("ascii")
    cl.open(path, nomodify=False)
    cl.setrefdir(0, "1rad", "0.5rad")
    cl.done()
    cl.open(path, nomodify=True)
    persisted = snapshot(cl)
    cl.setrefdir(0, "2rad", "-0.5rad")
    read_only_memory = snapshot(cl)
    cl.done()
    cl.open(path, nomodify=True)
    read_only_disk = snapshot(cl)
    cl.done()
    tb.open(path)
    direction_error = tb.getcell("Direction_Error", 0).tolist()
    tb.close()

print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": results,
      "persisted": persisted, "read_only_memory": read_only_memory, "read_only_disk": read_only_disk,
      "direction_error": direction_error}))
print("CASA_ARCHIVE=" + fixture)
