#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component direction-frame relabeling and a table fixture."""

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


FRAMES = ["J2000", "j20", "J2000suffix", "JMEAN", "JTRUE", "APP", "JNAT",
          "B1950", "B1950_VLA", "BMEAN", "BTRUE", "GALACTIC", "G", "SUPERGAL",
          "ICRS", "ECLIPTIC", "MECLIPTIC", "TECLIPTIC", "HADEC", "AZEL",
          "AZELNE", "AZELSW", "AZELGEO", "AZELNEGEO", "AZELSWGEO", "ITRF",
          "TOPO", "MARS", "SUN"]
CALLS = [("populated", [0, frame], {}) for frame in FRAMES] + [
    ("populated", [1, "ICRS"], {}), ("populated", [-1, "J2000"], {}),
    ("populated", [2, "J2000"], {}), ("populated", [2**32, "GALACTIC"], {}),
    ("populated", [None, "J2000"], {}), ("populated", [2**100, "J2000"], {}),
    ("populated", [1.5, "J2000"], {}), ("populated", [0, None], {}),
    ("populated", [0, "ICRS"], {"log": False}),
    ("populated", [0, "ICRS"], {"log": None}),
    ("populated", [0, "ICRS"], {"log": 1}),
    ("populated", [True, "J2000"], {}),
    ("populated", [0, {"bytes": [71]}], {}),
    ("populated", [0, {"bytes": [71, 65, 76, 65, 67, 84, 73, 67, 255]}], {}),
    ("populated", [0, {"bytes": [74, 50, 48, 48, 48, 0, 255]}], {}),
    ("populated", [0, "GALACTIC\u0000junk"], {}),
    ("empty", [0, "J2000"], {}), ("empty", [-1, "J2000"], {}),
]
results = []
for state, args, kwargs in CALLS:
    cl = componentlist()
    if state == "populated":
        components(cl)
    item = {"state": state, "args": args, "kwargs": kwargs, "before": snapshot(cl)}
    try:
        converted = [bytes(arg["bytes"]) if isinstance(arg, dict) and "bytes" in arg else arg for arg in args]
        item["result"] = cl.setrefdirframe(*converted, **kwargs)
    except Exception as error:
        message = str(error)
        if "Failed AlwaysAssert" in message:
            message = "Failed AlwaysAssert" + message.split("Failed AlwaysAssert", 1)[1]
        item["error"] = {"type": type(error).__name__, "message": message}
    item["after"] = snapshot(cl)
    results.append(item)
    cl.done()

with tempfile.TemporaryDirectory() as directory:
    path = str(Path(directory) / "frames.cl")
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
        archive.add(path, arcname="frames.cl")
    fixture = base64.b64encode(buffer.getvalue()).decode("ascii")
    cl.open(path, nomodify=False)
    cl.setrefdirframe(0, "GALACTIC")
    cl.done()
    cl.open(path, nomodify=True)
    persisted = snapshot(cl)
    cl.setrefdirframe(0, "ICRS")
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
