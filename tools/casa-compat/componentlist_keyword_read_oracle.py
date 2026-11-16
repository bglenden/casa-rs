#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component-list keyword reads and an on-disk fixture."""
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
from casatools import componentlist, version_string  # noqa: E402


def normalize(value):
    if isinstance(value, np.ndarray):
        return {"array": value.tolist(), "dtype": value.dtype.kind, "shape": list(value.shape)}
    if isinstance(value, dict):
        return {key: normalize(value[key]) for key in sorted(value)}
    if isinstance(value, np.generic):
        return value.item()
    return value


def record(method, *args):
    try:
        return {"value": normalize(method(*args))}
    except BaseException as error:
        return {"error": type(error).__name__, "message": str(error).splitlines()[0]}


with tempfile.TemporaryDirectory(prefix="casa-cl-keyword-reads-") as directory:
    path = Path(directory) / "keywords.cl"
    cl = componentlist()
    result = {"unattached_get": record(cl.getkeyword), "unattached_has": record(cl.haskeyword)}
    cl.addcomponent(freq="1.4GHz")
    cl.rename(str(path))
    cl.putkeyword("metadata", {"answer": 42, "nested": {"x": "yes"},
                               "ints": np.array([1, 2], dtype=np.int32),
                               "flags": np.array([True, False])})
    cl.putkeyword("matrix", np.array([[1, 2], [3, 4]], dtype=np.int32))
    cl.putkeyword("scalar", 5)
    for name in ("metadata", "metadata.answer", "metadata.nested.x", "metadata.absent",
                 "missing.x", "metadata.answer.x", ".x", "metadata.", "", "Metadata", "scalar", "matrix"):
        result["has:" + name] = record(cl.haskeyword, name)
        result["get:" + name] = record(cl.getkeyword, name)
    result["has_default"] = record(cl.haskeyword)
    result["get_default"] = record(cl.getkeyword)
    result["has_none"] = record(cl.haskeyword, None)
    result["get_none"] = record(cl.getkeyword, None)
    result["get_bytes"] = record(cl.getkeyword, b"scalar")
    cl.done()
    cl.open(str(path), nomodify=True)
    result["readonly"] = record(cl.getkeyword, "metadata")
    cl.done()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="keywords.cl", filter=lambda info: None if info.name.endswith("/table.lock") else info)
    print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "results": result}, sort_keys=True))
    print("CASA_ARCHIVE=" + base64.b64encode(archive.getvalue()).decode())
