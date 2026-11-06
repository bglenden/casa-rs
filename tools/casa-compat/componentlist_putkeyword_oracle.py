#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6-14 component-list keyword writes and a fixture."""
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


def normalize(value):
    if isinstance(value, np.ndarray):
        return {"array": value.tolist(), "dtype": value.dtype.kind, "shape": list(value.shape)}
    if isinstance(value, dict):
        return {key: normalize(value[key]) for key in sorted(value)}
    if isinstance(value, np.generic):
        return value.item()
    return value


def call(fn, *args):
    try:
        return {"value": normalize(fn(*args))}
    except BaseException as error:
        return {"error": type(error).__name__, "message": str(error).splitlines()[0]}


with tempfile.TemporaryDirectory(prefix="casa-cl-putkeyword-") as directory:
    path = Path(directory) / "keywords.cl"
    cl = componentlist()
    results = {"unattached": call(cl.putkeyword, "x", 3)}
    cl.addcomponent(freq="1.4GHz", label="unchanged")
    cl.rename(str(path))
    cl.putkeyword("initial", {"stable": 17})
    cl.done()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="keywords.cl", filter=lambda info: None if info.name.endswith("/table.lock") else info)
    cl.open(str(path))
    steps = [
        ("default", ()),
        ("integer", ("count", 42)), ("integer_update", ("count", 43)),
        ("mismatch", ("count", 1.5)),
        ("record", ("record", {"x": 1, "nested": {"n": 2}})),
        ("child_update", ("record.x", 9)), ("child_add", ("record.new", "yes")),
        ("missing_parent", ("missing.x", 5)), ("scalar_parent", ("record.x.y", 5)),
        ("invalid_name", (".bad", 1)), ("empty_1", ("", 7)), ("empty_2", ("", 8)),
        ("bool", ("flag", True)),
        ("matrix", ("matrix", np.array([[1, 2], [3, 4]], dtype=np.int32))),
        ("typed_empty", ("typed_empty", np.array([], dtype=np.int32))),
        ("nested_empty", ("nested_empty", {"none": None, "empty": [], "tuple": ()})),
        ("none", ("none", None)), ("wrong_name", (None, 3)),
    ]
    for name, args in steps:
        results[name] = call(cl.putkeyword, *args)
    results["record_after"] = call(cl.getkeyword, "record")
    results["count_after"] = call(cl.getkeyword, "count")
    cl.done()
    cl.open(str(path), nomodify=True)
    results["readonly_put"] = call(cl.putkeyword, "readonly", {"value": 19})
    results["readonly_get"] = call(cl.getkeyword, "readonly")
    cl.done()
    tb = table()
    tb.open(str(path))
    results["persisted"] = normalize(tb.getkeywords())
    results["component_label"] = tb.getcell("Label", 0)
    tb.close()
    print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "results": results}, sort_keys=True))
    print("CASA_ARCHIVE=" + base64.b64encode(archive.getvalue()).decode())
