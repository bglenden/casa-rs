#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA componentlist.setfreqframe behavior and table persistence."""
import base64
import io
import json
import tarfile
import tempfile
from pathlib import Path

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


frames = ["REST", "LSRK", "LSRD", "BARY", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB",
          "BARYCENTRIC", "GEOCENTRIC", "TOPOCENTRIC", "GALACTOCENTRIC", "lsrk", "r",
          "baryc", "ge", "gal", "t", "lg", "c", "LSRKsuffix", "TOPO-FRAME", "topo ",
          "l", "lsr", "g", "GALAXY", "", " LSRK ", "SOURCE", "Undefined", "u"]
cases = [{"which": 0, "frame": frame} for frame in frames]
cases += [
    {"which": 0, "omit_frame": True}, {"which": 1, "frame": "TOPO"},
    {"which": 2**32, "frame": "BARY"}, {"which": -1, "frame": "LSRK"},
    {"which": 2, "frame": "LSRK"}, {"which": None, "frame": "LSRK"},
    {"which": 2**63, "frame": "LSRK"}, {"which": 0.0, "frame": "LSRK"},
    {"which": "0", "frame": "LSRK"}, {"which": -1, "frame": "INVALID"},
    {"which": 2, "frame": "INVALID"}, {"which": None, "frame": "INVALID"},
    {"which": 2**63, "frame": "INVALID"}, {"which": -1, "frame": "u"},
    {"which": 2, "frame": "u"}, {"which": 0, "frame": "undefxxx"},
    {"which": 0, "frame": None},
    {"which": 0, "frame": 2}, {"which": 0, "frame": {"bytes": "BARY"}},
    {"which": 0, "frame": "LSRK\u0000TOPO"},
    {"which": 0, "frame": "BARY", "log": False},
    {"which": 0, "frame": "BARY", "log": None},
    {"which": 0, "frame": "BARY", "log": 1},
]
for case in cases:
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz", label="first")
    cl.addcomponent(freq="2.25GHz", label="second")
    cl.convertfrequnit(0, "MHz")
    frame = case.get("frame")
    if isinstance(frame, dict):
        frame = frame["bytes"].encode()
    args = [case["which"]]
    if not case.get("omit_frame"):
        args.append(frame)
    if "log" in case:
        args.append(case["log"])
    try:
        case["result"] = cl.setfreqframe(*args)
    except Exception as error:
        message = str(error)
        if ") Failed AlwaysAssert " in message:
            message = "Failed AlwaysAssert " + message.split(") Failed AlwaysAssert ", 1)[1]
        case["error"] = {"type": type(error).__name__, "message": message}
    case["after"] = [cl.getspectrum(i)["frequency"] for i in range(2)]
    cl.done()

with tempfile.TemporaryDirectory(prefix="casa-setfreqframe-") as directory:
    path = Path(directory) / "source.cl"
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz")
    cl.convertfrequnit(0, "MHz")
    cl.rename(str(path))
    cl.done()
    tb = table()
    tb.open(str(path), nomodify=False)
    tb.putcell("Frequency_Error", 0, 1.5)
    tb.putcell("Frequency_Error_Units", 0, "MHz")
    tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="source.cl")
    cl.open(str(path))
    cl.convertfrequnit(0, "MHz")
    cl.setfreqframe(0, "BARY")
    spectrum = cl.getspectrum(0)["frequency"]
    cl.done()
    tb.open(str(path))
    saved = {"hz": float(tb.getcell("Reference_Frequency", 0)),
             "frame": tb.getcell("Frequency_Frame", 0),
             "error": float(tb.getcell("Frequency_Error", 0)),
             "error_unit": tb.getcell("Frequency_Error_Units", 0),
             "raw": tb.getcell("Spectral_Record", 0)["frequency"]}
    tb.close()
    cl.open(str(path))
    reopened = cl.getspectrum(0)["frequency"]
    cl.done()

print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "cases": cases,
    "table": {"spectrum": spectrum, "saved": saved, "reopened": reopened},
    "archive_base64": base64.b64encode(archive.getvalue()).decode(),
}, indent=2))
