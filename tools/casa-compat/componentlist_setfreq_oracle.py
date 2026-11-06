#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA componentlist.setfreq behavior; run with CASA's Python."""
import base64
import io
import json
import resource
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, quanta, table, version_string  # noqa: E402


cases = [
    {"which": 0, "value": 1.5, "omit_unit": True},
    {"which": 0, "value": 1400, "unit": "MHz"},
    {"which": 1, "value": 125000, "unit": "kHz"},
    {"which": 0, "value": -2, "unit": "GHz"},
    {"which": 0, "value": 0, "unit": "Hz"},
    {"which": 0, "value": True, "unit": "MHz"},
    {"which": 0, "value": 1, "unit": {"bytes": "MHz"}},
    {"which": 2**32, "value": 1, "unit": "MHz"},
    {"which": -1, "value": 1, "unit": "GHz"},
    {"which": 2, "value": 1, "unit": "GHz"},
    {"which": None, "value": 1, "unit": "GHz"},
    {"which": 2**63, "value": 1, "unit": "GHz"},
    {"which": 0.0, "value": 1, "unit": "GHz"},
    {"which": "0", "value": 1, "unit": "GHz"},
    {"which": 0, "value": None, "unit": "GHz"},
    {"which": 0, "value": "1", "unit": "GHz"},
    {"which": 0, "value": 1, "unit": None},
    {"which": 0, "value": 1, "unit": 2},
    {"which": 0, "value": 1, "unit": "badunit!"},
    {"which": -1, "value": None, "unit": "badunit!"},
    {"which": -1, "value": None, "unit": "GHz"},
    {"which": 0, "value": 1, "unit": "MHz", "log": False},
    {"which": 0, "value": 1, "unit": "MHz", "log": None},
    {"which": 0, "value": 1, "unit": "MHz", "log": 1},
]
for case in cases:
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz", label="first")
    cl.addcomponent(freq="2.25GHz", label="second")
    unit = case.get("unit")
    if isinstance(unit, dict):
        unit = unit["bytes"].encode()
    args = [case["which"], case["value"]]
    if not case.get("omit_unit"):
        args.append(unit)
    if "log" in case:
        args.append(case["log"])
    try:
        case["result"] = cl.setfreq(*args)
    except Exception as error:
        message = str(error)
        if ") Failed AlwaysAssert " in message:
            message = "Failed AlwaysAssert " + message.split(") Failed AlwaysAssert ", 1)[1]
        case["error"] = {"type": type(error).__name__, "message": message}
    case["after"] = [cl.getspectrum(i)["frequency"] for i in range(2)]
    cl.done()

with tempfile.TemporaryDirectory(prefix="casa-setfreq-") as directory:
    path = Path(directory) / "source.cl"
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz")
    cl.setfreqframe(0, "BARY")
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
    cl.setfreq(0, 1500, "MHz")
    spectrum = cl.getspectrum(0)["frequency"]
    cl.done()
    tb.open(str(path))
    saved = {"hz": float(tb.getcell("Reference_Frequency", 0)),
             "error": float(tb.getcell("Frequency_Error", 0)),
             "error_unit": tb.getcell("Frequency_Error_Units", 0),
             "raw": tb.getcell("Spectral_Record", 0)["frequency"]}
    tb.close()
    cl.open(str(path))
    reopened = cl.getspectrum(0)["frequency"]
    cl.done()

q = quanta()
cl = componentlist()
cl.addcomponent(freq="1.4GHz")
q.define("Hz", "2s-1")
cl.setfreq(0, 5, "MHz")
redefined_hz = cl.getspectrum(0)["frequency"]
cl.done()

code = ("from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist; "
        "c=componentlist(); c.addcomponent(freq='1.4GHz'); "
        "\ntry: c.setfreq(0,1,'m')\nexcept Exception as e: print('set='+type(e).__name__,flush=True)"
        "\ntry: c.getspectrum(0)\nexcept Exception as e: print('read='+type(e).__name__,flush=True)"
        "\nc.done()")


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


invalid = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, preexec_fn=no_core)
print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "cases": cases, "redefined_hz": redefined_hz,
    "nonconforming": {"exit": invalid.returncode, "stdout": invalid.stdout},
    "table": {"spectrum": spectrum, "saved": saved, "reopened": reopened},
    "archive_base64": base64.b64encode(archive.getvalue()).decode(),
}, indent=2))
