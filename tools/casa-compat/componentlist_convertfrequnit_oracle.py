#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA component frequency-unit changes and table persistence."""
import base64
import io
import json
import resource
import subprocess
import sys
import tempfile
import tarfile
from pathlib import Path

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


def snapshot(cl):
    return [{"label": cl.getlabel(i), "spectrum": cl.getspectrum(i)} for i in range(cl.length())]


cases = [
    {"which": 0, "unit": "MHz"}, {"which": 1, "unit": "Hz"},
    {"which": 0, "unit": "s-1"}, {"which": 0, "omit_unit": True},
    {"which": 0, "unit": {"bytes": "kHz"}}, {"which": 2**32, "unit": "MHz"},
    {"which": True, "unit": "MHz"}, {"which": -1, "unit": "MHz"},
    {"which": 2, "unit": "MHz"}, {"which": None, "unit": "MHz"},
    {"which": 2**63, "unit": "MHz"}, {"which": 0.0, "unit": "MHz"},
    {"which": "0", "unit": "MHz"}, {"which": 0, "unit": None},
    {"which": 0, "unit": 2}, {"which": 0, "unit": "badunit!"},
    {"which": -1, "unit": "m"}, {"which": 2, "unit": "m"},
    {"which": -1, "unit": "badunit!"},
]
for case in cases:
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz", label="first")
    cl.addcomponent(freq="2.25GHz", label="second")
    unit = case.get("unit")
    if isinstance(unit, dict):
        unit = unit["bytes"].encode()
    args = (case["which"],) if case.get("omit_unit") else (case["which"], unit)
    try:
        case["result"] = cl.convertfrequnit(*args)
    except Exception as error:
        message = str(error)
        if ") Failed AlwaysAssert " in message:
            message = "Failed AlwaysAssert " + message.split(") Failed AlwaysAssert ", 1)[1]
        case["error"] = {"type": type(error).__name__, "message": message}
    case["after"] = snapshot(cl)
    cl.done()

with tempfile.TemporaryDirectory(prefix="casa-convertfrequnit-") as directory:
    path = Path(directory) / "components.cl"
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz", label="first")
    cl.addcomponent(freq="2.25GHz", label="second")
    cl.convertfrequnit(0, "MHz")
    in_memory = snapshot(cl)
    cl.rename(str(path))
    cl.close()
    tb = table()
    tb.open(str(path))
    raw = tb.getcell("Spectral_Record", 0)["frequency"]["m0"]
    raw = {"unit": raw["unit"], "value": float(raw["value"])}
    table_hz = float(tb.getcell("Reference_Frequency", 0))
    error_unit = tb.getcell("Frequency_Error_Units", 0)
    tb.close()
    cl.open(str(path), nomodify=True)
    reopened = snapshot(cl)
    cl.done()
    tb.open(str(path), nomodify=False)
    tb.putcell("Frequency_Error", 0, 1.5)
    tb.putcell("Frequency_Error_Units", 0, "MHz")
    tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="source.cl")
    cl.open(str(path))
    cl.convertfrequnit(0, "kHz")
    changed_with_error = snapshot(cl)
    cl.done()
    tb.open(str(path))
    error_after = {"value": float(tb.getcell("Frequency_Error", 0)),
                   "unit": tb.getcell("Frequency_Error_Units", 0)}
    tb.close()

cl = componentlist()
cl.addcomponent(freq="1.4GHz", label="first")
cl.addcomponent(freq="2.25GHz", label="second")
cl.convertfrequnit(0, "MHz")
cl.remove(0)
removed = snapshot(cl)
cl.recover()
recovered = snapshot(cl)
cl.done()


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


code = ("from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist; "
        "c=componentlist(); c.addcomponent(freq='1.4GHz'); "
        "\ntry: c.convertfrequnit(0,'m')\nexcept Exception as e: print('convert='+type(e).__name__,flush=True)"
        "\ntry: c.getspectrum(0)\nexcept Exception as e: print('read='+type(e).__name__,flush=True)"
        "\nc.done()")
invalid = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, preexec_fn=no_core)
code = ("import json; from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist,quanta; "
        "q=quanta(); q.define('casaFreq','1MHz'); c=componentlist(); c.addcomponent(freq='1.4GHz'); "
        "c.convertfrequnit(0,'casaFreq'); a=c.getspectrum(0)['frequency']['m0']; "
        "q.define('casaFreq','1kHz'); b=c.getspectrum(0)['frequency']['m0']; "
        "q.define('Hz','2Hz'); d=c.getspectrum(0)['frequency']['m0']; "
        "print('DYNAMIC='+json.dumps([a,b,d])); c.done()")
dynamic_process = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
dynamic = json.loads(dynamic_process.stdout.split("DYNAMIC=", 1)[1])
code = ("from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist,quanta; "
        "c=componentlist(); c.addcomponent(freq='1.4GHz'); c.convertfrequnit(0,'MHz'); "
        "q=quanta(); q.define('Hz','1m'); "
        "\ntry: c.getspectrum(0)\nexcept Exception as e: print('read='+type(e).__name__,flush=True)"
        "\nc.done()")
bad_definition = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, preexec_fn=no_core)
print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "cases": cases,
    "table": {"in_memory": in_memory, "raw": raw, "table_hz": table_hz,
              "error_unit": error_unit, "reopened": reopened,
              "changed_with_error": changed_with_error, "error_after": error_after},
    "table_base64": base64.b64encode(archive.getvalue()).decode("ascii"),
    "nonconforming": {"exit": invalid.returncode, "stdout": invalid.stdout},
    "redefined_units": dynamic,
    "nonconforming_hz": {"exit": bad_definition.returncode, "stdout": bad_definition.stdout},
    "recovery": {"removed": removed, "recovered": recovered},
}))
