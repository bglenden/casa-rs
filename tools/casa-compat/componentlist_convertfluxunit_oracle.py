#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA flux-unit conversion, including complex error values."""
import base64
import io
import json
import resource
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402


def raw(path):
    tb = table()
    tb.open(str(path))
    rows = []
    for i in range(tb.nrows()):
        row = {"unit": tb.getcell("Flux_Unit", i)}
        for name in ("Flux", "Flux_Error"):
            row[name] = [[float(v.real), float(v.imag)] for v in tb.getcell(name, i)]
        rows.append(row)
    tb.close()
    return rows


cases = [
    {"which": 0, "unit": "mJy"}, {"which": 1, "unit": "Jy"},
    {"which": -1, "unit": "uJy"}, {"which": 0, "omit_unit": True},
    {"which": 0, "unit": "Jy*Hz/Hz"}, {"which": 0, "unit": {"bytes": "mJy"}},
    {"which": 2**32, "unit": "mJy"}, {"which": True, "unit": "mJy"},
    {"which": 2, "unit": "mJy"}, {"which": -2, "unit": "mJy"},
    {"which": 2, "unit": "m"}, {"which": 0, "unit": "m"},
    {"which": 0, "unit": "badunit!"}, {"which": None, "unit": "mJy"},
    {"which": 2**63, "unit": "mJy"}, {"which": 0.0, "unit": "mJy"},
    {"which": "0", "unit": "mJy"}, {"which": 0, "unit": None},
    {"which": 0, "unit": 2},
]
with tempfile.TemporaryDirectory(prefix="casa-convertfluxunit-") as directory:
    root = Path(directory)
    source = root / "source.cl"
    cl = componentlist()
    cl.addcomponent(flux=1, label="first")
    cl.addcomponent(flux=2, label="second")
    cl.setflux(0, value=[1., 2., 3., 4.], unit="Jy", error=[1+2j, 3+4j, 5+6j, 7+8j])
    cl.setflux(1, value=[5., 6., 7., 8.], unit="mJy", error=[0.1+0.2j, 0.3+0.4j, 0.5+0.6j, 0.7+0.8j])
    cl.rename(str(source))
    cl.done()
    # setflux discards imaginary error components; write a valid CASA table
    # directly to verify that conversions preserve both parts of every value.
    tb = table()
    tb.open(str(source), nomodify=False)
    tb.putcell("Flux_Error", 0, np.array([1+2j, 3+4j, 5+6j, 7+8j]))
    tb.putcell("Flux", 1, np.array([5+1j, 6+2j, 7+3j, 8+4j]))
    tb.putcell("Flux_Error", 1, np.array([0.1+0.2j, 0.3+0.4j, 0.5+0.6j, 0.7+0.8j]))
    tb.putcell("Flux_Polarisation", 1, "Linear")
    tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(source, arcname="source.cl")
    before = raw(source)
    for n, case in enumerate(cases):
        path = root / f"case-{n}.cl"
        shutil.copytree(source, path)
        cl = componentlist()
        cl.open(str(path))
        unit = case.get("unit")
        if isinstance(unit, dict):
            unit = unit["bytes"].encode()
        args = (case["which"],) if case.get("omit_unit") else (case["which"], unit)
        try:
            case["result"] = cl.convertfluxunit(*args)
        except Exception as error:
            message = str(error)
            if "Index out of range." in message:
                message = "Index out of range.\nA component number is less than one or greater than the list length\nFlux not changed on any components"
            case["error"] = {"type": type(error).__name__, "message": message}
        cl.done()
        case["after"] = raw(path)
    path = root / "nonconforming.cl"
    shutil.copytree(source, path)
    tb = table()
    tb.open(str(path), nomodify=False)
    tb.putcell("Flux_Unit", 1, "Jy.km/s")
    tb.close()
    dangerous_before = raw(path)
    dangerous_archive = io.BytesIO()
    with tarfile.open(fileobj=dangerous_archive, mode="w:gz") as tar:
        tar.add(path, arcname="source.cl")
    cl = componentlist()
    cl.open(str(path))
    dangerous_result = cl.convertfluxunit(-1, "mJy")
    cl.done()
    dangerous_after = raw(path)


def no_core():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


code = ("from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist; "
        "componentlist().convertfluxunit(-1, 'mJy')")
empty_exit = subprocess.run([sys.executable, "-c", code], capture_output=True, preexec_fn=no_core).returncode
code = ("import json; from casaconfig import config; config.data_auto_update=False; "
        "config.measures_auto_update=False; from casatools import componentlist,quanta; "
        "c=componentlist(); c.addcomponent(flux=2); q=quanta(); q.define('Jy','1m'); "
        "r=c.convertfluxunit(0,'Jy'); "
        "print('OVERRIDE='+json.dumps({'result':r,'unit':c.getfluxunit(0),'values':c.getfluxvalue(0).tolist()}))")
override_process = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
override = json.loads(override_process.stdout.split("OVERRIDE=", 1)[1])
print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "cases": cases, "before": before,
    "table_base64": base64.b64encode(archive.getvalue()).decode("ascii"),
    "nonconforming_table_base64": base64.b64encode(dangerous_archive.getvalue()).decode("ascii"),
    "nonconforming": {"before": dangerous_before, "result": dangerous_result, "after": dangerous_after},
    "empty_exit": empty_exit,
    "same_name_override": override,
}))
