#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's Undefined frequency-frame behavior and a component table."""
import base64
import io
import json
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
from casatools import componentlist, measures, table, version_string  # noqa: E402

me = measures()
undefined = me.frequency("Undefined", "1.4GHz")
lsrk = me.frequency("LSRK", "1.4GHz")
measure_results = {
    "undefined": undefined,
    "same": me.measure(undefined, "Undefined"),
    "to_lsrk": me.measure(undefined, "LSRK"),
    "from_lsrk": me.measure(lsrk, "Undefined"),
}

with tempfile.TemporaryDirectory(prefix="casa-undefined-frequency-") as directory:
    path = Path(directory) / "source.cl"
    cl = componentlist()
    cl.addcomponent(freq="1.4GHz")
    cl.convertfrequnit(0, "MHz")
    cl.setfreqframe(0, "Undefined")
    spectrum = cl.getspectrum(0)["frequency"]
    cl.rename(str(path))
    cl.done()
    tb = table()
    tb.open(str(path), nomodify=False)
    tb.putcell("Frequency_Error", 0, 1.5)
    tb.putcell("Frequency_Error_Units", 0, "MHz")
    saved = {"hz": float(tb.getcell("Reference_Frequency", 0)),
             "frame": tb.getcell("Frequency_Frame", 0),
             "error": float(tb.getcell("Frequency_Error", 0)),
             "error_unit": tb.getcell("Frequency_Error_Units", 0),
             "raw": tb.getcell("Spectral_Record", 0)["frequency"]}
    tb.close()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="source.cl")
    # The variable-integer reference layout is also valid casacore storage.
    int_path = Path(directory) / "integer.cl"
    shutil.copytree(path, int_path)
    tb.open(str(int_path), nomodify=False)
    desc = tb.getcoldesc("Reference_Frequency")
    desc.update({"valueType": "int", "keywords": {}, "comment": "frequency reference code", "option": 0})
    tb.addcols({"Frequency_Code": desc})
    tb.putcell("Frequency_Code", 0, 64)
    names = ["REST", "LSRK", "LSRD", "BARY", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB", "Undefined"]
    codes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 64]
    tb.putcolkeyword("Reference_Frequency", "MEASINFO", {
        "type": "frequency", "VarRefCol": "Frequency_Code",
        "TabRefTypes": np.array(names), "TabRefCodes": np.array(codes, dtype=np.int32),
    })
    tb.close()
    int_archive = io.BytesIO()
    with tarfile.open(fileobj=int_archive, mode="w:gz") as tar:
        tar.add(int_path, arcname="integer.cl")
    # A table can assign its own integer to an otherwise canonical frame.
    remapped_path = Path(directory) / "remapped.cl"
    shutil.copytree(int_path, remapped_path)
    tb.open(str(remapped_path), nomodify=False)
    tb.putcell("Frequency_Code", 0, 9)
    remapped_codes = list(range(10))
    tb.putcolkeyword("Reference_Frequency", "MEASINFO", {
        "type": "frequency", "VarRefCol": "Frequency_Code",
        "TabRefTypes": np.array(names), "TabRefCodes": np.array(remapped_codes, dtype=np.int32),
    })
    tb.close()
    remapped_archive = io.BytesIO()
    with tarfile.open(fileobj=remapped_archive, mode="w:gz") as tar:
        tar.add(remapped_path, arcname="remapped.cl")
    cl.open(str(remapped_path))
    remapped_spectrum = cl.getspectrum(0)["frequency"]
    cl.done()
    # Older tables may list only some codes. Casacore infers codes for the
    # missing current types in memory, without rewriting these keywords.
    sparse_path = Path(directory) / "sparse.cl"
    shutil.copytree(int_path, sparse_path)
    tb.open(str(sparse_path), nomodify=False)
    tb.putcell("Frequency_Code", 0, 27)
    tb.putcolkeyword("Reference_Frequency", "MEASINFO", {
        "type": "frequency", "VarRefCol": "Frequency_Code",
        "TabRefTypes": np.array(["LSRK"]), "TabRefCodes": np.array([27], dtype=np.int32),
    })
    tb.close()
    cl.open(str(sparse_path))
    cl.setfreqframe(0, "Undefined")
    cl.done()
    tb.open(str(sparse_path))
    sparse_code = int(tb.getcell("Frequency_Code", 0))
    sparse_info = tb.getcolkeyword("Reference_Frequency", "MEASINFO")
    sparse_names = sparse_info["TabRefTypes"].tolist()
    sparse_codes = sparse_info["TabRefCodes"].tolist()
    tb.close()
    sparse_archive = io.BytesIO()
    with tarfile.open(fileobj=sparse_archive, mode="w:gz") as tar:
        tar.add(sparse_path, arcname="sparse.cl")
    cl.open(str(sparse_path), nomodify=True)
    sparse_spectrum = cl.getspectrum(0)["frequency"]
    cl.done()
    alias_path = Path(directory) / "alias.cl"
    shutil.copytree(int_path, alias_path)
    tb.open(str(alias_path), nomodify=False)
    tb.putcell("Frequency_Code", 0, 40)
    tb.putcolkeyword("Reference_Frequency", "MEASINFO", {
        "type": "frequency", "VarRefCol": "Frequency_Code",
        "TabRefTypes": np.array(["BARYCENTRIC"]), "TabRefCodes": np.array([40], dtype=np.int32),
    })
    tb.close()
    alias_archive = io.BytesIO()
    with tarfile.open(fileobj=alias_archive, mode="w:gz") as tar:
        tar.add(alias_path, arcname="alias.cl")
    # A failed CASA open can abort in the destructor. Exit immediately after
    # observing its error, leaving cleanup to this parent process.
    alias_code = ("import os,sys; from casaconfig import config; "
                  "config.data_auto_update=False; config.measures_auto_update=False; "
                  "from casatools import componentlist; c=componentlist(); "
                  "\ntry: c.open(sys.argv[1]); print('open=success',flush=True)"
                  "\nexcept Exception as e: print('open='+type(e).__name__,flush=True)"
                  "\nos._exit(0)")
    alias_result = subprocess.run([sys.executable, "-c", alias_code, str(alias_path)],
                                  capture_output=True, text=True, check=True).stdout
    cl.open(str(int_path))
    int_spectrum = cl.getspectrum(0)["frequency"]
    cl.done()
    cl.open(str(path))
    reopened = cl.getspectrum(0)["frequency"]
    cl.setfreqframe(0, "BARY")
    relabeled = cl.getspectrum(0)["frequency"]
    cl.done()

print("CASA_RESULT=" + json.dumps({
    "casatools": version_string(), "measures": measure_results,
    "table": {"spectrum": spectrum, "saved": saved, "reopened": reopened, "relabeled": relabeled},
    "integer_reference": {"names": names, "codes": codes, "spectrum": int_spectrum},
    "remapped_reference": {"names": names, "codes": remapped_codes, "spectrum": remapped_spectrum},
    "sparse_reference": {"names": sparse_names, "codes": sparse_codes, "row_code": sparse_code, "spectrum": sparse_spectrum},
    "alias_reference": {"name": "BARYCENTRIC", "code": 40, "stdout": alias_result},
    "archive_base64": base64.b64encode(archive.getvalue()).decode(),
    "integer_archive_base64": base64.b64encode(int_archive.getvalue()).decode(),
    "remapped_archive_base64": base64.b64encode(remapped_archive.getvalue()).decode(),
    "sparse_archive_base64": base64.b64encode(sparse_archive.getvalue()).decode(),
    "alias_archive_base64": base64.b64encode(alias_archive.getvalue()).decode(),
}, indent=2))
