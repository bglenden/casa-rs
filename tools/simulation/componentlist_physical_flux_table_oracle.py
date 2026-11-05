#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA physicality of complex Stokes and correlation-basis fluxes."""
import base64
import io
import json
import os
import tarfile
import tempfile
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, table, version_string  # noqa: E402

FLUXES = [
    ("Stokes", [1 + 1e-13j, 0j, 0j, 0j]),
    ("Stokes", [1 + 1.01e-13j, 0j, 0j, 0j]),
    ("Stokes", [-1 + 0j, 0j, 0j, 0j]),
    ("linear", [1 + 0j, 0 + 0.5j, 0 - 0.5j, 1 + 0j]),
    ("circular", [1 + 0j, 2 + 0j, 2 + 0j, 1 + 0j]),
    ("linear", [1 + 1j, 0j, 0j, 1 + 0j]),
]
with tempfile.TemporaryDirectory() as root:
    path = os.path.join(root, "physical.cl")
    cl = componentlist()
    for _ in FLUXES:
        cl.addcomponent(dir="J2000 0deg 0deg", flux=1.0)
    cl.rename(path)
    cl.done()
    tb = table()
    tb.open(path, nomodify=False)
    for index, (polarization, values) in enumerate(FLUXES):
        tb.putcell("Flux", index, np.array(values, dtype=np.complex128))
        tb.putcell("Flux_Polarisation", index, polarization)
    tb.done()
    cl.open(path)
    results = [cl.isphysical([index]) for index in range(len(FLUXES))]
    cl.done()
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        archive.add(path, arcname="physical.cl")
    print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "results": results, "archive": base64.b64encode(data.getvalue()).decode("ascii")}))
