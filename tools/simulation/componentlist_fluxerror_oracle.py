#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA flux-error getters and a portable component-list table."""
import base64
import io
import json
import tarfile
import tempfile
from pathlib import Path
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

with tempfile.TemporaryDirectory(prefix="casa-fluxerror-") as tmp:
    path = Path(tmp) / "fluxerrors.cl"
    cl = componentlist()
    cl.addcomponent(flux=1, dir="J2000 10deg 20deg")
    cl.addcomponent(flux=2, dir="GALACTIC 30deg 40deg")
    cl.setflux(0, value=[1., 2., 3., 4.], error=[0.1, 0.2, 0.3, 0.4])
    cl.setflux(1, value=[5., 6., 7., 8.], error=[1+2j, 3+4j, 5+6j, 7+8j])
    cl.rename(str(path))
    cl.done()
    cl.open(str(path), nomodify=True)
    cases = []
    for which in (0, 1, -1, 2, None, True, 1.5, "0", 2**32, 2**32+1, 2**40, 2**100):
        item = {"which": which}
        try:
            item["result"] = cl.getfluxerror(which).tolist()
        except Exception as error:
            item["error"] = {"type": type(error).__name__, "message": str(error).split('\n... thrown by', 1)[0]}
        cases.append(item)
    cl.done()
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        tar.add(path, arcname="fluxerrors.cl")
    print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases,
          "table_base64": base64.b64encode(archive.getvalue()).decode("ascii")}))
