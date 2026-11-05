#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA behavior for bare complex scalars passed to quanta."""
import json

import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

qa = quanta()
cases = []
for kind, scalar in (("python", complex), ("complex64", np.complex64), ("complex128", np.complex128)):
    for method in ("quantity", "getunit", "add"):
        case = {"kind": kind, "method": method}
        args = (scalar(1 + 2j),) + (("1m",) if method == "add" else ())
        try:
            case["result"] = getattr(qa, method)(*args)
        except Exception as error:
            case["error"] = {"type": type(error).__name__, "message": str(error)}
        cases.append(case)
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "cases": cases}))
