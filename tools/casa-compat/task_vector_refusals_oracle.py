#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14's direct vector type refusals (#1099)."""

import json
import tempfile

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

import casatasks  # noqa: E402
import casatools  # noqa: E402
from casatasks import applycal, flagdata, fluxscale, immoments, imregrid  # noqa: E402


TASKS = {"applycal": applycal, "flagdata": flagdata, "fluxscale": fluxscale,
         "immoments": immoments, "imregrid": imregrid}
CASES = [
    ("applycal", {"vis": "$EXISTING", "gainfield": 42}),
    ("applycal", {"vis": "$EXISTING", "gaintable": 42}),
    ("applycal", {"vis": "$EXISTING", "calwt": 42}),
    ("flagdata", {"vis": "$EXISTING", "clipminmax": ["bad"]}),
    ("immoments", {"imagename": "$EXISTING", "moments": ["bad"]}),
    ("imregrid", {"imagename": "$EXISTING", "axes": ["bad"]}),
    ("fluxscale", {"vis": "$EXISTING", "refspwmap": ["bad"]}),
]

results = []
with tempfile.TemporaryDirectory(prefix="casa-vector-refusals-") as directory:
    for task, args in CASES:
        actual = {name: directory if value == "$EXISTING" else value for name, value in args.items()}
        try:
            TASKS[task](**actual)
        except Exception as error:
            results.append({"task": task, "args": args, "type": type(error).__name__, "message": str(error)})
        else:
            raise AssertionError(f"CASA accepted {task}({args!r})")
print("CASA_RESULT=" + json.dumps({
    "casatasks": casatasks.version_string(),
    "casatools": casatools.version_string(),
    "cases": results,
}))
