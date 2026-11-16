#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's unambiguous required-path refusals."""

import json
import os
import tempfile
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

import casatasks  # noqa: E402
import casatools  # noqa: E402


CASES = [
    ("imstat", {"imagename": "$MISSING"}),
    ("imstat", {"imagename": "./missing"}),
    ("imstat", {"imagename": "../casa-path-probe-missing"}),
    ("imhead", {"imagename": "$MISSING"}),
    ("immoments", {"imagename": "$QUOTE", "outfile": "$OUTPUT"}),
    ("imsubimage", {"imagename": "$MISSING", "outfile": "$OUTPUT"}),
    ("split", {"vis": "$MISSING", "outputvis": "$OUTPUT"}),
    ("exportfits", {"imagename": "$MISSING", "fitsimage": "$OUTPUT"}),
]
results = []
with tempfile.TemporaryDirectory(prefix="casa-required-paths-") as directory:
    names = {"$MISSING": directory + "/missing", "$QUOTE": directory + "/a'quote", "$OUTPUT": directory + "/output"}
    previous = os.getcwd()
    try:
        os.chdir(directory)
        for task, args in CASES:
            actual = {key: names.get(value, value) for key, value in args.items()}
            try:
                getattr(casatasks, task)(**actual)
            except Exception as error:
                results.append({"task": task, "args": args, "type": type(error).__name__,
                                "message": str(error).replace(directory, "$TMP")})
            else:
                raise AssertionError(f"CASA accepted {task}({args!r})")
    finally:
        os.chdir(previous)
print("CASA_RESULT=" + json.dumps({"casatasks": casatasks.version_string(), "casatools": casatools.version_string(), "cases": results}))
