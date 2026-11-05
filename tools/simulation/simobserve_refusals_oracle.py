#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14's simobserve refusal messages on stdout (#1099)."""

import json
import os
import tempfile

from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False

from casatasks import simobserve  # noqa: E402


cases = [
    {"comp_nchan": -1.0},
    {"sdant": "badname"},
    {"seed": "bad"},
    {"project": ""},
]
results = []
original_directory = os.getcwd()
with tempfile.TemporaryDirectory(prefix="simobserve-refusals-") as directory:
    os.chdir(directory)
    try:
        for args in cases:
            try:
                simobserve(**args)
            except Exception as error:
                results.append({"args": args, "type": type(error).__name__, "message": str(error)})
            else:
                raise AssertionError(f"CASA accepted {args!r}")
    finally:
        os.chdir(original_directory)
print("CASA_RESULT=" + json.dumps(results))
