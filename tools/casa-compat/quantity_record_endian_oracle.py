#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's byte-order handling for integer quantity arrays."""
import json
import sys
import numpy as np
from casaconfig import config

config.data_auto_update = False
config.measures_auto_update = False
from casatools import quanta, version_string  # noqa: E402

q = quanta()
cases = []
for dtype in (">i2", ">i4", ">i8", ">u2", ">u4", ">u8", "<i4"):
    value = np.array([1], dtype=dtype)
    cases.append({"dtype": dtype, "quantity": q.quantity({"value": value, "unit": "m"}),
                  "add": q.add({"value": value, "unit": "m"}, "1m")})
print("CASA_RESULT=" + json.dumps({"casatools": version_string(), "byteorder": sys.byteorder, "cases": cases}))
