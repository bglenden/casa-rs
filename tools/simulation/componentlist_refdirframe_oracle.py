#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record componentlist.getrefdirframe results and index errors in CASA."""
import json
from casaconfig import config
config.data_auto_update = False
config.measures_auto_update = False
from casatools import componentlist, version_string  # noqa: E402

CASES = [
    ([], 0), ([], -1), ([], None),
    (["J2000 10h00m00 -30d00m00"], 0),
    (["GALACTIC 10deg 20deg"], 0),
    (["J2000 10h00m00 -30d00m00", "GALACTIC 10deg 20deg"], 1),
    (["J2000 10h00m00 -30d00m00"], -1),
    (["J2000 10h00m00 -30d00m00"], None),
    (["J2000 10h00m00 -30d00m00"], True),
    (["J2000 10h00m00 -30d00m00"], 1.5),
    (["J2000 10h00m00 -30d00m00"], "0"),
    (["J2000 10h00m00 -30d00m00"], 2**32),
    (["J2000 10h00m00 -30d00m00"], 2**40),
    (["J2000 10h00m00 -30d00m00"], 2**100),
]
results = []
for directions, which in CASES:
    c = componentlist()
    for direction in directions:
        c.addcomponent(flux=1, dir=direction, freq='1GHz')
    item = {"directions": directions, "which": which}
    try:
        item["result"] = c.getrefdirframe(which)
    except Exception as error:
        # CASA's trailing source-file path varies by installation.
        item["error"] = {"type": type(error).__name__, "message": str(error).split('\n... thrown by', 1)[0]}
    results.append(item)
    c.done()
print('CASA_RESULT=' + json.dumps({'casatools': version_string(), 'cases': results}))
