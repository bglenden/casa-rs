# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA side of the trial's ephemeris check (docs/measurement-set/trial/README.md,
"Ephemerides in MSv4"). Run with a CASA (casatools) Python; CASA is only the
oracle.

    check_ephemeris_casa.py ORIGINAL.ms ROUND_TRIP.ms

compares casacore's direction of field 0 (`msmetadata.phasecenter`, which is
`MSFieldColumns::phaseDirMeas` through `MeasComet`) on the moving-field MSv2
and on its MSv2 -> MSv4 -> MSv2 round trip, at every time of the original and
69.184 s and 110 s past the last, and prints the round trip's table keywords
and the differences as JSON.
"""

import json
import sys

import numpy as np
from casatools import measures, msmetadata, table


def main():
    original, back = sys.argv[1], sys.argv[2]
    me = measures()
    tb = table()
    tb.open(back + "/FIELD/EPHEM0_trial_0.tab")
    keywords, rows = tb.getkeywords(), tb.nrows()
    tb.close()
    tb.open(original)
    times = np.unique(tb.getcol("TIME"))
    tb.close()
    differences = []
    for time in list(times) + [times[-1] + 69.184, times[-1] + 110.0]:
        epoch = me.epoch("utc", f"{time}s")
        directions = []
        for ms in (original, back):
            md = msmetadata()
            md.open(ms)
            centre = md.phasecenter(0, epoch)
            md.close()
            directions.append((centre["m0"]["value"], centre["m1"]["value"], centre["refer"]))
        (a0, a1, a_frame), (b0, b1, b_frame) = directions
        difference = max(abs((a0 - b0 + np.pi) % (2 * np.pi) - np.pi), abs(a1 - b1))
        differences.append({"time_s": float(time), "diff_rad": float(difference), "refer": [a_frame, b_frame]})
    print(json.dumps({
        "keywords": {key: value if isinstance(value, (int, float, str)) else str(value) for key, value in keywords.items()},
        "rows": int(rows),
        "phasecenter": differences,
        "max_diff_rad": max(entry["diff_rad"] for entry in differences),
    }, indent=1))


if __name__ == "__main__":
    main()
