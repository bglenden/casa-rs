#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA sidereal angles at the simulation UVW regression epochs (#1053)."""

import argparse
import json
import math
import pathlib

import casatasks
from casatools import measures, quanta

POSITION_ITRF_M = [2225142.18, -5440307.37, -2481029.85]
# Equal stored TIME in the already paired simobserve MSs. These dates span
# both non-transit hour-angle cases and controls from the 17-case campaign.
CASES = [
    ("schedule_hour_number", 4990000547.719971),
    ("schedule_hour_quantity", 4973209426.751526),
    ("schedule_refdate_modern", 5243586867.583277),
    ("schedule_square", 4942269334.27481),
    ("model_direction", 4895227229.345121),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError("Expected CASA 6.7.6.14")
    me, qa = measures(), quanta()
    me.doframe(me.position("ITRF", *[str(x) + "m" for x in POSITION_ITRF_M]))
    cases = []
    for name, seconds in CASES:
        mjd = seconds / 86400.0
        last = me.measure(me.epoch("UT1", qa.quantity(mjd, "d")), "LAST")
        assert last["m0"]["unit"] == "d"
        cases.append({"name": name, "epoch_ut1_mjd": mjd,
                      "last_rad": (last["m0"]["value"] % 1) * 2 * math.pi})
    me.done()
    result = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/sidereal_oracle.py",
              "position_itrf_m": POSITION_ITRF_M, "cases": cases}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
