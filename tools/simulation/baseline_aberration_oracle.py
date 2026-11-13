#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA's baseline annual-aberration rotation independently (#1053).

APP->JNAT removes precession/nutation and annual aberration. JTRUE->J2000
removes only precession/nutation. Their matrix quotient isolates the inverse
annual-aberration rotation used by MCBaseline, without Earth-orientation data.
"""

import argparse
import json
import math
import pathlib

import casatasks
import numpy as np
from casatools import measures, quanta


def cartesian(qa, record):
    longitude = qa.convert(record["m0"], "rad")["value"]
    latitude = qa.convert(record["m1"], "rad")["value"]
    radius = qa.convert(record["m2"], "m")["value"]
    return [radius * math.cos(latitude) * math.cos(longitude),
            radius * math.cos(latitude) * math.sin(longitude), radius * math.sin(latitude)]


def matrix(me, qa, source, destination):
    columns = []
    for basis in np.eye(3):
        baseline = me.baseline(source, *[str(value) + "m" for value in basis])
        columns.append(cartesian(qa, me.measure(baseline, destination)))
    return np.array(columns).T


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError("Expected CASA 6.7.6.14")
    me, qa = measures(), quanta()
    cases = []
    for epoch in (57560.3, 57754.6, 60689.6):
        assert me.doframe(me.epoch("TDB", qa.quantity(epoch, "d")))
        for direction in ((2.6179938779914944, 0.593411945678072), (4.974188, -0.3995)):
            assert me.doframe(me.direction("J2000", *[str(angle) + "rad" for angle in direction]))
            apparent_to_natural = matrix(me, qa, "APP", "JNAT")
            true_to_mean = matrix(me, qa, "JTRUE", "J2000")
            rotation = apparent_to_natural @ true_to_mean.T
            assert np.max(np.abs(rotation @ rotation.T - np.eye(3))) < 1e-12
            cases.append({"epoch_tdb_mjd": epoch, "phase_center_rad": direction,
                          "app_to_jnat": apparent_to_natural.tolist(),
                          "jtrue_to_j2000": true_to_mean.tolist(),
                          "inverse_aberration_rotation": rotation.tolist()})
    me.done()
    result = {"casa_version": casatasks.version_string(), "work_issue": 1053,
              "recipe": "tools/simulation/baseline_aberration_oracle.py", "cases": cases}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
