#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA simutil behavior on existing axes and unused MS durations."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
import tarfile

os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np

from simobserve_graphics_oracle import artists


def main():
    import casatasks
    from casatasks.private.simutil import simutil
    from matplotlib import pyplot as plt

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    data = Path(__file__).resolve().parents[2] / "crates/casars-python/python/tests/data"
    with gzip.open(data / "casa_simutil_graphics.json.gz", "rt") as source:
        antenna = json.load(source)["antennas"][1]
    with tarfile.open(data / "simutil_graphics_observation.tar.gz") as archive:
        archive.extractall(root, filter="data")
    os.chdir(root)
    cases = []
    for method, total, ms in (("plotants", 0.0, None), ("ephemeris", 0.0, None),
                              ("ephemeris", "unused", "ephemeris.ms"),
                              ("ephemeris", {"value": 1.0, "unit": "Jy"}, "ephemeris.ms")):
        plt.close("all")
        utility = simutil(totaltime=total)
        utility.newfig(show=False)
        axis = plt.gca()
        axis.set_aspect(2.5, adjustable="box")
        axis.set(xlim=(-1, 1), ylim=(-1, 1), xlabel="existing x", ylabel="existing y")
        axis.set_title("existing title", fontsize=23, color="red", fontweight="bold")
        if method == "plotants":
            x, y, z = np.asarray(antenna["positions"]).T
            result = utility.plotants(x, y, z, antenna["diameters"], antenna["names"])
            options = {key: antenna[key] for key in ("positions", "diameters", "names")}
        else:
            options = {"date": "2014/01/01", "direction": "J2000 10h00m00 -30d00m00", "telescope": "ALMA", "ms": ms}
            result = utility.ephemeris(**options)
        cases.append({"method": method, "totaltime": total, "options": options,
                      "artists": artists(plt.gcf()), "returns_none": result is None,
                      "autoscale": [axis.get_autoscalex_on(), axis.get_autoscaley_on()],
                      "adjustable": axis.get_adjustable()})
    plt.close("all")
    with gzip.open(root / "simutil-graphics-context.json.gz", "wt") as output:
        json.dump({"casa_version": casatasks.version_string(), "cases": cases}, output, separators=(",", ":"))
    print(f"Recorded {len(cases)} unmodified CASA plotting-context controls")


if __name__ == "__main__":
    main()
