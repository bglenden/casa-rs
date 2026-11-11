#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record installed CASA's public simutil plotting behavior (#1053)."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
import tarfile

os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np

from simobserve_graphics_oracle import artists, inputs


def main():
    import casatasks
    from casatasks.private.simutil import simutil
    from matplotlib import pyplot as plt

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parents[2]
    with tarfile.open(source / "crates/casa-simulation/tests/data/simanalyze_analysis_inputs.tar.gz") as archive:
        archive.extractall(root, filter="data")
    os.chdir(root)
    record = {"casa_version": casatasks.version_string(), "lifecycle": {}, "antennas": [], "images": [], "ephemerides": [], "errors": []}

    utility = simutil()
    multi = [2, 2, 1]
    utility.newfig(multi, show=False)
    transitions = []
    for _ in range(5):
        utility.nextfig()
        transitions.append({"multi": multi.copy(), "axes": len(plt.gcf().axes), "subplot": list(plt.gca().get_subplotspec().get_geometry())})
    utility.endfig(show=False, filename="lifecycle.png")
    record["lifecycle"] = {"transitions": transitions, "interactive_after_end": plt.isinteractive(), "tick_size": plt.gca().get_xticklabels()[0].get_fontsize(), "returns_none": utility.newfig(show=False) is None}

    for count, diameter in ((1, 12.0), (8, 12.0), (8, 0.01), (12, 12.0)):
        positions = [[2225061.1636 + i * 23, -5440057.36994 + (i % 3) * 31, -2481681.15054 + (i % 2) * 8] for i in range(count)]
        x, y, z = np.asarray(positions).T
        names = [f"A{i:02d}" for i in range(count)]
        plt.close("all")
        utility.newfig(show=False)
        result = utility.plotants(x, y, z, np.full(count, diameter), names)
        record["antennas"].append({"positions": positions, "diameters": [diameter] * count, "names": names, "returns_none": result is None, "artists": artists(plt.gcf())})

    cases = [("image.flat", {}), ("image.flat", {"bar": False, "showstats": False}), ("stats.flat", {"disprange": []}), ("stats.flat", {"disprange": [0.5]}), ("stats.flat", {"disprange": [0.1, 0.8]}), ("stats.flat", {"disprange": 0.8}), ("masked_corner.flat", {}), ("cube.image", {}), ("kelvin.flat", {}), ("stats.flat", {"plot": False, "disprange": []})]
    for name, options in cases:
        plt.close("all")
        utility.newfig(show=False)
        original = json.loads(json.dumps(options))
        values = utility.statim(f"inputs/{name}", **options)
        record["images"].append({"image": name, "options": original, "options_after": options, "statistics": [float(v) for v in values[:3]] + [values[3]], "artists": artists(plt.gcf())})

    for hour_angle, direction, telescope, total in ((True, "J2000 10h00m00 -30d00m00", "ALMA", "40s"), (False, "J2000 10h00m00 -30d00m00", "ALMA", "0s"), (True, ["J2000 10h00m00 -30d00m00", "J2000 10h00m01 -30d00m01"], "VLA", "1h")):
        plt.close("all")
        utility = simutil(direction=direction, totaltime={"value": float(total[:-1]), "unit": total[-1]})
        utility.telescopename = telescope
        utility.newfig(show=False)
        original_total = utility.totaltime
        try:
            utility.ephemeris("2014/01/01/00:00:00", usehourangle=hour_angle)
        except TypeError as error:
            raw_error = str(error)
        else:
            raise AssertionError("Recheck the CASA quantity/dict comparison defect")
        # A numeric zero avoids the broken dict comparison; this independent
        # control has no duration bar. Preserve the raw failure, never count
        # its partial figure as a passed reference.
        utility.totaltime = 0.0
        utility.newfig(show=False)
        result = utility.ephemeris("2014/01/01/00:00:00", usehourangle=hour_angle)
        record["ephemerides"].append({"date": "2014/01/01/00:00:00", "usehourangle": hour_angle, "direction": direction, "telescope": telescope, "totaltime": 0.0, "original_totaltime": original_total, "raw_error": raw_error, "returns_none": result is None, "artists": artists(plt.gcf())})

    from casatools import measures
    location = measures().observatory("ALMA")
    science = root / "observation-inputs"
    science.mkdir(exist_ok=True)
    model, config = inputs(science)
    casatasks.simobserve(project="observation", skymodel=str(model), antennalist=str(config),
                        mapsize="1arcsec", integration="10s", totaltime="40s", refdate="2014/01/01",
                        thermalnoise="", graphics="none", overwrite=True)
    observation = "observation/observation.small.ms"
    with tarfile.open(root / "simutil-graphics-observation.tar.gz", "w:gz") as archive:
        archive.add(observation, arcname="ephemeris.ms")
    extras = [
        {"date": {"value": 56658.0, "unit": "d"}, "telescope": "custom", "cofa": location},
        {"date": "2014/01/01/13:25:00", "telescope": "ALMA", "usehourangle": False},
        {"date": "2014/01/01/00:00:00", "telescope": "ALMA", "ms": observation},
    ]
    for options in extras:
        plt.close("all")
        utility = simutil(direction="J2000 10h00m00 -30d00m00", totaltime=0.0)
        utility.newfig(show=False)
        result = utility.ephemeris(direction=utility.direction, **options)
        case = {"date": options["date"], "usehourangle": options.get("usehourangle", True),
                "direction": utility.direction, "telescope": options["telescope"], "totaltime": 0.0,
                "returns_none": result is None, "artists": artists(plt.gcf())}
        if "cofa" in options:
            case["cofa"] = options["cofa"]
        if "ms" in options:
            case["ms"] = "ephemeris.ms"
        record["ephemerides"].append(case)
    for direction, telescope in (("J2000 10h00m00 -30d00m00", "custom"), ("J2000 00h00m00 +89d00m00", "ALMA")):
        plt.close("all")
        utility = simutil(totaltime=0.0)
        options = {"direction": direction, "telescope": telescope}
        try:
            utility.ephemeris("2014/01/01", **options)
        except Exception as error:
            record["errors"].append({"options": options, "type": type(error).__name__, "message": str(error)})
        else:
            raise AssertionError("Expected the unobservable/unknown-site refusal")
    plt.close("all")
    utility = simutil(direction="J2000 00h00m00 -89d00m00", totaltime=0.0)
    utility.telescopename = "ALMA"
    utility.newfig(show=False)
    result = utility.ephemeris("2014/01/01")
    record["ephemerides"].append({"date": "2014/01/01", "usehourangle": True,
        "direction": utility.direction, "telescope": "ALMA", "totaltime": 0.0,
        "returns_none": result is None, "artists": artists(plt.gcf())})
    plt.close("all")
    with gzip.open(root / "simutil-graphics-reference.json.gz", "wt") as output:
        json.dump(record, output, separators=(",", ":"))
    print(f"Recorded CASA {record['casa_version']}: lifecycle, {len(record['antennas'])} antenna, {len(record['images'])} image and {len(record['ephemerides'])} ephemeris cases")


if __name__ == "__main__":
    main()
