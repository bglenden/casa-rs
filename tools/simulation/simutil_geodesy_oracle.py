#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's `simutil` geodesy on a grid of inputs, for casa-rs's binding (#1053).

Run with CASA 6.7.6.14's Python:

    CASAPY -I tools/simulation/simutil_geodesy_oracle.py \\
        --out crates/casars-python/python/tests/data/casa_simutil_geodesy.json

It records what CASA's `simutil` returns, or the message it raises, for:
- `getdatum` of every datum casa-rs tabulates, and of an unknown one;
- `tconpc` and `tmgeod` of UTM zones;
- `utm2long`, `utm2xyz`, `long2xyz`, `xyz2long` and `xyz2long_old` on
  several datums;
- `locxyz2itrf` and `itrf2locname` about several observatories.
"""

from __future__ import annotations

import argparse
import json
import math

import casatools
from casatasks.private.simutil import simutil

DATUMS = [
    "AGD66", "AGD84", "ASTRO", "CAPE", "ED50", "ED79", "GRB36", "HAWAII", "KAUAI", "MAUI", "OAHU",
    "INDIA", "NAD83", "CANADA", "ALASKA", "NAD27", "CARIBB", "MEXICO", "CAMER", "SAM56", "SAM69",
    "CAMPO", "WGS72", "WGS84",
]
# (easting, northing, elevation, zone, nors): ALMA, the VLA, WSRT and the
# southern edge of a zone.
UTM = [
    (627736.0, 7453409.0, 5040.0, 19, "S"),
    (322453.0, 3757700.0, 2124.0, 13, "N"),
    (667318.0, 5853860.0, 5.0, 31, "N"),
    (500000.0, 100.0, 0.0, 30, "N"),
    (166021.0, 0.0, 0.0, 1, "S"),
]
# (longitude, latitude, height) in radians and metres.
GEODETIC = [
    (-1.1825465, -0.4018150, 5058.7),
    (-1.8782943, 0.5916939, 2124.0),
    (0.1146, 0.9152, 5.0),
    (0.0, 0.0, 0.0),
    (math.pi / 2, math.pi / 3, 1000.0),
]
OBSERVATORIES = ["ALMA", "VLA", "WSRT", "ACA", "SMA"]


def call(function, *args, **kwargs):
    """What CASA returns, as plain floats, or the message it raises."""
    try:
        result = function(*args, **kwargs)
    except Exception as error:  # noqa: BLE001 - recording CASA's refusal
        return {"error": str(error)}
    return plain(result)


def plain(value):
    """`value` with NumPy arrays and scalars as lists and floats, and each
    value's type recorded where CASA returns an array."""
    if hasattr(value, "tolist") and getattr(value, "ndim", 0) > 0:
        return {"ndarray": value.tolist()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    u = simutil()
    me = casatools.measures()
    record = {"casa_version": casatools.version_string(),
              "generator": "tools/simulation/simutil_geodesy_oracle.py",
              "getdatum": {code: call(u.getdatum, code) for code in DATUMS + ["NOWHERE"]}}
    record["tconpc"] = []
    record["tmgeod"] = []
    for code in ("WGS84", "NAD27", "ED50"):
        _, _, _, er, rf = u.getdatum(code)
        f = 1.0 / rf
        esq = 2 * f - f * f
        constants = call(u.tconpc, 0.9996, 0.0, er, esq, rf)
        record["tconpc"].append({"args": [0.9996, 0.0, er, esq, rf], "result": constants})
        eps, r, so, v0, v2, v4, v6 = constants
        for easting, northing, _, zone, nors in UTM:
            icm = 183 - 6 * zone if zone < 30 else 543 - 6 * zone
            cm = float(icm) / (180.0 / math.pi)
            fn = 10000000.0 if nors == "S" else 0.0
            tm_args = [northing, easting, eps, cm, 500000.0, 0.9996, so, r, v0, v2, v4, v6, fn, er, esq]
            record["tmgeod"].append({"args": tm_args, "result": call(u.tmgeod, *tm_args)})
    record["utm2long"] = []
    record["utm2xyz"] = []
    for code in ("WGS84", "NAD27", "SAM56", "ED50"):
        for easting, northing, elevation, zone, nors in UTM:
            record["utm2long"].append({"args": [easting, northing, zone, code, nors],
                                       "result": call(u.utm2long, easting, northing, zone, code, nors)})
            record["utm2xyz"].append({"args": [easting, northing, elevation, zone, code, nors],
                                      "result": call(u.utm2xyz, easting, northing, elevation, zone, code, nors)})
    record["long2xyz"] = []
    record["xyz2long"] = []
    record["xyz2long_old"] = []
    for code in ("WGS84", "NAD27", "SAM56", "ED50"):
        for lon, lat, height in GEODETIC:
            xyz = call(u.long2xyz, lon, lat, height, code)
            record["long2xyz"].append({"args": [lon, lat, height, code], "result": xyz})
            record["xyz2long"].append({"args": [*xyz, code], "result": call(u.xyz2long, *xyz, code)})
            record["xyz2long_old"].append({"args": [*xyz, code], "result": call(u.xyz2long_old, *xyz, code)})
    record["locxyz2itrf"] = []
    for lat, lon, alt in ((-23.022886, -67.754929, 5058.7), (34.07875, -107.61833, 2124.0), (0.0, 0.0, 0.0)):
        for local in ((0.0, 0.0, 0.0), (100.0, -250.0, 3.5), (-1500.0, 800.0, -12.0)):
            record["locxyz2itrf"].append({"args": [lat, lon, alt, *local],
                                          "result": call(u.locxyz2itrf, lat, lon, alt, *local)})
    record["itrf2locname"] = []
    for name in OBSERVATORIES:
        centre = me.measure(me.observatory(name), "ITRF")
        x0, y0, z0 = (float(centre[key]["value"]) for key in ("m0", "m1", "m2"))
        # m0..m2 of an ITRF measure are longitude, latitude and radius:
        # convert to Cartesian for points near the observatory.
        cx = z0 * math.cos(y0) * math.cos(x0)
        cy = z0 * math.cos(y0) * math.sin(x0)
        cz = z0 * math.sin(y0)
        xs = [cx, cx + 100.0, cx - 40.0]
        ys = [cy, cy + 30.0, cy + 250.0]
        zs = [cz, cz - 10.0, cz + 5.0]
        result = call(u.itrf2locname, xs, ys, zs, name)
        record["itrf2locname"].append({"args": [xs, ys, zs, name], "result": result})
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
