#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's `simutil` helpers on a grid of inputs, for casa-rs's port (#841 Phase 5a).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`):

    python3 -I tools/simulation/simutil_oracle.py --configs <dir> \\
        --out crates/casa-simulation/tests/data/simutil_casa.json

`--configs` holds the antenna configuration files `approxBeam` reads (CASA's
`alma/simmos`, for example `~/.casa/data/alma/simmos`).

It records, from CASA's `casatasks.private.simutil.simutil` and `casatools.quanta`:
- `calc_pointings2` for each map type and alias, map size (strings and
  quantity records, square and oblong), spacing and direction. The spacings
  are formed as `simobserve` forms them: `"%fPB" % (ratio/pbcoeff)`, the number
  read back by its regular expression, times the primary beam, as
  `"%farcsec"`. The directions include RA near 0h and 24h, high and southern
  declinations, galactic coordinates, and the `formxxx` form `modifymodel`
  returns; lists of one direction (centred at their rounded mean) and of
  more, valid and not; and the spacings CASA refuses (`refused`);
- `read_pointings` of pointing files in both formats (casa-rs's own and the
  ALMA OT's SEXAGESIMAL lines), and `write_pointings`' text;
- `average_direction`, `median_direction`, `direction_splitter`, `wrapang`;
- `noisetemp` for each telescope, band edge and correlator, `bandname` and
  `polsettings`;
- `baselineLengths` and `approxBeam` for configuration files;
- `qa.formxxx` and `qa.tos`.

The casa-rs tests in `crates/casa-simulation/tests/simutil_casa.rs` compare
the strings exactly.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import tempfile

import casatools
from casatasks.private.simutil import simutil

qa = casatools.quanta()

PBCOEFF = 1.13
GRIDRATIO_INT = 1.0 / math.sqrt(3.0)
GRIDRATIO_TP = 1.0 / 3.0
NYQUIST = 0.5 / PBCOEFF

DIRECTIONS = [
    "J2000 18h00m00.031s -22d59m59.6s",
    "J2000 19h00m00 -23d00m00",
    "J2000 00h00m02.0 +30d00m00",
    "J2000 23h59m58.5 -45d00m00",
    "J2000 12h00m00 +80d00m00",
    "J2000 18:00:00.03100 -022.59.59.600000",
    "ICRS 05h35m17.3 -05d23m28",
    "GALACTIC 10d00m00 +20d00m00",
]

# A direction whose seconds are finer than simutil's precision 5, so that a
# list of it, centred at its rounded mean, differs from the string.
SINGLETON = "J2000 18h00m00.123456789s -23d00m00.123456789s"

MAPTYPES = ["hexagonal", "hex", "square", "ALMA", "alma", "ALMA-OT", "ALMA2012"]

MAPSIZES = [
    ["10arcsec", "10arcsec"],
    ["30arcsec", "30arcsec"],
    ["60arcsec", "20arcsec"],
    ["20arcsec", "60arcsec"],
    ["2arcmin", "2arcmin"],
    ["0.76arcsec", "0.76arcsec"],
]

# (frequency GHz, dish m): the primary beams simobserve derives.
BEAMS = [(100.0, 12.0), (230.0, 12.0), (345.0, 7.0), (672.0, 12.0), (44.0, 25.0)]


def primary_beam(freq_ghz: float, dish_m: float) -> float:
    """simobserve's pb, arcsec."""
    return PBCOEFF * 0.29979 / freq_ghz / dish_m * 3600.0 * 180 / math.pi


def simobserve_spacing(ratio: float, pb: float) -> str:
    """The spacing string simobserve gives calc_pointings2 for a ratio of the beam."""
    pointingspacing = "%fPB" % (ratio / PBCOEFF)
    z = re.compile(r"(\d+.?\d+)\s*PB").match(pointingspacing.upper()).groups()
    return "%farcsec" % (float(z[0]) * pb)


def pointing_cases(util: simutil) -> list[dict]:
    cases = []

    def run(spacing, size, maptype, direction, beam, relmargin=0.5):
        case = {"spacing": spacing, "size": size, "maptype": maptype, "direction": direction,
                "relmargin": relmargin, "beam": beam}
        try:
            case["pointings"] = list(util.calc_pointings2(spacing, size, maptype=maptype, direction=direction,
                                                          relmargin=relmargin, beam=beam))
        except NameError as error:
            # The ALMA tiling's beam default reads an undefined `pbcoeff`.
            case["error"] = f"NameError: {error}"
        except Exception as error:
            # A direction that does not split, or a count of rows or columns
            # Python cannot convert to an integer.
            case["refused"] = f"{type(error).__name__}: {error}"
        cases.append(case)

    for freq, dish in BEAMS:
        pb = primary_beam(freq, dish)
        for ratio in (GRIDRATIO_INT, GRIDRATIO_TP, NYQUIST * PBCOEFF):
            spacing = simobserve_spacing(ratio, pb)
            for maptype in MAPTYPES:
                for size in MAPSIZES:
                    # Large maps only at the wide beams, to keep the fixture small.
                    if size[0] == "2arcmin" and pb < 30.0:
                        continue
                    run(spacing, size, maptype, DIRECTIONS[0], pb)
    # Every direction, on a few layouts.
    pb = primary_beam(230.0, 12.0)
    spacing = simobserve_spacing(GRIDRATIO_INT, pb)
    for direction in DIRECTIONS:
        for maptype in ("hexagonal", "square", "ALMA", "ALMA2012"):
            for size in (["60arcsec", "20arcsec"], ["20arcsec", "60arcsec"], ["45arcsec", "45arcsec"]):
                run(spacing, size, maptype, direction, pb)
    # Sizes as the quantity records modifymodel returns, in other units.
    for size in ([qa.quantity(0.01, "deg"), qa.quantity(0.01, "deg")],
                 [qa.quantity(150.0, "arcsec"), qa.quantity(75.0, "arcsec")],
                 [qa.quantity(1.5, "arcmin"), qa.quantity(0.5, "arcmin")]):
        for maptype in ("hexagonal", "square", "ALMA"):
            run(spacing, size, maptype, DIRECTIONS[1], pb)
    # A single size; a map smaller than one spacing; the beam left at 0 for ALMA.
    run(spacing, "40arcsec", "hexagonal", DIRECTIONS[1], pb)
    run(spacing, ["1arcsec", "1arcsec"], "hexagonal", DIRECTIONS[1], pb)
    run(spacing, ["1arcsec", "1arcsec"], "square", DIRECTIONS[1], pb)
    run(spacing, ["30arcsec", "30arcsec"], "ALMA", DIRECTIONS[1], 0.0)
    run(spacing, ["30arcsec", "30arcsec"], "hexagonal", DIRECTIONS[1], pb, relmargin=0.0)
    # Spacings given directly, in other units.
    for spacing_text in ("10arcsec", "0.1arcmin", "0.002deg"):
        run(spacing_text, ["40arcsec", "40arcsec"], "hexagonal", DIRECTIONS[1], pb)
        run(spacing_text, ["40arcsec", "40arcsec"], "ALMA", DIRECTIONS[1], pb)
    # A list of more than one direction is returned as given, once every
    # direction in it splits.
    run(spacing, ["30arcsec", "30arcsec"], "hexagonal", ["J2000 10h00m00 -30d00m00", "J2000 10h00m01 -30d00m00"], pb)
    run(spacing, ["30arcsec", "30arcsec"], "hexagonal", ["not-a-direction", "also-not-a-direction"], pb)
    run(spacing, ["30arcsec", "30arcsec"], "hexagonal", ["J2000 10h00m00 -30d00m00", "also-not-a-direction"], pb)
    # A list of one direction is centred at its mean, written at precision 5
    # and read back (#913 review); a map too small for a pointing gives the
    # direction as it was given.
    for direction in (SINGLETON, DIRECTIONS[0]):
        for maptype in ("hexagonal", "square", "ALMA", "ALMA2012"):
            run("10arcsec", ["30arcsec", "30arcsec"], maptype, [direction], pb)
            run("10arcsec", ["30arcsec", "30arcsec"], maptype, direction, pb)
    run("10arcsec", ["1arcsec", "1arcsec"], "hexagonal", [SINGLETON], pb)
    # Counts Python cannot convert: a zero spacing (a margin that is not
    # finite, which JSON cannot hold, is casa-rs's unit test). A negative
    # spacing lays out nothing.
    for maptype in ("hexagonal", "square", "ALMA", "ALMA2012"):
        run("0arcsec", ["30arcsec", "30arcsec"], maptype, DIRECTIONS[1], pb)
        run("-10arcsec", ["30arcsec", "30arcsec"], maptype, DIRECTIONS[1], pb)
    run("0arcsec", ["0arcsec", "0arcsec"], "hexagonal", DIRECTIONS[1], pb)
    return [case_with_records(case) for case in cases]


def case_with_records(case: dict) -> dict:
    size = case["size"]
    if isinstance(size, list):
        case["size"] = [s if isinstance(s, str) else {"value": float(s["value"]), "unit": s["unit"]} for s in size]
    return case


POINTING_FILES = {
    "casa_rs_format": "#Epoch     RA          DEC      TIME[sec]\n"
                      "J2000 18:59:57.29543 -023.00.38.805516  10.0\n"
                      "J2000 18h59m57.2954 -23d00m30.18 12.5\n"
                      "J2000 18:59:57.29552 -023.00.21.558620\n"
                      "RA DEC header line that is skipped\n"
                      "-- dashes skipped\n"
                      "two fields\n",
    "ot_sexagesimal": "# OT pointing file\n"
                      "18:59:57.29543, -23:00:38.8055, SEXAGESIMAL, J2000, 15.0\n"
                      "18:59:58.0, -22.59.30.0, SEXAGESIMAL, J2000\n",
    "galactic": "GALACTIC 10d00m00 +20d00m00 7.0\n",
}


def pointing_file_cases(util: simutil, scratch: str) -> list[dict]:
    cases = []
    for name, text in POINTING_FILES.items():
        path = os.path.join(scratch, name + ".ptg.txt")
        with open(path, "w") as handle:
            handle.write(text)
        n, pointings, times = util.read_pointings(path)
        cases.append({"name": name, "text": text, "n": n, "pointings": list(pointings),
                      "times": [float(t) for t in times]})
    return cases


def write_cases(util: simutil, scratch: str) -> list[dict]:
    cases = []
    pointings = ["J2000 18:59:57.29543 -023.00.38.805516", "J2000 18:59:58.39413 -022.59.27.993751"]
    for time in (10.0, [10.0, 12.5], 1e-05, [1.0], 3600, 0.1 + 0.2, 1e16, 123456789.0):
        path = os.path.join(scratch, "w.ptg.txt")
        util.write_pointings(path, pointings, time=time)
        with open(path) as handle:
            text = handle.read()
        cases.append({"pointings": pointings, "time": time, "text": text})
    path = os.path.join(scratch, "w1.ptg.txt")
    util.write_pointings(path, pointings[0], time=5.0)
    with open(path) as handle:
        cases.append({"pointings": pointings[0], "time": 5.0, "text": handle.read()})
    return cases


def direction_cases(util: simutil) -> dict:
    lists = [
        ["J2000 10h00m00 -30d00m00", "J2000 10h00m01 -30d00m00", "J2000 10h00m02 -30d00m10"],
        ["J2000 23h59m59 +10d00m00", "J2000 00h00m01 +10d00m00"],
        ["J2000 00h00m01 +10d00m00", "J2000 23h59m59 +10d00m00", "J2000 23h59m58 +10d00m30",
         "J2000 00h00m03 +10d00m40"],
        ["J2000 18:59:57.29543 -023.00.38.805516"],
        ["ICRS 05h35m17.3 -05d23m28", "J2000 05h35m18.3 -05d23m28"],
    ]
    average, median = [], []
    for directions in lists:
        text, offsets = util.average_direction(directions)
        average.append({"directions": directions, "direction": text, "offsets": offsets.tolist()})
        text, offsets = util.median_direction(directions)
        median.append({"directions": directions, "direction": text, "offsets": offsets.tolist()})
    splitter = []
    # A list, even of one direction, is split at its mean.
    for direction in DIRECTIONS + ["18h00m00 -23d00m00", "J2000 18h00m00 23.00.00", "J2000 18h00m00 -23d00m",
                                   SINGLETON, [SINGLETON], [DIRECTIONS[0]], lists[0]]:
        try:
            epoch, x, y = util.direction_splitter(direction)
        except (IndexError, AttributeError) as error:
            # Without an epoch it reads past the fields; a declination its
            # regular expression does not match has no groups.
            splitter.append({"direction": direction, "error": f"{type(error).__name__}: {error}"})
            continue
        splitter.append({"direction": direction, "epoch": epoch, "x": qa.convert(x, "deg")["value"],
                         "y": qa.convert(y, "deg")["value"], "x_unit": x["unit"], "y_unit": y["unit"]})
    wrapang = []
    for ang, target in ((350.0, 10.0), (10.0, 350.0), (720.5, 0.0), (-190.0, 0.0), (180.0, 0.0), (181.0, 0.0),
                        (-180.0, 0.0), (539.0, 0.0), (540.0, 0.0)):
        wrapang.append({"ang": ang, "target": target, "period": 360.0, "out": util.wrapang(ang, target, 360.0)})
    return {"average_direction": average, "median_direction": median, "direction_splitter": splitter,
            "wrapang": wrapang}


def noise_cases(util: simutil) -> dict:
    noisetemp = []
    telescopes = ["ALMA", "ACA", "ALMASD", "EVLA", "VLA", "SMA", "alma", "WSRT"]
    freqs = ["0.2GHz", "0.3GHz", "1GHz", "1.5GHz", "8GHz", "26.5GHz", "35GHz", "45GHz", "84GHz", "100GHz", "116GHz",
             "230GHz", "275GHz", "345GHz", "500GHz", "672GHz", "720GHz", "950GHz", "1000GHz", "60GHz", "100e9Hz"]
    for telescope in telescopes:
        for freq in freqs:
            for correlator in (None, "", "BLC", "BLC_4x4bit", "ACA", "ACASpec"):
                case = {"telescope": telescope, "freq": freq, "correlator": correlator}
                try:
                    case["result"] = [float(x) for x in util.noisetemp(telescope=telescope, freq=freq,
                                                                         correlator=correlator)]
                except IndexError as error:
                    # The receiver lookup reads f0[z] before testing z: an
                    # unknown telescope above its last tabulated frequency.
                    case["error"] = f"IndexError: {error}"
                noisetemp.append(case)
    noisetemp.append({"telescope": "ALMA", "freq": "230GHz", "correlator": None, "diam": 7.0, "epsilon": 30.0,
                      "result": [float(x) for x in util.noisetemp(telescope="ALMA", freq="230GHz", diam=7.0,
                                                                   epsilon=30.0)]})
    bandname = [{"freq": f, "band": util.bandname(f)}
                for f in (0.1, 0.3, 0.5, 1.0, 1.9, 2.0, 4.0, 8.0, 12.0, 18.0, 26.5, 30.0, 40.0, 45.0, 50.0, 50.1, 75.0,
                          75.1, 90.0, 90.1, 100.0, 149.9, 150.0, 230.0, 345.796, 672.0, 950.0)]
    polsettings = [{"telescope": t, "poltype": p, "out": list(util.polsettings(t, poltype=p))}
                   for t in ("VLA", "EVLA", "ALMA", "DRAO", "WSRT") for p in (None, "circular", "linear", "RR", "x")]
    return {"noisetemp": noisetemp, "bandname": bandname, "polsettings": polsettings}


def beam_cases(util: simutil, configs: str) -> list[dict]:
    cases = []
    for name in ("alma.out01.cfg", "alma.out20.cfg", "alma.cycle5.3.cfg", "aca.i.cfg", "vla.a.cfg", "vla.d.cfg"):
        path = os.path.join(configs, name)
        if not os.path.exists(path):
            continue
        lengths = util.baselineLengths(path)
        for freq in (44.0, 100.0, 230.0, 672.0):
            cases.append({"config": name, "freq_ghz": freq, "beam_arcsec": float(util.approxBeam(path, freq))})
        cases.append({"config": name, "baselines": len(lengths), "max_m": float(max(lengths)),
                      "min_m": float(min(lengths)), "sum_m": float(sum(lengths))})
    return cases


def quanta_cases() -> dict:
    formxxx = []
    values = ["18h00m00.031s", "-22d59m59.6s", "0.5deg", "-0.5deg", "359.9999999deg", "-180deg", "180deg",
              "12h", "24h", "-1h", "1e-9rad", "-1e-9rad", "3.14159265358979rad", "45d30m00.0000005", "86399.9999999s",
              "-89.99999999deg", "90deg", "720.25deg"]
    for value in values:
        for form in ("hms", "dms", "deg", "rad", "+deg", "xyz"):
            for prec in (2, 5, 0, 9):
                formxxx.append({"q": value, "format": form, "prec": prec, "out": qa.formxxx(value, format=form, prec=prec)})
    tos = []
    for value in ("100GHz", "229.98GHz", "1.25e9Hz", "-0.5arcsec", "7200s", "0.1", "1e-12Jy", "12345678.9Hz"):
        for prec in (9, 2, 0, 15):
            tos.append({"q": value, "prec": prec, "out": qa.tos(qa.quantity(value), prec=prec)})
    return {"formxxx": formxxx, "tos": tos}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--configs", required=True, help="CASA's alma/simmos directory")
    parser.add_argument("--out", required=True, help="the JSON fixture to write")
    args = parser.parse_args()
    util = simutil()
    with tempfile.TemporaryDirectory() as scratch:
        fixture = {
            "generator": "tools/simulation/simutil_oracle.py (CASA casatasks.private.simutil)",
            "casa_version": casatools.version_string(),
            "calc_pointings2": pointing_cases(util),
            "read_pointings": pointing_file_cases(util, scratch),
            "write_pointings": write_cases(util, scratch),
            **direction_cases(util),
            **noise_cases(util),
            "beams": beam_cases(util, args.configs),
            **quanta_cases(),
        }
    with open(args.out, "w") as handle:
        json.dump(fixture, handle, separators=(",", ":"))
        handle.write("\n")
    print(f"wrote {args.out}: {len(fixture['calc_pointings2'])} pointing cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
