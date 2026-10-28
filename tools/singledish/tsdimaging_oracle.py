#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's single-dish gridding, for casa-imaging-singledish (#938 part 1, #841 Phase 6c).

Run with CASA 6.7.6's Python (for example
`/Applications/CASA.app/Contents/Frameworks/Python.framework/Versions/3.12/bin/python3.12 -I`):

    python3 -I tools/singledish/tsdimaging_oracle.py --work WORK \\
        --out crates/casa-imaging-singledish/tests/data/tsdimaging_casa.json.gz

It makes a total-power MeasurementSet with CASA's own `simobserve`:
- a 230 GHz model of three channels with a few Gaussians;
- one ACA total-power antenna (T701 of `aca.tp.cfg`) mapping a 90″ square
  in 9″ steps, two passes.

It then changes the MeasurementSet so that every input of the gridder
matters:
- the parallel hands differ;
- the two hands' weights differ, row by row;
- some rows (`FLAG_ROW`) and some values (`FLAG`) are flagged.

A copy, `gapped.ms`, has time gaps between the map's rows, so that CASA's
raster-sampling estimate finds rows and measures the interval across them.

For each case of `CASES` it runs `tsdimaging` on one of the two and records:
- the image, its mask and its weight image, flattened in Fortran order as
  little-endian float32 in base64;
- the restoring beam and the brightness unit.

The record also holds, for each MeasurementSet row:
- its time and antenna, its flags and weights;
- its values from the column the gridder reads;
- its pointing, the POINTING row of its antenna nearest in time, in J2000;
- that pointing's pixel on the images, from the image's coordinate system.

It also holds:
- the sampling `im.pointingsampling(pattern='raster')` estimates, as
  `tsdimaging`'s `set_beam_size` asks for it;
- `sdbeamutil.TheoreticalBeam`'s beam over a grid of kernels, cells and
  samplings, and its `primaryBeamArcsec` and `centralObstructionFactor`.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import math
import os
import shutil

import numpy as np
from casatasks import simobserve, tsdimaging
from casatasks.private import sdbeamutil
from casatools import componentlist, image, imager, quanta, table

qa = quanta()
ARCSEC = math.pi / (180.0 * 3600.0)
CENTRE = "J2000 18h00m00.0s -23d00m00.0s"
CELL_ARCSEC = 3.0
IMSIZE = [40, 40]

# The cases: tsdimaging's gridder parameters, on `ms` (the map) or
# `gapped` (the map with time gaps between its rows).
CASES = [
    {"name": "box", "ms": "map", "params": {"gridfunction": "BOX"}},
    {"name": "sf", "ms": "map", "params": {"gridfunction": "SF"}},
    {"name": "sf6", "ms": "map", "params": {"gridfunction": "SF", "convsupport": 6}},
    {"name": "gauss", "ms": "map", "params": {"gridfunction": "GAUSS"}},
    {
        "name": "gauss_given",
        "ms": "map",
        "params": {"gridfunction": "GAUSS", "gwidth": "4arcsec", "truncate": "10arcsec"},
    },
    {"name": "gjinc", "ms": "map", "params": {"gridfunction": "GJINC"}},
    {
        "name": "gjinc_given",
        "ms": "map",
        "params": {"gridfunction": "GJINC", "gwidth": "6arcsec", "jwidth": "5arcsec", "truncate": "8arcsec"},
    },
    {"name": "pb", "ms": "map", "params": {"gridfunction": "PB"}},
    {"name": "sf_gapped", "ms": "gapped", "params": {"gridfunction": "SF", "convsupport": 6}},
]

# The direct TheoreticalBeam setups: kernel, its parameters, cells and
# samplings in arcsec, the antenna's diameter and blockage in m.
BEAM_KERNELS = [
    ("BOX", -1, -1, -1, -1),
    ("SF", -1, -1, -1, -1),
    ("SF", 6, -1, -1, -1),
    ("GJINC", -1, -1, -1, -1),
    ("GAUSS", -1, -1, -1, -1),
]
BEAM_GEOMETRIES = [
    {"cell": [3.0, 3.0], "sampling": [9.0], "diameter": 12.0, "blockage": 0.0},
    {"cell": [3.0, 3.0], "sampling": [9.0], "diameter": 12.0, "blockage": 0.75},
    {"cell": [3.0, 3.0], "sampling": [9.0, 12.0], "diameter": 12.0, "blockage": 0.0},
    {"cell": [5.0, 5.0], "sampling": [0.05], "diameter": 7.0, "blockage": 0.75},
    {"cell": [2.0, 4.0], "sampling": [9.0], "diameter": 12.0, "blockage": 0.0},
]


def floats(values) -> str:
    """Little-endian float32, base64."""
    return base64.b64encode(np.asarray(values, dtype="<f4").tobytes()).decode()


def booleans(values) -> str:
    return base64.b64encode(np.asarray(values, dtype=np.uint8).tobytes()).decode()


def make_model(path: str) -> None:
    """A 230 GHz model of three 10 MHz channels: Gaussians in Jy/pixel."""
    ia = image()
    ia.fromshape(path, [128, 128, 1, 3], overwrite=True)
    cs = ia.coordsys()
    cs.setreferencecode("J2000", "direction")
    cs.setunits(["rad", "rad", "", "Hz"])
    direction = cs.referencevalue(format="n")["numeric"]
    direction[0] = qa.convert(qa.quantity("270deg"), "rad")["value"]
    direction[1] = qa.convert(qa.quantity("-23deg"), "rad")["value"]
    direction[3] = 230.0e9
    cs.setreferencevalue(direction)
    cs.setreferencepixel([64.0, 64.0, 0.0, 0.0])
    cs.setincrement([-ARCSEC, ARCSEC, 1.0, 10.0e6])
    cs.setrestfrequency("230GHz")
    ia.setcoordsys(cs.torecord())
    cs.done()
    y, x = np.mgrid[0:128, 0:128]
    pixels = np.zeros((128, 128, 1, 3), dtype=np.float32)
    for channel in range(3):
        sky = (
            1.0 * np.exp(-((x - 64) ** 2 + (y - 64) ** 2) / (2 * 6.0**2))
            + 0.6 * np.exp(-((x - 40) ** 2 + (y - 80) ** 2) / (2 * 3.0**2)) * (1 + 0.3 * channel)
            + 0.3 * np.exp(-((x - 90) ** 2 + (y - 50) ** 2) / (2 * 10.0**2))
        )
        pixels[:, :, 0, channel] = (sky * 1.0e-3).T
    ia.putchunk(pixels)
    ia.setbrightnessunit("Jy/pixel")
    ia.close()


def make_map(work: str) -> str:
    """The total-power map, perturbed as the module docstring says."""
    make_model(os.path.join(work, "model.image"))
    os.chdir(work)
    simobserve(
        project="tp",
        skymodel="model.image",
        setpointings=True,
        direction=CENTRE,
        mapsize=["90arcsec", "90arcsec"],
        maptype="square",
        pointingspacing="9arcsec",
        integration="1s",
        obsmode="sd",
        sdantlist="aca.tp.cfg",
        sdant=0,
        refdate="2014/05/21",
        hourangle="transit",
        totaltime="242s",
        thermalnoise="",
        graphics="none",
        overwrite=True,
    )
    path = os.path.join(work, "tp", "tp.aca.tp.sd.ms")
    tb = table()
    tb.open(path, nomodify=False)
    nrow = tb.nrows()
    rows = np.arange(nrow)
    # The hands differ; a value grows with its channel.
    for column in ("DATA", "CORRECTED_DATA", "FLOAT_DATA"):
        if column not in tb.colnames():
            continue
        data = tb.getcol(column)  # [corr, chan, row]
        data[1] = data[1] * 0.8 + 2.0e-4 * np.sin(rows)[None, :]
        tb.putcol(column, data)
    weight = tb.getcol("WEIGHT")  # [corr, row]
    weight[0] = 1.0 + 0.5 * np.cos(0.7 * rows)
    weight[1] = 2.0 + 0.3 * np.sin(1.3 * rows)
    tb.putcol("WEIGHT", weight)
    if "WEIGHT_SPECTRUM" in tb.colnames() and tb.iscelldefined("WEIGHT_SPECTRUM", 0):
        spectrum = tb.getcol("WEIGHT_SPECTRUM")
        spectrum[:] = weight[:, None, :]
        tb.putcol("WEIGHT_SPECTRUM", spectrum)
    flag_row = tb.getcol("FLAG_ROW")
    flag_row[rows % 37 == 5] = True
    tb.putcol("FLAG_ROW", flag_row)
    flag = tb.getcol("FLAG")  # [corr, chan, row]
    flag[0, 1, rows % 11 == 3] = True
    flag[1, 2, rows % 13 == 4] = True
    tb.putcol("FLAG", flag)
    tb.close()
    return path


def make_gapped(path: str) -> str:
    """A copy of the map with 20 s between the rows of the raster: a row
    ends where the pointing's latitude changes."""
    gapped = os.path.join(os.path.dirname(path), "gapped.ms")
    shutil.rmtree(gapped, ignore_errors=True)
    shutil.copytree(path, gapped)
    tb = table()
    tb.open(os.path.join(gapped, "POINTING"), nomodify=False)
    ptime = tb.getcol("TIME")
    pdir = tb.getcol("DIRECTION")[:, 0, :]
    order = np.argsort(ptime, kind="stable")
    shift = np.zeros(len(ptime))
    total = 0.0
    previous = None
    for index in order:
        latitude = round(pdir[1, index] / ARCSEC, 3)
        if previous is not None and latitude != previous:
            total += 20.0
        previous = latitude
        shift[index] = total
    tb.putcol("TIME", ptime + shift)
    tb.close()
    # Each MAIN row moves with its nearest POINTING row.
    tb.open(gapped, nomodify=False)
    time = tb.getcol("TIME")
    moved = shift[[int(np.argmin(np.abs(ptime - t))) for t in time]]
    for column in ("TIME", "TIME_CENTROID"):
        tb.putcol(column, tb.getcol(column) + moved)
    tb.close()
    return gapped


def pointings(path: str):
    """Each MAIN row's pointing: the nearest-in-time POINTING row of its
    antenna, its J2000 direction in rad."""
    tb = table()
    tb.open(os.path.join(path, "POINTING"))
    ptime = tb.getcol("TIME")
    pant = tb.getcol("ANTENNA_ID")
    pdir = tb.getcol("DIRECTION")[:, 0, :]
    tb.close()
    tb.open(path)
    time = tb.getcol("TIME")
    antenna = tb.getcol("ANTENNA1")
    tb.close()
    directions = []
    for t, a in zip(time, antenna):
        candidates = np.where(pant == a)[0]
        best = candidates[np.argmin(np.abs(ptime[candidates] - t))]
        directions.append([float(pdir[0, best]), float(pdir[1, best])])
    return time, antenna, directions


def record_rows(path: str, image_path: str) -> dict:
    tb = table()
    tb.open(path)
    column = "CORRECTED_DATA" if "CORRECTED_DATA" in tb.colnames() else "DATA"
    values = np.real(tb.getcol(column)).astype(np.float32)  # [corr, chan, row]
    flags = tb.getcol("FLAG")
    flag_row = tb.getcol("FLAG_ROW")
    weight = tb.getcol("WEIGHT")  # [corr, row]
    tb.close()
    time, antenna, directions = pointings(path)
    ia = image()
    ia.open(image_path)
    cs = ia.coordsys()
    ia.close()
    reference = cs.referencevalue(format="n")["numeric"]
    pixels = []
    for direction in directions:
        world = list(reference)
        world[0], world[1] = direction
        pixel = cs.topixel(world)["numeric"]
        pixels.append([float(pixel[0]), float(pixel[1])])
    cs.done()
    ncorr, nchan, nrow = values.shape
    return {
        "column": column,
        "rows": nrow,
        "channels": nchan,
        "correlations": ncorr,
        "time": time.tolist(),
        "antenna": antenna.tolist(),
        "direction": directions,
        "pixel": pixels,
        "flag_row": [bool(f) for f in flag_row],
        # [row][chan][corr], C order.
        "values": floats(np.transpose(values, (2, 1, 0)).ravel()),
        "flags": booleans(np.transpose(flags, (2, 1, 0)).ravel()),
        # [row][corr], C order.
        "weight": floats(np.transpose(weight).ravel()),
    }


def record_image(path: str, pixels: bool = True) -> dict:
    """An image's shape, unit and beam, and unless `pixels` is false its
    pixels and mask."""
    ia = image()
    ia.open(path)
    out = {"shape": list(ia.shape()), "unit": ia.brightnessunit()}
    if pixels:
        out["pixels"] = floats(ia.getchunk().ravel(order="F"))
        out["mask"] = booleans(ia.getchunk(getmask=True).ravel(order="F"))
    beam = ia.restoringbeam()
    ia.close()
    if beam:
        out["beam_arcsec"] = qa.convert(beam["major"], "arcsec")["value"]
        out["beam_pa_deg"] = qa.convert(beam["positionangle"], "deg")["value"]
    return out


def sampling(path: str) -> dict:
    """`im.pointingsampling` as `set_beam_size` asks for it."""
    im = imager()
    im.open(path)
    im.selectvis(field="", spw="", nchan=-1, start=0, step=1, baseline="T701&&&",
                 scan="", intent="OBSERVE_TARGET#ON_SOURCE", time="")
    params = im.pointingsampling(pattern="raster", ref="J2000", movingsource="",
                                 pointingcolumntouse="direction", antenna="T701&&&")
    im.close()
    return {
        "interval_arcsec": [float(v) for v in qa.getvalue(qa.convert(params["sampling"], "arcsec"))],
        "angle_deg": float(qa.getvalue(qa.convert(params["angle"], "deg"))[0]),
    }


def theoretical_beams() -> list:
    out = []
    for name, convsupport, truncate, gwidth, jwidth in BEAM_KERNELS:
        for geometry in BEAM_GEOMETRIES:
            bu = sdbeamutil.TheoreticalBeam()
            bu.set_antenna(f"{geometry['diameter']}m", f"{geometry['blockage']}m")
            bu.set_sampling([f"{s}arcsec" for s in geometry["sampling"]], "0.0deg")
            bu.set_image_param([f"{c}arcsec" for c in geometry["cell"]], "230GHz", name,
                               convsupport, truncate, gwidth, jwidth, False)
            beam = bu.get_beamsize_image()
            out.append({
                "gridfunction": name,
                "convsupport": convsupport,
                **geometry,
                "frequency_hz": 230.0e9,
                "beam_arcsec": qa.convert(beam["major"], "arcsec")["value"],
            })
    return out


def beam_parts() -> dict:
    epsilons = [0.0, 0.0625, 0.1, 0.107, 0.15, 0.25, 0.33, 0.35, 0.4]
    return {
        "central_obstruction": [
            [e, float(sdbeamutil.centralObstructionFactor(1.0, e))] for e in epsilons
        ],
        "primary_beam_arcsec": [
            [f, d, o, float(sdbeamutil.primaryBeamArcsec(f, d, o, 10, showEquation=False))]
            for f, d, o in [(230.0e9, 12.0, 0.0), (230.0e9, 12.0, 0.75), (100.0e9, 7.0, 0.75)]
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--work", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out_path = os.path.abspath(args.out)
    work = os.path.abspath(args.work)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    maps = {"map": make_map(work)}
    maps["gapped"] = make_gapped(maps["map"])
    record = {"cell_arcsec": CELL_ARCSEC, "imsize": IMSIZE, "phasecenter": CENTRE, "cases": []}
    for case in CASES:
        outfile = os.path.join(work, case["name"])
        tsdimaging(infiles=[maps[case["ms"]]], outfile=outfile, overwrite=True, imsize=IMSIZE,
                   cell=f"{CELL_ARCSEC}arcsec", phasecenter=CENTRE, **case["params"])
        # The gapped map's images are the map's: only its beam differs.
        pixels = case["ms"] == "map"
        record["cases"].append({
            "name": case["name"],
            "ms": case["ms"],
            "params": case["params"],
            "image": record_image(outfile + ".image", pixels),
            "weight": record_image(outfile + ".weight", pixels),
        })
    first = os.path.join(work, CASES[0]["name"] + ".image")
    record["rows"] = {name: record_rows(path, first) for name, path in maps.items()}
    record["sampling"] = {name: sampling(path) for name, path in maps.items()}
    ia = image()
    ia.open(first)
    cs = ia.coordsys()
    record["image_frequencies_hz"] = [
        float(cs.toworld([0, 0, 0, c])["numeric"][3]) for c in range(ia.shape()[3])
    ]
    record["restfreq_hz"] = float(qa.convert(cs.restfrequency(), "Hz")["value"][0])
    cs.done()
    ia.close()
    tb = table()
    tb.open(os.path.join(maps["map"], "SPECTRAL_WINDOW"))
    record["data_frequencies_hz"] = tb.getcell("CHAN_FREQ", 0).tolist()
    tb.close()
    tb.open(os.path.join(maps["map"], "ANTENNA"))
    record["antenna"] = {"name": tb.getcell("NAME", 0), "diameter_m": float(tb.getcell("DISH_DIAMETER", 0))}
    tb.close()
    tb.open(os.path.join(maps["map"], "OBSERVATION"))
    record["telescope"] = tb.getcell("TELESCOPE_NAME", 0)
    tb.close()
    record["theoretical_beams"] = theoretical_beams()
    record["beam_parts"] = beam_parts()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with gzip.open(out_path, "wt") as handle:
        # numpy's scalars as Python's.
        json.dump(record, handle, indent=1, default=lambda value: value.item())
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
