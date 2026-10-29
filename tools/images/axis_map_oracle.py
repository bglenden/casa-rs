#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA images over transposed axes, for casa-coordinates' axis maps (#1001).

casacore keeps a coordinate system's coordinates in the order they were
added and records the axes each owns in `pixelmap<i>`/`worldmap<i>`. A
transpose keeps the coordinates in place and permutes the maps. Run with
CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`):

    python3 -I tools/images/axis_map_oracle.py fixture \\
        --out crates/casa-images/tests/data/casa-6.7.6/axis_maps

writes these images from one coordinate system, `newcoordsys` with a
direction, Stokes I and Q, and a spectral axis (`direction0`, `stokes1`,
`spectral2`):
- `transposed.image`: `ia.fromarray` with the system after
  `cs.transpose([2, 0, 1, 3])`, pixel axes [Stokes, RA, Dec, Frequency];
- `spectral_first.image`: `imtrans(order='3012')`, pixel axes [Frequency,
  RA, Dec, Stokes];
- `swapped.image`: `imtrans(order='1023')`, Dec before RA;
- `interleaved.image`: `imtrans(order='0213')`, Stokes between RA and Dec;
- `dropdeg_ra.image`: `imsubimage(dropdeg=True)` of an image one RA pixel
  wide, which removes the RA axis from the direction coordinate;
- `pv.image`: `impv` of a 12 by 12 pixel image, whose position-velocity
  output keeps a two-axis linear offset coordinate with its second axis
  removed.

Each pixel holds `sum(index[k] * 10**k) + 0.5` over its own image's axes
before any transpose, so a value names where CASA put it. `axis_maps.json`
records, for each image, CASA's shape, axis coordinate types, axis names and
units, the axis maps of its `coords` keyword, and at a few pixels the world
coordinates (`cs.toworld`) and the pixel value (`ia.getchunk`).

    python3 -I tools/images/axis_map_oracle.py readback IMAGE \\
        --reference crates/casa-images/tests/data/casa-6.7.6/axis_maps/axis_maps.json \\
        --name transposed.image

opens an image another program rewrote from one of the fixtures and checks
that CASA reads it as it read the fixture: the same shape, axis types, names
and units, world coordinates and pixel values at the recorded pixels.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

import numpy as np
from casatasks import impv, imsubimage, imtrans
from casatools import coordsys, ctsys, image, table

MAP_KEYS = ("worldmap", "pixelmap", "worldreplace", "pixelreplace")


def coordinate_system():
    """The fixtures' system before any transpose: [RA, Dec, Stokes, Freq],
    with the direction in radians as images on disk hold it (`newcoordsys`
    gives arcminutes)."""
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes=["I", "Q"])
    cs.setunits(type="direction", value=["rad", "rad"])
    cs.setreferencevalue(type="direction", value=[1.2, -0.4])
    cs.setincrement(type="direction", value=[-1.0e-5, 1.0e-5])
    cs.setreferencepixel(type="direction", value=[1.5, 1.0])
    cs.setreferencevalue(type="spectral", value=[1.415e9])
    cs.setincrement(type="spectral", value=[2.0e6])
    cs.setreferencepixel(type="spectral", value=[2.0])
    return cs


def pixels(shape):
    """`sum(index[k] * 10**k) + 0.5` at each pixel."""
    grid = np.indices(shape, dtype=np.float64)
    return (sum(g * 10.0**k for k, g in enumerate(grid)) + 0.5).astype(np.float32)


def write_image(path, shape, cs):
    ia = image()
    ia.fromarray(outfile=path, pixels=pixels(shape), csys=cs.torecord(), overwrite=True)
    ia.done()


def axis_maps(path):
    tb = table()
    tb.open(path)
    coords = tb.getkeyword("coords")
    tb.close()
    maps = {}
    index = 0
    while f"pixelmap{index}" in coords:
        name = next(key for key in coords if key.endswith(str(index)) and isinstance(coords[key], dict))
        maps[name] = {key: [float(v) if "replace" in key else int(v) for v in coords[f"{key}{index}"]] for key in MAP_KEYS}
        index += 1
    return maps


def sample_pixels(shape):
    """The first and last pixel, and two inside."""
    shape = np.array(shape)
    return [
        [0] * len(shape),
        (shape - 1).tolist(),
        (shape // 2).tolist(),
        [min(1, n - 1) for n in shape],
    ]


def describe(path):
    ia = image()
    ia.open(path)
    cs = ia.coordsys()
    shape = [int(n) for n in ia.shape()]
    data = ia.getchunk()
    samples = []
    for pixel in sample_pixels(shape):
        samples.append({
            "pixel": pixel,
            "world": [float(v) for v in cs.toworld([float(p) for p in pixel], "n")["numeric"]],
            "value": float(data[tuple(pixel)]),
        })
    record = {
        "shape": shape,
        "axis_types": list(cs.axiscoordinatetypes()),
        "names": list(cs.names()),
        "units": list(cs.units()),
        "maps": axis_maps(path),
        "samples": samples,
    }
    cs.done()
    ia.done()
    return record


def fixture(out):
    os.makedirs(out, exist_ok=True)
    scratch = os.path.join(out, "scratch")
    shutil.rmtree(scratch, ignore_errors=True)
    os.makedirs(scratch)
    base = os.path.join(scratch, "base.image")
    write_image(base, [4, 3, 2, 5], coordinate_system())

    images = {}
    cs = coordinate_system()
    cs.transpose([2, 0, 1, 3])
    write_image(os.path.join(out, "transposed.image"), [2, 4, 3, 5], cs)
    images["transposed.image"] = "ia.fromarray after cs.transpose([2, 0, 1, 3])"
    for name, order in [("spectral_first", "3012"), ("swapped", "1023"), ("interleaved", "0213")]:
        path = os.path.join(out, f"{name}.image")
        shutil.rmtree(path, ignore_errors=True)
        imtrans(imagename=base, outfile=path, order=order)
        images[f"{name}.image"] = f"imtrans(order='{order}')"
    narrow = os.path.join(scratch, "narrow.image")
    write_image(narrow, [1, 3, 2, 5], coordinate_system())
    path = os.path.join(out, "dropdeg_ra.image")
    shutil.rmtree(path, ignore_errors=True)
    imsubimage(imagename=narrow, outfile=path, dropdeg=True, overwrite=True)
    images["dropdeg_ra.image"] = "imsubimage(dropdeg=True) of an image one RA pixel wide"
    wide = os.path.join(scratch, "wide.image")
    write_image(wide, [12, 12, 2, 3], coordinate_system())
    path = os.path.join(out, "pv.image")
    shutil.rmtree(path, ignore_errors=True)
    impv(imagename=wide, outfile=path, start=[2, 2], end=[9, 7])
    images["pv.image"] = "impv(start=[2, 2], end=[9, 7]) of a 12 by 12 pixel image"
    shutil.rmtree(scratch)

    document = {
        "casa_version": ctsys.version_string(),
        "images": {name: {"made_by": how, **describe(os.path.join(out, name))} for name, how in images.items()},
    }
    with open(os.path.join(out, "axis_maps.json"), "w") as handle:
        json.dump(document, handle, indent=1)
        handle.write("\n")


def readback(path, reference, name):
    with open(reference) as handle:
        expected = json.load(handle)["images"][name]
    got = describe(path)
    problems = []
    for key in ("shape", "axis_types", "names", "units"):
        if got[key] != expected[key]:
            problems.append(f"{key}: CASA reads {got[key]}, the fixture has {expected[key]}")
    for ours, theirs in zip(got["samples"], expected["samples"]):
        pixel = theirs["pixel"]
        if ours["value"] != theirs["value"]:
            problems.append(f"pixel {pixel}: value {ours['value']} against {theirs['value']}")
        if not np.allclose(ours["world"], theirs["world"], rtol=1e-12, atol=1e-12):
            problems.append(f"pixel {pixel}: world {ours['world']} against {theirs['world']}")
    print(json.dumps({"image": path, "reads_as": name, "maps": got["maps"], "problems": problems}, indent=1))
    return 1 if problems else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    making = commands.add_parser("fixture")
    making.add_argument("--out", required=True)
    reading = commands.add_parser("readback")
    reading.add_argument("image")
    reading.add_argument("--reference", required=True)
    reading.add_argument("--name", required=True)
    args = parser.parse_args()
    if args.command == "fixture":
        fixture(args.out)
        return 0
    return readback(args.image, args.reference, args.name)


if __name__ == "__main__":
    sys.exit(main())
