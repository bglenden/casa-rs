# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's direction coordinates of tclean images in each projection (#1064).

Evidence tooling, not a runtime dependency: run with CASA 6.7.6.14's Python,

    python3 -I tools/casa-compat/probe_tclean_projections.py MS OUTDIR

in a directory holding a copy of MS (refim_twochan.ms of casatestdata's
unittest/tclean). tclean images MS dirty in each projection, 100 × 100 cells
of 8 arcsec as test_onefield_projections does; OUTDIR/projections.json
records each image's direction coordinate and the world directions, in
radians, casatools gives a set of its pixels, or the refusal for a name CASA
does not take, and again for images centred south of the equator and on it. It is libs/imaging/casa-imaging/tests/data/
projections-casa-6.7.6.14.json.
"""

import json
import math
import sys
from pathlib import Path

import casatasks
import casatools

PIXELS = [[0.0, 0.0], [99.0, 99.0], [10.0, 80.0], [50.0, 50.0], [75.5, 20.25], [-40.0, 130.0]]
# Airy uses a linear limit near the reference point and is also defined across
# the far hemisphere. The off-image pixels test both sides of the formula.
AIR_PIXELS = PIXELS + [[54.35, 50.0], [54.38, 50.0], [30050.0, 20050.0], [50050.0, 50050.0]]
PSEUDOCYLINDRICAL_PIXELS = PIXELS + [[30050.0, 25050.0], [-16950.0, -25950.0]]
PCO_PIXELS = PIXELS + [[18050.0, 50.02], [18050.0, 50.06], [18050.0, 25050.0], [25832.0, 41000.0]]
CUBE_NAMES = {"TSC", "CSC", "QSC"}
CUBE_FACE_SAMPLES = [
    (0.35, 0.55), (2.52, 0.17), (4.31, -0.45), (6.42, 0.39),
    (0.39, 2.59), (0.59, -2.61), (-2.21, -0.58),
]
NAMES = ["SIN", "NCP", "TAN", "ARC", "STG", "ZEA", "CAR", "SFL", "AIT", "MER", "AZP",
         "SZP", "ZPN", "AIR", "CYP", "CEA", "PAR", "MOL", "COP", "COE", "COD", "COO", "BON", "PCO", "TSC", "CSC", "QSC", "HPX", "tan", "ncp", " TAN", "XYZ"]
# The same images centred south of the celestial equator, where a projection
# whose reference is on its native equator takes the native pole at 180°.
SOUTH = "J2000 19:59:28.500 -40.44.01.50"
SOUTH_NAMES = ["SIN", "NCP", "TAN", "CAR", "SFL", "AIT", "MER", "AZP", "SZP", "AIR", "PAR", "MOL", "PCO", "TSC", "CSC", "QSC"]
# And on the celestial equator, where the native pole of a projection whose
# reference is on its native equator is the celestial north pole. NCP is
# undefined there (its slant cot(dec) is infinite).
EQUATOR = "J2000 19:59:28.500 +00.00.00.00"
EQUATOR_NAMES = ["SIN", "TAN", "CAR", "SFL", "AIT", "MER", "PAR", "MOL", "PCO", "TSC", "CSC", "QSC"]


def plain(value):
    return value.tolist() if hasattr(value, "tolist") else value


def probe(ms, out, name, phasecenter=""):
    suffix = ("lc" if name.islower() else "") + ("sp" if name != name.strip() else "")
    suffix += {"": "", SOUTH: "south", EQUATOR: "equator"}[phasecenter]
    image = out / f"tst{name.strip()}{suffix}"
    try:
        casatasks.tclean(vis=ms, imagename=str(image), imsize=100, cell="8.0arcsec",
                         projection=name, niter=0, phasecenter=phasecenter)
    except Exception as error:  # CASA's refusal
        return {"refused": f"{type(error).__name__}: {error}"}
    pixels = {
        "AIR": AIR_PIXELS,
        "PAR": PSEUDOCYLINDRICAL_PIXELS,
        "MOL": PSEUDOCYLINDRICAL_PIXELS,
        "PCO": PCO_PIXELS,
    }.get(name, PIXELS)
    inverse = {}
    ia = casatools.image()
    ia.open(f"{image}.image")
    try:
        cs = ia.coordsys()
        try:
            record = {key: plain(value) for key, value in cs.torecord()["direction0"].items()}
            if name in CUBE_NAMES:
                x_scale, y_scale = [math.pi / (4 * cell) for cell in record["cdelt"]]
                cx, cy = record["crpix"]
                pixels = pixels + [
                    [cx + x * x_scale, cy + y * y_scale] for x, y in CUBE_FACE_SAMPLES
                ]
                records = [cs.toworld(pixel + [0, 0], "n")["numeric"] for pixel in pixels]
                worlds = [world[:2].tolist() for world in records]
                # WCSLIB canonicalizes aliases in the unfolded cube. The
                # two single-precision CSC polynomials are approximations.
                inverse["pixels_from_world"] = [
                    cs.topixel(world)["numeric"][:2].tolist() for world in records
                ]
            else:
                worlds = [cs.toworld(pixel + [0, 0], "n")["numeric"][:2].tolist() for pixel in pixels]
            if name == "PCO":
                # WCSLIB pcox2s returns NaN at x=0 with a nonzero offset.
                # Check the finite central-meridian worlds in the other direction.
                for rows in (-3, 3):
                    centre_x, centre_y = record["crpix"]
                    pixel = [centre_x, centre_y + rows]
                    world = cs.referencevalue()["numeric"]
                    world[1] += rows * record["cdelt"][1]
                    measured = cs.topixel(world)["numeric"][:2].tolist()
                    assert all(abs(a - b) < 1e-7 for a, b in zip(measured, pixel))
                    pixels = pixels + [pixel]
                    worlds.append(world[:2].tolist())
        finally:
            cs.done()
    finally:
        ia.done()
    return {
        "projection": record["projection"],
        "projection_parameters": record["projection_parameters"],
        "crval_rad": record["crval"],
        "crpix": record["crpix"],
        "cdelt_rad": record["cdelt"],
        "longpole_deg": record["longpole"],
        "latpole_deg": record["latpole"],
        "pixels": pixels,
        "worlds_rad": worlds,
        **inverse,
    }


def main():
    ms, out = sys.argv[1], Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    probes = {name: probe(ms, out, name) for name in NAMES}
    result = {
        "source": "tools/casa-compat/probe_tclean_projections.py on refim_twochan.ms "
                  f"(unittest/tclean), CASA {casatasks.version_string()}: tclean imsize=100, "
                  "cell='8.0arcsec', niter=0, one image per projection",
        "projections": {name: entry for name, entry in probes.items() if "refused" not in entry},
        "refused": {name: entry["refused"] for name, entry in probes.items() if "refused" in entry},
        "south": {
            "phasecenter": SOUTH,
            "projections": {name: probe(ms, out, name, SOUTH) for name in SOUTH_NAMES},
        },
        "equator": {
            "phasecenter": EQUATOR,
            "projections": {name: probe(ms, out, name, EQUATOR) for name in EQUATOR_NAMES},
        },
    }
    (out / "projections.json").write_text(json.dumps(result, indent=1) + "\n")


if __name__ == "__main__":
    main()
