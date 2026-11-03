# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's direction coordinates of tclean images in each projection (#1064).

Evidence tooling, not a runtime dependency: run with CASA 6.7.6.14's Python,

    python3 -I tools/casa-compat/probe_tclean_projections.py MS OUTDIR

in a directory holding a copy of MS (refim_twochan.ms of casatestdata's
unittest/tclean). tclean images MS dirty in each projection, 100 × 100 cells
of 8 arcsec as test_onefield_projections does; OUTDIR/projections.json
records each image's direction coordinate and the world directions, in
radians, casatools gives a set of its pixels, or the refusal for a name CASA
does not take, and again for images centred south of the equator. It is crates/casa-imaging-application/tests/data/
projections-casa-6.7.6.14.json.
"""

import json
import sys
from pathlib import Path

import casatasks
import casatools

PIXELS = [[0.0, 0.0], [99.0, 99.0], [10.0, 80.0], [50.0, 50.0], [75.5, 20.25], [-40.0, 130.0]]
NAMES = ["SIN", "NCP", "TAN", "ARC", "STG", "ZEA", "CAR", "SFL", "AIT", "MER", "AZP",
         "tan", "ncp", " TAN", "XYZ"]
# The same images centred south of the celestial equator, where a projection
# whose reference is on its native equator takes the native pole at 180°.
SOUTH = "J2000 19:59:28.500 -40.44.01.50"
SOUTH_NAMES = ["SIN", "NCP", "TAN", "CAR", "SFL", "AIT", "MER"]


def plain(value):
    return value.tolist() if hasattr(value, "tolist") else value


def probe(ms, out, name, phasecenter=""):
    suffix = ("lc" if name.islower() else "") + ("sp" if name != name.strip() else "")
    suffix += "south" if phasecenter else ""
    image = out / f"tst{name.strip()}{suffix}"
    try:
        casatasks.tclean(vis=ms, imagename=str(image), imsize=100, cell="8.0arcsec",
                         projection=name, niter=0, phasecenter=phasecenter)
    except Exception as error:  # CASA's refusal
        return {"refused": f"{type(error).__name__}: {error}"}
    ia = casatools.image()
    ia.open(f"{image}.image")
    try:
        cs = ia.coordsys()
        try:
            record = {key: plain(value) for key, value in cs.torecord()["direction0"].items()}
            worlds = [cs.toworld(pixel + [0, 0], "n")["numeric"][:2].tolist() for pixel in PIXELS]
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
        "pixels": PIXELS,
        "worlds_rad": worlds,
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
    }
    (out / "projections.json").write_text(json.dumps(result, indent=1) + "\n")


if __name__ == "__main__":
    main()
