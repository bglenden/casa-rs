#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA edits to a non-spectral tabular image axis."""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, exportfits, imhead
from casatools import coordsys, image, version_string

ROOT = Path("crates/casars-imexplore/tests/data/imhead_crpix")
EDITS = [
    ("m", "cunit1", "km"), ("m", "crval1", "5m"),
    ("m", "cdelt1", "2m"), ("m", "crpix1", 2.0),
    ("km", "crval1", "5km"), ("km", "cdelt1", "2km"),
    ("km", "crpix1", 2.0),
]


def fits_axis(path):
    cards = {}
    data = path.read_bytes()
    for offset in range(0, len(data), 80):
        card = data[offset:offset + 80].decode("ascii")
        key = card[:8].strip()
        if key == "END": break
        if key not in {"CTYPE1", "CRVAL1", "CDELT1", "CRPIX1", "CUNIT1"}: continue
        value = card[10:].split("/", 1)[0].strip()
        cards[key] = value.strip("' ") if value.startswith("'") else float(value.replace("D", "E"))
    return cards


def main():
    casalog.setlogfile("/tmp/casa-977-generic-tabular.log")
    casalog.showconsole(False)
    cases = []
    linear_cases = []
    with tempfile.TemporaryDirectory(prefix="imhead-generic-tabular-") as scratch:
        source = Path(scratch) / "source.im"
        coords = coordsys().newcoordsys(tabular=True)
        coords.setunits(["m"])
        coords.settabular(pixel=[0, 1, 2, 3], world=[1, 2, 4, 7])
        ia = image(); ia.fromshape(str(source), [4], csys=coords.torecord(), overwrite=True)
        ia.done(); coords.done()
        target = ROOT / "generic_tabular.im"
        if target.exists(): shutil.rmtree(target)
        shutil.copytree(source, target, ignore=shutil.ignore_patterns("table.lock"))
        for index, (unit, key, value) in enumerate(EDITS):
            path = Path(scratch) / f"{index}.im"
            shutil.copytree(source, path)
            if unit != "m": imhead(str(path), mode="put", hdkey="cunit1", hdvalue=unit)
            assert imhead(str(path), mode="put", hdkey=key, hdvalue=value) is None
            summary = imhead(str(path), mode="summary")
            ia.open(str(path)); coords = ia.coordsys()
            pixels = [0.0, 1.0, 1.5, 3.0]
            worlds = [coords.toworld([pixel])["numeric"].tolist()[0] for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"{index}.fits"
            exportfits(imagename=str(path), fitsimage=str(fits), overwrite=True)
            cases.append({"unit": unit, "key": key, "value": value,
                "axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                "incr": summary["incr"].tolist(), "refpix": summary["refpix"].tolist(),
                "pixels": pixels, "worlds": worlds, "fits": fits_axis(fits)})
        linear_source = Path(scratch) / "linear.im"
        coords = coordsys().newcoordsys(tabular=True)
        coords.setunits(["m"]); coords.setreferencevalue([1.0]); coords.setincrement([2.0]); coords.setreferencepixel([1.0])
        ia.fromshape(str(linear_source), [4], csys=coords.torecord(), overwrite=True)
        ia.done(); coords.done()
        target = ROOT / "generic_linear_tabular.im"
        if target.exists(): shutil.rmtree(target)
        shutil.copytree(linear_source, target, ignore=shutil.ignore_patterns("table.lock"))
        for index, (unit, key, value) in enumerate(EDITS):
            path = Path(scratch) / f"linear-{index}.im"
            shutil.copytree(linear_source, path)
            if unit != "m": imhead(str(path), mode="put", hdkey="cunit1", hdvalue=unit)
            assert imhead(str(path), mode="put", hdkey=key, hdvalue=value) is None
            summary = imhead(str(path), mode="summary")
            ia.open(str(path)); coords = ia.coordsys()
            pixels = [0.0, 1.0, 1.5, 3.0]
            worlds = [coords.toworld([pixel])["numeric"].tolist()[0] for pixel in pixels]
            coords.done(); ia.done()
            fits = Path(scratch) / f"linear-{index}.fits"
            exportfits(imagename=str(path), fitsimage=str(fits), overwrite=True)
            linear_cases.append({"unit": unit, "key": key, "value": value,
                "axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                "incr": summary["incr"].tolist(), "refpix": summary["refpix"].tolist(),
                "pixels": pixels, "worlds": worlds, "fits": fits_axis(fits)})
    (ROOT / "generic_tabular_expected.json").write_text(json.dumps(
        {"casa_version": version_string(), "cases": cases, "linear_cases": linear_cases}, indent=2) + "\n")


if __name__ == "__main__":
    main()
