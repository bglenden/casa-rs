#!/usr/bin/env python3
"""Record CASA 6.7.6.14's imhead beam values and the images it read."""

import json
from pathlib import Path

from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_beams")
    root.mkdir(parents=True, exist_ok=True)
    cases = {
        "degrees": ("3arcmin", "2arcsec", "200deg"),
        "radians": ("3arcmin", "2arcsec", "0.3rad"),
        "radian_wrap": ("3arcmin", "2arcsec", "3.5rad"),
        "negative": ("0.1deg", "5arcmin", "-90deg"),
    }
    result = {"casa_version": version_string(), "cases": {}}
    for name, (major, minor, pa) in cases.items():
        path = root / (name + ".im")
        ia = image()
        ia.fromshape(str(path), [4, 4, 1, 2], overwrite=True)
        ia.setrestoringbeam(major=major, minor=minor, pa=pa)
        ia.done()
        keys = ("beammajor", "beamminor", "beampa", "bmaj", "bmin", "bpa")
        values = {key: imhead(str(path), mode="get", hdkey=key) for key in keys}
        listed = imhead(str(path), mode="list")
        result["cases"][name] = {
            "get": values,
            "list": {key: listed[key] for key in keys[:3]},
            "summary": imhead(str(path), mode="summary")["restoringbeam"],
        }
        for lock in path.rglob("table.lock"):
            lock.unlink()
    result["errors"] = {}
    for name in ("none", "multiple"):
        path = root / (name + ".im")
        ia = image()
        ia.fromshape(str(path), [4, 4, 1, 2], overwrite=True)
        if name == "multiple":
            for channel in range(2):
                ia.setrestoringbeam(major=f"{channel + 2}arcsec", minor="1arcsec", pa="0deg", channel=channel, polarization=0)
        ia.done()
        try:
            imhead(str(path), mode="get", hdkey="beammajor")
        except RuntimeError as error:
            result["errors"][name] = str(error).split(" at File:", 1)[0]
        else:
            raise AssertionError(f"CASA accepted {name} beam")
        for lock in path.rglob("table.lock"):
            lock.unlink()
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
