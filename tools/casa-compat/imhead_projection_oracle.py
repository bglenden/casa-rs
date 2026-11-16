#!/usr/bin/env python3
"""Record CASA imhead projection strings on CASA-created images."""

import json
import math
from pathlib import Path

from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_projection")
    root.mkdir(parents=True, exist_ok=True)
    cases = {
        "sin": ("SIN", [], False),
        "tan": ("TAN", [], False),
        "slant": ("SIN", [1.23456789, -0.00000123], False),
        "ncp": ("SIN", [0.0, 1.0 / math.tan(1.0)], True),
    }
    result = {"casa_version": version_string(), "cases": {}}
    for name, (projection, parameters, ncp) in cases.items():
        path = root / f"{name}.im"
        ia = image()
        ia.fromshape(str(path), [4, 4, 1, 4], overwrite=True)
        cs = ia.coordsys()
        if ncp:
            cs.setunits(["rad", "rad"], type="direction")
            cs.setreferencevalue([0.0, 1.0], type="direction")
        cs.setprojection(projection, parameters)
        ia.setcoordsys(cs.torecord())
        cs.done()
        ia.done()
        result["cases"][name] = imhead(str(path), mode="get", hdkey="projection")
        for lock in path.rglob("table.lock"):
            lock.unlink()
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
