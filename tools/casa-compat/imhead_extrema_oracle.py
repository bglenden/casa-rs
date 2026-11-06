#!/usr/bin/env python3
"""Record CASA imhead extrema on real, masked and complex images."""

import json
import math
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_extrema")
    root.mkdir(parents=True, exist_ok=True)
    cases = ("ordinary", "partial", "masked", "complex", "ties", "nanfirst", "nanlater", "infinities", "four", "linear", "galactic", "tiled", "hadec180", "ecliptic180")
    keys = ("datamin", "datamax", "minpixpos", "maxpixpos", "minpos", "maxpos")
    result = {"casa_version": version_string(), "cases": {}}
    for name in cases:
        path = root / f"{name}.im"
        ia = image()
        shape = (2, 2, 4, 2) if name == "four" else (513, 514) if name == "tiled" else (1, 1) if name in ("hadec180", "ecliptic180") else (3, 4)
        ia.fromshape(str(path), list(shape), overwrite=True, type="c" if name == "complex" else "f", linear=name == "linear")
        if name == "galactic":
            cs = ia.coordsys()
            cs.setreferencecode("GALACTIC", type="direction", adjust=False)
            ia.setcoordsys(cs.torecord())
            cs.done()
        if name in ("hadec180", "ecliptic180"):
            cs = ia.coordsys()
            cs.setreferencecode("HADEC" if name == "hadec180" else "ECLIPTIC", type="direction", adjust=False)
            cs.setunits(["rad", "rad"], type="direction")
            cs.setreferencevalue([math.pi, 0.0], type="direction")
            ia.setcoordsys(cs.torecord())
            cs.done()
        data = np.arange(np.prod(shape), dtype=np.float32).reshape(shape, order="F")
        if name == "ties":
            data[0, 0] = data[2, 3] = -2
            data[1, 0] = data[0, 3] = 99
        elif name == "tiled":
            data[512, 0] = data[0, 1] = -5
            data[512, 512] = data[0, 513] = 999999
        elif name == "nanfirst":
            data[0, 0] = np.nan
        elif name == "nanlater":
            data[2, 0] = np.nan
        elif name == "infinities":
            data[0, 0], data[1, 0] = -np.inf, np.inf
        ia.putchunk(data)
        if name in ("partial", "masked"):
            ia.calcmask(f'"{path}" > ' + ("4" if name == "partial" else "99"), name="selection")
        ia.done()
        listed = imhead(str(path), mode="list")
        result["cases"][name] = {}
        for key in keys:
            if key in listed:
                value = imhead(str(path), mode="get", hdkey=key)
                if isinstance(value, float) and not math.isfinite(value):
                    value = "nan" if math.isnan(value) else "inf" if value > 0 else "-inf"
                result["cases"][name][key] = value.tolist() if isinstance(value, np.ndarray) else value
            else:
                try:
                    imhead(str(path), mode="get", hdkey=key)
                except RuntimeError as error:
                    result["cases"][name][key] = {"error": str(error).split(" at File:", 1)[0]}
                else:
                    raise AssertionError(f"{name}: CASA unexpectedly returned {key}")
        for lock in path.rglob("table.lock"):
            lock.unlink()
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
