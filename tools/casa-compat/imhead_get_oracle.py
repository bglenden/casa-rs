#!/usr/bin/env python3
"""Record imhead header values with CASA 6.7.6.14.

Run in CASA's Python: python tools/casa-compat/imhead_get_oracle.py.
The fixture is compared by casars-imexplore's casa_returns tests.
"""

import json
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import image, imagemetadata, version_string


def as_json(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def main():
    with tempfile.TemporaryDirectory(prefix="imhead-oracle-") as directory:
        path = str(Path(directory) / "head.im")
        ia = image()
        ia.fromshape(path, [4, 4, 1, 4])
        ia.setbrightnessunit("Jy/beam")
        ia.done()
        keys = [
            "bunit", "BUNIT", "object", "imtype", "masks", "shape", "observer", "telescope", "equinox",
            "cdelt1", "cdelt3", "crval1", "crval3", "crpix1", "ctype1", "cunit1",
        ]
        values = {key: as_json(imhead(path, mode="get", hdkey=key)) for key in keys}
        summary = imhead(path, mode="summary")
        complex_path = str(Path(directory) / "complex.im")
        ia.fromshape(complex_path, [4, 4], type="c")
        ia.done()
        metadata = imagemetadata()
        metadata.open(complex_path)
        metadata.set("object", "Galaxy")
        metadata.set("imtype", "Intensity")
        metadata.done()
        complex_values = {
            key: as_json(imhead(complex_path, mode="get", hdkey=key))
            for key in ("object", "imtype")
        }
        stokes_path = str(Path(directory) / "stokes.im")
        ia.fromshape(stokes_path, [4, 4, 4, 4])
        ia.done()
        print(json.dumps({
            "casa_version": version_string(),
            "get": values,
            "complex_get": complex_values,
            "stokes_get": as_json(imhead(stokes_path, mode="get", hdkey="crval3")),
            "summary": {key: as_json(summary[key]) for key in ("messages", "tileshape")},
        }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
