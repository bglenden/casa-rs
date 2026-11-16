#!/usr/bin/env python3
"""Record imhead's spectral header values on CASA-created images."""

import json
from pathlib import Path

from casatasks import imhead
from casatools import image, version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_spectral")
    root.mkdir(parents=True, exist_ok=True)
    result = {"casa_version": version_string(), "cases": {}}
    for unit in ("Hz", "GHz", "MHz"):
        path = root / f"{unit}.im"
        ia = image()
        ia.fromshape(str(path), [4, 4, 1, 4], overwrite=True)
        cs = ia.coordsys()
        cs.setunits([unit], type="spectral")
        ia.setcoordsys(cs.torecord())
        cs.done()
        ia.done()
        listed = imhead(str(path), mode="list")
        result["cases"][unit] = {
            "get_restfreq": imhead(str(path), mode="get", hdkey="restfreq"),
            "get_reffreqtype": imhead(str(path), mode="get", hdkey="reffreqtype"),
            "list_restfreq": listed["restfreq"].tolist(),
            "list_reffreqtype": listed["reffreqtype"],
        }
        for lock in path.rglob("table.lock"):
            lock.unlink()
    (root / "expected.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
