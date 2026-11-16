#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record casacore unit spelling normalization on image axes."""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import casalog, imhead
from casatools import image, version_string

ROOT = Path("crates/casars-imexplore/tests/data/imhead_crpix")
CASES = [("cunit1", "deg^1"), ("cunit1", "deg**1"), ("cunit1", "arcsec*1"),
         ("cunit4", "GHz^1"), ("cunit4", "GHz*1")]


def main():
    casalog.setlogfile("/tmp/casa-977-unit-spellings.log")
    casalog.showconsole(False)
    cases = []
    with tempfile.TemporaryDirectory(prefix="imhead-unit-spellings-") as scratch:
        for index, (key, unit) in enumerate(CASES):
            path = Path(scratch) / f"{index}.im"
            shutil.copytree(ROOT / "normal.im", path)
            assert imhead(str(path), mode="put", hdkey=key, hdvalue=unit) is None
            summary = imhead(str(path), mode="summary")
            ia = image(); ia.open(str(path)); coords = ia.coordsys()
            world = coords.toworld([1.0, 1.0, 0.0, 1.0])["numeric"].tolist()
            coords.done(); ia.done()
            cases.append({"key": key, "unit": unit, "value": imhead(str(path), mode="get", hdkey=key),
                "axisunits": summary["axisunits"].tolist(), "refval": summary["refval"].tolist(),
                "incr": summary["incr"].tolist(), "world": world})
    (ROOT / "unit_spellings_expected.json").write_text(json.dumps(
        {"casa_version": version_string(), "cases": cases}, indent=2) + "\n")


if __name__ == "__main__":
    main()
