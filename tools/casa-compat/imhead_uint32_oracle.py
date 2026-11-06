#!/usr/bin/env python3
"""Record CASA's return for a casacore UInt32 array in an image record.

The input fixture is a native casacore image record with UInt32 values
[1, 4294967295]; casatools' Python writer ordinarily promotes this input to
signed integers, so the fixture was written through the shared Rust table API.
"""

import json
import shutil
import tempfile
from pathlib import Path

from casatasks import imhead
from casatools import version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_crpix")
    with tempfile.TemporaryDirectory(prefix="imhead-uint32-") as temp:
        path = Path(temp) / "image.im"
        shutil.copytree(root / "custom_uint32.im", path)
        value = imhead(str(path), mode="get", hdkey="custom")["value"]
        result = {"casa_version": version_string(), "dtype": str(value.dtype),
                  "shape": list(value.shape), "value": value.tolist()}
    (root / "custom_uint32_expected.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
