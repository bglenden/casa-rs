#!/usr/bin/env python3
"""Create a CASA image with empty arrays of different types and shapes."""

import json
import shutil
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def main():
    root = Path("crates/casars-imexplore/tests/data/imhead_crpix")
    target = root / "custom_empty_arrays.im"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(root / "normal.im", target)
    arrays = {"bool": np.empty((0,), dtype=np.bool_), "int": np.empty((0, 2), dtype=np.int16),
              "float": np.empty((2, 0), dtype=np.float32), "complex": np.empty((2, 0, 3), dtype=np.complex64),
              "string": np.empty((0, 2), dtype="U4")}
    assert imhead(str(target), mode="add", hdkey="custom", hdvalue=arrays) is None
    actual = imhead(str(target), mode="get", hdkey="custom")
    expected = {key: {"dtype": str(value.dtype), "shape": list(value.shape)} for key, value in actual.items()}
    for info in target.rglob("table.info"):
        info.write_text("\n".join(line.rstrip() for line in info.read_text().splitlines()).rstrip() + "\n")
    for lock in target.rglob("table.lock"):
        lock.unlink()
    (root / "custom_empty_arrays_expected.json").write_text(json.dumps({"casa_version": version_string(), "record": expected}, indent=2) + "\n")


if __name__ == "__main__":
    main()
