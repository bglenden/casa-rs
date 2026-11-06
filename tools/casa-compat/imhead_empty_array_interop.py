#!/usr/bin/env python3
"""Check empty arrays written by casa-rs using CASA's own image reader."""

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import image, version_string


def run(binary, image_path, key, typed):
    request = {"surface": "imhead", "values": {"imagename": str(image_path), "mode": "add", "hdkey": key}, "typed_values": {"hdvalue": typed}}
    result = subprocess.run([binary, "imhead", "--json-run", "-"], input=json.dumps(request), text=True, capture_output=True, check=True)
    assert json.loads(result.stdout)["status"] == "succeeded", result.stdout


def main():
    binary = Path(sys.argv[1]).resolve()
    source = Path("crates/casars-imexplore/tests/data/imhead_crpix/normal.im")
    dtypes = {"bool": "bool", "integer": "int64", "float": "float64", "complex": "complex128", "string": "U5"}
    shapes = [(0,), (0, 2), (2, 0), (2, 0, 3)]
    with tempfile.TemporaryDirectory(prefix="imhead-empty-write-") as temp:
        path = Path(temp) / "image.im"
        shutil.copytree(source, path)
        for index, (dtype, expected) in enumerate(dtypes.items()):
            for number, shape in enumerate(shapes):
                typed = {"kind": "empty_array", "value": {"dtype": dtype, "shape": shape}}
                key = f"empty{index}_{number}"
                run(binary, path, key, {"kind": "table", "value": {"value": typed}})
                actual = imhead(str(path), mode="get", hdkey=key)["value"]
                assert actual.dtype == np.dtype(expected), (key, actual.dtype)
                assert actual.shape == ((0,) if dtype == "string" and len(shape) > 1 else shape), (key, actual.shape)
            if dtype != "complex":
                key = f"direct{index}"
                shape = (0, 2)
                run(binary, path, key, {"kind": "empty_array", "value": {"dtype": dtype, "shape": shape}})
                ia = image(); ia.open(str(path))
                actual = ia.miscinfo()[key]
                assert actual.dtype == np.dtype(expected) and actual.shape == shape, (key, actual)
                ia.done()
    print(f"CASA {version_string()} reopened 20 nested and 4 direct empty arrays")


if __name__ == "__main__":
    main()
