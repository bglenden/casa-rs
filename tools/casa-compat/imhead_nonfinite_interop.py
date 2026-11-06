#!/usr/bin/env python3
"""Verify CASA reads non-finite custom header values written by casa-rs."""

import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def run(binary, image, key, value):
    request = {"surface": "imhead", "values": {"imagename": str(image), "mode": "add", "hdkey": key}, "typed_values": {"hdvalue": value}}
    result = subprocess.run([binary, "imhead", "--json-run", "-"], input=json.dumps(request), text=True, capture_output=True, check=True)
    assert json.loads(result.stdout)["status"] == "succeeded", result.stdout


def main():
    binary = Path(sys.argv[1]).resolve()
    source = Path("crates/casars-imexplore/tests/data/imhead_crpix/normal.im")
    with tempfile.TemporaryDirectory(prefix="imhead-nonfinite-") as temp:
        path = Path(temp) / "image.im"
        shutil.copytree(source, path)
        run(binary, path, "scalar", {"kind": "float", "value": "nan"})
        assert math.isnan(imhead(str(path), mode="get", hdkey="scalar"))
        value = {"kind": "table", "value": {
            "scalar": {"kind": "float", "value": "-inf"},
            "negative_nan": {"kind": "float", "value": "-nan"},
            "array": {"kind": "array", "value": [{"kind": "float", "value": "inf"}, {"kind": "float", "value": "-nan"}]},
            "words": {"kind": "array", "value": [{"kind": "float", "value": "-nan"}, {"kind": "string", "value": "x"}]},
        }}
        run(binary, path, "custom", value)
        actual = imhead(str(path), mode="get", hdkey="custom")
        assert actual["scalar"] == -float("inf")
        assert np.isnan(actual["negative_nan"]) and np.signbit(actual["negative_nan"])
        assert actual["array"].dtype == np.dtype("float64")
        assert np.isposinf(actual["array"][0]) and np.isnan(actual["array"][1]) and np.signbit(actual["array"][1])
        np.testing.assert_array_equal(actual["words"], ["-nan", "x"])
    print(f"CASA {version_string()} reopened non-finite scalar and record values")


if __name__ == "__main__":
    main()
