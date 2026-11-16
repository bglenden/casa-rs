#!/usr/bin/env python3
"""Have CASA reopen a custom record written by casa-rs with complex values."""

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from casatasks import imhead
from casatools import version_string


def main():
    binary = Path(sys.argv[1]).resolve()
    source = Path("crates/casars-imexplore/tests/data/imhead_crpix/normal.im")
    with tempfile.TemporaryDirectory(prefix="imhead-complex-write-") as temp:
        image = Path(temp) / "image.im"
        shutil.copytree(source, image)
        value = {"kind": "table", "value": {
            "scalar": {"kind": "complex", "value": [1.25, -2.5]},
            "nested": {"kind": "table", "value": {
                "imag": {"kind": "complex", "value": [-0.0, -3.0]},
                "special": {"kind": "complex", "value": ["inf", "-inf"]},
            }},
            "numbers": {"kind": "array", "value": [{"kind": "complex", "value": [1.0, 2.0]}, {"kind": "integer", "value": 3}, {"kind": "float", "value": 2.5}, {"kind": "bool", "value": True}]},
            "matrix": {"kind": "array", "value": [
                {"kind": "array", "value": [{"kind": "complex", "value": [1.0, 2.0]}, {"kind": "integer", "value": 3}, {"kind": "complex", "value": [5.0, 6.0]}]},
                {"kind": "array", "value": [{"kind": "integer", "value": 7}, {"kind": "complex", "value": [9.0, 10.0]}, {"kind": "integer", "value": 11}]},
            ]},
            "words": {"kind": "array", "value": [{"kind": "complex", "value": [1.0, 2.0]}, {"kind": "complex", "value": ["nan", "inf"]}, {"kind": "string", "value": "x"}]},
        }}
        request = {"surface": "imhead", "values": {"imagename": str(image), "mode": "add", "hdkey": "custom"}, "typed_values": {"hdvalue": value}}
        result = subprocess.run([binary, "imhead", "--json-run", "-"], input=json.dumps(request), text=True, capture_output=True, check=True)
        assert json.loads(result.stdout)["status"] == "succeeded", result.stdout
        actual = imhead(str(image), mode="get", hdkey="custom")
        assert actual["scalar"] == 1.25 - 2.5j
        assert actual["nested"] == {"imag": -3j, "special": complex(float("inf"), -float("inf"))}
        assert type(actual["scalar"]) is complex
        np.testing.assert_array_equal(actual["numbers"], [1 + 2j, 3, 2.5, 1])
        np.testing.assert_array_equal(actual["matrix"], [[1 + 2j, 3, 5 + 6j], [7, 9 + 10j, 11]])
        assert actual["matrix"].dtype == np.dtype("complex128")
        np.testing.assert_array_equal(actual["words"], ["(1.000000,2.000000)", "(nan,inf)", "x"])
    print(f"CASA {version_string()} reopened complex header record")


if __name__ == "__main__":
    main()
