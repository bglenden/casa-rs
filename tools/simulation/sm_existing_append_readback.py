#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA C++ reads appended native MSs and compares the actual CASA references."""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
from casatasks import version_string

from sm_existing_append_oracle import snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    with gzip.open(args.fixture, "rt") as stream:
        fixture = json.load(stream)
    count = 0
    for case in fixture["cases"]:
        for workers in (1, 3):
            path = args.native / f"w{workers}-{case['name']}.ms"
            actual = snapshot(path)
            expected = case["after"]
            assert len(actual["rows"]) == len(expected["rows"]), path
            worst = 0.
            for row, (observed, reference) in enumerate(zip(actual["rows"], expected["rows"])):
                for name, value in reference.items():
                    left, right = np.asarray(observed[name]), np.asarray(value)
                    assert left.shape == right.shape, (path, row, name)
                    if name in ("data", "model_data", "corrected_data"):
                        error = np.linalg.norm(left - right) / max(np.linalg.norm(right), 1.)
                        worst = max(worst, float(error))
                        assert error < 1e-3, (path, row, name, error)
                    elif name == "uvw":
                        assert np.max(np.abs(left - right)) < 1e-3, (path, row, name)
                    else:
                        assert np.array_equal(left, right), (path, row, name)
            for name, reference in expected["metadata"].items():
                observed = actual["metadata"][name]
                assert observed["rows"] == reference["rows"], (path, name)
                for column, value in reference["columns"].items():
                    left, right = np.asarray(observed["columns"][column]), np.asarray(value)
                    if left.dtype.kind == "f":
                        limit = 1e-9 if column in ("DIRECTION", "TARGET") else 1e-6
                        assert np.allclose(left, right, rtol=0., atol=limit), (path, name, column)
                    else:
                        assert np.array_equal(left, right), (path, name, column)
            print(f"CASA C++ reads {path.name}: DATA NRMS <= {worst:.6g}, quality and metadata agree")
            count += 1
    assert count == 14


if __name__ == "__main__":
    main()
