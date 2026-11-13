#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Read native quick PSFs with CASA and compare their independent reference."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

import numpy as np

from simobserve_graphics_oracle import image_record


def main():
    import casatasks

    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError("The interoperability oracle must be CASA 6.7.6.14")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    with gzip.open(args.reference, "rt") as handle:
        reference = json.load(handle)
    results = []
    for case in reference["cases"]:
        if "psf" not in case:
            continue
        expected = case["psf"]
        paths = sorted(args.native.glob("*/" + case["name"] + "/*.quick.psf"))
        if not paths:
            raise RuntimeError(f"No native PSF for {case['name']}")
        for path in paths:
            actual = image_record(path)
            assert actual["shape"] == expected["shape"], path
            assert actual["unit"] == expected["unit"], path
            assert actual["beam"] == expected["beam"], path
            for key in ("reference", "increment"):
                np.testing.assert_allclose(actual[key], expected[key], rtol=1e-12, atol=0)
            a, b = np.asarray(actual["pixels"]), np.asarray(expected["pixels"])
            assert np.isfinite(a).all() and np.isfinite(b).all(), path
            power = np.linalg.norm(b)
            assert power > 0, path
            nrms = float(np.linalg.norm(a - b) / power)
            assert nrms <= 1e-3, (path, nrms)
            results.append({"case": case["name"], "path": str(path), "shape": actual["shape"],
                            "unit": actual["unit"], "data_nrms": nrms, "limit": 1e-3})
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"casa_version": casatasks.version_string(),
                                    "passed": len(results), "results": results}, indent=1,
                                   allow_nan=False) + "\n")
    print(f"CASA C++ reads and agrees with {len(results)} native quick PSFs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
