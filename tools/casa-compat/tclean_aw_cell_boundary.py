#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native and CASA AW cubes across a frequency-cell boundary (#1121).

Run with CASA's Python, giving the staged refim_point_linRL.ms, the staged
cfcache_oneshiftpoint_mosaic_cbFalse and a release casars-imager binary. The
output directory must not exist; inputs are copied before modification.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

from casatasks import tclean
from casatools import table
import casatools
import numpy as np

import tclean_compare


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vis", type=Path, required=True)
    parser.add_argument("--cfcache", type=Path, required=True)
    parser.add_argument("--imager", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    ms = args.output / "boundary.ms"
    cache = args.output / "casa-cfcache"
    shutil.copytree(args.vis, ms)
    shutil.copytree(args.cfcache, cache)

    spw = table()
    spw.open(str(ms / "SPECTRAL_WINDOW"), nomodify=False)
    try:
        if spw.nrows() != 1 or int(spw.getcell("NUM_CHAN", 0)) != 3:
            raise ValueError("fixture must have one spectral window with three channels")
        frequencies = np.array([1_349_930_000.0, 1_350_010_000.0, 1_350_090_000.0])
        spw.putcell("CHAN_FREQ", 0, frequencies)
        for name in ("CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"):
            spw.putcell(name, 0, np.full(3, 80_000.0))
        spw.putcell("REF_FREQUENCY", 0, frequencies[0])
        spw.putcell("TOTAL_BANDWIDTH", 0, 240_000.0)
    finally:
        spw.close()

    parameters = {
        "vis": str(ms),
        "imagename": str(args.output / "casa"),
        "specmode": "cube",
        "start": 0,
        "width": 1,
        "nchan": 3,
        "interpolation": "nearest",
        "spw": "0",
        "imsize": 1024,
        "cell": "10arcsec",
        "gridder": "awproject",
        "field": "0",
        "cfcache": str(cache),
        "conjbeams": False,
        "wbawp": True,
        "psterm": False,
        "pblimit": 0.1,
        "reffreq": "1.5GHz",
        "pbcor": True,
        "mosweight": False,
        "parallel": False,
        "niter": 0,
        "weighting": "natural",
    }
    tclean(**parameters)
    native = dict(parameters)
    native.update(
        imagename=str(args.output / "native"),
        cfcache=str(args.cfcache),
        backend="cpu",
        gridprecision="f64",
    )
    request = args.output / "request.json"
    request.write_text(json.dumps({"surface": "tclean", "values": native}, indent=2))
    with (args.output / "native.json").open("w") as stdout, (
        args.output / "native.log"
    ).open("w") as stderr:
        subprocess.run(
            [str(args.imager), "--json-run", str(request)],
            stdout=stdout,
            stderr=stderr,
            check=True,
        )
    reply = json.loads((args.output / "native.json").read_text())
    if reply.get("status") != "succeeded":
        raise RuntimeError(f"native tclean failed: {reply.get('error', reply)}")

    reader = tclean_compare.load_reader()
    results = {}
    for suffix in ("image", "residual", "psf", "model", "sumwt", "pb", "weight"):
        result = tclean_compare.compare_product(
            args.output / f"native.{suffix}",
            args.output / f"casa.{suffix}",
            f"image.{suffix}",
            reader,
            casatools,
            np,
        )
        results[suffix] = result
        print(suffix, "pass", result["passed"], "nrms", result.get("nrms"), flush=True)
    (args.output / "comparison.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result["passed"] for result in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
