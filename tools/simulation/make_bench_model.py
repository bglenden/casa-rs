#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Write the benchmark model images for simulation Phase 1d (#841).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
Each entry of the scenario file's `models` (`bench_scenarios.json` by default)
names an image relative to `--model-root` and gives its field centre, band,
plane count, size and cell. The image is written by
`casa_sm_predict_oracle.write_model`: Gaussian blobs and points, a few Jy in
total, J2000 at the field centre, Jy/pixel, and a spectral axis on the case's
window (one plane per channel, or one plane spanning the band). Images that
already exist are kept unless `--force` is given.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from casa_sm_predict_oracle import write_model  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios",
        default=os.path.join(HERE, "bench_scenarios.json"),
        help="scenario file with a 'models' section",
    )
    parser.add_argument("--model-root", required=True, help="directory the images go in")
    parser.add_argument("--only", nargs="*", help="write only these images")
    parser.add_argument("--force", action="store_true", help="replace existing images")
    return parser.parse_args()


def model_args(spec: dict) -> argparse.Namespace:
    """`write_model`'s arguments for one `models` entry."""
    return argparse.Namespace(
        ra=spec["ra"],
        dec=spec["dec"],
        freq_ghz=float(spec["freq_ghz"]),
        chan_width_mhz=float(spec["chan_width_mhz"]),
        nchan=int(spec["nchan"]),
        imsize=int(spec["imsize"]),
        cell_arcsec=float(spec["cell_arcsec"]),
        offset_arcsec=(0.0, 0.0),
        crpix_shift=(0, 0),
        mfs_width_mhz=spec.get("mfs_width_mhz"),
        spectral_unit="Hz",
    )


def main() -> int:
    args = parse_args()
    with open(args.scenarios, encoding="utf-8") as handle:
        models = json.load(handle)["models"]
    os.makedirs(args.model_root, exist_ok=True)
    for name, spec in models.items():
        if args.only and name not in args.only:
            continue
        path = os.path.join(args.model_root, name)
        if os.path.exists(path) and not args.force:
            print(name, "exists")
            continue
        shutil.rmtree(path, ignore_errors=True)
        started = time.perf_counter()
        write_model(path, model_args(spec), int(spec["planes"]))
        print(name, f"written in {time.perf_counter() - started:.1f} s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
