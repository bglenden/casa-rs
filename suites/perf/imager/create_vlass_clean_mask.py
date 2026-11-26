#!/usr/bin/env python3
"""Create the checksum-bound VLASS deterministic-clean mask.

Run this with the frozen CASA Python environment.  The output is a CASA image
with the exact coordinate system and shape of the accepted dirty `.image.tt0`.
Only the inclusive pixel box supplied on the command line is non-zero.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

import numpy as np
from casatools import image


def parse_pixel(value: str) -> tuple[int, int]:
    parts = value.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("pixel coordinates must be X,Y")
    try:
        pixel = tuple(int(part) for part in parts)
    except ValueError as error:
        raise argparse.ArgumentTypeError("pixel coordinates must be integers") from error
    if any(coordinate < 0 for coordinate in pixel):
        raise argparse.ArgumentTypeError("pixel coordinates must be non-negative")
    return pixel  # type: ignore[return-value]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dirty-image",
        type=pathlib.Path,
        required=True,
        help="accepted CASA dirty .image.tt0",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        required=True,
        help="new CASA image mask; the path must not exist",
    )
    parser.add_argument(
        "--blc",
        type=parse_pixel,
        default=(6243, 6003),
        help="inclusive bottom-left X,Y pixel (default: 6243,6003)",
    )
    parser.add_argument(
        "--trc",
        type=parse_pixel,
        default=(6306, 6066),
        help="inclusive top-right X,Y pixel (default: 6306,6066)",
    )
    return parser.parse_args()


def read_image_contract(path: pathlib.Path) -> tuple[list[int], dict[str, Any]]:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"cannot open dirty image {path}")
        shape = [int(value) for value in tool.shape()]
        coordinates = tool.coordsys().torecord()
    finally:
        tool.done()
    return shape, coordinates


def create_mask(
    output: pathlib.Path,
    shape: list[int],
    coordinates: dict[str, Any],
    blc: tuple[int, int],
    trc: tuple[int, int],
) -> int:
    if output.exists():
        raise RuntimeError(f"refusing to overwrite existing output {output}")
    if len(shape) != 4 or shape[2:] != [1, 1]:
        raise RuntimeError(f"expected a four-dimensional singleton-plane image, got {shape}")
    if blc[0] > trc[0] or blc[1] > trc[1]:
        raise RuntimeError(f"descending mask box is invalid: blc={blc}, trc={trc}")
    if trc[0] >= shape[0] or trc[1] >= shape[1]:
        raise RuntimeError(f"mask box blc={blc}, trc={trc} exceeds image shape {shape}")

    output.parent.mkdir(parents=True, exist_ok=True)
    tool = image()
    try:
        created = tool.fromshape(
            outfile=str(output),
            shape=shape,
            csys=coordinates,
            overwrite=False,
            log=False,
        )
        if not created:
            raise RuntimeError(f"cannot create mask image {output}")
        tool.set(pixels=0.0)
        region_shape = [trc[0] - blc[0] + 1, trc[1] - blc[1] + 1, 1, 1]
        pixels = np.ones(region_shape, dtype=np.float32, order="F")
        if not tool.putchunk(pixels, blc=[blc[0], blc[1], 0, 0]):
            raise RuntimeError(f"cannot write mask pixels to {output}")
    finally:
        tool.done()
    return (trc[0] - blc[0] + 1) * (trc[1] - blc[1] + 1)


def main() -> int:
    args = parse_args()
    dirty_image = args.dirty_image.expanduser().resolve()
    output = args.output.expanduser().resolve()
    if not dirty_image.is_dir():
        raise RuntimeError(f"dirty CASA image does not exist: {dirty_image}")
    shape, coordinates = read_image_contract(dirty_image)
    selected_pixels = create_mask(output, shape, coordinates, args.blc, args.trc)
    print(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "vlass_deterministic_clean_mask",
                "dirty_image": str(dirty_image),
                "output": str(output),
                "shape": shape,
                "blc": list(args.blc),
                "trc": list(args.trc),
                "selected_pixels": selected_pixels,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
