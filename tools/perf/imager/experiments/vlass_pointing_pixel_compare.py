#!/usr/bin/env python3
"""Compare casa-rs and CASA direction-to-pixel rounding for VLASS POINTING.

This is an isolated coordinate diagnostic.  It consumes casa-rs's existing
POINTING-group trace and an already-frozen CASA image; it does not run imaging.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np
from casatools import image


INPUT_RE = re.compile(
    r"casa_aw_pointing_group_input "
    r"field_id=(?P<field>\d+) antenna_id=(?P<antenna>\d+) "
    r"direction_ra_rad=(?P<ra>[-+0-9.eE]+) "
    r"direction_dec_rad=(?P<dec>[-+0-9.eE]+) "
    r"pixel_x=(?P<x>[-+0-9.eE]+) pixel_y=(?P<y>[-+0-9.eE]+)"
)


def f32_bits(value: float) -> int:
    return int(np.asarray(np.float32(value)).view(np.uint32))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace-log", required=True, type=Path)
    parser.add_argument("--casa-image", required=True, type=Path)
    parser.add_argument("--trace-imsize", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    rows = []
    for line in args.trace_log.read_text(encoding="utf-8").splitlines():
        match = INPUT_RE.search(line)
        if match is not None:
            rows.append(
                {
                    "field_id": int(match["field"]),
                    "antenna_id": int(match["antenna"]),
                    "ra_rad": float(match["ra"]),
                    "dec_rad": float(match["dec"]),
                    "trace_pixel": [float(match["x"]), float(match["y"])],
                }
            )
    if not rows:
        raise RuntimeError(f"no POINTING inputs found in {args.trace_log}")

    tool = image()
    try:
        if not tool.open(str(args.casa_image)):
            raise RuntimeError(f"failed to open CASA image {args.casa_image}")
        target_shape = [int(value) for value in tool.shape()]
        target_imsize = target_shape[0]
        if target_shape[1] != target_imsize:
            raise RuntimeError(f"CASA image is not square: {target_shape}")
        center_delta = 0.5 * (args.trace_imsize - target_imsize)
        comparisons = []
        for row in rows:
            casa_pixel = [
                float(value)
                # The frozen image coordinate units are radians.  Supplying a
                # direction-measure record to image.topixel is ambiguous in
                # CASA 6.7.5.18 and silently resolves to the reference world
                # coordinate, so use the documented numeric world vector.
                for value in tool.topixel(
                    [row["ra_rad"], row["dec_rad"]]
                )["numeric"][:2]
            ]
            rust_pixel = [
                row["trace_pixel"][0] - center_delta,
                row["trace_pixel"][1] - center_delta,
            ]
            delta = [
                rust_pixel[0] - casa_pixel[0],
                rust_pixel[1] - casa_pixel[1],
            ]
            rust_f32_bits = [f32_bits(value) for value in rust_pixel]
            casa_f32_bits = [f32_bits(value) for value in casa_pixel]
            comparisons.append(
                {
                    **row,
                    "rust_target_pixel": rust_pixel,
                    "casa_target_pixel": casa_pixel,
                    "rust_minus_casa_pixel": delta,
                    "rust_f32_bits": rust_f32_bits,
                    "casa_f32_bits": casa_f32_bits,
                    "f32_bits_match": rust_f32_bits == casa_f32_bits,
                }
            )
    finally:
        tool.close()

    result = {
        "kind": "vlass_pointing_direction_to_pixel_comparison",
        "role": "isolated_coordinate_diagnostic_not_promotion_evidence",
        "trace_log": str(args.trace_log),
        "casa_image": str(args.casa_image),
        "trace_imsize": args.trace_imsize,
        "target_imsize": target_imsize,
        "comparison_count": len(comparisons),
        "f32_match_count": sum(row["f32_bits_match"] for row in comparisons),
        "max_abs_pixel_delta": [
            max(abs(row["rust_minus_casa_pixel"][axis]) for row in comparisons)
            for axis in range(2)
        ],
        "comparisons": comparisons,
    }
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite output: {args.output}")
    args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="", flush=True)


if __name__ == "__main__":
    main()
