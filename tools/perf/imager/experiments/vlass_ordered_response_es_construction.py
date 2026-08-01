#!/usr/bin/env python3
"""Build a high-accuracy 2x-oversampled VLASS response artifact.

This construction-only variant replaces the production J7 gridding kernel
with a width-14 exponential-of-semicircle NUFFT kernel and preserves exact f32
subcell coordinates.  The compact 192-square resident response and hot
application shape remain unchanged.
"""

from __future__ import annotations

import argparse
import importlib.util
import math
import pathlib
import sys

import numpy as np


BASE_SCRIPT = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_segmented_construction.py"
)
SCHEMA = "casa-rs-vlass-ordered-response-segmented-construction/v12"
CONSTRUCTION_SIDE = 384
ES_WIDTH = 14.0
ES_SUPPORT_WIDTH = 15
ES_BETA = 2.30 * ES_WIDTH
KERNEL_DESCRIPTION = (
    "high-accuracy width-14 beta-32.2 exponential-of-semicircle NUFFT kernel "
    "with exact f32 subcell coordinates"
)


def es_kernel_weight(offset: float, delta: int) -> float:
    distance = float(delta) + float(offset)
    normalized = 2.0 * distance / ES_WIDTH
    if abs(normalized) >= 1.0:
        return 0.0
    return math.exp(ES_BETA * (math.sqrt(1.0 - normalized * normalized) - 1.0))


def exact_route_geometry(module, rows, facet_uvw):
    spacing = 1.0 / (CONSTRUCTION_SIDE * module.CELL_RAD)
    x_position = (
        np.asarray(facet_uvw[:, 0], dtype=np.float64) / spacing + CONSTRUCTION_SIDE / 2
    )
    y_position = (
        -np.asarray(facet_uvw[:, 1], dtype=np.float64) / spacing + CONSTRUCTION_SIDE / 2
    )
    x = np.rint(x_position).astype(np.int16)
    y = np.rint(y_position).astype(np.int16)
    offset_x = np.asarray(x - x_position, dtype=np.float32)
    offset_y = np.asarray(y - y_position, dtype=np.float32)
    radius = ES_SUPPORT_WIDTH // 2
    if (
        np.any(x < radius)
        or np.any(y < radius)
        or np.any(x >= CONSTRUCTION_SIDE - radius)
        or np.any(y >= CONSTRUCTION_SIDE - radius)
    ):
        raise module.ConstructionError(
            "high-accuracy ES support crosses the construction boundary"
        )
    return tuple(np.repeat(value, 2) for value in (x, y, offset_x, offset_y))


def load_base_module():
    spec = importlib.util.spec_from_file_location(
        "vlass_ordered_response_es_construction_base",
        BASE_SCRIPT,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load base construction script: {BASE_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.SCHEMA = SCHEMA
    module.SIDE = CONSTRUCTION_SIDE
    module.PIXELS = CONSTRUCTION_SIDE * CONSTRUCTION_SIDE
    module.SUPPORT_WIDTH = ES_SUPPORT_WIDTH
    module.GROUP_META_DTYPE = np.dtype(
        [("offset_x", "<f4"), ("offset_y", "<f4")],
        align=False,
    )
    module.GROUP_META_FILE_TAG = "f32"
    module.KERNEL_DESCRIPTION = KERNEL_DESCRIPTION
    module.route_geometry = lambda rows, facet_uvw: exact_route_geometry(
        module,
        rows,
        facet_uvw,
    )
    module.controlled_kernel_weight = es_kernel_weight
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-contract", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output_dir.exists():
        raise RuntimeError(f"refusing to overwrite {args.output_dir}")
    construction = load_base_module()
    result = construction.derive(args.source_contract, args.output_dir)
    print(
        "manifest={manifest} construction_side={side} response_groups={response_groups} "
        "rhs_groups={rhs_groups} in_memory_s={elapsed:.6f}".format(
            manifest=result["manifest"]["path"],
            side=CONSTRUCTION_SIDE,
            response_groups=result["counts"]["response_groups"],
            rhs_groups=result["counts"]["rhs_groups"],
            elapsed=result["timings"]["in_memory_total_s"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
