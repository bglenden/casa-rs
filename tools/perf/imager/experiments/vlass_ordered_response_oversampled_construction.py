#!/usr/bin/env python3
"""Build the 2x-oversampled VLASS ordered-response construction artifact.

The hot resident operator remains 192-square.  This construction-only variant
halves the UV-cell spacing on a 384-square grid, allowing a standard NUFFT
deapodization and central lag-domain crop before the compact response bank is
formed.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys


BASE_SCRIPT = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_segmented_construction.py"
)
SCHEMA = "casa-rs-vlass-ordered-response-segmented-construction/v11"
CONSTRUCTION_SIDE = 384


def load_base_module():
    spec = importlib.util.spec_from_file_location(
        "vlass_ordered_response_segmented_construction_base",
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
