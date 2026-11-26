#!/usr/bin/env python3
"""Compare the flat-sky MT-MFS model inputs used for AW prediction.

This is a read-only bounded correctness diagnostic. It evaluates the CASA
``divideModelByWeight`` arithmetic for CASA and casa-rs model/weight products,
including the cross combinations that isolate model pixels from weight pixels.
It does not invoke tclean or create a new CASA oracle.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
from casatools import image


def read_plane(path: Path) -> np.ndarray:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        values = np.asarray(tool.getchunk())
    finally:
        tool.close()
    plane = np.squeeze(values)
    if plane.ndim != 2:
        raise RuntimeError(f"expected a two-dimensional image plane at {path}: {plane.shape}")
    return np.asarray(plane, dtype=np.float32)


def flat_sky_model(model: np.ndarray, weight: np.ndarray, pb_limit: float) -> np.ndarray:
    if model.shape != weight.shape:
        raise RuntimeError(f"model/weight shapes disagree: {model.shape} != {weight.shape}")
    weight_peak = np.max(weight).astype(np.float32)
    if not (np.isfinite(weight_peak) and weight_peak > 0):
        raise RuntimeError(f"invalid weight peak: {weight_peak}")
    pb_scale = np.sqrt(weight_peak, dtype=np.float32)
    deno = np.sqrt(np.abs(weight), dtype=np.float32)
    deno = np.asarray(deno / pb_scale, dtype=np.float32)
    result = np.zeros_like(model, dtype=np.float32)
    valid = np.isfinite(deno) & (deno > np.float32(abs(pb_limit)))
    np.divide(model, deno, out=result, where=valid)
    return result


def metrics(candidate: np.ndarray, reference: np.ndarray, epsilon: float) -> dict:
    difference = np.asarray(candidate, dtype=np.float64) - np.asarray(reference, dtype=np.float64)
    reference64 = np.asarray(reference, dtype=np.float64)
    difference_l2 = float(np.linalg.norm(difference.ravel()))
    reference_l2 = float(np.linalg.norm(reference64.ravel()))
    absolute = np.abs(difference)
    flat_index = int(np.argmax(absolute))
    location = [int(value) for value in np.unravel_index(flat_index, difference.shape)]
    return {
        "relative_l2": difference_l2 / max(reference_l2, epsilon),
        "difference_l2": difference_l2,
        "reference_l2": reference_l2,
        "difference_linf": float(absolute.flat[flat_index]),
        "reference_linf": float(np.max(np.abs(reference64))),
        "max_difference_location": location,
        "candidate_at_max_difference": float(candidate[tuple(location)]),
        "reference_at_max_difference": float(reference[tuple(location)]),
        "different_float_pixels": int(np.count_nonzero(candidate != reference)),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-prefix", required=True, type=Path)
    parser.add_argument("--rust-prefix", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pb-limit", type=float, default=0.2)
    parser.add_argument("--absolute-tolerance", type=float, default=1.0e-30)
    args = parser.parse_args()
    if not (math.isfinite(args.pb_limit) and args.pb_limit >= 0):
        raise SystemExit("--pb-limit must be finite and non-negative")
    if not (math.isfinite(args.absolute_tolerance) and args.absolute_tolerance > 0):
        raise SystemExit("--absolute-tolerance must be finite and positive")

    casa_weight = read_plane(Path(f"{args.casa_prefix}.weight.tt0"))
    rust_weight = read_plane(Path(f"{args.rust_prefix}.weight.tt0"))
    result = {
        "kind": "vlass_flat_sky_prediction_model_comparison",
        "role": "bounded_correctness_diagnostic_not_performance_evidence",
        "arithmetic": "CASA Float divideModelByWeight with sqrt(weight peak), abs weight, and strict pblimit",
        "pb_limit": args.pb_limit,
        "prefixes": {
            "casa": str(args.casa_prefix),
            "rust": str(args.rust_prefix),
        },
        "weight": metrics(rust_weight, casa_weight, args.absolute_tolerance),
        "terms": {},
    }
    for term in (0, 1):
        casa_model = read_plane(Path(f"{args.casa_prefix}.model.tt{term}"))
        rust_model = read_plane(Path(f"{args.rust_prefix}.model.tt{term}"))
        combinations = {
            "casa_model_casa_weight": flat_sky_model(
                casa_model, casa_weight, args.pb_limit
            ),
            "rust_model_rust_weight": flat_sky_model(
                rust_model, rust_weight, args.pb_limit
            ),
            "rust_model_casa_weight": flat_sky_model(
                rust_model, casa_weight, args.pb_limit
            ),
            "casa_model_rust_weight": flat_sky_model(
                casa_model, rust_weight, args.pb_limit
            ),
        }
        result["terms"][f"tt{term}"] = {
            "model": metrics(rust_model, casa_model, args.absolute_tolerance),
            "nonzero_model_pixels": {
                "casa": int(np.count_nonzero(casa_model)),
                "rust": int(np.count_nonzero(rust_model)),
                "union": int(np.count_nonzero((casa_model != 0) | (rust_model != 0))),
            },
            "rust_model_weight_isolation": metrics(
                combinations["rust_model_rust_weight"],
                combinations["rust_model_casa_weight"],
                args.absolute_tolerance,
            ),
            "casa_model_weight_isolation": metrics(
                combinations["casa_model_rust_weight"],
                combinations["casa_model_casa_weight"],
                args.absolute_tolerance,
            ),
            "combined_current_paths": metrics(
                combinations["rust_model_rust_weight"],
                combinations["casa_model_casa_weight"],
                args.absolute_tolerance,
            ),
            "same_casa_weight_model_isolation": metrics(
                combinations["rust_model_casa_weight"],
                combinations["casa_model_casa_weight"],
                args.absolute_tolerance,
            ),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
