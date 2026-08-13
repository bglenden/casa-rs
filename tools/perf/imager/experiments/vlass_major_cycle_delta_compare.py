#!/usr/bin/env python3
"""Compare complete CASA and casa-rs MT-MFS major-cycle model responses."""

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
        values = np.asarray(tool.getchunk(), dtype=np.float64)
    finally:
        tool.close()
    return np.squeeze(values)


def norms(candidate: np.ndarray, reference: np.ndarray, epsilon: float) -> dict:
    difference = candidate - reference
    candidate_flat = candidate.ravel()
    reference_flat = reference.ravel()
    diff_l2 = float(np.linalg.norm(difference.ravel()))
    reference_l2 = float(np.linalg.norm(reference_flat))
    diff_linf = float(np.max(np.abs(difference)))
    reference_linf = float(np.max(np.abs(reference)))
    candidate_std = float(np.std(candidate_flat))
    reference_std = float(np.std(reference_flat))
    correlation = (
        float(np.corrcoef(candidate_flat, reference_flat)[0, 1])
        if candidate_std > 0.0 and reference_std > 0.0
        else math.nan
    )
    return {
        "relative_l2": diff_l2 / max(reference_l2, epsilon),
        "relative_linf": diff_linf / max(reference_linf, epsilon),
        "correlation": correlation,
        "candidate_l2": float(np.linalg.norm(candidate_flat)),
        "reference_l2": reference_l2,
        "difference_l2": diff_l2,
        "candidate_linf": float(np.max(np.abs(candidate))),
        "reference_linf": reference_linf,
        "difference_linf": diff_linf,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-zero-prefix", required=True, type=Path)
    parser.add_argument("--casa-model-prefix", required=True, type=Path)
    parser.add_argument("--rust-zero-prefix", required=True, type=Path)
    parser.add_argument("--rust-model-prefix", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--absolute-tolerance", type=float, default=1.0e-12)
    args = parser.parse_args()
    if not (args.absolute_tolerance > 0.0 and math.isfinite(args.absolute_tolerance)):
        raise SystemExit("--absolute-tolerance must be finite and positive")

    result = {
        "kind": "vlass_major_cycle_delta_comparison",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "definitions": {
            "model_response": "zero_model_residual - common_model_residual",
            "dirty_error": "rust_zero_model_residual - casa_zero_model_residual",
            "relative_l2_pass_ceiling": 2.0e-4,
            "relative_linf_pass_ceiling": 5.0e-4,
            "correlation_pass_floor": 0.99999998,
            "no_regression_multiplier": 1.25,
            "no_regression_reference": "max(dirty_residual, psf)",
        },
        "prefixes": {
            "casa_zero": str(args.casa_zero_prefix),
            "casa_model": str(args.casa_model_prefix),
            "rust_zero": str(args.rust_zero_prefix),
            "rust_model": str(args.rust_model_prefix),
        },
        "terms": {},
    }
    for term in (0, 1):
        suffix = f".residual.tt{term}"
        casa_zero = read_plane(Path(f"{args.casa_zero_prefix}{suffix}"))
        casa_model = read_plane(Path(f"{args.casa_model_prefix}{suffix}"))
        rust_zero = read_plane(Path(f"{args.rust_zero_prefix}{suffix}"))
        rust_model = read_plane(Path(f"{args.rust_model_prefix}{suffix}"))
        casa_psf = read_plane(Path(f"{args.casa_zero_prefix}.psf.tt{term}"))
        rust_psf = read_plane(Path(f"{args.rust_zero_prefix}.psf.tt{term}"))
        shapes = {
            tuple(casa_zero.shape),
            tuple(casa_model.shape),
            tuple(rust_zero.shape),
            tuple(rust_model.shape),
            tuple(casa_psf.shape),
            tuple(rust_psf.shape),
        }
        if len(shapes) != 1:
            raise RuntimeError(f"tt{term} residual shapes disagree: {sorted(shapes)}")
        casa_response = casa_zero - casa_model
        rust_response = rust_zero - rust_model
        operator = norms(rust_response, casa_response, args.absolute_tolerance)
        dirty = norms(rust_zero, casa_zero, args.absolute_tolerance)
        psf = norms(rust_psf, casa_psf, args.absolute_tolerance)
        no_regression_reference = max(dirty["relative_l2"], psf["relative_l2"])
        no_regression_ceiling = 1.25 * no_regression_reference
        checks = {
            "relative_l2": operator["relative_l2"] <= 2.0e-4,
            "relative_linf": operator["relative_linf"] <= 5.0e-4,
            "correlation": operator["correlation"] >= 0.99999998,
            "no_regression": operator["relative_l2"] <= no_regression_ceiling,
        }
        result["terms"][f"tt{term}"] = {
            "shape": list(casa_zero.shape),
            "operator": operator,
            "dirty": dirty,
            "psf": psf,
            "no_regression_reference_relative_l2": no_regression_reference,
            "no_regression_relative_l2_ceiling": no_regression_ceiling,
            "checks": checks,
            "passed": all(checks.values()),
        }
    result["passed"] = all(term["passed"] for term in result["terms"].values())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
