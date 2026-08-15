#!/usr/bin/env python3
"""Compare isolated CASA and casa-rs gridding-oracle products."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
from casatools import image


def read_image(path: Path) -> np.ndarray:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        return np.squeeze(np.asarray(tool.getchunk(), dtype=np.float64))
    finally:
        tool.close()


def compare(candidate: np.ndarray, reference: np.ndarray) -> dict[str, object]:
    if candidate.shape != reference.shape:
        raise RuntimeError(
            f"shape mismatch: candidate={candidate.shape}, reference={reference.shape}"
        )
    difference = candidate - reference
    candidate_flat = candidate.ravel()
    reference_flat = reference.ravel()
    reference_l2 = float(np.linalg.norm(reference_flat))
    reference_linf = float(np.max(np.abs(reference_flat)))
    denominator = float(np.dot(candidate_flat, candidate_flat))
    best_scale = (
        float(np.dot(candidate_flat, reference_flat) / denominator)
        if denominator > 0.0
        else math.nan
    )
    candidate_std = float(np.std(candidate_flat))
    reference_std = float(np.std(reference_flat))
    correlation = (
        float(np.corrcoef(candidate_flat, reference_flat)[0, 1])
        if candidate_flat.size > 1
        and candidate_std > 0.0
        and reference_std > 0.0
        else math.nan
    )
    candidate_peak = np.unravel_index(
        int(np.argmax(np.abs(candidate))), candidate.shape
    )
    reference_peak = np.unravel_index(
        int(np.argmax(np.abs(reference))), reference.shape
    )
    return {
        "shape": list(candidate.shape),
        "candidate_value": (
            float(candidate_flat[0]) if candidate_flat.size == 1 else None
        ),
        "reference_value": (
            float(reference_flat[0]) if reference_flat.size == 1 else None
        ),
        "relative_l2": float(np.linalg.norm(difference.ravel()))
        / max(reference_l2, 1.0e-30),
        "relative_linf": float(np.max(np.abs(difference)))
        / max(reference_linf, 1.0e-30),
        "correlation": correlation,
        "best_candidate_scale_to_reference": best_scale,
        "candidate_peak_index": [int(index) for index in candidate_peak],
        "reference_peak_index": [int(index) for index in reference_peak],
        "peak_index_matches": candidate_peak == reference_peak,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-prefix", required=True, type=Path)
    parser.add_argument("--rust-prefix", required=True, type=Path)
    parser.add_argument("--suffix", action="append", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    products: dict[str, object] = {}
    for suffix in args.suffix:
        products[suffix] = compare(
            read_image(Path(f"{args.rust_prefix}{suffix}")),
            read_image(Path(f"{args.casa_prefix}{suffix}")),
        )
    result = {
        "kind": "vlass_isolated_gridding_oracle_comparison",
        "role": "isolated_semantic_diagnostic_not_promotion_or_performance_evidence",
        "casa_prefix": str(args.casa_prefix),
        "rust_prefix": str(args.rust_prefix),
        "products": products,
    }
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="", flush=True)


if __name__ == "__main__":
    main()
