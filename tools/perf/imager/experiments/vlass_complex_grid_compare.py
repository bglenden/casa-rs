#!/usr/bin/env python3
"""Compare two raw little-endian complex-f32 VLASS diagnostic grids."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


def read_grid(path: Path, side: int) -> np.ndarray:
    components = np.fromfile(path, dtype="<f4")
    expected = side * side * 2
    if components.size != expected:
        raise RuntimeError(
            f"{path} has {components.size} f32 components, expected {expected}"
        )
    components = components.reshape((side, side, 2))
    return np.asarray(components[..., 0] + 1j * components[..., 1], dtype=np.complex64)


def metrics(candidate: np.ndarray, reference: np.ndarray) -> dict:
    difference = np.asarray(candidate - reference, dtype=np.complex64)
    candidate64 = np.asarray(candidate, dtype=np.complex128).ravel()
    reference64 = np.asarray(reference, dtype=np.complex128).ravel()
    difference64 = candidate64 - reference64
    reference_l2 = float(np.linalg.norm(reference64))
    difference_l2 = float(np.linalg.norm(difference64))
    denominator = np.vdot(reference64, reference64)
    best_scale = (
        np.vdot(reference64, candidate64) / denominator
        if denominator.real > 0
        else complex(np.nan, np.nan)
    )
    max_index = np.unravel_index(int(np.argmax(np.abs(difference))), difference.shape)
    return {
        "bit_identical": bool(np.array_equal(candidate.view("<u4"), reference.view("<u4"))),
        "candidate_l2": float(np.linalg.norm(candidate64)),
        "reference_l2": reference_l2,
        "difference_l2": difference_l2,
        "relative_l2": difference_l2 / reference_l2 if reference_l2 else None,
        "max_absolute_error": float(np.max(np.abs(difference))),
        "max_error_location": [int(value) for value in max_index],
        "best_complex_scale": [float(best_scale.real), float(best_scale.imag)],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--side", type=int, default=4096)
    args = parser.parse_args()

    candidate = read_grid(args.candidate, args.side)
    reference = read_grid(args.reference, args.side)
    result = {
        "kind": "vlass_raw_complex_grid_comparison",
        "role": "bounded_correctness_diagnostic_not_performance_evidence",
        "shape": [args.side, args.side],
        "candidate": str(args.candidate),
        "reference": str(args.reference),
        "metrics": metrics(candidate, reference),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
