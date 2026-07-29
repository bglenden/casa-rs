#!/usr/bin/env python3
"""Report compact CASA-image diagnostics for a VLASS MT-MFS clean comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from casatools import image


def image_plane(path: Path) -> tuple[np.ndarray, list[str]]:
    tool = image()
    tool.open(str(path))
    try:
        values = np.asarray(tool.getchunk()).squeeze()
        history = list(tool.history(list=False))
    finally:
        tool.close()
    if values.ndim != 2:
        raise RuntimeError(f"{path} has non-plane shape {values.shape}")
    return values, history


def plane_stats(values: np.ndarray, mask_box: tuple[int, int, int, int]) -> dict[str, object]:
    x0, y0, x1, y1 = mask_box
    masked = values[x0 : x1 + 1, y0 : y1 + 1]
    nonzero = np.argwhere(values != 0.0)
    peak_flat = int(np.nanargmax(np.abs(values)))
    peak = np.unravel_index(peak_flat, values.shape)
    masked_peak_flat = int(np.nanargmax(np.abs(masked)))
    masked_peak_local = np.unravel_index(masked_peak_flat, masked.shape)
    return {
        "shape": list(values.shape),
        "sum_f64": float(np.sum(values, dtype=np.float64)),
        "rms_f64": float(np.sqrt(np.mean(np.square(values, dtype=np.float64)))),
        "min": float(np.nanmin(values)),
        "max": float(np.nanmax(values)),
        "nonzero": int(nonzero.shape[0]),
        "nonzero_bounds": (
            None
            if nonzero.size == 0
            else {
                "blc": nonzero.min(axis=0).tolist(),
                "trc": nonzero.max(axis=0).tolist(),
            }
        ),
        "peak": {
            "position": [int(peak[0]), int(peak[1])],
            "value": float(values[peak]),
        },
        "masked_peak": {
            "position": [
                int(masked_peak_local[0] + x0),
                int(masked_peak_local[1] + y0),
            ],
            "value": float(masked[masked_peak_local]),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("prefix", nargs="+", type=Path)
    parser.add_argument("--mask-box", default="575,2125,638,2188")
    parser.add_argument(
        "--delta",
        action="store_true",
        help="also report model-plane deltas between consecutive prefixes",
    )
    args = parser.parse_args()
    mask_box = tuple(int(value) for value in args.mask_box.split(","))
    if len(mask_box) != 4:
        raise SystemExit("--mask-box requires x0,y0,x1,y1")

    result: dict[str, object] = {"mask_box": list(mask_box), "prefixes": {}}
    model_planes: dict[str, dict[str, np.ndarray]] = {}
    for prefix in args.prefix:
        products: dict[str, object] = {}
        for suffix in (".model.tt0", ".model.tt1", ".residual.tt0", ".residual.tt1"):
            values, history = image_plane(Path(f"{prefix}{suffix}"))
            if suffix.startswith(".model"):
                model_planes.setdefault(str(prefix), {})[suffix] = values
            products[suffix] = {
                "stats": plane_stats(values, mask_box),
                "history_tail": history[-40:],
            }
        result["prefixes"][str(prefix)] = products
    if args.delta:
        deltas: list[dict[str, object]] = []
        for left, right in zip(args.prefix, args.prefix[1:]):
            left_key = str(left)
            right_key = str(right)
            products = {}
            for suffix in (".model.tt0", ".model.tt1"):
                products[suffix] = plane_stats(
                    model_planes[right_key][suffix] - model_planes[left_key][suffix],
                    mask_box,
                )
            deltas.append({"left": left_key, "right": right_key, "products": products})
        result["model_deltas"] = deltas
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
