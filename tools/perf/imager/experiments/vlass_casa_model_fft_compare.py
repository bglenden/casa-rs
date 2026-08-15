#!/usr/bin/env python3
"""Compare a dumped casa-rs AW model FFT with CASA's casacore FFTServer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from casatools import image


def read_plane(path: Path) -> np.ndarray:
    tool = image()
    try:
        tool.open(str(path))
        values = np.squeeze(np.asarray(tool.getchunk(), dtype=np.float32))
    finally:
        tool.done()
    if values.ndim != 2:
        raise RuntimeError(f"expected a two-dimensional plane at {path}: {values.shape}")
    return values


def flat_sky_model(model: np.ndarray, weight: np.ndarray, pb_limit: float) -> np.ndarray:
    weight_peak = np.max(weight).astype(np.float32)
    pb_scale = np.sqrt(weight_peak, dtype=np.float32)
    denominator = np.asarray(
        np.sqrt(np.abs(weight), dtype=np.float32) / pb_scale,
        dtype=np.float32,
    )
    result = np.zeros_like(model, dtype=np.float32)
    valid = np.isfinite(denominator) & (
        denominator > np.float32(abs(pb_limit))
    )
    np.divide(model, denominator, out=result, where=valid)
    return result


def metrics(candidate: np.ndarray, reference: np.ndarray) -> dict:
    difference = candidate - reference
    reference_l2 = float(np.linalg.norm(reference.ravel()))
    difference_l2 = float(np.linalg.norm(difference.ravel()))
    max_index = np.unravel_index(int(np.argmax(np.abs(difference))), difference.shape)
    denominator = np.vdot(reference.ravel(), reference.ravel())
    best_scale = (
        np.vdot(reference.ravel(), candidate.ravel()) / denominator
        if denominator.real > 0
        else complex(np.nan, np.nan)
    )
    return {
        "reference_l2": reference_l2,
        "difference_l2": difference_l2,
        "relative_l2": difference_l2 / reference_l2 if reference_l2 else None,
        "max_absolute_error": float(np.max(np.abs(difference))),
        "max_error_location": [int(value) for value in max_index],
        "best_complex_scale": [float(best_scale.real), float(best_scale.imag)],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared-raw", type=Path)
    parser.add_argument("--model-image", type=Path)
    parser.add_argument("--weight-image", type=Path)
    parser.add_argument("--pb-limit", type=float, default=0.0001)
    parser.add_argument("--rust-grid-raw", required=True, type=Path)
    parser.add_argument("--casa-grid-raw-output", required=True, type=Path)
    parser.add_argument("--prepared-raw-output", type=Path)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--side", type=int, default=4096)
    args = parser.parse_args()

    args.workspace.mkdir(parents=True, exist_ok=False)
    if args.prepared_raw is not None:
        if args.model_image is not None or args.weight_image is not None:
            parser.error("--prepared-raw cannot be combined with CASA model/weight images")
        prepared = np.fromfile(args.prepared_raw, dtype="<f4").reshape(
            (args.side, args.side)
        )
        prepared_source = {"raw": str(args.prepared_raw)}
    else:
        if args.model_image is None or args.weight_image is None:
            parser.error("provide --prepared-raw or both --model-image and --weight-image")
        prepared = flat_sky_model(
            read_plane(args.model_image),
            read_plane(args.weight_image),
            args.pb_limit,
        )
        prepared_source = {
            "model_image": str(args.model_image),
            "weight_image": str(args.weight_image),
            "pb_limit": args.pb_limit,
        }
    if args.prepared_raw_output is not None:
        args.prepared_raw_output.parent.mkdir(parents=True, exist_ok=True)
        np.asarray(prepared, dtype="<f4").tofile(args.prepared_raw_output)
        prepared_source["raw_output"] = str(args.prepared_raw_output)
    rust_components = np.fromfile(args.rust_grid_raw, dtype="<f4").reshape(
        (args.side, args.side, 2)
    )
    rust_grid = rust_components[..., 0] + 1j * rust_components[..., 1]
    rust_grid = np.asarray(rust_grid, dtype=np.complex64)

    input_path = args.workspace / "prepared-float.image"
    complex_input_path = args.workspace / "prepared-complex.image"
    casa_grid_path = args.workspace / "casa-grid-c2c.image"
    tool = image()
    try:
        tool.fromarray(
            outfile=str(input_path),
            pixels=prepared,
            overwrite=False,
            type="f",
        )
        tool.fft(complex=str(complex_input_path), axes=[0, 1])
    finally:
        tool.done()

    tool = image()
    try:
        tool.open(str(complex_input_path))
        tool.putchunk(np.asarray(prepared, dtype=np.complex64))
        tool.fft(complex=str(casa_grid_path), axes=[0, 1])
    finally:
        tool.done()

    tool = image()
    try:
        tool.open(str(casa_grid_path))
        casa_grid = np.squeeze(np.asarray(tool.getchunk(), dtype=np.complex64))
    finally:
        tool.done()
    if casa_grid.shape != rust_grid.shape:
        raise RuntimeError(
            f"CASA grid shape {casa_grid.shape} does not match Rust {rust_grid.shape}"
        )
    casa_components = np.stack((casa_grid.real, casa_grid.imag), axis=-1).astype(
        "<f4", copy=False
    )
    args.casa_grid_raw_output.parent.mkdir(parents=True, exist_ok=True)
    casa_components.tofile(args.casa_grid_raw_output)

    variants = {
        "direct": casa_grid,
        "conjugated": np.conj(casa_grid),
        "transposed": casa_grid.T,
        "quadrant_shifted": np.fft.fftshift(casa_grid),
    }
    result = {
        "kind": "vlass_casa_model_fft_grid_comparison",
        "role": "bounded_correctness_diagnostic_not_performance_evidence",
        "casa_version": "6.7.5.18",
        "shape": [args.side, args.side],
        "prepared_source": prepared_source,
        "rust_grid_raw": str(args.rust_grid_raw),
        "casa_grid_raw": str(args.casa_grid_raw_output),
        "workspace": str(args.workspace),
        "variants": {
            name: metrics(candidate, rust_grid) for name, candidate in variants.items()
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
