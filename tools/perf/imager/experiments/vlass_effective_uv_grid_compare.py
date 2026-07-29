#!/usr/bin/env python3
"""Compare a dumped casa-rs AW grid with a grid inferred from a frozen CASA PSF.

This is an isolated semantic diagnostic, not promotion or performance
evidence. CASA writes only the Float Stokes PSF, so the inferred UV grid is
defined only up to one scalar and includes the noise introduced by the
DComplex-to-Complex and Complex-to-Float boundaries.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from casatools import image


def read_casa_image(path: Path) -> np.ndarray:
    tool = image()
    try:
        if not tool.open(str(path)):
            raise RuntimeError(f"failed to open CASA image {path}")
        values = np.squeeze(np.asarray(tool.getchunk(), dtype=np.float32))
    finally:
        tool.close()
    if values.ndim != 2:
        raise RuntimeError(f"expected a 2D CASA image, got {values.shape}")
    return values


def read_rust_grid(meta_path: Path) -> np.ndarray:
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if metadata.get("dtype") != "complex128":
        raise RuntimeError(f"unsupported dump dtype: {metadata.get('dtype')}")
    shape = tuple(int(value) for value in metadata["shape"])
    data_path = meta_path.with_suffix(".bin")
    values = np.fromfile(data_path, dtype="<f8")
    expected = int(np.prod(shape)) * 2
    if values.size != expected:
        raise RuntimeError(
            f"dump has {values.size} f64 values, expected {expected}"
        )
    return values.reshape(shape + (2,)).view(np.complex128).reshape(shape)


def casa_sinc_axis(size: int, sampling: int) -> np.ndarray:
    index = np.arange(size, dtype=np.int64)
    offset = (index - size // 2).astype(np.float32)
    denominator = np.float32(size) * np.float32(sampling)
    argument = (
        np.float64(np.pi) * offset.astype(np.float64) / np.float64(denominator)
    ).astype(np.float32)
    sinc = np.sin(argument).astype(np.float32) / argument
    sinc[size // 2] = np.float32(1.0)
    return sinc


def centered_fft2(values: np.ndarray) -> np.ndarray:
    return np.fft.fftshift(np.fft.fft2(np.fft.ifftshift(values)))


def best_complex_scale(candidate: np.ndarray, reference: np.ndarray) -> complex:
    denominator = np.vdot(candidate, candidate)
    if denominator == 0.0:
        return complex(np.nan, np.nan)
    return complex(np.vdot(candidate, reference) / denominator)


def compare_variant(
    name: str,
    candidate: np.ndarray,
    reference: np.ndarray,
    support: np.ndarray,
) -> dict[str, object]:
    candidate_support = candidate[support]
    reference_support = reference[support]
    scale = best_complex_scale(candidate_support, reference_support)
    difference = candidate_support * scale - reference_support
    reference_l2 = float(np.linalg.norm(reference_support))
    reference_peak = float(np.max(np.abs(reference_support)))
    return {
        "name": name,
        "scale": {"real": scale.real, "imag": scale.imag},
        "relative_l2_on_rust_support": float(np.linalg.norm(difference))
        / max(reference_l2, 1.0e-300),
        "relative_linf_on_rust_support": float(np.max(np.abs(difference)))
        / max(reference_peak, 1.0e-300),
    }


def fit_affine_complex_tap_ratio(
    candidate: np.ndarray,
    reference: np.ndarray,
    support: np.ndarray,
) -> dict[str, object]:
    positions = np.argwhere(support)
    support_center = (positions.min(axis=0) + positions.max(axis=0)) / 2.0
    delta = positions.astype(np.float64) - support_center
    candidate_support = candidate[support]
    reference_support = reference[support]
    design = np.column_stack(
        (
            candidate_support,
            candidate_support * delta[:, 0],
            candidate_support * delta[:, 1],
        )
    )
    coefficients, _, _, _ = np.linalg.lstsq(
        design, reference_support, rcond=None
    )
    predicted = design @ coefficients
    difference = predicted - reference_support
    reference_l2 = float(np.linalg.norm(reference_support))
    reference_peak = float(np.max(np.abs(reference_support)))
    normalized = coefficients[1:] / coefficients[0]
    return {
        "support_center": support_center.tolist(),
        "coefficients": [
            {"real": float(value.real), "imag": float(value.imag)}
            for value in coefficients
        ],
        "normalized_axis_coefficients": [
            {"real": float(value.real), "imag": float(value.imag)}
            for value in normalized
        ],
        "relative_l2_on_rust_support": float(np.linalg.norm(difference))
        / max(reference_l2, 1.0e-300),
        "relative_linf_on_rust_support": float(np.max(np.abs(difference)))
        / max(reference_peak, 1.0e-300),
    }


def top_entries(values: np.ndarray, count: int) -> list[dict[str, object]]:
    flat = np.abs(values).ravel()
    count = min(count, flat.size)
    indices = np.argpartition(flat, -count)[-count:]
    indices = indices[np.argsort(flat[indices])[::-1]]
    result = []
    for flat_index in indices:
        position = np.unravel_index(int(flat_index), values.shape)
        value = values[position]
        result.append(
            {
                "index": [int(axis) for axis in position],
                "abs": float(abs(value)),
                "real": float(value.real),
                "imag": float(value.imag),
            }
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-psf", required=True, type=Path)
    parser.add_argument("--rust-prefft-meta", required=True, type=Path)
    parser.add_argument("--conv-sampling", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--top", type=int, default=24)
    args = parser.parse_args()

    casa_psf = read_casa_image(args.casa_psf)
    rust_grid = read_rust_grid(args.rust_prefft_meta)
    if casa_psf.shape != rust_grid.shape:
        raise RuntimeError(
            f"shape mismatch: CASA={casa_psf.shape}, rust={rust_grid.shape}"
        )

    sinc = casa_sinc_axis(casa_psf.shape[0], args.conv_sampling)
    casa_uncorrected = (
        casa_psf * sinc[:, np.newaxis] * sinc[np.newaxis, :]
    ).astype(np.float32)
    casa_effective_grid = centered_fft2(casa_uncorrected)

    rust_peak = float(np.max(np.abs(rust_grid)))
    support = np.abs(rust_grid) > rust_peak * 1.0e-12
    support_count = int(np.count_nonzero(support))
    variants = [
        ("identity", rust_grid),
        ("conjugate", np.conjugate(rust_grid)),
        ("transpose", rust_grid.T),
        ("transpose_conjugate", np.conjugate(rust_grid.T)),
        ("flip_both", np.flip(rust_grid, axis=(0, 1))),
        ("flip_both_conjugate", np.conjugate(np.flip(rust_grid, axis=(0, 1)))),
    ]
    comparisons = [
        compare_variant(name, candidate, casa_effective_grid, support)
        for name, candidate in variants
    ]
    comparisons.sort(key=lambda item: item["relative_l2_on_rust_support"])

    outside = np.abs(casa_effective_grid[~support])
    result = {
        "kind": "vlass_frozen_casa_effective_uv_grid_comparison",
        "role": "isolated_semantic_diagnostic_not_promotion_or_performance_evidence",
        "casa_psf": str(args.casa_psf),
        "rust_prefft_meta": str(args.rust_prefft_meta),
        "shape": list(casa_psf.shape),
        "conv_sampling": args.conv_sampling,
        "rust_support_count": support_count,
        "rust_peak": rust_peak,
        "casa_effective_grid_peak": float(np.max(np.abs(casa_effective_grid))),
        "casa_effective_grid_outside_rust_support": {
            "rms": float(np.sqrt(np.mean(outside * outside))),
            "peak": float(np.max(outside)),
        },
        "variant_comparisons": comparisons,
        "identity_affine_complex_tap_ratio": fit_affine_complex_tap_ratio(
            rust_grid, casa_effective_grid, support
        ),
        "rust_top": top_entries(rust_grid, args.top),
        "casa_effective_grid_top": top_entries(casa_effective_grid, args.top),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
