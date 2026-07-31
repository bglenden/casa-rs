#!/usr/bin/env python3
"""Gate a localized dual-screen VLASS normal-operator representation.

This reducer is deliberately narrower than a production IDG implementation.
It uses the exact CASA-generated pre-W forward and conjugate-frequency reverse
screen families, samples them over the one guarded 128-square reconstruction
facet, and asks whether separate local screen bases can satisfy the proposed
normal-operator error and state ceilings. It also computes a conservative
local W-curvature bound from a measured maximum |w|.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-localized-dual-screen-gate/v1"
SOURCE_SCHEMA = "casa-rs-vlass-evla-pre-w-screens/v2"
ARCSEC_TO_RAD = math.pi / (180.0 * 3600.0)
SCREEN_RMS_LIMIT = 2.0e-5
SCREEN_MAX_LIMIT = 2.0e-4
SCREEN_WORST_STATE_RMS_LIMIT = 6.0e-5
PRODUCT_RMS_LIMIT = 1.0e-5
PRODUCT_MAX_LIMIT = 1.0e-4
PRODUCT_WORST_STATE_RMS_LIMIT = 3.0e-5
W_COMPLEX_MAX_LIMIT = 2.0e-5
PREFERRED_SCREEN_RANK = 4
MAX_SCREEN_RANK = 8
MAX_NORMAL_CHANNELS = 768
PREFERRED_W_PHASE_RAD = 0.10
CONDITIONAL_W_PHASE_RAD = 0.25


class LocalizedGateError(RuntimeError):
    """The localized-screen evidence is incomplete or violates its contract."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LocalizedGateError(f"cannot read screen manifest {path}: {error}") from error
    if payload.get("schema") != SOURCE_SCHEMA:
        raise LocalizedGateError(f"screen manifest must use schema {SOURCE_SCHEMA}")
    states = payload.get("states")
    crop_shape = payload.get("crop_shape")
    if not isinstance(states, list) or not states:
        raise LocalizedGateError("screen manifest states must be a non-empty list")
    if (
        not isinstance(crop_shape, list)
        or len(crop_shape) != 2
        or crop_shape[0] != crop_shape[1]
        or not all(isinstance(value, int) and value > 0 for value in crop_shape)
    ):
        raise LocalizedGateError("screen manifest crop_shape must be a positive square")
    return payload


def resolve_artifact(
    manifest_path: pathlib.Path, manifest: dict[str, Any], key: str
) -> pathlib.Path:
    value = manifest.get(key)
    if not isinstance(value, str) or not value:
        raise LocalizedGateError(f"screen manifest lacks {key}")
    path = pathlib.Path(value)
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.is_file():
        raise LocalizedGateError(f"screen artifact does not exist: {path}")
    return path


def load_family(
    path: pathlib.Path, *, state_count: int, crop_side: int
) -> np.memmap:
    expected = state_count * crop_side * crop_side * np.dtype(np.complex64).itemsize
    if path.stat().st_size != expected:
        raise LocalizedGateError(
            f"{path} has {path.stat().st_size} bytes, expected exactly {expected}"
        )
    return np.memmap(
        path,
        dtype=np.complex64,
        mode="r",
        shape=(state_count, crop_side, crop_side),
    )


def facet_bounds(
    mask_min: tuple[int, int],
    mask_max: tuple[int, int],
    *,
    largest_scale: int,
    facet_side: int,
) -> dict[str, Any]:
    if any(high < low for low, high in zip(mask_min, mask_max, strict=True)):
        raise LocalizedGateError("mask bounds are inverted")
    mask_shape = tuple(
        high - low + 1 for low, high in zip(mask_min, mask_max, strict=True)
    )
    active_shape = tuple(side + 2 * largest_scale for side in mask_shape)
    if any(side > facet_side for side in active_shape):
        raise LocalizedGateError("scale-dilated mask does not fit the requested facet")
    center = tuple(
        (low + high) / 2.0 for low, high in zip(mask_min, mask_max, strict=True)
    )
    start = tuple(int(round(value - (facet_side - 1) / 2.0)) for value in center)
    end = tuple(value + facet_side - 1 for value in start)
    guard = tuple((facet_side - side) // 2 for side in active_shape)
    return {
        "mask_shape": list(mask_shape),
        "scale_dilated_shape": list(active_shape),
        "center_pixel": list(center),
        "start_pixel": list(start),
        "end_pixel": list(end),
        "guard_pixels": list(guard),
    }


def image_to_screen_coordinates(
    pixels: np.ndarray,
    *,
    image_reference_pixel: float,
    cell_arcsec: float,
    screen_reference_pixel: float,
    crop_start: int,
    screen_increment_rad: float,
) -> np.ndarray:
    if screen_increment_rad <= 0.0:
        raise LocalizedGateError("screen increment must be positive")
    angular_offset = (pixels - image_reference_pixel) * cell_arcsec * ARCSEC_TO_RAD
    return (
        screen_reference_pixel + angular_offset / screen_increment_rad - crop_start
    )


def bilinear_sample(family: np.ndarray, x: np.ndarray, y: np.ndarray) -> np.ndarray:
    if family.ndim != 3:
        raise LocalizedGateError("screen family must have state, y, x axes")
    side_y, side_x = family.shape[1:]
    x0 = np.floor(x).astype(np.int64)
    y0 = np.floor(y).astype(np.int64)
    if (
        np.any(x0 < 0)
        or np.any(y0 < 0)
        or np.any(x0 + 1 >= side_x)
        or np.any(y0 + 1 >= side_y)
    ):
        raise LocalizedGateError("facet lies outside the persisted screen crop")
    fx = (x - x0).astype(np.float64)
    fy = (y - y0).astype(np.float64)
    v00 = np.asarray(family[:, y0[:, None], x0[None, :]], dtype=np.complex128)
    v01 = np.asarray(family[:, y0[:, None], (x0 + 1)[None, :]], dtype=np.complex128)
    v10 = np.asarray(family[:, (y0 + 1)[:, None], x0[None, :]], dtype=np.complex128)
    v11 = np.asarray(
        family[:, (y0 + 1)[:, None], (x0 + 1)[None, :]], dtype=np.complex128
    )
    wx0 = 1.0 - fx
    wy0 = 1.0 - fy
    return (
        v00 * wy0[None, :, None] * wx0[None, None, :]
        + v01 * wy0[None, :, None] * fx[None, None, :]
        + v10 * fy[None, :, None] * wx0[None, None, :]
        + v11 * fy[None, :, None] * fx[None, None, :]
    )


def low_rank_approximations(
    family: np.ndarray, *, max_rank: int, equalize_state_energy: bool
) -> tuple[dict[str, Any], list[np.ndarray]]:
    values = family.reshape(family.shape[0], -1)
    state_scale = np.sqrt(np.sum(np.abs(values) ** 2, axis=1))
    if np.any(state_scale <= 0.0):
        raise LocalizedGateError("screen family has a zero-energy local state")
    fitted_values = (
        values / state_scale[:, None] if equalize_state_energy else values
    )
    gram = fitted_values @ fitted_values.conj().T
    eigenvalues, eigenvectors = np.linalg.eigh(gram)
    order = np.argsort(eigenvalues.real)[::-1]
    eigenvalues = np.maximum(eigenvalues[order].real, 0.0)
    eigenvectors = eigenvectors[:, order]
    approximations: list[np.ndarray] = []
    ranks: list[dict[str, Any]] = []
    for rank in range(1, min(max_rank, values.shape[0]) + 1):
        basis = eigenvectors[:, :rank]
        fitted_approximation = basis @ (basis.conj().T @ fitted_values)
        approximation = (
            fitted_approximation * state_scale[:, None]
            if equalize_state_energy
            else fitted_approximation
        ).reshape(family.shape)
        approximations.append(approximation)
        ranks.append({"rank": rank, **normalized_error(approximation, family)})
    return (
        {
            "states": int(values.shape[0]),
            "domain_pixels": int(values.shape[1]),
            "fit_weighting": (
                "equal-state-energy" if equalize_state_energy else "global-energy"
            ),
            "singular_values": [float(math.sqrt(value)) for value in eigenvalues],
            "ranks": ranks,
        },
        approximations,
    )


def normalized_error(candidate: np.ndarray, reference: np.ndarray) -> dict[str, Any]:
    error = candidate - reference
    reference_energy = np.sum(np.abs(reference) ** 2, axis=(1, 2))
    error_energy = np.sum(np.abs(error) ** 2, axis=(1, 2))
    total_reference = float(np.sum(reference_energy))
    if total_reference <= 0.0 or np.any(reference_energy <= 0.0):
        raise LocalizedGateError("reference family has a zero-energy state")
    state_error = error_energy / reference_energy
    worst_state = int(np.argmax(state_error))
    return {
        "relative_rms": float(
            math.sqrt(float(np.sum(error_energy)) / total_reference)
        ),
        "max_abs_error": float(np.max(np.abs(error))),
        "worst_state_relative_rms": float(math.sqrt(float(state_error[worst_state]))),
        "worst_state_index": worst_state,
    }


def passes_screen_gate(row: dict[str, Any]) -> bool:
    return (
        row["relative_rms"] <= SCREEN_RMS_LIMIT
        and row["max_abs_error"] <= SCREEN_MAX_LIMIT
        and row["worst_state_relative_rms"] <= SCREEN_WORST_STATE_RMS_LIMIT
    )


def passes_product_gate(row: dict[str, Any]) -> bool:
    return (
        row["relative_rms"] <= PRODUCT_RMS_LIMIT
        and row["max_abs_error"] <= PRODUCT_MAX_LIMIT
        and row["worst_state_relative_rms"] <= PRODUCT_WORST_STATE_RMS_LIMIT
    )


def minimum_passing_rank(metrics: dict[str, Any]) -> int | None:
    for row in metrics["ranks"]:
        if passes_screen_gate(row):
            return int(row["rank"])
    return None


def required_taylor_terms(max_phase_rad: float, limit: float) -> int:
    if max_phase_rad < 0.0 or not math.isfinite(max_phase_rad):
        raise LocalizedGateError("maximum phase must be finite and non-negative")
    if limit <= 0.0 or not math.isfinite(limit):
        raise LocalizedGateError("Taylor error limit must be finite and positive")
    term = 1.0
    for terms in range(1, 65):
        term *= max_phase_rad / terms
        if math.exp(max_phase_rad) * term <= limit:
            return terms
    raise LocalizedGateError("W Taylor rank exceeds the bounded search")


def reduce_gate(
    manifest_path: pathlib.Path,
    *,
    mask_min: tuple[int, int],
    mask_max: tuple[int, int],
    image_side: int,
    image_reference_pixel: float,
    cell_arcsec: float,
    largest_scale: int,
    facet_side: int,
    max_screen_rank: int,
    maximum_w_lambda: float,
) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    states = manifest["states"]
    crop_side = int(manifest["crop_shape"][0])
    paths = {
        role: resolve_artifact(manifest_path, manifest, f"{role}_path")
        for role in ("forward", "reverse", "normal")
    }
    families = {
        role: load_family(path, state_count=len(states), crop_side=crop_side)
        for role, path in paths.items()
    }
    bounds = facet_bounds(
        mask_min,
        mask_max,
        largest_scale=largest_scale,
        facet_side=facet_side,
    )
    start_x, start_y = bounds["start_pixel"]
    end_x, end_y = bounds["end_pixel"]
    x_pixels = np.arange(start_x, end_x + 1, dtype=np.float64)
    y_pixels = np.arange(start_y, end_y + 1, dtype=np.float64)
    increments = manifest.get("derived_sky_increment_rad")
    references = manifest.get("uv_reference_pixel")
    crop_starts = manifest.get("crop_start")
    if (
        not isinstance(increments, list)
        or len(increments) != 2
        or not isinstance(references, list)
        or len(references) != 2
        or not isinstance(crop_starts, list)
        or len(crop_starts) != 2
    ):
        raise LocalizedGateError("manifest lacks screen coordinate geometry")
    x_screen = image_to_screen_coordinates(
        x_pixels,
        image_reference_pixel=image_reference_pixel,
        cell_arcsec=cell_arcsec,
        screen_reference_pixel=float(references[0]),
        crop_start=int(crop_starts[0]),
        screen_increment_rad=abs(float(increments[0])),
    )
    y_screen = image_to_screen_coordinates(
        y_pixels,
        image_reference_pixel=image_reference_pixel,
        cell_arcsec=cell_arcsec,
        screen_reference_pixel=float(references[1]),
        crop_start=int(crop_starts[1]),
        screen_increment_rad=abs(float(increments[1])),
    )
    local = {
        role: bilinear_sample(family, x_screen, y_screen)
        for role, family in families.items()
    }
    fit_strategies = {
        "global-energy": False,
        "equal-state-energy": True,
    }
    forward_fits = {
        strategy: low_rank_approximations(
            local["forward"],
            max_rank=max_screen_rank,
            equalize_state_energy=equalize,
        )
        for strategy, equalize in fit_strategies.items()
    }
    reverse_fits = {
        strategy: low_rank_approximations(
            local["reverse"],
            max_rank=max_screen_rank,
            equalize_state_energy=equalize,
        )
        for strategy, equalize in fit_strategies.items()
    }
    forward_metrics = forward_fits["global-energy"][0]
    reverse_metrics = reverse_fits["global-energy"][0]
    forward_rank = min(
        (
            passing_rank
            for metrics, _ in forward_fits.values()
            if (passing_rank := minimum_passing_rank(metrics)) is not None
        ),
        default=None,
    )
    reverse_rank = min(
        (
            passing_rank
            for metrics, _ in reverse_fits.values()
            if (passing_rank := minimum_passing_rank(metrics)) is not None
        ),
        default=None,
    )

    exact_product = local["forward"] * np.conj(local["reverse"])
    product_source_consistency = normalized_error(local["normal"], exact_product)
    wrong_same_frequency = local["forward"] * np.conj(local["forward"])
    wrong_same_frequency_error = normalized_error(wrong_same_frequency, exact_product)

    product_curve: list[dict[str, Any]] = []
    for forward_strategy, (
        forward_fit_metrics,
        forward_approximations,
    ) in forward_fits.items():
        for reverse_strategy, (
            reverse_fit_metrics,
            reverse_approximations,
        ) in reverse_fits.items():
            for forward_index, forward_approximation in enumerate(
                forward_approximations, start=1
            ):
                forward_screen = forward_fit_metrics["ranks"][forward_index - 1]
                for reverse_index, reverse_approximation in enumerate(
                    reverse_approximations, start=1
                ):
                    reverse_screen = reverse_fit_metrics["ranks"][reverse_index - 1]
                    row = normalized_error(
                        forward_approximation * np.conj(reverse_approximation),
                        exact_product,
                    )
                    screen_gate_passed = passes_screen_gate(
                        forward_screen
                    ) and passes_screen_gate(reverse_screen)
                    product_curve.append(
                        {
                            "forward_strategy": forward_strategy,
                            "reverse_strategy": reverse_strategy,
                            "forward_rank": forward_index,
                            "reverse_rank": reverse_index,
                            "forward_screen": forward_screen,
                            "reverse_screen": reverse_screen,
                            **row,
                            "screen_gate_passed": screen_gate_passed,
                            "product_gate_passed": passes_product_gate(row),
                            "gate_passed": (
                                screen_gate_passed and passes_product_gate(row)
                            ),
                        }
                    )
    passing_products = [row for row in product_curve if row["gate_passed"]]
    passing_products.sort(
        key=lambda row: (
            row["forward_rank"] * row["reverse_rank"],
            row["forward_rank"] + row["reverse_rank"],
            row["forward_strategy"],
            row["reverse_strategy"],
            row["forward_rank"],
            row["reverse_rank"],
        )
    )
    minimum_product = passing_products[0] if passing_products else None

    gradient = np.zeros(local["forward"].shape, dtype=np.float64)
    for family in (local["forward"], local["reverse"]):
        gradient[:, :, 1:] = np.maximum(
            gradient[:, :, 1:], np.abs(np.diff(family, axis=2))
        )
        gradient[:, 1:, :] = np.maximum(
            gradient[:, 1:, :], np.abs(np.diff(family, axis=1))
        )
    gradient_state, gradient_y, gradient_x = np.unravel_index(
        int(np.argmax(gradient)), gradient.shape
    )

    corner_radius_rad = (
        facet_side / 2.0 * cell_arcsec * ARCSEC_TO_RAD * math.sqrt(2.0)
    )
    maximum_w_phase_rad = math.pi * maximum_w_lambda * corner_radius_rad**2
    w_rank = required_taylor_terms(maximum_w_phase_rad, W_COMPLEX_MAX_LIMIT)
    if maximum_w_phase_rad <= PREFERRED_W_PHASE_RAD:
        w_decision = "promote-local-w-compression"
    elif maximum_w_phase_rad <= CONDITIONAL_W_PHASE_RAD:
        w_decision = "conditional-local-w-compression"
    else:
        w_decision = "reject-local-w-compression"

    factorized_forward_rank = (
        int(minimum_product["forward_rank"]) if minimum_product else None
    )
    factorized_reverse_rank = (
        int(minimum_product["reverse_rank"]) if minimum_product else None
    )
    factorized_normal_channels = (
        3 * factorized_forward_rank * factorized_reverse_rank * w_rank * w_rank
        if factorized_forward_rank is not None
        and factorized_reverse_rank is not None
        else None
    )
    factorized_kernel_bytes = (
        factorized_normal_channels * facet_side * facet_side * 8
        if factorized_normal_channels is not None
        else None
    )
    paired_w_channels = w_rank * (w_rank + 1) // 2
    paired_normal_channels = 3 * len(states) * paired_w_channels
    paired_kernel_bytes = paired_normal_channels * facet_side * facet_side * 8
    paired_screen_texture_bytes = 2 * len(states) * facet_side * facet_side * 8
    paired_rhs_bytes = 2 * facet_side * facet_side * 8
    paired_compact_operator_bytes = (
        paired_kernel_bytes + paired_screen_texture_bytes + paired_rhs_bytes
    )
    screen_gate_passed = (
        forward_rank is not None
        and forward_rank <= MAX_SCREEN_RANK
        and reverse_rank is not None
        and reverse_rank <= MAX_SCREEN_RANK
    )
    wrong_path_is_sensitive = not passes_product_gate(wrong_same_frequency_error)
    factorized_state_gate_passed = (
        factorized_normal_channels is not None
        and factorized_normal_channels <= MAX_NORMAL_CHANNELS
    )
    paired_state_gate_passed = (
        paired_normal_channels <= MAX_NORMAL_CHANNELS
        and paired_compact_operator_bytes <= 151_257_904
    )
    factorized_promoted = (
        screen_gate_passed
        and minimum_product is not None
        and w_decision != "reject-local-w-compression"
        and factorized_state_gate_passed
        and wrong_path_is_sensitive
    )
    paired_state_promoted = (
        w_decision != "reject-local-w-compression"
        and paired_state_gate_passed
        and wrong_path_is_sensitive
    )
    promoted = factorized_promoted or paired_state_promoted

    return {
        "schema": SCHEMA,
        "role": "production-inert-architecture-discriminator",
        "source_manifest": str(manifest_path),
        "source_manifest_sha256": sha256_file(manifest_path),
        "source_artifact_sha256": {
            role: sha256_file(path) for role, path in paths.items()
        },
        "geometry": {
            "image_shape": [image_side, image_side],
            "image_reference_pixel": image_reference_pixel,
            "cell_arcsec": cell_arcsec,
            "mask_min_pixel": list(mask_min),
            "mask_max_pixel": list(mask_max),
            "largest_scale_pixels": largest_scale,
            "facet_side": facet_side,
            **bounds,
            "screen_x_range": [float(x_screen[0]), float(x_screen[-1])],
            "screen_y_range": [float(y_screen[0]), float(y_screen[-1])],
            "screen_interpolation": "bilinear-complex",
            "screen_source_sampling_arcsec": [
                float(abs(value) / ARCSEC_TO_RAD) for value in increments
            ],
        },
        "forward": {
            **forward_metrics,
            "minimum_passing_rank": forward_rank,
            "fit_strategies": {
                strategy: metrics for strategy, (metrics, _) in forward_fits.items()
            },
            "preferred_rank_ceiling": PREFERRED_SCREEN_RANK,
            "maximum_rank_ceiling": MAX_SCREEN_RANK,
        },
        "reverse": {
            **reverse_metrics,
            "minimum_passing_rank": reverse_rank,
            "fit_strategies": {
                strategy: metrics for strategy, (metrics, _) in reverse_fits.items()
            },
            "preferred_rank_ceiling": PREFERRED_SCREEN_RANK,
            "maximum_rank_ceiling": MAX_SCREEN_RANK,
        },
        "dual_screen_product": {
            "source_normal_consistency": product_source_consistency,
            "wrong_same_frequency_hermitian_error": wrong_same_frequency_error,
            "wrong_path_is_sensitive": wrong_path_is_sensitive,
            "minimum_passing_ranks": minimum_product,
            "rank_curve": product_curve,
            "limits": {
                "relative_rms": PRODUCT_RMS_LIMIT,
                "max_abs_error": PRODUCT_MAX_LIMIT,
                "worst_state_relative_rms": PRODUCT_WORST_STATE_RMS_LIMIT,
            },
        },
        "maximum_local_gradient": {
            "state_index": int(gradient_state),
            "image_pixel": [start_x + int(gradient_x), start_y + int(gradient_y)],
            "magnitude": float(gradient[gradient_state, gradient_y, gradient_x]),
        },
        "local_w": {
            "maximum_w_lambda": maximum_w_lambda,
            "bound_kind": "raw-w-conservative-before-exact-facet-rotation",
            "corner_radius_rad": corner_radius_rad,
            "maximum_phase_rad": maximum_w_phase_rad,
            "preferred_phase_limit_rad": PREFERRED_W_PHASE_RAD,
            "conditional_phase_limit_rad": CONDITIONAL_W_PHASE_RAD,
            "complex_error_limit": W_COMPLEX_MAX_LIMIT,
            "required_forward_rank": w_rank,
            "required_reverse_rank": w_rank,
            "decision": w_decision,
        },
        "factorized_screen_state": {
            "normal_channels": factorized_normal_channels,
            "normal_channel_limit": MAX_NORMAL_CHANNELS,
            "complex_f32_kernel_bytes": factorized_kernel_bytes,
            "gate_passed": factorized_state_gate_passed,
            "decision": (
                "retain-factorized-screen-normal"
                if factorized_promoted
                else "retire-factorized-screen-normal-for-channel-expansion"
            ),
        },
        "paired_state_normal": {
            "representation": (
                "exact forward/reverse state pairs with relative-W Taylor "
                "moments before separable expansion"
            ),
            "physical_screen_states": len(states),
            "taylor_psf_orders": 3,
            "w_taylor_terms": w_rank,
            "separable_w_channels_per_state_and_psf_order": paired_w_channels,
            "normal_channels": paired_normal_channels,
            "normal_channel_limit": MAX_NORMAL_CHANNELS,
            "complex_f32_kernel_bytes": paired_kernel_bytes,
            "exact_dual_screen_texture_bytes": paired_screen_texture_bytes,
            "rhs_bytes": paired_rhs_bytes,
            "compact_operator_bytes": paired_compact_operator_bytes,
            "compact_operator_byte_limit": 151_257_904,
            "remaining_bytes_for_rows_plans_and_metadata": (
                151_257_904 - paired_compact_operator_bytes
            ),
            "screen_factorization_error": 0.0,
            "gate_passed": paired_state_gate_passed,
            "decision": (
                "promote-paired-state-normal-to-row-discriminator"
                if paired_state_promoted
                else "reject-paired-state-normal"
            ),
        },
        "gate": {
            "screen_gate_passed": screen_gate_passed,
            "product_gate_passed": minimum_product is not None,
            "w_gate_passed": w_decision != "reject-local-w-compression",
            "factorized_state_gate_passed": factorized_state_gate_passed,
            "paired_state_gate_passed": paired_state_gate_passed,
            "wrong_path_sensitivity_passed": wrong_path_is_sensitive,
            "factorized_screen_path_promoted": factorized_promoted,
            "paired_state_path_promoted": paired_state_promoted,
            "promoted_to_row_operator_discriminator": promoted,
        },
        "limitations": [
            "screen values are bilinearly sampled from CASA's persisted pre-W screen lattice",
            "the raw-w bound is conservative and does not replace exact facet-frame UVW rotation",
            "paired-state kernel byte counts exclude row, plan, coefficient, and FFT-plan bytes",
            "this gate does not measure visibility prediction, restricted Hm, IDG occupancy, products, or time",
        ],
        "recommendation": (
            "build-frozen-paired-state-row-operator-discriminator"
            if paired_state_promoted
            else (
                "build-frozen-factorized-screen-row-operator-discriminator"
                if factorized_promoted
                else "reject-or-revise-localized-dual-screen-representation"
            )
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--mask-min", default="575,2125")
    parser.add_argument("--mask-max", default="638,2188")
    parser.add_argument("--image-side", type=int, default=4096)
    parser.add_argument("--image-reference-pixel", type=float, default=2048.0)
    parser.add_argument("--cell-arcsec", type=float, default=0.6)
    parser.add_argument("--largest-scale", type=int, default=12)
    parser.add_argument("--facet-side", type=int, default=128)
    parser.add_argument("--max-screen-rank", type=int, default=8)
    parser.add_argument("--maximum-w-lambda", type=float, required=True)
    args = parser.parse_args()

    def pair(value: str, label: str) -> tuple[int, int]:
        try:
            items = tuple(int(item) for item in value.split(","))
        except ValueError as error:
            raise SystemExit(f"{label} must contain two comma-separated integers") from error
        if len(items) != 2:
            raise SystemExit(f"{label} must contain two comma-separated integers")
        return items

    if args.image_side <= 0 or args.facet_side <= 0 or args.max_screen_rank <= 0:
        raise SystemExit("image, facet, and rank values must be positive")
    try:
        receipt = reduce_gate(
            args.manifest,
            mask_min=pair(args.mask_min, "--mask-min"),
            mask_max=pair(args.mask_max, "--mask-max"),
            image_side=args.image_side,
            image_reference_pixel=args.image_reference_pixel,
            cell_arcsec=args.cell_arcsec,
            largest_scale=args.largest_scale,
            facet_side=args.facet_side,
            max_screen_rank=args.max_screen_rank,
            maximum_w_lambda=args.maximum_w_lambda,
        )
    except LocalizedGateError as error:
        raise SystemExit(f"VLASS localized dual-screen gate: {error}") from error
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
