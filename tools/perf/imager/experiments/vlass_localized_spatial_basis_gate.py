#!/usr/bin/env python3
"""Gate a state-count-independent spatial basis for the VLASS local operator.

The real row census has 54 imaging/prediction CF-state pairs.  Materializing
one normal channel family per pair is still compact but exceeds the provisional
channel ceiling.  This reducer fits every exact forward/reverse A-screen with
the same low-order spatial polynomial basis.  Row-state coefficients can then
be contracted into normal kernels during the first pass, so resident channel
count depends on spatial basis size rather than frequency-pair count.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-localized-spatial-basis-gate/v1"
ROW_CONTRACT_SCHEMA = "casa-rs-vlass-localized-row-census-contract/v3"
SCREEN_RMS_LIMIT = 2.0e-5
SCREEN_MAX_LIMIT = 2.0e-4
SCREEN_WORST_STATE_RMS_LIMIT = 6.0e-5
PAIR_RMS_LIMIT = 1.0e-5
PAIR_MAX_LIMIT = 1.0e-4
PAIR_WORST_STATE_RMS_LIMIT = 3.0e-5
NORMAL_CHANNEL_LIMIT = 768
COMPACT_STATE_LIMIT = 151_257_904
W_CHANNELS = 6
TAYLOR_PSF_ORDERS = 3


class SpatialBasisGateError(RuntimeError):
    """The exact screen/row-pair evidence violates the gate contract."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SpatialBasisGateError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise SpatialBasisGateError(f"JSON root must be an object: {path}")
    return value


def load_localized_gate_module() -> Any:
    path = pathlib.Path(__file__).with_name("vlass_localized_dual_screen_gate.py")
    spec = importlib.util.spec_from_file_location("vlass_localized_gate", path)
    if spec is None or spec.loader is None:
        raise SpatialBasisGateError(f"cannot load localized gate module {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def total_degree_exponents(degree: int) -> list[tuple[int, int]]:
    if degree < 0:
        raise SpatialBasisGateError("polynomial degree must be non-negative")
    return [
        (x_degree, total_degree - x_degree)
        for total_degree in range(degree + 1)
        for x_degree in range(total_degree + 1)
    ]


def spatial_basis(side: int, degree: int) -> tuple[np.ndarray, list[tuple[int, int]]]:
    if side <= 0:
        raise SpatialBasisGateError("facet side must be positive")
    coordinates = np.linspace(-1.0, 1.0, side, dtype=np.float64)
    x, y = np.meshgrid(coordinates, coordinates)
    exponents = total_degree_exponents(degree)
    basis = np.stack(
        [(x**x_degree * y**y_degree).reshape(-1) for x_degree, y_degree in exponents],
        axis=1,
    )
    return basis, exponents


def fit_spatial_family(
    family: np.ndarray, degree: int
) -> tuple[np.ndarray, np.ndarray, list[tuple[int, int]]]:
    if family.ndim != 3 or family.shape[1] != family.shape[2]:
        raise SpatialBasisGateError("screen family must have state x square-y x square-x")
    basis, exponents = spatial_basis(family.shape[1], degree)
    values = family.reshape(family.shape[0], -1).T.astype(np.complex128)
    coefficients, _, _, _ = np.linalg.lstsq(basis, values, rcond=None)
    approximation = (basis @ coefficients).T.reshape(family.shape)
    return approximation, coefficients.T, exponents


def normalized_error(candidate: np.ndarray, reference: np.ndarray) -> dict[str, Any]:
    if candidate.shape != reference.shape or candidate.ndim < 2:
        raise SpatialBasisGateError("candidate/reference shapes differ")
    axes = tuple(range(1, reference.ndim))
    error_energy = np.sum(np.abs(candidate - reference) ** 2, axis=axes)
    reference_energy = np.sum(np.abs(reference) ** 2, axis=axes)
    if np.any(reference_energy <= 0.0):
        raise SpatialBasisGateError("reference contains a zero-energy state")
    state_relative = np.sqrt(error_energy / reference_energy)
    worst_state = int(np.argmax(state_relative))
    return {
        "relative_rms": float(
            np.sqrt(np.sum(error_energy) / np.sum(reference_energy))
        ),
        "max_abs_error": float(np.max(np.abs(candidate - reference))),
        "worst_state_relative_rms": float(state_relative[worst_state]),
        "worst_state_index": worst_state,
    }


def state_index(
    states: list[dict[str, Any]], frequency_hz: float, mueller_element: int
) -> int:
    matches = [
        index
        for index, state in enumerate(states)
        if float(state["frequency_hz"]) == frequency_hz
        and int(state["mueller_element"]) == mueller_element
    ]
    if len(matches) != 1:
        raise SpatialBasisGateError(
            f"expected one screen state for {frequency_hz} Hz Mueller {mueller_element}"
        )
    return matches[0]


def pair_error(
    exact_grid: np.ndarray,
    exact_prediction: np.ndarray,
    approximate_grid: np.ndarray,
    approximate_prediction: np.ndarray,
) -> dict[str, Any]:
    exact = exact_grid[:, :, None] * np.conj(exact_prediction[:, None, :])
    approximate = approximate_grid[:, :, None] * np.conj(
        approximate_prediction[:, None, :]
    )
    return normalized_error(approximate, exact)


def sample_positions(side: int, count_per_axis: int = 16) -> np.ndarray:
    if count_per_axis <= 1 or count_per_axis > side:
        raise SpatialBasisGateError("invalid cross-screen sample count")
    indices = np.rint(np.linspace(0, side - 1, count_per_axis)).astype(np.int64)
    y, x = np.meshgrid(indices, indices, indexing="ij")
    return (y * side + x).reshape(-1)


def passes_screen(error: dict[str, Any]) -> bool:
    return (
        error["relative_rms"] <= SCREEN_RMS_LIMIT
        and error["max_abs_error"] <= SCREEN_MAX_LIMIT
        and error["worst_state_relative_rms"] <= SCREEN_WORST_STATE_RMS_LIMIT
    )


def passes_pair(error: dict[str, Any]) -> bool:
    return (
        error["relative_rms"] <= PAIR_RMS_LIMIT
        and error["max_abs_error"] <= PAIR_MAX_LIMIT
        and error["worst_state_relative_rms"] <= PAIR_WORST_STATE_RMS_LIMIT
    )


def reduce_gate(
    screen_manifest_path: pathlib.Path,
    row_contract_path: pathlib.Path,
    *,
    max_degree: int,
    mask_min: tuple[int, int] = (575, 2125),
    mask_max: tuple[int, int] = (638, 2188),
    image_side: int = 4096,
    image_reference_pixel: float = 2047.5,
    cell_arcsec: float = 0.6,
    largest_scale: int = 12,
    facet_side: int = 128,
) -> dict[str, Any]:
    gate = load_localized_gate_module()
    screen_manifest = gate.load_manifest(screen_manifest_path)
    row_contract = load_json(row_contract_path)
    if row_contract.get("schema") != ROW_CONTRACT_SCHEMA:
        raise SpatialBasisGateError(
            f"row contract must use {ROW_CONTRACT_SCHEMA}"
        )
    states = screen_manifest["states"]
    crop_side = int(screen_manifest["crop_shape"][0])
    paths = {
        role: gate.resolve_artifact(
            screen_manifest_path, screen_manifest, f"{role}_path"
        )
        for role in ("forward", "reverse")
    }
    families = {
        role: gate.load_family(path, state_count=len(states), crop_side=crop_side)
        for role, path in paths.items()
    }
    bounds = gate.facet_bounds(
        mask_min,
        mask_max,
        largest_scale=largest_scale,
        facet_side=facet_side,
    )
    start_x, start_y = bounds["start_pixel"]
    end_x, end_y = bounds["end_pixel"]
    x_pixels = np.arange(start_x, end_x + 1, dtype=np.float64)
    y_pixels = np.arange(start_y, end_y + 1, dtype=np.float64)
    increments = screen_manifest["derived_sky_increment_rad"]
    references = screen_manifest["uv_reference_pixel"]
    crop_starts = screen_manifest["crop_start"]
    x_screen = gate.image_to_screen_coordinates(
        x_pixels,
        image_reference_pixel=image_reference_pixel,
        cell_arcsec=cell_arcsec,
        screen_reference_pixel=float(references[0]),
        crop_start=int(crop_starts[0]),
        screen_increment_rad=abs(float(increments[0])),
    )
    y_screen = gate.image_to_screen_coordinates(
        y_pixels,
        image_reference_pixel=image_reference_pixel,
        cell_arcsec=cell_arcsec,
        screen_reference_pixel=float(references[1]),
        crop_start=int(crop_starts[1]),
        screen_increment_rad=abs(float(increments[1])),
    )
    local = {
        role: gate.bilinear_sample(family, x_screen, y_screen)
        for role, family in families.items()
    }

    screen_selection = row_contract.get("aw_screen_selection")
    if not isinstance(screen_selection, dict):
        raise SpatialBasisGateError("row contract lacks aw_screen_selection")
    pairs = screen_selection.get("imaging_prediction_state_pairs")
    if not isinstance(pairs, list) or not pairs:
        raise SpatialBasisGateError("row contract lacks imaging/prediction pairs")
    pair_indices = [
        (
            state_index(
                states,
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            ),
            state_index(
                states,
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            ),
        )
        for pair in pairs
    ]
    selected_positions = sample_positions(facet_side)
    exact_forward = local["forward"].reshape(len(states), -1)[:, selected_positions]
    exact_reverse = local["reverse"].reshape(len(states), -1)[:, selected_positions]

    degree_rows: list[dict[str, Any]] = []
    for degree in range(max_degree + 1):
        approximate_forward, forward_coefficients, exponents = fit_spatial_family(
            local["forward"], degree
        )
        approximate_reverse, reverse_coefficients, _ = fit_spatial_family(
            local["reverse"], degree
        )
        forward_error = normalized_error(approximate_forward, local["forward"])
        reverse_error = normalized_error(approximate_reverse, local["reverse"])
        approximate_forward_sample = approximate_forward.reshape(len(states), -1)[
            :, selected_positions
        ]
        approximate_reverse_sample = approximate_reverse.reshape(len(states), -1)[
            :, selected_positions
        ]
        pair_candidates = []
        pair_references = []
        for imaging_index, prediction_index in pair_indices:
            # Check both physical orientations.  The row-level discriminator
            # will select the authoritative orientation; a shared basis must
            # satisfy either without a new fit.
            for exact_grid, exact_prediction, approximate_grid, approximate_prediction in [
                (
                    exact_reverse[imaging_index],
                    exact_forward[prediction_index],
                    approximate_reverse_sample[imaging_index],
                    approximate_forward_sample[prediction_index],
                ),
                (
                    exact_forward[imaging_index],
                    exact_reverse[prediction_index],
                    approximate_forward_sample[imaging_index],
                    approximate_reverse_sample[prediction_index],
                ),
            ]:
                pair_references.append(
                    exact_grid[:, None] * np.conj(exact_prediction[None, :])
                )
                pair_candidates.append(
                    approximate_grid[:, None]
                    * np.conj(approximate_prediction[None, :])
                )
        pair_error_row = normalized_error(
            np.stack(pair_candidates), np.stack(pair_references)
        )
        basis_terms = len(exponents)
        normal_channels = (
            basis_terms * basis_terms * W_CHANNELS * TAYLOR_PSF_ORDERS
        )
        kernel_bytes = normal_channels * facet_side * facet_side * 8
        coefficient_bytes = (
            forward_coefficients.size + reverse_coefficients.size
        ) * np.dtype(np.complex64).itemsize
        rhs_bytes = 2 * facet_side * facet_side * 8
        compact_bytes = kernel_bytes + coefficient_bytes + rhs_bytes
        gate_passed = (
            passes_screen(forward_error)
            and passes_screen(reverse_error)
            and passes_pair(pair_error_row)
            and normal_channels <= NORMAL_CHANNEL_LIMIT
            and compact_bytes <= COMPACT_STATE_LIMIT
        )
        degree_rows.append(
            {
                "degree": degree,
                "basis_terms": basis_terms,
                "exponents": [list(value) for value in exponents],
                "forward_error": forward_error,
                "reverse_error": reverse_error,
                "sampled_two_position_pair_error": pair_error_row,
                "normal_channels": normal_channels,
                "normal_channel_limit": NORMAL_CHANNEL_LIMIT,
                "kernel_bytes": kernel_bytes,
                "coefficient_bytes": coefficient_bytes,
                "rhs_bytes": rhs_bytes,
                "compact_bytes": compact_bytes,
                "compact_state_limit": COMPACT_STATE_LIMIT,
                "gate_passed": gate_passed,
            }
        )

    passing = [row for row in degree_rows if row["gate_passed"]]
    selected = min(passing, key=lambda row: row["degree"]) if passing else None
    # Adversarially erase spectral state from the best available degree.  A
    # useful screen-only precursor must notice the resulting two-position
    # error before the row-level TT1/alpha probe.
    adversarial_degree = selected["degree"] if selected else max_degree
    approximate_forward, _, _ = fit_spatial_family(
        local["forward"], adversarial_degree
    )
    approximate_reverse, _, _ = fit_spatial_family(
        local["reverse"], adversarial_degree
    )
    collapsed_forward = np.broadcast_to(
        np.mean(approximate_forward, axis=0, keepdims=True),
        approximate_forward.shape,
    )
    collapsed_reverse = np.broadcast_to(
        np.mean(approximate_reverse, axis=0, keepdims=True),
        approximate_reverse.shape,
    )
    collapsed_forward_sample = collapsed_forward.reshape(len(states), -1)[
        :, selected_positions
    ]
    collapsed_reverse_sample = collapsed_reverse.reshape(len(states), -1)[
        :, selected_positions
    ]
    collapsed_candidates = []
    collapsed_references = []
    for imaging_index, prediction_index in pair_indices:
        collapsed_references.append(
            exact_reverse[imaging_index, :, None]
            * np.conj(exact_forward[prediction_index, None, :])
        )
        collapsed_candidates.append(
            collapsed_reverse_sample[imaging_index, :, None]
            * np.conj(collapsed_forward_sample[prediction_index, None, :])
        )
    collapsed_error = normalized_error(
        np.stack(collapsed_candidates), np.stack(collapsed_references)
    )
    wrong_path_sensitive = not passes_pair(collapsed_error)

    return {
        "schema": SCHEMA,
        "role": "production-inert-spatial-basis-and-state-contraction-discriminator",
        "sources": {
            "screen_manifest": str(screen_manifest_path),
            "screen_manifest_sha256": sha256_file(screen_manifest_path),
            "row_contract": str(row_contract_path),
            "row_contract_sha256": sha256_file(row_contract_path),
            "screen_artifact_sha256": {
                role: sha256_file(path) for role, path in paths.items()
            },
        },
        "geometry": {
            "image_side": image_side,
            "image_reference_pixel": image_reference_pixel,
            "cell_arcsec": cell_arcsec,
            "facet_side": facet_side,
            **bounds,
        },
        "row_state_pairs": len(pair_indices),
        "two_position_sample_positions": int(selected_positions.size),
        "degree_rows": degree_rows,
        "selected_degree": selected["degree"] if selected else None,
        "adversarial_frequency_collapsed_error": collapsed_error,
        "wrong_path_is_sensitive": wrong_path_sensitive,
        "decision": (
            "promote-spatial-basis-to-real-row-contraction-race"
            if selected is not None and wrong_path_sensitive
            else "retire-spatial-polynomial-contraction"
        ),
        "limits": {
            "screen_relative_rms": SCREEN_RMS_LIMIT,
            "screen_max_abs_error": SCREEN_MAX_LIMIT,
            "screen_worst_state_relative_rms": SCREEN_WORST_STATE_RMS_LIMIT,
            "pair_relative_rms": PAIR_RMS_LIMIT,
            "pair_max_abs_error": PAIR_MAX_LIMIT,
            "pair_worst_state_relative_rms": PAIR_WORST_STATE_RMS_LIMIT,
            "normal_channels": NORMAL_CHANNEL_LIMIT,
            "compact_bytes": COMPACT_STATE_LIMIT,
        },
        "claim_boundary": (
            "screen and sampled two-position pair precursor only; does not claim "
            "row contraction, NUFFT/gridding, Hm, finalizer, products, or timing"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--screen-manifest", required=True, type=pathlib.Path)
    parser.add_argument("--row-contract", required=True, type=pathlib.Path)
    parser.add_argument("--max-degree", type=int, default=4)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise SpatialBasisGateError(f"refusing to overwrite {args.output}")
    payload = reduce_gate(
        args.screen_manifest, args.row_contract, max_degree=args.max_degree
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
