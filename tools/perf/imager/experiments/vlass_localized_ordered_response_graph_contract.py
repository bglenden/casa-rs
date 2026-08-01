#!/usr/bin/env python3
"""Certify the contracted VLASS localized ordered-response execution graph.

This is a production-inert algebra and liveness discriminator.  It proves that
the six binomial applications from a total-order-two residual-W expansion can
share three translation-invariant kernels per ordered AW state pair and
MT-MFS response moment.  It does not claim that any particular irregular-UV
gridding or NUFFT construction meets the scientific or timing gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pathlib
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-localized-ordered-response-graph-contract/v1"
SOURCE_SCHEMA = "casa-rs-vlass-localized-row-census-contract/v3"
EXPECTED_PAIR_COUNT = 54
EXPECTED_IMAGING_STATES = 28
EXPECTED_PREDICTION_STATES = 32
W_ORDERS = 3
W_BINOMIAL_ACTIONS = 6
RESPONSE_MOMENTS = 3
MODEL_TERMS = 2
OUTPUT_TERMS = 2
ETA_POWERS = 3
ACTIVE_SIDE = 88
FACET_SIDE = 128
EMBEDDING_SIDE = 192
COMPLEX_F32_BYTES = np.dtype(np.complex64).itemsize
MAJOR_OPERATOR_APPLICATIONS = 11
CELL_ARCSEC = 0.6
GRID_OVERSAMPLING = 100
GRID_SUPPORT_WIDTH = 7
DIRECT_EQUIVALENCE_LIMIT = 2.0e-12
ADVERSARIAL_RATIO_MIN = 10.0
ROW_DTYPE = np.dtype(
    [
        ("uvw_lambda", "<f8", (3,)),
        ("frequency_hz", "<f8"),
        ("beam_frequency_hz", "<f8"),
        ("pointing_direction_rad", "<f8", (2,)),
        ("pointing_pixel", "<f8", (2,)),
        ("weight", "<f4"),
        ("sumwt_factor", "<f4"),
        ("scalar_visibility", "<c8"),
        ("first_visibility", "<c8"),
        ("second_visibility", "<c8"),
        ("source_phase", "<c8"),
        ("first_prediction_normalization", "<c8"),
        ("second_prediction_normalization", "<c8"),
        ("replay_block_ordinal", "<u4"),
        ("window_ordinal", "<u4"),
        ("sample_index", "<u4"),
        ("group_index", "<u4"),
    ],
    align=False,
)


class GraphContractError(RuntimeError):
    """The source contract or derived execution graph is inconsistent."""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GraphContractError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(payload, dict):
        raise GraphContractError(f"JSON root must be an object: {path}")
    return payload


def state_key(frequency_hz: float, mueller_element: int) -> tuple[int, int]:
    """Return an integer-stable key for a persisted AW screen state."""

    return (int(round(frequency_hz)), int(mueller_element))


def nearest_indices(values: np.ndarray, candidates: np.ndarray) -> np.ndarray:
    """Select persisted frequencies with the frozen left-tie rule."""

    positions = np.searchsorted(candidates, values)
    right = np.minimum(positions, candidates.size - 1)
    left = np.maximum(positions - 1, 0)
    choose_right = np.abs(values - candidates[right]) < np.abs(
        values - candidates[left]
    )
    return np.where(choose_right, right, left)


def source_pairs(contract: dict[str, Any]) -> list[dict[str, Any]]:
    if contract.get("schema") != SOURCE_SCHEMA:
        raise GraphContractError(f"source contract must use {SOURCE_SCHEMA}")
    state_universe = contract.get("ordered_response_state_universe")
    if state_universe is None:
        selection = contract.get("aw_screen_selection")
        if not isinstance(selection, dict):
            raise GraphContractError("source contract lacks aw_screen_selection")
        pairs = selection.get("imaging_prediction_state_pairs")
    else:
        if not isinstance(state_universe, dict):
            raise GraphContractError(
                "ordered_response_state_universe must be an object"
            )
        source_path = pathlib.Path(
            str(state_universe.get("source_contract", ""))
        )
        if not source_path.is_absolute():
            raise GraphContractError(
                "ordered-response state-universe source must be absolute"
            )
        if sha256_file(source_path) != state_universe.get(
            "source_contract_sha256"
        ):
            raise GraphContractError(
                "ordered-response state-universe source SHA-256 differs"
            )
        universe_contract = load_json(source_path)
        if universe_contract.get("schema") != SOURCE_SCHEMA:
            raise GraphContractError(
                f"state-universe source contract must use {SOURCE_SCHEMA}"
            )
        universe_sources = universe_contract.get("sources")
        universe_selection = universe_contract.get("aw_screen_selection")
        if not isinstance(universe_sources, dict) or not isinstance(
            universe_selection, dict
        ):
            raise GraphContractError(
                "state-universe source lacks sources or aw_screen_selection"
            )
        if universe_sources.get(
            "screen_manifest_sha256"
        ) != state_universe.get("source_screen_manifest_sha256"):
            raise GraphContractError(
                "state-universe source screen-manifest SHA-256 differs"
            )
        pairs = state_universe.get("imaging_prediction_state_pairs")
        if pairs != universe_selection.get("imaging_prediction_state_pairs"):
            raise GraphContractError(
                "resident ordered AW pairs differ from their source contract"
            )
    if not isinstance(pairs, list) or len(pairs) != EXPECTED_PAIR_COUNT:
        observed = len(pairs) if isinstance(pairs, list) else "non-list"
        raise GraphContractError(
            f"expected {EXPECTED_PAIR_COUNT} ordered AW pairs, got {observed}"
        )
    return pairs


def graph_dimensions(contract: dict[str, Any]) -> dict[str, int]:
    pairs = source_pairs(contract)
    imaging_states = {
        state_key(
            float(pair["imaging_frequency_hz"]),
            int(pair["imaging_mueller_element"]),
        )
        for pair in pairs
    }
    prediction_states = {
        state_key(
            float(pair["prediction_frequency_hz"]),
            int(pair["prediction_mueller_element"]),
        )
        for pair in pairs
    }
    if len(imaging_states) != EXPECTED_IMAGING_STATES:
        raise GraphContractError(
            f"expected {EXPECTED_IMAGING_STATES} imaging states, "
            f"got {len(imaging_states)}"
        )
    if len(prediction_states) != EXPECTED_PREDICTION_STATES:
        raise GraphContractError(
            f"expected {EXPECTED_PREDICTION_STATES} prediction states, "
            f"got {len(prediction_states)}"
        )
    minimum_embedding_side = 2 * ACTIVE_SIDE - 1
    if EMBEDDING_SIDE < minimum_embedding_side:
        raise GraphContractError(
            f"{EMBEDDING_SIDE}-square embedding aliases "
            f"{minimum_embedding_side}-pixel linear support"
        )
    response_kernels = len(pairs) * W_ORDERS * RESPONSE_MOMENTS
    stored_binomial_moment_channels = (
        len(pairs) * W_BINOMIAL_ACTIONS * RESPONSE_MOMENTS
    )
    mixing_actions_per_hm = (
        len(pairs) * W_BINOMIAL_ACTIONS * MODEL_TERMS * OUTPUT_TERMS
    )
    mixing_complex_macs_per_hm = mixing_actions_per_hm * (
        EMBEDDING_SIDE * EMBEDDING_SIDE
    )
    right_transforms = len(prediction_states) * ETA_POWERS * MODEL_TERMS
    left_transforms = len(imaging_states) * ETA_POWERS * OUTPUT_TERMS
    transforms_per_hm = right_transforms + left_transforms
    rhs_grids = len(imaging_states) * W_ORDERS * OUTPUT_TERMS
    return {
        "ordered_aw_pairs": len(pairs),
        "imaging_states": len(imaging_states),
        "prediction_states": len(prediction_states),
        "minimum_embedding_side": minimum_embedding_side,
        "embedding_side": EMBEDDING_SIDE,
        "response_kernels": response_kernels,
        "stored_binomial_moment_channels": stored_binomial_moment_channels,
        "mixing_actions_per_hm": mixing_actions_per_hm,
        "mixing_actions_for_major_trajectory": (
            MAJOR_OPERATOR_APPLICATIONS * mixing_actions_per_hm
        ),
        "mixing_complex_macs_per_hm": mixing_complex_macs_per_hm,
        "mixing_complex_macs_for_major_trajectory": (
            MAJOR_OPERATOR_APPLICATIONS * mixing_complex_macs_per_hm
        ),
        "right_transforms_per_hm": right_transforms,
        "left_transforms_per_hm": left_transforms,
        "transforms_per_hm": transforms_per_hm,
        "transforms_for_major_trajectory": (
            MAJOR_OPERATOR_APPLICATIONS * transforms_per_hm
        ),
        "rhs_grids": rhs_grids,
    }


def construction_work_census(
    contract: dict[str, Any], source_path: pathlib.Path
) -> dict[str, Any]:
    """Measure route locality for the frozen simple-kernel construction."""

    pairs = source_pairs(contract)
    sources = contract.get("sources")
    rows_contract = contract.get("rows")
    if not isinstance(sources, dict) or not isinstance(rows_contract, dict):
        raise GraphContractError("source contract lacks rows or sources")
    if ROW_DTYPE.itemsize != int(rows_contract.get("record_bytes", -1)):
        raise GraphContractError("localized row layout is not the frozen 128 bytes")
    row_path = pathlib.Path(str(sources.get("row_payload", "")))
    if not row_path.is_absolute():
        row_path = source_path.parent / row_path
    if sha256_file(row_path) != sources.get("row_payload_sha256"):
        raise GraphContractError("localized row payload SHA-256 differs")
    row_count = int(rows_contract.get("count", -1))
    rows = np.memmap(row_path, dtype=ROW_DTYPE, mode="r", shape=(row_count,))

    row_manifest_path = pathlib.Path(str(sources.get("row_manifest", "")))
    if not row_manifest_path.is_absolute():
        row_manifest_path = source_path.parent / row_manifest_path
    if sha256_file(row_manifest_path) != sources.get("row_manifest_sha256"):
        raise GraphContractError("localized row manifest SHA-256 differs")
    row_manifest = load_json(row_manifest_path)
    row_manifest_contract = row_manifest.get("contract")
    if not isinstance(row_manifest_contract, dict):
        raise GraphContractError("localized row manifest lacks contract")
    reference_frequency_hz = float(
        row_manifest_contract["reference_frequency_hz"]
    )

    selection = contract["aw_screen_selection"]
    persisted_frequencies = np.asarray(
        selection["persisted_cf_frequencies_hz"], dtype=np.float64
    )
    sample_frequencies = np.asarray(rows["frequency_hz"], dtype=np.float64)
    imaging_frequency_indices = nearest_indices(
        np.sqrt(
            2.0 * reference_frequency_hz * reference_frequency_hz
            - sample_frequencies * sample_frequencies
        ),
        persisted_frequencies,
    )
    prediction_frequency_indices = nearest_indices(
        sample_frequencies, persisted_frequencies
    )
    imaging_frequencies = persisted_frequencies[imaging_frequency_indices]
    prediction_frequencies = persisted_frequencies[prediction_frequency_indices]
    positive_w = np.asarray(rows["uvw_lambda"][:, 2] > 0.0)
    pair_index = {
        (
            state_key(
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            ),
            state_key(
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            ),
        ): index
        for index, pair in enumerate(pairs)
    }

    cell_rad = CELL_ARCSEC * math.pi / (180.0 * 3600.0)
    grid_spacing_lambda = 1.0 / (EMBEDDING_SIDE * cell_rad)
    position_x = (
        np.asarray(rows["uvw_lambda"][:, 0], dtype=np.float64)
        / grid_spacing_lambda
        + EMBEDDING_SIDE / 2
    )
    position_y = (
        -np.asarray(rows["uvw_lambda"][:, 1], dtype=np.float64)
        / grid_spacing_lambda
        + EMBEDDING_SIDE / 2
    )
    location_x = np.rint(position_x).astype(np.int16)
    location_y = np.rint(position_y).astype(np.int16)
    offset_x = np.rint(
        (location_x - position_x) * GRID_OVERSAMPLING
    ).astype(np.int16)
    offset_y = np.rint(
        (location_y - position_y) * GRID_OVERSAMPLING
    ).astype(np.int16)

    routed_pair_indices: list[np.ndarray] = []
    for base_mueller in (0, 15):
        imaging_mueller = np.where(
            positive_w, base_mueller, 15 - base_mueller
        )
        prediction_mueller = np.where(
            positive_w, 15 - base_mueller, base_mueller
        )
        routed_pair_indices.append(
            np.fromiter(
                (
                    pair_index[
                        (
                            state_key(imaging_frequency, imaging_mueller_element),
                            state_key(
                                prediction_frequency,
                                prediction_mueller_element,
                            ),
                        )
                    ]
                    for (
                        imaging_frequency,
                        imaging_mueller_element,
                        prediction_frequency,
                        prediction_mueller_element,
                    ) in zip(
                        imaging_frequencies,
                        imaging_mueller,
                        prediction_frequencies,
                        prediction_mueller,
                        strict=True,
                    )
                ),
                dtype=np.int16,
                count=row_count,
            )
        )
    routed_pairs = np.concatenate(routed_pair_indices)
    routed_location_x = np.tile(location_x, 2)
    routed_location_y = np.tile(location_y, 2)
    routed_offset_x = np.tile(offset_x, 2)
    routed_offset_y = np.tile(offset_y, 2)
    exact_route_keys = np.rec.fromarrays(
        [
            routed_pairs,
            routed_location_x,
            routed_location_y,
            routed_offset_x,
            routed_offset_y,
        ],
        names="pair,x,y,offset_x,offset_y",
    )
    center_bucket_keys = np.rec.fromarrays(
        [routed_pairs, routed_location_x, routed_location_y],
        names="pair,x,y",
    )
    _, exact_route_counts = np.unique(exact_route_keys, return_counts=True)
    _, center_bucket_counts = np.unique(center_bucket_keys, return_counts=True)
    pair_route_counts = np.bincount(
        routed_pairs, minlength=EXPECTED_PAIR_COUNT
    )
    support_radius = GRID_SUPPORT_WIDTH // 2
    outside = (
        (location_x < support_radius)
        | (location_y < support_radius)
        | (location_x >= EMBEDDING_SIDE - support_radius)
        | (location_y >= EMBEDDING_SIDE - support_radius)
    )
    hand_routes = int(routed_pairs.size)
    exact_routes = int(exact_route_counts.size)
    unique_pointing_pixels = np.unique(
        np.asarray(rows["pointing_pixel"], dtype=np.float64), axis=0
    )
    unique_pointing_directions = np.unique(
        np.asarray(rows["pointing_direction_rad"], dtype=np.float64), axis=0
    )
    response_coefficients_per_hand_route = W_ORDERS * RESPONSE_MOMENTS
    rhs_coefficients_per_hand_route = W_ORDERS * OUTPUT_TERMS
    tap_cells = GRID_SUPPORT_WIDTH * GRID_SUPPORT_WIDTH

    def count_summary(counts: np.ndarray) -> dict[str, float | int]:
        return {
            "p50": float(np.percentile(counts, 50)),
            "p90": float(np.percentile(counts, 90)),
            "p99": float(np.percentile(counts, 99)),
            "maximum": int(counts.max()),
        }

    return {
        "row_payload": str(row_path),
        "row_payload_sha256": sources["row_payload_sha256"],
        "physical_rows": row_count,
        "parallel_hand_routes": hand_routes,
        "unique_pointing_pixels": int(unique_pointing_pixels.shape[0]),
        "unique_pointing_directions": int(unique_pointing_directions.shape[0]),
        "unique_visibility_frequencies": int(
            np.unique(sample_frequencies).size
        ),
        "unique_pb_grouping_frequencies": int(
            np.unique(np.asarray(rows["beam_frequency_hz"])).size
        ),
        "cell_arcsec": CELL_ARCSEC,
        "grid_spacing_lambda": grid_spacing_lambda,
        "grid_oversampling": GRID_OVERSAMPLING,
        "grid_support_width": GRID_SUPPORT_WIDTH,
        "route_location_bounds": {
            "x": [int(location_x.min()), int(location_x.max())],
            "y": [int(location_y.min()), int(location_y.max())],
            "outside_support_safe_embedding": int(np.count_nonzero(outside)),
        },
        "identical_pair_cell_subpixel_routes": {
            "unique": exact_routes,
            "coalescing_ratio": hand_routes / exact_routes,
            "multiplicity": count_summary(exact_route_counts),
            "semantics": (
                "exact coalescing within the selected 100x oversampled "
                "7x7 gridding discretization"
            ),
        },
        "pair_cell_scheduling_buckets": {
            "unique": int(center_bucket_counts.size),
            "routes_per_bucket": count_summary(center_bucket_counts),
            "semantics": (
                "gather/tile scheduling locality only; subpixel states remain "
                "distinct and may not be merged"
            ),
        },
        "routes_per_ordered_pair": {
            "minimum": int(pair_route_counts.min()),
            "p50": float(np.median(pair_route_counts)),
            "maximum": int(pair_route_counts.max()),
        },
        "construction_work": {
            "response_coefficient_deposits": (
                hand_routes * response_coefficients_per_hand_route
            ),
            "rhs_coefficient_deposits": (
                hand_routes * rhs_coefficients_per_hand_route
            ),
            "combined_coefficient_deposits": (
                hand_routes
                * (
                    response_coefficients_per_hand_route
                    + rhs_coefficients_per_hand_route
                )
            ),
            "response_literal_tap_updates": (
                hand_routes * response_coefficients_per_hand_route * tap_cells
            ),
            "rhs_literal_tap_updates": (
                hand_routes * rhs_coefficients_per_hand_route * tap_cells
            ),
            "combined_literal_tap_updates": (
                hand_routes
                * (
                    response_coefficients_per_hand_route
                    + rhs_coefficients_per_hand_route
                )
                * tap_cells
            ),
            "combined_subpixel_coalesced_tap_updates": (
                exact_routes
                * (
                    response_coefficients_per_hand_route
                    + rhs_coefficients_per_hand_route
                )
                * tap_cells
            ),
        },
    }


def linear_convolution_fft(kernel: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Apply K[x-x'] through an alias-free circulant embedding."""

    if kernel.ndim != 1 or values.ndim != 1:
        raise GraphContractError("synthetic convolution inputs must be vectors")
    side = values.size
    if kernel.size != 2 * side - 1:
        raise GraphContractError("synthetic kernel must cover every signed separation")
    embedding_side = 1 << (2 * side - 2).bit_length()
    kernel_embedding = np.zeros(embedding_side, dtype=np.complex128)
    value_embedding = np.zeros(embedding_side, dtype=np.complex128)
    value_embedding[:side] = values
    separations = np.arange(-(side - 1), side)
    kernel_embedding[separations % embedding_side] = kernel
    transformed = np.fft.ifft(
        np.fft.fft(kernel_embedding) * np.fft.fft(value_embedding)
    )
    return transformed[:side]


def synthetic_equivalence(
    pairs: list[dict[str, Any]], *, seed: int = 0x564C4153
) -> dict[str, float]:
    """Compare literal six-action and contracted ordered-response graphs."""

    rng = np.random.default_rng(seed)
    side = 5
    positions = np.linspace(-0.85, 0.9, side, dtype=np.float64)
    eta = -0.017 * positions * positions - 0.003 * positions
    imaging_keys = sorted(
        {
            state_key(
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            )
            for pair in pairs
        }
    )
    prediction_keys = sorted(
        {
            state_key(
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            )
            for pair in pairs
        }
    )
    imaging_index = {key: index for index, key in enumerate(imaging_keys)}
    prediction_index = {key: index for index, key in enumerate(prediction_keys)}
    pair_indices = np.array(
        [
            [
                imaging_index[
                    state_key(
                        float(pair["imaging_frequency_hz"]),
                        int(pair["imaging_mueller_element"]),
                    )
                ],
                prediction_index[
                    state_key(
                        float(pair["prediction_frequency_hz"]),
                        int(pair["prediction_mueller_element"]),
                    )
                ],
            ]
            for pair in pairs
        ],
        dtype=np.int64,
    )
    wrong_prediction_indices = np.array(
        [
            prediction_index.get(
                state_key(
                    float(pair["imaging_frequency_hz"]),
                    int(pair["prediction_mueller_element"]),
                ),
                pair_indices[pair_index, 1],
            )
            for pair_index, pair in enumerate(pairs)
        ],
        dtype=np.int64,
    )
    left_screens = (
        rng.normal(size=(len(imaging_keys), side))
        + 1j * rng.normal(size=(len(imaging_keys), side))
    )
    right_screens = (
        rng.normal(size=(len(prediction_keys), side))
        + 1j * rng.normal(size=(len(prediction_keys), side))
    )
    model = rng.normal(size=(MODEL_TERMS, side))
    model = model + 1j * rng.normal(size=model.shape)

    rows_per_pair = 3
    row_count = len(pairs) * rows_per_pair
    row_pair = np.repeat(np.arange(len(pairs)), rows_per_pair)
    row_u = rng.uniform(-0.45, 0.45, size=row_count)
    row_w = rng.uniform(-2.0, 2.0, size=row_count)
    row_tau = rng.uniform(-0.8, 0.9, size=row_count)
    row_beam_tau = 0.31 * row_tau + 0.23
    row_weight = rng.uniform(0.2, 1.5, size=row_count)
    separations = np.arange(-(side - 1), side, dtype=np.float64)
    phase = np.exp(
        1j
        * math.tau
        * row_u[:, None]
        * separations[None, :]
        / float(side)
    )

    def build_kernels(taylor_coordinate: np.ndarray) -> np.ndarray:
        kernels = np.zeros(
            (len(pairs), W_ORDERS, RESPONSE_MOMENTS, 2 * side - 1),
            dtype=np.complex128,
        )
        for row_index in range(row_count):
            pair_index = row_pair[row_index]
            iw = 1j * math.tau * row_w[row_index]
            for order in range(W_ORDERS):
                w_coefficient = iw**order / math.factorial(order)
                for moment in range(RESPONSE_MOMENTS):
                    kernels[pair_index, order, moment] += (
                        row_weight[row_index]
                        * w_coefficient
                        * taylor_coordinate[row_index] ** moment
                        * phase[row_index]
                    )
        return kernels

    kernels = build_kernels(row_tau)
    wrong_taylor_kernels = build_kernels(row_beam_tau)

    def apply_literal(
        kernel_bank: np.ndarray, prediction_for_pair: np.ndarray
    ) -> np.ndarray:
        output = np.zeros((OUTPUT_TERMS, side), dtype=np.complex128)
        for pair_index, (left_index, _right_index) in enumerate(pair_indices):
            right_index = prediction_for_pair[pair_index]
            for output_term in range(OUTPUT_TERMS):
                for model_term in range(MODEL_TERMS):
                    moment = output_term + model_term
                    source = right_screens[right_index] * model[model_term]
                    for order in range(W_ORDERS):
                        for left_power in range(order + 1):
                            right_power = order - left_power
                            coefficient = math.comb(order, left_power)
                            values = ((-eta) ** right_power) * source
                            convolved = linear_convolution_fft(
                                kernel_bank[pair_index, order, moment], values
                            )
                            output[output_term] += (
                                left_screens[left_index]
                                * eta**left_power
                                * coefficient
                                * convolved
                            )
        return output

    def apply_contracted(kernel_bank: np.ndarray) -> np.ndarray:
        right_actions: dict[tuple[int, int, int], np.ndarray] = {}
        for right_index in range(len(prediction_keys)):
            for right_power in range(ETA_POWERS):
                for model_term in range(MODEL_TERMS):
                    right_actions[(right_index, right_power, model_term)] = (
                        (-eta) ** right_power
                        * right_screens[right_index]
                        * model[model_term]
                    )
        mixed = {
            (left_index, left_power, output_term): np.zeros(
                side, dtype=np.complex128
            )
            for left_index in range(len(imaging_keys))
            for left_power in range(ETA_POWERS)
            for output_term in range(OUTPUT_TERMS)
        }
        for pair_index, (left_index, right_index) in enumerate(pair_indices):
            for output_term in range(OUTPUT_TERMS):
                for model_term in range(MODEL_TERMS):
                    moment = output_term + model_term
                    for order in range(W_ORDERS):
                        for left_power in range(order + 1):
                            right_power = order - left_power
                            values = right_actions[
                                (right_index, right_power, model_term)
                            ]
                            mixed[(left_index, left_power, output_term)] += (
                                math.comb(order, left_power)
                                * linear_convolution_fft(
                                    kernel_bank[pair_index, order, moment],
                                    values,
                                )
                            )
        output = np.zeros((OUTPUT_TERMS, side), dtype=np.complex128)
        for (
            left_index,
            left_power,
            output_term,
        ), values in mixed.items():
            output[output_term] += (
                left_screens[left_index] * eta**left_power * values
            )
        return output

    literal = apply_literal(kernels, pair_indices[:, 1])
    contracted = apply_contracted(kernels)
    wrong_pair = apply_literal(kernels, wrong_prediction_indices)
    wrong_taylor = apply_literal(wrong_taylor_kernels, pair_indices[:, 1])
    reference_norm = float(np.linalg.norm(literal))
    if reference_norm <= 0.0:
        raise GraphContractError("synthetic reference has zero norm")
    candidate_error = float(np.linalg.norm(contracted - literal) / reference_norm)
    wrong_pair_error = float(np.linalg.norm(wrong_pair - literal) / reference_norm)
    wrong_taylor_error = float(
        np.linalg.norm(wrong_taylor - literal) / reference_norm
    )
    denominator = max(candidate_error, np.finfo(np.float64).eps)
    return {
        "contracted_relative_error": candidate_error,
        "wrong_same_frequency_pair_relative_error": wrong_pair_error,
        "wrong_taylor_coordinate_relative_error": wrong_taylor_error,
        "wrong_pair_error_ratio": wrong_pair_error / denominator,
        "wrong_taylor_error_ratio": wrong_taylor_error / denominator,
    }


def derive_contract(source_path: pathlib.Path) -> dict[str, Any]:
    source = load_json(source_path)
    pairs = source_pairs(source)
    dimensions = graph_dimensions(source)
    construction = construction_work_census(source, source_path)
    equivalence = synthetic_equivalence(pairs)
    if equivalence["contracted_relative_error"] > DIRECT_EQUIVALENCE_LIMIT:
        raise GraphContractError(
            "contracted graph did not match the literal six-action graph"
        )
    if (
        equivalence["wrong_pair_error_ratio"] < ADVERSARIAL_RATIO_MIN
        or equivalence["wrong_taylor_error_ratio"] < ADVERSARIAL_RATIO_MIN
    ):
        raise GraphContractError("an adversarial semantic control was insensitive")

    cells = EMBEDDING_SIDE * EMBEDDING_SIDE
    response_kernel_bytes = (
        dimensions["response_kernels"] * cells * COMPLEX_F32_BYTES
    )
    rhs_grid_bytes = dimensions["rhs_grids"] * cells * COMPLEX_F32_BYTES
    dual_screen_bytes = (
        2
        * EXPECTED_PREDICTION_STATES
        * FACET_SIDE
        * FACET_SIDE
        * COMPLEX_F32_BYTES
    )
    reduced_rhs_bytes = OUTPUT_TERMS * cells * COMPLEX_F32_BYTES
    accounted_resident_payload = (
        response_kernel_bytes
        + rhs_grid_bytes
        + dual_screen_bytes
        + reduced_rhs_bytes
    )
    return {
        "schema": SCHEMA,
        "role": "production-inert-algebra-liveness-and-execution-graph-certificate",
        "source": {
            "contract": str(source_path),
            "contract_sha256": sha256_file(source_path),
        },
        "classification": {
            "operator": (
                "ordered local G*WA response; Hermitian and positive-semidefinite "
                "properties are not assumed"
            ),
            "kernel_sharing": "proven exact for total-order-two W polynomial",
            "w_polynomial": "controlled approximation requiring row-level science gate",
            "irregular_uv_construction": (
                "not certified here; NUFFT or gridding discretization must be "
                "measured separately"
            ),
            "timing": "not measured by this certificate",
        },
        "required_next_proofs": [
            (
                "tiny full-matrix direct-f64 row response versus explicit "
                "Toeplitz contraction"
            ),
            "Toeplitz embedding and FFT normalization",
            "ordered complex-f32 Metal response from identical prebuilt kernels",
            "controlled irregular-UV construction error",
            (
                "all row-dependent POINTING, Mueller, parallactic-angle, "
                "antenna, and illumination state is represented"
            ),
            "exact discrete scale support remains inside the 88-square domain",
        ],
        "forbidden_without_separate_proof": [
            "Hermitian kernel completion",
            "half-spectrum or real FFTs",
            "pair-reversal collapse",
            "CG or Cholesky",
            "positive-semidefinite assumptions",
            "real projection before the complete hand sum",
        ],
        "graph": dimensions,
        "construction_census": construction,
        "memory": {
            "complex_f32_bytes": COMPLEX_F32_BYTES,
            "response_kernel_bytes": response_kernel_bytes,
            "rhs_grid_bytes": rhs_grid_bytes,
            "dual_screen_bytes": dual_screen_bytes,
            "reduced_rhs_bytes": reduced_rhs_bytes,
            "accounted_resident_payload_bytes": accounted_resident_payload,
            "projected_runtime_peak_range_bytes": [
                int(0.6 * 1024**3),
                int(1.3 * 1024**3),
            ],
            "projection_basis": (
                "persistent response, screen, right/left FFT batches plus "
                "reused 256-384-square irregular-UV workspaces"
            ),
            "unaccounted_until_runtime": [
                "FFT workspaces",
                "Metal hidden allocations",
                "row staging",
                "command buffers",
                "pair-mixing scratch",
            ],
        },
        "synthetic_equivalence": equivalence,
        "timing_gates_seconds": {
            "construction_breakthrough": 1.50,
            "eleven_hm_breakthrough": 0.85,
            "combined_breakthrough": 2.35,
            "eleven_hm_architecturally_viable": 1.25,
            "combined_full_row_worthy": 3.20,
            "eleven_hm_kill_without_finalizer_evidence": 1.75,
            "combined_kill_without_finalizer_evidence": 3.70,
            "dynamic_requirement": (
                "construction + eleven_hm <= 6.785 - finalizer_p90 "
                "- integration_p90"
            ),
        },
        "decision": (
            "pass-algebra-and-liveness-only; proceed to a real-row construction "
            "and eleven-Hm timing discriminator without claiming promotion"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-contract", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise GraphContractError(f"refusing to overwrite {args.output}")
    payload = derive_contract(args.source_contract)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        "decision={decision} kernels={kernels} transforms_per_hm={transforms} "
        "mixing_actions_per_hm={mixing} trajectory_transforms={trajectory} "
        "resident_payload_bytes={resident}".format(
            decision=payload["decision"],
            kernels=payload["graph"]["response_kernels"],
            transforms=payload["graph"]["transforms_per_hm"],
            mixing=payload["graph"]["mixing_actions_per_hm"],
            trajectory=payload["graph"]["transforms_for_major_trajectory"],
            resident=payload["memory"]["accounted_resident_payload_bytes"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
