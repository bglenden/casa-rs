#!/usr/bin/env python3
"""Bind the localized VLASS ordered response to physical CASA A-term screens.

This production-inert discriminator applies a deterministic three-atom
multiscale model to every observed imaging/prediction state-pair stratum in the
frozen 4096-square, full-16-SPW row payload.  It compares the total-order-two
localized W contraction against an independent direct-f64 exact-W row
evaluation for pure TT0 and pure TT1 models.

CASA's AW visibility convolution function contains the forward A-term.  The
forward times conjugate-frequency reverse product belongs to the weight CF, not
to visibility prediction or gridding.  The response evaluated here is
therefore ``conj(A_imaging(x)) * A_prediction(x')``.  The persisted reverse
screens are still hash-bound and checked against CASA's weight-screen product
so a later product finalizer cannot silently substitute a same-frequency
screen.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import pathlib
import sys
import time
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-ordered-response-physical-semantic-gate/v3"
GRAPH_SCRIPT = "vlass_localized_ordered_response_graph_contract.py"
SCREEN_SCHEMA = "casa-rs-vlass-evla-pre-w-screens/v2"
IMAGE_SIDE = 4096
IMAGE_REFERENCE_PIXEL = IMAGE_SIDE / 2.0
CELL_ARCSEC = 0.6
ARCSEC_TO_RAD = math.pi / (180.0 * 3600.0)
CELL_RAD = CELL_ARCSEC * ARCSEC_TO_RAD
MASK_MIN = (575, 2125)
MASK_MAX = (638, 2188)
ACTIVE_MIN = (563, 2113)
ACTIVE_MAX = (650, 2200)
FACET_MIN = (543, 2093)
FACET_MAX = (670, 2220)
FACET_CENTER = (
    (FACET_MIN[0] + FACET_MAX[0]) / 2.0,
    (FACET_MIN[1] + FACET_MAX[1]) / 2.0,
)
EMBEDDING_SIDE = 192
FACET_EMBEDDING_MARGIN = (EMBEDDING_SIDE - 128) // 2
EMBEDDING_ORIGIN = (
    FACET_MIN[0] - FACET_EMBEDDING_MARGIN,
    FACET_MIN[1] - FACET_EMBEDDING_MARGIN,
)
SCALE12_CENTER = MASK_MIN
SCALE5_CENTER = MASK_MAX
ORDERED_PAIRS = 54
W_ORDERS = 3
MODEL_TERMS = 2
ROW_CHUNK = 4096
W_RELATIVE_L2_LIMIT = 2.0e-5
W_NORMALIZED_LINF_LIMIT = 1.0e-4
WORST_PAIR_RELATIVE_L2_LIMIT = 1.0e-4
CONTROL_RATIO_MIN = 10.0


class PhysicalSemanticGateError(RuntimeError):
    """The frozen physical-response contract is incomplete or fails its gate."""


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
        raise PhysicalSemanticGateError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise PhysicalSemanticGateError(f"{path} must contain a JSON object")
    return value


def load_graph_module() -> Any:
    path = pathlib.Path(__file__).with_name(GRAPH_SCRIPT)
    spec = importlib.util.spec_from_file_location("ordered_response_graph", path)
    if spec is None or spec.loader is None:
        raise PhysicalSemanticGateError(f"cannot load graph contract module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def resolve_artifact(
    manifest_path: pathlib.Path,
    manifest: dict[str, Any],
    key: str,
) -> pathlib.Path:
    value = manifest.get(key)
    if not isinstance(value, str) or not value:
        raise PhysicalSemanticGateError(f"screen manifest lacks {key}")
    path = pathlib.Path(value)
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.is_file():
        raise PhysicalSemanticGateError(f"screen artifact does not exist: {path}")
    return path


def load_screen_family(
    path: pathlib.Path,
    *,
    state_count: int,
    crop_side: int,
) -> np.memmap:
    expected_bytes = (
        state_count * crop_side * crop_side * np.dtype(np.complex64).itemsize
    )
    if path.stat().st_size != expected_bytes:
        raise PhysicalSemanticGateError(
            f"{path} has {path.stat().st_size} bytes, expected {expected_bytes}"
        )
    return np.memmap(
        path,
        dtype=np.complex64,
        mode="r",
        shape=(state_count, crop_side, crop_side),
    )


def multiscale_spheroidal(nu: np.float32) -> np.float32:
    """Match the f32 arithmetic boundaries in casa-rs/CASA MatrixCleaner."""

    nu = np.float32(nu)
    if nu <= np.float32(0.0):
        return np.float32(1.0)
    if nu >= np.float32(1.0):
        return np.float32(0.0)
    if nu < np.float32(0.75):
        p = np.asarray(
            [
                8.203343e-2,
                -3.644705e-1,
                6.27866e-1,
                -5.335581e-1,
                2.312756e-1,
            ],
            dtype=np.float32,
        )
        q = np.asarray([1.0, 8.212018e-1, 2.078043e-1], dtype=np.float32)
        nu_end = np.float32(0.75)
    else:
        p = np.asarray(
            [
                4.028559e-3,
                -3.697768e-2,
                1.021332e-1,
                -1.201436e-1,
                6.412774e-2,
            ],
            dtype=np.float32,
        )
        q = np.asarray([1.0, 9.599102e-1, 2.918724e-1], dtype=np.float32)
        nu_end = np.float32(1.0)
    delta = np.float32(np.float32(nu ** np.float32(2.0)) - nu_end ** np.float32(2.0))
    numerator = np.float32(p[0])
    for power, coefficient in enumerate(p[1:], start=1):
        numerator = np.float32(
            numerator + np.float32(coefficient * delta ** np.float32(power))
        )
    denominator = np.float32(q[0])
    for power, coefficient in enumerate(q[1:], start=1):
        denominator = np.float32(
            denominator + np.float32(coefficient * delta ** np.float32(power))
        )
    if denominator == np.float32(0.0):
        return np.float32(0.0)
    return np.float32(numerator / denominator)


def compact_multiscale_atom(
    center: tuple[int, int],
    scale: int,
    amplitude: np.float32,
) -> dict[tuple[int, int], np.float32]:
    if scale <= 0:
        return {center: np.float32(amplitude)}
    side = 2 * scale + 1
    reference = side // 2
    values: dict[tuple[int, int], np.float32] = {}
    volume = np.float32(0.0)
    for local_y in range(side):
        y_part = np.float32(((float(reference - local_y) / float(scale)) ** 2))
        for local_x in range(side):
            radius_squared = np.float32(
                float(y_part) + (float(reference - local_x) / float(scale)) ** 2
            )
            if radius_squared >= np.float32(1.0):
                continue
            radius = (
                np.float32(0.0)
                if radius_squared <= np.float32(0.0)
                else np.float32(np.sqrt(radius_squared))
            )
            value = np.float32(
                (1.0 - float(radius_squared)) * float(multiscale_spheroidal(radius))
            )
            pixel = (
                center[0] + local_x - reference,
                center[1] + local_y - reference,
            )
            values[pixel] = value
            volume = np.float32(volume + value)
    if volume <= np.float32(0.0):
        raise PhysicalSemanticGateError(f"scale {scale} produced zero volume")
    return {
        pixel: np.float32(amplitude * np.float32(value / volume))
        for pixel, value in values.items()
    }


def merge_atoms(
    atoms: list[dict[tuple[int, int], np.float32]],
) -> tuple[np.ndarray, np.ndarray]:
    merged: dict[tuple[int, int], np.float32] = {}
    for atom in atoms:
        for pixel, value in atom.items():
            merged[pixel] = np.float32(merged.get(pixel, np.float32(0.0)) + value)
    positions = np.asarray(sorted(merged), dtype=np.int32)
    values = np.asarray([merged[tuple(pixel)] for pixel in positions], dtype=np.float64)
    return positions, values


def active_support_pixels() -> np.ndarray:
    """Return the exact scale-12-expanded deterministic CLEAN support."""

    scale_offsets = compact_multiscale_atom(
        (0, 0),
        12,
        np.float32(1.0),
    )
    active = {
        (mask_x + offset_x, mask_y + offset_y)
        for mask_y in range(MASK_MIN[1], MASK_MAX[1] + 1)
        for mask_x in range(MASK_MIN[0], MASK_MAX[0] + 1)
        for offset_x, offset_y in scale_offsets
    }
    return np.asarray(sorted(active), dtype=np.int32)


def complex_array_payload(values: np.ndarray) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.complex128)
    flattened = values.reshape(-1)
    return {
        "shape": list(values.shape),
        "values": [[float(value.real), float(value.imag)] for value in flattened],
    }


def direct_pixel_coordinates(pixels: np.ndarray) -> tuple[np.ndarray, ...]:
    x = np.asarray(pixels[:, 0], dtype=np.float64)
    y = np.asarray(pixels[:, 1], dtype=np.float64)
    direction_l = (x - IMAGE_REFERENCE_PIXEL) * CELL_RAD
    m = (IMAGE_REFERENCE_PIXEL - y) * CELL_RAD
    radial = direction_l * direction_l + m * m
    if np.any(radial >= 1.0):
        raise PhysicalSemanticGateError(
            "a diagnostic pixel lies outside the SIN domain"
        )
    eta = np.sqrt(1.0 - radial) - 1.0
    return direction_l, m, eta


def facet_basis() -> np.ndarray:
    center = np.asarray([FACET_CENTER], dtype=np.float64)
    center_l, center_m, center_eta = direct_pixel_coordinates(center)
    center_n = 1.0 + center_eta[0]
    tangent_norm = math.hypot(center_n, center_l[0])
    tangent_l = np.asarray(
        [center_n / tangent_norm, 0.0, -center_l[0] / tangent_norm],
        dtype=np.float64,
    )
    tangent_m = np.asarray(
        [
            -center_l[0] * center_m[0] / tangent_norm,
            tangent_norm,
            -center_n * center_m[0] / tangent_norm,
        ],
        dtype=np.float64,
    )
    normal = np.asarray([center_l[0], center_m[0], center_n], dtype=np.float64)
    basis = np.column_stack([tangent_l, tangent_m, normal])
    np.testing.assert_allclose(basis.T @ basis, np.eye(3), rtol=0.0, atol=2.0e-15)
    return basis


def facet_pixel_coordinates(pixels: np.ndarray) -> tuple[np.ndarray, ...]:
    direction_l, direction_m, eta = direct_pixel_coordinates(pixels)
    directions = np.column_stack([direction_l, direction_m, 1.0 + eta])
    local = directions @ facet_basis()
    return local[:, 0], local[:, 1], local[:, 2] - 1.0


def rotate_uvw_to_facet(uvw: np.ndarray) -> np.ndarray:
    uvw = np.asarray(uvw, dtype=np.float64)
    if uvw.ndim != 2 or uvw.shape[1] != 3:
        raise PhysicalSemanticGateError("UVW must have shape [rows,3]")
    return uvw @ facet_basis()


def screen_coordinates(
    pixels: np.ndarray,
    *,
    pointing_pixel: np.ndarray,
    manifest: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray]:
    references = manifest.get("uv_reference_pixel")
    crop_start = manifest.get("crop_start")
    increments = manifest.get("derived_sky_increment_rad")
    if not (
        isinstance(references, list)
        and len(references) == 2
        and isinstance(crop_start, list)
        and len(crop_start) == 2
        and isinstance(increments, list)
        and len(increments) == 2
    ):
        raise PhysicalSemanticGateError("screen manifest lacks coordinate geometry")
    pixels = np.asarray(pixels, dtype=np.float64)
    pointing_pixel = np.asarray(pointing_pixel, dtype=np.float64)
    x = (
        float(references[0])
        + (pixels[:, 0] - pointing_pixel[0]) * CELL_RAD / abs(float(increments[0]))
        - int(crop_start[0])
    )
    y = (
        float(references[1])
        + (pixels[:, 1] - pointing_pixel[1]) * CELL_RAD / abs(float(increments[1]))
        - int(crop_start[1])
    )
    return x, y


def bilinear_sample_points(
    family: np.ndarray,
    x: np.ndarray,
    y: np.ndarray,
) -> np.ndarray:
    if family.ndim != 3 or x.shape != y.shape:
        raise PhysicalSemanticGateError("invalid screen sampling shapes")
    x0 = np.floor(x).astype(np.int64)
    y0 = np.floor(y).astype(np.int64)
    side_y, side_x = family.shape[1:]
    if (
        np.any(x0 < 0)
        or np.any(y0 < 0)
        or np.any(x0 + 1 >= side_x)
        or np.any(y0 + 1 >= side_y)
    ):
        raise PhysicalSemanticGateError("diagnostic pixels escape the screen crop")
    fx = x - x0
    fy = y - y0
    v00 = np.asarray(family[:, y0, x0], dtype=np.complex128)
    v01 = np.asarray(family[:, y0, x0 + 1], dtype=np.complex128)
    v10 = np.asarray(family[:, y0 + 1, x0], dtype=np.complex128)
    v11 = np.asarray(family[:, y0 + 1, x0 + 1], dtype=np.complex128)
    return (
        v00 * (1.0 - fy)[None, :] * (1.0 - fx)[None, :]
        + v01 * (1.0 - fy)[None, :] * fx[None, :]
        + v10 * fy[None, :] * (1.0 - fx)[None, :]
        + v11 * fy[None, :] * fx[None, :]
    )


def maximum_active_gradient(
    forward: np.ndarray,
    *,
    pointing_pixel: np.ndarray,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    x_values = np.arange(ACTIVE_MIN[0], ACTIVE_MAX[0] + 1, dtype=np.int32)
    y_values = np.arange(ACTIVE_MIN[1], ACTIVE_MAX[1] + 1, dtype=np.int32)
    y_grid, x_grid = np.meshgrid(y_values, x_values, indexing="ij")
    pixels = np.column_stack([x_grid.ravel(), y_grid.ravel()])
    screen_x, screen_y = screen_coordinates(
        pixels,
        pointing_pixel=pointing_pixel,
        manifest=manifest,
    )
    sampled = bilinear_sample_points(forward, screen_x, screen_y).reshape(
        forward.shape[0], y_values.size, x_values.size
    )
    gradient = np.zeros(sampled.shape, dtype=np.float64)
    gradient[:, :, 1:] = np.maximum(
        gradient[:, :, 1:], np.abs(np.diff(sampled, axis=2))
    )
    gradient[:, 1:, :] = np.maximum(
        gradient[:, 1:, :], np.abs(np.diff(sampled, axis=1))
    )
    state, y_index, x_index = np.unravel_index(int(np.argmax(gradient)), gradient.shape)
    return {
        "state_index": int(state),
        "image_pixel": [
            int(x_values[x_index]),
            int(y_values[y_index]),
        ],
        "magnitude": float(gradient[state, y_index, x_index]),
    }


def output_probe_pixels(point_pixel: tuple[int, int]) -> np.ndarray:
    x_values = np.linspace(ACTIVE_MIN[0], ACTIVE_MAX[0], 5, dtype=np.int32)
    y_values = np.linspace(ACTIVE_MIN[1], ACTIVE_MAX[1], 5, dtype=np.int32)
    probes = {(int(x), int(y)) for x in x_values for y in y_values}
    probes.update(
        {
            SCALE12_CENTER,
            SCALE5_CENTER,
            point_pixel,
            MASK_MIN,
            MASK_MAX,
            ACTIVE_MIN,
            ACTIVE_MAX,
            (ACTIVE_MIN[0], ACTIVE_MAX[1]),
            (ACTIVE_MAX[0], ACTIVE_MIN[1]),
        }
    )
    return np.asarray(sorted(probes), dtype=np.int32)


def state_and_pair_routes(
    source: dict[str, Any],
    rows: np.ndarray,
    graph: Any,
    screen_manifest: dict[str, Any],
) -> dict[str, np.ndarray]:
    pairs = graph.source_pairs(source)
    if len(pairs) != ORDERED_PAIRS:
        raise PhysicalSemanticGateError(
            f"expected {ORDERED_PAIRS} ordered pairs, got {len(pairs)}"
        )
    state_by_key = {
        graph.state_key(
            float(state["frequency_hz"]),
            int(state["mueller_element"]),
        ): int(state["index"])
        for state in screen_manifest["states"]
    }
    if len(state_by_key) != len(screen_manifest["states"]):
        raise PhysicalSemanticGateError("screen state keys are not unique")
    pair_by_keys = {
        (
            graph.state_key(
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            ),
            graph.state_key(
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            ),
        ): index
        for index, pair in enumerate(pairs)
    }
    persisted = np.asarray(
        source["aw_screen_selection"]["persisted_cf_frequencies_hz"],
        dtype=np.float64,
    )
    reference = float(
        load_json(pathlib.Path(source["sources"]["row_manifest"]))["contract"][
            "reference_frequency_hz"
        ]
    )
    frequency = np.asarray(rows["frequency_hz"], dtype=np.float64)
    imaging_frequency = persisted[
        graph.nearest_indices(
            np.sqrt(2.0 * reference * reference - frequency * frequency),
            persisted,
        )
    ]
    prediction_frequency = persisted[graph.nearest_indices(frequency, persisted)]
    positive_w = np.asarray(rows["uvw_lambda"][:, 2] > 0.0)
    hand = np.tile(np.arange(2, dtype=np.int8), rows.size)
    route_positive_w = np.repeat(positive_w, 2)
    imaging_mueller_index = np.where(route_positive_w, hand, 1 - hand)
    prediction_mueller_index = 1 - imaging_mueller_index
    imaging_frequency = np.repeat(imaging_frequency, 2)
    prediction_frequency = np.repeat(prediction_frequency, 2)
    imaging_state = np.empty(hand.size, dtype=np.uint8)
    prediction_state = np.empty(hand.size, dtype=np.uint8)
    wrong_prediction_state = np.empty(hand.size, dtype=np.uint8)
    pair_index = np.empty(hand.size, dtype=np.uint8)
    for route in range(hand.size):
        imaging_key = graph.state_key(
            float(imaging_frequency[route]),
            int(imaging_mueller_index[route] * 15),
        )
        prediction_key = graph.state_key(
            float(prediction_frequency[route]),
            int(prediction_mueller_index[route] * 15),
        )
        wrong_key = graph.state_key(
            float(imaging_frequency[route]),
            int(prediction_mueller_index[route] * 15),
        )
        try:
            imaging_state[route] = state_by_key[imaging_key]
            prediction_state[route] = state_by_key[prediction_key]
            wrong_prediction_state[route] = state_by_key[wrong_key]
            pair_index[route] = pair_by_keys[(imaging_key, prediction_key)]
        except KeyError as error:
            raise PhysicalSemanticGateError(
                f"route {route} has an unrepresented physical state"
            ) from error
    counts = np.bincount(pair_index, minlength=ORDERED_PAIRS)
    if np.any(counts == 0):
        raise PhysicalSemanticGateError("an ordered pair stratum has no real rows")
    return {
        "pair_index": pair_index,
        "imaging_state": imaging_state,
        "prediction_state": prediction_state,
        "wrong_prediction_state": wrong_prediction_state,
        "pair_counts": counts,
    }


def relative_metrics(
    candidate: np.ndarray,
    reference: np.ndarray,
) -> dict[str, float]:
    delta = candidate - reference
    reference_l2 = max(float(np.linalg.norm(reference)), np.finfo(float).tiny)
    reference_linf = max(float(np.max(np.abs(reference))), np.finfo(float).tiny)
    return {
        "relative_l2": float(np.linalg.norm(delta)) / reference_l2,
        "normalized_linf": float(np.max(np.abs(delta))) / reference_linf,
    }


def evaluate_pair(
    *,
    u: np.ndarray,
    v: np.ndarray,
    w: np.ndarray,
    weight: np.ndarray,
    taylor: np.ndarray,
    wrong_taylor: np.ndarray,
    source_l: np.ndarray,
    source_m: np.ndarray,
    source_eta: np.ndarray,
    output_l: np.ndarray,
    output_m: np.ndarray,
    output_eta: np.ndarray,
    model_values: np.ndarray,
    model_term: int,
    prediction_screen: np.ndarray,
    wrong_prediction_screen: np.ndarray,
    prediction_inverse_normalization: np.ndarray,
    left_screen: np.ndarray,
    row_chunk: int = ROW_CHUNK,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Return exact-W, contracted, same-frequency, and wrong-Taylor responses."""

    rows = u.size
    if prediction_inverse_normalization.shape != (rows,):
        raise PhysicalSemanticGateError(
            "prediction inverse normalization does not match the route count"
        )
    outputs = output_l.size
    exact = np.zeros((MODEL_TERMS, outputs), dtype=np.complex128)
    contracted = np.zeros_like(exact)
    wrong_screen = np.zeros_like(exact)
    wrong_taylor_result = np.zeros_like(exact)
    right_basis = np.column_stack(
        [
            model_values * prediction_screen * (-source_eta) ** power
            for power in range(W_ORDERS)
        ]
    )
    wrong_right_basis = np.column_stack(
        [
            model_values * wrong_prediction_screen * (-source_eta) ** power
            for power in range(W_ORDERS)
        ]
    )
    for start in range(0, rows, row_chunk):
        stop = min(rows, start + row_chunk)
        chunk = slice(start, stop)
        chunk_u = u[chunk]
        chunk_v = v[chunk]
        chunk_w = w[chunk]
        chunk_weight = weight[chunk]
        chunk_taylor = taylor[chunk]
        chunk_wrong_taylor = wrong_taylor[chunk]
        chunk_prediction_inverse_normalization = prediction_inverse_normalization[chunk]
        uv_source = np.exp(
            -1j * math.tau * (np.outer(chunk_u, source_l) + np.outer(chunk_v, source_m))
        )
        source_moments = uv_source @ right_basis
        wrong_source_moments = uv_source @ wrong_right_basis
        source_moments *= chunk_prediction_inverse_normalization[:, None]
        wrong_source_moments *= chunk_prediction_inverse_normalization[:, None]
        exact_source = np.exp(
            -1j
            * math.tau
            * (
                np.outer(chunk_u, source_l)
                + np.outer(chunk_v, source_m)
                + np.outer(chunk_w, source_eta)
            )
        )
        exact_prediction = exact_source @ (model_values * prediction_screen)
        exact_prediction *= chunk_prediction_inverse_normalization
        if model_term:
            exact_prediction *= chunk_taylor
        uv_output = np.exp(
            1j * math.tau * (np.outer(chunk_u, output_l) + np.outer(chunk_v, output_m))
        )
        exact_output = uv_output * np.exp(1j * math.tau * np.outer(chunk_w, output_eta))
        iw = 1j * math.tau * chunk_w
        for output_term in range(MODEL_TERMS):
            exact[output_term] += left_screen * (
                exact_output.T
                @ (chunk_weight * chunk_taylor**output_term * exact_prediction)
            )
            moment = model_term + output_term
            for order in range(W_ORDERS):
                row_coefficient = (
                    chunk_weight
                    * chunk_taylor**moment
                    * iw**order
                    / math.factorial(order)
                )
                wrong_taylor_coefficient = (
                    chunk_weight
                    * chunk_wrong_taylor**moment
                    * iw**order
                    / math.factorial(order)
                )
                for left_power in range(order + 1):
                    right_power = order - left_power
                    output_factor = (
                        left_screen
                        * output_eta**left_power
                        * math.comb(order, left_power)
                    )
                    contracted[output_term] += output_factor * (
                        uv_output.T @ (row_coefficient * source_moments[:, right_power])
                    )
                    wrong_screen[output_term] += output_factor * (
                        uv_output.T
                        @ (row_coefficient * wrong_source_moments[:, right_power])
                    )
                    wrong_taylor_result[output_term] += output_factor * (
                        uv_output.T
                        @ (wrong_taylor_coefficient * source_moments[:, right_power])
                    )
    return exact, contracted, wrong_screen, wrong_taylor_result


def screen_weight_product_consistency(
    forward: np.ndarray,
    reverse: np.ndarray,
    normal: np.ndarray,
) -> dict[str, float]:
    expected = np.asarray(forward, dtype=np.complex128) * np.conj(
        np.asarray(reverse, dtype=np.complex128)
    )
    return relative_metrics(np.asarray(normal, dtype=np.complex128), expected)


def run_gate(
    source_path: pathlib.Path,
    screen_manifest_path: pathlib.Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    graph = load_graph_module()
    source = load_json(source_path)
    row_path = pathlib.Path(source["sources"]["row_payload"])
    expected_row_hash = source["sources"]["row_payload_sha256"]
    if sha256_file(row_path) != expected_row_hash:
        raise PhysicalSemanticGateError("frozen row payload hash differs")
    rows = np.memmap(
        row_path,
        dtype=graph.ROW_DTYPE,
        mode="r",
        shape=(int(source["rows"]["count"]),),
    )
    screen_manifest = load_json(screen_manifest_path)
    if screen_manifest.get("schema") != SCREEN_SCHEMA:
        raise PhysicalSemanticGateError(
            f"screen manifest must use schema {SCREEN_SCHEMA}"
        )
    states = screen_manifest.get("states")
    crop_shape = screen_manifest.get("crop_shape")
    if (
        not isinstance(states, list)
        or len(states) != 32
        or not isinstance(crop_shape, list)
        or crop_shape != [512, 512]
    ):
        raise PhysicalSemanticGateError("screen manifest state geometry differs")
    screen_paths = {
        role: resolve_artifact(
            screen_manifest_path,
            screen_manifest,
            f"{role}_path",
        )
        for role in ("forward", "reverse", "normal")
    }
    screens = {
        role: load_screen_family(
            path,
            state_count=len(states),
            crop_side=int(crop_shape[0]),
        )
        for role, path in screen_paths.items()
    }
    pointing_pixels = np.unique(
        np.asarray(rows["pointing_pixel"], dtype=np.float64),
        axis=0,
    )
    if pointing_pixels.shape != (1, 2):
        raise PhysicalSemanticGateError(
            "selected-field semantic gate requires exactly one POINTING"
        )
    pointing_pixel = pointing_pixels[0]
    gradient = maximum_active_gradient(
        screens["forward"],
        pointing_pixel=pointing_pixel,
        manifest=screen_manifest,
    )
    point_pixel = tuple(gradient["image_pixel"])
    source_pixels, model_values = merge_atoms(
        [
            compact_multiscale_atom(
                SCALE12_CENTER,
                12,
                np.float32(1.0),
            ),
            compact_multiscale_atom(
                SCALE5_CENTER,
                5,
                np.float32(0.625),
            ),
            compact_multiscale_atom(
                point_pixel,
                0,
                np.float32(0.3125),
            ),
        ]
    )
    if (
        np.any(source_pixels[:, 0] < ACTIVE_MIN[0])
        or np.any(source_pixels[:, 0] > ACTIVE_MAX[0])
        or np.any(source_pixels[:, 1] < ACTIVE_MIN[1])
        or np.any(source_pixels[:, 1] > ACTIVE_MAX[1])
    ):
        raise PhysicalSemanticGateError("three-atom support escapes the active domain")
    output_pixels = output_probe_pixels(point_pixel)
    source_l, source_m, source_eta = facet_pixel_coordinates(source_pixels)
    output_l, output_m, output_eta = facet_pixel_coordinates(output_pixels)
    source_screen_x, source_screen_y = screen_coordinates(
        source_pixels,
        pointing_pixel=pointing_pixel,
        manifest=screen_manifest,
    )
    output_screen_x, output_screen_y = screen_coordinates(
        output_pixels,
        pointing_pixel=pointing_pixel,
        manifest=screen_manifest,
    )
    source_screens = bilinear_sample_points(
        screens["forward"],
        source_screen_x,
        source_screen_y,
    )
    output_screens = bilinear_sample_points(
        screens["forward"],
        output_screen_x,
        output_screen_y,
    )
    routes = state_and_pair_routes(
        source,
        rows,
        graph,
        screen_manifest,
    )
    pair_index = routes["pair_index"]
    facet_uvw = rotate_uvw_to_facet(np.asarray(rows["uvw_lambda"], dtype=np.float64))
    repeated_uvw = np.repeat(
        facet_uvw,
        2,
        axis=0,
    )
    repeated_weight = np.repeat(
        np.asarray(rows["weight"], dtype=np.float64),
        2,
    )
    prediction_normalization = np.stack(
        [
            np.asarray(
                rows["first_prediction_normalization"],
                dtype=np.complex128,
            ),
            np.asarray(
                rows["second_prediction_normalization"],
                dtype=np.complex128,
            ),
        ],
        axis=1,
    ).reshape(-1)
    prediction_normalization_norm_squared = np.abs(prediction_normalization) ** 2
    if (
        np.any(~np.isfinite(prediction_normalization))
        or np.any(~np.isfinite(prediction_normalization_norm_squared))
        or np.any(prediction_normalization_norm_squared <= 0.0)
    ):
        raise PhysicalSemanticGateError(
            "prediction normalization contains a non-finite or zero route"
        )
    prediction_inverse_normalization = 1.0 / np.conj(prediction_normalization)
    row_manifest = load_json(pathlib.Path(source["sources"]["row_manifest"]))
    reference_frequency = float(row_manifest["contract"]["reference_frequency_hz"])
    taylor = np.repeat(
        np.asarray(rows["frequency_hz"], dtype=np.float64) / reference_frequency - 1.0,
        2,
    )
    wrong_taylor = np.repeat(
        np.asarray(rows["beam_frequency_hz"], dtype=np.float64) / reference_frequency
        - 1.0,
        2,
    )
    exact_values = np.zeros(
        (
            ORDERED_PAIRS,
            MODEL_TERMS,
            MODEL_TERMS,
            output_pixels.shape[0],
        ),
        dtype=np.complex128,
    )
    candidate_values = np.zeros_like(exact_values)
    wrong_screen_values = np.zeros_like(exact_values)
    wrong_taylor_values = np.zeros_like(exact_values)
    pair_receipts: list[dict[str, Any]] = []
    pairs = graph.source_pairs(source)
    for pair in range(ORDERED_PAIRS):
        indices = np.flatnonzero(pair_index == pair)
        imaging_states = np.unique(routes["imaging_state"][indices])
        prediction_states = np.unique(routes["prediction_state"][indices])
        wrong_prediction_states = np.unique(routes["wrong_prediction_state"][indices])
        if (
            imaging_states.size != 1
            or prediction_states.size != 1
            or wrong_prediction_states.size != 1
        ):
            raise PhysicalSemanticGateError(
                f"ordered pair {pair} is not state-homogeneous"
            )
        imaging_state = int(imaging_states[0])
        prediction_state = int(prediction_states[0])
        wrong_prediction_state = int(wrong_prediction_states[0])
        for model_term in range(MODEL_TERMS):
            exact, candidate, wrong_screen, wrong_taylor_result = evaluate_pair(
                u=repeated_uvw[indices, 0],
                v=repeated_uvw[indices, 1],
                w=repeated_uvw[indices, 2],
                weight=repeated_weight[indices],
                taylor=taylor[indices],
                wrong_taylor=wrong_taylor[indices],
                source_l=source_l,
                source_m=source_m,
                source_eta=source_eta,
                output_l=output_l,
                output_m=output_m,
                output_eta=output_eta,
                model_values=model_values,
                model_term=model_term,
                prediction_screen=source_screens[prediction_state],
                wrong_prediction_screen=source_screens[wrong_prediction_state],
                prediction_inverse_normalization=prediction_inverse_normalization[
                    indices
                ],
                left_screen=np.conj(output_screens[imaging_state]),
            )
            exact_values[pair, model_term] = exact
            candidate_values[pair, model_term] = candidate
            wrong_screen_values[pair, model_term] = wrong_screen
            wrong_taylor_values[pair, model_term] = wrong_taylor_result
        pair_receipts.append(
            {
                "pair_index": pair,
                "routes": int(indices.size),
                "imaging_state": imaging_state,
                "prediction_state": prediction_state,
                "wrong_same_frequency_prediction_state": wrong_prediction_state,
                "pair": pairs[pair],
                "candidate_vs_exact_w": relative_metrics(
                    candidate_values[pair],
                    exact_values[pair],
                ),
                "wrong_same_frequency_vs_exact_w": relative_metrics(
                    wrong_screen_values[pair],
                    exact_values[pair],
                ),
                "pb_frequency_taylor_vs_exact_w": relative_metrics(
                    wrong_taylor_values[pair],
                    exact_values[pair],
                ),
            }
        )
        print(
            f"pair {pair + 1}/{ORDERED_PAIRS} routes={indices.size}",
            flush=True,
        )
    candidate_metrics = relative_metrics(candidate_values, exact_values)
    wrong_screen_metrics = relative_metrics(wrong_screen_values, exact_values)
    wrong_taylor_metrics = relative_metrics(wrong_taylor_values, exact_values)
    candidate_denominator = max(
        candidate_metrics["relative_l2"],
        np.finfo(float).eps,
    )
    wrong_screen_ratio = wrong_screen_metrics["relative_l2"] / candidate_denominator
    wrong_taylor_ratio = wrong_taylor_metrics["relative_l2"] / candidate_denominator
    worst_pair = max(
        pair_receipts,
        key=lambda value: value["candidate_vs_exact_w"]["relative_l2"],
    )
    candidate_passed = (
        candidate_metrics["relative_l2"] <= W_RELATIVE_L2_LIMIT
        and candidate_metrics["normalized_linf"] <= W_NORMALIZED_LINF_LIMIT
        and worst_pair["candidate_vs_exact_w"]["relative_l2"]
        <= WORST_PAIR_RELATIVE_L2_LIMIT
    )
    controls_passed = (
        wrong_screen_ratio >= CONTROL_RATIO_MIN
        and wrong_taylor_ratio >= CONTROL_RATIO_MIN
    )
    source_phase = np.asarray(rows["source_phase"], dtype=np.complex128)
    source_phase_distance_from_identity = np.abs(source_phase - 1.0)
    active_pixels = active_support_pixels()
    if active_pixels.shape != (7_304, 2):
        raise PhysicalSemanticGateError(
            f"active support has shape {active_pixels.shape}, expected (7304,2)"
        )
    imaging_screen_states = sorted(
        {int(receipt["imaging_state"]) for receipt in pair_receipts}
    )
    prediction_screen_states = sorted(
        {int(receipt["prediction_state"]) for receipt in pair_receipts}
    )
    if len(imaging_screen_states) != 28 or len(prediction_screen_states) != 32:
        raise PhysicalSemanticGateError(
            "physical screen-state inventory differs from the resident operator"
        )
    compact_imaging_state = {
        screen_state: index for index, screen_state in enumerate(imaging_screen_states)
    }
    compact_prediction_state = {
        screen_state: index
        for index, screen_state in enumerate(prediction_screen_states)
    }
    resident_pair_map = [
        {
            "pair_index": int(receipt["pair_index"]),
            "imaging_state": compact_imaging_state[int(receipt["imaging_state"])],
            "prediction_state": compact_prediction_state[
                int(receipt["prediction_state"])
            ],
            "imaging_screen_state": int(receipt["imaging_state"]),
            "prediction_screen_state": int(receipt["prediction_state"]),
        }
        for receipt in pair_receipts
    ]
    payload = {
        "schema": SCHEMA,
        "role": "production-inert-real-row-physical-screen-semantic-gate",
        "sources": {
            "ordered_response_contract": str(source_path),
            "ordered_response_contract_sha256": sha256_file(source_path),
            "row_payload": str(row_path),
            "row_payload_sha256": expected_row_hash,
            "screen_manifest": str(screen_manifest_path),
            "screen_manifest_sha256": sha256_file(screen_manifest_path),
            "screen_artifact_sha256": {
                role: sha256_file(path) for role, path in screen_paths.items()
            },
            "casa_source_semantics": {
                "aw_visibility_cf": (
                    "AWConvFunc::fillConvFuncBuffer2 multiplies cfBuf by ftATerm_l only"
                ),
                "aw_weight_cf": (
                    "AWConvFunc::fillConvFuncBuffer2 multiplies cfWtBuf by "
                    "ftATerm_l*conj(ftATermSq_l)"
                ),
                "response": "conj(A_imaging(output))*A_prediction(source)",
                "prediction_cf_normalization": (
                    "each explicit RR/LL prediction route is divided by the "
                    "conjugate executable compact-CF normalization"
                ),
                "parallel_hand_weight": (
                    "the stored average-hand weight is applied once to each "
                    "explicit RR/LL route; sumwt_factor is not applied again"
                ),
            },
        },
        "geometry": {
            "image_shape": [IMAGE_SIDE, IMAGE_SIDE],
            "image_reference_pixel": IMAGE_REFERENCE_PIXEL,
            "cell_arcsec": CELL_ARCSEC,
            "pointing_pixel": pointing_pixel.tolist(),
            "mask_min": list(MASK_MIN),
            "mask_max": list(MASK_MAX),
            "active_min": list(ACTIVE_MIN),
            "active_max": list(ACTIVE_MAX),
            "facet_min": list(FACET_MIN),
            "facet_max": list(FACET_MAX),
            "facet_center_pixel": list(FACET_CENTER),
            "uvw_frame": "orthonormal tangent frame at facet center",
            "screen_sampling": "pointing-relative bilinear complex128",
            "maximum_active_forward_screen_gradient": gradient,
            "maximum_residual_w_phase_rad": float(
                math.tau
                * np.max(np.abs(facet_uvw[:, 2]))
                * np.max(np.abs(output_eta[:, None] - source_eta[None, :]))
            ),
        },
        "model": {
            "cases": ["pure-tt0", "pure-tt1"],
            "atoms": [
                {
                    "scale_pixels": 12,
                    "center_pixel": list(SCALE12_CENTER),
                    "amplitude": 1.0,
                },
                {
                    "scale_pixels": 5,
                    "center_pixel": list(SCALE5_CENTER),
                    "amplitude": 0.625,
                },
                {
                    "scale_pixels": 0,
                    "center_pixel": list(point_pixel),
                    "amplitude": 0.3125,
                },
            ],
            "nonzero_pixels": int(source_pixels.shape[0]),
            "value_sum": float(np.sum(model_values)),
            "output_probe_pixels": output_pixels.tolist(),
        },
        "resident_integration_fixture": {
            "embedding": {
                "side": EMBEDDING_SIDE,
                "origin_image_pixel": list(EMBEDDING_ORIGIN),
                "facet_margin_pixels": FACET_EMBEDDING_MARGIN,
                "mapping": (
                    "embedding_xy=image_xy-origin_image_pixel; response UV grids "
                    "must be ifftshifted before an ordinary FFT convolution"
                ),
            },
            "active_pixels": active_pixels.tolist(),
            "source_pixels": source_pixels.tolist(),
            "model_values": model_values.tolist(),
            "output_probe_pixels": output_pixels.tolist(),
            "ordered_pair_map": resident_pair_map,
            "aggregate_total_order_two": complex_array_payload(
                np.sum(candidate_values, axis=0)
            ),
            "aggregate_direct_exact_w": complex_array_payload(
                np.sum(exact_values, axis=0)
            ),
            "factor_recipe": {
                "right": "A_prediction(source)*(-eta_source)^right_power",
                "left": "conj(A_imaging(output))*eta_output^left_power",
                "eta": "facet-frame direction cosine n_local-1",
                "screen_interpolation": "pointing-relative bilinear complex128 then complex64",
            },
        },
        "coverage": {
            "physical_rows": int(rows.size),
            "parallel_hand_routes": int(pair_index.size),
            "ordered_pair_strata": ORDERED_PAIRS,
            "pair_route_count_min": int(np.min(routes["pair_counts"])),
            "pair_route_count_max": int(np.max(routes["pair_counts"])),
            "visibility_frequencies": int(
                np.unique(np.asarray(rows["frequency_hz"])).size
            ),
            "pb_grouping_frequencies": int(
                np.unique(np.asarray(rows["beam_frequency_hz"])).size
            ),
            "source_phase_max_distance_from_identity": float(
                np.max(source_phase_distance_from_identity)
            ),
            "source_phase_note": (
                "the selected phase-center field has an effectively identity "
                "source phase; multi-field source-frame behavior remains a "
                "separate all-fields gate"
            ),
        },
        "metrics": {
            "candidate_total_order_two_vs_direct_exact_w": candidate_metrics,
            "worst_pair_candidate_vs_direct_exact_w": {
                "pair_index": worst_pair["pair_index"],
                **worst_pair["candidate_vs_exact_w"],
            },
            "weight_screen_source_consistency": screen_weight_product_consistency(
                screens["forward"],
                screens["reverse"],
                screens["normal"],
            ),
        },
        "adversarial_controls": {
            "wrong_same_frequency_prediction_screen": {
                **wrong_screen_metrics,
                "relative_l2_error_ratio": wrong_screen_ratio,
            },
            "pb_group_frequency_as_taylor_coordinate": {
                **wrong_taylor_metrics,
                "relative_l2_error_ratio": wrong_taylor_ratio,
            },
            "minimum_error_ratio": CONTROL_RATIO_MIN,
        },
        "pair_strata": pair_receipts,
        "gate": {
            "candidate_passed": candidate_passed,
            "adversarial_controls_passed": controls_passed,
            "passed": candidate_passed and controls_passed,
            "limits": {
                "candidate_relative_l2": W_RELATIVE_L2_LIMIT,
                "candidate_normalized_linf": W_NORMALIZED_LINF_LIMIT,
                "worst_pair_relative_l2": WORST_PAIR_RELATIVE_L2_LIMIT,
                "control_error_ratio": CONTROL_RATIO_MIN,
            },
        },
        "timing": {"wall_s": time.perf_counter() - started},
        "limitations": [
            "the gate evaluates deterministic active-domain output probes rather than all 7304 pixels",
            "it validates the physical-screen and localized-W semantics but does not time construction",
            "it does not run the final prediction, full-resolution finalizer, product assembler, or CLEAN",
            "the selected-field source phase is effectively identity and cannot promote all-fields phase behavior",
        ],
        "decision": (
            "pass-physical-semantic-gate; bind-real-screens-to-resident-operator"
            if candidate_passed and controls_passed
            else "reject-or-correct-physical-ordered-response"
        ),
    }
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-contract", type=pathlib.Path, required=True)
    parser.add_argument("--screen-manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output.exists():
        raise PhysicalSemanticGateError(f"refusing to overwrite {args.output}")
    payload = run_gate(args.source_contract, args.screen_manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    metrics = payload["metrics"]["candidate_total_order_two_vs_direct_exact_w"]
    print(
        "decision={decision} relative_l2={l2:.9e} normalized_linf={linf:.9e} "
        "wall_s={wall:.6f}".format(
            decision=payload["decision"],
            l2=metrics["relative_l2"],
            linf=metrics["normalized_linf"],
            wall=payload["timing"]["wall_s"],
        )
    )
    return 0 if payload["gate"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
