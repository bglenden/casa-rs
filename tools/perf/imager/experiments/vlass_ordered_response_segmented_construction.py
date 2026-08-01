#!/usr/bin/env python3
"""Build deterministic compact route groups for the VLASS ordered response.

This production-inert discriminator uses the frozen localized-row payload and
the controlled 192/J7 discretization.  It measures stable source-order route
segmentation and emits compact response/RHS group streams for the separate
Metal output-owner construction race.  The coefficient recipe is a controlled
total-order-two W/MT-MFS recipe, not yet a scientific replacement for the
production AWProject operator.
"""

from __future__ import annotations

import argparse
import functools
import gc
import hashlib
import importlib.util
import json
import math
import pathlib
import resource
import sys
import time
from typing import Any

import numpy as np


SCHEMA = "casa-rs-vlass-ordered-response-segmented-construction/v10"
GRAPH_SCRIPT = "vlass_localized_ordered_response_graph_contract.py"
PAIR_COUNT = 54
IMAGING_STATE_COUNT = 28
PREDICTION_STATE_COUNT = 32
SIDE = 192
PIXELS = SIDE * SIDE
SUPPORT_WIDTH = 7
OVERSAMPLING = 100
RESPONSE_COEFFICIENTS = 9
RHS_COEFFICIENTS = 6
COMPLEX_F32_BYTES = np.dtype("<c8").itemsize
IMAGE_REFERENCE_PIXEL = 2048.0
CELL_RAD = 0.6 * math.pi / (180.0 * 3600.0)
FACET_CENTER = (606.5, 2156.5)
ROUTE_KEY_DTYPE = np.dtype(
    [
        ("state", "<u2"),
        ("x", "<u2"),
        ("y", "<u2"),
        ("offset_x", "<i2"),
        ("offset_y", "<i2"),
    ],
    align=False,
)
GROUP_META_DTYPE = np.dtype([("offset_x", "<i2"), ("offset_y", "<i2")], align=False)
GROUP_META_FILE_TAG = "i16"
KERNEL_DESCRIPTION = "production StandardGridder normalized separable 100x support-3 spheroidal J7 kernel"


class ConstructionError(RuntimeError):
    """The frozen row contract or deterministic construction is invalid."""


def load_graph_module() -> Any:
    path = pathlib.Path(__file__).with_name(GRAPH_SCRIPT)
    spec = importlib.util.spec_from_file_location("ordered_response_graph", path)
    if spec is None or spec.loader is None:
        raise ConstructionError(f"cannot load graph contract module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform.startswith("linux"):
        return value * 1024
    return value


def timed(result: dict[str, float], name: str, operation: Any) -> Any:
    started = time.perf_counter()
    value = operation()
    result[name] = time.perf_counter() - started
    return value


def state_inventory(
    pairs: list[dict[str, Any]],
    graph: Any,
) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    imaging = sorted(
        {
            graph.state_key(
                float(pair["imaging_frequency_hz"]),
                int(pair["imaging_mueller_element"]),
            )
            for pair in pairs
        }
    )
    prediction = sorted(
        {
            graph.state_key(
                float(pair["prediction_frequency_hz"]),
                int(pair["prediction_mueller_element"]),
            )
            for pair in pairs
        }
    )
    if len(imaging) != IMAGING_STATE_COUNT:
        raise ConstructionError(
            f"expected {IMAGING_STATE_COUNT} imaging states, got {len(imaging)}"
        )
    if len(prediction) != PREDICTION_STATE_COUNT:
        raise ConstructionError(
            f"expected {PREDICTION_STATE_COUNT} prediction states, got {len(prediction)}"
        )
    return imaging, prediction


def route_state_indices(
    source: dict[str, Any],
    rows: np.ndarray,
    graph: Any,
) -> tuple[np.ndarray, np.ndarray]:
    pairs = graph.source_pairs(source)
    imaging_states, prediction_states = state_inventory(pairs, graph)
    persisted = np.asarray(
        source["aw_screen_selection"]["persisted_cf_frequencies_hz"],
        dtype=np.float64,
    )
    frequencies = np.asarray(rows["frequency_hz"], dtype=np.float64)
    reference = float(
        graph.load_json(pathlib.Path(source["sources"]["row_manifest"]))["contract"][
            "reference_frequency_hz"
        ]
    )
    imaging_frequency_index = graph.nearest_indices(
        np.sqrt(2.0 * reference * reference - frequencies * frequencies),
        persisted,
    )
    prediction_frequency_index = graph.nearest_indices(frequencies, persisted)
    frequency_index = {
        int(round(frequency)): index for index, frequency in enumerate(persisted)
    }
    pair_lut = np.full((persisted.size, 2, persisted.size, 2), -1, dtype=np.int16)
    imaging_lut = np.full((persisted.size, 2), -1, dtype=np.int16)
    for index, (frequency, mueller) in enumerate(imaging_states):
        imaging_lut[frequency_index[frequency], mueller // 15] = index
    for pair_index, pair in enumerate(pairs):
        pair_lut[
            frequency_index[int(round(float(pair["imaging_frequency_hz"])))],
            int(pair["imaging_mueller_element"]) // 15,
            frequency_index[int(round(float(pair["prediction_frequency_hz"])))],
            int(pair["prediction_mueller_element"]) // 15,
        ] = pair_index

    positive_w = np.asarray(rows["uvw_lambda"][:, 2] > 0.0)
    physical_count = rows.size
    row_index = np.repeat(np.arange(physical_count), 2)
    hand = np.tile(np.arange(2, dtype=np.int8), physical_count)
    route_positive_w = np.repeat(positive_w, 2)
    if route_positive_w.shape != hand.shape:
        raise AssertionError("parallel-hand W sign expansion is not one dimensional")
    imaging_mueller = np.where(route_positive_w, hand, 1 - hand)
    prediction_mueller = 1 - imaging_mueller
    repeated_imaging_frequency = np.repeat(imaging_frequency_index, 2)
    repeated_prediction_frequency = np.repeat(prediction_frequency_index, 2)
    pair_index = pair_lut[
        repeated_imaging_frequency,
        imaging_mueller,
        repeated_prediction_frequency,
        prediction_mueller,
    ]
    imaging_state = imaging_lut[repeated_imaging_frequency, imaging_mueller]
    if np.any(pair_index < 0) or np.any(imaging_state < 0):
        raise ConstructionError(
            "a real row route is absent from the frozen state inventory"
        )
    if row_index.size != 2 * physical_count:
        raise AssertionError("parallel-hand route expansion is inconsistent")
    return pair_index.astype(np.uint16), imaging_state.astype(np.uint16)


def facet_basis() -> np.ndarray:
    center_l = (FACET_CENTER[0] - IMAGE_REFERENCE_PIXEL) * CELL_RAD
    center_m = (IMAGE_REFERENCE_PIXEL - FACET_CENTER[1]) * CELL_RAD
    center_n = math.sqrt(1.0 - center_l * center_l - center_m * center_m)
    tangent_norm = math.hypot(center_n, center_l)
    tangent_l = np.asarray(
        [center_n / tangent_norm, 0.0, -center_l / tangent_norm],
        dtype=np.float64,
    )
    tangent_m = np.asarray(
        [
            -center_l * center_m / tangent_norm,
            tangent_norm,
            -center_n * center_m / tangent_norm,
        ],
        dtype=np.float64,
    )
    normal = np.asarray([center_l, center_m, center_n], dtype=np.float64)
    return np.column_stack([tangent_l, tangent_m, normal])


def rotate_uvw_to_facet(rows: np.ndarray) -> np.ndarray:
    uvw = np.asarray(rows["uvw_lambda"], dtype=np.float64)
    return uvw @ facet_basis()


def route_geometry(
    rows: np.ndarray,
    facet_uvw: np.ndarray,
) -> tuple[np.ndarray, ...]:
    spacing = 1.0 / (SIDE * CELL_RAD)
    x_position = np.asarray(facet_uvw[:, 0], dtype=np.float64) / spacing + SIDE / 2
    y_position = -np.asarray(facet_uvw[:, 1], dtype=np.float64) / spacing + SIDE / 2
    x = np.rint(x_position).astype(np.int16)
    y = np.rint(y_position).astype(np.int16)
    offset_x = np.rint((x - x_position) * OVERSAMPLING).astype(np.int16)
    offset_y = np.rint((y - y_position) * OVERSAMPLING).astype(np.int16)
    radius = SUPPORT_WIDTH // 2
    if np.any(x < radius) or np.any(y < radius):
        raise ConstructionError("controlled support crosses the low embedding boundary")
    if np.any(x >= SIDE - radius) or np.any(y >= SIDE - radius):
        raise ConstructionError(
            "controlled support crosses the high embedding boundary"
        )
    return tuple(np.repeat(value, 2) for value in (x, y, offset_x, offset_y))


def coefficient_basis(
    rows: np.ndarray,
    facet_uvw: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    manifest = pathlib.Path(str(rows.filename)).with_name("manifest.json")
    contract = json.loads(manifest.read_text(encoding="utf-8"))["contract"]
    reference = float(contract["reference_frequency_hz"])
    frequency = np.asarray(rows["frequency_hz"], dtype=np.float64)
    taylor = frequency / reference - 1.0
    w = np.asarray(facet_uvw[:, 2], dtype=np.float64)
    # VisibilityBatch stores the average parallel-hand weight and records the
    # two-hand sumwt multiplier separately.  This construction expands RR and
    # LL explicitly, so each route receives the stored weight exactly once.
    weight = np.asarray(rows["weight"], dtype=np.float64)
    iw = 1j * math.tau * w
    w_coefficients = np.stack([np.ones(rows.size), iw, iw * iw / 2.0], axis=1)
    return weight, w_coefficients, taylor


def expanded_prediction_inverse_normalization(rows: np.ndarray) -> np.ndarray:
    normalization = np.stack(
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
    norm_squared = np.abs(normalization) ** 2
    if (
        np.any(~np.isfinite(normalization))
        or np.any(~np.isfinite(norm_squared))
        or np.any(norm_squared <= 0.0)
    ):
        raise ConstructionError(
            "prediction normalization contains a non-finite or zero route"
        )
    return 1.0 / np.conj(normalization)


def controlled_response_coefficients(
    rows: np.ndarray,
    facet_uvw: np.ndarray,
) -> np.ndarray:
    weight, w_coefficients, taylor = coefficient_basis(rows, facet_uvw)
    moments = np.stack([np.ones(rows.size), taylor, taylor * taylor], axis=1)
    response = (
        weight[:, None, None] * w_coefficients[:, :, None] * moments[:, None, :]
    ).reshape(rows.size, RESPONSE_COEFFICIENTS)
    expanded = np.repeat(response, 2, axis=0).astype(np.complex128)
    expanded *= expanded_prediction_inverse_normalization(rows)[:, None]
    return expanded


def controlled_rhs_coefficients(
    rows: np.ndarray,
    facet_uvw: np.ndarray,
) -> np.ndarray:
    weight, w_coefficients, taylor = coefficient_basis(rows, facet_uvw)
    hand_visibility = np.stack(
        [
            np.asarray(rows["first_visibility"], dtype=np.complex128),
            np.asarray(rows["second_visibility"], dtype=np.complex128),
        ],
        axis=1,
    ).reshape(-1)
    source_phase = np.repeat(np.asarray(rows["source_phase"], dtype=np.complex128), 2)
    rhs_moments = np.stack([np.ones(rows.size), taylor], axis=1)
    rhs_base = (
        weight[:, None, None] * w_coefficients[:, :, None] * rhs_moments[:, None, :]
    ).reshape(rows.size, RHS_COEFFICIENTS)
    rhs = np.repeat(rhs_base, 2, axis=0)
    rhs *= (hand_visibility * source_phase)[:, None]
    return rhs.astype(np.complex128)


def stable_segment(
    state: np.ndarray,
    geometry: tuple[np.ndarray, ...],
    coefficients: np.ndarray,
    state_count: int,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    dict[str, float],
    list[dict[str, Any]],
]:
    x, y, offset_x, offset_y = geometry
    if state.size != coefficients.shape[0]:
        raise ConstructionError("route keys and coefficients have different lengths")
    source_ordinal = np.arange(state.size, dtype=np.uint32)
    # Dense bucket indices are state-major, then y-major, then x-major.  Keep
    # the grouped values in precisely that order so each prefix interval owns
    # the metadata and coefficients for its recorded bucket.
    order = np.lexsort((source_ordinal, offset_y, offset_x, x, y, state))
    sorted_state = state[order]
    sorted_x = x[order]
    sorted_y = y[order]
    sorted_offset_x = offset_x[order]
    sorted_offset_y = offset_y[order]
    change = np.empty(order.size, dtype=bool)
    change[0] = True
    change[1:] = (
        (sorted_state[1:] != sorted_state[:-1])
        | (sorted_x[1:] != sorted_x[:-1])
        | (sorted_y[1:] != sorted_y[:-1])
        | (sorted_offset_x[1:] != sorted_offset_x[:-1])
        | (sorted_offset_y[1:] != sorted_offset_y[:-1])
    )
    starts = np.flatnonzero(change)
    sums = np.add.reduceat(coefficients[order], starts, axis=0)
    group_cell = (
        sorted_state[starts].astype(np.int64) * PIXELS
        + sorted_y[starts].astype(np.int64) * SIDE
        + sorted_x[starts].astype(np.int64)
    )
    bucket_counts = np.bincount(group_cell, minlength=state_count * PIXELS)
    bucket_offsets = np.empty(state_count * PIXELS + 1, dtype="<u4")
    bucket_offsets[0] = 0
    np.cumsum(bucket_counts, dtype=np.uint32, out=bucket_offsets[1:])
    if int(bucket_offsets[-1]) != starts.size:
        raise ConstructionError("dense center-bucket index lost segmented groups")

    meta = np.empty(starts.size, dtype=GROUP_META_DTYPE)
    meta["offset_x"] = sorted_offset_x[starts]
    meta["offset_y"] = sorted_offset_y[starts]
    converted = sums.astype("<c8")
    roundtrip = converted.astype(np.complex128)
    denominator = max(float(np.linalg.norm(sums)), np.finfo(float).tiny)
    metrics = {
        "f32_relative_l2": float(np.linalg.norm(roundtrip - sums)) / denominator,
        "f32_normalized_linf": float(np.max(np.abs(roundtrip - sums)))
        / max(float(np.max(np.abs(sums))), np.finfo(float).tiny),
    }
    samples = sampled_f64_output(bucket_offsets, meta, sums, state_count)
    return bucket_offsets, meta, converted, metrics, samples


def grdsf(nu: float) -> float:
    p0 = [8.203343e-2, -3.644705e-1, 6.278660e-1, -5.335581e-1, 2.312756e-1]
    p1 = [4.028559e-3, -3.697768e-2, 1.021332e-1, -1.201436e-1, 6.412774e-2]
    q0 = [1.0, 8.212018e-1, 2.078043e-1]
    q1 = [1.0, 9.599102e-1, 2.918724e-1]
    if not 0.0 <= nu <= 1.0:
        return 0.0
    p, q, nu_end = (p0, q0, 0.75) if nu < 0.75 else (p1, q1, 1.0)
    delta = nu * nu - nu_end * nu_end
    numerator = sum(value * delta**order for order, value in enumerate(p))
    denominator = sum(value * delta**order for order, value in enumerate(q))
    return 0.0 if denominator == 0.0 else numerator / denominator


def spheroidal_kernel(distance: float, support: float = 3.0) -> np.float32:
    if not math.isfinite(distance) or distance > support:
        return np.float32(0.0)
    nu = distance / support
    if nu > 1.0:
        return np.float32(0.0)
    return np.float32((1.0 - nu * nu) * grdsf(nu))


@functools.cache
def standard_j7_kernel_lut() -> np.ndarray:
    values = np.zeros((OVERSAMPLING + 1, SUPPORT_WIDTH), dtype=np.float32)
    radius = SUPPORT_WIDTH // 2
    kernel_table = np.zeros(
        OVERSAMPLING * (radius + 1),
        dtype=np.float32,
    )
    for index in range(OVERSAMPLING * radius):
        kernel_table[index] = spheroidal_kernel(index / OVERSAMPLING)
    for offset_index, offset in enumerate(
        range(-(OVERSAMPLING // 2), OVERSAMPLING // 2 + 1)
    ):
        normalization = np.float32(0.0)
        for tap, delta in enumerate(range(-radius, radius + 1)):
            lookup = abs(delta * OVERSAMPLING + offset)
            weight = (
                kernel_table[lookup] if lookup < kernel_table.size else np.float32(0.0)
            )
            values[offset_index, tap] = weight
            normalization = np.float32(normalization + weight)
        if normalization > np.float32(0.0):
            values[offset_index] = np.asarray(
                values[offset_index] / normalization,
                dtype=np.float32,
            )
    return values


def controlled_kernel_weight(offset: int, delta: int) -> float:
    offset = int(offset)
    if not -50 <= offset <= 50 or not -3 <= delta <= 3:
        return 0.0
    return float(standard_j7_kernel_lut()[offset + 50, delta + 3])


def sampled_f64_output(
    bucket_offsets: np.ndarray,
    meta: np.ndarray,
    sums: np.ndarray,
    state_count: int,
) -> list[dict[str, Any]]:
    coefficient_count = sums.shape[1]
    samples: list[dict[str, Any]] = []
    radius = SUPPORT_WIDTH // 2
    for state in range(state_count):
        state_start = state * PIXELS
        state_offsets = bucket_offsets[state_start : state_start + PIXELS + 1]
        nonempty = np.flatnonzero(np.diff(state_offsets))
        if nonempty.size == 0:
            samples.append(
                {
                    "state": state,
                    "x": 0,
                    "y": 0,
                    "values": [
                        [0.0, 0.0] for _ in range(coefficient_count)
                    ],
                    "empty_state": True,
                }
            )
            continue
        selected = nonempty[np.linspace(0, nonempty.size - 1, 4, dtype=np.int64)]
        for pixel in selected:
            output_y, output_x = divmod(int(pixel), SIDE)
            accumulator = np.zeros(coefficient_count, dtype=np.complex128)
            for center_y in range(
                max(0, output_y - radius),
                min(SIDE, output_y + radius + 1),
            ):
                for center_x in range(
                    max(0, output_x - radius),
                    min(SIDE, output_x + radius + 1),
                ):
                    bucket = state_start + center_y * SIDE + center_x
                    begin = int(bucket_offsets[bucket])
                    end = int(bucket_offsets[bucket + 1])
                    if begin == end:
                        continue
                    delta_x = output_x - center_x
                    delta_y = output_y - center_y
                    for group in range(begin, end):
                        weight = controlled_kernel_weight(
                            meta["offset_x"][group].item(), delta_x
                        ) * controlled_kernel_weight(
                            meta["offset_y"][group].item(), delta_y
                        )
                        accumulator += weight * sums[group]
            samples.append(
                {
                    "state": state,
                    "x": output_x,
                    "y": output_y,
                    "values": [
                        [float(value.real), float(value.imag)] for value in accumulator
                    ],
                }
            )
    return samples


def write_array(path: pathlib.Path, values: np.ndarray) -> dict[str, Any]:
    values.tofile(path)
    return {
        "path": path.name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "dtype": values.dtype.descr if values.dtype.fields else values.dtype.str,
        "shape": list(values.shape),
    }


def derive(source_path: pathlib.Path, output_dir: pathlib.Path) -> dict[str, Any]:
    graph = load_graph_module()
    source = graph.load_json(source_path)
    graph.source_pairs(source)
    row_path = pathlib.Path(source["sources"]["row_payload"])
    if sha256_file(row_path) != source["sources"]["row_payload_sha256"]:
        raise ConstructionError("frozen row payload hash differs")
    rows = np.memmap(
        row_path,
        dtype=graph.ROW_DTYPE,
        mode="r",
        shape=(int(source["rows"]["count"]),),
    )
    timings: dict[str, float] = {}
    total_started = time.perf_counter()

    pair_index, imaging_state = timed(
        timings,
        "route_state_s",
        lambda: route_state_indices(source, rows, graph),
    )
    print("phase route-state complete", flush=True)
    facet_uvw = timed(
        timings,
        "facet_uvw_rotation_s",
        lambda: rotate_uvw_to_facet(rows),
    )
    print("phase facet-uvw-rotation complete", flush=True)
    geometry = timed(
        timings,
        "route_geometry_s",
        lambda: route_geometry(rows, facet_uvw),
    )
    print("phase route-geometry complete", flush=True)
    response_coefficients = timed(
        timings,
        "response_coefficients_s",
        lambda: controlled_response_coefficients(rows, facet_uvw),
    )
    print("phase response-coefficients complete", flush=True)
    response_segment_started = time.perf_counter()
    response = stable_segment(pair_index, geometry, response_coefficients, PAIR_COUNT)
    timings["response_stable_segment_s"] = (
        time.perf_counter() - response_segment_started
    )
    del response_coefficients
    gc.collect()
    print("phase response-segment complete", flush=True)
    rhs_coefficients = timed(
        timings,
        "rhs_coefficients_s",
        lambda: controlled_rhs_coefficients(rows, facet_uvw),
    )
    print("phase rhs-coefficients complete", flush=True)
    rhs_segment_started = time.perf_counter()
    rhs = stable_segment(imaging_state, geometry, rhs_coefficients, IMAGING_STATE_COUNT)
    timings["rhs_stable_segment_s"] = time.perf_counter() - rhs_segment_started
    del rhs_coefficients
    gc.collect()
    print("phase rhs-segment complete", flush=True)
    in_memory_total = time.perf_counter() - total_started

    output_dir.mkdir(parents=True)
    (
        response_offsets,
        response_meta,
        response_values,
        response_metrics,
        response_samples,
    ) = response
    rhs_offsets, rhs_meta, rhs_values, rhs_metrics, rhs_samples = rhs
    artifacts = {
        "response_bucket_offsets": write_array(
            output_dir / "response-bucket-offsets-u32-le.bin",
            response_offsets,
        ),
        "response_group_meta": write_array(
            output_dir / f"response-group-meta-{GROUP_META_FILE_TAG}-le.bin",
            response_meta,
        ),
        "response_group_coefficients": write_array(
            output_dir / "response-group-coefficients-c64-le.bin",
            response_values,
        ),
        "rhs_bucket_offsets": write_array(
            output_dir / "rhs-bucket-offsets-u32-le.bin", rhs_offsets
        ),
        "rhs_group_meta": write_array(
            output_dir / f"rhs-group-meta-{GROUP_META_FILE_TAG}-le.bin", rhs_meta
        ),
        "rhs_group_coefficients": write_array(
            output_dir / "rhs-group-coefficients-c64-le.bin", rhs_values
        ),
    }
    artifact_write = time.perf_counter() - total_started - in_memory_total
    manifest = {
        "schema": SCHEMA,
        "role": "production-inert-real-route-segmentation-and-metal-construction-input",
        "source": {
            "contract": str(source_path),
            "contract_sha256": sha256_file(source_path),
            "row_payload": str(row_path),
            "row_payload_sha256": sha256_file(row_path),
        },
        "classification": {
            "route_geometry": (
                f"real frozen 4096-square {rows.size}-row selection in the "
                "orthonormal tangent frame at the 128-square "
                "reconstruction-facet center"
            ),
            "coalescing": (
                f"exact within the {KERNEL_DESCRIPTION} discretization on the "
                f"{SIDE}-square construction grid"
            ),
            "accumulation": "stable source-order complex-f64 segmented sum",
            "coefficient_recipe": (
                "one stored average-hand weight per explicit RR/LL route, "
                "per-route inverse-conjugate executable prediction-CF "
                "normalization, facet-frame total-order-two W, and "
                "actual-frequency MT-MFS recipe validated by the physical "
                "semantic gate"
            ),
            "metal_output_construction": "not measured by this receipt",
        },
        "geometry": {
            "image_reference_pixel": IMAGE_REFERENCE_PIXEL,
            "cell_arcsec": 0.6,
            "construction_grid_side": SIDE,
            "spatial_oversampling_vs_resident_192": SIDE / 192,
            "facet_center_pixel": list(FACET_CENTER),
            "uvw_frame": "orthonormal tangent frame at facet center",
            "maximum_abs_facet_w_lambda": float(np.max(np.abs(facet_uvw[:, 2]))),
        },
        "counts": {
            "physical_rows": int(rows.size),
            "parallel_hand_routes": int(2 * rows.size),
            "response_groups": int(response_meta.size),
            "rhs_groups": int(rhs_meta.size),
            "response_center_buckets": PAIR_COUNT * PIXELS,
            "rhs_center_buckets": IMAGING_STATE_COUNT * PIXELS,
            "response_coefficients_per_group": RESPONSE_COEFFICIENTS,
            "rhs_coefficients_per_group": RHS_COEFFICIENTS,
        },
        "timings": {
            **timings,
            "in_memory_total_s": in_memory_total,
            "artifact_write_s": artifact_write,
        },
        "memory": {"process_peak_rss_bytes": peak_rss_bytes()},
        "conversion": {
            "response": response_metrics,
            "rhs": rhs_metrics,
        },
        "sampled_f64_output": {
            "kernel": KERNEL_DESCRIPTION,
            "response": response_samples,
            "rhs": rhs_samples,
        },
        "artifacts": artifacts,
        "next_gate": (
            f"construct all 486 response and 168 RHS {SIDE}-square complex-f32 "
            "grids with the output-owner Metal kernel and compare sampled cells "
            "against direct complex-f64 route sums"
        ),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest["manifest"] = {
        "path": manifest_path.name,
        "sha256": sha256_file(manifest_path),
    }
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-contract", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output_dir.exists():
        raise ConstructionError(f"refusing to overwrite {args.output_dir}")
    result = derive(args.source_contract, args.output_dir)
    print(
        "manifest={manifest} response_groups={response_groups} "
        "rhs_groups={rhs_groups} in_memory_s={elapsed:.6f}".format(
            manifest=result["manifest"]["path"],
            response_groups=result["counts"]["response_groups"],
            rhs_groups=result["counts"]["rhs_groups"],
            elapsed=result["timings"]["in_memory_total_s"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
