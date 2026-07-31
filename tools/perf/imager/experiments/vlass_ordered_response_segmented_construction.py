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


SCHEMA = "casa-rs-vlass-ordered-response-segmented-construction/v1"
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
GROUP_META_DTYPE = np.dtype(
    [("offset_x", "<i2"), ("offset_y", "<i2")], align=False
)


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
        graph.load_json(pathlib.Path(source["sources"]["row_manifest"]))[
            "contract"
        ]["reference_frequency_hz"]
    )
    imaging_frequency_index = graph.nearest_indices(
        np.sqrt(2.0 * reference * reference - frequencies * frequencies),
        persisted,
    )
    prediction_frequency_index = graph.nearest_indices(frequencies, persisted)
    frequency_index = {
        int(round(frequency)): index
        for index, frequency in enumerate(persisted)
    }
    pair_lut = np.full(
        (persisted.size, 2, persisted.size, 2), -1, dtype=np.int16
    )
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
    imaging_state = imaging_lut[
        repeated_imaging_frequency, imaging_mueller
    ]
    if np.any(pair_index < 0) or np.any(imaging_state < 0):
        raise ConstructionError("a real row route is absent from the frozen state inventory")
    if row_index.size != 2 * physical_count:
        raise AssertionError("parallel-hand route expansion is inconsistent")
    return pair_index.astype(np.uint16), imaging_state.astype(np.uint16)


def route_geometry(rows: np.ndarray) -> tuple[np.ndarray, ...]:
    cell_rad = 0.6 * math.pi / (180.0 * 3600.0)
    spacing = 1.0 / (SIDE * cell_rad)
    x_position = (
        np.asarray(rows["uvw_lambda"][:, 0], dtype=np.float64) / spacing
        + SIDE / 2
    )
    y_position = (
        -np.asarray(rows["uvw_lambda"][:, 1], dtype=np.float64) / spacing
        + SIDE / 2
    )
    x = np.rint(x_position).astype(np.int16)
    y = np.rint(y_position).astype(np.int16)
    offset_x = np.rint((x - x_position) * OVERSAMPLING).astype(np.int16)
    offset_y = np.rint((y - y_position) * OVERSAMPLING).astype(np.int16)
    radius = SUPPORT_WIDTH // 2
    if np.any(x < radius) or np.any(y < radius):
        raise ConstructionError("controlled support crosses the low embedding boundary")
    if np.any(x >= SIDE - radius) or np.any(y >= SIDE - radius):
        raise ConstructionError("controlled support crosses the high embedding boundary")
    return tuple(np.repeat(value, 2) for value in (x, y, offset_x, offset_y))


def coefficient_basis(
    rows: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    manifest = pathlib.Path(str(rows.filename)).with_name("manifest.json")
    contract = json.loads(manifest.read_text(encoding="utf-8"))["contract"]
    reference = float(contract["reference_frequency_hz"])
    frequency = np.asarray(rows["frequency_hz"], dtype=np.float64)
    taylor = frequency / reference - 1.0
    w = np.asarray(rows["uvw_lambda"][:, 2], dtype=np.float64)
    weight = (
        np.asarray(rows["weight"], dtype=np.float64)
        * np.asarray(rows["sumwt_factor"], dtype=np.float64)
    )
    iw = 1j * math.tau * w
    w_coefficients = np.stack(
        [np.ones(rows.size), iw, iw * iw / 2.0], axis=1
    )
    return weight, w_coefficients, taylor


def controlled_response_coefficients(rows: np.ndarray) -> np.ndarray:
    weight, w_coefficients, taylor = coefficient_basis(rows)
    moments = np.stack([np.ones(rows.size), taylor, taylor * taylor], axis=1)
    response = (
        weight[:, None, None]
        * w_coefficients[:, :, None]
        * moments[:, None, :]
    ).reshape(rows.size, RESPONSE_COEFFICIENTS)
    return np.repeat(response, 2, axis=0).astype(np.complex128)


def controlled_rhs_coefficients(rows: np.ndarray) -> np.ndarray:
    weight, w_coefficients, taylor = coefficient_basis(rows)
    hand_visibility = np.stack(
        [
            np.asarray(rows["first_visibility"], dtype=np.complex128),
            np.asarray(rows["second_visibility"], dtype=np.complex128),
        ],
        axis=1,
    ).reshape(-1)
    source_phase = np.repeat(
        np.asarray(rows["source_phase"], dtype=np.complex128), 2
    )
    rhs_moments = np.stack([np.ones(rows.size), taylor], axis=1)
    rhs_base = (
        weight[:, None, None]
        * w_coefficients[:, :, None]
        * rhs_moments[:, None, :]
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
    order = np.lexsort(
        (source_ordinal, offset_y, offset_x, y, x, state)
    )
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
    bucket_counts = np.bincount(
        group_cell, minlength=state_count * PIXELS
    )
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
    samples = sampled_f64_output(
        bucket_offsets, meta, sums, state_count
    )
    return bucket_offsets, meta, converted, metrics, samples


def controlled_kernel_weight(offset: int, delta: int) -> float:
    relative = float(delta) + float(offset) / OVERSAMPLING
    return math.exp(-0.5 * (relative / 1.15) ** 2)


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
            raise ConstructionError(f"state {state} has no construction groups")
        selected = nonempty[
            np.linspace(0, nonempty.size - 1, 4, dtype=np.int64)
        ]
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
                            int(meta["offset_x"][group]), delta_x
                        ) * controlled_kernel_weight(
                            int(meta["offset_y"][group]), delta_y
                        )
                        accumulator += weight * sums[group]
            samples.append(
                {
                    "state": state,
                    "x": output_x,
                    "y": output_y,
                    "values": [
                        [float(value.real), float(value.imag)]
                        for value in accumulator
                    ],
                }
            )
    return samples


def write_array(path: pathlib.Path, values: np.ndarray) -> dict[str, Any]:
    values.tofile(path)
    return {
        "path": str(path),
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
    geometry = timed(timings, "route_geometry_s", lambda: route_geometry(rows))
    print("phase route-geometry complete", flush=True)
    response_coefficients = timed(
        timings,
        "response_coefficients_s",
        lambda: controlled_response_coefficients(rows),
    )
    print("phase response-coefficients complete", flush=True)
    response_segment_started = time.perf_counter()
    response = stable_segment(
        pair_index, geometry, response_coefficients, PAIR_COUNT
    )
    timings["response_stable_segment_s"] = (
        time.perf_counter() - response_segment_started
    )
    del response_coefficients
    gc.collect()
    print("phase response-segment complete", flush=True)
    rhs_coefficients = timed(
        timings,
        "rhs_coefficients_s",
        lambda: controlled_rhs_coefficients(rows),
    )
    print("phase rhs-coefficients complete", flush=True)
    rhs_segment_started = time.perf_counter()
    rhs = stable_segment(
        imaging_state, geometry, rhs_coefficients, IMAGING_STATE_COUNT
    )
    timings["rhs_stable_segment_s"] = (
        time.perf_counter() - rhs_segment_started
    )
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
            output_dir / "response-group-meta-i16-le.bin", response_meta
        ),
        "response_group_coefficients": write_array(
            output_dir / "response-group-coefficients-c64-le.bin",
            response_values,
        ),
        "rhs_bucket_offsets": write_array(
            output_dir / "rhs-bucket-offsets-u32-le.bin", rhs_offsets
        ),
        "rhs_group_meta": write_array(
            output_dir / "rhs-group-meta-i16-le.bin", rhs_meta
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
            "route_geometry": "real frozen 4096-square four-SPW row",
            "coalescing": "exact within the controlled 192/J7 discretization",
            "accumulation": "stable source-order complex-f64 segmented sum",
            "coefficient_recipe": (
                "controlled total-order-two W and MT-MFS recipe for construction "
                "timing; not a scientific AWProject replacement"
            ),
            "metal_output_construction": "not measured by this receipt",
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
            "kernel": (
                "separable exp(-0.5*((delta+subpixel/100)/1.15)^2) "
                "controlled J7 construction kernel"
            ),
            "response": response_samples,
            "rhs": rhs_samples,
        },
        "artifacts": artifacts,
        "next_gate": (
            "construct all 486 response and 168 RHS 192-square complex-f32 "
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
        "path": str(manifest_path),
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
