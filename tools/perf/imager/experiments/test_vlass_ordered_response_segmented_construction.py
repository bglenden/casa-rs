#!/usr/bin/env python3
"""Tests for the deterministic ordered-response construction reducer."""

from __future__ import annotations

import importlib.util
import pathlib

import numpy as np


MODULE_PATH = pathlib.Path(__file__).with_name(
    "vlass_ordered_response_segmented_construction.py"
)
SPEC = importlib.util.spec_from_file_location(
    "vlass_ordered_response_segmented_construction", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


def test_stable_segment_preserves_source_order_and_builds_dense_buckets() -> None:
    state = np.asarray([1, 0, 1, 0], dtype=np.uint16)
    x = np.asarray([10, 10, 10, 10], dtype=np.int16)
    y = x.copy()
    offset_x = np.asarray([0, 1, 0, 1], dtype=np.int16)
    offset_y = np.zeros(4, dtype=np.int16)
    coefficients = np.arange(8, dtype=np.float64).reshape(4, 2)
    coefficients = coefficients.astype(np.complex128)

    offsets, meta, values, metrics, samples = subject.stable_segment(
        state,
        (x, y, offset_x, offset_y),
        coefficients,
        2,
    )

    assert values.shape == (2, 2)
    np.testing.assert_allclose(values[0], coefficients[1] + coefficients[3])
    np.testing.assert_allclose(values[1], coefficients[0] + coefficients[2])
    assert int(offsets[-1]) == 2
    assert meta["offset_x"].tolist() == [1, 0]
    assert metrics == {
        "f32_relative_l2": 0.0,
        "f32_normalized_linf": 0.0,
    }
    assert len(samples) == 8


def test_dense_bucket_prefixes_follow_state_y_x_order() -> None:
    state = np.zeros(4, dtype=np.uint16)
    x = np.asarray([8, 7, 8, 7], dtype=np.int16)
    y = np.asarray([7, 8, 8, 7], dtype=np.int16)
    offset_x = np.asarray([1, 2, 3, 4], dtype=np.int16)
    offset_y = np.zeros(4, dtype=np.int16)
    coefficients = np.asarray([[10], [20], [30], [40]], dtype=np.complex128)

    offsets, meta, values, _, _ = subject.stable_segment(
        state,
        (x, y, offset_x, offset_y),
        coefficients,
        1,
    )

    expected = {
        (7, 7): (4, 40),
        (8, 7): (1, 10),
        (7, 8): (2, 20),
        (8, 8): (3, 30),
    }
    for (x_value, y_value), (expected_offset, expected_value) in expected.items():
        bucket = y_value * subject.SIDE + x_value
        begin = int(offsets[bucket])
        end = int(offsets[bucket + 1])
        assert end - begin == 1
        assert int(meta["offset_x"][begin]) == expected_offset
        assert values[begin, 0] == expected_value


def test_unobserved_resident_state_has_an_explicit_zero_sample() -> None:
    state = np.asarray([1], dtype=np.uint16)
    x = np.asarray([10], dtype=np.int16)
    y = x.copy()
    offset = np.zeros(1, dtype=np.int16)
    coefficients = np.asarray([[3 + 4j]], dtype=np.complex128)

    _, _, _, _, samples = subject.stable_segment(
        state,
        (x, y, offset, offset),
        coefficients,
        2,
    )

    assert samples[0] == {
        "state": 0,
        "x": 0,
        "y": 0,
        "values": [[0.0, 0.0]],
        "empty_state": True,
    }
    assert [sample["state"] for sample in samples[1:]] == [1, 1, 1, 1]


def test_standard_j7_kernel_lut_is_normalized_symmetric_and_sensitive() -> None:
    assert subject.controlled_kernel_weight(0, -2) == subject.controlled_kernel_weight(
        0, 2
    )
    assert subject.controlled_kernel_weight(25, 1) != subject.controlled_kernel_weight(
        -25, 1
    )
    for offset in (-50, -25, 0, 25, 50):
        total = sum(
            subject.controlled_kernel_weight(offset, delta) for delta in range(-3, 4)
        )
        assert abs(total - 1.0) < 2.0e-7
    assert subject.controlled_kernel_weight(0, 0) < 1.0


def test_facet_rotation_preserves_direction_phase_differences() -> None:
    dtype = np.dtype([("uvw_lambda", "<f8", (3,))])
    rows = np.zeros(2, dtype=dtype)
    rows["uvw_lambda"] = [
        [-8196.2, -13477.0, 12544.9],
        [104409.9, -86371.3, -35215.4],
    ]
    pixels = np.asarray([[563, 2113], [650, 2200]], dtype=np.float64)
    global_l = (pixels[:, 0] - subject.IMAGE_REFERENCE_PIXEL) * subject.CELL_RAD
    global_m = (subject.IMAGE_REFERENCE_PIXEL - pixels[:, 1]) * subject.CELL_RAD
    global_eta = np.sqrt(1.0 - global_l**2 - global_m**2) - 1.0
    global_directions = np.column_stack([global_l, global_m, global_eta])
    basis = subject.facet_basis()
    full_directions = np.column_stack([global_l, global_m, 1.0 + global_eta])
    local = full_directions @ basis
    local_directions = np.column_stack([local[:, 0], local[:, 1], local[:, 2] - 1.0])

    global_phase = rows["uvw_lambda"] @ (global_directions[1] - global_directions[0])
    local_phase = subject.rotate_uvw_to_facet(rows) @ (
        local_directions[1] - local_directions[0]
    )

    np.testing.assert_allclose(
        local_phase,
        global_phase,
        rtol=2.0e-12,
        atol=2.0e-12,
    )


def test_parallel_hand_shape_guard_rejects_missing_route_coefficients() -> None:
    state = np.asarray([0, 0], dtype=np.uint16)
    geometry = tuple(np.asarray([10, 10], dtype=np.int16) for _ in range(4))
    coefficients = np.ones((1, 2), dtype=np.complex128)
    try:
        subject.stable_segment(state, geometry, coefficients, 1)
    except subject.ConstructionError as error:
        assert "different lengths" in str(error)
    else:
        raise AssertionError("route/coefficient shape mismatch unexpectedly passed")


def test_parallel_hand_weights_and_prediction_normalizations_are_applied_once() -> None:
    rows = np.zeros(2, dtype=subject.load_graph_module().ROW_DTYPE)
    rows["weight"] = [2.0, 3.0]
    rows["sumwt_factor"] = [2.0, 2.0]
    rows["first_prediction_normalization"] = [2.0 + 0.0j, 1.0 + 1.0j]
    rows["second_prediction_normalization"] = [4.0 + 0.0j, 2.0 - 2.0j]

    inverse = subject.expanded_prediction_inverse_normalization(rows)

    np.testing.assert_allclose(
        inverse,
        [
            0.5 + 0.0j,
            0.25 + 0.0j,
            0.5 + 0.5j,
            0.25 - 0.25j,
        ],
    )
    np.testing.assert_allclose(
        np.repeat(np.asarray(rows["weight"], dtype=np.float64), 2) * inverse,
        [
            1.0 + 0.0j,
            0.5 + 0.0j,
            1.5 + 1.5j,
            0.75 - 0.75j,
        ],
    )
