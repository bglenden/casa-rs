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


def test_controlled_kernel_lut_is_symmetric_and_subpixel_sensitive() -> None:
    assert subject.controlled_kernel_weight(0, -2) == subject.controlled_kernel_weight(
        0, 2
    )
    assert subject.controlled_kernel_weight(25, 1) != subject.controlled_kernel_weight(
        -25, 1
    )
    assert subject.controlled_kernel_weight(0, 0) == 1.0


def test_parallel_hand_shape_guard_rejects_missing_route_coefficients() -> None:
    state = np.asarray([0, 0], dtype=np.uint16)
    geometry = tuple(
        np.asarray([10, 10], dtype=np.int16) for _ in range(4)
    )
    coefficients = np.ones((1, 2), dtype=np.complex128)
    try:
        subject.stable_segment(state, geometry, coefficients, 1)
    except subject.ConstructionError as error:
        assert "different lengths" in str(error)
    else:
        raise AssertionError("route/coefficient shape mismatch unexpectedly passed")
