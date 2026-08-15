#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import struct
import sys
import unittest
from pathlib import Path

import numpy as np


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_prediction_boundary_hash_compare as subject  # noqa: E402


class PredictionBoundaryHashCompareTests(unittest.TestCase):
    def test_selection_uses_source_row_channel_order_and_parallel_hand_contract(self) -> None:
        observed = np.zeros((2, 4, 2), dtype=np.complex64)
        model = np.zeros_like(observed)
        flags = np.zeros_like(observed, dtype=np.bool_)
        weights = np.ones((2, 4), dtype=np.float32)
        observed[0, 0, 0] = np.complex64(1.0 + 2.0j)
        observed[0, 3, 0] = np.complex64(3.0 + 4.0j)
        observed[0, 0, 1] = np.complex64(5.0 + 6.0j)
        observed[0, 3, 1] = np.complex64(7.0 + 8.0j)
        flags[0, 3, 1] = True
        trace = {
            "observed_data": observed,
            "model_data": model,
            "flag": flags,
            "weight": weights,
            "uv_range_selected": np.array([True, True]),
            "antenna1": np.array([0, 1]),
            "antenna2": np.array([1, 1]),
            "row_id": np.array([10, 11]),
            "data_description_id": np.array([2, 2]),
        }

        selected, selected_model, census = subject.selected_parallel_hands(trace)

        np.testing.assert_array_equal(
            selected,
            np.array([[1.0 + 2.0j, 3.0 + 4.0j]], dtype=np.complex64),
        )
        np.testing.assert_array_equal(
            selected_model,
            np.zeros((1, 2), dtype=np.complex64),
        )
        self.assertEqual(census["samples_admitted"], 1)

    def test_selection_groups_rows_by_ddid_before_channel_order(self) -> None:
        observed = np.zeros((2, 4, 1), dtype=np.complex64)
        observed[0, 0, 0] = 2.0
        observed[0, 3, 0] = 20.0
        observed[1, 0, 0] = 1.0
        observed[1, 3, 0] = 10.0
        trace = {
            "observed_data": observed,
            "model_data": np.zeros_like(observed),
            "flag": np.zeros_like(observed, dtype=np.bool_),
            "weight": np.ones((2, 4), dtype=np.float32),
            "uv_range_selected": np.array([True, True]),
            "antenna1": np.array([0, 0]),
            "antenna2": np.array([1, 1]),
            "row_id": np.array([10, 11]),
            "data_description_id": np.array([7, 2]),
        }

        selected, _, census = subject.selected_parallel_hands(trace)

        np.testing.assert_array_equal(
            selected,
            np.array([[1.0, 10.0], [2.0, 20.0]], dtype=np.complex64),
        )
        self.assertEqual(census["ddid_execution_order"], [2, 7])

    def test_hash_is_little_endian_ordinal_then_rr_and_ll_float_bits(self) -> None:
        values = np.array([[1.0 + 2.0j, 3.0 + 4.0j]], dtype=np.complex64)
        expected = hashlib.sha256(
            struct.pack("<Qffff", 0, 1.0, 2.0, 3.0, 4.0)
        ).hexdigest()

        self.assertEqual(subject.hash_parallel_hands(values), expected)

    def test_source_trace_applies_exact_phasor_and_validates_collapse(self) -> None:
        observed = np.zeros((1, 4, 1), dtype=np.complex64)
        model = np.zeros_like(observed)
        observed[0, 0, 0] = np.complex64(1.0 + 2.0j)
        observed[0, 3, 0] = np.complex64(3.0 + 4.0j)
        model[0, 0, 0] = np.complex64(0.5 + 0.25j)
        model[0, 3, 0] = np.complex64(0.75 + 0.5j)
        trace = {
            "observed_data": observed,
            "model_data": model,
            "flag": np.zeros_like(observed, dtype=np.bool_),
            "weight": np.ones((1, 4), dtype=np.float32),
            "uv_range_selected": np.array([True]),
            "antenna1": np.array([0]),
            "antenna2": np.array([1]),
            "row_id": np.array([10]),
            "data_description_id": np.array([2]),
            "spectral_window_id": np.array([2]),
        }
        source_trace = {
            "samples": [
                {
                    "source_ordinal": 0,
                    "row_id": 10,
                    "ddid": 2,
                    "spw_id": 2,
                    "channel": 0,
                    "phase_re_bits": subject.f32_bits(np.float32(1.0)),
                    "phase_im_bits": subject.f32_bits(np.float32(0.0)),
                    "collapsed_visibility_re_bits": subject.f32_bits(np.float32(2.0)),
                    "collapsed_visibility_im_bits": subject.f32_bits(np.float32(3.0)),
                }
            ]
        }

        selected, selected_model, census = subject.source_trace_parallel_hands(
            trace,
            source_trace,
        )

        np.testing.assert_array_equal(
            selected,
            np.array([[1.0 + 2.0j, 3.0 + 4.0j]], dtype=np.complex64),
        )
        np.testing.assert_array_equal(
            selected_model,
            np.array([[0.5 + 0.25j, 0.75 + 0.5j]], dtype=np.complex64),
        )
        self.assertEqual(census["collapsed_visibility_bit_mismatches"], 0)

    def test_recovered_prediction_uses_two_f32_subtractions(self) -> None:
        observed = np.array([[1.0 + 0.0j, 2.0 + 0.0j]], dtype=np.complex64)
        prediction = np.array(
            [[np.nextafter(np.float32(1.0), np.float32(0.0)) + 0.0j, 0.5 + 0.0j]],
            dtype=np.complex64,
        )

        residual, recovered = subject.casa_f32_residual_and_recovered_prediction(
            observed,
            prediction,
        )

        np.testing.assert_array_equal(
            residual,
            np.asarray(observed - prediction, dtype=np.complex64),
        )
        np.testing.assert_array_equal(
            recovered,
            np.asarray(observed - residual, dtype=np.complex64),
        )


if __name__ == "__main__":
    unittest.main()
