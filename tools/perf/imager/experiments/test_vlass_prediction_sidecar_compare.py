#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_prediction_sidecar_compare as subject  # noqa: E402


class PredictionSidecarCompareTests(unittest.TestCase):
    def test_binary_layout_matches_metal_contract(self) -> None:
        self.assertEqual(subject.AUDIT_DTYPE.itemsize, 104)
        self.assertEqual(subject.RESULT_DTYPE.itemsize, 16)
        self.assertEqual(subject.AUDIT_DTYPE.fields["first_model_term0_re"][1], 24)
        self.assertEqual(subject.AUDIT_DTYPE.fields["second_model_term0_re"][1], 64)

    def test_literal_taylor_pair_uses_two_terms(self) -> None:
        records = np.zeros(1, dtype=subject.AUDIT_DTYPE)
        records["taylor_power0"] = np.float32(1.0)
        records["taylor_power1"] = np.float32(0.25)
        records["first_model_term0_re"] = np.float32(1.0)
        records["first_model_term0_im"] = np.float32(2.0)
        records["first_model_term1_re"] = np.float32(4.0)
        records["first_model_term1_im"] = np.float32(-4.0)
        records["second_model_term0_re"] = np.float32(3.0)
        records["second_model_term1_im"] = np.float32(8.0)

        actual = subject.literal_taylor_pair(records)

        np.testing.assert_array_equal(
            actual,
            np.array([[2.0 + 1.0j, 3.0 + 2.0j]], dtype=np.complex64),
        )

    def test_canonical_residuals_undo_mueller_role_swap(self) -> None:
        audit = np.zeros(2, dtype=subject.AUDIT_DTYPE)
        audit["first_imaging_mueller"] = np.array([0, 15], dtype=np.uint32)
        audit["second_imaging_mueller"] = np.array([15, 0], dtype=np.uint32)
        results = np.zeros(2, dtype=subject.RESULT_DTYPE)
        results["first_residual_re"] = np.array([1.0, 20.0], dtype=np.float32)
        results["second_residual_re"] = np.array([2.0, 10.0], dtype=np.float32)

        actual = subject.canonical_returned_residuals(audit, results)

        np.testing.assert_array_equal(
            actual,
            np.array([[1.0, 2.0], [10.0, 20.0]], dtype=np.complex64),
        )

    def test_first_pair_mismatch_reports_source_and_role(self) -> None:
        expected = np.zeros((3, 2), dtype=np.complex64)
        actual = expected.copy()
        actual[2, 1] = np.complex64(1.0 + 0.0j)

        self.assertEqual(subject.first_pair_mismatch(actual, expected), (2, 1))
        self.assertIsNone(subject.first_pair_mismatch(expected, expected))


if __name__ == "__main__":
    unittest.main()
