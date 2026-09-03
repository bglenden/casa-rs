from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from compare_model_data import (
    FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE,
    arrays_bitwise_equal,
    casa_weight_contract_satisfied,
    float32_weight_roundtrip_metrics,
)


class CompareModelDataTests(unittest.TestCase):
    def test_bitwise_comparison_detects_signed_zero(self) -> None:
        positive_zero = np.array([0.0], dtype=np.float64)
        negative_zero = np.array([-0.0], dtype=np.float64)

        self.assertTrue(arrays_bitwise_equal(positive_zero, positive_zero.copy()))
        self.assertFalse(arrays_bitwise_equal(positive_zero, negative_zero))

    def test_accepts_measured_casa_float32_weight_roundtrip(self) -> None:
        source = np.array([0.08700665, 0.07936401], dtype=np.float64)
        casa = np.array([0.08700664, 0.07936402], dtype=np.float64)

        metrics = float32_weight_roundtrip_metrics(source, casa)

        self.assertTrue(metrics["finite"])
        self.assertTrue(metrics["within_tolerance"])
        self.assertLessEqual(
            metrics["maximum_relative_difference"],
            FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE,
        )

    def test_rejects_nonfinite_or_larger_weight_drift(self) -> None:
        source = np.array([0.1, 0.2], dtype=np.float64)
        drifted = source * (1.0 + 3.0 * FLOAT32_WEIGHT_ROUNDTRIP_RELATIVE_TOLERANCE)
        nonfinite = np.array([0.1, np.inf], dtype=np.float64)

        self.assertFalse(
            float32_weight_roundtrip_metrics(source, drifted)["within_tolerance"]
        )
        self.assertFalse(float32_weight_roundtrip_metrics(source, nonfinite)["finite"])

    def test_roundtrip_allowance_is_continuum_residual_only(self) -> None:
        self.assertTrue(
            casa_weight_contract_satisfied(
                "continuum_residual",
                bitwise_unchanged=False,
                within_float32_roundtrip_tolerance=True,
            )
        )
        self.assertFalse(
            casa_weight_contract_satisfied(
                "model_data",
                bitwise_unchanged=False,
                within_float32_roundtrip_tolerance=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
