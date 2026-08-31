from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from t43_mtmfs_clean_compare import ComparisonError, compare_documents


def casa_result() -> dict[str, object]:
    return {
        "iterdone": 8,
        "stopcode": 1,
        "stopDescription": "iteration limit",
        "summaryminor": {
            "0": {
                "0": {
                    "0": {
                        "iterDone": [2.0, 2.0, 2.0, 2.0],
                        "cycleThresh": [0.024, 0.022, 0.020, 0.019],
                        "peakRes": [0.030, 0.027, 0.025, 0.023],
                        "modelFlux": [0.010, 0.001, 0.004, 0.0042],
                    }
                }
            }
        },
    }


def product_arrays() -> dict[str, np.ndarray]:
    return {
        "model_tt0": np.asarray([[1.0, 2.0], [0.0, 0.0]]),
        "model_tt1": np.asarray([[0.2, 0.4], [0.0, 0.0]]),
        "residual_tt0": np.asarray([[0.4, -0.1], [0.2, 0.3]]),
        "residual_tt1": np.asarray([[0.08, -0.02], [0.04, 0.06]]),
    }


def casa_products() -> dict[str, tuple[np.ndarray, np.ndarray]]:
    return {
        name: (values.copy(), np.ones(values.shape, dtype=bool))
        for name, values in product_arrays().items()
    }


def rust_document(scale: float = 1.0) -> dict[str, object]:
    result = casa_result()
    leaf = result["summaryminor"]["0"]["0"]["0"]  # type: ignore[index]
    cycles = []
    for index in range(4):
        cycles.append(
            {
                "iterations": 2,
                "cycle_threshold": leaf["cycleThresh"][index] * scale,
                "peak_residual": leaf["peakRes"][index] * scale,
                "model_flux": leaf["modelFlux"][index] * scale,
                "stop_reason": "iteration_bound",
            }
        )
    products = {
        name: (values * scale).reshape(-1).tolist()
        for name, values in product_arrays().items()
    }
    return {
        "schema": "casa-rs-t43-mtmfs-clean-v1",
        "geometry": {"shape": [2, 2], "layout": "x,y"},
        "trajectory": {
            "cycles": cycles,
            "total_iterations": 8,
            "stop_reason": "iteration_limit",
        },
        "products": products,
    }


class T43MtmfsCleanComparisonTests(unittest.TestCase):
    def test_accepts_the_declared_nrms_envelope_without_component_order(self) -> None:
        summary = compare_documents(casa_result(), casa_products(), rust_document(1.0005))
        self.assertTrue(summary["pass"])
        self.assertFalse(summary["contract"]["component_order_normative"])
        self.assertLessEqual(
            summary["spectral_behavior"]["normalized_rms"],
            summary["spectral_behavior"]["ceiling"],
        )

    def test_rejects_a_scientifically_wrong_taylor_coefficient(self) -> None:
        rust = rust_document()
        rust["products"]["model_tt1"][0] = 0.3  # type: ignore[index]
        summary = compare_documents(casa_result(), casa_products(), rust)
        self.assertFalse(summary["pass"])
        self.assertIn(
            "recovered_spectral_behavior_normalized_rms", summary["failures"]
        )

    def test_rejects_the_wrong_cycle_cardinality(self) -> None:
        rust = rust_document()
        rust["trajectory"]["cycles"].pop()  # type: ignore[index]
        with self.assertRaisesRegex(ComparisonError, "must contain 4 cycles"):
            compare_documents(casa_result(), casa_products(), rust)


if __name__ == "__main__":
    unittest.main()
