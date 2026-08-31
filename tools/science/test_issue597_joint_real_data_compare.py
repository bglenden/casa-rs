from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from issue597_joint_real_data_compare import (
    ComparisonError,
    compare_arrays,
    validate_oracle,
)


def products() -> dict[str, dict[str, np.ndarray]]:
    return {
        workflow: {
            product: np.arange(8, dtype=np.float64).reshape(2, 2, 2) + offset
            for offset, product in enumerate(("model", "residual", "image", "psf", "mask"))
        }
        for workflow in ("continuum", "line")
    }


def oracle() -> dict[str, object]:
    return {
        "schema": "casa-rs-issue597-sequential-oracle-v1",
        "role": "sequential_casa_reference_not_a_joint_solver",
        "source": {
            "tree_sha256_excluding_table_lock":
                "ae80d9199e2d313e951b650ed670881bebc8d686eff4b38c017d3df917fb2710"
        },
        "selection": {
            "source_channels": "0:52~67",
            "continuum_anchors": "0:52~59",
            "line_support": "0:60~67",
        },
        "imaging": {
            "image_size": 32,
            "cell": "0.01arcsec",
            "weighting": "natural",
            "deconvolver": "hogbom",
            "threshold": "0.002Jy",
            "niter": 512,
            "cycleniter": 16,
            "gain": 0.1,
        },
        "products": {
            "numeric_archive": "tools/science/issue597_joint_sequential_products.npz",
            "numeric_archive_sha256": "fixture-digest",
        },
        "recipe": {
            "path": "tools/science/issue597_joint_sequential_oracle.py",
            "sha256": "recipe-digest",
        },
    }


class Issue597ComparisonTests(unittest.TestCase):
    def test_accepts_matched_full_products(self) -> None:
        reference = products()
        candidate = {
            workflow: {name: values * 1.0005 for name, values in workflow_products.items()}
            for workflow, workflow_products in reference.items()
        }
        summary = compare_arrays(reference, candidate)
        self.assertTrue(summary["pass"])
        self.assertEqual(summary["contract"]["casa_role"], "sequential_reference_only")

    def test_rejects_a_perturbed_line_product(self) -> None:
        reference = products()
        candidate = {
            workflow: {name: values.copy() for name, values in workflow_products.items()}
            for workflow, workflow_products in reference.items()
        }
        candidate["line"]["model"][0, 0, 0] += 1.0
        summary = compare_arrays(reference, candidate)
        self.assertFalse(summary["pass"])
        self.assertIn("line_model", summary["failures"])

    def test_rejects_wrong_shape(self) -> None:
        reference = products()
        candidate = products()
        candidate["continuum"]["residual"] = np.zeros((2, 2))
        with self.assertRaisesRegex(ComparisonError, "product shapes differ"):
            compare_arrays(reference, candidate)

    def test_rejects_provenance_or_parameter_drift(self) -> None:
        receipt = oracle()
        validate_oracle(receipt)
        receipt["selection"]["line_support"] = "0:61~67"  # type: ignore[index]
        with self.assertRaisesRegex(ComparisonError, "channel selection changed"):
            validate_oracle(receipt)


if __name__ == "__main__":
    unittest.main()
