from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from casa_rust_solver_crosscheck import (
    normalized_sample_errors,
    visibility_product_identities,
)
from casa_solver_support import copy_seed, normalize


class CasaSolverSupportTests(unittest.TestCase):
    def test_normalize_and_copy_seed_are_shared_without_casa_imports(self) -> None:
        self.assertEqual(
            normalize({"value": np.float32(1.25), "array": np.array([1, 2])}),
            {"value": 1.25, "array": [1, 2]},
        )
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "seed.model").mkdir()
            (root / "seed.model" / "payload").write_text("model")
            copy_seed(root / "seed", root / "copy", ("model",))
            self.assertEqual((root / "copy.model" / "payload").read_text(), "model")

    def test_visibility_product_encoding_commits_exact_lineage_and_residual(self) -> None:
        diagnostic = {
            "problem_id": "11" * 32,
            "final_model_generation": "22" * 32,
            "selected_generation": "33" * 32,
            "weighting_generation": "44" * 32,
        }
        sample = {
            "row": 2,
            "data_description_id": 3,
            "spectral_window_id": 4,
            "channel": 5,
            "frequency_centre_hz": 1.25e9,
            "frequency_lower_hz": 1.2495e9,
            "frequency_upper_hz": 1.2505e9,
            "channel_width_hz": 1.0e6,
            "frequency_frame_tag": 0,
            "polarization_id": 6,
            "correlation": 1,
            "correlation_type": 6,
            "prediction": complex(1.5, -2.25),
            "residual": complex(3.0, 4.5),
        }

        model, residual = visibility_product_identities(
            diagnostic, "55" * 32, [sample]
        )
        self.assertEqual(
            model,
            "0f8809b8d029d92e9466c9a1b5526dbe1c45082f6c7798c5bb1b7296fe4e7596",
        )
        self.assertEqual(
            residual,
            "bdaa9cc2e0614ecb19fc10ae00e532d666bb52d9dc66075b237103d92a51cf96",
        )
        sample["residual"] = complex(3.0, 4.25)
        self.assertNotEqual(
            visibility_product_identities(diagnostic, "55" * 32, [sample])[1],
            residual,
        )

    def test_visibility_comparison_retains_each_complex_sample_error(self) -> None:
        expected = np.array([complex(1.0, 2.0), complex(-3.0, 4.0)])
        actual = np.array([complex(1.0, 2.0), complex(-3.0, 4.5)])
        errors = normalized_sample_errors(actual, expected)
        self.assertEqual(errors.shape, (2,))
        self.assertEqual(errors[0], 0.0)
        self.assertGreater(errors[1], 0.0)


if __name__ == "__main__":
    unittest.main()
