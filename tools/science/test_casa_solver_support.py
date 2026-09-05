from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from casa_rust_solver_crosscheck import (
    assert_controlled_source_clones,
    compare_per_scale_recovered_flux,
    controlled_source_identity,
    normalized_sample_errors,
    visibility_product_identities,
)
from casa_solver_support import (
    controlled_point_extended_fixture,
    copy_seed,
    materialize_solver_seed,
    normalize,
)


class CasaSolverSupportTests(unittest.TestCase):
    def test_both_solver_gates_share_the_exact_bounded_seed_geometry(self) -> None:
        calls = []

        def fake_tclean(**parameters) -> None:
            calls.append(parameters)

        prefix = pathlib.Path("controlled")
        self.assertEqual(
            materialize_solver_seed(fake_tclean, pathlib.Path("input.ms"), prefix),
            prefix,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["imsize"], [64, 64])
        self.assertEqual(calls[0]["cell"], "0.02arcsec")
        self.assertEqual(calls[0]["datacolumn"], "data")
        self.assertEqual(calls[0]["field"], "1")
        self.assertEqual(calls[0]["spw"], "1")

    def test_controlled_point_extended_source_is_identical_for_both_sides(self) -> None:
        psf = np.zeros((64, 64), dtype=np.float64)
        psf[32, 32] = 1.0
        sky, dirty = controlled_point_extended_fixture(psf)
        np.testing.assert_allclose(dirty, sky, rtol=0.0, atol=1.0e-12)
        self.assertGreater(sky[22, 21], 4.0)
        self.assertGreater(np.count_nonzero(sky > 1.0e-3), 1)

        rows = [
            np.asarray([[1.0 + 2.0j, 3.0 - 4.0j]], dtype=np.complex64),
            np.asarray([[5.0 + 6.0j]], dtype=np.complex64),
        ]
        identity = controlled_source_identity(rows)
        self.assertEqual(
            assert_controlled_source_clones(identity, rows, [row.copy() for row in rows]),
            identity,
        )
        changed = [row.copy() for row in rows]
        changed[1][0, 0] += np.complex64(0.25)
        with self.assertRaisesRegex(AssertionError, "DATA differs at row 1"):
            assert_controlled_source_clones(identity, rows, changed)

    def test_per_scale_recovered_flux_checks_every_component(self) -> None:
        rust = [
            {"scale_px": 0, "flux": 1.25},
            {"scale_px": 7, "flux": 2.5},
            {"scale_px": 0, "flux": -0.25},
        ]
        casa = [
            {
                "scale_px": 0,
                "per_scale_recovered_flux": {"0": 1.25, "7": 0.0},
            },
            {
                "scale_px": 7,
                "per_scale_recovered_flux": {"0": 1.25, "7": 2.5},
            },
            {
                "scale_px": 0,
                "per_scale_recovered_flux": {"0": 1.0, "7": 2.5},
            },
        ]
        self.assertEqual(
            compare_per_scale_recovered_flux("controlled", [0, 7], rust, casa),
            {"0": 1.0, "7": 2.5},
        )

        mismatched = [dict(component) for component in casa]
        mismatched[1] = dict(mismatched[1], scale_px=0)
        with self.assertRaisesRegex(AssertionError, "component 1 scale"):
            compare_per_scale_recovered_flux(
                "controlled", [0, 7], rust, mismatched
            )

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
