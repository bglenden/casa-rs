from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

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


if __name__ == "__main__":
    unittest.main()
