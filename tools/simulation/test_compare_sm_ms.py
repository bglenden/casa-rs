# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `compare_sm_ms.py`'s MAIN.DATA comparison (#848 and #853 reviews).

DATA must be zero on both sides and, like every other column, defined in
the same cells with the same [correlation, channel] shapes. A predicted or
noisy DATA with a NaN or infinite sample, or a statistic that is not a
number, fails. Run with
`python3 tools/simulation/test_compare_sm_ms.py` (or pytest); only NumPy is
needed.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from compare_sm_ms import compare_model_data, data_differences, ks_failures, noise_classes


def zero_cells(rows: int, correlations: int = 2, channels: int = 4) -> list:
    return [np.zeros((correlations, channels), dtype=np.complex64) for _ in range(rows)]


def aspects(casa: list, casars: list) -> list[str]:
    return [key for key, _ in data_differences(casa, casars)]


class DataComparisonTests(unittest.TestCase):
    def test_zero_cells_of_one_shape_agree(self) -> None:
        self.assertEqual(data_differences(zero_cells(3), zero_cells(3)), [])

    def test_an_undefined_cell_differs(self) -> None:
        # The review's first injected defect: the first casa-rs cell undefined.
        casars = zero_cells(3)
        casars[0] = None
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:defined"])

    def test_a_zero_cell_of_another_shape_differs(self) -> None:
        # The review's second injected defect: a zero cell of shape (1, 1).
        casars = zero_cells(3)
        casars[0] = np.zeros((1, 1))
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:shape"])

    def test_nonzero_data_differs(self) -> None:
        casars = zero_cells(3)
        casars[2] = casars[2] + 1j
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:nonzero", "MAIN.DATA:values"])

    def test_a_missing_row_differs(self) -> None:
        self.assertEqual(aspects(zero_cells(3), zero_cells(2)), ["MAIN.DATA:cells"])


def model_side(data: np.ndarray) -> dict:
    rows = data.shape[0]
    return {"data": data, "flag_row": np.zeros(rows, dtype=bool), "auto": np.zeros(rows, dtype=bool)}


class NonFiniteDataTests(unittest.TestCase):
    """The #853 review's injection: finite CASA DATA against all-NaN casa-rs
    DATA, shape (2, 2, 2), unflagged, returned NRMS NaN and no failures."""

    def test_finite_model_data_within_the_limit_passes(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        stats, failures = compare_model_data(model_side(casa), model_side(casa * (1 + 1e-6)), 1e-3)
        self.assertEqual(failures, [])
        self.assertLess(stats["nrms"], 1e-3)

    def test_nan_model_data_fails(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        ours = np.full((2, 2, 2), np.nan, dtype=np.complex128)
        _, failures = compare_model_data(model_side(casa), model_side(ours), 1e-3)
        self.assertEqual(failures, ["MAIN.DATA: casa-rs: 8 non-finite values"])

    def test_one_infinite_sample_fails_even_on_a_flagged_row(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        ours = casa.copy()
        ours[1, 0, 1] = np.inf
        side = model_side(ours)
        side["flag_row"][1] = True
        _, failures = compare_model_data(model_side(casa), side, 1e-3)
        self.assertEqual(failures, ["MAIN.DATA: casa-rs: 1 non-finite values"])

    def test_nan_noise_fails(self) -> None:
        rng = np.random.default_rng(1)
        noise = (rng.normal(size=(64, 2, 8)) + 1j * rng.normal(size=(64, 2, 8))) * 0.1
        auto = np.zeros(64, dtype=bool)
        self.assertEqual(noise_classes(noise, auto, 0.01)[1], [])
        noise[3, 1, 2] = complex(np.nan, 0.0)
        self.assertEqual(noise_classes(noise, auto, 0.01)[1], ["noise: 1 non-finite values"])

    def test_a_nan_ks_p_value_fails(self) -> None:
        self.assertEqual(ks_failures(0.01, 0.5), [])
        self.assertEqual(len(ks_failures(0.2, 1e-5)), 1)
        self.assertEqual(len(ks_failures(float("nan"), float("nan"))), 1)


if __name__ == "__main__":
    unittest.main()
