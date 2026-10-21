# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `compare_sm_ms.py`'s MAIN.DATA comparison (#848 review).

DATA must be zero on both sides and, like every other column, defined in
the same cells with the same [correlation, channel] shapes. Run with
`python3 tools/simulation/test_compare_sm_ms.py` (or pytest); only NumPy is
needed.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from compare_sm_ms import data_differences


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


if __name__ == "__main__":
    unittest.main()
