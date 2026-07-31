#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS quotient-response CUR reducer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

import numpy as np


MODULE_PATH = Path(__file__).with_name("vlass_quotient_response_cur.py")
SPEC = importlib.util.spec_from_file_location("vlass_quotient_response_cur", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
cur = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cur
SPEC.loader.exec_module(cur)


def row_metadata(rows: int) -> list[dict]:
    """Return metadata spanning every reducer stratum."""

    return [
        {
            "frequency_hz": 2.0e9 + index * 1.0e6,
            "represented_w_lambda": float(index - rows // 2),
            "mueller_element": index % 2,
        }
        for index in range(rows)
    ]


class VlassQuotientResponseCurTest(unittest.TestCase):
    def analyze(self, matrix: np.ndarray, *, max_rank: int = 2) -> dict:
        """Analyze a small half-train, half-holdout synthetic matrix."""

        rows, columns = matrix.shape
        return cur.analyze_matrix(
            matrix.astype(np.complex128),
            train_rows=rows // 2,
            train_columns=columns // 2,
            row_weights=np.linspace(0.5, 1.5, rows),
            row_metadata=row_metadata(rows),
            unique_response_rows=100,
            component_atoms=200,
            plan_references=300,
            max_rank=max_rank,
        )

    def test_low_rank_cross_block_promotes_only_exact_discriminator(self) -> None:
        rng = np.random.default_rng(7)
        left = rng.normal(size=(32, 2)) + 1j * rng.normal(size=(32, 2))
        right = rng.normal(size=(2, 24)) + 1j * rng.normal(size=(2, 24))
        result = self.analyze(left @ right)
        self.assertEqual(
            result["selection"]["decision"],
            "promote-exact-prepared-atom-cur-discriminator",
        )
        self.assertEqual(result["selection"]["first_survivor_rank"], 2)

    def test_high_rank_cross_block_retires_current_quotient(self) -> None:
        rng = np.random.default_rng(11)
        matrix = rng.normal(size=(32, 24)) + 1j * rng.normal(size=(32, 24))
        result = self.analyze(matrix)
        self.assertEqual(
            result["selection"]["decision"],
            "retire-current-quotient-response-cur-for-high-operator-core-rank",
        )
        self.assertIsNone(result["selection"]["first_survivor_rank"])

    def test_rejects_nonfinite_or_invalid_weights(self) -> None:
        matrix = np.ones((8, 8), dtype=np.complex128)
        matrix[0, 0] = np.nan
        with self.assertRaisesRegex(cur.QuotientResponseError, "non-finite"):
            self.analyze(matrix, max_rank=1)
        matrix[0, 0] = 1.0
        with self.assertRaisesRegex(cur.QuotientResponseError, "positive"):
            cur.analyze_matrix(
                matrix,
                train_rows=4,
                train_columns=4,
                row_weights=np.zeros(8),
                row_metadata=row_metadata(8),
                unique_response_rows=10,
                component_atoms=10,
                plan_references=10,
                max_rank=1,
            )


if __name__ == "__main__":
    unittest.main()
