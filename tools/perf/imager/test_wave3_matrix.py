#!/usr/bin/env python3
"""Focused tests for the Wave 3 single-plane matrix helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wave3_matrix


class Wave3MatrixTests(unittest.TestCase):
    def test_repository_matrix_has_all_mode_tickets_and_tiers(self) -> None:
        matrix = wave3_matrix.load_matrix(wave3_matrix.MATRIX_PATH)
        rows = wave3_matrix.enumerate_rows(matrix)

        issues = {row["issue"] for row in rows}
        self.assertEqual(set(range(276, 285)), issues)
        for issue in issues:
            tiers = {row["tier"] for row in rows if row["issue"] == issue}
            self.assertEqual({"smoke", "medium", "stress"}, tiers)

    def test_review_contract_requires_before_cpu_gpu_and_casa_roles(self) -> None:
        matrix = wave3_matrix.load_matrix(wave3_matrix.MATRIX_PATH)

        roles = set(matrix["review_contract"]["required_evidence_roles"])

        self.assertIn("before_baseline", roles)
        self.assertIn("after_multi_worker_cpu", roles)
        self.assertIn("after_gpu_metal", roles)
        self.assertIn("casa_cpp", roles)


if __name__ == "__main__":
    unittest.main()
