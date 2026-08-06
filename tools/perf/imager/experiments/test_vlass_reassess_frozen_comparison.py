#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for frozen full-array comparison reassessment."""

from __future__ import annotations

import copy
import unittest

import vlass_reassess_frozen_comparison as subject


class FrozenComparisonReassessmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = {
            "contract_version": 2,
            "default": {"require_topology_parity": True},
            "products": {".alpha": {"mask_mismatch_fraction": 1.0e-6}},
        }
        self.comparison = {
            "products": {
                ".alpha": {
                    "status": "topology_mismatch",
                    "full_array": {
                        "total_elements": 10_000_000,
                        "topology": {
                            "mask_mismatch_count": 10,
                            "finite_equal": True,
                            "nonfinite_kind_equal": True,
                        },
                    },
                }
            }
        }

    def test_promotes_only_bounded_mask_topology(self) -> None:
        promotions = subject.promote_bounded_mask_products(
            self.comparison,
            self.contract,
        )

        self.assertEqual("compared", self.comparison["products"][".alpha"]["status"])
        self.assertEqual(1, len(promotions))
        self.assertEqual(1.0e-6, promotions[0]["mask_mismatch_fraction"])

    def test_does_not_promote_non_mask_topology_or_unnamed_product(self) -> None:
        finite_mismatch = copy.deepcopy(self.comparison)
        finite_mismatch["products"][".alpha"]["full_array"]["topology"][
            "finite_equal"
        ] = False
        self.assertEqual(
            [],
            subject.promote_bounded_mask_products(finite_mismatch, self.contract),
        )

        unnamed = copy.deepcopy(self.comparison)
        unnamed["products"][".image.tt0"] = unnamed["products"].pop(".alpha")
        self.assertEqual(
            [],
            subject.promote_bounded_mask_products(unnamed, self.contract),
        )

    def test_does_not_promote_mask_mismatch_above_contract_ceiling(self) -> None:
        over_ceiling = copy.deepcopy(self.comparison)
        over_ceiling["products"][".alpha"]["full_array"]["topology"][
            "mask_mismatch_count"
        ] = 11

        self.assertEqual(
            [],
            subject.promote_bounded_mask_products(over_ceiling, self.contract),
        )
        self.assertEqual(
            "topology_mismatch",
            over_ceiling["products"][".alpha"]["status"],
        )


if __name__ == "__main__":
    unittest.main()
