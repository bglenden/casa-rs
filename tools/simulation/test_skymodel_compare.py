# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for `skymodel_compare.py`, the comparisons of `skymodel_oracle.py
--check`: they must fail what differs, not only pass what agrees."""

from __future__ import annotations

import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from skymodel_compare import pixel_problems, record_differences  # noqa: E402


class PixelTest(unittest.TestCase):
    def test_equal_pixels_pass_and_small_differences_are_measured(self) -> None:
        problems, worst = pixel_problems("x", [1.0, 2.0, 4.0], [1.0, 2.0, 4.0 + 1e-7])
        self.assertEqual(problems, [])
        self.assertAlmostEqual(worst, 2.5e-8)

    def test_a_large_difference_fails(self) -> None:
        problems, _ = pixel_problems("x", [1.0, 2.0], [1.0, 2.1])
        self.assertEqual(len(problems), 1)

    def test_non_finite_pixels_must_meet_non_finite_ones(self) -> None:
        # NaN everywhere against finite values fails, as does finite against
        # a recorded NaN; NaN against NaN passes.
        self.assertTrue(pixel_problems("x", [None, None], [1.0, 2.0])[0])
        self.assertTrue(pixel_problems("x", [float("nan"), 2.0], [1.0, 2.0])[0])
        self.assertTrue(pixel_problems("x", [0.0, 2.0], [None, 2.0])[0])
        self.assertEqual(pixel_problems("x", [None, 2.0], [None, 2.0])[0], [])

    def test_masks_must_agree_and_masked_values_are_not_compared(self) -> None:
        self.assertTrue(pixel_problems("x", [1.0, 9.0], [1.0, 0.0], [True, True], [True, False])[0])
        self.assertTrue(pixel_problems("x", [1.0, 9.0], [1.0, 0.0], None, [True, False])[0])
        self.assertEqual(pixel_problems("x", [1.0, 9.0], [1.0, 0.0], [True, False], [True, False])[0], [])

    def test_lengths_must_agree(self) -> None:
        problems, worst = pixel_problems("x", [1.0], [1.0, 2.0])
        self.assertTrue(problems)
        self.assertEqual(worst, math.inf)


class RecordTest(unittest.TestCase):
    def differences(self, actual: object, expected: object, skipped: tuple[str, ...] = ()) -> list[str]:
        found: list[str] = []
        record_differences(actual, expected, "csys", found.append, skipped)
        return found

    def test_numbers_strings_and_keys_are_compared(self) -> None:
        self.assertEqual(self.differences({"a": [1.0, "J2000"]}, {"a": [1.0 + 1e-12, "J2000"]}), [])
        self.assertTrue(self.differences({"a": [1.1]}, {"a": [1.0]}))
        self.assertTrue(self.differences({"a": "B1950"}, {"a": "J2000"}))
        self.assertTrue(self.differences({}, {"a": 1}))
        self.assertTrue(self.differences({"a": [1, 2]}, {"a": [1]}))

    def test_skipped_keys_are_left_out(self) -> None:
        self.assertEqual(self.differences({"worldreplace0": [0.0]}, {"worldreplace0": [1.0]}, ("worldreplace",)), [])


if __name__ == "__main__":
    unittest.main()
