#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Deterministic checks for the serial timing score and regression gates."""

import math
import unittest

from t55_serial_autoresearch import affected_tests, paired_statistics, pair_order


class PairedTimingTests(unittest.TestCase):
    def test_constant_speedup_survives_host_drift(self):
        parent = [10, 20, 30, 40, 50]
        candidate = [value * 0.8 for value in parent]
        result = paired_statistics(candidate, parent)
        self.assertAlmostEqual(result["ratio"], 0.8)
        for bound in result["approximate_95pct_ratio_interval"]:
            self.assertAlmostEqual(bound, 0.8)

    def test_uses_geometric_paired_ratios_not_ratio_of_medians(self):
        result = paired_statistics([1, 2, 4, 8, 16], [1, 1, 1, 1, 16])
        self.assertAlmostEqual(result["ratio"], 64 ** 0.2)

    def test_mixed_noise_does_not_establish_a_win(self):
        result = paired_statistics([9.5, 10.2, 9.8, 10.4, 9.9], [10] * 5)
        low, high = result["approximate_95pct_ratio_interval"]
        self.assertLess(low, 1)
        self.assertGreater(high, 1)

    def test_rejects_invalid_measurements(self):
        for values in ([1] * 4, [0] * 5, [-1] * 5, [math.nan] * 5, [math.inf] * 5):
            with self.subTest(values=values), self.assertRaises(AssertionError):
                paired_statistics(values, [1] * 5)

    def test_alternates_baseline_and_trial_order(self):
        self.assertEqual(pair_order(0, has_parent=False), ["candidate", "casa"])
        self.assertEqual(pair_order(1, has_parent=False), ["casa", "candidate"])
        self.assertEqual(pair_order(0, has_parent=True), ["parent", "candidate", "casa"])
        self.assertEqual(pair_order(1, has_parent=True), ["casa", "candidate", "parent"])

    def test_storage_changes_select_interoperability(self):
        commands = affected_tests([
            "crates/casa-tables/src/storage/tiled_stman.rs",
            "crates/casa-images/src/lib.rs",
            "crates/casa-ms/src/selected_observation_buffer.rs",
        ])
        flattened = [argument for command in commands for argument in command]
        self.assertIn("tables_cross_matrix_tiled_stman", flattened)
        self.assertIn("images_interop", flattened)
        self.assertIn("ms_data_interop", flattened)
        self.assertEqual(flattened.count("cpp-interop-tests"), 3)

    def test_unrelated_docs_do_not_add_library_tests(self):
        self.assertEqual(affected_tests(["docs/guide.md"]), [])


if __name__ == "__main__":
    unittest.main()
