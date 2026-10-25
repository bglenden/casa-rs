# SPDX-License-Identifier: LGPL-3.0-or-later
"""The tropospheric phase checks of `troposphere_stats.py` (#841 Phase
4d), on synthetic tables."""

import unittest

import numpy as np

from troposphere_stats import (
    DATA_TOLERANCE,
    data_failures,
    excess_from_table,
    excess_statistics,
    rms_failures,
    slot_of,
)


class ExcessTests(unittest.TestCase):
    def test_one_excess_per_row_is_recovered_through_its_model(self) -> None:
        rng = np.random.default_rng(1)
        delays = np.linspace(26.0, 29.0, 32)
        excess = rng.normal(0.0, 0.02, 50)
        phases = np.angle(np.exp(1j * excess[:, None] * delays[None, :]))
        found, spread = excess_from_table(phases, delays)
        self.assertTrue(np.allclose(found, excess, rtol=1e-9, atol=1e-12))
        self.assertLess(spread.max(), 1e-9)
        # Through another model the excess is not one value per row.
        _, other = excess_from_table(phases, delays * np.linspace(1.0, 1.2, 32))
        self.assertGreater(np.median(other), 0.05)

    def test_channels_near_a_dispersive_zero_are_left_out(self) -> None:
        # The dispersive delay crosses zero above a line: there the quotient
        # is all rounding, and those channels do not count.
        delays = np.array([1.5, 1.0, 0.4, 1e-6, -0.3, -0.1])
        phases = (0.03 * delays + np.array([0, 0, 0, 1e-6, 0, 0]))[None, :]
        found, spread = excess_from_table(phases, delays)
        self.assertAlmostEqual(found[0], 0.03, places=12)
        self.assertLess(spread[0], 1e-12)


class DataTests(unittest.TestCase):
    def test_data_must_be_the_tables_gains(self) -> None:
        expected = np.exp(1j * np.linspace(0, 1, 12)).reshape(3, 4)
        worst, failures = data_failures(expected * (1 + 1e-7), expected)
        self.assertEqual(failures, [])
        self.assertLess(worst, DATA_TOLERANCE)
        _, failures = data_failures(np.conj(expected), expected)
        self.assertEqual(len(failures), 1)
        _, failures = data_failures(expected * np.nan, expected)
        self.assertEqual(len(failures), 1)


class SlotTests(unittest.TestCase):
    def test_solutions_take_the_nearest_slot_the_earlier_of_two(self) -> None:
        # Slots centred 5 s after each 10 s from the first solution: 10 s
        # and 20 s are as near two slots each, and take the earlier.
        times = 5.0e9 + np.array([0.0, 4.0, 10.0, 11.0, 20.0, 21.0])
        self.assertEqual(slot_of(times).tolist(), [0, 0, 0, 1, 1, 2])


class StatisticsTests(unittest.TestCase):
    def test_each_antennas_series_has_one_value_a_slot(self) -> None:
        slots = np.array([0, 0, 1, 1, 2, 2, 0, 1, 2])
        antennas = np.array([0, 0, 0, 0, 0, 0, 1, 1, 1])
        excess = np.array([0.01, 0.01, -0.01, -0.01, 0.01, 0.01, 0.0, 0.02, 0.04])
        found = excess_statistics(excess, antennas, slots)
        self.assertEqual(found[0]["slots"], 3)
        self.assertAlmostEqual(found[0]["mean"], 0.01 / 3)
        self.assertAlmostEqual(found[1]["mean"], 0.02)
        self.assertAlmostEqual(found[1]["rms"], np.sqrt(2 / 3) * 0.02)
        self.assertEqual(found[1]["structure"][0], 0.02**2)
        self.assertEqual(rms_failures({0: {"rms": 0.01}}, 0.01), [])
        self.assertEqual(len(rms_failures({0: {"rms": 0.0101}}, 0.01)), 1)


if __name__ == "__main__":
    unittest.main()
