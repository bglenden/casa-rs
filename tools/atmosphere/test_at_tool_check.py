# SPDX-License-Identifier: LGPL-3.0-or-later
"""Fault cases for `at_tool_check.compare`: it must refuse to report success
when a result is missing, short, extra or not finite, as well as when a
gated quantity is out of tolerance. Needs no CASA:

  python3 -m pytest -q tools/atmosphere/test_at_tool_check.py
"""

from __future__ import annotations

import copy
import math
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import at_tool_check  # noqa: E402

CONFIG = {
    "profiles": [{"name": "a"}, {"name": "b"}],
    "bands": [{"centre_ghz": 90.0, "channels": 2}, {"centre_ghz": 230.0, "channels": 3}],
}


def result(scale: float = 1.0) -> dict:
    """A plausible result for CONFIG: every quantity, finite."""
    windows = []
    for band in CONFIG["bands"]:
        window = {
            key: [scale * (1.0 + 0.1 * c + 0.01 * k) for c in range(band["channels"])]
            for k, key in enumerate(at_tool_check.CHANNEL_QUANTITIES)
        }
        for key in at_tool_check.WINDOW_QUANTITIES:
            window[key] = scale * 7.5
        windows.append(window)
    return {"ground_water_m": scale * 1.5e-3, "windows": windows}


def casa() -> list:
    return [result() for _ in CONFIG["profiles"]]


def rust() -> dict:
    return {"profiles": [result() for _ in CONFIG["profiles"]]}


class CompareTests(unittest.TestCase):
    def problems(self, casa_profiles, rust_result):
        return at_tool_check.compare(CONFIG, casa_profiles, rust_result)[1]

    def assertRefused(self, casa_profiles, rust_result, fragment):
        problems = self.problems(casa_profiles, rust_result)
        self.assertTrue(problems, "a faulty result was accepted")
        self.assertTrue(any(fragment in p for p in problems), problems)

    def test_identical_results_pass(self):
        worst, problems = at_tool_check.compare(CONFIG, casa(), rust())
        self.assertEqual(problems, [])
        expected = {*at_tool_check.CHANNEL_QUANTITIES, *at_tool_check.WINDOW_QUANTITIES, "ground_water_m"}
        self.assertEqual(set(worst), expected)
        self.assertTrue(all(d == 0.0 for d, _ in worst.values()))

    def test_no_profiles_is_refused(self):
        self.assertRefused(casa(), {"profiles": []}, "casa-rs gave 0 profiles for 2")

    def test_missing_profiles_key_is_refused(self):
        self.assertRefused(casa(), {}, "casa-rs gave no profiles")

    def test_a_short_at_tool_run_is_refused(self):
        self.assertRefused(casa()[:1], rust(), "the at tool gave 1 profiles for 2")

    def test_an_extra_profile_is_refused(self):
        extra = rust()
        extra["profiles"].append(result())
        self.assertRefused(casa(), extra, "casa-rs gave 3 profiles for 2")

    def test_a_missing_window_is_refused(self):
        short = rust()
        short["profiles"][1]["windows"].pop()
        self.assertRefused(casa(), short, "b: casa-rs gave 1 windows for 2")

    def test_missing_windows_are_refused(self):
        bare = rust()
        del bare["profiles"][0]["windows"]
        self.assertRefused(casa(), bare, "a: casa-rs gave no windows for 2")

    def test_an_empty_tebb_array_is_refused(self):
        empty = rust()
        empty["profiles"][0]["windows"][1]["tebb"] = []
        self.assertRefused(casa(), empty, "band 230.0 GHz tebb: casa-rs has 0 and the at tool 3 channels for 3")

    def test_a_short_at_tool_array_is_refused(self):
        short = casa()
        short[1]["windows"][0]["dry"].pop()
        self.assertRefused(short, rust(), "b band 90.0 GHz dry: casa-rs has 2 and the at tool 1 channels for 2")

    def test_all_nan_tebb_is_refused(self):
        nan = rust()
        for window in nan["profiles"][0]["windows"]:
            window["tebb"] = [math.nan] * len(window["tebb"])
        self.assertRefused(casa(), nan, "tebb: not a finite pair")

    def test_infinite_at_tool_value_is_refused(self):
        inf = casa()
        inf[0]["windows"][0]["wet"][1] = math.inf
        self.assertRefused(inf, rust(), "wet: not a finite pair")

    def test_a_missing_quantity_is_refused(self):
        missing = rust()
        for profile in missing["profiles"]:
            for window in profile["windows"]:
                del window["tebb_set"]
        self.assertRefused(casa(), missing, "tebb_set: casa-rs has None")

    def test_a_missing_window_average_is_refused(self):
        missing = casa()
        del missing[0]["windows"][0]["average_trj"]
        self.assertRefused(missing, rust(), "average_trj: not a finite pair")

    def test_a_non_numeric_value_is_refused(self):
        text = rust()
        text["profiles"][0]["windows"][0]["dry"][0] = "0.01"
        self.assertRefused(casa(), text, "dry: not a finite pair")

    def test_a_missing_ground_water_column_is_refused(self):
        bare = rust()
        del bare["profiles"][1]["ground_water_m"]
        self.assertRefused(casa(), bare, "ground_water_m: not a finite pair")

    def test_a_zero_at_tool_value_against_a_nonzero_one_is_refused(self):
        zero = casa()
        zero[0]["windows"][0]["co"][0] = 0.0
        self.assertRefused(zero, rust(), "co: difference not finite")

    def test_a_gated_quantity_beyond_tolerance_is_refused(self):
        off = rust()
        off["profiles"][0]["windows"][0]["tebb"][0] *= 1 + 2e-6
        self.assertRefused(casa(), off, "tebb: 2.000e-06")

    def test_an_ungated_quantity_beyond_tolerance_is_reported_not_refused(self):
        off = rust()
        off["profiles"][0]["windows"][0]["o3_path"][0] *= 1 + 1e-3
        worst, problems = at_tool_check.compare(CONFIG, casa(), off)
        self.assertEqual(problems, [])
        self.assertAlmostEqual(worst["o3_path"][0], 1e-3, places=12)

    def test_a_result_differing_within_tolerance_passes(self):
        close = copy.deepcopy(rust())
        close["profiles"][1]["windows"][1]["tebb"][2] *= 1 + 1e-9
        self.assertEqual(self.problems(casa(), close), [])


if __name__ == "__main__":
    unittest.main()
