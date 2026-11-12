# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `simobserve_parity.py` (#920 review), with CASA's tools
replaced by stand-ins.

The re-prediction of a calibrator case reads the case's component list
from `OUT/inputs` whatever the caller's working directory, and the image
comparison fails on a pixel that is not finite, on either side. Run with
`python3 tools/simulation/test_simobserve_parity.py` (or pytest); only
NumPy is needed.
"""

from __future__ import annotations

import os
import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from simobserve_parity import CASES, casars_params, compare_frequency_frames, compare_images, copy_inputs, main, repredict


class ComponentLists:
    """`cl`, recording the lists concatenated, each of which must exist
    where it is named from the working directory."""

    def __init__(self):
        self.concatenated = []

    def done(self):
        pass

    def concatenate(self, path):
        if not os.path.exists(path):
            raise RuntimeError(f"Cannot read componentlist {path}")
        self.concatenated.append(path)

    def rename(self, path):
        os.makedirs(path)


class Simulator:
    """`sm`, recording what it predicts from."""

    def __init__(self):
        self.predicted = None

    def openfromms(self, ms):
        self.ms = ms

    def setdata(self, fieldid):
        pass

    def setvp(self, dovp, usedefaultvp):
        pass

    def predict(self, imagename, complist):
        self.predicted = (imagename, complist)

    def done(self):
        pass


class Images:
    """`ia` over images given by path as their pixels."""

    def __init__(self, pixels: dict):
        self.pixels = pixels
        self.path = None

    def open(self, path):
        self.path = path

    def close(self):
        self.path = None

    def shape(self):
        return list(self.pixels[self.path].shape)

    def brightnessunit(self):
        return "Jy/pixel"

    def getchunk(self):
        return self.pixels[self.path]

    def coordsys(self):
        return Coordinates()


class Coordinates:
    def referencevalue(self):
        return {"numeric": np.array([1.0, 2.0])}

    def referencepixel(self):
        return {"numeric": np.array([3.0, 4.0])}

    def increment(self):
        return {"numeric": np.array([-1e-6, 1e-6])}

    def done(self):
        pass


class RepredictTest(unittest.TestCase):
    def test_the_component_list_is_read_from_out_whatever_the_working_directory(self):
        case = next(case for case in CASES if case["name"] == "calibrator_lists")
        self.assertTrue(case["params"]["complist"].startswith("inputs/"))
        with tempfile.TemporaryDirectory() as out, tempfile.TemporaryDirectory() as elsewhere:
            os.makedirs(os.path.join(out, case["params"]["complist"]))
            project = os.path.join(out, "casa", case["name"])
            for suffix in (".ptg.txt", ".ms", ".cal.cclist", ".skymodel"):
                path = os.path.join(project, "calibrator_lists.alma.out01" + suffix)
                if suffix == ".ptg.txt":
                    os.makedirs(project, exist_ok=True)
                    open(path, "w", encoding="utf-8").close()
                else:
                    os.makedirs(path)
            tools = {"sm": Simulator(), "cl": ComponentLists()}
            cwd = os.getcwd()
            os.chdir(elsewhere)
            try:
                repredict(tools, case, project, out)
            finally:
                os.chdir(cwd)
            self.assertEqual(
                tools["cl"].concatenated,
                [
                    os.path.join(out, case["params"]["complist"]),
                    os.path.join(project, "calibrator_lists.alma.out01.cal.cclist"),
                ],
            )
            image, merged = tools["sm"].predicted
            self.assertEqual(image, os.path.join(project, "calibrator_lists.alma.out01.skymodel"))
            self.assertEqual(merged, os.path.join(project, "merged.cl"))


class CompareImagesTest(unittest.TestCase):
    def differences(self, casa, casars):
        return compare_images({"ia": Images({"a/x.flat": casa, "b/x.flat": casars})}, "a/x.flat", "b/x.flat")

    def test_finite_images_within_the_bound_agree(self):
        pixels = np.ones((4, 4), dtype=np.float32)
        self.assertEqual(self.differences(pixels, pixels * (1 + 1e-7)), [])
        self.assertEqual(len(self.differences(pixels, pixels * 1.01)), 1)

    def test_a_pixel_that_is_not_finite_fails_on_either_side(self):
        pixels = np.ones((4, 4), dtype=np.float32)
        for bad in (np.nan, np.inf):
            with_bad = pixels.copy()
            with_bad[1, 2] = bad
            self.assertEqual(self.differences(pixels, with_bad), ["x.flat: casa-rs has 1 pixels that are not finite"])
            self.assertEqual(self.differences(with_bad, pixels), ["x.flat: CASA has 1 pixels that are not finite"])
            self.assertEqual(len(self.differences(with_bad, with_bad)), 2)


class SpectralWindows:
    def __init__(self, windows):
        self.windows = windows

    def open(self, path):
        self.rows = self.windows[path]

    def nrows(self):
        return len(self.rows)

    def getcell(self, column, row):
        return self.rows[row][column]

    def close(self):
        pass


class FrequencyFramesTest(unittest.TestCase):
    def test_frame_and_channel_mismatches_are_reported(self):
        windows = {"a/SPECTRAL_WINDOW": [{"MEAS_FREQ_REF": 5, "CHAN_FREQ": [1e9, 1.1e9]}],
                   "b/SPECTRAL_WINDOW": [{"MEAS_FREQ_REF": 5, "CHAN_FREQ": [1e9, 1.1e9]}]}
        table = SpectralWindows(windows)
        details = {}
        self.assertEqual(compare_frequency_frames(table, "a", "b", details), [])
        self.assertEqual(details["CASA"][0]["reference"], 5)
        self.assertEqual(details["casa-rs"][0]["channels_hz"], [1e9, 1.1e9])
        windows["b/SPECTRAL_WINDOW"][0]["MEAS_FREQ_REF"] = 3
        windows["b/SPECTRAL_WINDOW"][0]["CHAN_FREQ"] = [1e9, 1.2e9]
        self.assertEqual(len(compare_frequency_frames(table, "a", "b")), 2)


class InputSelectionTest(unittest.TestCase):
    def test_only_selected_inputs_are_required(self):
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as out:
            source = pathlib.Path(data) / "unittest/simobserve/core5ps.skymodel"
            source.mkdir(parents=True)
            (source / "data").write_text("model", encoding="utf-8")
            selected = [case for case in CASES if case["name"] == "frame_topo"]
            copy_inputs(data, out, selected)
            self.assertEqual((pathlib.Path(out) / "inputs/core5ps.skymodel/data").read_text(), "model")
            self.assertFalse((pathlib.Path(out) / "inputs/core5ps.clist").exists())


class ExpectedOutcomeTest(unittest.TestCase):
    """Matching errors do not prove that a supported simulation works."""

    def run_case(self, expect_refusal, refused):
        case = {"name": "probe", "params": {}, "expect_refusal": expect_refusal}
        with tempfile.TemporaryDirectory() as out:
            args = ["simobserve_parity.py", "--out", out, "--casars-bin", "unused", "--casatestdata", "unused"]
            with (
                patch("simobserve_parity.CASES", [case]),
                patch.object(sys, "argv", args),
                patch("simobserve_parity.copy_inputs"),
                patch("simobserve_parity.tools", return_value={}),
                patch("simobserve_parity.run_casa", side_effect=RuntimeError("refused") if refused else None),
                patch("simobserve_parity.run_casars", return_value=("unused", {"error": "refused"} if refused else {})),
                patch("simobserve_parity.compare_case", return_value={"name": "probe", "differences": []}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                pathlib.Path(out, "casa").mkdir()
                status = main()
            return status, json.loads(pathlib.Path(out, "report.json").read_text())[0]

    def test_matching_refusals_fail_a_success_case(self):
        status, report = self.run_case(expect_refusal=False, refused=True)
        self.assertEqual(status, 1)
        self.assertIn("expected a successful simulation, not a refusal", report["differences"])

    def test_matching_successes_fail_a_refusal_case(self):
        status, report = self.run_case(expect_refusal=True, refused=False)
        self.assertEqual(status, 1)
        self.assertIn("expected a refusal, but both simulations succeeded", report["differences"])

    def test_the_expected_success_or_refusal_passes(self):
        for refused in (False, True):
            status, report = self.run_case(expect_refusal=refused, refused=refused)
            self.assertEqual(status, 0)
            self.assertEqual(report["differences"], [])


class ScheduleAlignmentTest(unittest.TestCase):
    def test_time_alignment_preserves_the_numeric_hours_input_form(self):
        params = {"hourangle": "-1.5", "refdate": "2017/01/02/09:30:00"}
        with (patch("simobserve_parity.hour_angle_s", return_value=-5400.0),
              patch("simobserve_parity.tai_minus_utc_s", return_value=37.0)):
            aligned = casars_params({}, params)
        self.assertAlmostEqual(float(aligned["hourangle"]) * 3600.0, -5363.0, places=9)
        self.assertEqual(params["hourangle"], "-1.5")
        self.assertEqual(aligned["refdate"], params["refdate"])

    def test_quantity_and_transit_inputs_keep_the_seconds_alignment(self):
        for hourangle, seconds in (("90min", 5400.0), ("transit", 0.0)):
            with (patch("simobserve_parity.hour_angle_s", return_value=seconds),
                  patch("simobserve_parity.tai_minus_utc_s", return_value=36.0)):
                aligned = casars_params({}, {"hourangle": hourangle})
            self.assertEqual(aligned["hourangle"], f"{seconds + 36.0}s")


if __name__ == "__main__":
    unittest.main()
