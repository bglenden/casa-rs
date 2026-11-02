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
import pathlib
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from simobserve_parity import CASES, compare_images, repredict


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


if __name__ == "__main__":
    unittest.main()
