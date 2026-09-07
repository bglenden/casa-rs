# SPDX-License-Identifier: LGPL-3.0-or-later

import unittest
from unittest.mock import patch

from .t51_fixed_model_cycle import fixed_model_cycle


class FixedModelCycleTests(unittest.TestCase):
    def exercise(self, *, invalid=False, duplicate=False):
        events = []
        receipt = {}

        class Normalizer:
            def multiplymodelbyweight(self):
                events.append("multiply")

        class Imager:
            NF = 1
            IBtool = None
            allimpars = {"0": {"specmode": "mfs", "deconvolver": "mtmfs"}}
            PStools = [Normalizer()]

            def runMajorCycle(self, isCleanCycle=True):
                self_outer.assertFalse(isCleanCycle)
                events.append("divide/scatter")
                self.runMajorCycleCore(lastcycle=True)
                events.append("normalize residual/multiply")
                return 42

            def runMajorCycleCore(self, lastcycle):
                self_outer.assertTrue(lastcycle)
                events.append("predict/refresh")

        def check(imager):
            self.assertIsInstance(imager, Imager)
            events.append("check physical model")
            if invalid:
                raise ValueError("mismatched physical model")
            return {"verified": True}

        self_outer = self
        original = (Imager.runMajorCycle, Imager.runMajorCycleCore)
        # Preparation 0..1; normalization before validation 2..3;
        # validation 3..13; remaining major envelope 14..17; core 15..16.
        ticks = [0, 1, 2, 3, 13, 14, 15, 16, 17]
        if invalid:
            ticks = [0, 1, 2, 3, 13, 14]
        try:
            with patch("time.perf_counter", side_effect=ticks):
                with fixed_model_cycle(Imager, check_physical_model=check, receipt=receipt):
                    self.assertEqual(Imager().runMajorCycle(isCleanCycle=False), 42)
                    if duplicate:
                        Imager().runMajorCycle(isCleanCycle=False)
        finally:
            self.assertEqual(original, (Imager.runMajorCycle, Imager.runMajorCycleCore))
            if invalid:
                self.assertNotIn("predict/refresh", events)
                self.assertFalse(receipt["completed"])
            else:
                self.assertEqual(events, ["multiply", "divide/scatter", "check physical model",
                                          "predict/refresh", "normalize residual/multiply"])
                self.assertEqual(receipt["normalized_refresh_seconds"], 4)
                self.assertEqual(receipt["validation_seconds"], 10)
                self.assertEqual(receipt["core_seconds"], 1)
        return receipt

    def test_intervals_exclude_actual_check_and_preserve_native_call_order(self):
        self.assertTrue(self.exercise()["completed"])

    def test_invalid_physical_model_never_predicts(self):
        with self.assertRaisesRegex(ValueError, "mismatched physical model"):
            self.exercise(invalid=True)

    def test_second_major_cycle_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "expected one"):
            self.exercise(duplicate=True)

    def test_no_major_cycle_is_not_success(self):
        class Imager:
            runMajorCycle = object()
            runMajorCycleCore = object()

        with self.assertRaisesRegex(ValueError, "did not complete"):
            with fixed_model_cycle(Imager, check_physical_model=None, receipt={}):
                pass


if __name__ == "__main__":
    unittest.main()
