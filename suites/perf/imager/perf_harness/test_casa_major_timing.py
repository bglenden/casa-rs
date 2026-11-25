# SPDX-License-Identifier: LGPL-3.0-or-later
"""The diagnostic must preserve the native call and restore it after failures."""

from contextlib import redirect_stdout
import io
import json
import os
import unittest
from unittest.mock import patch

from .casa_major_timing import trace_major_cycles


class MajorTimingTests(unittest.TestCase):
    def test_disabled_does_not_import_casa_or_read_a_clock(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("time.perf_counter", side_effect=AssertionError("clock read")):
                with trace_major_cycles():
                    pass

    def test_calls_and_returns_are_preserved_and_methods_restored(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                calls = []
                sentinel = object()

                class Imager:
                    def runMajorCycle(self, isCleanCycle=True):
                        calls.append(("major", isCleanCycle))
                        return self.runMajorCycleCore(lastcycle=False)

                    def runMajorCycleCore(self, lastcycle):
                        calls.append(("core", lastcycle))
                        if fail:
                            raise ValueError("native failure")
                        return sentinel

                original = (Imager.runMajorCycle, Imager.runMajorCycleCore)
                output = io.StringIO()
                with patch.dict(os.environ, {"CASA_RS_TRACE_MAJOR_CYCLE_ENVELOPES": "1"}):
                    with redirect_stdout(output), patch("time.perf_counter", side_effect=[0, 1, 3, 5]):
                        with trace_major_cycles(Imager):
                            if fail:
                                with self.assertRaisesRegex(ValueError, "native failure"):
                                    Imager().runMajorCycle(isCleanCycle=False)
                            else:
                                self.assertIs(Imager().runMajorCycle(isCleanCycle=False), sentinel)
                self.assertEqual(original, (Imager.runMajorCycle, Imager.runMajorCycleCore))
                self.assertEqual(calls, [("major", False), ("core", False)])
                line, = output.getvalue().splitlines()
                self.assertTrue(line.startswith("casa_major_cycle_envelope "))
                record = json.loads(line.split(" ", 1)[1])
                self.assertEqual(record, {
                    "ordinal": 1, "is_clean_cycle": False, "core_calls": 1,
                    "core_seconds": 2.0, "major_cycle_seconds": 5,
                    "outside_core_seconds": 3.0, "completed": not fail,
                })


if __name__ == "__main__":
    unittest.main()
