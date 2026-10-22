# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `bench_campaign.py`'s acceptance of CASA runs (#853 review).

A benchmark times completed work: a CASA manifest with a call that returned
`False` or raised is not usable, and gets no CASA multiple. Run with
`python3 tools/simulation/test_bench_campaign.py` (or pytest); only the
standard library is needed.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from bench_campaign import add_multiples, failed_calls


def manifest_entry(predict: dict) -> dict:
    """A case-1-like manifest entry whose `predict` call recorded `predict`."""
    calls = [
        {"op": "open", "returned": True},
        {"op": "setconfig", "returned": True},
        {"op": "observe", "returned": True, "seconds": 11.0},
        dict({"op": "predict", "seconds": 0.1}, **predict),
        {"op": "close", "returned": True},
    ]
    return {"name": "case1-alma-cube", "calls": calls, "seconds": 12.0}


def case(casa: dict) -> dict:
    return {
        "casa": casa,
        "casa_rs": {"cpu": {"exit": 0, "wall_s": 4.0}, "metal-1": {"exit": 1, "wall_s": 1.0}},
        "models": None,
    }


class CasaAcceptanceTests(unittest.TestCase):
    def test_every_call_returning_true_is_usable(self) -> None:
        self.assertEqual(failed_calls(manifest_entry({"returned": True})), [])

    def test_a_raised_predict_is_not_usable(self) -> None:
        # The review's injection: predict raising "No such VisVector type".
        entry = manifest_entry({"returned": None, "raised": "No such VisVector type"})
        self.assertEqual(failed_calls(entry), ["predict raised No such VisVector type"])

    def test_a_false_return_is_not_usable(self) -> None:
        self.assertEqual(
            failed_calls(manifest_entry({"returned": False})), ["predict returned False"]
        )

    def test_only_a_usable_casa_run_gets_multiples(self) -> None:
        usable = case({"exit": 0, "casa_total_s": 12.0, "failed_calls": []})
        failed = case({"exit": 0, "casa_total_s": 12.0, "failed_calls": ["predict returned False"]})
        cases = {"usable": usable, "failed": failed}
        add_multiples(cases)
        self.assertEqual(usable["casa_rs"]["cpu"]["casa_multiple_wall"], 3.0)
        self.assertNotIn("casa_multiple_wall", usable["casa_rs"]["metal-1"])
        self.assertNotIn("casa_multiple_wall", failed["casa_rs"]["cpu"])


if __name__ == "__main__":
    unittest.main()
