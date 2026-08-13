#!/usr/bin/env python3
"""Focused tests for the one-source AW prediction-prefix comparator."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("vlass_aw_prediction_prefix_compare.py")
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location(
    "vlass_aw_prediction_prefix_compare", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

ONE = 0x3F80_0000
HALF = 0x3F00_0000
NEG_HALF = 0xBF00_0000


def casa_tap(index: int = 0) -> dict[str, str]:
    result: dict[str, str] = {
        "index": str(index),
        "iy": "0",
        "ix": "0",
        "grid_x": "8",
        "grid_y": "9",
    }
    for label in ("post_phase_cf", "grid", "product", "accumulator"):
        result[f"{label}_re"] = str(ONE)
        result[f"{label}_im"] = str(HALF)
    return result


def casa_trace(*, support: int = 0, tap_count: int = 1) -> dict:
    return {
        "meta": {
            "row": "3",
            "channel": "38",
            "pol": "0",
            "mcol": "0",
            "loc_x": "8",
            "loc_y": "9",
            "support_x": str(support),
            "support_y": str(support),
            "frequency_hz": "2040926546.1131351",
            "data_w_m": "2396.243683492994",
        },
        "taps": [casa_tap(index) for index in range(tap_count)],
        "result": {
            "normalization_re": str(ONE),
            "normalization_im": str(HALF),
            "pre_phasor_re": str(ONE),
            "pre_phasor_im": str(HALF),
            "phasor_re": str(ONE),
            "phasor_im": "0",
            "post_phasor_re": str(ONE),
            "post_phasor_im": str(HALF),
            "prediction_re": str(ONE),
            "prediction_im": str(HALF),
        },
    }


def casars_trace(*, support: int = 0, tap_count: int = 1) -> dict:
    tap = {
        "tap_ordinal": 0,
        "iy": 0,
        "ix": 0,
        "grid_x": 8,
        "grid_y": 9,
        "degrid_coefficient_bits": [ONE, HALF],
        "model_tt0_bits": [ONE, HALF],
        "product_bits": [ONE, HALF],
        "accumulator_bits": [ONE, HALF],
    }
    return {
        "schema": "casa-rs-vlass-aw-prediction-prefix-trace-v2",
        "source_ordinal": 110,
        "source_sample_index": 110,
        "logical_role": "rr",
        "model_term": 0,
        "source_sample": {"prediction_w_lambda": 1.0},
        "plan": {"loc": [8, 9], "support": [support, support]},
        "taps": [dict(tap, tap_ordinal=index) for index in range(tap_count)],
        "result": {
            "tap_count": tap_count,
            "normalizer_bits": [ONE, NEG_HALF],
            "numerator_bits": [ONE, HALF],
        },
    }


class PredictionPrefixCompareTest(unittest.TestCase):
    def test_exact_single_tap_prefix(self) -> None:
        result = MODULE.analyze(
            casa_trace=casa_trace(),
            casars_trace=casars_trace(),
            expected_row=3,
            expected_channel=38,
            expected_source_ordinal=110,
        )
        self.assertEqual(result["classification"], "exact-prefix")
        self.assertTrue(result["footprint"]["geometry_exact"])
        self.assertTrue(result["tap_prefix"]["arithmetic_comparable"])
        self.assertTrue(result["normalizer"]["comparison"]["exact"])

    def test_support_divergence_disables_arithmetic_comparison(self) -> None:
        result = MODULE.analyze(
            casa_trace=casa_trace(support=1, tap_count=9),
            casars_trace=casars_trace(),
            expected_row=3,
            expected_channel=38,
            expected_source_ordinal=110,
        )
        self.assertEqual(result["classification"], "geometry-divergence")
        self.assertFalse(result["footprint"]["geometry_exact"])
        self.assertEqual(result["footprint"]["casa_tap_count"], 9)
        self.assertEqual(result["footprint"]["casa_rs_tap_count"], 1)
        self.assertFalse(result["tap_prefix"]["arithmetic_comparable"])
        self.assertEqual(
            result["tap_prefix"]["first_mismatch"]["field"], "support"
        )
        self.assertIsNone(result["normalizer"]["comparison"])
        self.assertIsNone(result["numerator"]["comparison"])


if __name__ == "__main__":
    unittest.main()
