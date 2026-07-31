#!/usr/bin/env python3
"""Focused tests for the source-1446 AW tap-prefix comparator."""

from __future__ import annotations

import copy
import unittest

import vlass_source1446_aw_prefix_compare as subject


def pair(label: str, bits: list[int]) -> dict[str, str]:
    return {f"{label}_re": str(bits[0]), f"{label}_im": str(bits[1])}


def fixture() -> tuple[dict, dict, dict]:
    coefficient = [10, 11]
    model = [20, 21]
    product = [30, 31]
    accumulator = [40, 41]
    casa_tap = {
        "index": "0",
        "iy": "0",
        "ix": "0",
        "grid_x": "2",
        "grid_y": "3",
        **pair("post_phase_cf", coefficient),
        **pair("grid", model),
        **pair("product", product),
        **pair("accumulator", accumulator),
    }
    casa = {
        "meta": {
            "row": "35",
            "channel": "19",
            "pol": "0",
            "mcol": "0",
            "frequency_hz": "2002926299.5681725",
            "loc_x": "2",
            "loc_y": "3",
            "support_x": "0",
            "support_y": "0",
        },
        "taps": [casa_tap],
        "result": {
            **pair("normalization", [100, 101]),
            **pair("pre_phasor", accumulator),
            **pair("prediction", [50, 51]),
            **pair("phasor", [60, 61]),
        },
    }
    casars = {
        "schema": subject.CASARS_SCHEMA,
        "source_ordinal": 1446,
        "logical_role": "rr",
        "model_term": 0,
        "program": {"sample_count": 98239},
        "plan": {
            "loc": [2, 3],
            "support": [0, 0],
        },
        "taps": [
            {
                "tap_ordinal": 0,
                "iy": 0,
                "ix": 0,
                "grid_x": 2,
                "grid_y": 3,
                "degrid_coefficient_bits": coefficient,
                "model_tt0_bits": model,
                "product_bits": product,
                "accumulator_bits": accumulator,
            }
        ],
        "result": {
            "tap_count": 1,
            "normalizer_bits": [100, 101 ^ 0x8000_0000],
            "numerator_bits": accumulator,
        },
    }
    phase = {
        "schema": subject.PHASE_SCHEMA,
        "first_mismatch": {
            "boundary": "raw_tt0",
            "actual_bits": [70, 71],
            "expected_bits": [69, 71],
            "source": {
                "current": {
                    "source_ordinal": 1446,
                    "row_id": 353635,
                    "spw_id": 2,
                    "channel": 19,
                    "role": "rr",
                }
            },
        },
    }
    return casa, casars, phase


class Source1446PrefixTests(unittest.TestCase):
    def test_exact_prefix_localizes_final_normalization(self) -> None:
        casa, casars, phase = fixture()
        receipt = subject.analyze(
            casa_trace=casa,
            casars_trace=casars,
            phase_receipt=phase,
        )
        self.assertTrue(receipt["tap_prefix"]["exact"])
        self.assertTrue(receipt["normalizer"]["exact"])
        self.assertTrue(receipt["numerator"]["exact"])
        self.assertEqual(
            receipt["classification"],
            "exact-tap-prefix-final-normalization-boundary",
        )
        self.assertEqual(
            receipt["first_proven_divergence"]["stage"],
            "final_normalization",
        )

    def test_tap_difference_is_not_misclassified_as_division(self) -> None:
        casa, casars, phase = fixture()
        broken = copy.deepcopy(casars)
        broken["taps"][0]["product_bits"][0] += 1
        receipt = subject.analyze(
            casa_trace=casa,
            casars_trace=broken,
            phase_receipt=phase,
        )
        self.assertEqual(
            receipt["classification"],
            "tap-prefix-or-normalizer-divergence",
        )
        self.assertEqual(
            receipt["first_proven_divergence"]["stage"],
            "tap_product",
        )

    def test_support_controls_required_tap_count(self) -> None:
        casa, casars, phase = fixture()
        casa["meta"]["support_x"] = "1"
        casars["plan"]["support"] = [1, 0]
        with self.assertRaisesRegex(RuntimeError, "support-derived footprint"):
            subject.analyze(
                casa_trace=casa,
                casars_trace=casars,
                phase_receipt=phase,
            )


if __name__ == "__main__":
    unittest.main()
