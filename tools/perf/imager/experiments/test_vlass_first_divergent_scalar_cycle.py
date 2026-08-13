#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_first_divergent_scalar_cycle as subject  # noqa: E402


def numerical(
    cycle: int,
    metric: str,
    *,
    equal: bool,
) -> dict:
    return {
        "cycle": cycle,
        "metric": metric,
        "f32_equal": equal,
    }


class FirstDivergentScalarCycleTests(unittest.TestCase):
    def test_f32_comparison_uses_binary_identity_not_decimal_spelling(
        self,
    ) -> None:
        comparison = subject.compare_f32_metric(
            cycle=0,
            name="start_peak",
            casa_value=0.03540397062897682,
            rust_text="0.03540397",
        )

        self.assertTrue(comparison["f32_equal"])
        self.assertEqual(comparison["ulp_distance"], 0)

    def test_f32_comparison_records_one_ulp_difference(self) -> None:
        comparison = subject.compare_f32_metric(
            cycle=0,
            name="start_peak",
            casa_value=0.03540397062897682,
            rust_text="0.035403974",
        )

        self.assertFalse(comparison["f32_equal"])
        self.assertEqual(comparison["ulp_distance"], 1)

    def test_parameter_identity_allows_only_trace_controls(self) -> None:
        full = {
            "field": "1525",
            "niter": 2000,
            "cycleniter": 2000,
            "imagename": "full",
        }
        trace = {
            "field": "1525",
            "niter": 270,
            "cycleniter": 270,
            "imagename": "trace",
            "fullsummary": True,
        }

        self.assertTrue(subject.parameters_match(trace, full))
        trace["field"] = "different"
        self.assertFalse(subject.parameters_match(trace, full))

    def test_discrete_divergence_invalidates_classification(self) -> None:
        classification = subject.classify(
            [{"equal": False}],
            [numerical(0, "start_peak", equal=False)],
        )

        self.assertEqual(classification, "invalid-discrete-trajectory")

    def test_cycle_zero_input_difference_is_distinct(self) -> None:
        classification = subject.classify(
            [{"equal": True}],
            [
                numerical(0, "start_peak", equal=False),
                numerical(0, "model_flux", equal=False),
            ],
        )

        self.assertEqual(
            classification,
            "diverges-at-cycle-0-input-scalar",
        )

    def test_cycle_zero_internal_and_later_classifications(self) -> None:
        scenarios = (
            (
                [numerical(0, "start_peak", equal=True)],
                [numerical(0, "model_flux", equal=False)],
                "diverges-within-cycle-0-scalars",
            ),
            (
                [numerical(0, "model_flux", equal=True)],
                [numerical(1, "start_peak", equal=False)],
                "diverges-after-cycle-0-scalars",
            ),
            (
                [numerical(0, "start_peak", equal=True)],
                [numerical(1, "model_flux", equal=True)],
                "no-divergence-in-exact-scalar-window",
            ),
        )
        for prefix, suffix, expected in scenarios:
            with self.subTest(expected=expected):
                self.assertEqual(
                    subject.classify([{"equal": True}], prefix + suffix),
                    expected,
                )

    def test_exact_window_excludes_truncated_cycle_two_outputs(self) -> None:
        casa_rows = [
            {
                "cycle": cycle,
                "start_iteration": start,
                "cycle_start_iteration": start,
                "reported_updates": updates,
                "start_peak": 1.0,
                "cycle_threshold": 0.5,
                "unmasked_end_peak": 0.25,
                "model_flux": 0.125,
                "stop_code": 2,
            }
            for cycle, start, updates in ((0, 0, 6), (1, 6, 263), (2, 269, 1))
        ]
        rust_rows = [
            {
                "cycle": cycle,
                "start_iteration": start,
                "reported_updates": updates,
                "actual_updates": updates,
                "start_peak": "1.0",
                "cycle_threshold": "0.5",
                "unmasked_end_peak": "0.25",
                "model_flux": "0.125",
            }
            for cycle, start, updates in (
                (0, 0, 6),
                (1, 6, 263),
                (2, 269, 239),
            )
        ]

        discrete, metrics = subject.build_exact_window(casa_rows, rust_rows)

        self.assertFalse(
            any(
                item["cycle"] == 2 and item["field"] == "reported_updates"
                for item in discrete
            )
        )
        self.assertFalse(
            any(
                item["cycle"] == 2
                and item["metric"] in ("unmasked_end_peak", "model_flux")
                for item in metrics
            )
        )


if __name__ == "__main__":
    unittest.main()
