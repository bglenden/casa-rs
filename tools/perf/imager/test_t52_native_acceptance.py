# SPDX-License-Identifier: LGPL-3.0-or-later
"""T52 must not accept numeric-only success after a product-contract failure."""

import unittest
from unittest.mock import patch

from perf_harness.t52_native_acceptance import accept_comparison


class T52ComparisonAcceptanceTests(unittest.TestCase):
    def test_contract_failure_cannot_be_hidden_by_passing_numerics(self):
        output = {"status": "comparison_failed", "reason": "exact product inventory differs"}
        with patch("perf_harness.t52_native_acceptance.validate_comparison_output"), patch(
            "perf_harness.t52_native_acceptance.evaluate_comparison_tolerances",
            return_value={"status": "passed"},
        ) as evaluate:
            with self.assertRaisesRegex(ValueError, "exact product inventory differs"):
                accept_comparison(output, {}, {})
            evaluate.assert_not_called()

    def test_completed_comparison_still_requires_numerical_acceptance(self):
        with patch("perf_harness.t52_native_acceptance.validate_comparison_output"), patch(
            "perf_harness.t52_native_acceptance.evaluate_comparison_tolerances",
            return_value={"status": "failed"},
        ):
            with self.assertRaisesRegex(ValueError, "tolerances failed"):
                accept_comparison({"status": "completed"}, {}, {})

    def test_completed_and_numerically_passing_comparison_is_accepted(self):
        with patch("perf_harness.t52_native_acceptance.validate_comparison_output"), patch(
            "perf_harness.t52_native_acceptance.evaluate_comparison_tolerances",
            return_value={"status": "passed"},
        ):
            self.assertEqual(
                accept_comparison({"status": "completed"}, {}, {}), {"status": "passed"}
            )
