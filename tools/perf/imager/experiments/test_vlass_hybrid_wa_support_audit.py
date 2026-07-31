#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS hybrid W/A support audit."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_hybrid_wa_support_audit.py")
SPEC = importlib.util.spec_from_file_location("vlass_hybrid_wa_support_audit", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def synthetic_log(*, second_window_samples: int = 40) -> str:
    """Return a two-window diagnostic with a failing A-only floor."""

    stacks0 = [100, 130, *([55] * 29), 53]
    stacks1 = [200, 260, *([110] * 29), 106]
    return "\n".join(
        (
            "awproject_hybrid_support_audit block=0 window=0 samples=60 "
            "plan_references=960 represented_w_extent=8.5e4 "
            "w_increment=1.1e-2 current_weighted_taps=100 "
            "a_only_weighted_taps=52 stack_counts=1-32 "
            f"stack_weighted_taps={','.join(map(str, stacks0))}",
            "awproject_hybrid_support_audit block=1 window=0 "
            f"samples={second_window_samples} plan_references=640 "
            "represented_w_extent=8.5e4 w_increment=1.1e-2 "
            "current_weighted_taps=200 a_only_weighted_taps=104 "
            "stack_counts=1-32 "
            f"stack_weighted_taps={','.join(map(str, stacks1))}",
            "awproject_metal_grid_summary pass=initial_dirty kernel_values=75",
        )
    )


class HybridWaSupportAuditTest(unittest.TestCase):
    def test_rejects_candidate_above_support_floor(self) -> None:
        result = audit.analyze_log(synthetic_log(), expected_samples=100)
        self.assertEqual(result["coverage"]["plan_references"], 1600)
        self.assertAlmostEqual(result["a_only_floor"]["support_ratio"], 0.52)
        self.assertEqual(result["selection"]["best_stack_count"], 32)
        self.assertAlmostEqual(result["selection"]["best_support_ratio"], 0.53)
        self.assertFalse(result["selection"]["support_gate_passed"])

    def test_can_promote_a_stricter_support_curve(self) -> None:
        result = audit.analyze_log(
            synthetic_log(),
            expected_samples=100,
            maximum_support_ratio=0.55,
        )
        self.assertTrue(result["selection"]["support_gate_passed"])

    def test_rejects_sample_contract_mismatch(self) -> None:
        with self.assertRaisesRegex(audit.HybridAuditError, "expected 100"):
            audit.analyze_log(
                synthetic_log(second_window_samples=39),
                expected_samples=100,
            )

    def test_rejects_plan_multiplicity_mismatch(self) -> None:
        text = synthetic_log().replace("plan_references=640", "plan_references=639")
        with self.assertRaisesRegex(audit.HybridAuditError, "plan multiplicity"):
            audit.analyze_log(text, expected_samples=100)


if __name__ == "__main__":
    unittest.main()
