#!/usr/bin/env python3
"""Focused tests for performance candidate eligibility."""

from __future__ import annotations

import contextlib
import copy
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import performance_candidate_gate as gate


FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures/performance_candidate_terminal_phase.json"
)


def regression_record() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


class PerformanceCandidateGateTests(unittest.TestCase):
    def test_terminal_phase_regression_is_rejected_at_ten_percent(self) -> None:
        result = gate.evaluate(regression_record())
        metrics = result["metrics"]

        self.assertAlmostEqual(
            176.846,
            metrics["phase_occurrence_weighted_baseline_seconds"],
        )
        self.assertAlmostEqual(
            176.846 / 1814.446,
            metrics["phase_occurrence_weighted_baseline_contribution_fraction"],
        )
        self.assertAlmostEqual(
            0.1416,
            metrics["affected_removable_ceiling_fraction"],
        )
        self.assertAlmostEqual(
            0.013801123648761111,
            metrics["optimistic_maximum_end_to_end_improvement_fraction"],
        )
        self.assertAlmostEqual(
            1.380112364876111,
            metrics["optimistic_maximum_end_to_end_improvement_percent"],
        )
        self.assertEqual("rejected_below_campaign_threshold", result["decision"])
        self.assertFalse(result["allowed"])
        self.assertFalse(result["implementation_eligible"])

    def test_occurrence_weighting_can_clear_the_campaign_threshold(self) -> None:
        record = regression_record()
        record.update(
            {
                "projected_total_seconds": 100.0,
                "affected_fraction": 0.5,
                "removable_fraction": 0.5,
            }
        )
        record["phase"].update({"seconds_per_occurrence": 20.0, "occurrence_count": 3})

        result = gate.evaluate(record)

        self.assertAlmostEqual(
            60.0,
            result["metrics"]["phase_occurrence_weighted_baseline_seconds"],
        )
        self.assertAlmostEqual(
            0.15,
            result["metrics"]["optimistic_maximum_end_to_end_improvement_fraction"],
        )
        self.assertEqual("eligible_for_implementation", result["decision"])
        self.assertTrue(result["allowed"])
        self.assertTrue(result["implementation_eligible"])
        self.assertEqual(
            record["parent_revision"],
            result["campaign_control"]["parent_revision"],
        )

    def test_diagnostic_only_record_is_allowed_below_threshold(self) -> None:
        record = regression_record()
        record["record_kind"] = "diagnostic_only"

        result = gate.evaluate(record)

        self.assertEqual("allowed_diagnostic_only", result["decision"])
        self.assertTrue(result["allowed"])
        self.assertFalse(result["implementation_eligible"])
        self.assertAlmostEqual(
            0.013801123648761111,
            result["metrics"]["optimistic_maximum_end_to_end_improvement_fraction"],
        )

    def test_cli_emits_json_and_rejects_below_threshold(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = gate.main([str(FIXTURE_PATH)])

        self.assertEqual(1, exit_code)
        result = json.loads(stdout.getvalue())
        self.assertEqual("evaluated", result["status"])
        self.assertEqual("rejected_below_campaign_threshold", result["decision"])

    def test_invalid_contribution_fails_closed(self) -> None:
        record = copy.deepcopy(regression_record())
        record["phase"]["occurrence_count"] = 11

        with self.assertRaisesRegex(
            gate.CandidateError,
            "occurrence-weighted seconds must not exceed",
        ):
            gate.evaluate(record)

    def test_implementation_record_requires_campaign_control(self) -> None:
        record = regression_record()
        del record["falsifier"]

        with self.assertRaisesRegex(gate.CandidateError, "falsifier"):
            gate.evaluate(record)

    def test_diagnostic_record_does_not_claim_campaign_control(self) -> None:
        record = regression_record()
        record["record_kind"] = "diagnostic_only"
        for field in (
            "artifact_retention_class",
            "discriminator",
            "falsifier",
            "hypothesis",
            "limits",
            "parent_revision",
            "reversion",
        ):
            del record[field]

        result = gate.evaluate(record)

        self.assertIsNone(result["campaign_control"])


if __name__ == "__main__":
    unittest.main()
