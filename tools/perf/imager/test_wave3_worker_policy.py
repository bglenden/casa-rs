#!/usr/bin/env python3
"""Focused tests for Wave 3 CPU multi-worker policy summaries."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wave3_worker_policy


def result(acceleration: str, runs: list[float], *, label: str = "good") -> dict:
    panel_status = "not_run" if label == "not_run" else "ready"
    return {
        "workload": {"mode_id": "standard-mfs-single-term"},
        "dataset": {"key": "wave1-vla-single-medium"},
        "mode": {
            "specmode": "mfs",
            "gridder": "standard",
            "deconvolver": "hogbom",
            "nterms": 1,
            "niter": 500,
            "channel_count": 64,
            "width": None,
            "weighting": "briggs",
            "standard_mfs_acceleration": acceleration,
        },
        "benchmark_features": {
            "image": {"imsize_x": 1024, "imsize_y": 1024},
            "mode_cost": {"multiscale_scale_count": None},
        },
        "results": {
            "rust": {
                "timings_seconds": {
                    "runs": runs,
                }
            }
        },
        "human_review": {
            "panel_status": panel_status,
            "structured_difference_label": label,
        },
    }


class Wave3WorkerPolicyTests(unittest.TestCase):
    def test_reliable_win_requires_repeated_paired_evidence(self) -> None:
        report = wave3_worker_policy.build_policy_report(
            [
                result("cpu", [100.0, 101.0, 99.0, 100.5, 100.2, 99.8, 100.1]),
                result("multi-cpu", [75.0, 75.5, 74.8, 75.2, 74.9, 75.1, 75.0]),
            ]
        )

        scenario = report["scenario_reports"][0]

        self.assertEqual("reliable_win", scenario["classification"])
        self.assertEqual(7, scenario["pair_count"])
        self.assertGreater(scenario["median_speedup"], 0.20)
        self.assertIn("allow CPU multi-worker", scenario["planner_recommendation"])

    def test_three_pairs_are_screening_not_policy(self) -> None:
        report = wave3_worker_policy.build_policy_report(
            [
                result("cpu", [100.0, 101.0, 99.0]),
                result("multi-cpu", [80.0, 81.0, 79.0]),
            ]
        )

        scenario = report["scenario_reports"][0]

        self.assertEqual("screening_win_needs_confirmation", scenario["classification"])
        self.assertIn("confirmation", scenario["planner_recommendation"])

    def test_correctness_blocks_performance_classification(self) -> None:
        report = wave3_worker_policy.build_policy_report(
            [
                result("cpu", [100.0, 101.0, 99.0, 100.5, 100.2], label="good"),
                result("multi-cpu", [75.0, 75.5, 74.8, 75.2, 74.9], label="bad"),
            ]
        )

        scenario = report["scenario_reports"][0]

        self.assertEqual("correctness_blocked", scenario["classification"])
        self.assertEqual("blocked", scenario["correctness_status"])

    def test_timing_only_rows_need_explicit_correctness_evidence(self) -> None:
        report = wave3_worker_policy.build_policy_report(
            [
                result("cpu", [100.0, 101.0, 99.0, 100.5, 100.2], label="not_run"),
                result("multi-cpu", [75.0, 75.5, 74.8, 75.2, 74.9], label="not_run"),
            ]
        )

        scenario = report["scenario_reports"][0]

        self.assertEqual("correctness_blocked", scenario["classification"])
        self.assertEqual("not_evaluated", scenario["correctness_status"])

    def test_accepted_external_correctness_can_unlock_timing_only_rows(self) -> None:
        report = wave3_worker_policy.build_policy_report(
            [
                result("cpu", [100.0, 101.0, 99.0, 100.5, 100.2], label="not_run"),
                result("multi-cpu", [75.0, 75.5, 74.8, 75.2, 74.9], label="not_run"),
            ],
            accepted_correctness_note="accepted #276 standard MFS correctness",
        )

        scenario = report["scenario_reports"][0]

        self.assertEqual("reliable_win", scenario["classification"])
        self.assertEqual("accepted_external", scenario["correctness_status"])
        self.assertEqual(
            "accepted #276 standard MFS correctness",
            scenario["correctness_evidence"],
        )

    def test_sidecar_json_is_not_treated_as_result_bundle(self) -> None:
        self.assertTrue(wave3_worker_policy.is_result_bundle(result("cpu", [1.0])))
        self.assertFalse(
            wave3_worker_policy.is_result_bundle(
                {
                    "products": {},
                    "comparison_input": True,
                }
            )
        )

    def test_failed_backend_reasons_are_reported(self) -> None:
        failed = result("cpu", [])
        failed["status"] = "failed_execution"
        failed["results"]["rust"]["reason"] = "bounded source stream rejected request"

        report = wave3_worker_policy.build_policy_report([failed])
        scenario = report["scenario_reports"][0]

        self.assertIn("serial_cpu", scenario["backend_failures"])
        self.assertIn(
            "bounded source stream rejected request",
            scenario["backend_failures"]["serial_cpu"][0],
        )


if __name__ == "__main__":
    unittest.main()
