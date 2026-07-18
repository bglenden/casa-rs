#!/usr/bin/env python3
"""Focused tests for counterbalanced workload comparison orchestration."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_alternating_comparison as alternating


class ScheduleTests(unittest.TestCase):
    def test_default_schedule_matches_requested_counterbalance(self) -> None:
        schedule = alternating.build_schedule(
            "baseline", "candidate", warmup_pair_count=2, measured_pair_count=3
        )

        warmup = [
            alternating.short_role(item.role)
            for item in schedule
            if item.phase == "warmup"
        ]
        measured = [
            alternating.short_role(item.role)
            for item in schedule
            if item.phase == "measured"
        ]

        self.assertEqual(list("ABBA"), warmup)
        self.assertEqual(list("ABBABAABABBA"), measured)
        self.assertEqual(list(range(1, 17)), [item.sequence_index for item in schedule])

    def test_measured_pair_count_has_a_minimum_of_three(self) -> None:
        with self.assertRaisesRegex(alternating.ComparisonError, "at least 3"):
            alternating.build_schedule(
                "baseline", "candidate", warmup_pair_count=0, measured_pair_count=2
            )


class AggregationTests(unittest.TestCase):
    def test_robust_summary_reports_median_mad_and_iqr(self) -> None:
        summary = alternating.robust_summary([1.0, 2.0, 3.0, 4.0])

        self.assertEqual(2.5, summary["median"])
        self.assertEqual(1.0, summary["mad"])
        self.assertEqual(1.75, summary["q1"])
        self.assertEqual(3.25, summary["q3"])
        self.assertEqual(1.5, summary["iqr"])

    def test_report_aggregates_blocks_stages_backends_and_verdict(self) -> None:
        config = comparison_config(slowdown_tolerance=0.03)
        schedule = alternating.build_schedule(
            "baseline", "candidate", warmup_pair_count=0, measured_pair_count=3
        )
        baseline_walls = iter([10.0, 10.2, 9.8, 10.0, 10.1, 9.9])
        candidate_walls = iter([10.1, 10.3, 9.9, 10.1, 10.2, 10.0])
        runs = []
        for item in schedule:
            wall = next(baseline_walls if item.role == "baseline" else candidate_walls)
            runs.append(fake_run(item, wall))

        report = alternating.build_report(
            config,
            comparison_id="comparison",
            report_path=Path("/tmp/comparison.json"),
            comparison_artifact_root=Path("/tmp/artifacts/comparison"),
            schedule=schedule,
            runs=runs,
        )

        details = report["results"]["alternating_comparison"]
        self.assertEqual(3, len(details["paired_deltas"]))
        self.assertEqual(6, len(details["adjacent_pair_deltas"]))
        self.assertEqual("pass", details["verdict"]["status"])
        self.assertTrue(details["verdict"]["no_slowdown"])
        self.assertEqual(
            6, details["measured_summaries"]["baseline"]["total_wall_seconds"]["count"]
        )
        self.assertEqual(
            100.0,
            details["measured_summaries"]["baseline"]["stage_timings_ms"]["rust"][
                "total"
            ]["median"],
        )
        self.assertEqual(
            "cpu",
            details["measured_summaries"]["baseline"]["backend_identities"][0][
                "benchmark_features"
            ]["resolved_backend"],
        )

    def test_incomplete_measurements_make_verdict_inconclusive(self) -> None:
        config = comparison_config()
        schedule = alternating.build_schedule(
            "baseline", "candidate", warmup_pair_count=0, measured_pair_count=3
        )
        runs = [fake_run(item, 10.0) for item in schedule[:-1]]

        report = alternating.build_report(
            config,
            comparison_id="comparison",
            report_path=Path("/tmp/comparison.json"),
            comparison_artifact_root=Path("/tmp/artifacts/comparison"),
            schedule=schedule,
            runs=runs,
        )

        verdict = report["results"]["alternating_comparison"]["verdict"]
        self.assertEqual("inconclusive", verdict["status"])
        self.assertIsNone(verdict["no_slowdown"])

    def test_slowdown_beyond_tolerance_fails_verdict(self) -> None:
        verdict = alternating.build_verdict(
            [
                {
                    "relative_delta": 0.05,
                },
                {
                    "relative_delta": 0.04,
                },
                {
                    "relative_delta": 0.06,
                },
            ],
            tolerance=0.03,
            evidence_complete=True,
        )

        self.assertEqual("fail", verdict["status"])
        self.assertFalse(verdict["no_slowdown"])


class OrchestrationTests(unittest.TestCase):
    def test_injected_runner_receives_one_repeat_per_item_and_report_keeps_paths(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = comparison_config(
                warmup_pair_count=1,
                output_root=root / "output",
                artifact_root=root / "artifacts",
                run_workload_options=("--stream-log", "--set-imaging", "niter=0"),
            )
            commands: list[list[str]] = []
            environments: list[dict[str, str]] = []

            def runner(
                command: list[str], environment: dict[str, str]
            ) -> subprocess.CompletedProcess[str]:
                commands.append(command)
                environments.append(environment)
                output_dir = Path(command[command.index("--output-dir") + 1])
                output_dir.mkdir(parents=True, exist_ok=True)
                result_path = output_dir / "result.json"
                wall = 10.0 if command[2] == "baseline" else 9.5
                result_path.write_text(
                    json.dumps(fake_result(wall, output_dir)), encoding="utf-8"
                )
                return subprocess.CompletedProcess(
                    command, 0, f"noise\n{result_path}\n", None
                )

            with mock.patch.dict(
                os.environ,
                {
                    "ALTERNATING_COMPARISON_TEST_SENTINEL": "preserved",
                    "CASA_RS_BENCH_PROFILE_REPEATS": "99",
                },
            ):
                report_path, report = alternating.run_comparison(
                    config, command_runner=runner, comparison_id="test-comparison"
                )

            self.assertEqual(14, len(commands))
            self.assertEqual(14, len(environments))
            for command, environment in zip(commands, environments, strict=True):
                self.assertEqual("1", command[command.index("--repeats") + 1])
                self.assertEqual(1, command.count(str(alternating.RUN_WORKLOAD)))
                self.assertEqual(
                    ["--stream-log", "--set-imaging", "niter=0"], command[-3:]
                )
                self.assertEqual("1", environment["CASA_RS_BENCH_PROFILE_REPEATS"])
                self.assertEqual(
                    "preserved",
                    environment["ALTERNATING_COMPARISON_TEST_SENTINEL"],
                )
            self.assertTrue(report_path.is_file())
            details = report["results"]["alternating_comparison"]
            self.assertEqual("pass", details["verdict"]["status"])
            first_paths = details["runs"][0]["recorded_paths"]
            self.assertIn("result_json", {entry["field"] for entry in first_paths})
            self.assertIn(
                "/artifacts/products_root", {entry["field"] for entry in first_paths}
            )
            self.assertIn(
                "/results/product_comparison/panel_dir",
                {entry["field"] for entry in first_paths},
            )

    def test_managed_run_workload_options_are_rejected(self) -> None:
        config = comparison_config(run_workload_options=("--repeats=4",))

        with self.assertRaisesRegex(alternating.ComparisonError, "managed"):
            alternating.validate_config(config)

    def test_role_specific_imaging_overrides_reach_only_their_role(self) -> None:
        baseline = alternating.ScheduleItem(1, "measured", 1, 1, "baseline", "workload")
        candidate = alternating.ScheduleItem(2, "measured", 1, 2, "candidate", "workload")

        baseline_command = alternating.build_run_command(
            baseline,
            output_dir=Path("/tmp/baseline"),
            artifact_root=Path("/tmp/artifacts"),
            run_workload_options=("--stream-log",),
            imaging_overrides=("chanchunks=1",),
        )
        candidate_command = alternating.build_run_command(
            candidate,
            output_dir=Path("/tmp/candidate"),
            artifact_root=Path("/tmp/artifacts"),
            run_workload_options=("--stream-log",),
            imaging_overrides=("chanchunks=4", "parallel=true"),
        )

        self.assertEqual(
            ["--set-imaging", "chanchunks=1"], baseline_command[-2:]
        )
        self.assertEqual(
            [
                "--set-imaging",
                "chanchunks=4",
                "--set-imaging",
                "parallel=true",
            ],
            candidate_command[-4:],
        )

    def test_role_specific_imaging_override_requires_key_and_value(self) -> None:
        config = comparison_config(baseline_imaging_overrides=("chanchunks=",))

        with self.assertRaisesRegex(alternating.ComparisonError, "KEY=VALUE"):
            alternating.validate_config(config)

    def test_command_failure_stops_and_writes_partial_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = comparison_config(
                output_root=root / "output", artifact_root=root / "artifacts"
            )
            commands: list[list[str]] = []

            def runner(
                command: list[str], environment: dict[str, str]
            ) -> subprocess.CompletedProcess[str]:
                commands.append(command)
                return subprocess.CompletedProcess(
                    command, 7, "benchmark failed\n", None
                )

            report_path, report = alternating.run_comparison(
                config, command_runner=runner, comparison_id="failed-comparison"
            )

            self.assertEqual(1, len(commands))
            self.assertTrue(report_path.is_file())
            self.assertEqual("failed_execution", report["status"])
            details = report["results"]["alternating_comparison"]
            self.assertEqual("execution_failed", details["runs"][0]["result_status"])
            self.assertEqual("inconclusive", details["verdict"]["status"])


class MainExitTests(unittest.TestCase):
    def test_failed_no_slowdown_verdict_exits_nonzero(self) -> None:
        report = {
            "status": "completed",
            "results": {
                "alternating_comparison": {
                    "verdict": {"status": "fail", "no_slowdown": False}
                }
            },
        }
        with mock.patch.object(
            alternating,
            "run_comparison",
            return_value=(Path("/tmp/failed-verdict.json"), report),
        ):
            self.assertEqual(1, alternating.main(["baseline", "candidate"]))

    def test_inconclusive_verdict_exits_nonzero(self) -> None:
        report = {
            "status": "completed",
            "results": {
                "alternating_comparison": {
                    "verdict": {"status": "inconclusive", "no_slowdown": None}
                }
            },
        }
        with mock.patch.object(
            alternating,
            "run_comparison",
            return_value=(Path("/tmp/inconclusive-verdict.json"), report),
        ):
            self.assertEqual(1, alternating.main(["baseline", "candidate"]))


def comparison_config(**overrides: object) -> alternating.ComparisonConfig:
    values = {
        "baseline_workload": "baseline",
        "candidate_workload": "candidate",
        "warmup_pair_count": 0,
        "measured_pair_count": 3,
        "output_root": Path("/tmp/output"),
        "artifact_root": Path("/tmp/artifacts"),
        "slowdown_tolerance": 0.0,
        "run_workload_options": (),
    }
    values.update(overrides)
    return alternating.ComparisonConfig(**values)  # type: ignore[arg-type]


def fake_run(item: alternating.ScheduleItem, wall: float) -> dict:
    return {
        **asdict(item),
        "command": ["python3", "run_workload.py"],
        "result_status": "completed",
        "result_path": f"/tmp/{item.sequence_index}.json",
        "recorded_paths": [],
        "total_wall_seconds": wall,
        "stage_timings_ms": {"rust": {"total": 100.0}},
        "backend_identity": {
            "benchmark_features": {
                "resolved_backend": "cpu" if item.role == "baseline" else "metal"
            }
        },
    }


def fake_result(wall: float, output_dir: Path) -> dict:
    return {
        "schema_version": 2,
        "kind": "workload_run",
        "status": "completed",
        "run_id": "fake-run",
        "created_at": "2026-07-18T00:00:00Z",
        "manifest_path": "/tmp/workload.json",
        "environment": {},
        "artifacts": {"products_root": str(output_dir / "products")},
        "benchmark_features": {"backend": {"resolved_backend": "cpu"}},
        "results": {
            "rust": {"timings_seconds": {"runs": [wall], "median": wall}},
            "stage_medians_ms": {"rust": {"total": wall * 1000.0}},
            "product_comparison": {"panel_dir": str(output_dir / "panels")},
        },
    }


if __name__ == "__main__":
    unittest.main()
