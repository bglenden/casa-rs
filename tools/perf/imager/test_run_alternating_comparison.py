#!/usr/bin/env python3
"""Focused tests for counterbalanced workload comparison orchestration."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from perf_harness import ContractError, RUN_RESULT_SCHEMA_VERSION, validate_run_result
from test_support import canonical_test_environment

import run_alternating_comparison as alternating
import two_build_comparison as two_build


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

    def test_products_come_from_each_roles_first_measured_run(self) -> None:
        schedule = alternating.build_schedule(
            "workload", "workload", warmup_pair_count=2, measured_pair_count=3
        )

        self.assertEqual(
            {"baseline": 5, "candidate": 6}, alternating.product_run_indexes(schedule)
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

        validate_run_result(report, source="report")
        details = report["results"]["alternating_comparison"]
        self.assertEqual(3, len(details["paired_deltas"]))
        self.assertEqual(6, len(details["adjacent_pair_deltas"]))
        self.assertEqual("pass", details["verdict"]["status"])
        self.assertTrue(details["verdict"]["no_slowdown"])
        self.assertEqual("pass", details["verdicts"]["wall_time"]["status"])
        self.assertEqual("not_evaluated", details["verdicts"]["peak_rss"]["status"])
        self.assertEqual("not_evaluated", details["verdicts"]["products"]["status"])
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


class WallTimeVerdictTests(unittest.TestCase):
    # Baseline walls with stdev 0.405 s about a 10 s median: sigma 4.05%.
    NOISY_BASELINE = [10.0, 10.5, 9.5, 10.4, 9.6, 10.0]
    # Baseline walls with stdev 0.141 s about a 10 s median: sigma 1.41%.
    QUIET_BASELINE = [10.0, 10.2, 9.8, 10.1, 9.9, 10.0]

    def test_noise_band_widens_the_allowance_to_two_baseline_sigmas(self) -> None:
        verdict = two_build.wall_time_verdict(
            [0.05, 0.04, 0.06],
            self.NOISY_BASELINE,
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=True,
        )

        self.assertEqual("pass", verdict["status"])
        self.assertAlmostEqual(0.0405, verdict["baseline_walls"]["relative_sigma"], 4)
        self.assertAlmostEqual(0.0810, verdict["allowed_relative_delta"], 4)
        self.assertEqual(0.05, verdict["observed_median_relative_delta"])

    def test_noise_band_fails_a_slowdown_beyond_two_sigmas(self) -> None:
        verdict = two_build.wall_time_verdict(
            [0.09, 0.085, 0.095],
            self.NOISY_BASELINE,
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=True,
        )

        self.assertEqual("fail", verdict["status"])

    def test_wall_tolerance_is_the_floor_of_a_quiet_baseline(self) -> None:
        verdict = two_build.wall_time_verdict(
            [0.025, 0.026, 0.024],
            [10.0, 10.01, 9.99, 10.0, 10.0, 10.0],
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=True,
        )

        self.assertEqual("fail", verdict["status"])
        self.assertEqual(0.02, verdict["allowed_relative_delta"])
        quiet = two_build.wall_time_verdict(
            [0.025, 0.026, 0.024],
            self.QUIET_BASELINE,
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=True,
        )
        self.assertEqual("pass", quiet["status"])
        self.assertAlmostEqual(0.02828, quiet["allowed_relative_delta"], 5)

    def test_zero_noise_sigmas_is_the_slowdown_tolerance_check(self) -> None:
        verdict = two_build.wall_time_verdict(
            [0.05, 0.04, 0.06],
            self.NOISY_BASELINE,
            wall_tolerance=0.03,
            noise_sigmas=0.0,
            evidence_complete=True,
        )
        legacy = alternating.build_verdict(
            [{"relative_delta": value} for value in (0.05, 0.04, 0.06)],
            tolerance=0.03,
            evidence_complete=True,
        )

        self.assertEqual("fail", verdict["status"])
        self.assertEqual(legacy["status"], verdict["status"])
        self.assertEqual(0.03, verdict["allowed_relative_delta"])

    def test_missing_band_or_evidence_is_inconclusive(self) -> None:
        no_band = two_build.wall_time_verdict(
            [0.0, 0.0, 0.0],
            [10.0],
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=True,
        )
        incomplete = two_build.wall_time_verdict(
            [0.0, 0.0],
            self.QUIET_BASELINE,
            wall_tolerance=0.02,
            noise_sigmas=2.0,
            evidence_complete=False,
        )

        self.assertEqual("inconclusive", no_band["status"])
        self.assertIn("spread", no_band["reason"])
        self.assertEqual("inconclusive", incomplete["status"])


class PeakRssTests(unittest.TestCase):
    def test_peak_rss_is_the_terminal_imager_peak(self) -> None:
        result = {
            "results": {"backend_plan_logs": {"summary": {"peak_rss_bytes": 4096}}},
            "benchmark_features": {"resources": {"peak_rss_bytes": 1}},
        }
        mirrored = {"benchmark_features": {"resources": {"peak_rss_bytes": 2048}}}

        self.assertEqual(4096, two_build.extract_peak_rss_bytes(result))
        self.assertEqual(2048, two_build.extract_peak_rss_bytes(mirrored))
        for value in (None, 0, -1, 1024.0, True, "4096"):
            with self.subTest(value=value):
                rejected = {
                    "results": {
                        "backend_plan_logs": {"summary": {"peak_rss_bytes": value}}
                    }
                }
                self.assertIsNone(two_build.extract_peak_rss_bytes(rejected))
        self.assertIsNone(two_build.extract_peak_rss_bytes({}))

    def test_run_summary_records_workers_backend_and_phase_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            prefix = str(Path(tmp) / "rust")
            write_run_summary(prefix, [".image", ".psf"], workers=4)

            summary = two_build.read_run_summary(prefix)
            missing = two_build.read_run_summary(str(Path(tmp) / "absent"))

        self.assertEqual("completed", summary["status"])
        self.assertEqual(4, summary["workers"])
        self.assertEqual("cpu", summary["backend"])
        self.assertEqual("f64", summary["precision"])
        self.assertEqual(3000, summary["process_peak_rss_bytes"])
        self.assertEqual(2500, summary["peak_reserved_bytes"])
        self.assertEqual(
            ["preparation", "products"], [p["name"] for p in summary["phases"]]
        )
        self.assertEqual("missing", missing["status"])

    def test_rss_within_tolerance_passes_and_beyond_it_fails(self) -> None:
        baseline = [1000] * 6
        within = two_build.peak_rss_verdict(
            baseline, [1040] * 6, rss_tolerance=0.05, evidence_complete=True
        )
        beyond = two_build.peak_rss_verdict(
            baseline, [1060] * 6, rss_tolerance=0.05, evidence_complete=True
        )

        self.assertEqual("pass", within["status"])
        self.assertAlmostEqual(1.04, within["observed_ratio"])
        self.assertEqual("fail", beyond["status"])
        self.assertAlmostEqual(1.05, beyond["allowed_ratio"])

    def test_missing_rss_is_inconclusive_not_a_pass(self) -> None:
        verdict = two_build.peak_rss_verdict(
            [1000] * 6,
            [1000] * 5 + [None],
            rss_tolerance=0.05,
            evidence_complete=True,
        )

        self.assertEqual("inconclusive", verdict["status"])
        self.assertEqual(1, verdict["missing_run_count"])


class ProductVerdictTests(unittest.TestCase):
    def test_every_product_within_nrms_passes(self) -> None:
        verdict = two_build.product_verdict(
            summarized({".image": 4.0e-7, ".psf": 0.0}), product_nrms=1.0e-6
        )

        self.assertEqual("pass", verdict["status"])
        self.assertEqual(4.0e-7, verdict["worst_nrms"])
        self.assertEqual({".image": "pass", ".psf": "pass"}, verdict["products"])

    def test_one_product_beyond_nrms_fails(self) -> None:
        verdict = two_build.product_verdict(
            summarized({".image": 2.0e-6, ".psf": 0.0}), product_nrms=1.0e-6
        )

        self.assertEqual("fail", verdict["status"])
        self.assertIn(".image NRMS", verdict["reason"])

    def test_disagreeing_products_fail(self) -> None:
        shape = summarized({".image": 0.0})
        shape["products"][".image"] = {"status": "shape_mismatch", "nrms": None}
        lost = summarized({".image": 0.0})
        lost["products"][".image"] = {
            "status": "missing",
            "nrms": None,
            "left_exists": False,
            "right_exists": True,
        }
        inventory = summarized({".image": 0.0})
        inventory["product_inventory"]["status"] = "mismatch"
        inventory["product_inventory"]["left_extra"] = [".weight"]

        for name, comparison in (
            ("shape", shape),
            ("lost", lost),
            ("inventory", inventory),
        ):
            with self.subTest(name=name):
                verdict = two_build.product_verdict(comparison, product_nrms=1.0e-6)
                self.assertEqual("fail", verdict["status"])

    def test_unmeasured_products_are_inconclusive(self) -> None:
        unmeasured = summarized({".image": 0.0})
        unmeasured["products"][".image"] = {"status": "no_finite_overlap", "nrms": None}
        unavailable = {
            "status": "unavailable",
            "reason": "CASA Python is not configured",
            "products": {},
        }

        for comparison in (
            None,
            two_build.not_compared("products were not compared"),
            unavailable,
            unmeasured,
            summarized({}),
        ):
            with self.subTest(comparison=comparison):
                verdict = two_build.product_verdict(comparison, product_nrms=1.0e-6)
                self.assertEqual("inconclusive", verdict["status"])

    def test_product_list_falls_back_to_the_manifest_without_exact_inventory(
        self,
    ) -> None:
        def run(index: int, prefix: str) -> dict[str, Any]:
            return {
                "sequence_index": index,
                "run_summary": {"status": "missing"},
                "products": {"rust_prefix": prefix, "retained": True},
            }

        result = {
            "comparison": {"products": [".psf", ".image"], "full_chunk_elements": 64}
        }
        comparer = FakeComparer({".image": 0.0, ".psf": 0.0})
        with tempfile.TemporaryDirectory() as tmp:
            summary = two_build.compare_role_products(
                {
                    "baseline": (run(1, "/a/rust"), result),
                    "candidate": (run(2, "/b/rust"), {}),
                },
                product_nrms=1.0e-6,
                artifact_root=Path(tmp) / "artifacts",
                report_root=Path(tmp) / "report",
                comparer=comparer,
                casa_python=None,
            )
            shared = two_build.compare_role_products(
                {
                    "baseline": (run(1, "/a/rust"), result),
                    "candidate": (run(2, "/a/rust"), {}),
                },
                product_nrms=1.0e-6,
                artifact_root=Path(tmp) / "artifacts",
                report_root=Path(tmp) / "report",
                comparer=comparer,
                casa_python=None,
            )

        [request] = comparer.requests
        self.assertEqual([".image", ".psf"], request["products"])
        self.assertFalse(request["require_exact_product_inventory"])
        self.assertEqual(64, request["full_chunk_elements"])
        self.assertEqual("workload_manifest", summary["product_list_source"])
        self.assertEqual("not_compared", shared["status"])
        self.assertIn("one prefix", shared["reason"])

    def test_request_compares_candidate_against_baseline_over_full_arrays(
        self,
    ) -> None:
        request = two_build.product_comparison_request(
            baseline_prefix="/a/rust",
            candidate_prefix="/b/rust",
            suffixes=[".image", ".psf"],
            exact_inventory=True,
            product_nrms=1.0e-6,
            full_chunk_elements=1_000_000,
            panel_dir=Path("/artifacts/panels"),
            structure_workspace_dir=Path("/artifacts/structure"),
        )
        from perf_harness.image_compare import normalize_comparison_request

        normalized = normalize_comparison_request(request)

        self.assertEqual("full", normalized["mode"])
        self.assertEqual("/b/rust", normalized["left_prefix"])
        self.assertEqual("/a/rust", normalized["right_prefix"])
        self.assertEqual("candidate", normalized["left_label"])
        self.assertTrue(normalized["require_exact_product_inventory"])
        self.assertEqual(
            {"diff_rms_over_right_rms": 1.0e-6},
            normalized["tolerances"]["default"],
        )


class GateTests(unittest.TestCase):
    def test_slowdown_tolerance_keeps_its_meaning_in_the_default_gate(self) -> None:
        args = alternating.build_arg_parser().parse_args(
            ["a", "b", "--slowdown-tolerance", "0.03"]
        )
        config = comparison_config(**alternating.gate_settings(args))

        self.assertEqual("slowdown", config.gate)
        self.assertEqual((0.03, 0.0), config.wall_thresholds())
        self.assertEqual(
            {"wall_tolerance_fraction": 0.03, "noise_sigmas": 0.0},
            config.thresholds(),
        )

    def test_blocking_gate_takes_decision_four_defaults_and_overrides(self) -> None:
        parser = alternating.build_arg_parser()
        defaults = comparison_config(
            **alternating.gate_settings(
                parser.parse_args(["w", "w", "--gate", "blocking"])
            )
        )
        overridden = comparison_config(
            **alternating.gate_settings(
                parser.parse_args(
                    [
                        "w",
                        "w",
                        "--gate",
                        "blocking",
                        "--wall-tolerance",
                        "0.01",
                        "--noise-sigmas",
                        "3",
                        "--rss-tolerance",
                        "0.1",
                        "--product-nrms",
                        "1e-5",
                    ]
                )
            )
        )

        self.assertEqual(
            {
                "wall_tolerance_fraction": 0.02,
                "noise_sigmas": 2.0,
                "rss_tolerance_fraction": 0.05,
                "product_nrms": 1.0e-6,
            },
            defaults.thresholds(),
        )
        self.assertEqual(
            {
                "wall_tolerance_fraction": 0.01,
                "noise_sigmas": 3.0,
                "rss_tolerance_fraction": 0.1,
                "product_nrms": 1.0e-5,
            },
            overridden.thresholds(),
        )

    def test_a_threshold_of_the_other_gate_is_rejected(self) -> None:
        self.assertEqual(2, alternating.main(["a", "b", "--rss-tolerance", "0.1"]))
        self.assertEqual(
            2,
            alternating.main(
                ["a", "b", "--gate", "blocking", "--slowdown-tolerance", "0.1"]
            ),
        )

    def test_negative_threshold_is_rejected(self) -> None:
        with self.assertRaisesRegex(alternating.ComparisonError, "--product-nrms"):
            alternating.validate_config(comparison_config(product_nrms=-1.0))


class RoleRepoTests(unittest.TestCase):
    def test_each_role_runs_its_own_checkout_and_records_its_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            baseline_repo = fake_checkout(root, "baseline")
            candidate_repo = fake_checkout(root, "candidate")
            baseline_commit = git_commit(baseline_repo)
            candidate_commit = git_commit(candidate_repo)
            (candidate_repo / "scripts/bench-imager-vs-casa.sh").write_text("# edit\n")
            binary = baseline_repo / "target/release/casars-imager"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"baseline imager")
            config = comparison_config(
                baseline_workload="workload",
                candidate_workload="workload",
                output_root=root / "output",
                artifact_root=root / "artifacts",
                baseline_repo=baseline_repo,
                candidate_repo=candidate_repo,
            )
            runner = FakeWorkloadRunner(root)

            _, report = alternating.run_comparison(
                config, command_runner=runner, comparison_id="roles"
            )

            for command, _, cwd in runner.calls:
                output_dir = Path(command[command.index("--output-dir") + 1])
                role = output_dir.name.rsplit("-", 1)[-1]
                role_repo = baseline_repo if role == "baseline" else candidate_repo
                self.assertEqual(role_repo, cwd)
                self.assertEqual(
                    str(role_repo / "tools/perf/imager/run_workload.py"), command[1]
                )
                self.assertEqual("workload", command[2])
            details = report["results"]["alternating_comparison"]
            roles = details["roles"]
            self.assertEqual(str(baseline_repo), roles["baseline"]["repo"])
            self.assertEqual(baseline_commit, roles["baseline"]["commit"])
            self.assertFalse(roles["baseline"]["dirty"])
            self.assertEqual(candidate_commit, roles["candidate"]["commit"])
            self.assertTrue(roles["candidate"]["dirty"])
            self.assertEqual(
                str(candidate_repo), details["configuration"]["candidate_repo"]
            )
            self.assertEqual(
                {str(baseline_repo)},
                {run["repo"] for run in details["runs"] if run["role"] == "baseline"},
            )
            self.assertEqual(
                [hashlib.sha256(b"baseline imager").hexdigest()],
                details["measured_summaries"]["baseline"]["casars_imager_sha256"],
            )
            self.assertEqual(
                [], details["measured_summaries"]["candidate"]["casars_imager_sha256"]
            )

    def test_two_checkouts_sharing_a_build_directory_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_repo = fake_checkout(root, "baseline")
            candidate_repo = fake_checkout(root, "candidate")
            repos = {"baseline": baseline_repo, "candidate": candidate_repo}

            with self.assertRaisesRegex(
                alternating.ComparisonError, "CARGO_TARGET_DIR"
            ):
                two_build.validate_role_repos(repos, {"CARGO_TARGET_DIR": "/shared"})
            two_build.validate_role_repos(
                {"baseline": baseline_repo, "candidate": baseline_repo},
                {"CARGO_TARGET_DIR": "/shared"},
            )
            shared = root / "shared-target"
            shared.mkdir()
            (baseline_repo / "target").symlink_to(shared)
            (candidate_repo / "target").symlink_to(shared)
            with self.assertRaisesRegex(alternating.ComparisonError, "share"):
                two_build.validate_role_repos(repos, {})

    def test_a_checkout_without_the_harness_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = comparison_config(candidate_repo=Path(tmp))

            with self.assertRaisesRegex(alternating.ComparisonError, "run_workload.py"):
                alternating.validate_config(config, environment={})


class BlockingComparisonTests(unittest.TestCase):
    def test_aa_run_of_one_build_records_the_band_and_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            config = blocking_config(root, warmup_pair_count=1)
            runner = FakeWorkloadRunner(root)
            comparer = FakeComparer({".image": 0.0, ".psf": 0.0, ".sumwt": 0.0})

            report_path, report = alternating.run_comparison(
                config,
                command_runner=runner,
                product_comparer=comparer,
                comparison_id="aa",
            )

            self.assertTrue(report_path.is_file())
            self.assertEqual("completed", report["status"])
            details = report["results"]["alternating_comparison"]
            self.assertEqual(
                {"workload"}, {item["workload"] for item in details["schedule"]}
            )
            self.assertEqual(
                {"pass"},
                {verdict["status"] for verdict in details["verdicts"].values()},
            )
            band = details["verdicts"]["wall_time"]["baseline_walls"]
            self.assertEqual(6, band["count"])
            self.assertGreater(band["relative_sigma"], 0.0)
            self.assertEqual("pass", details["verdict"]["status"])
            self.assertEqual("blocking", details["configuration"]["gate"])
            self.assertEqual([4], details["measured_summaries"]["candidate"]["workers"])
            self.assertEqual(
                1000.0,
                details["measured_summaries"]["baseline"]["peak_rss_bytes"]["median"],
            )

    def test_products_of_one_measured_run_per_role_are_compared_and_the_rest_pruned(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            config = blocking_config(root, warmup_pair_count=1)
            runner = FakeWorkloadRunner(root)
            comparer = FakeComparer({".image": 0.0, ".psf": 0.0, ".sumwt": 0.0})

            _, report = alternating.run_comparison(
                config,
                command_runner=runner,
                product_comparer=comparer,
                comparison_id="products",
            )

            details = report["results"]["alternating_comparison"]
            runs = {run["sequence_index"]: run for run in details["runs"]}
            baseline_run, candidate_run = runs[3], runs[4]
            self.assertTrue(baseline_run["products"]["retained"])
            self.assertTrue(candidate_run["products"]["retained"])
            [request] = comparer.requests
            self.assertEqual(
                candidate_run["products"]["rust_prefix"], request["left_prefix"]
            )
            self.assertEqual(
                baseline_run["products"]["rust_prefix"], request["right_prefix"]
            )
            self.assertEqual([".image", ".psf", ".sumwt"], request["products"])
            self.assertEqual("full", request["mode"])
            self.assertTrue(request["require_exact_product_inventory"])
            self.assertEqual(
                1.0e-6, request["tolerances"]["default"]["diff_rms_over_right_rms"]
            )
            self.assertTrue(Path(request["right_prefix"] + ".image").is_dir())
            self.assertTrue(Path(request["left_prefix"] + ".image").is_dir())
            pruned = [run for index, run in runs.items() if index not in {3, 4}]
            self.assertEqual(len(details["runs"]) - 2, len(pruned))
            for run in pruned:
                self.assertFalse(run["products"]["retained"])
                [path] = run["products"]["pruned"]
                self.assertFalse(Path(path).exists())
            self.assertEqual(
                "baseline_run_summary",
                details["product_comparison"]["product_list_source"],
            )

    def test_slowdown_gate_keeps_every_runs_products(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            config = comparison_config(
                baseline_workload="workload",
                candidate_workload="workload",
                output_root=root / "output",
                artifact_root=root / "artifacts",
            )

            _, report = alternating.run_comparison(
                config, command_runner=FakeWorkloadRunner(root), comparison_id="keep"
            )

            details = report["results"]["alternating_comparison"]
            self.assertIsNone(details["product_comparison"])
            for run in details["runs"]:
                self.assertTrue(run["products"]["retained"])
                self.assertTrue(
                    Path(run["products"]["rust_prefix"] + ".image").is_dir()
                )

    def test_a_slower_candidate_fails_the_wall_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            runner = FakeWorkloadRunner(root, candidate_slowdown=0.10)

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=runner,
                product_comparer=FakeComparer({".image": 0.0}),
                comparison_id="slow",
            )

            details = report["results"]["alternating_comparison"]
            self.assertEqual("out_of_tolerance", report["status"])
            self.assertEqual("fail", details["verdicts"]["wall_time"]["status"])
            self.assertEqual("pass", details["verdicts"]["peak_rss"]["status"])
            self.assertFalse(details["verdict"]["no_slowdown"])

    def test_missing_rss_makes_the_comparison_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            runner = FakeWorkloadRunner(root, drop_rss_at={8})

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=runner,
                product_comparer=FakeComparer({".image": 0.0}),
                comparison_id="no-rss",
            )

            details = report["results"]["alternating_comparison"]
            self.assertEqual("failed_comparison", report["status"])
            self.assertEqual("pass", details["verdicts"]["wall_time"]["status"])
            self.assertEqual("inconclusive", details["verdicts"]["peak_rss"]["status"])
            self.assertEqual("inconclusive", details["verdict"]["status"])
            self.assertIn("peak_rss", details["verdict"]["reason"])

    def test_unavailable_comparator_makes_the_comparison_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=FakeWorkloadRunner(root),
                product_comparer=lambda **_: {
                    "status": "unavailable",
                    "reason": "CASA Python is not configured",
                    "products": {},
                },
                comparison_id="no-comparator",
            )

            details = report["results"]["alternating_comparison"]
            self.assertEqual("failed_comparison", report["status"])
            self.assertEqual("inconclusive", details["verdicts"]["products"]["status"])
            self.assertIn("CASA Python", details["verdicts"]["products"]["reason"])

    def test_the_real_comparator_without_casa_python_is_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            os.environ.pop("CASA_RS_CASA_PYTHON", None)

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=FakeWorkloadRunner(root),
                comparison_id="real-comparator",
            )

            details = report["results"]["alternating_comparison"]
            comparison = details["product_comparison"]
            self.assertEqual("unavailable", comparison["status"])
            request = json.loads(Path(comparison["input"]).read_text(encoding="utf-8"))
            self.assertEqual("full", request["mode"])
            self.assertEqual("candidate", request["left_label"])
            self.assertEqual("inconclusive", details["verdicts"]["products"]["status"])
            self.assertEqual("failed_comparison", report["status"])

    def test_a_failed_run_leaves_every_verdict_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)
            comparer = FakeComparer({".image": 0.0})

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=FakeWorkloadRunner(root, fail_at=6),
                product_comparer=comparer,
                comparison_id="failed",
            )

            details = report["results"]["alternating_comparison"]
            self.assertEqual("failed_execution", report["status"])
            self.assertEqual([], comparer.requests)
            self.assertEqual("not_compared", details["product_comparison"]["status"])
            self.assertEqual(
                {"inconclusive"},
                {verdict["status"] for verdict in details["verdicts"].values()},
            )

    def test_differing_products_fail_the_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, clean_cargo_target():
            root = Path(tmp)

            _, report = alternating.run_comparison(
                blocking_config(root),
                command_runner=FakeWorkloadRunner(root),
                product_comparer=FakeComparer(
                    {".image": 3.0e-6, ".psf": 0.0, ".sumwt": 0.0}
                ),
                comparison_id="differ",
            )

            details = report["results"]["alternating_comparison"]
            self.assertEqual("out_of_tolerance", report["status"])
            self.assertEqual("fail", details["verdicts"]["products"]["status"])
            self.assertEqual(3.0e-6, details["verdicts"]["products"]["worst_nrms"])

    def test_schema_rejects_an_unknown_gate_verdict_status(self) -> None:
        config = comparison_config()
        schedule = alternating.build_schedule(
            "baseline", "candidate", warmup_pair_count=0, measured_pair_count=3
        )
        report = alternating.build_report(
            config,
            comparison_id="comparison",
            report_path=Path("/tmp/comparison.json"),
            comparison_artifact_root=Path("/tmp/artifacts/comparison"),
            schedule=schedule,
            runs=[fake_run(item, 10.0) for item in schedule],
        )
        details = report["results"]["alternating_comparison"]
        details["verdicts"]["peak_rss"]["status"] = "maybe"

        with self.assertRaisesRegex(ContractError, "peak_rss.status"):
            validate_run_result(report, source="report")


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
            directories: list[Path] = []

            def runner(
                command: list[str], environment: dict[str, str], cwd: Path
            ) -> subprocess.CompletedProcess[str]:
                commands.append(command)
                environments.append(environment)
                directories.append(cwd)
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
            self.assertEqual({alternating.REPO_ROOT}, set(directories))
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
        candidate = alternating.ScheduleItem(
            2, "measured", 1, 2, "candidate", "workload"
        )

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

        self.assertEqual(["--set-imaging", "chanchunks=1"], baseline_command[-2:])
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
                command: list[str], environment: dict[str, str], cwd: Path
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


def blocking_config(root: Path, **overrides: object) -> alternating.ComparisonConfig:
    values: dict[str, object] = {
        "baseline_workload": "workload",
        "candidate_workload": "workload",
        "output_root": root / "output",
        "artifact_root": root / "artifacts",
        "gate": "blocking",
    }
    values.update(overrides)
    return comparison_config(**values)


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


def fake_result(
    wall: float,
    output_dir: Path,
    *,
    peak_rss_bytes: int | None = None,
    products_root: Path | None = None,
    rust_prefix: str | None = None,
) -> dict:
    environment = canonical_test_environment()
    environment["migration"] = {
        "source_schema_version": 2,
        "method": "synthetic historical comparison fixture",
    }
    results: dict[str, Any] = {
        "rust": {"timings_seconds": {"runs": [wall], "median": wall}},
        "stage_medians_ms": {"rust": {"total": wall * 1000.0}},
        "product_comparison": {
            "status": "completed",
            "products": {},
            "beam_info": {"status": "synthetic_fixture"},
            "structured_difference_review": {
                "label": "good",
                "summary": "synthetic comparison fixture",
            },
            "input": str(output_dir / "comparison-request.json"),
            "log": str(output_dir / "comparison.log"),
            "panel_dir": str(output_dir / "panels"),
        },
    }
    if peak_rss_bytes is not None:
        results["backend_plan_logs"] = {
            "schema_version": 1,
            "summary": {"peak_rss_bytes": peak_rss_bytes},
        }
    if rust_prefix is not None:
        results["product_paths"] = {"rust_prefix": rust_prefix}
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": "completed",
        "run_id": "fake-run",
        "created_at": "2026-07-18T00:00:00Z",
        "started_at": "2026-07-18T00:00:01Z",
        "completed_at": "2026-07-18T00:00:02Z",
        "exit_code": 0,
        "manifest_path": "/tmp/workload.json",
        "environment": environment,
        "artifacts": {"products_root": str(products_root or output_dir / "products")},
        "benchmark_features": {"backend": {"resolved_backend": "cpu"}},
        "results": results,
    }


def write_run_summary(prefix: str, suffixes: list[str], *, workers: int) -> None:
    summary = {
        "status": "completed",
        "workers": workers,
        "workers_policy": workers,
        "backend": "cpu",
        "precision": "f64",
        "phases": [
            {
                "name": "preparation",
                "completed": True,
                "seconds": 0.5,
                "process_peak_rss_bytes": 2000,
                "peak_reserved_bytes": 2500,
                "reserved_bytes": 0,
            },
            {
                "name": "products",
                "completed": True,
                "seconds": 0.1,
                "process_peak_rss_bytes": 3000,
                "peak_reserved_bytes": 1000,
                "reserved_bytes": 0,
            },
        ],
        "products": [prefix + suffix for suffix in suffixes],
    }
    Path(prefix + ".summary.json").write_text(json.dumps(summary), encoding="utf-8")


class FakeWorkloadRunner:
    """Stands in for ``run_workload.py``: keeps products the way it does, under
    ``<artifact-root>/products/<run-id>/rust/rust``, and prints a result path.
    Baseline walls repeat a small noisy cycle; the candidate's are scaled."""

    BASELINE_WALLS = (10.0, 10.2, 9.8, 10.1, 9.9, 10.0)

    def __init__(
        self,
        root: Path,
        *,
        candidate_slowdown: float = 0.0,
        drop_rss_at: set[int] = frozenset(),  # type: ignore[assignment]
        fail_at: int | None = None,
        suffixes: tuple[str, ...] = (".image", ".psf", ".sumwt"),
    ) -> None:
        self.root = root
        self.candidate_slowdown = candidate_slowdown
        self.drop_rss_at = drop_rss_at
        self.fail_at = fail_at
        self.suffixes = suffixes
        self.calls: list[tuple[list[str], dict[str, str], Path]] = []

    def __call__(
        self, command: list[str], environment: dict[str, str], cwd: Path
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((command, environment, cwd))
        sequence = len(self.calls)
        if sequence == self.fail_at:
            return subprocess.CompletedProcess(command, 3, "Error: boom\n", None)
        output_dir = Path(command[command.index("--output-dir") + 1])
        artifact_root = Path(command[command.index("--artifact-root") + 1])
        role = output_dir.name.rsplit("-", 1)[-1]
        wall = self.BASELINE_WALLS[sequence % len(self.BASELINE_WALLS)]
        if role == "candidate":
            wall *= 1.0 + self.candidate_slowdown
        products_root = artifact_root / "products" / f"run-{sequence:03d}"
        prefix = str(products_root / "rust" / "rust")
        for suffix in self.suffixes:
            Path(prefix + suffix).mkdir(parents=True)
        write_run_summary(prefix, list(self.suffixes), workers=4)
        output_dir.mkdir(parents=True, exist_ok=True)
        result_path = output_dir / "result.json"
        result = fake_result(
            wall,
            output_dir,
            peak_rss_bytes=None if sequence in self.drop_rss_at else 1000,
            products_root=products_root,
            rust_prefix=prefix,
        )
        result_path.write_text(json.dumps(result), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, f"{result_path}\n", None)


class FakeComparer:
    """Stands in for ``perf_harness.image_compare.compare_products``."""

    def __init__(self, nrms: dict[str, float]) -> None:
        self.nrms = nrms
        self.requests: list[dict[str, Any]] = []

    def __call__(
        self,
        *,
        request: dict[str, Any],
        artifact_prefix: Path,
        casa_python: str | None,
    ) -> dict[str, Any]:
        self.requests.append(request)
        return {
            "status": "completed",
            "reason": None,
            "left_label": request["left_label"],
            "right_label": request["right_label"],
            "left_prefix": request["left_prefix"],
            "right_prefix": request["right_prefix"],
            "product_inventory": {
                "status": "matched",
                "left_missing": [],
                "left_extra": [],
                "right_missing": [],
                "right_extra": [],
            },
            "tolerance_evaluation": {
                "status": "passed",
                "failed_checks": [],
                "incomplete_checks": [],
            },
            "output": str(artifact_prefix.with_suffix(".comparison.json")),
            "products": {
                suffix: {
                    "status": "compared",
                    "diff_rms_over_right_rms": value,
                    "shape": [8, 8, 1, 1],
                }
                for suffix, value in self.nrms.items()
            },
        }


def summarized(nrms: dict[str, float]) -> dict[str, Any]:
    comparer = FakeComparer(nrms)
    return two_build.summarize_product_comparison(
        comparer(
            request={
                "left_label": "candidate",
                "right_label": "baseline",
                "left_prefix": "/b/rust",
                "right_prefix": "/a/rust",
            },
            artifact_prefix=Path("/tmp/comparison"),
            casa_python=None,
        )
    )


def fake_checkout(root: Path, name: str) -> Path:
    repo = root / name
    (repo / "tools/perf/imager").mkdir(parents=True)
    (repo / "tools/perf/imager/run_workload.py").write_text("# stand-in\n")
    (repo / "scripts").mkdir()
    (repo / "scripts/bench-imager-vs-casa.sh").write_text("# stand-in\n")
    return repo


def git_commit(repo: Path) -> str:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-c", "init.defaultBranch=main", *args],
            cwd=repo,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.strip()

    git("init", "-q")
    git("add", "-A")
    git(
        "-c",
        "user.name=test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-q",
        "-m",
        "stand-in checkout",
    )
    return git("rev-parse", "HEAD")


class clean_cargo_target:
    """Run with ``CARGO_TARGET_DIR`` unset, as a two-checkout comparison needs."""

    def __enter__(self) -> None:
        self.patch = mock.patch.dict(os.environ)
        self.patch.start()
        os.environ.pop("CARGO_TARGET_DIR", None)

    def __exit__(self, *exc: object) -> None:
        self.patch.stop()


if __name__ == "__main__":
    unittest.main()
