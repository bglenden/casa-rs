#!/usr/bin/env python3
"""Focused tests for the Wave 1 simobserve benchmark helpers."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_simobserve


class NativePerformanceSummaryTests(unittest.TestCase):
    def test_strict_data_violation_requires_same_cell_to_fail_both_tolerances(self) -> None:
        self.assertFalse(
            bench_simobserve.strict_data_cell_violates(
                0.20,
                2000.0,
                data_atol=0.05,
                data_rtol=0.001,
            )
        )
        self.assertFalse(
            bench_simobserve.strict_data_cell_violates(
                0.001,
                0.002,
                data_atol=0.05,
                data_rtol=0.001,
            )
        )
        self.assertTrue(
            bench_simobserve.strict_data_cell_violates(
                0.20,
                1.0,
                data_atol=0.05,
                data_rtol=0.001,
            )
        )

    def test_native_performance_summary_uses_reported_streamed_io_bytes(self) -> None:
        native = {
            "best_seconds": 20.0,
            "size_bytes": 10_000_000_000,
            "last_result": {
                "report": {
                    "timing": {
                        "main_rows": {
                            "data_io_bytes": 8_000_000_000,
                            "data_io_write_millis": 4_000,
                        }
                    }
                }
            },
        }

        summary = bench_simobserve.native_performance_summary(native)

        self.assertEqual(summary["native_output_mb_per_second"], 500.0)
        self.assertEqual(summary["data_io_mb_per_second"], 2000.0)
        self.assertEqual(summary["data_io_bytes"], 8_000_000_000)
        self.assertEqual(summary["data_io_write_millis"], 4_000.0)
        self.assertEqual(
            summary["stage_timing"]["stages_millis"]["data_io_write_millis"],
            4_000.0,
        )

    def test_stage_timing_summary_reports_prediction_gpu_candidate(self) -> None:
        summary = bench_simobserve.stage_timing_summary(
            {
                "timing": {
                    "total_millis": 1000,
                    "validate_millis": 1,
                    "setup_millis": 2,
                    "metadata_millis": 3,
                    "model_prepare_millis": 4,
                    "save_millis": 5,
                    "main_rows": {
                        "uvw_and_row_setup_millis": 50,
                        "prediction_millis": 600,
                        "prediction_worker_wall_millis": 1200,
                        "prediction_gather_millis": 20,
                        "corruption_millis": 10,
                        "data_io_enqueue_millis": 10,
                        "data_io_finalize_millis": 10,
                        "data_io_assemble_millis": 20,
                        "data_io_write_millis": 30,
                        "main_write_millis": 40,
                    },
                }
            }
        )

        self.assertAlmostEqual(summary["prediction_fraction"], 0.6)
        self.assertAlmostEqual(summary["streamed_io_fraction"], 0.07)
        self.assertTrue(summary["gpu_candidate"])

    def test_casa_relative_timing_reports_both_ratios(self) -> None:
        summary = bench_simobserve.casa_relative_timing(
            {"best_seconds": 2.0, "size_bytes": 1_000_000_000},
            {"best_seconds": 10.0, "size_bytes": 2_000_000_000},
        )

        self.assertEqual(summary["native_speedup_vs_casa"], 5.0)
        self.assertEqual(summary["native_time_fraction_of_casa"], 0.2)
        self.assertEqual(summary["native_output_mb_per_second"], 500.0)
        self.assertEqual(summary["casa_output_mb_per_second"], 200.0)
        self.assertEqual(summary["native_size_fraction_of_casa"], 0.5)

    def test_analytic_native_tier_performance_marks_medium_rate(self) -> None:
        summary = bench_simobserve.analytic_native_tier_performance(
            {"tier": "medium"},
            {"request": {"model": {"kind": "analytic_components"}}},
            {
                "native_output_mb_per_second": 625.0,
                "data_io_mb_per_second": 910.0,
            },
        )

        self.assertEqual(summary["status"], "reported")
        self.assertIsNone(summary["small_native_output_mb_per_second"])
        self.assertEqual(summary["medium_native_output_mb_per_second"], 625.0)
        self.assertEqual(summary["streamed_main_column_mb_per_second"], 910.0)

    def test_analytic_native_tier_performance_skips_fits_model(self) -> None:
        summary = bench_simobserve.analytic_native_tier_performance(
            {"tier": "small"},
            {"request": {"model": {"kind": "fits_image"}}},
            {
                "native_output_mb_per_second": 625.0,
                "data_io_mb_per_second": 910.0,
            },
        )

        self.assertEqual(summary["status"], "not_applicable")
        self.assertIsNone(summary["small_native_output_mb_per_second"])

    def test_native_worker_comparison_reports_serial_auto_fixed(self) -> None:
        comparison = bench_simobserve.native_worker_comparison(
            {
                "best_seconds": 10.0,
                "median_seconds": 11.0,
                "size_bytes": 100,
            },
            {
                "best_seconds": 25.0,
                "median_seconds": 26.0,
                "size_bytes": 100,
            },
            {
                "best_seconds": 8.0,
                "median_seconds": 9.0,
                "size_bytes": 100,
            },
            primary_channel_workers=None,
            fixed_channel_workers=8,
        )

        self.assertEqual(comparison["status"], "complete")
        self.assertEqual(comparison["primary_mode"], "auto")
        self.assertEqual(comparison["serial"]["channel_workers"], 1)
        self.assertIsNone(comparison["auto"]["channel_workers"])
        self.assertEqual(comparison["fixed"]["channel_workers"], 8)
        self.assertEqual(comparison["ratios"]["auto_speedup_vs_serial"], 2.5)
        self.assertEqual(comparison["ratios"]["fixed_speedup_vs_serial"], 3.125)

    def test_native_worker_comparison_records_missing_fixed_mode(self) -> None:
        comparison = bench_simobserve.native_worker_comparison(
            {
                "best_seconds": 10.0,
                "median_seconds": 11.0,
                "size_bytes": 100,
            },
            None,
            None,
            primary_channel_workers=None,
            fixed_channel_workers=None,
        )

        self.assertEqual(comparison["status"], "partial")
        self.assertEqual(comparison["serial"]["status"], "not_run")
        self.assertEqual(comparison["fixed"]["status"], "not_run")
        self.assertIsNone(comparison["ratios"]["auto_speedup_vs_serial"])

    def test_casa_oracle_status_skips_missing_python_cleanly(self) -> None:
        status = bench_simobserve.casa_oracle_status(
            "/definitely/missing/casa-python",
            skip_casa=False,
        )

        self.assertEqual(status["status"], "skipped")
        self.assertIn("does not exist", status["reason"])

    def test_attach_casa_skip_marks_strict_values_skipped(self) -> None:
        correctness = {"status": "passed", "strict_values": None}

        bench_simobserve.attach_casa_skip_to_correctness(
            correctness,
            {"status": "skipped", "reason": "missing CASA"},
            strict_values=True,
        )

        self.assertEqual(correctness["casa_status"], "skipped")
        self.assertEqual(correctness["strict_values"]["status"], "skipped")

    def test_skipped_correctness_records_missing_casatools(self) -> None:
        correctness = bench_simobserve.skipped_correctness(
            {"status": "skipped", "reason": "missing CASA"},
            strict_values=True,
        )

        self.assertEqual(correctness["status"], "skipped")
        self.assertEqual(correctness["reasons"], ["missing CASA"])
        self.assertEqual(correctness["strict_values"]["status"], "skipped")

    def test_parse_image_product_suffixes_requires_dot_prefixes(self) -> None:
        self.assertEqual(
            bench_simobserve.parse_image_product_suffixes(".image,.residual"),
            [".image", ".residual"],
        )
        with self.assertRaisesRegex(bench_simobserve.BenchError, "start with"):
            bench_simobserve.parse_image_product_suffixes(".image,residual")

    def test_image_product_comparison_skips_without_prefixes(self) -> None:
        summary = bench_simobserve.image_product_comparison(
            "/missing/casa-python",
            None,
            None,
            [".image"],
            {"status": "skipped", "reason": "missing CASA"},
        )

        self.assertEqual(summary["status"], "not_run")

    def test_image_product_comparison_requires_paired_prefixes(self) -> None:
        summary = bench_simobserve.image_product_comparison(
            "/missing/casa-python",
            Path("native"),
            None,
            [".image"],
            {"status": "available"},
        )

        self.assertEqual(summary["status"], "skipped")
        self.assertIn("must be supplied together", summary["reason"])

    def test_oracle_comparison_summary_covers_ms_columns_and_image_products(self) -> None:
        summary = bench_simobserve.oracle_comparison_summary(
            {
                "strict_values": {
                    "status": "passed",
                    "row_count": {"native": 10, "casa": 10},
                    "rows_sampled": 10,
                    "uvw": {"max_abs": 0.0},
                    "flag_counts": {"native": {}, "casa": {}},
                    "raw_flag_mismatches": 0,
                    "effective_flag_mismatches": 0,
                    "weight": {"max_abs": 0.0},
                    "sigma": {"max_abs": 0.0},
                    "data": {"max_abs": 0.0},
                }
            },
            image_products={"status": "passed", "products": {".image": {}}},
        )

        samples = summary["measurement_set_samples"]
        self.assertEqual(samples["status"], "passed")
        self.assertEqual(samples["row_count"]["native"], 10)
        self.assertEqual(samples["uvw"]["max_abs"], 0.0)
        self.assertEqual(samples["raw_flag_mismatches"], 0)
        self.assertEqual(samples["weight"]["max_abs"], 0.0)
        self.assertEqual(samples["sigma"]["max_abs"], 0.0)
        self.assertEqual(samples["data"]["max_abs"], 0.0)
        self.assertEqual(summary["imaging_products"]["status"], "passed")

    def test_oracle_comparison_summary_preserves_skipped_ms_reason(self) -> None:
        summary = bench_simobserve.oracle_comparison_summary(
            {"status": "skipped", "reasons": ["missing CASA"], "strict_values": None},
            image_products={"status": "skipped", "reason": "missing CASA"},
        )

        self.assertEqual(summary["measurement_set_samples"]["status"], "skipped")
        self.assertEqual(summary["measurement_set_samples"]["reason"], "missing CASA")

    def test_native_performance_targets_fail_below_threshold(self) -> None:
        args = argparse.Namespace(
            require_native_throughput_mb_s=600.0,
            require_data_io_throughput_mb_s=None,
        )

        with self.assertRaisesRegex(bench_simobserve.BenchError, "below target"):
            bench_simobserve.enforce_native_performance_targets(
                args,
                {
                    "native_output_mb_per_second": 500.0,
                    "data_io_mb_per_second": 2000.0,
                },
            )

    def test_data_io_performance_targets_fail_below_threshold(self) -> None:
        args = argparse.Namespace(
            require_native_throughput_mb_s=None,
            require_data_io_throughput_mb_s=2500.0,
        )

        with self.assertRaisesRegex(bench_simobserve.BenchError, "below target"):
            bench_simobserve.enforce_native_performance_targets(
                args,
                {
                    "native_output_mb_per_second": 500.0,
                    "data_io_mb_per_second": 2000.0,
                },
            )


if __name__ == "__main__":
    unittest.main()
