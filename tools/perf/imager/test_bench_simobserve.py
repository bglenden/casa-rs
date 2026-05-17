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
