#!/usr/bin/env python3
"""Focused tests for the Wave 1 imager workload harness helpers."""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_workload


class StageBreakdownTests(unittest.TestCase):
    def test_dirty_workload_marks_clean_only_categories_skipped(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "dirty",
                "niter": 0,
            }
        }
        stages = {
            "open_measurement_set": 1.0,
            "prepare_plane_input": 2.0,
            "extract_phase_center": 3.0,
            "weighting": 4.0,
            "psf_grid": 5.0,
            "psf_fft": 6.0,
            "psf_normalize": 7.0,
            "residual_degrid_grid": 8.0,
            "residual_fft": 9.0,
            "residual_normalize": 10.0,
            "minor_cycle_solve": 0.0,
            "major_cycle_refresh": 0.0,
            "beam_fit": 0.0,
            "restore": 0.0,
            "build_coordinate_system": 11.0,
            "write_products": 12.0,
            "frontend_total": 30.0,
            "total": 40.0,
        }

        breakdown = run_workload.build_rust_stage_breakdown(plan, stages)
        categories = breakdown["categories"]

        self.assertEqual(
            categories["frontend_ms_preparation"]["total_ms"],
            6.0,
        )
        self.assertEqual(categories["gridding_degridding"]["total_ms"], 13.0)
        self.assertEqual(categories["fft"]["total_ms"], 15.0)
        self.assertEqual(categories["normalization_pb_correction"]["total_ms"], 17.0)
        self.assertEqual(categories["deconvolution_minor_cycle"]["status"], "skipped")
        self.assertEqual(
            categories["model_prediction_and_residual_refresh"]["status"],
            "skipped",
        )
        self.assertEqual(categories["preview_sidecar_generation"]["status"], "skipped")

    def test_clean_workload_reports_clean_categories_when_measured(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "clean",
                "niter": 25,
            }
        }
        stages = {
            "minor_cycle_solve": 3.5,
            "major_cycle_refresh": 8.0,
            "beam_fit": 1.0,
            "restore": 2.0,
        }

        breakdown = run_workload.build_rust_stage_breakdown(plan, stages)
        categories = breakdown["categories"]

        self.assertEqual(categories["deconvolution_minor_cycle"]["status"], "measured")
        self.assertEqual(categories["deconvolution_minor_cycle"]["total_ms"], 3.5)
        self.assertEqual(
            categories["model_prediction_and_residual_refresh"]["status"],
            "measured",
        )
        self.assertEqual(categories["restore_and_beam_fit"]["total_ms"], 3.0)

    def test_attach_stage_breakdown_does_not_require_casa_stage_data(self) -> None:
        plan = {
            "mode": {
                "bench_mode": "dirty",
                "niter": 0,
            }
        }
        parsed = {
            "stage_medians_ms": {
                "rust": {"psf_grid": 1.0},
                "casa": {},
            }
        }

        run_workload.attach_stage_breakdown(plan, parsed)

        self.assertEqual(parsed["stage_breakdown"]["schema_version"], 1)
        self.assertEqual(parsed["stage_breakdown"]["rust"]["status"], "reported")
        self.assertEqual(parsed["stage_breakdown"]["casa"]["status"], "missing")


if __name__ == "__main__":
    unittest.main()
