#!/usr/bin/env python3
"""Focused tests for the Wave 1 imager workload harness helpers."""

from __future__ import annotations

import unittest
from unittest import mock
from pathlib import Path
import tempfile
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_workload


class StageBreakdownTests(unittest.TestCase):
    def test_parse_log_marks_missing_timing_sections_without_claiming_runs(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """Rust release CLI timings (seconds):
  run=1 real=1.500
  median=1.500

CASA PySynthesisImager stage medians (milliseconds):
  total=42.000
"""
        )

        self.assertEqual("ran", parsed["rust"]["status"])
        self.assertIsNone(parsed["rust"]["reason"])
        self.assertEqual(1.5, parsed["rust"]["timings_seconds"]["median"])
        self.assertEqual("missing", parsed["casa"]["status"])
        self.assertIn("not reported", parsed["casa"]["reason"])
        self.assertIsNone(parsed["casa"]["timings_seconds"]["median"])

    def test_empty_results_include_reasons_for_both_sides(self) -> None:
        results = run_workload.empty_results(
            casa_status="blocked",
            reason="benchmark command exited 2",
        )

        self.assertEqual("not_run", results["rust"]["status"])
        self.assertEqual("benchmark command exited 2", results["rust"]["reason"])
        self.assertEqual("blocked", results["casa"]["status"])
        self.assertEqual("benchmark command exited 2", results["casa"]["reason"])

    def test_unsupported_wterm_fails_in_preflight(self) -> None:
        manifest = {
            "id": "unsupported-wterm",
            "mode_id": "standard-mfs-dirty-wterm",
            "dataset": {
                "key": "fake.ms",
                "path": "/tmp/fake.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
                "wterm": "direct",
            },
        }

        with self.assertRaisesRegex(run_workload.HarnessError, "wterm='direct'"):
            run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

    def test_missing_casa_python_fails_before_running_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            dataset = Path(tempdir) / "input.ms"
            dataset.mkdir()
            manifest = {
                "id": "requires-casa",
                "mode_id": "standard-mfs-dirty-control",
                "dataset": {
                    "key": "input.ms",
                    "path": str(dataset),
                },
                "imaging": {
                    "mode": "dirty",
                    "specmode": "mfs",
                    "gridder": "standard",
                },
            }

            with mock.patch.dict("os.environ", {}, clear=True):
                with self.assertRaisesRegex(
                    run_workload.HarnessError,
                    "CASA_RS_CASA_PYTHON is required",
                ):
                    run_workload.build_plan(
                        manifest_path=Path("manifest.json"),
                        manifest=manifest,
                        repeats_override=1,
                        run_label_override=None,
                        storage_label_override=None,
                        dry_run=False,
                    )

    def test_missing_dataset_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "missing-dataset",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "missing.ms",
                "path": "/definitely/not/a/dataset.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"CASA_RS_CASA_PYTHON": sys.executable},
            clear=True,
        ):
            with self.assertRaisesRegex(
                run_workload.HarnessError,
                "dataset path does not exist",
            ):
                run_workload.build_plan(
                    manifest_path=Path("manifest.json"),
                    manifest=manifest,
                    repeats_override=1,
                    run_label_override=None,
                    storage_label_override=None,
                    dry_run=False,
                )

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

    def test_phasecenter_field_flows_to_environment(self) -> None:
        manifest = {
            "id": "mosaic-smoke",
            "mode_id": "mosaic-mfs-clean-primary",
            "dataset": {
                "key": "mosaic.ms",
                "relative_path": "wave1/vla/mosaic/small/ms/mosaic.ms",
                "root_env": "CASA_RS_IMPERF_DATA_ROOT",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "mosaic",
                "field": "",
                "phasecenter_field": 0,
                "spw": "0",
                "deconvolver": "mtmfs",
                "nterms": 2,
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"CASA_RS_IMPERF_DATA_ROOT": "/tmp", "CASA_RS_CASA_PYTHON": "/tmp/casa-python"},
            clear=False,
        ):
            plan = run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_PHASECENTER_FIELD"])
        self.assertEqual("2", plan["command"]["env"]["IMAGER_BENCH_NTERMS"])
        self.assertEqual(2, plan["mode"]["nterms"])

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

    def test_parse_rust_stage_section_keeps_full_core_timing_set(self) -> None:
        log = """Rust stage medians (milliseconds):
  run=1 frontend_total_ms=100.000 open_ms=1.000 prepare_ms=2.000 phase_center_ms=3.000 imaging_ms=4.000 coords_ms=5.000 write_ms=6.000 core_total_ms=40.000 controller_ms=7.000 weighting_ms=8.000 major_refresh_ms=9.000 psf_grid_ms=10.000 psf_fft_ms=11.000 psf_normalize_ms=12.000 model_fft_ms=13.000 residual_grid_ms=14.000 residual_fft_ms=15.000 residual_normalize_ms=16.000 minor_ms=17.000 minor_solve_ms=18.000 beam_fit_ms=19.000 restore_ms=20.000
  frontend:
  open_measurement_set=1.000
  prepare_plane_input=2.000
  extract_phase_center=3.000
  run_imaging=4.000
  build_coordinate_system=5.000
  write_products=6.000
  frontend_total=100.000
  core:
  controller_overhead=7.000
  weighting=8.000
  psf_grid=10.000
  psf_fft=11.000
  psf_normalize=12.000
  model_fft=13.000
  residual_degrid_grid=14.000
  residual_fft=15.000
  residual_normalize=16.000
  major_cycle_refresh=9.000
  minor_cycle=17.000
  minor_cycle_solve=18.000
  beam_fit=19.000
  restore=20.000
  total=40.000

CASA tclean timings (seconds):
"""

        stages = run_workload.parse_stage_section(log, "Rust stage medians")

        for name in [
            "psf_normalize",
            "model_fft",
            "residual_normalize",
            "major_cycle_refresh",
            "minor_cycle",
            "minor_cycle_solve",
            "beam_fit",
            "restore",
        ]:
            self.assertIn(name, stages)
        self.assertEqual(stages["psf_normalize"], 12.0)
        self.assertEqual(stages["model_fft"], 13.0)
        self.assertEqual(stages["restore"], 20.0)
        self.assertNotIn("niter", stages)
        self.assertNotIn("imsize", stages)
        self.assertNotIn("psf_grid_ms", stages)


if __name__ == "__main__":
    unittest.main()
