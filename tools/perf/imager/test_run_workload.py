#!/usr/bin/env python3
"""Focused tests for the Wave 1 imager workload harness helpers."""

from __future__ import annotations

import unittest
from unittest import mock
from pathlib import Path
import tempfile
import sys

import numpy as np

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

    def test_parse_casa_timing_section_tolerates_casa_warning_noise(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA tclean timings (seconds):
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
2026-05-18 22:58:25 SEVERE ::casa

0%....10....100%
  run=1 real=56.589418
  median=56.589418
  kept_casa_prefix=/tmp/casa

Kept benchmark products:
  product_root=/tmp/products
"""
        )

        self.assertEqual("ran", parsed["casa"]["status"])
        self.assertEqual([56.589418], parsed["casa"]["timings_seconds"]["runs"])
        self.assertEqual(56.589418, parsed["casa"]["timings_seconds"]["median"])

    def test_parse_casa_stage_section_tolerates_warning_noise(self) -> None:
        parsed = run_workload.parse_benchmark_log(
            """CASA PySynthesisImager stage medians (milliseconds):
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
2026-05-19 01:55:09 SEVERE ::casa

0%....10....100%
  run=1 total_ms=530361.545 param_setup_ms=0.277 construct_imager_ms=0.007 make_psf_ms=220368.238 calcres_major_ms=305834.961 restore_ms=38.185
  stage medians (ms):
    parameter_setup=0.277
    construct_imager=0.007
    make_psf=220368.238
    calcres_major_cycle=305834.961
    restore_images=38.185
    total=530361.545
  result medians: clean_major_cycles=0 minor_cycles=0
"""
        )

        stages = parsed["stage_medians_ms"]["casa"]

        self.assertEqual(0.277, stages["parameter_setup"])
        self.assertEqual(220368.238, stages["make_psf"])
        self.assertEqual(305834.961, stages["calcres_major_cycle"])
        self.assertEqual(530361.545, stages["total"])

    def test_empty_results_include_reasons_for_both_sides(self) -> None:
        results = run_workload.empty_results(
            casa_status="blocked",
            reason="benchmark command exited 2",
        )

        self.assertEqual("not_run", results["rust"]["status"])
        self.assertEqual("benchmark command exited 2", results["rust"]["reason"])
        self.assertEqual("blocked", results["casa"]["status"])
        self.assertEqual("benchmark command exited 2", results["casa"]["reason"])

    def test_wterm_manifest_can_be_dry_run_for_wave3_matrix(self) -> None:
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

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("dry_run_only", plan["run_support"]["status"])
        self.assertIn("wterm='direct'", plan["run_support"]["reason"])

    def test_non_runnable_wterm_fails_only_for_real_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            dataset = Path(tempdir) / "input.ms"
            dataset.mkdir()
            manifest = {
                "id": "direct-wterm-smoke",
                "mode_id": "direct-wterm-mfs-dirty",
                "dataset": {
                    "key": "input.ms",
                    "path": str(dataset),
                },
                "imaging": {
                    "mode": "dirty",
                    "specmode": "mfs",
                    "gridder": "standard",
                    "wterm": "direct",
                },
            }

            with mock.patch.dict(
                "os.environ",
                {"CASA_RS_CASA_PYTHON": sys.executable},
                clear=True,
            ):
                with self.assertRaisesRegex(
                    run_workload.HarnessError,
                    "wterm='direct'",
                ):
                    run_workload.build_plan(
                        manifest_path=Path("manifest.json"),
                        manifest=manifest,
                        repeats_override=1,
                        run_label_override=None,
                        storage_label_override=None,
                        dry_run=False,
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
            "get_ms_values_into_processing_buffer": 2.5,
            "prepare_processing_buffer": 3.5,
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
        self.assertEqual(categories["standard_mfs_buffer_load"]["total_ms"], 2.5)
        self.assertEqual(categories["standard_mfs_buffer_prepare"]["total_ms"], 3.5)
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
                "hogbom_iteration_mode": "casa",
                "nterms": 2,
                "pblimit": 0.17,
                "write_pb": True,
                "pbcor": True,
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
        self.assertEqual("casa", plan["command"]["env"]["IMAGER_BENCH_HOGBOM_ITERATION_MODE"])
        self.assertEqual("0.17", plan["command"]["env"]["IMAGER_BENCH_PBLIMIT"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_WRITE_PB"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PBCOR"])
        self.assertEqual("casa", plan["mode"]["hogbom_iteration_mode"])
        self.assertEqual(2, plan["mode"]["nterms"])

    def test_ms_staging_defaults_to_direct(self) -> None:
        manifest = {
            "id": "medium-direct",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("direct", plan["run"]["ms_staging"])
        self.assertEqual("direct", plan["command"]["env"]["IMAGER_BENCH_MS_STAGING"])
        self.assertEqual("0", plan["run"]["phase_probe"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_PHASE_PROBE"])
        self.assertEqual("0", plan["command"]["env"]["IMAGER_BENCH_SKIP_PROFILE"])
        self.assertEqual("runnable", plan["run_support"]["status"])
        self.assertEqual("pending", run_workload.human_review_gate(plan, None)["status"])
        self.assertEqual("Brian", plan["review"]["required_reviewer"])

    def test_turnaround_workload_can_skip_profile_rerun(self) -> None:
        manifest = {
            "id": "large-turnaround",
            "mode_id": "mosaic-cube-one-channel",
            "dataset": {
                "key": "large.ms",
                "path": "/tmp/large.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "cube",
                "gridder": "mosaic",
            },
            "run": {
                "skip_profile": "1",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_SKIP_PROFILE"])

    def test_cubedata_is_accepted_as_dry_run_only_wave3_mode(self) -> None:
        manifest = {
            "id": "cubedata-one-plane",
            "mode_id": "cubedata-one-channel",
            "dataset": {
                "key": "input.ms",
                "path": "/tmp/input.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "cubedata",
                "gridder": "standard",
                "channel_count": 1,
            },
            "run": {
                "evidence_role": "after_gpu_metal",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("cubedata", plan["mode"]["specmode"])
        self.assertEqual("dry_run_only", plan["run_support"]["status"])
        self.assertIn("specmode='cubedata'", plan["run_support"]["reason"])
        self.assertEqual("after_gpu_metal", plan["review"]["evidence_role"])

    def test_phase_probe_is_opt_in_for_casa_stage_diagnostics(self) -> None:
        manifest = {
            "id": "stage-probe",
            "mode_id": "standard-mfs-clean-current",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "phase_probe": "1",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("1", plan["run"]["phase_probe"])
        self.assertEqual("1", plan["command"]["env"]["IMAGER_BENCH_PHASE_PROBE"])

    def test_invalid_phase_probe_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "bad-stage-probe",
            "mode_id": "standard-mfs-clean-current",
            "dataset": {
                "key": "medium.ms",
                "path": "/tmp/medium.ms",
            },
            "imaging": {
                "mode": "clean",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "phase_probe": "sometimes",
            },
        }

        with self.assertRaisesRegex(run_workload.HarnessError, "phase_probe"):
            run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

    def test_ms_staging_can_be_set_for_small_copy_runs(self) -> None:
        manifest = {
            "id": "small-copy",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "small.ms",
                "path": "/tmp/small.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "ms_staging": "copy",
            },
        }

        plan = run_workload.build_plan(
            manifest_path=Path("manifest.json"),
            manifest=manifest,
            repeats_override=1,
            run_label_override=None,
            storage_label_override=None,
            dry_run=True,
        )

        self.assertEqual("copy", plan["run"]["ms_staging"])
        self.assertEqual("copy", plan["command"]["env"]["IMAGER_BENCH_MS_STAGING"])

    def test_invalid_ms_staging_fails_before_running_benchmark(self) -> None:
        manifest = {
            "id": "bad-staging",
            "mode_id": "standard-mfs-dirty-control",
            "dataset": {
                "key": "input.ms",
                "path": "/tmp/input.ms",
            },
            "imaging": {
                "mode": "dirty",
                "specmode": "mfs",
                "gridder": "standard",
            },
            "run": {
                "ms_staging": "mirror",
            },
        }

        with self.assertRaisesRegex(run_workload.HarnessError, "ms_staging"):
            run_workload.build_plan(
                manifest_path=Path("manifest.json"),
                manifest=manifest,
                repeats_override=1,
                run_label_override=None,
                storage_label_override=None,
                dry_run=True,
            )

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

    def test_human_review_gate_reports_panel_readiness(self) -> None:
        plan = {
            "review": {
                "required_reviewer": "Brian",
                "required_evidence_roles": ["before_baseline", "after_gpu_metal"],
                "evidence_role": "after_gpu_metal",
            }
        }
        comparison = {
            "status": "completed",
            "structured_difference_review": {
                "label": "investigate",
                "summary": "overall investigate; investigate: .image",
                "legend": {
                    "good": "No review action expected from this check.",
                    "investigate": "Plausible but needs review in context.",
                    "bad": "Structured or large enough difference; do not close without explanation.",
                    "unknown": "Check could not be evaluated for this product.",
                },
            },
            "products": {
                ".image": {
                    "status": "compared",
                    "review_panel": {"status": "written", "path": "/tmp/image.png"},
                }
            },
        }

        gate = run_workload.human_review_gate(plan, comparison)

        self.assertEqual("pending", gate["status"])
        self.assertEqual("ready", gate["panel_status"])
        self.assertEqual("after_gpu_metal", gate["evidence_role"])
        self.assertEqual("investigate", gate["structured_difference_label"])
        self.assertIn("overall investigate", gate["structured_difference_summary"])
        self.assertIn("bad", gate["structured_difference_legend"])

    def test_product_review_panels_are_square_and_labeled(self) -> None:
        script = run_workload.PRODUCT_COMPARISON_SCRIPT

        self.assertIn('aspect="equal"', script)
        self.assertIn('axis.set_box_aspect(1)', script)
        self.assertIn('label=value_label', script)
        self.assertIn('label=f"casa-rs - CASA ({value_label})"', script)
        self.assertIn('f"casa-rs {product_label}"', script)
        self.assertIn('f"CASA {product_label}"', script)
        self.assertIn('f"difference {product_label}', script)
        self.assertIn('return "Jy/beam"', script)

    def test_product_comparison_stride_preserves_spatial_aspect(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(run_workload.PRODUCT_COMPARISON_SCRIPT, namespace)

        self.assertEqual(
            [2, 2, 1, 1],
            namespace["stride_for"]([1024, 1024, 1, 1], 1_000_000),
        )

    def test_product_comparison_reports_beam_normalized_structure_metrics(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(run_workload.PRODUCT_COMPARISON_SCRIPT, namespace)

        y, x = np.indices((64, 64))
        casa = np.ones((64, 64), dtype=np.float64)
        rust = casa + 0.01 + 0.005 * (x / 63.0)
        diff = rust - casa
        beam_info = {
            "status": "estimated_from_psf",
            "beam_block_side_pixels": 4,
        }

        metrics = namespace["structured_difference_metrics"](
            ".weight",
            rust,
            casa,
            diff,
            beam_info,
        )

        self.assertEqual("computed", metrics["status"])
        self.assertEqual(4, metrics["beam_block_side_pixels"])
        self.assertEqual("weight_union_support", metrics["mask"]["type"])
        self.assertGreater(metrics["low_order_r2_quadratic"], 0.95)
        self.assertGreater(metrics["large_scale_power_fraction"]["fraction"], 0.5)
        self.assertGreater(len(metrics["beam_block_rms_by_scale"]), 2)
        self.assertIsNotNone(metrics["block_rms_decay_slope_vs_independent_beams"])
        self.assertIn("normalized_block_mean_rms", metrics["beam_block_rms_by_scale"][0])
        self.assertEqual("computed", metrics["scale_offset_gradient_fit"]["status"])
        self.assertIn("dx_pixels", metrics["scale_offset_gradient_fit"]["coefficients"])
        self.assertEqual("bad", metrics["classification"]["overall"])
        self.assertEqual("bad", metrics["classification"]["amplitude"])
        self.assertEqual("bad", metrics["classification"]["structure"])
        self.assertEqual(
            "bad",
            metrics["classification"]["structure_components"]["low_order_r2_quadratic"],
        )
        self.assertIn("thresholds", metrics["classification"])
        self.assertEqual("bad", metrics["review"]["label"])
        self.assertIn("correctness blocker", metrics["review"]["summary"])
        self.assertEqual(
            "bad",
            next(
                check
                for check in metrics["review"]["checks"]
                if check["name"] == "large_scale_power_fraction"
            )["label"],
        )
        self.assertIn("good", metrics["review"]["legend"])

    def test_product_comparison_rolls_up_structured_review_labels(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(run_workload.PRODUCT_COMPARISON_SCRIPT, namespace)

        rollup = namespace["summarize_product_reviews"](
            {
                ".image": {
                    "structured_difference": {
                        "review": {
                            "label": "good",
                            "summary": ".image: good",
                            "checks": [
                                {"name": "normalized_diff_rms", "label": "good"}
                            ],
                        }
                    }
                },
                ".weight": {
                    "structured_difference": {
                        "review": {
                            "label": "investigate",
                            "summary": ".weight: investigate",
                            "checks": [
                                {
                                    "name": "large_scale_power_fraction",
                                    "label": "bad",
                                }
                            ],
                        }
                    }
                },
            }
        )

        self.assertEqual("investigate", rollup["label"])
        self.assertEqual("good", rollup["products"][".image"])
        self.assertEqual("investigate", rollup["products"][".weight"])
        self.assertIn("overall investigate", rollup["summary"])
        self.assertIn("normalized_diff_rms", rollup["thresholds"])
        self.assertEqual(
            "bad", rollup["checks_by_product"]["large_scale_power_fraction"][".weight"]
        )
        self.assertIn("bad", rollup["legend"])

    def test_review_panel_records_structured_difference_label(self) -> None:
        namespace: dict[str, object] = {"__name__": "product_comparison_test"}
        with mock.patch.dict("sys.modules", {"casatools": mock.MagicMock()}):
            exec(run_workload.PRODUCT_COMPARISON_SCRIPT, namespace)

        class FakeAxis:
            def imshow(self, *args, **kwargs):
                return object()

            def set_title(self, *args, **kwargs):
                return None

            def set_aspect(self, *args, **kwargs):
                return None

            def set_box_aspect(self, *args, **kwargs):
                return None

            def set_xticks(self, *args, **kwargs):
                return None

            def set_yticks(self, *args, **kwargs):
                return None

        class FakeFigure:
            def suptitle(self, *args, **kwargs):
                return None

            def colorbar(self, *args, **kwargs):
                return None

            def savefig(self, *args, **kwargs):
                return None

        class FakePlot:
            def subplots(self, *args, **kwargs):
                return FakeFigure(), [FakeAxis(), FakeAxis(), FakeAxis()]

            def close(self, *args, **kwargs):
                return None

        namespace["plt"] = FakePlot()

        rust = np.ones((8, 8), dtype=np.float64)
        casa = np.ones((8, 8), dtype=np.float64) * 0.99
        diff = rust - casa
        review = {
            "label": "investigate",
            "summary": ".weight: investigate; amplitude is good and structure is bad.",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            panel = namespace["write_review_panel"](
                temp_dir,
                ".weight",
                rust,
                casa,
                diff,
                review=review,
            )

        self.assertEqual("written", panel["status"])
        self.assertEqual("investigate", panel["structured_difference_label"])
        self.assertIn(".weight: investigate", panel["structured_difference_summary"])

    def test_parse_rust_stage_section_keeps_full_core_timing_set(self) -> None:
        log = """Rust stage medians (milliseconds):
  run=1 frontend_total_ms=100.000 open_ms=1.000 prepare_ms=2.000 phase_center_ms=3.000 imaging_ms=4.000 coords_ms=5.000 write_ms=6.000 core_total_ms=40.000 controller_ms=7.000 weighting_ms=8.000 major_refresh_ms=9.000 psf_grid_ms=10.000 psf_fft_ms=11.000 psf_normalize_ms=12.000 model_fft_ms=13.000 residual_grid_ms=14.000 residual_fft_ms=15.000 residual_normalize_ms=16.000 minor_ms=17.000 minor_solve_ms=18.000 beam_fit_ms=19.000 restore_ms=20.000
  frontend:
  open_measurement_set=1.000
  prepare_plane_input=2.000
  get_ms_values_into_processing_buffer=2.500
  prepare_processing_buffer=3.500
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
  residual_refresh_overhead=9.500
  clean_cycle_setup=9.250
  deconvolver_setup=9.750
  multiscale_scale_refresh=9.125
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
            "residual_refresh_overhead",
            "clean_cycle_setup",
            "deconvolver_setup",
            "multiscale_scale_refresh",
            "minor_cycle",
            "minor_cycle_solve",
            "beam_fit",
            "restore",
        ]:
            self.assertIn(name, stages)
        self.assertEqual(stages["psf_normalize"], 12.0)
        self.assertEqual(stages["model_fft"], 13.0)
        self.assertEqual(stages["residual_refresh_overhead"], 9.5)
        self.assertEqual(stages["clean_cycle_setup"], 9.25)
        self.assertEqual(stages["deconvolver_setup"], 9.75)
        self.assertEqual(stages["multiscale_scale_refresh"], 9.125)
        self.assertEqual(stages["get_ms_values_into_processing_buffer"], 2.5)
        self.assertEqual(stages["prepare_processing_buffer"], 3.5)
        self.assertEqual(stages["restore"], 20.0)
        self.assertNotIn("niter", stages)
        self.assertNotIn("imsize", stages)
        self.assertNotIn("psf_grid_ms", stages)


if __name__ == "__main__":
    unittest.main()
