#!/usr/bin/env python3
"""Focused tests for the Wave 4 acceleration matrix closeout gate."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wave4_acceleration_matrix


class Wave4AccelerationMatrixTests(unittest.TestCase):
    def test_repository_matrix_covers_required_rows(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        rows = wave4_acceleration_matrix.enumerate_rows(matrix)

        self.assertEqual(wave4_acceleration_matrix.REQUIRED_ROW_IDS, {row["row_id"] for row in rows})
        self.assertEqual(12, len(rows))
        clean_rows = [row for row in rows if row["phase"] == "clean"]
        self.assertTrue(
            all("metal_vs_multi_worker_cpu" in row["required_speedups"] for row in clean_rows)
        )
        mosaic_dirty = next(row for row in rows if row["row_id"] == "mosaic_cube_dirty")
        self.assertIn("auto_vs_single_plane_stream", mosaic_dirty["required_speedups"])

    def test_matrix_contract_requires_brian_review_and_speedup_columns(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )

        review = matrix["review_contract"]
        self.assertEqual("Brian", review["required_reviewer"])
        self.assertEqual(314, review["review_required_before_pr_ready"])
        self.assertIn("blocked", review["blocking_statuses"])
        self.assertIn("speedup_default_vs_casa", matrix["closeout_table_columns"])
        self.assertGreaterEqual(
            matrix["performance_targets"]["multi_worker_speedup_vs_serial"], 2.0
        )

    def test_closeout_table_computes_speedups_and_target_status(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        serial = fake_result(
            "standard_cube_clean_hogbom",
            "serial_cpu",
            rust_seconds=120.0,
            casa_seconds=1000.0,
            backend="serial_cpu",
            workers=1,
        )
        multi = fake_result(
            "standard_cube_clean_hogbom",
            "multi_worker_cpu",
            rust_seconds=40.0,
            casa_seconds=1000.0,
            backend="wave3_fixed_tile_cpu",
            workers=8,
        )
        metal = fake_result(
            "standard_cube_clean_hogbom",
            "metal_default",
            rust_seconds=20.0,
            casa_seconds=1000.0,
            backend="wave3_metal_grouped",
            workers=8,
        )

        rows = wave4_acceleration_matrix.build_closeout_table(matrix, [serial, multi, metal])
        row = next(row for row in rows if row["row_id"] == "standard_cube_clean_hogbom")

        self.assertEqual(3.0, row["speedup_auto_vs_serial"])
        self.assertEqual(2.0, row["speedup_metal_vs_multi_worker_cpu"])
        self.assertEqual(50.0, row["speedup_default_vs_casa"])
        self.assertEqual("selected", row["metal_status"])
        self.assertEqual("met", row["target_status"])

    def test_missing_required_evidence_keeps_row_blocked(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )

        rows = wave4_acceleration_matrix.build_closeout_table(matrix, [])

        self.assertTrue(all(row["target_status"] == "blocked" for row in rows))

    def test_casa_speedup_requires_comparable_tier_and_shape(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        large = fake_result(
            "standard_cube_dirty",
            "multi_worker_cpu",
            rust_seconds=500.0,
            casa_seconds=None,
            backend="cpu_slab",
            workers=8,
        )
        large["dataset"]["key"] = "wave1-alma-large"
        large["mode"]["channel_count"] = 1024
        large["mode"]["image_shape"] = [4096, 4096]
        medium_casa = fake_result(
            "standard_cube_dirty",
            "casa_cpp",
            rust_seconds=160.0,
            casa_seconds=1900.0,
            backend="cpu_slab",
            workers=8,
        )

        rows = wave4_acceleration_matrix.build_closeout_table(
            matrix, [large, medium_casa]
        )
        row = next(row for row in rows if row["row_id"] == "standard_cube_dirty")

        self.assertIsNone(row["casa_s"])
        self.assertIsNone(row["speedup_default_vs_casa"])

    def test_worker_and_metal_speedups_require_comparable_tier_and_shape(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        serial = fake_result(
            "cubedata_clean_hogbom",
            "serial_cpu",
            rust_seconds=300.0,
            casa_seconds=None,
            backend="serial_cpu",
            workers=1,
        )
        multi = fake_result(
            "cubedata_clean_hogbom",
            "multi_worker_cpu",
            rust_seconds=100.0,
            casa_seconds=None,
            backend="wave3_fixed_tile_cpu",
            workers=8,
        )
        metal = fake_result(
            "cubedata_clean_hogbom",
            "metal_default",
            rust_seconds=80.0,
            casa_seconds=None,
            backend="wave3_metal_grouped",
            workers=8,
        )
        metal["mode"]["channel_count"] = 512

        rows = wave4_acceleration_matrix.build_closeout_table(
            matrix, [serial, multi, metal]
        )
        row = next(row for row in rows if row["row_id"] == "cubedata_clean_hogbom")

        self.assertEqual(3.0, row["speedup_auto_vs_serial"])
        self.assertIsNone(row["speedup_metal_vs_multi_worker_cpu"])
        self.assertEqual("512 ch, 1024", row["shape"])

    def test_speedups_require_comparable_iteration_count(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        shallow_casa = fake_result(
            "standard_cube_clean_clark",
            "casa_cpp",
            rust_seconds=30.0,
            casa_seconds=300.0,
            backend="wave3_metal_grouped",
            workers=8,
        )
        shallow_casa["mode"]["niter"] = 2
        deep_default = fake_result(
            "standard_cube_clean_clark",
            "metal_default",
            rust_seconds=45.0,
            casa_seconds=None,
            backend="wave3_metal_grouped",
            workers=8,
        )
        deep_default["mode"]["niter"] = 10000

        rows = wave4_acceleration_matrix.build_closeout_table(
            matrix, [shallow_casa, deep_default]
        )
        row = next(row for row in rows if row["row_id"] == "standard_cube_clean_clark")

        self.assertIsNone(row["casa_s"])
        self.assertIsNone(row["speedup_default_vs_casa"])

    def test_cli_writes_markdown_table_from_result_json(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        result = fake_result(
            "mosaic_cube_dirty",
            "single_plane_stream_baseline",
            rust_seconds=90.0,
            casa_seconds=None,
            backend="mosaic_single_plane_stream",
            workers=1,
        )
        auto = fake_result(
            "mosaic_cube_dirty",
            "metal_default",
            rust_seconds=45.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_slab",
            workers=8,
        )
        rows = wave4_acceleration_matrix.build_closeout_table(matrix, [result, auto])
        row = next(row for row in rows if row["row_id"] == "mosaic_cube_dirty")

        self.assertEqual(2.0, row["speedup_default_vs_large_or_single_plane_baseline"])
        self.assertEqual("mosaic_multi_plane_slab", row["selected_backend"])
        self.assertIn("| mode_family | phase |", wave4_acceleration_matrix.render_markdown_table(rows))

    def test_mosaic_roles_can_be_inferred_from_worker_count(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        baseline = fake_result(
            "mosaic_cube_dirty",
            "unspecified",
            rust_seconds=120.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_stream",
            workers=1,
        )
        auto = fake_result(
            "mosaic_cube_dirty",
            "unspecified",
            rust_seconds=60.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_stream",
            workers=4,
        )
        baseline.pop("review")
        baseline["run"].pop("evidence_role")
        auto.pop("review")
        auto["run"].pop("evidence_role")

        rows = wave4_acceleration_matrix.build_closeout_table(matrix, [baseline, auto])
        row = next(row for row in rows if row["row_id"] == "mosaic_cube_dirty")

        self.assertEqual(2.0, row["speedup_default_vs_large_or_single_plane_baseline"])
        self.assertEqual(4, row["worker_count"])

    def test_correctness_can_come_from_separate_evidence(self) -> None:
        matrix = wave4_acceleration_matrix.load_matrix(
            wave4_acceleration_matrix.MATRIX_PATH
        )
        baseline = fake_result(
            "mosaic_cube_dirty",
            "single_plane_stream_baseline",
            rust_seconds=90.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_stream",
            workers=1,
        )
        auto = fake_result(
            "mosaic_cube_dirty",
            "multi_worker_cpu",
            rust_seconds=45.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_stream",
            workers=4,
        )
        auto["results"]["product_comparison"] = {"status": "missing"}
        correctness = fake_result(
            "mosaic_cube_dirty",
            "casa_cpp",
            rust_seconds=5.0,
            casa_seconds=3.0,
            backend="mosaic_multi_plane_stream",
            workers=4,
        )

        rows = wave4_acceleration_matrix.build_closeout_table(
            matrix, [baseline, auto, correctness]
        )
        row = next(row for row in rows if row["row_id"] == "mosaic_cube_dirty")

        self.assertEqual("good", row["correctness_status"])
        self.assertEqual("met", row["target_status"])
        self.assertIsNotNone(row["performance_evidence_link"])
        self.assertIsNotNone(row["correctness_evidence_link"])

    def test_correctness_selection_prefers_investigate_over_missing_panels(self) -> None:
        missing = fake_result(
            "standard_cube_dirty",
            "large_baseline",
            rust_seconds=900.0,
            casa_seconds=None,
            backend="cpu_slab",
            workers=8,
        )
        missing["results"]["product_comparison"] = {
            "structured_difference_review": {"label": "missing"}
        }
        investigate = fake_result(
            "standard_cube_dirty",
            "casa_cpp",
            rust_seconds=180.0,
            casa_seconds=1900.0,
            backend="cpu_slab",
            workers=8,
        )
        investigate["results"]["product_comparison"] = {
            "structured_difference_review": {"label": "investigate"}
        }

        best = wave4_acceleration_matrix.best_correctness_result(
            {"large_baseline": missing, "casa_cpp": investigate}
        )

        self.assertIs(best, investigate)

    def test_sumwt_only_stale_structure_flag_is_not_a_correctness_blocker(self) -> None:
        result = fake_result(
            "standard_cube_dirty",
            "casa_cpp",
            rust_seconds=180.0,
            casa_seconds=1900.0,
            backend="cpu_slab",
            workers=8,
        )
        result["results"]["product_comparison"] = {
            "status": "completed",
            "structured_difference_review": {
                "label": "investigate",
                "products": {
                    ".image": "good",
                    ".residual": "good",
                    ".psf": "good",
                    ".sumwt": "investigate",
                },
            },
            "products": {
                ".sumwt": {
                    "structured_difference": {
                        "classification": {
                            "amplitude": "good",
                            "structure": "investigate",
                            "overall": "investigate",
                        },
                        "normalized_diff_rms": 1.0e-8,
                    }
                }
            },
        }

        self.assertEqual("good", wave4_acceleration_matrix.correctness_status(result))

    def test_sumwt_amplitude_difference_remains_a_correctness_blocker(self) -> None:
        result = fake_result(
            "standard_cube_dirty",
            "casa_cpp",
            rust_seconds=180.0,
            casa_seconds=1900.0,
            backend="cpu_slab",
            workers=8,
        )
        result["results"]["product_comparison"] = {
            "status": "completed",
            "structured_difference_review": {
                "label": "investigate",
                "products": {".sumwt": "investigate"},
            },
            "products": {
                ".sumwt": {
                    "structured_difference": {
                        "classification": {
                            "amplitude": "investigate",
                            "structure": "investigate",
                            "overall": "investigate",
                        },
                        "normalized_diff_rms": 2.0e-4,
                    }
                }
            },
        }

        self.assertEqual(
            "investigate", wave4_acceleration_matrix.correctness_status(result)
        )

    def test_mosaic_resolved_metal_backends_infer_metal_default(self) -> None:
        result = fake_result(
            "mosaic_cube_clean_clark",
            "unspecified",
            rust_seconds=12.0,
            casa_seconds=None,
            backend="mosaic_multi_plane_stream",
            workers=8,
        )
        result.pop("review")
        result["run"].pop("evidence_role")
        result["benchmark_features"]["backend"]["resolved_initial_dirty_backend"] = (
            "metal-row-run-grouped"
        )
        result["benchmark_features"]["backend"]["resolved_residual_backend"] = (
            "metal-row-run-grouped"
        )

        self.assertEqual(
            "metal_default", wave4_acceleration_matrix.evidence_role(result)
        )
        self.assertEqual("selected", wave4_acceleration_matrix.metal_status(result))

    def test_load_result_preserves_source_path_for_evidence_link(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "result.json"
            path.write_text(
                json.dumps(fake_result("cubedata_dirty", "metal_default", 10.0, None))
                + "\n",
                encoding="utf-8",
            )

            result = wave4_acceleration_matrix.load_result(path)

        self.assertEqual(str(path), result["_source_path"])

    def test_evidence_list_can_override_row_and_role(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result_path = Path(tempdir) / "result.json"
            evidence_path = Path(tempdir) / "evidence.json"
            result = fake_result(
                "standard_cube_clean_hogbom",
                "unspecified",
                rust_seconds=90.0,
                casa_seconds=None,
                backend="wave3_fixed_tile_cpu",
                workers=8,
            )
            result["mode"].pop("wave4_matrix_row_id")
            result["review"]["evidence_role"] = "unspecified"
            result["run"]["evidence_role"] = "unspecified"
            result_path.write_text(json.dumps(result) + "\n", encoding="utf-8")
            evidence_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "results": [
                            {
                                "path": str(result_path),
                                "row_id": "cubedata_clean_clark",
                                "role": "multi_worker_cpu",
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            loaded = wave4_acceleration_matrix.load_evidence_list(evidence_path)

        self.assertEqual(1, len(loaded))
        self.assertEqual("cubedata_clean_clark", wave4_acceleration_matrix.explicit_row_id(loaded[0]))
        self.assertEqual("multi_worker_cpu", wave4_acceleration_matrix.evidence_role(loaded[0]))


def fake_result(
    row_id: str,
    role: str,
    rust_seconds: float,
    casa_seconds: float | None,
    *,
    backend: str = "wave3_fixed_tile_cpu",
    workers: int = 4,
) -> dict:
    mode = {
        "specmode": "cube",
        "gridder": "standard",
        "deconvolver": "hogbom",
        "niter": 100,
        "channel_count": 64,
        "image_shape": [1024, 1024],
        "wave4_matrix_row_id": row_id,
    }
    if row_id.startswith("cubedata"):
        mode["specmode"] = "cubedata"
    if row_id.startswith("mosaic_cube"):
        mode["gridder"] = "mosaic"
        backend_features = {
            "mosaic_cube_slab_executor_capabilities": backend,
            "mosaic_cube_slab_worker_count": workers,
        }
    else:
        backend_features = {
            "cube_per_plane_backend": backend,
            "cube_per_plane_workers": workers,
            "cube_per_plane_metal_eligible": "metal" in backend,
            "metal_device": True,
        }
    if row_id.endswith("_dirty"):
        mode["niter"] = 0
    if row_id.endswith("_clark"):
        mode["deconvolver"] = "clark"
    if row_id.endswith("_multiscale"):
        mode["deconvolver"] = "multiscale"
    return {
        "schema_version": 1,
        "status": "completed",
        "mode": mode,
        "dataset": {"key": "wave1-vla-single-medium"},
        "run": {"storage_label": "medium-row", "evidence_role": role},
        "review": {"evidence_role": role},
        "benchmark_features": {
            "backend": backend_features
        },
        "results": {
            "rust": {"timings_seconds": {"median": rust_seconds}},
            "casa": {"timings_seconds": {"median": casa_seconds}},
            "product_comparison": {
                "status": "completed",
                "structured_difference_review": {"label": "good"},
            },
        },
        "logs": {"benchmark_log": "/tmp/run.log"},
    }


if __name__ == "__main__":
    unittest.main()
