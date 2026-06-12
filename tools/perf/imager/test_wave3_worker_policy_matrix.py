#!/usr/bin/env python3
"""Tests for the Wave 3 #287 worker-policy benchmark matrix."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wave3_worker_policy_matrix


class Wave3WorkerPolicyMatrixTests(unittest.TestCase):
    def test_matrix_covers_required_mode_families_and_backends(self) -> None:
        rows = wave3_worker_policy_matrix.build_rows(
            3,
            1,
            Path("target/imperformance-wave3/worker-policy"),
            skip_casa=True,
        )

        families = {row["mode_family"] for row in rows}
        self.assertEqual(
            {
                "standard_mfs_hogbom",
                "standard_mfs_clark",
                "mfs_multiscale",
                "mtmfs",
                "wprojection_hogbom",
                "wprojection_clark",
                "mosaic_mfs_hogbom",
                "mosaic_mfs_clark",
                "mosaic_mfs_multiscale",
                "aw_widefield_hogbom",
                "aw_widefield_clark",
                "standard_cube_hogbom",
                "standard_cube_clark",
                "cubedata_hogbom",
                "cubedata_clark",
                "mosaic_cube_clark",
            },
            families,
        )
        for family in families:
            backends = {row["backend"] for row in rows if row["mode_family"] == family}
            self.assertEqual({"cpu", "multi-cpu", "auto", "metal"}, backends)
            blocks = {row["paired_block"] for row in rows if row["mode_family"] == family}
            self.assertEqual({1, 2, 3}, blocks)
        self.assertEqual(16 * 4 * 3, len(rows))

    def test_generated_commands_skip_casa_by_default_and_use_overrides(self) -> None:
        rows = wave3_worker_policy_matrix.build_rows(3, 1, Path("out"), skip_casa=True)

        cube = next(
            row
            for row in rows
            if row["scenario_id"] == "standard-cube-hogbom-one-channel"
            and row["backend"] == "cpu"
            and row["paired_block"] == 1
        )

        self.assertEqual(
            {"CASA_RS_BENCH_SKIP_CASA": "1", "CASA_RS_BENCH_PROFILE_REPEATS": "1"},
            cube["env"],
        )
        self.assertEqual(1, cube["repeats"])
        self.assertEqual("cube", cube["overrides"]["specmode"])
        self.assertEqual("64", cube["overrides"]["width"])
        self.assertIn("CASA_RS_BENCH_SKIP_CASA=1", cube["shell_command"])
        self.assertIn("CASA_RS_BENCH_PROFILE_REPEATS=1", cube["shell_command"])
        self.assertIn("--repeats 1", cube["shell_command"])
        self.assertIn("--set-imaging specmode=cube", cube["shell_command"])
        self.assertIn("block-01", cube["shell_command"])

    def test_grouped_backend_order_remains_available_for_manual_replays(self) -> None:
        rows = wave3_worker_policy_matrix.build_rows(
            3,
            1,
            Path("out"),
            skip_casa=True,
            paired_blocks=False,
        )

        self.assertEqual(16 * 4, len(rows))
        self.assertTrue(all("paired_block" not in row for row in rows))
        self.assertTrue(all(row["repeats"] == 3 for row in rows))

    def test_filters_allow_scenario_backend_subsets(self) -> None:
        rows = wave3_worker_policy_matrix.build_rows(
            2,
            1,
            Path("out"),
            skip_casa=True,
            skip_profile=True,
            scenario_filter={"standard-mfs-hogbom-heavy"},
            backend_filter={"cpu", "multi-cpu"},
        )

        self.assertEqual(4, len(rows))
        self.assertEqual({"standard-mfs-hogbom-heavy"}, {row["scenario_id"] for row in rows})
        self.assertEqual({"cpu", "multi-cpu"}, {row["backend"] for row in rows})
        self.assertEqual({1, 2}, {row["paired_block"] for row in rows})
        self.assertTrue(all(row["env"]["CASA_RS_BENCH_SKIP_PROFILE"] == "1" for row in rows))
        self.assertTrue(all("CASA_RS_BENCH_SKIP_PROFILE=1" in row["shell_command"] for row in rows))


if __name__ == "__main__":
    unittest.main()
