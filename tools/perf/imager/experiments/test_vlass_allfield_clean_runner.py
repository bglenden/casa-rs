#!/usr/bin/env python3
"""Contract tests for the reduced all-field VLASS clean runner."""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
RUNNER = (
    REPO_ROOT
    / "tools"
    / "perf"
    / "imager"
    / "experiments"
    / "run_vlass_clean_4096_all_fields_four_spw.sh"
)


class VlassAllFieldCleanRunnerTests(unittest.TestCase):
    def run_preflight(
        self,
        experiment_root: pathlib.Path,
        windowed_hybrid: str | None,
        overrides: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "CASA_RS_VLASS_EXPERIMENT_ROOT": str(experiment_root),
                "CASA_RS_VLASS_EXPERIMENT_BINARY": str(
                    experiment_root / "missing-casars-imager"
                ),
                "CASA_RS_VLASS_SELECTED_EXACT_HYBRID": "1",
                "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION": "cpu",
                "CASA_RS_VLASS_INITIAL_DIRTY_BACKEND": "cpu",
                "CASA_RS_VLASS_RESIDUAL_BACKEND": "cpu",
            }
        )
        if windowed_hybrid is not None:
            environment["CASA_RS_VLASS_WINDOWED_HYBRID_CLEAN"] = windowed_hybrid
        if overrides is not None:
            environment.update(overrides)
        return subprocess.run(
            [str(RUNNER)],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            check=False,
            text=True,
        )

    def test_explicit_windowed_hybrid_rejects_cpu_residual_backend(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(pathlib.Path(directory), "1")

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "WINDOWED_HYBRID_CLEAN=1 requires a Metal residual backend",
            result.stderr,
        )

    def test_auto_windowed_hybrid_allows_cpu_preflight_to_reach_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(pathlib.Path(directory), None)

        self.assertEqual(result.returncode, 2)
        self.assertIn("required matched-row input does not exist", result.stderr)
        self.assertNotIn("requires a Metal residual backend", result.stderr)

    def test_compact_global_metal_replay_rejects_cpu_residual_backend(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                "0",
                {
                    "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY": "1",
                    "CASA_RS_VLASS_REPLAY_RETENTION_BYTES": "8589934592",
                },
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "COMPACT_GLOBAL_METAL_REPLAY=1 requires a Metal residual backend",
            result.stderr,
        )

    def test_compact_global_metal_replay_reaches_metal_input_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                "0",
                {
                    "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY": "1",
                    "CASA_RS_VLASS_REPLAY_RETENTION_BYTES": "8589934592",
                    "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION": "metal",
                    "CASA_RS_VLASS_RESIDUAL_BACKEND": "metal",
                },
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("required matched-row input does not exist", result.stderr)
        self.assertNotIn("requires a Metal residual backend", result.stderr)

    def test_logical_tap_budget_rejects_nonpositive_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                None,
                {"CASA_RS_VLASS_LOGICAL_TAP_BUDGET_MIB": "0"},
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "CASA_RS_VLASS_LOGICAL_TAP_BUDGET_MIB must be a positive integer",
            result.stderr,
        )

    def test_logical_tap_budget_reaches_input_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                None,
                {"CASA_RS_VLASS_LOGICAL_TAP_BUDGET_MIB": "512"},
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("required matched-row input does not exist", result.stderr)
        self.assertNotIn("LOGICAL_TAP_BUDGET_MIB must be", result.stderr)

    def test_grouped_segment_target_rejects_nonpositive_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                None,
                {"CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES": "0"},
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES must be a positive integer",
            result.stderr,
        )

    def test_grouped_segment_target_reaches_input_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                None,
                {"CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES": "536870912"},
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("required matched-row input does not exist", result.stderr)
        self.assertNotIn("GROUPED_SEGMENT_TARGET_BYTES must be", result.stderr)

    def test_removed_separable_global_phase_flag_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                "0",
                {"CASA_RS_VLASS_SEPARABLE_GLOBAL_PHASE": "1"},
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "SEPARABLE_GLOBAL_PHASE is no longer supported",
            result.stderr,
        )

    def test_removed_separable_global_phase_flag_cannot_override_production(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_preflight(
                pathlib.Path(directory),
                "0",
                {
                    "CASA_RS_VLASS_SEPARABLE_GLOBAL_PHASE": "0",
                    "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY": "1",
                    "CASA_RS_VLASS_REPLAY_RETENTION_BYTES": "8589934592",
                    "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION": "metal",
                    "CASA_RS_VLASS_RESIDUAL_BACKEND": "metal",
                },
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "separable Metal phase replay is the production default",
            result.stderr,
        )


if __name__ == "__main__":
    unittest.main()
