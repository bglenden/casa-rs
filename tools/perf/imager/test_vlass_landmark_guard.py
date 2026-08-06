# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests for the fail-closed VLASS CLEAN landmark guard."""

from __future__ import annotations

import copy
import tempfile
from pathlib import Path
import unittest

from vlass_landmark_guard import evaluate, find_landmark, parse_log


LANDMARK_ID = "VLASS-LANDMARK-SINGLE-4096-4SPW-CLEAN-N2000-v1"


def landmark() -> dict:
    return {
        "id": LANDMARK_ID,
        "historical_casa_rs_wall_seconds": 28.65,
        "maximum_regression_fraction_without_user_approval": 0.1,
        "required_runtime": {
            "fftw_threads": 8,
            "fftw_f64_wisdom_sha256": "wisdom-hash",
            "image_response_storage": "raw",
            "image_response_bytes": 536870912,
            "global_metal_program_required": True,
        },
        "required_activity": {
            "frozen_model_allowed": False,
            "actual_minor_iterations": 2000,
            "minimum_major_cycles": 1,
            "minimum_minor_cycle_records": 1,
            "minimum_image_response_calibrations": 1,
            "minimum_image_response_syntheses": 1,
            "exact_final_refreshes": 1,
            "sparse_rhs_required": True,
            "radix_madfm_required": True,
        },
    }


def valid_log() -> str:
    return "\n".join(
        [
            "fftw_runtime_provenance precision=f64 fft_threads=8 "
            "planner_flags=wisdom-only wisdom_sha256=wisdom-hash",
            "mtmfs_multiscale_rhs_experiment storage=sparse-positions",
            "robust_rms_order_statistic algorithm=exact-radix-histogram",
            "mosaic_mtmfs_minor_cycle cycle=0 actual_updates=2000",
            "awproject_image_response_calibrated position=(1, 2) "
            "response_bytes=536870912",
            "awproject_image_response_synthesize position=(1, 2)",
            "awproject_image_response_final_refresh algorithm=exact-production",
            "awproject_compact_replay_cache global_metal_program=true",
            "Wrote CASA-compatible products at prefix /tmp/rust "
            "(100 gridded samples, 2 major cycles, 2000 minor iterations, "
            "stop=Some(IterationLimitReached))",
        ]
    )


class VlassLandmarkGuardTests(unittest.TestCase):
    def test_valid_clean_activity_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "release/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"release")
            errors = evaluate(
                landmark(),
                parse_log(valid_log()),
                binary=binary,
                wall_seconds=29.0,
            )
        self.assertEqual([], errors)

    def test_frozen_proxy_and_missing_fast_paths_fail(self) -> None:
        runtime = parse_log(
            "\n".join(
                [
                    "awproject_frozen_model_refresh prefix=/tmp/frozen",
                    "Wrote CASA-compatible products at prefix /tmp/rust "
                    "(100 gridded samples, 1 major cycles, 2000 minor iterations, "
                    "stop=Some(IterationLimitReached))",
                ]
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "debug/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"debug")
            errors = evaluate(
                landmark(),
                runtime,
                binary=binary,
                wall_seconds=100.0,
            )
        self.assertIn("timed executable is not from a release directory", errors)
        self.assertIn(
            "frozen-model execution cannot satisfy a CLEAN landmark",
            errors,
        )
        self.assertIn("real minor-cycle records are missing", errors)
        self.assertIn("image-response calibration was not exercised", errors)
        self.assertIn("image-response synthesis was not exercised", errors)
        self.assertIn("sparse MT-MFS RHS was not exercised", errors)
        self.assertIn("exact radix statistics were not exercised", errors)
        self.assertIn("global Metal replay program was not exercised", errors)
        self.assertTrue(any("no-signoff ceiling" in error for error in errors))

    def test_changed_wisdom_and_dyadic_storage_fail(self) -> None:
        runtime = parse_log(
            valid_log()
            .replace("wisdom_sha256=wisdom-hash", "wisdom_sha256=other")
            .replace("global_metal_program=true", "global_metal_program=false")
            .replace(
                "awproject_image_response_calibrated",
                "awproject_image_response_dyadic_encode response_bytes=1\n"
                "awproject_image_response_calibrated",
            )
            .replace("response_bytes=536870912", "response_bytes=339941888")
        )
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "release/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"release")
            errors = evaluate(
                landmark(),
                runtime,
                binary=binary,
                wall_seconds=29.0,
            )
        self.assertIn(
            "f64 FFTW threads or immutable wisdom differ from the landmark contract",
            errors,
        )
        self.assertIn(
            "dyadic response storage cannot reproduce the raw landmark",
            errors,
        )
        self.assertIn(
            "image-response byte count differs from the raw landmark",
            errors,
        )
        self.assertIn("global Metal replay program was not exercised", errors)

    def test_unknown_landmark_and_duplicate_completion_fail(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown or duplicate"):
            find_landmark(
                {"performance_preservation": {"landmark_rows": []}},
                LANDMARK_ID,
            )
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_log(valid_log() + "\n" + valid_log().splitlines()[-1])

    def test_exact_iteration_contract_is_not_a_minimum(self) -> None:
        runtime = parse_log(valid_log().replace("2000 minor iterations", "1999 minor iterations"))
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "release/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"release")
            errors = evaluate(
                copy.deepcopy(landmark()),
                runtime,
                binary=binary,
                wall_seconds=29.0,
            )
        self.assertIn(
            "minor-iteration count differs from the landmark contract",
            errors,
        )


if __name__ == "__main__":
    unittest.main()
