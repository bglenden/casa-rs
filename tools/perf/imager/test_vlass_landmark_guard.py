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
        "production_baseline_casa_rs_wall_seconds": 38.5,
        "maximum_regression_fraction_without_user_approval": 0.1,
        "required_runtime": {
            "fftw_threads": 8,
            "fftw_f64_planner_flags": "estimate",
            "fftw_f64_core_sha256": "core-hash",
            "fftw_f64_threads_sha256": "threads-hash",
            "fftw_version": "fftw-3.3.11",
            "image_response_storage": "raw",
            "image_response_bytes": 536870912,
            "grouped_resident_replay_required": True,
            "grouped_omitted_squared_l2_energy": 0.0,
            "grouped_tile_side": 16,
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
            "planner_flags=estimate core_sha256=core-hash "
            "threads_sha256=threads-hash version=fftw-3.3.11",
            "mtmfs_multiscale_rhs_experiment storage=sparse-positions",
            "robust_rms_order_statistic algorithm=exact-radix-histogram",
            "mosaic_mtmfs_minor_cycle cycle=0 actual_updates=2000",
            "awproject_image_response_calibrated position=(1, 2) "
            "response_bytes=536870912",
            "awproject_image_response_synthesize position=(1, 2)",
            "awproject_image_response_final_refresh algorithm=exact-production",
            "awproject_grouped_replay_plan architecture=source-order-grouped-tile-v1 "
            "omitted_squared_l2_energy=0.000000000e0 tile_side=16",
            "awproject_aot_grouped_tile_receipt segment=0 "
            "omitted_energy_fraction_bits=0 "
            "grouped_plans_hash_prefix=abc "
            "legacy_grouped_plans_hash_prefix=abc "
            "grouped_route_hash_prefix=def "
            "legacy_grouped_route_hash_prefix=def",
            "awproject_effective_support segment=0 omitted_energy_fraction=0 "
            "prediction_plans=2 tile_plans=2 "
            "prediction_cropped_plans=0 tile_cropped_plans=0 "
            "prediction_original_tap_visits=10 prediction_retained_tap_visits=10 "
            "tile_original_tap_visits=20 tile_retained_tap_visits=20 "
            "max_omitted_energy_fraction=0 "
            "resident_kernel_bytes_before=100 resident_kernel_bytes_after=100",
            "awproject_metal_grouped_replay_retention "
            "decision=resident-complete segments=1 program_bytes=1024",
            "awproject_compact_replay_cache rejected_blocks=0 "
            "resident_global_segments=1 resident_global_program_bytes=1024 "
            "segmented_global_ready=true",
            "awproject_metal_resident_grouped_replay_summary segments=1 "
            "program_bytes=1024 spill_read_bytes=0 runtime_grouping_builds=0 "
            "runtime_sort_builds=0 runtime_route_builds=0",
            "mosaic_mtmfs_stream_replay invocation=0 pass=ResidualRefresh",
            "awproject_compact_replay_release stage=residual-grid-end "
            "resident_bytes=1024 resident_global_segments=1 next_use=none",
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
        self.assertIn(
            "source-order grouped-tile planner receipt is missing or duplicated",
            errors,
        )
        self.assertIn("AOT grouped-tile compiler receipts are missing", errors)
        self.assertIn(
            "complete grouped replay working set was not retained exactly once",
            errors,
        )
        self.assertIn(
            "resident grouped replay did not cover every residual stream replay",
            errors,
        )
        self.assertIn(
            "grouped replay lifetime was not released at residual-grid end",
            errors,
        )
        self.assertTrue(any("no-signoff ceiling" in error for error in errors))

    def test_changed_fftw_dyadic_storage_and_grouped_topology_fail(self) -> None:
        runtime = parse_log(
            valid_log()
            .replace("core_sha256=core-hash", "core_sha256=other")
            .replace(
                "omitted_squared_l2_energy=0.000000000e0",
                "omitted_squared_l2_energy=1.000000000e-4",
            )
            .replace(
                "omitted_energy_fraction_bits=0",
                "omitted_energy_fraction_bits=4547007122018943789",
            )
            .replace("prediction_cropped_plans=0", "prediction_cropped_plans=1")
            .replace(
                "legacy_grouped_route_hash_prefix=def",
                "legacy_grouped_route_hash_prefix=bad",
            )
            .replace(
                "awproject_metal_grouped_replay_retention",
                "awproject_aot_grouped_tile_receipt segment=0 "
                "grouped_plans_hash_prefix=abc "
                "legacy_grouped_plans_hash_prefix=abc "
                "grouped_route_hash_prefix=def "
                "legacy_grouped_route_hash_prefix=bad\n"
                "awproject_metal_grouped_replay_retention",
            )
            .replace("spill_read_bytes=0", "spill_read_bytes=1")
            .replace(
                "Wrote CASA-compatible products",
                "awproject_metal_segmented_global_replay_summary segments=1\n"
                "Wrote CASA-compatible products",
            )
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
            "f64 FFTW threads, planner, library hashes, or version differ from the landmark contract",
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
        self.assertIn(
            "AOT grouped-tile topology differs from the incumbent construction",
            errors,
        )
        self.assertIn(
            "grouped replay support policy differs from the production contract",
            errors,
        )
        self.assertIn(
            "AOT grouped-tile support threshold differs from the production contract",
            errors,
        )
        self.assertIn(
            "exact-support receipts are not unique and contiguous with AOT segments",
            errors,
        )
        self.assertIn(
            "AOT grouped-tile segment receipts are not unique and contiguous",
            errors,
        )
        self.assertIn(
            "resident grouped replay performed spill I/O or rebuilt runtime topology",
            errors,
        )
        self.assertIn(
            "grouped replay fell back to per-refresh spill loading",
            errors,
        )

    def test_grouped_support_cropping_is_rejected(self) -> None:
        runtime = parse_log(
            valid_log().replace(
                "prediction_cropped_plans=0", "prediction_cropped_plans=1"
            )
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
        self.assertIn("grouped replay did not preserve exact CF support", errors)

    def test_unknown_landmark_and_duplicate_completion_fail(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown or duplicate"):
            find_landmark(
                {"performance_preservation": {"landmark_rows": []}},
                LANDMARK_ID,
            )
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_log(valid_log() + "\n" + valid_log().splitlines()[-1])

    def test_exact_iteration_contract_is_not_a_minimum(self) -> None:
        runtime = parse_log(
            valid_log().replace("2000 minor iterations", "1999 minor iterations")
        )
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

    def test_approved_production_baseline_sets_no_signoff_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "release/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"release")
            accepted = evaluate(
                landmark(),
                parse_log(valid_log()),
                binary=binary,
                wall_seconds=42.35,
            )
            rejected = evaluate(
                landmark(),
                parse_log(valid_log()),
                binary=binary,
                wall_seconds=42.350001,
            )
        self.assertEqual([], accepted)
        self.assertIn(
            "wall time 42.350001s exceeds the no-signoff ceiling 42.350000s",
            rejected,
        )


if __name__ == "__main__":
    unittest.main()
