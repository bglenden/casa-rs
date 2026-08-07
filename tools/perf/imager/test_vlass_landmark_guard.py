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
            "fftw_f64_planner_flags": "estimate",
            "fftw_f64_core_sha256": "core-hash",
            "fftw_f64_threads_sha256": "threads-hash",
            "fftw_version": "fftw-3.3.11",
            "image_response_storage": "raw",
            "image_response_bytes": 536870912,
            "resident_tile_chain_required": True,
            "selected_field_count": 1,
            "resident_tile_chain_blocks": 4,
            "residual_f64_term_parallelism": 2,
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
            "awproject_replay_plan architecture=resident-tile-chain-v1 "
            "selected_fields=1 retention_bytes=4096",
            "awproject_metal_resident_tile_chain built=true "
            "gpu_residual_replay=false program_bytes=256",
            "awproject_metal_resident_tile_chain built=true "
            "gpu_residual_replay=false program_bytes=256",
            "awproject_metal_resident_tile_chain built=true "
            "gpu_residual_replay=false program_bytes=256",
            "awproject_metal_resident_tile_chain built=true "
            "gpu_residual_replay=false program_bytes=256",
            "awproject_metal_resident_tile_chain built=false "
            "gpu_residual_replay=true program_bytes=256",
            "awproject_metal_resident_tile_chain built=false "
            "gpu_residual_replay=true program_bytes=256",
            "awproject_metal_resident_tile_chain built=false "
            "gpu_residual_replay=true program_bytes=256",
            "awproject_metal_resident_tile_chain built=false "
            "gpu_residual_replay=true program_bytes=256",
            "awproject_compact_replay_cache rejected_blocks=0 resident_blocks=4 resident_programs=4 "
            "resident_global_segments=0 global_metal_program=false hits=4 misses=4",
            "awproject_residual_term_fft_plan term_parallelism=2 "
            "source=planner-residual-transform-headroom",
            "awproject_residual_term_fft_plan term_parallelism=2 "
            "source=planner-residual-transform-headroom",
            "mosaic_mtmfs_stream_replay invocation=0 pass=ResidualRefresh",
            "mosaic_mtmfs_stream_replay invocation=1 pass=ResidualRefresh",
            "awproject_compact_replay_release stage=residual-grid-end "
            "resident_bytes=1024 resident_blocks=4 resident_programs=4 resident_global_segments=0 "
            "next_use=none",
            "Wrote CASA-compatible products at prefix /tmp/rust "
            "(100 gridded samples, 2 major cycles, 2000 minor iterations, "
            "stop=Some(IterationLimitReached))",
        ]
    )


class VlassLandmarkGuardTests(unittest.TestCase):
    def evaluate_log(self, text: str, *, wall_seconds: float = 29.0) -> list[str]:
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "release/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"release")
            return evaluate(
                landmark(),
                parse_log(text),
                binary=binary,
                wall_seconds=wall_seconds,
            )

    def test_valid_clean_activity_passes(self) -> None:
        self.assertEqual([], self.evaluate_log(valid_log()))

    def test_frozen_proxy_and_missing_fast_paths_fail(self) -> None:
        text = "\n".join(
            [
                "awproject_frozen_model_refresh prefix=/tmp/frozen",
                "Wrote CASA-compatible products at prefix /tmp/rust "
                "(100 gridded samples, 1 major cycles, 2000 minor iterations, "
                "stop=Some(IterationLimitReached))",
            ]
        )
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "debug/casars-imager"
            binary.parent.mkdir()
            binary.write_bytes(b"debug")
            errors = evaluate(
                landmark(),
                parse_log(text),
                binary=binary,
                wall_seconds=100.0,
            )
        for expected in [
            "timed executable is not from a release directory",
            "frozen-model execution cannot satisfy a CLEAN landmark",
            "real minor-cycle records are missing",
            "image-response calibration was not exercised",
            "image-response synthesis was not exercised",
            "sparse MT-MFS RHS was not exercised",
            "exact radix statistics were not exercised",
            "single-field resident tile-chain planner receipt is missing or duplicated",
            "resident tile-chain cache did not retain the complete source-block working set",
            "resident tile-chain programs were not built once and reused for residual replay",
            "residual-term FFT was not admitted by planner transform headroom",
            "resident tile-chain lifetime was not released at residual-grid end",
        ]:
            self.assertIn(expected, errors)
        self.assertTrue(any("no-signoff ceiling" in error for error in errors))

    def test_changed_fftw_architecture_storage_and_fft_plan_fail(self) -> None:
        text = (
            valid_log()
            .replace("planner_flags=estimate", "planner_flags=wisdom-only")
            .replace(
                "architecture=resident-tile-chain-v1",
                "architecture=source-order-grouped-tile-v1",
            )
            .replace(
                "source=planner-residual-transform-headroom",
                "source=experiment",
            )
            .replace(
                "Wrote CASA-compatible products",
                "awproject_grouped_replay_plan "
                "architecture=source-order-grouped-tile-v1\n"
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
        errors = self.evaluate_log(text)
        for expected in [
            "f64 FFTW planner, threads, version, or library hashes differ from the landmark contract",
            "dyadic response storage cannot reproduce the raw landmark",
            "image-response byte count differs from the raw landmark",
            "single-field resident tile-chain planner receipt is missing or duplicated",
            "single-field resident landmark exercised grouped replay",
            "residual-term FFT was not admitted by planner transform headroom",
            "resident tile-chain landmark used grouped spill replay",
        ]:
            self.assertIn(expected, errors)

    def test_unreceipted_extra_resident_program_fails(self) -> None:
        first_reuse = (
            "awproject_metal_resident_tile_chain built=false "
            "gpu_residual_replay=true program_bytes=256"
        )
        text = valid_log().replace(
            first_reuse,
            "awproject_metal_resident_tile_chain built=true "
            "gpu_residual_replay=false program_bytes=256\n" + first_reuse,
            1,
        )
        text = text.replace(
            "awproject_compact_replay_cache ",
            first_reuse + "\nawproject_compact_replay_cache ",
            1,
        )
        self.assertIn(
            "resident tile-chain programs were not built once and reused for residual replay",
            self.evaluate_log(text),
        )

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


if __name__ == "__main__":
    unittest.main()
