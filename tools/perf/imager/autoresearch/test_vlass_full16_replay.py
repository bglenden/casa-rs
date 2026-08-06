"""Contract tests for the frozen full-band VLASS replay benchmark."""

from __future__ import annotations

import copy
import json
import os
import unittest
from unittest import mock

from autoresearch.vlass_full16_replay import (
    FIELD_IDS,
    FOUR_SPW,
    FULL16,
    ContractError,
    EFFECTIVE_SUPPORT_ENV,
    benchmark_environment,
    parse_benchmark_result,
    result_errors,
    validate_manifest,
)


def replay_result(*, effective_support_requested: bool = True) -> dict:
    def variant(samples: int, segments: int, spws: list[int], seconds: float) -> dict:
        if effective_support_requested and segments >= 2:
            decision = "enabled"
            reason = None
            compiled_segments = segments
            telemetry_markers = segments
            total_compile_seconds = 0.01
        elif effective_support_requested:
            decision = "rejected"
            reason = "single_segment_no_prefetch_overlap"
            compiled_segments = 0
            telemetry_markers = 0
            total_compile_seconds = 0.0
        else:
            decision = "not_requested"
            reason = None
            compiled_segments = 0
            telemetry_markers = 0
            total_compile_seconds = 0.0
        return {
            "seconds": seconds,
            "samples": samples,
            "segments": segments,
            "rejected_samples": 0,
            "payload_bytes": 100,
            "reload_bytes": 100,
            "nrmse": [0.0, 1e-4],
            "provenance": {
                "field_ids": FIELD_IDS,
                "spw_ids": spws,
                "use_pointing": True,
            },
            "byte_ledger": {
                "payload_bytes": 100,
                "kernel_payload_bytes": 80,
                "unique_kernel_bytes": 20,
                "duplicated_kernel_bytes": 60,
                "segment_local_non_kernel_bytes": 20,
            },
            "effective_support_telemetry_markers": telemetry_markers,
            "effective_support": {
                "requested": effective_support_requested,
                "decision": decision,
                "reason": reason,
                "segment_count": segments,
                "compiled_segment_count": compiled_segments,
                "total_compile_seconds": total_compile_seconds,
                "initial_prepare_seconds": 0.01,
                "prefetch_wait_seconds": 0.001 if segments >= 2 else 0.0,
            },
            "segment_receipts": [{} for _ in range(segments)],
        }

    return {
        "schema": "casa-rs-vlass-full16-aw-replay-campaign-v1",
        "seconds": 250.0,
        "full16": variant(25_030_848, 10, list(range(2, 18)), 250.0),
        "four_spw": variant(6_416_526, 1, [2, 7, 12, 17], 8.0),
    }


class ReplayContractTests(unittest.TestCase):
    def test_benchmark_environment_only_forwards_explicit_effective_support(self) -> None:
        name = EFFECTIVE_SUPPORT_ENV
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(name, None)
            self.assertNotIn(name, benchmark_environment())
        with mock.patch.dict(os.environ, {name: "1e-6"}, clear=False):
            self.assertEqual(benchmark_environment()[name], "1e-6")

    def test_benchmark_environment_selects_captured_residual_semantics(self) -> None:
        environment = benchmark_environment()

        self.assertEqual(
            environment["CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_CLEAN"], "1"
        )
        self.assertEqual(
            environment["CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE"],
            "1",
        )
        self.assertEqual(
            environment["CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR"], "1"
        )

    def test_result_guard_accepts_complete_practical_contract(self) -> None:
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}
        self.assertEqual(
            result_errors(
                replay_result(),
                telemetry,
                four_spw_baseline_seconds=8.0,
                effective_support_requested=True,
            ),
            [],
        )

    def test_result_guard_accepts_dense_not_requested_contract(self) -> None:
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}
        self.assertEqual(
            result_errors(
                replay_result(effective_support_requested=False),
                telemetry,
                four_spw_baseline_seconds=8.0,
                effective_support_requested=False,
            ),
            [],
        )

    def test_result_guard_rejects_wrong_effective_support_admission(self) -> None:
        result = replay_result()
        result["full16"]["effective_support"]["decision"] = "rejected"
        result["four_spw"]["effective_support"]["reason"] = None
        result["four_spw"]["effective_support"]["compiled_segment_count"] = 1
        result["four_spw"]["effective_support_telemetry_markers"] = 1
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}

        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=8.0,
            effective_support_requested=True,
        )

        self.assertIn("full16 effective-support decision changed", errors)
        self.assertIn("four_spw effective-support reason changed", errors)
        self.assertIn(
            "four_spw effective-support compiled segment count changed", errors
        )
        self.assertIn(
            "four_spw effective-support telemetry marker count changed", errors
        )

    def test_result_guard_rejects_science_memory_and_scale_regressions(self) -> None:
        result = replay_result()
        result["full16"]["nrmse"][0] = 0.002
        result["four_spw"]["seconds"] = 8.5
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 33 * 1024**3}}
        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=8.0,
            effective_support_requested=True,
        )
        self.assertIn("full16 NRMSE exceeds 1e-3", errors)
        self.assertIn("process physical footprint exceeded 32 GiB", errors)
        self.assertIn("four-SPW replay regressed by more than 5%", errors)

    def test_manifest_guard_binds_topology_and_cardinality(self) -> None:
        section_names = (
            "prediction_samples",
            "source_sample_indices",
            "kernels",
            "prediction_phases",
            "tile_samples",
            "tile_phases",
            "term_weights",
            "active_tile_ids",
            "tile_fragment_offsets",
            "fragments",
        )

        def make_program(samples: int) -> dict:
            value = {
                name: {"byte_len": 1, "sha256": "0" * 64} for name in section_names
            }
            value["prediction_samples"]["len"] = samples
            value["payload_bytes"] = len(section_names)
            return value

        manifest = {
            "schema": "casa-rs-vlass-full16-aw-replay-private-v1",
            "programs": [
                make_program(FULL16["expected_samples"] // FULL16["expected_segments"])
                for _ in range(FULL16["expected_segments"] - 1)
            ],
            "provenance": {
                "field_ids": FIELD_IDS,
                "spw_ids": FULL16["spw_ids"],
                "use_pointing": True,
            },
            "payload": {"bytes": 100},
            "byte_ledger": {
                "payload_bytes": FULL16["expected_segments"] * len(section_names)
            },
            "model_grids": [
                {"offset": 0, "byte_len": 1, "sha256": "1" * 64},
                {"offset": 1, "byte_len": 1, "sha256": "2" * 64},
            ],
            "baseline_residual_grids": [
                {"offset": 2, "byte_len": 1, "sha256": "3" * 64},
                {"offset": 3, "byte_len": 1, "sha256": "4" * 64},
            ],
        }
        manifest["programs"].append(
            make_program(
                FULL16["expected_samples"]
                - sum(
                    program["prediction_samples"]["len"]
                    for program in manifest["programs"]
                )
            )
        )
        self.assertEqual(validate_manifest(manifest, FULL16), [])
        changed = copy.deepcopy(manifest)
        changed["provenance"]["spw_ids"] = FOUR_SPW["spw_ids"]
        self.assertIn(
            "fixture SPW topology changed", validate_manifest(changed, FULL16)
        )
        changed = copy.deepcopy(manifest)
        changed["model_grids"][0]["sha256"] = "0" * 64
        self.assertIn(
            "fixture model_grids section SHA-256 is invalid",
            validate_manifest(changed, FULL16),
        )

    def test_parser_requires_one_explicit_campaign_object(self) -> None:
        result = replay_result()
        output = f"test banner ... VLASS_REPLAY_BENCHMARK_JSON {json.dumps(result)}\n"
        self.assertEqual(parse_benchmark_result(output), result)
        with self.assertRaises(ContractError):
            parse_benchmark_result("test banner\n")


if __name__ == "__main__":
    unittest.main()
