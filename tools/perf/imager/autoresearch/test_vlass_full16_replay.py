"""Contract tests for the frozen full-band VLASS replay benchmark."""

from __future__ import annotations

import copy
import json
import unittest

from autoresearch.vlass_full16_replay import (
    AOT_COMPILE_ADMISSION_LIMIT_BYTES,
    AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV,
    AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV,
    AOT_GROUPED_COMPILER_BINARY_SHA256_ENV,
    AOT_GROUPED_THRESHOLD_BITS,
    AOT_HASHMAP_MINIMUM_RESERVE_BYTES,
    FIELD_IDS,
    FOUR_SPW_PROMOTION_MAX_SECONDS,
    FULL16_PROMOTION_MAX_SECONDS,
    FOUR_SPW,
    FULL16,
    ContractError,
    aot_grouped_compiler_key_is_valid,
    aot_grouped_segment_receipt_is_valid,
    benchmark_environment,
    parse_benchmark_result,
    result_errors,
    validate_manifest,
)


def replay_result() -> dict:
    def effective_support_receipt() -> dict:
        return {
            "omitted_energy_fraction": 1e-6,
            "unique_stencils": 2,
            "stencil_lookups": 4,
            "crop_evaluations": 2,
            "index_peak_entries": 2,
            "index_estimated_bytes": 256,
            "prefix_scratch_peak_bytes": 288,
            "prediction": {
                "plan_count": 2,
                "unique_stencils": 2,
                "original_tap_visits": 50,
                "retained_tap_visits": 18,
                "cropped_plans": 2,
            },
            "tile": {
                "plan_count": 2,
                "unique_stencils": 2,
                "original_tap_visits": 50,
                "retained_tap_visits": 18,
                "cropped_plans": 2,
            },
            "max_omitted_energy_fraction": 1e-7,
            "fallback_counts": {},
            "compile_seconds": 0.001,
            "resident_kernel_bytes_before": 640,
            "resident_kernel_bytes_after": 640,
        }

    def aot_receipt(samples: int) -> dict:
        hashes = {
            "crop_decisions_sha256": "1" * 64,
            "grouped_plans_sha256": "2" * 64,
            "sample_role_groups_sha256": "3" * 64,
            "grouped_route_sha256": "4" * 64,
            "legacy_grouped_plans_sha256": "2" * 64,
            "legacy_grouped_route_sha256": "4" * 64,
        }
        raw_resident_bytes = 1_000
        raw_prediction_bytes = 8
        canonical_group_plan_capacity_bytes = 64
        canonical_group_sum_capacity_bytes = 64
        canonical_hashmap_estimated_bytes = 32
        tile_planner_known_peak_bytes = 64
        sample_role_group_capacity_bytes = 8
        final_hashmap_estimated_bytes = 32
        aot_group_sum_bytes = 64
        fixed_scale_bytes = 16
        effective_support_hashmap_estimated_bytes = 256
        effective_support_prefix_scratch_bytes = 288
        effective_support_scratch_estimated_bytes = (
            effective_support_hashmap_estimated_bytes
            + effective_support_prefix_scratch_bytes
        )
        compile_transient_bytes_peak_estimated = sum(
            (
                raw_resident_bytes,
                canonical_group_plan_capacity_bytes,
                canonical_group_sum_capacity_bytes,
                canonical_hashmap_estimated_bytes,
                tile_planner_known_peak_bytes,
                sample_role_group_capacity_bytes,
                final_hashmap_estimated_bytes,
                aot_group_sum_bytes,
                fixed_scale_bytes,
                effective_support_hashmap_estimated_bytes,
                effective_support_prefix_scratch_bytes,
            )
        )
        return {
            "omitted_energy_fraction_bits": AOT_GROUPED_THRESHOLD_BITS,
            "sample_count": samples,
            "group_count": 2,
            **hashes,
            "ledger": {
                "raw_resident_bytes_before_compile": raw_resident_bytes,
                "raw_prediction_sample_bytes_replaced": raw_prediction_bytes,
                "cropped_prediction_sample_bytes": raw_prediction_bytes,
                "raw_tile_sample_bytes_released": 128,
                "raw_route_bytes_released": 12,
                "grouped_plan_bytes": 64,
                "sample_role_group_bytes": 8,
                "grouped_route_bytes": 12,
                "canonical_group_plan_capacity_bytes": canonical_group_plan_capacity_bytes,
                "canonical_group_sum_capacity_bytes": canonical_group_sum_capacity_bytes,
                "canonical_hashmap_estimated_bytes": canonical_hashmap_estimated_bytes,
                "tile_planner_known_peak_bytes": tile_planner_known_peak_bytes,
                "sample_role_group_capacity_bytes": sample_role_group_capacity_bytes,
                "final_hashmap_estimated_bytes": final_hashmap_estimated_bytes,
                "aot_group_sum_bytes": aot_group_sum_bytes,
                "fixed_scale_bytes": fixed_scale_bytes,
                "effective_support_hashmap_estimated_bytes": effective_support_hashmap_estimated_bytes,
                "effective_support_prefix_scratch_bytes": effective_support_prefix_scratch_bytes,
                "effective_support_scratch_estimated_bytes": effective_support_scratch_estimated_bytes,
                "compile_transient_bytes_peak_estimated": compile_transient_bytes_peak_estimated,
                "hashmap_uncertainty_reserve_bytes": AOT_HASHMAP_MINIMUM_RESERVE_BYTES,
                "compile_admission_bytes": compile_transient_bytes_peak_estimated
                + AOT_HASHMAP_MINIMUM_RESERVE_BYTES,
                "compile_admission_limit_bytes": AOT_COMPILE_ADMISSION_LIMIT_BYTES,
                "persisted_tile_bytes": 84,
            },
        }

    def variant(samples: int, segments: int, spws: list[int], seconds: float) -> dict:
        segment_samples = [
            samples // segments + (1 if ordinal < samples % segments else 0)
            for ordinal in range(segments)
        ]
        raw_reload_bytes = segments * 60
        sidecar_reload_bytes = segments * 40
        raw_prediction_bytes = segments * 8
        selected_payload_bytes = raw_reload_bytes + sidecar_reload_bytes
        return {
            "seconds": seconds,
            "samples": samples,
            "segments": segments,
            "rejected_samples": 0,
            "payload_bytes": selected_payload_bytes,
            "reload_bytes": selected_payload_bytes,
            "raw_reload_bytes": raw_reload_bytes,
            "sidecar_reload_bytes": sidecar_reload_bytes,
            "raw_prediction_sample_bytes_not_read": raw_prediction_bytes,
            "sidecar_cropped_prediction_sample_bytes_read": raw_prediction_bytes,
            "raw_replaced_section_bytes_read": 0,
            "timed_io_bytes": selected_payload_bytes + sidecar_reload_bytes,
            "sidecar_payload_verification": {
                "bytes": sidecar_reload_bytes,
                "seconds": 0.01,
                "included_in_seconds": True,
            },
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
            "effective_support_telemetry_markers": 0,
            "effective_support": {
                "requested": True,
                "decision": "enabled",
                "reason": None,
                "segment_count": segments,
                "compiled_segment_count": 0,
                "total_compile_seconds": 0.0,
                "initial_prepare_seconds": 0.01,
                "prefetch_wait_seconds": 0.001 if segments >= 2 else 0.0,
            },
            "aot_grouped_tile": {
                "enabled": True,
                "use_count": segments,
                "runtime_grouping_builds": 0,
                "runtime_sort_builds": 0,
                "runtime_route_builds": 0,
                "sidecar_artifact_bytes": sidecar_reload_bytes,
                "byte_lifetime_ledger": {
                    "raw_prediction_sample_bytes_replaced_at_compile": raw_prediction_bytes,
                    "cropped_prediction_sample_bytes_persisted": raw_prediction_bytes,
                    "raw_prediction_sample_bytes_retained_for_replay": 0,
                    "raw_prediction_sample_bytes_read_during_replay": 0,
                    "prediction_replacement_equation": "raw_prediction_sample_bytes_replaced_at_compile == "
                    "cropped_prediction_sample_bytes_persisted",
                    "raw_tile_sample_bytes_released_at_compile": 128 * segments,
                    "raw_ungrouped_route_bytes_released_at_compile": 12 * segments,
                    "specialized_sidecar_section_bytes": sidecar_reload_bytes,
                    "specialized_sidecar_file_bytes": sidecar_reload_bytes,
                    "raw_sections_replaced_not_read": [
                        "prediction_samples",
                        "tile_samples",
                        "active_tile_ids",
                        "tile_fragment_offsets",
                        "fragments",
                    ],
                    "raw_sections_referenced_not_copied": [
                        "source_sample_indices",
                        "kernels",
                        "prediction_phases",
                        "tile_phases",
                        "term_weights",
                    ],
                    "runtime_grouping_builds": 0,
                    "runtime_sort_builds": 0,
                    "runtime_route_builds": 0,
                },
            },
            "segment_receipts": [
                {
                    "samples": segment_sample_count,
                    "payload_bytes": 100,
                    "raw_reload_bytes": 60,
                    "sidecar_reload_bytes": 40,
                    "raw_prediction_sample_bytes_not_read": 8,
                    "sidecar_cropped_prediction_sample_bytes_read": 8,
                    "raw_replaced_section_bytes_read": 0,
                    "reload_bytes": 100,
                    "effective_support": effective_support_receipt(),
                    "aot_grouped_tile": aot_receipt(segment_sample_count),
                }
                for segment_sample_count in segment_samples
            ],
        }

    return {
        "schema": "casa-rs-vlass-full16-aw-replay-campaign-v1",
        "seconds": 63.0,
        "full16": variant(25_030_848, 10, list(range(2, 18)), 63.0),
        "four_spw": variant(6_416_526, 1, [2, 7, 12, 17], 8.0),
    }


class ReplayContractTests(unittest.TestCase):
    def test_benchmark_environment_requires_both_aot_sidecars(self) -> None:
        compiler_sha256 = "a" * 64
        environment = benchmark_environment(compiler_sha256)

        self.assertIn(AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV, environment)
        self.assertIn(AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV, environment)
        self.assertEqual(
            compiler_sha256,
            environment[AOT_GROUPED_COMPILER_BINARY_SHA256_ENV],
        )
        self.assertIn(
            "aot-grouped-tile-1e-6-v3",
            environment[AOT_GROUPED_SIDECAR_FULL16_PREFIX_ENV],
        )
        self.assertIn(
            "aot-grouped-tile-1e-6-v3",
            environment[AOT_GROUPED_SIDECAR_FOUR_SPW_PREFIX_ENV],
        )

    def test_aot_receipt_guard_binds_incumbent_differential_hashes(self) -> None:
        receipt = replay_result()["full16"]["segment_receipts"][0]["aot_grouped_tile"]

        self.assertTrue(aot_grouped_segment_receipt_is_valid(receipt))
        receipt["legacy_grouped_route_sha256"] = "5" * 64
        self.assertFalse(aot_grouped_segment_receipt_is_valid(receipt))

    def test_benchmark_environment_selects_captured_residual_semantics(self) -> None:
        environment = benchmark_environment("a" * 64)

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

    def test_aot_sidecar_key_rejects_stale_compiler_executable(self) -> None:
        compiler_sha256 = "a" * 64
        key = {"compiler_binary_sha256": compiler_sha256}

        self.assertTrue(aot_grouped_compiler_key_is_valid(key, compiler_sha256))
        key["compiler_binary_sha256"] = "b" * 64
        self.assertFalse(aot_grouped_compiler_key_is_valid(key, compiler_sha256))

    def test_result_guard_accepts_complete_practical_contract(self) -> None:
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}
        self.assertEqual(
            result_errors(
                replay_result(),
                telemetry,
                four_spw_baseline_seconds=8.0,
            ),
            [],
        )

    def test_result_guard_rejects_runtime_compilation_and_grouping(self) -> None:
        result = replay_result()
        result["full16"]["effective_support"]["decision"] = "rejected"
        result["four_spw"]["effective_support"]["reason"] = (
            "single_segment_no_prefetch_overlap"
        )
        result["four_spw"]["effective_support"]["compiled_segment_count"] = 1
        result["four_spw"]["effective_support_telemetry_markers"] = 1
        result["full16"]["aot_grouped_tile"]["runtime_grouping_builds"] = 1
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}

        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=8.0,
        )

        self.assertIn("full16 effective-support decision changed", errors)
        self.assertIn("four_spw effective-support reason changed", errors)
        self.assertIn(
            "four_spw effective-support compiled segment count changed", errors
        )
        self.assertIn(
            "four_spw effective-support telemetry marker count changed", errors
        )
        self.assertIn(
            "full16 AOT grouped-tile runtime_grouping_builds is not zero", errors
        )

    def test_result_guard_rejects_wrong_effective_support_compiler_receipt(
        self,
    ) -> None:
        result = replay_result()
        receipts = result["full16"]["segment_receipts"]
        receipts[0]["effective_support"]["crop_evaluations"] = 1
        receipts[1]["effective_support"]["prediction"]["retained_tap_visits"] = 51
        receipts[2]["effective_support"]["resident_kernel_bytes_after"] = 639
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}

        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=8.0,
        )

        self.assertIn("full16 specialized segment receipt changed", errors)

    def test_result_guard_rejects_science_memory_and_scale_regressions(self) -> None:
        result = replay_result()
        result["full16"]["nrmse"][0] = 0.002
        result["four_spw"]["seconds"] = 8.5
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 33 * 1024**3}}
        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=8.0,
        )
        self.assertIn("full16 NRMSE exceeds 1e-3", errors)
        self.assertIn("process physical footprint exceeded 32 GiB", errors)
        self.assertIn("four-SPW replay regressed by more than 5%", errors)

    def test_result_guard_enforces_hard_full16_and_four_spw_limits(self) -> None:
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}
        result = replay_result()
        result["full16"]["seconds"] = FULL16_PROMOTION_MAX_SECONDS - 1e-9
        result["four_spw"]["seconds"] = FOUR_SPW_PROMOTION_MAX_SECONDS - 1e-9
        self.assertNotIn(
            "full16 replay exceeded hard promotion limit",
            result_errors(
                result,
                telemetry,
                four_spw_baseline_seconds=None,
            ),
        )
        result["full16"]["seconds"] = FULL16_PROMOTION_MAX_SECONDS + 1e-9
        result["four_spw"]["seconds"] = FOUR_SPW_PROMOTION_MAX_SECONDS + 1e-9
        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=None,
        )
        self.assertIn("full16 replay exceeded hard promotion limit", errors)
        self.assertIn("four_spw replay exceeded hard promotion limit", errors)

    def test_result_guard_rejects_prediction_replacement_and_admission_mutations(
        self,
    ) -> None:
        telemetry = {"summary": {"process_physical_footprint_bytes_peak": 12 * 1024**3}}
        result = replay_result()
        segment = result["full16"]["segment_receipts"][0]
        segment["raw_prediction_sample_bytes_not_read"] += 1
        segment["aot_grouped_tile"]["ledger"][
            "compile_transient_bytes_peak_estimated"
        ] += 1
        result["four_spw"]["sidecar_payload_verification"]["included_in_seconds"] = (
            False
        )

        errors = result_errors(
            result,
            telemetry,
            four_spw_baseline_seconds=None,
        )

        self.assertIn("full16 specialized segment receipt changed", errors)
        self.assertIn("four_spw timed sidecar payload verification changed", errors)

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
            value["source_sample_indices"] = {
                "len": 0,
                "byte_len": 0,
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            }
            value["payload_bytes"] = len(section_names) - 1
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
                "by_section": {"source_sample_indices": 0},
                "payload_bytes": FULL16["expected_segments"] * (len(section_names) - 1),
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
        retained = copy.deepcopy(manifest)
        retained_program = retained["programs"][0]
        retained_count = retained_program["prediction_samples"]["len"]
        retained_bytes = retained_count * 4
        retained_program["source_sample_indices"] = {
            "len": retained_count,
            "byte_len": retained_bytes,
            "sha256": "1" * 64,
        }
        retained_program["payload_bytes"] += retained_bytes
        retained["byte_ledger"]["payload_bytes"] += retained_bytes
        retained["byte_ledger"]["by_section"]["source_sample_indices"] += retained_bytes
        self.assertEqual(validate_manifest(retained, FULL16), [])
        partial = copy.deepcopy(manifest)
        partial_program = partial["programs"][0]
        partial_program["source_sample_indices"] = {
            "len": 1,
            "byte_len": 4,
            "sha256": "1" * 64,
        }
        partial_program["payload_bytes"] += 4
        partial["byte_ledger"]["payload_bytes"] += 4
        partial["byte_ledger"]["by_section"]["source_sample_indices"] += 4
        self.assertIn(
            "fixture source-sample index cardinality changed",
            validate_manifest(partial, FULL16),
        )
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
