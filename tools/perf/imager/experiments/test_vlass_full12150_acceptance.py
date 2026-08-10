#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused contract tests for the bounded full-VLASS acceptance supervisor."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


MODULE_PATH = Path(__file__).with_name("vlass_full12150_acceptance.py")
SPEC = importlib.util.spec_from_file_location("vlass_full12150_acceptance", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
acceptance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = acceptance
SPEC.loader.exec_module(acceptance)
from perf_harness.casa_tclean import tree_inventory  # noqa: E402
from perf_harness.tree_identity import tree_identity  # noqa: E402


def host_sample(
    *,
    headroom: int,
    swapouts: int = 4,
    throttled: int = 0,
    compressed_bytes: int = 0,
) -> dict:
    page_size = 4096
    pages = headroom // page_size
    return {
        "page_size_bytes": page_size,
        "pages_free": pages,
        "pages_inactive": 0,
        "pages_speculative": 0,
        "pages_throttled": throttled,
        "swapouts": swapouts,
        "swap_used_bytes": 1024,
        "host_compressed_memory_bytes": compressed_bytes,
    }


def valid_probe_log(
    target_mib: int,
    *,
    memory_pressure_policy: str = "conservative-no-swap",
    headroom_bytes: int | None = None,
) -> str:
    target_bytes = target_mib * acceptance.MIB
    target_origin = (
        "cli-intentional-oversubscription"
        if memory_pressure_policy == "oversubscribe"
        else "cli-imaging"
    )
    decisions = {
        "awproject_selected_field_count": "63",
        "awproject_initial_grid_backend": "source-major-grouped-metal-f64",
        "awproject_source_major_architecture": "direct-source-major-v4-high-only-dense-residual",
        "awproject_source_major_initial_accumulation": "high-limb-only",
        "awproject_source_major_initial_grid_bytes": "9447840000",
        "awproject_multifield_initial_grid_admission": "admitted",
        "awproject_grouped_replay_replaced_generic_caches": "true",
        "awproject_grouped_metal_generic_scratch_bytes": "0",
        "awproject_grouped_metal_residual_output_bytes": "2361960000",
        "awproject_grouped_metal_residual_compensation_bytes": "2361960000",
        "awproject_grouped_metal_model_wrapper_bytes": "2361960000",
        "awproject_grouped_metal_safety_reserve_bytes": str(64 * acceptance.MIB),
    }
    lines = [
        "standard_mfs_planning_resources "
        f"memory_target_bytes={target_bytes} memory_target_origin={target_origin} "
        f"no_swap_headroom_bytes={headroom_bytes or target_bytes + acceptance.GIB}",
        "standard_mfs_runtime_plan initial_dirty_backend=metal-row-run-grouped "
        "residual_backend=metal-row-run-grouped",
        "awproject_grouped_replay_plan architecture=source-order-grouped-tile-v1 "
        "tile_side=11 omitted_squared_l2_energy=0.000000000e0",
    ]
    lines.extend(
        f"standard_mfs_execution_decision name={name} value={value} origin=Planner"
        for name, value in decisions.items()
    )
    lines.append(
        "standard_mfs_planner_preflight status=admitted "
        "grouped_metal_status=admitted rows_total=655200 ddids=16 "
        "selected_channels=64 correlations=4 "
        f"memory_pressure_policy={memory_pressure_policy} "
        "visibility_streamed=false replay_compiled=false grids_allocated=false "
        "products_materialized=false"
    )
    return "\n".join(lines)


def valid_runtime_log() -> str:
    lines = [
        "awproject_source_major_block source_block=0 accepted_samples=1 "
        "initial_partitions=2 initial_grid_bytes=9447840000 "
        "initial_compensation_bytes=0 spill_bytes=0 reload_bytes=0 "
        "architecture=direct-source-major-v4-high-only-dense-residual "
        "initial_accumulation=high-limb-only",
        "awproject_metal_initial_readback products=8 "
        "residency=metal-shared-high-limb-only-grid resident_bytes=9447840000",
        "awproject_grouped_metal_admission phase=sealed segment=0 "
        "source_boundary_upper_bytes=100 exact_additional_bytes=90 all_fit=true",
        "awproject_effective_support segment=0 omitted_energy_fraction=0 "
        "max_omitted_energy_fraction=0 prediction_plans=2 "
        "prediction_cropped_plans=0 prediction_original_tap_visits=10 "
        "prediction_retained_tap_visits=10 tile_plans=2 tile_cropped_plans=0 "
        "tile_original_tap_visits=20 tile_retained_tap_visits=20 "
        "resident_kernel_bytes_before=100 resident_kernel_bytes_after=100",
        "awproject_aot_grouped_tile_receipt segment=0 "
        "omitted_energy_fraction_bits=0 grouped_plans_hash_prefix=123 "
        "legacy_grouped_plans_hash_prefix=abc grouped_route_hash_prefix=def "
        "legacy_grouped_route_hash_prefix=def "
        "compile_transient_bytes_peak_estimated=90 compile_admission_limit_bytes=100 "
        "raw_kernel_atlas_bytes=100 compact_kernel_atlas_bytes=40 "
        "compact_kernel_stencils=2 compact_kernel_plan_references=4 "
        "compact_kernel_scratch_bytes=64",
        "awproject_source_major_kernel_compaction source_block=0 raw_bytes=100 "
        "compact_bytes=40 stencils=2 plan_references=4 scratch_bytes=64 "
        "applied=true bit_exact=true",
        "awproject_metal_grouped_replay_retention decision=resident-complete "
        "segments=1 program_bytes=1000",
        "awproject_grouped_metal_admission phase=runtime segment=0 all_fit=true "
        "prechecks=fit postchecks=fit host_bytes_retained_during_tile=0 "
        "persistent_post_combined_bytes=10 persistent_maximum_current_bytes=20 "
        "prediction_post_combined_bytes=11 prediction_maximum_current_bytes=20 "
        "tile_post_combined_bytes=12 tile_maximum_current_bytes=20",
        "awproject_grouped_metal_host_lifetime segment=0 "
        "candidate_audit_allocation_bytes=0 host_bytes_retained_during_tile=0 "
        "dispatch_released_before_tile=true "
        "candidate_auxiliary_released_before_tile=true "
        "candidate_result_released_before_tile=true",
        "awproject_metal_resident_grouped_replay_summary segments=1 "
        "program_bytes=1000 spill_read_bytes=0 runtime_grouping_builds=0 "
        "runtime_sort_builds=0 runtime_route_builds=0",
    ]
    for suffix in acceptance.EXPECTED_PRODUCTS:
        shape = "1x1x1x1" if suffix.startswith(".sumwt") else "12150x12150x1x1"
        lines.append(
            "image_product_write "
            f"suffix={suffix} role=test shape={shape} elements=147622500 "
            "elapsed_ms=1.0"
        )
    return "\n".join(lines)


class FullVlassAcceptanceContractTest(unittest.TestCase):
    def test_frozen_inputs_use_their_earned_identity_algorithms(self) -> None:
        paths = acceptance.default_paths(Path("/frozen"))
        compact_results = [
            {"tree_sha256": acceptance.MS_TREE_SHA256},
            {"tree_sha256": acceptance.MASK_TREE_SHA256},
        ]
        with (
            mock.patch.object(
                acceptance,
                "compact_tree_identity_uncached",
                side_effect=compact_results,
            ) as compact,
            mock.patch.object(
                acceptance,
                "casa_tree_inventory_uncached",
                return_value={"stable_tree_sha256": acceptance.CF_TREE_SHA256},
            ) as casa_inventory,
        ):
            observed = acceptance.validate_input_identities(paths)
        self.assertEqual(acceptance.MS_TREE_SHA256, observed["ms"]["tree_sha256"])
        compact.assert_any_call(paths.ms)
        compact.assert_any_call(paths.mask, excluded_names={"table.lock"})
        casa_inventory.assert_called_once_with(paths.cf_cache)

    def test_uncached_identities_match_canonical_algorithms(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "nested").mkdir()
            (root / "nested/payload").write_bytes(b"science")
            (root / "nested/table.lock").write_bytes(b"volatile")
            compact = acceptance.compact_tree_identity_uncached(
                root, excluded_names={"table.lock"}
            )
            casa = acceptance.casa_tree_inventory_uncached(root)
            self.assertEqual(
                tree_identity(root, excluded_names={"table.lock"})["tree_sha256"],
                compact["tree_sha256"],
            )
            self.assertEqual(
                tree_inventory(root)["stable_tree_sha256"],
                casa["stable_tree_sha256"],
            )
            self.assertTrue(casa["darwin_f_nocache_applied"])

    def test_no_swap_target_has_hard_floor_and_two_gib_reserve(self) -> None:
        with self.assertRaisesRegex(acceptance.AcceptanceError, "below"):
            acceptance.target_mib_for_headroom(
                acceptance.MINIMUM_NO_SWAP_HEADROOM_BYTES - 1
            )
        headroom = acceptance.MINIMUM_NO_SWAP_HEADROOM_BYTES
        self.assertEqual(
            (headroom - acceptance.HOST_RESERVE_BYTES) // acceptance.MIB,
            acceptance.target_mib_for_headroom(headroom),
        )
        self.assertEqual(
            acceptance.MAX_TARGET_MIB,
            acceptance.target_mib_for_headroom(40 * acceptance.GIB),
        )
        self.assertEqual(
            acceptance.MAX_TARGET_MIB,
            acceptance.target_mib_for_headroom(
                acceptance.MINIMUM_NO_SWAP_HEADROOM_BYTES - 1,
                allow_pressure_experiment=True,
            ),
        )

    def test_baseline_rejects_pressure_swapout_and_throttling(self) -> None:
        first = host_sample(headroom=25_000_000_000)
        second = host_sample(headroom=25_000_000_000)
        self.assertGreater(
            acceptance.validate_baseline_samples(first, second, 1).target_mib, 0
        )
        with self.assertRaisesRegex(acceptance.AcceptanceError, "pressure"):
            acceptance.validate_baseline_samples(first, second, 2)
        changed_swap = dict(second, swapouts=5)
        with self.assertRaisesRegex(acceptance.AcceptanceError, "swapout"):
            acceptance.validate_baseline_samples(first, changed_swap, 1)
        changed_throttle = dict(second, pages_throttled=1)
        with self.assertRaisesRegex(acceptance.AcceptanceError, "throttled"):
            acceptance.validate_baseline_samples(first, changed_throttle, 1)
        experimental = acceptance.validate_baseline_samples(
            host_sample(headroom=19_000_000_000),
            host_sample(headroom=19_000_000_000),
            1,
            allow_pressure_experiment=True,
        )
        self.assertEqual(acceptance.MAX_TARGET_MIB, experimental.target_mib)

    def test_direct_command_carries_exact_science_and_private_topology(self) -> None:
        paths = acceptance.default_paths(Path("/frozen"))
        common = acceptance.common_imager_command(
            Path("/frozen/casars-imager"), paths, Path("/out/rust"), 22_000
        )

        def value(option: str) -> str:
            return common[common.index(option) + 1]

        self.assertEqual(acceptance.ALL_FIELDS, value("--field"))
        self.assertEqual("2~17", value("--spw"))
        self.assertEqual("12150", value("--imsize"))
        self.assertEqual("20000", value("--niter"))
        self.assertNotIn("--standard-mfs-initial-dirty-backend", common)
        self.assertEqual(
            "metal-row-run-grouped", value("--standard-mfs-residual-backend")
        )
        self.assertEqual(
            "conservative-no-swap", value("--imaging-memory-pressure-policy")
        )
        self.assertEqual(1, common.count("--spw"))
        self.assertIn("--usepointing", common)
        self.assertIn("--mask-image", common)
        self.assertIn("--cfcache", common)
        environment = acceptance.restricted_environment(paths)
        self.assertNotIn(
            "CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GLOBAL_TILE_REPLAY", environment
        )
        self.assertEqual(
            "0", environment["CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES"]
        )

    def test_probe_contract_is_fail_closed(self) -> None:
        target_mib = 22_000
        accepted = valid_probe_log(target_mib)
        self.assertEqual(
            "admitted",
            acceptance.validate_probe_log(accepted, target_mib)["preflight"]["status"],
        )
        mutations = (
            ("grouped_metal_status=admitted", "grouped_metal_status=rejected"),
            ("rows_total=655200", "rows_total=1"),
            (
                "initial_dirty_backend=metal-row-run-grouped",
                "initial_dirty_backend=cpu",
            ),
            (
                "omitted_squared_l2_energy=0.000000000e0",
                "omitted_squared_l2_energy=1e-6",
            ),
            ("tile_side=11", "tile_side=16"),
            (
                "awproject_selected_field_count value=63",
                "awproject_selected_field_count value=1",
            ),
            (
                "memory_target_origin=cli-imaging",
                "memory_target_origin=cli-capped-to-no-swap-headroom",
            ),
        )
        for old, new in mutations:
            with self.subTest(mutation=old):
                self.assertIn(old, accepted)
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.validate_probe_log(
                        accepted.replace(old, new), target_mib
                    )

    def test_pressure_experiment_probe_is_explicit_and_still_fail_closed(self) -> None:
        target_mib = acceptance.MAX_TARGET_MIB
        accepted = valid_probe_log(
            target_mib,
            memory_pressure_policy="oversubscribe",
            headroom_bytes=19_000_000_000,
        )
        observed = acceptance.validate_probe_log(
            accepted,
            target_mib,
            memory_pressure_policy="oversubscribe",
            require_target_within_headroom=False,
        )
        self.assertEqual("admitted", observed["preflight"]["status"])
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.validate_probe_log(
                accepted.replace(
                    "memory_target_origin=cli-intentional-oversubscription",
                    "memory_target_origin=cli-imaging",
                ),
                target_mib,
                memory_pressure_policy="oversubscribe",
                require_target_within_headroom=False,
            )
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.validate_probe_log(
                accepted.replace(
                    "memory_pressure_policy=oversubscribe",
                    "memory_pressure_policy=conservative-no-swap",
                ),
                target_mib,
                memory_pressure_policy="oversubscribe",
                require_target_within_headroom=False,
            )

    def test_runtime_contract_is_fail_closed(self) -> None:
        accepted = valid_runtime_log()
        result = acceptance.validate_runtime_log(accepted)
        self.assertEqual(1, result["segment_count"])
        self.assertEqual(19, result["product_count"])
        mutations = (
            ("initial_compensation_bytes=0", "initial_compensation_bytes=9447840000"),
            ("all_fit=true", "all_fit=false"),
            ("prediction_cropped_plans=0", "prediction_cropped_plans=1"),
            (
                "legacy_grouped_route_hash_prefix=def",
                "legacy_grouped_route_hash_prefix=bad",
            ),
            ("decision=resident-complete", "decision=spill-prefetch"),
            ("spill_read_bytes=0", "spill_read_bytes=1"),
            ("runtime_route_builds=0", "runtime_route_builds=1"),
            ("bit_exact=true", "bit_exact=false"),
            (
                "candidate_audit_allocation_bytes=0",
                "candidate_audit_allocation_bytes=16",
            ),
            ("shape=12150x12150x1x1", "shape=4096x4096x1x1"),
        )
        for old, new in mutations:
            with self.subTest(mutation=old):
                self.assertIn(old, accepted)
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.validate_runtime_log(accepted.replace(old, new, 1))

    def test_monitor_policy_stops_on_each_destructive_signal(self) -> None:
        first = host_sample(headroom=25_000_000_000)
        baseline = acceptance.validate_baseline_samples(first, dict(first), 1)
        self.assertIsNone(
            acceptance.monitor_stop_reason(
                baseline=baseline,
                sample=dict(first),
                pressure_level=1,
                pressure_warning_samples=0,
                swap_used_growth_samples=0,
            )
        )
        self.assertIsNone(
            acceptance.monitor_stop_reason(
                baseline=baseline,
                sample=dict(first),
                pressure_level=2,
                pressure_warning_samples=1,
                swap_used_growth_samples=0,
            )
        )
        cases = (
            (dict(first), 2, 2, 0, "pressure"),
            (dict(first), 4, 0, 0, "pressure"),
            (dict(first, pages_throttled=1), 1, 0, 0, "throttled"),
            (dict(first, swapouts=5), 1, 0, 0, "swapout"),
            (dict(first), 1, 0, 2, "swap-used"),
            (
                dict(
                    first,
                    host_compressed_memory_bytes=2 * acceptance.GIB + 1,
                ),
                1,
                0,
                0,
                "compressed",
            ),
            (
                host_sample(headroom=acceptance.HOST_RESERVE_BYTES - 1),
                1,
                0,
                0,
                "headroom",
            ),
        )
        for sample, level, warnings, growth, expected in cases:
            with self.subTest(expected=expected):
                reason = acceptance.monitor_stop_reason(
                    baseline=baseline,
                    sample=sample,
                    pressure_level=level,
                    pressure_warning_samples=warnings,
                    swap_used_growth_samples=growth,
                    max_compressed_growth_bytes=(
                        2 * acceptance.GIB if expected == "compressed" else None
                    ),
                )
                self.assertIsNotNone(reason)
                self.assertIn(expected, reason.lower())

    def test_comparison_request_reuses_frozen_full_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            comparison = {
                "products": list(acceptance.EXPECTED_PRODUCTS),
                "max_elements_per_product": 1_000_000,
                "mode": "full",
                "full_chunk_elements": 262_144,
                "require_exact_product_inventory": True,
                "require_metadata_parity": True,
                "source_regions": [{"products": [".image.tt0"]}],
                "tolerances": {"schema_version": 2},
            }
            request = acceptance.comparison_request(
                {"comparison": comparison},
                root / "rust",
                root / "casa",
                root,
            )
        self.assertEqual(list(acceptance.EXPECTED_PRODUCTS), request["products"])
        self.assertEqual("full", request["mode"])
        self.assertTrue(request["require_exact_product_inventory"])
        self.assertTrue(request["require_metadata_parity"])


if __name__ == "__main__":
    unittest.main()
