# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

from pathlib import Path
import unittest

from tools.perf.imager.experiments import vlass_full12150_single_acceptance as single


def valid_probe_log(target_mib: int) -> str:
    target_bytes = target_mib * single.shared.MIB
    decisions = {
        "awproject_selected_field_count": "1",
        "awproject_initial_grid_backend": "source-major-grouped-metal-f64",
        "awproject_source_major_architecture": "direct-source-major-v3-high-only-initial",
        "awproject_source_major_initial_accumulation": "high-limb-only",
        "awproject_source_major_initial_grid_bytes": "9447840000",
        "awproject_multifield_initial_grid_admission": "admitted",
        "awproject_grouped_replay_replaced_generic_caches": "true",
        "awproject_grouped_metal_generic_scratch_bytes": "0",
        "awproject_grouped_metal_residual_output_bytes": "2361960000",
        "awproject_grouped_metal_residual_compensation_bytes": "2361960000",
        "awproject_grouped_metal_model_wrapper_bytes": "2361960000",
        "awproject_grouped_metal_safety_reserve_bytes": str(64 * single.shared.MIB),
    }
    return "\n".join(
        [
            "standard_mfs_planning_resources "
            f"memory_target_bytes={target_bytes} "
            "memory_target_origin=cli-intentional-oversubscription "
            "no_swap_headroom_bytes=19000000000",
            "standard_mfs_runtime_plan initial_dirty_backend=metal-row-run-grouped "
            "residual_backend=metal-row-run-grouped",
            "awproject_grouped_replay_plan architecture=source-order-grouped-tile-v1 "
            "tile_side=11 omitted_squared_l2_energy=0.000000000e0",
            *(
                f"standard_mfs_execution_decision name={name} value={value} origin=Planner"
                for name, value in decisions.items()
            ),
            *(
                "casa_mfs_frequency_edge_range "
                f"source_low_hz={spw} source_high_hz={spw + 1} "
                "low_field=1525 high_field=1525"
                for spw in range(2, 18)
            ),
            "standard_mfs_planner_preflight status=admitted "
            "grouped_metal_status=admitted rows_total=10400 ddids=16 "
            "selected_channels=64 correlations=4 memory_pressure_policy=oversubscribe "
            "visibility_streamed=false replay_compiled=false grids_allocated=false "
            "products_materialized=false",
        ]
    )


def valid_runtime_log() -> str:
    lines = [
        "awproject_source_major_block source_block=0 accepted_samples=1 "
        "initial_partitions=2 initial_grid_bytes=9447840000 "
        "initial_compensation_bytes=0 spill_bytes=0 reload_bytes=0 "
        "architecture=direct-source-major-v3-high-only-initial "
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
        "omitted_energy_fraction_bits=0 grouped_plans_hash_prefix=abc "
        "legacy_grouped_plans_hash_prefix=abc grouped_route_hash_prefix=def "
        "legacy_grouped_route_hash_prefix=def "
        "compile_transient_bytes_peak_estimated=90 compile_admission_limit_bytes=100",
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
    for suffix in single.shared.EXPECTED_PRODUCTS:
        shape = "1x1x1x1" if suffix.startswith(".sumwt") else "12150x12150x1x1"
        lines.append(
            f"image_product_write suffix={suffix} role=test shape={shape} elements=1 elapsed_ms=1"
        )
    return "\n".join(lines)


class FullVlassSingleAcceptanceContractTest(unittest.TestCase):
    def test_command_binds_single_field_science_for_source_major_planning(self) -> None:
        paths = single.default_paths(Path("/frozen"))
        command = single.common_imager_command(
            Path("/frozen/casars-imager"),
            paths,
            Path("/out/rust"),
            32_000,
            memory_pressure_policy="oversubscribe",
        )

        def value(option: str) -> str:
            return command[command.index(option) + 1]

        self.assertEqual("1525", value("--field"))
        self.assertEqual("2~17", value("--spw"))
        self.assertEqual("12150", value("--imsize"))
        self.assertEqual("2000", value("--niter"))
        self.assertEqual("metal", value("--standard-mfs-acceleration"))
        self.assertEqual(
            "metal-row-run-grouped", value("--standard-mfs-residual-backend")
        )
        self.assertNotIn("--standard-mfs-initial-dirty-backend", command)
        self.assertEqual(str(paths.cf_cache), value("--cfcache"))
        self.assertEqual(str(paths.mask), value("--mask-image"))

    def test_probe_contract_rejects_topology_mutations(self) -> None:
        target_mib = 32_000
        accepted = valid_probe_log(target_mib)
        result = single.validate_probe_log(
            accepted,
            target_mib,
            memory_pressure_policy="oversubscribe",
            require_target_within_headroom=False,
        )
        self.assertEqual(16, len(result["frequency_edges"]))
        self.assertEqual("1", result["decisions"]["awproject_selected_field_count"])
        mutations = (
            ("grouped_metal_status=admitted", "grouped_metal_status=not-applicable"),
            ("rows_total=10400", "rows_total=655200"),
            (
                "initial_dirty_backend=metal-row-run-grouped",
                "initial_dirty_backend=cpu",
            ),
            ("tile_side=11", "tile_side=16"),
            ("direct-source-major-v3-high-only-initial", "legacy-windowed"),
            ("low_field=1525", "low_field=1107"),
        )
        for old, new in mutations:
            with self.subTest(mutation=old):
                with self.assertRaises(single.shared.AcceptanceError):
                    single.validate_probe_log(
                        accepted.replace(old, new),
                        target_mib,
                        memory_pressure_policy="oversubscribe",
                        require_target_within_headroom=False,
                    )

    def test_runtime_contract_requires_resident_reuse_release_and_products(
        self,
    ) -> None:
        accepted = valid_runtime_log()
        result = single.validate_runtime_log(accepted)
        self.assertEqual(19, result["product_count"])
        mutations = (
            ("initial_compensation_bytes=0", "initial_compensation_bytes=9447840000"),
            (
                "spill_read_bytes=0",
                "spill_read_bytes=1",
            ),
            ("all_fit=true", "all_fit=false"),
            ("runtime_grouping_builds=0", "runtime_grouping_builds=1"),
            (
                "candidate_result_released_before_tile=true",
                "candidate_result_released_before_tile=false",
            ),
            ("shape=12150x12150x1x1", "shape=1x1x1x1"),
        )
        for old, new in mutations:
            with self.subTest(mutation=old):
                with self.assertRaises(single.shared.AcceptanceError):
                    single.validate_runtime_log(accepted.replace(old, new, 1))


if __name__ == "__main__":
    unittest.main()
