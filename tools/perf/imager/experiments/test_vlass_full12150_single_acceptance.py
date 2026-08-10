# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

from pathlib import Path
import unittest

from tools.perf.imager.experiments import vlass_full12150_single_acceptance as single


def valid_probe_log(target_mib: int) -> str:
    target_bytes = target_mib * single.shared.MIB
    return "\n".join(
        [
            "standard_mfs_planning_resources "
            f"memory_target_bytes={target_bytes} "
            "memory_target_origin=cli-intentional-oversubscription "
            "no_swap_headroom_bytes=19000000000",
            "standard_mfs_runtime_plan initial_dirty_backend=metal-row-run-grouped "
            "residual_backend=metal-row-run-grouped",
            "standard_mfs_execution_decision name=awproject_selected_field_count "
            "value=1 origin=Workload",
            "standard_mfs_planner_preflight status=admitted "
            "grouped_metal_status=not-applicable rows_total=10400 ddids=16 "
            "selected_channels=64 correlations=4 memory_pressure_policy=oversubscribe "
            "visibility_streamed=false replay_compiled=false grids_allocated=false "
            "products_materialized=false",
        ]
    )


def valid_runtime_log() -> str:
    lines = [
        "awproject_metal_resident_tile_chain built=true gpu_residual_replay=false "
        "program_bytes=100 samples=1",
        "awproject_metal_resident_tile_chain built=false gpu_residual_replay=true "
        "program_bytes=100 samples=1",
        "awproject_compact_replay_cache budget_bytes=1000 resident_bytes=100 "
        "compiled_total_bytes=100 compiled_total_bytes_complete=true resident_blocks=1 "
        "partial_blocks=0 rejected_blocks=0 spilled_global_read_bytes=0 hits=1 misses=1",
        "awproject_compact_replay_release stage=residual-grid-end resident_bytes=100 "
        "resident_global_segments=0 next_use=none",
    ]
    for suffix in single.shared.EXPECTED_PRODUCTS:
        shape = "1x1x1x1" if suffix.startswith(".sumwt") else "12150x12150x1x1"
        lines.append(
            f"image_product_write suffix={suffix} role=test shape={shape} elements=1 elapsed_ms=1"
        )
    return "\n".join(lines)


class FullVlassSingleAcceptanceContractTest(unittest.TestCase):
    def test_command_binds_single_field_science_without_source_major_override(
        self,
    ) -> None:
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
        self.assertEqual("1", result["decisions"]["awproject_selected_field_count"])
        mutations = (
            ("grouped_metal_status=not-applicable", "grouped_metal_status=admitted"),
            ("rows_total=10400", "rows_total=655200"),
            (
                "initial_dirty_backend=metal-row-run-grouped",
                "initial_dirty_backend=cpu",
            ),
            (
                "awproject_selected_field_count value=1",
                "awproject_selected_field_count value=63",
            ),
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
            (
                "built=false gpu_residual_replay=true",
                "built=false gpu_residual_replay=false",
            ),
            ("rejected_blocks=0", "rejected_blocks=1"),
            ("hits=1", "hits=0"),
            ("next_use=none", "next_use=unknown"),
            ("shape=12150x12150x1x1", "shape=1x1x1x1"),
        )
        for old, new in mutations:
            with self.subTest(mutation=old):
                with self.assertRaises(single.shared.AcceptanceError):
                    single.validate_runtime_log(accepted.replace(old, new, 1))


if __name__ == "__main__":
    unittest.main()
