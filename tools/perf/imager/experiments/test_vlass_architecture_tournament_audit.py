#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS architecture tournament audit."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_architecture_tournament_audit.py")
SPEC = importlib.util.spec_from_file_location("vlass_architecture_audit", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def synthetic_workload() -> dict[str, object]:
    """Return a minimal promoted workload contract."""

    return {
        "id": "vlass-fragment-single-field-clean-4096-full-16-spw",
        "imaging": {
            "imsize": 4096,
            "spw": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "weighting": "briggs",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "scales": [0, 5, 12],
            "niter": 2000,
            "aterm": True,
            "psterm": False,
            "wbawp": True,
            "conjbeams": True,
            "usepointing": True,
            "mask_image": "/tmp/mask",
            "mask_sha256": "a" * 64,
        },
        "comparison": {
            "source_regions": [
                {
                    "id": "source",
                    "blc": [10, 20],
                    "trc": [13, 23],
                }
            ]
        },
    }


def synthetic_log() -> str:
    """Return a consistent two-block, two-refresh run log."""

    return "\n".join(
        (
            "mfs_ddid_execution_plan "
            "spws=2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 "
            "rows=10 selected_channel_visits=640",
            "awproject_metal_grid_summary pass=initial_dirty calls=2 "
            "samples=100 kernel_values=1000",
            "awproject_compact_source_order routed_samples=50 "
            "packed_sample_bytes=196 materialize_ms=2.5",
            "awproject_compact_source_order routed_samples=50 "
            "packed_sample_bytes=196 materialize_ms=3.5",
            *(
                "awproject_compact_source_order routed_samples=0 "
                "packed_sample_bytes=196 materialize_ms=0.0"
                for _ in range(14)
            ),
            "awproject_compact_replay_cache resident_bytes=4000 resident_blocks=16",
            "mtmfs_multiscale_minor_cycle_profile "
            "updates=2 candidate_positions=[16, 24, 32]",
            "mosaic_mtmfs_minor_cycle actual_updates=2",
            "awproject_metal_resident_tile_chain samples=50 "
            "prediction_kernel_values=100 imaging_kernel_values=200",
            "awproject_metal_resident_tile_chain samples=50 "
            "prediction_kernel_values=150 imaging_kernel_values=250",
            "awproject_metal_grid_summary pass=residual_refresh calls=2 "
            "samples=100 kernel_values=700",
            "mosaic_mtmfs_residual_refresh major_cycle=1 reported_iterations=2",
            "mtmfs_multiscale_minor_cycle_profile "
            "updates=3 candidate_positions=[16, 24, 32]",
            "mosaic_mtmfs_minor_cycle actual_updates=3",
            "awproject_metal_resident_tile_chain samples=50 "
            "prediction_kernel_values=110 imaging_kernel_values=210",
            "awproject_metal_resident_tile_chain samples=50 "
            "prediction_kernel_values=140 imaging_kernel_values=240",
            "awproject_metal_grid_summary pass=residual_refresh calls=2 "
            "samples=100 kernel_values=700",
            "mosaic_mtmfs_residual_refresh major_cycle=2 reported_iterations=5",
            "core stage=run_summary controller_overhead_ms=1 "
            "weighting_ms=2 psf_grid_ms=30 psf_fft_ms=4 psf_normalize_ms=5 "
            "model_fft_ms=6 residual_degrid_grid_ms=7 residual_fft_ms=8 "
            "minor_cycle_ms=9 major_cycle_refresh_ms=40 restore_ms=10 total_ms=122",
            "standard_mfs_stage_memory lifetime_peak_rss_bytes=1000 "
            "stage_observed_peak_process_physical_footprint_bytes=900 "
            "stage_observed_peak_metal_allocated_bytes=800",
            "Wrote CASA-compatible products at prefix /tmp/rust "
            "(100 gridded samples, 3 major cycles, 5 minor iterations, "
            "stop=Some(NsigmaThresholdReached))",
            "real 0.15",
        )
    )


class VlassArchitectureTournamentAuditTest(unittest.TestCase):
    def test_parser_preserves_list_with_spaces(self) -> None:
        name, values = audit.parse_event(
            "profile candidate_positions=[4096, 4604, 5136] total_ms=1.25"
        )
        self.assertEqual(name, "profile")
        self.assertEqual(values["candidate_positions"], [4096, 4604, 5136])
        self.assertEqual(values["total_ms"], 1.25)

    def test_analysis_derives_operator_work_and_selects_direct_path(self) -> None:
        result = audit.analyze_frozen_run(synthetic_workload(), synthetic_log())
        trajectory = result["incumbent"]["operator_trajectory"]
        self.assertEqual(trajectory["residual_refreshes"], 2)
        self.assertEqual(trajectory["logical_expensive_operator_calls"], 5)
        self.assertEqual(trajectory["total_kernel_interactions"], 2400)
        self.assertEqual(trajectory["replay_materialization_ms"], 6.0)
        self.assertEqual(trajectory["candidate_positions_by_scale"], [16, 24, 32])
        self.assertEqual(
            result["selection"]["first_executable_discriminator"],
            "visibility-resident-mask-local",
        )
        cards = {card["id"]: card for card in result["candidate_cards"]}
        self.assertEqual(
            cards["visibility-resident-mask-local"]["work_proxy"][
                "direct_component_pairs_actual_trajectory"
            ],
            500,
        )
        self.assertGreater(
            cards["idg-image-domain-subgrid"]["side_sweep"][0]["standalone_work_ratio"],
            1.0,
        )

    def test_rejects_inconsistent_residual_block_count(self) -> None:
        lines = synthetic_log().splitlines()
        lines.remove(
            "awproject_metal_resident_tile_chain samples=50 "
            "prediction_kernel_values=140 imaging_kernel_values=240"
        )
        with self.assertRaisesRegex(
            audit.TournamentError,
            "residual replay line count",
        ):
            audit.analyze_frozen_run(synthetic_workload(), "\n".join(lines))

    def test_rejects_mask_cardinality_mismatch(self) -> None:
        workload = copy.deepcopy(synthetic_workload())
        workload["comparison"]["source_regions"][0]["trc"] = [14, 23]
        with self.assertRaisesRegex(
            audit.TournamentError,
            "mask cardinality",
        ):
            audit.analyze_frozen_run(workload, synthetic_log())

    def test_rejects_changed_science_contract(self) -> None:
        workload = copy.deepcopy(synthetic_workload())
        workload["imaging"]["usepointing"] = False
        with self.assertRaisesRegex(
            audit.TournamentError,
            "promoted imaging contract",
        ):
            audit.analyze_frozen_run(workload, synthetic_log())


if __name__ == "__main__":
    unittest.main()
