#!/usr/bin/env python3
"""Focused tests for the real-CLEAN VLASS autoresearch adapter."""

from __future__ import annotations

import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest

from autoresearch import vlass_4096_recovery as subject


class Vlass4096RecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = subject.load_object(subject.DEFAULT_CONTRACT)

    def test_contract_pins_100x_target_and_five_percent_all_field_guard(self) -> None:
        subject.validate_contract(self.contract)
        self.assertAlmostEqual(
            100.0,
            self.contract["single_field"]["matched_casa_wall_seconds"]
            / self.contract["single_field"]["target_wall_seconds"],
        )
        self.assertAlmostEqual(
            1.05,
            self.contract["all_fields"]["maximum_wall_seconds"]
            / self.contract["all_fields"]["baseline_wall_seconds"],
        )

    def test_contract_rejects_weakened_target_or_all_field_guard(self) -> None:
        changed = copy.deepcopy(self.contract)
        changed["single_field"]["target_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "exact 100x"):
            subject.validate_contract(changed)

        changed = copy.deepcopy(self.contract)
        changed["all_fields"]["maximum_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "exactly 5%"):
            subject.validate_contract(changed)

    def test_parse_wall_requires_one_positive_time_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            log.write_text("work\nreal 28.65\n", encoding="utf-8")
            self.assertEqual(28.65, subject.parse_wall(log))
            log.write_text("real 28.65\nreal 29.0\n", encoding="utf-8")
            with self.assertRaisesRegex(subject.ContractError, "exactly one"):
                subject.parse_wall(log)

    def test_all_field_log_rejects_spill_or_topology_rebuild(self) -> None:
        row = self.contract["all_fields"]
        good = "\n".join(
            [
                f"mfs_ddid_execution_plan ddids={row['ddids']} spws=2,7,12,17",
                "standard_mfs_execution_plan selected_channels=64",
                "standard_mfs_execution_allocation component=POINTING index",
                f"awproject_selected_field_count selected_fields={row['selected_fields']}",
                f"awproject_grouped_replay_plan architecture=source-order-grouped-tile-v1 segment_target_bytes={row['segment_target_bytes']} omitted_squared_l2_energy=0.000000000e0",
                "awproject_plan usepointing=true",
                "awproject_aot_grouped_tile_receipt segment=0 omitted_energy_fraction_bits=0",
                "awproject_metal_resident_grouped_replay_summary spill_read_bytes=0 runtime_grouping_builds=0 runtime_sort_builds=0 runtime_route_builds=0",
                "standard_mfs_stage_memory swapout_bytes_delta=0",
                f"Wrote CASA-compatible products at prefix /tmp/rust ({row['gridded_samples']} gridded samples, {row['major_cycles']} major cycles, {row['minor_iterations']} minor iterations, stop=Some(NsigmaThresholdReached))",
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            log.write_text(good + "\n", encoding="utf-8")
            receipt = subject.validate_all_field_log(self.contract, log)
            self.assertEqual(1, receipt["segments"])
            log.write_text(
                good.replace("spill_read_bytes=0", "spill_read_bytes=1") + "\n"
            )
            with self.assertRaisesRegex(subject.ContractError, "spill_read_bytes"):
                subject.validate_all_field_log(self.contract, log)

    def test_raw_comparison_status_can_be_deferred_only_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "command.log"
            command = [sys.executable, "-c", "raise SystemExit(1)"]
            with self.assertRaisesRegex(subject.ContractError, "exited 1"):
                subject.run_checked(
                    command,
                    environment=os.environ.copy(),
                    log_path=log,
                )
            completed = subject.run_checked(
                command,
                environment=os.environ.copy(),
                log_path=log,
                accepted_returncodes=(0, 1),
            )
            self.assertEqual(1, completed.returncode)


if __name__ == "__main__":
    unittest.main()
