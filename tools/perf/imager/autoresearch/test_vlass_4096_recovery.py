#!/usr/bin/env python3
"""Focused tests for the real-CLEAN VLASS autoresearch adapter."""

from __future__ import annotations

import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from autoresearch import vlass_4096_recovery as subject


class Vlass4096RecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = subject.load_object(subject.DEFAULT_CONTRACT)

    def test_contract_pins_both_100x_targets_and_sequential_guard(self) -> None:
        subject.validate_contract(self.contract)
        self.assertEqual(120, self.contract["phase_two_single_guard_cooldown_seconds"])
        self.assertEqual(75.0, self.contract["host_idle"]["minimum_idle_cpu_percent"])
        self.assertEqual(2, self.contract["host_idle"]["consecutive_samples"])
        self.assertEqual(1, self.contract["single_field"]["warmup_runs"])
        self.assertEqual(3, self.contract["single_field"]["timed_repetitions"])
        self.assertEqual(
            60, self.contract["single_field"]["inter_run_quiescence_seconds"]
        )
        self.assertEqual(7, self.contract["all_fields"]["requested_grid_threads"])
        for row_name in ("single_field", "all_fields"):
            row = self.contract[row_name]
            self.assertAlmostEqual(
                100.0,
                row["matched_casa_wall_seconds"] / row["target_wall_seconds"],
            )
        self.assertAlmostEqual(
            1.05,
            self.contract["all_fields"]["sequential_guard_maximum_wall_seconds"]
            / self.contract["all_fields"]["sequential_guard_baseline_wall_seconds"],
        )
        for baseline, maximum in (
            ("cold_instructions_retired", "maximum_instructions_retired"),
            ("cold_cycles_elapsed", "maximum_cycles_elapsed"),
        ):
            row = self.contract["all_fields"]
            self.assertEqual(int(row[baseline] * 1.05), row[maximum])

    def test_contract_rejects_weakened_target_or_all_field_guard(self) -> None:
        changed = copy.deepcopy(self.contract)
        changed["single_field"]["target_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "exact 100x"):
            subject.validate_contract(changed)

        changed = copy.deepcopy(self.contract)
        changed["all_fields"]["sequential_guard_maximum_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "exactly 5%"):
            subject.validate_contract(changed)

        changed = copy.deepcopy(self.contract)
        changed["all_fields"]["target_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "all-field target"):
            subject.validate_contract(changed)

        changed = copy.deepcopy(self.contract)
        changed["phase_two_single_guard_cooldown_seconds"] = 0
        with self.assertRaisesRegex(subject.ContractError, "cooldown"):
            subject.validate_contract(changed)

        changed = copy.deepcopy(self.contract)
        changed["single_field"]["stability_maximum_wall_seconds"] += 1.0
        with self.assertRaisesRegex(subject.ContractError, "stability ceiling"):
            subject.validate_contract(changed)

    def test_parse_wall_requires_one_positive_time_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            log.write_text("work\nreal 28.65\n", encoding="utf-8")
            self.assertEqual(28.65, subject.parse_wall(log))
            log.write_text("real 28.65\nreal 29.0\n", encoding="utf-8")
            with self.assertRaisesRegex(subject.ContractError, "exactly one"):
                subject.parse_wall(log)

    def test_time_counters_are_exact_and_enforce_cold_five_percent_guard(self) -> None:
        row = self.contract["all_fields"]
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            log.write_text(
                f"  {row['maximum_instructions_retired']} instructions retired\n"
                f"  {row['maximum_cycles_elapsed']} cycles elapsed\n",
                encoding="utf-8",
            )
            self.assertEqual(
                {
                    "instructions_retired": row["maximum_instructions_retired"],
                    "cycles_elapsed": row["maximum_cycles_elapsed"],
                },
                subject.parse_time_counters(log),
            )
            log.write_text("1 instructions retired\n", encoding="utf-8")
            with self.assertRaisesRegex(subject.ContractError, "cycles_elapsed"):
                subject.parse_time_counters(log)

    def test_single_series_uses_median_and_rejects_unstable_work(self) -> None:
        def run(wall: float, instructions: int, cycles: int, user: float) -> dict:
            return {
                "wall_seconds": wall,
                "outer_time_counters": {
                    "instructions_retired": instructions,
                    "cycles_elapsed": cycles,
                },
                "outer_process_times": {"user_seconds": user, "sys_seconds": 1.0},
            }

        good = [
            run(35.8, 1_000_000, 2_000_000, 100.0),
            run(36.0, 1_005_000, 2_020_000, 101.0),
            run(36.1, 999_000, 1_990_000, 99.5),
        ]
        summary = subject.validate_single_series(self.contract, good)
        self.assertEqual(36.0, summary["median_wall_seconds"])
        self.assertEqual(1, summary["median_run_index"])

        product_write_outlier = [
            run(37.71, 1_000_000, 2_000_000, 98.29),
            run(35.52, 1_005_000, 2_020_000, 100.12),
            run(34.49, 999_000, 1_990_000, 100.47),
        ]
        summary = subject.validate_single_series(self.contract, product_write_outlier)
        self.assertEqual(35.52, summary["median_wall_seconds"])
        self.assertEqual(37.71, summary["maximum_wall_seconds"])

        slow = copy.deepcopy(good)
        slow[2]["wall_seconds"] = (
            self.contract["single_field"]["stability_maximum_wall_seconds"] + 0.01
        )
        with self.assertRaisesRegex(subject.ContractError, "maximum wall"):
            subject.validate_single_series(self.contract, slow)

        unstable = copy.deepcopy(good)
        unstable[2]["outer_process_times"]["user_seconds"] = 110.0
        with self.assertRaisesRegex(subject.ContractError, "user CPU"):
            subject.validate_single_series(self.contract, unstable)

    def test_host_idle_wait_requires_consecutive_qualified_samples(self) -> None:
        with (
            mock.patch.object(
                subject,
                "sample_host_idle_cpu_percent",
                side_effect=[90.0, 10.0, 80.0, 85.0],
            ),
            mock.patch.object(subject, "assert_no_competing_imager"),
            mock.patch.object(subject.time, "sleep") as sleep,
        ):
            receipt = subject.wait_for_host_idle(self.contract)
        self.assertEqual("idle", receipt["status"])
        self.assertEqual([90.0, 10.0, 80.0, 85.0], receipt["observed_idle_cpu_percent"])
        self.assertEqual(3, sleep.call_count)

    def test_all_field_log_rejects_spill_or_topology_rebuild(self) -> None:
        row = self.contract["all_fields"]
        good = "\n".join(
            [
                f"mfs_ddid_execution_plan ddids={row['ddids']} spws=2,7,12,17",
                "standard_mfs_execution_plan selected_channels=64 workers=4",
                "standard_mfs_execution_allocation component=POINTING index",
                f"awproject_selected_field_count selected_fields={row['selected_fields']}",
                f"awproject_grouped_replay_plan architecture=source-order-grouped-tile-v1 segment_target_bytes={row['segment_target_bytes']} omitted_squared_l2_energy=0.000000000e0",
                "awproject_plan usepointing=true",
                "awproject_aot_grouped_tile_receipt segment=0 omitted_energy_fraction_bits=0",
                "awproject_metal_resident_grouped_replay_summary spill_read_bytes=0 runtime_grouping_builds=0 runtime_sort_builds=0 runtime_route_builds=0",
                "standard_mfs_stage_memory swapout_bytes_delta=0",
                f"Wrote CASA-compatible products at prefix /tmp/rust ({row['gridded_samples']} gridded samples, {row['major_cycles']} major cycles, {row['minor_iterations']} minor iterations, stop=Some(NsigmaThresholdReached))",
                f"  {row['cold_instructions_retired']} instructions retired",
                f"  {row['cold_cycles_elapsed']} cycles elapsed",
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "run.log"
            log.write_text(good + "\n", encoding="utf-8")
            receipt = subject.validate_all_field_log(
                self.contract, log, enforce_sequential_guard=True
            )
            self.assertEqual(1, receipt["segments"])
            self.assertEqual(4, receipt["effective_workers"])
            log.write_text(
                good.replace("spill_read_bytes=0", "spill_read_bytes=1") + "\n"
            )
            with self.assertRaisesRegex(subject.ContractError, "spill_read_bytes"):
                subject.validate_all_field_log(self.contract, log)

            too_many = good.replace(
                str(row["cold_cycles_elapsed"]),
                str(row["maximum_cycles_elapsed"] + 1),
            )
            log.write_text(too_many + "\n", encoding="utf-8")
            with self.assertRaisesRegex(subject.ContractError, "cycles_elapsed"):
                subject.validate_all_field_log(
                    self.contract, log, enforce_sequential_guard=True
                )

    def test_all_field_launch_binds_requested_grid_workers(self) -> None:
        captured: dict[str, str] = {}

        def stop_before_launch(*args, **kwargs):
            captured.update(kwargs["environment"])
            raise subject.ContractError("stop before launch")

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(
                subject, "run_checked", side_effect=stop_before_launch
            ):
                with self.assertRaisesRegex(
                    subject.ContractError, "stop before launch"
                ):
                    subject.run_all_fields(
                        self.contract,
                        Path(directory) / "casars-imager",
                        Path(directory),
                        "test",
                        enforce_sequential_guard=False,
                    )
        self.assertEqual(
            str(self.contract["all_fields"]["requested_grid_threads"]),
            captured["CASA_RS_VLASS_GRID_THREADS"],
        )

    def test_single_field_launch_pins_two_grid_workers(self) -> None:
        captured: dict[str, str] = {}

        def stop_before_launch(*args, **kwargs):
            captured.update(kwargs["environment"])
            raise subject.ContractError("stop before launch")

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(
                subject, "run_checked", side_effect=stop_before_launch
            ):
                with self.assertRaisesRegex(
                    subject.ContractError, "stop before launch"
                ):
                    subject.run_single(
                        self.contract,
                        Path(directory) / "casars-imager",
                        Path(directory),
                        "test",
                    )
        self.assertEqual("2", captured["CASA_RS_VLASS_GRID_THREADS"])

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
