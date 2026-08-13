# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for bounded host memory/swap evidence."""

from __future__ import annotations

import copy
from pathlib import Path
import subprocess
import tempfile
import unittest

from perf_harness.host_telemetry import (
    DarwinHostTelemetrySampler,
    HostTelemetryError,
    LEGACY_SAMPLE_FIELDS,
    LEGACY_SCHEMA_VERSION,
    LEGACY_SCOPE,
    SAMPLE_FIELDS,
    build_host_telemetry_result,
    read_darwin_host_snapshot,
    read_process_pid_receipt,
    read_darwin_volume_snapshot,
    validate_host_telemetry,
)


VM_STAT = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free: 100.
Pages active: 200.
Pages inactive: 300.
Pages speculative: 4.
Pages throttled: 0.
Pages wired down: 50.
Pages purgeable: 6.
Pages stored in compressor: 70.
Pages occupied by compressor: 8.
Pageins: 900.
Pageouts: 10.
Swapins: 20.
Swapouts: 30.
"""
MEMORY_PRESSURE = """The system has 34359738368 (2097152 pages with a page size of 16384).
System-wide memory free percentage: 86%
"""
SWAP_USAGE = "total = 4096.00M  used = 768.25M  free = 3327.75M"
DISKUTIL_PLIST = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>DeviceIdentifier</key><string>disk5s1</string>
  <key>ParentWholeDisk</key><string>disk5</string>
  <key>APFSPhysicalStores</key>
  <array>
    <dict>
      <key>DeviceIdentifier</key><string>disk4s2</string>
    </dict>
  </array>
</dict>
</plist>
"""
IOREG = """+-o IOBlockStorageDriver  <class IOBlockStorageDriver>
  | {
  |   "Statistics" = {"Bytes (Read)"=1694439639040,"Bytes (Write)"=967807643648}
  | }
  +-o External Media  <class IOMedia>
    | {
    |   "Whole" = Yes
    |   "BSD Name" = "disk4"
    | }
"""


class HostTelemetryTests(unittest.TestCase):
    def test_darwin_snapshot_parses_required_pressure_and_swap_counters(self) -> None:
        outputs = iter((VM_STAT, MEMORY_PRESSURE, SWAP_USAGE))

        def run(*args, **kwargs):
            return subprocess.CompletedProcess(args[0], 0, next(outputs), None)

        snapshot = read_darwin_host_snapshot(command_runner=run)

        self.assertEqual(16_384, snapshot["page_size_bytes"])
        self.assertEqual(34_359_738_368, snapshot["physical_memory_bytes"])
        self.assertEqual(86, snapshot["memory_free_percent"])
        self.assertEqual(20, snapshot["swapins"])
        self.assertEqual(30, snapshot["swapouts"])
        self.assertEqual(8 * 16_384, snapshot["host_compressed_memory_bytes"])
        self.assertEqual(round(768.25 * 1024**2), snapshot["swap_used_bytes"])

    def test_volume_snapshot_resolves_physical_device_and_ioreg_counters(self) -> None:
        outputs = iter((DISKUTIL_PLIST, IOREG))

        def run(*args, **kwargs):
            return subprocess.CompletedProcess(args[0], 0, next(outputs), None)

        snapshot = read_darwin_volume_snapshot(
            "/Volumes/EXTERNAL/spill",
            command_runner=run,
        )

        self.assertEqual("disk4", snapshot["spill_volume_device"])
        self.assertEqual(1_694_439_639_040, snapshot["spill_volume_read_bytes"])
        self.assertEqual(967_807_643_648, snapshot["spill_volume_write_bytes"])

    def test_summary_reports_amount_rate_and_minimum_pressure(self) -> None:
        first = self._sample(elapsed=0.0, free=86, swapins=20, swapouts=30)
        last = self._sample(elapsed=5.0, free=31, swapins=120, swapouts=230)

        result = build_host_telemetry_result(
            interval_seconds=5.0,
            samples=[first, last],
            errors=[],
        )

        validate_host_telemetry(result)
        summary = result["summary"]
        self.assertEqual(31, summary["memory_free_percent_min"])
        self.assertEqual(100 * 16_384, summary["swapin_bytes_delta"])
        self.assertEqual(200 * 16_384, summary["swapout_bytes_delta"])
        self.assertEqual(300 * 16_384 / 5.0, summary["swap_io_bytes_per_second_max"])
        self.assertEqual(8 * 16_384, summary["host_compressed_memory_bytes_peak"])
        self.assertEqual(805_568_512, summary["swap_used_bytes_peak"])

    def test_sampler_attaches_pid_and_spill_volume_after_start(self) -> None:
        command_outputs = {
            "/usr/bin/vm_stat": VM_STAT,
            "/usr/bin/memory_pressure": MEMORY_PRESSURE,
            "/usr/sbin/sysctl": SWAP_USAGE,
        }

        def run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, command_outputs[args[0]], None)

        process_calls = 0

        def read_process(pid):
            nonlocal process_calls
            process_calls += 1
            return {
                "process_physical_footprint_bytes": 1_000 + process_calls * 100,
                "process_physical_footprint_bytes_lifetime_peak": (
                    1_500 + process_calls * 100
                ),
                "process_resident_memory_bytes": 800 + process_calls * 100,
                "process_page_faults": 20 + process_calls * 3,
                "process_disk_read_bytes": 4_000 + process_calls * 50,
                "process_disk_write_bytes": 6_000 + process_calls * 70,
            }

        volume_calls = 0

        def read_volume(path):
            nonlocal volume_calls
            volume_calls += 1
            return {
                "spill_volume_device": "disk4",
                "spill_volume_read_bytes": 10_000 + volume_calls * 500,
                "spill_volume_write_bytes": 20_000 + volume_calls * 700,
            }

        clock = iter((10.0, 10.0, 11.0, 12.0))
        sampler = DarwinHostTelemetrySampler(
            interval_seconds=3600.0,
            command_runner=run,
            monotonic=lambda: next(clock),
            utc_now=lambda: "2026-07-30T00:00:00Z",
            platform_system=lambda: "Darwin",
            process_snapshot_reader=read_process,
            volume_snapshot_reader=read_volume,
        )
        sampler.start()
        sampler.attach_targets(
            process_pid=4242,
            spill_volume_path="/Volumes/EXTERNAL/spill",
        )
        result = sampler.stop()

        validate_host_telemetry(result)
        self.assertIsNone(result["samples"][0]["process_pid"])
        self.assertEqual(4242, result["samples"][1]["process_pid"])
        self.assertEqual(
            "/Volumes/EXTERNAL/spill",
            result["samples"][1]["spill_volume_path"],
        )
        self.assertEqual(
            1_700,
            result["summary"]["process_physical_footprint_bytes_peak"],
        )
        self.assertEqual(3, result["summary"]["process_page_faults_delta"])
        self.assertEqual(500, result["summary"]["spill_volume_read_bytes_delta"])
        self.assertEqual(700, result["summary"]["spill_volume_write_bytes_delta"])

    def test_process_pid_receipt_is_absent_until_exec_and_then_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "casars-imager.pid"
            self.assertIsNone(read_process_pid_receipt(path))
            path.write_text("", encoding="utf-8")
            self.assertIsNone(read_process_pid_receipt(path))
            path.write_text("4242\n", encoding="utf-8")
            self.assertEqual(4242, read_process_pid_receipt(path))
            path.write_text("4242 trailing\n", encoding="utf-8")
            with self.assertRaisesRegex(HostTelemetryError, "positive integer"):
                read_process_pid_receipt(path)

    def test_sampler_resolves_actual_child_pid_from_receipt(self) -> None:
        command_outputs = {
            "/usr/bin/vm_stat": VM_STAT,
            "/usr/bin/memory_pressure": MEMORY_PRESSURE,
            "/usr/sbin/sysctl": SWAP_USAGE,
        }

        def run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, command_outputs[args[0]], None)

        observed_pids: list[int] = []

        def read_process(pid):
            observed_pids.append(pid)
            return {
                "process_physical_footprint_bytes": 1_000,
                "process_physical_footprint_bytes_lifetime_peak": 1_200,
                "process_resident_memory_bytes": 800,
                "process_page_faults": 20,
                "process_disk_read_bytes": 4_000,
                "process_disk_write_bytes": 6_000,
            }

        with tempfile.TemporaryDirectory() as temporary:
            pid_path = Path(temporary) / "casars-imager.pid"
            pid_path.write_text("5151\n", encoding="utf-8")
            clock = iter((10.0, 10.0, 11.0, 12.0))
            sampler = DarwinHostTelemetrySampler(
                interval_seconds=3600.0,
                command_runner=run,
                monotonic=lambda: next(clock),
                utc_now=lambda: "2026-07-30T00:00:00Z",
                platform_system=lambda: "Darwin",
                process_snapshot_reader=read_process,
            )
            sampler.start()
            sampler.attach_targets(process_pid_file=pid_path)
            result = sampler.stop()

        validate_host_telemetry(result)
        self.assertEqual([5151, 5151], observed_pids)
        self.assertEqual(5151, result["samples"][1]["process_pid"])
        self.assertEqual(1_200, result["summary"]["process_physical_footprint_bytes_peak"])

    def test_validator_accepts_legacy_v1_receipt(self) -> None:
        sample = {
            field: value
            for field, value in self._sample(
                elapsed=0.0,
                free=86,
                swapins=20,
                swapouts=30,
            ).items()
            if field in LEGACY_SAMPLE_FIELDS
        }
        legacy = {
            "schema_version": LEGACY_SCHEMA_VERSION,
            "scope": LEGACY_SCOPE,
            "status": "partial",
            "interval_seconds": 5.0,
            "sampling_errors": [],
            "samples": [sample],
            "summary": {
                "duration_seconds": 0.0,
                "sample_count": 1,
                "memory_free_percent_min": 86,
                "memory_free_percent_end": 86,
                "pages_throttled_max": 0,
                "pageouts_delta": 0,
                "swapins_delta": 0,
                "swapouts_delta": 0,
                "swapin_bytes_delta": 0,
                "swapout_bytes_delta": 0,
                "swap_io_bytes_per_second_max": 0.0,
            },
        }

        validate_host_telemetry(legacy)

    def test_validator_rejects_inconsistent_summary(self) -> None:
        result = build_host_telemetry_result(
            interval_seconds=5.0,
            samples=[
                self._sample(elapsed=0.0, free=86, swapins=20, swapouts=30),
                self._sample(elapsed=5.0, free=80, swapins=21, swapouts=31),
            ],
            errors=[],
        )
        invalid = copy.deepcopy(result)
        invalid["summary"]["sample_count"] = 3

        with self.assertRaisesRegex(HostTelemetryError, "sample count"):
            validate_host_telemetry(invalid)

    @staticmethod
    def _sample(
        *, elapsed: float, free: int, swapins: int, swapouts: int
    ) -> dict[str, object]:
        sample: dict[str, object] = {
            "observed_at": "2026-07-21T00:00:00Z",
            "elapsed_seconds": elapsed,
            "physical_memory_bytes": 34_359_738_368,
            "memory_free_percent": free,
            "page_size_bytes": 16_384,
            "pages_free": 100,
            "pages_active": 200,
            "pages_inactive": 300,
            "pages_speculative": 4,
            "pages_throttled": 0,
            "pages_wired_down": 50,
            "pages_purgeable": 6,
            "pages_stored_in_compressor": 70,
            "pages_occupied_by_compressor": 8,
            "pageins": 900,
            "pageouts": 10,
            "swapins": swapins,
            "swapouts": swapouts,
            "host_compressed_memory_bytes": 8 * 16_384,
            "swap_used_bytes": 805_568_512,
            "process_pid": None,
            "process_physical_footprint_bytes": None,
            "process_physical_footprint_bytes_lifetime_peak": None,
            "process_resident_memory_bytes": None,
            "process_page_faults": None,
            "process_disk_read_bytes": None,
            "process_disk_write_bytes": None,
            "spill_volume_path": None,
            "spill_volume_device": None,
            "spill_volume_read_bytes": None,
            "spill_volume_write_bytes": None,
        }
        assert set(sample) == SAMPLE_FIELDS
        return sample


if __name__ == "__main__":
    unittest.main()
