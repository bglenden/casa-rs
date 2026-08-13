# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for bounded host memory/swap evidence."""

from __future__ import annotations

import copy
import subprocess
import unittest

from perf_harness.host_telemetry import (
    HostTelemetryError,
    SAMPLE_FIELDS,
    build_host_telemetry_result,
    read_darwin_host_snapshot,
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


class HostTelemetryTests(unittest.TestCase):
    def test_darwin_snapshot_parses_required_pressure_and_swap_counters(self) -> None:
        outputs = iter((VM_STAT, MEMORY_PRESSURE))

        def run(*args, **kwargs):
            return subprocess.CompletedProcess(args[0], 0, next(outputs), None)

        snapshot = read_darwin_host_snapshot(command_runner=run)

        self.assertEqual(16_384, snapshot["page_size_bytes"])
        self.assertEqual(34_359_738_368, snapshot["physical_memory_bytes"])
        self.assertEqual(86, snapshot["memory_free_percent"])
        self.assertEqual(20, snapshot["swapins"])
        self.assertEqual(30, snapshot["swapouts"])

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
        }
        assert set(sample) == SAMPLE_FIELDS
        return sample


if __name__ == "__main__":
    unittest.main()
