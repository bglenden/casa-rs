#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS exact-plan subgrid speed-of-light reducer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_subgrid_speed_of_light.py")
SPEC = importlib.util.spec_from_file_location(
    "vlass_subgrid_speed_of_light",
    MODULE_PATH,
)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def synthetic_event(
    *,
    block: int,
    samples: int,
    cell_updates: int,
    core_device_ms: float,
    projected_bytes: int = 1_000_000,
) -> str:
    """Return one exact-trajectory diagnostic event."""

    return (
        f"awproject_subgrid_speed_of_light block={block} window=0 "
        f"samples={samples} plans={samples * 8} "
        f"stored_plan_references={samples * 24} "
        f"logical_plan_references={samples * 56} "
        "stored_stripes=10 logical_groups=30 logical_stripes=40 "
        f"logical_dispatches=11 logical_cell_updates={cell_updates} "
        "screen_values=100 output_cells=200 screen_bytes=800 "
        "row_phase_bytes=1600 trace_bytes=2400 output_bytes=1600 "
        f"resident_bytes=6400 projected_full_geometry_peak_bytes={projected_bytes} "
        "build_ms=2.000 executor_setup_ms=3.000 buffer_alloc_ms=4.000 "
        "warm_wall_ms=6.000 warm_device_ms=5.000 "
        f"core_wall_ms={core_device_ms + 1:.3f} "
        f"core_device_ms={core_device_ms:.3f} "
        "readback_ms=1.000 cell_gs=4.000000 "
        f"output_sha256={'1' * 63}{block + 1:x}"
    )


def synthetic_log(core_device_ms: float = 1_000.0) -> str:
    """Return a two-window exact trajectory."""

    return "\n".join(
        (
            synthetic_event(
                block=0,
                samples=4,
                cell_updates=8_000,
                core_device_ms=core_device_ms / 2,
            ),
            synthetic_event(
                block=1,
                samples=6,
                cell_updates=12_000,
                core_device_ms=core_device_ms / 2,
            ),
        )
    )


class VlassSubgridSpeedOfLightTest(unittest.TestCase):
    def test_green_trace_promotes_only_the_full_race(self) -> None:
        result = audit.analyze_log(
            synthetic_log(1_000.0),
            expected_samples=10,
            expected_references=560,
            expected_cell_updates=20_000,
            aw_budget_seconds=3.0,
        )
        self.assertEqual(
            result["gates"]["decision"],
            "promote-full-inverse-cf-race",
        )
        self.assertEqual(result["timing"]["source"], "metal-device-time")
        self.assertAlmostEqual(result["timing"]["core_seconds"], 1.0)

    def test_amber_trace_requires_omitted_floor_measurement(self) -> None:
        result = audit.analyze_log(
            synthetic_log(1_700.0),
            expected_samples=10,
            expected_references=560,
            expected_cell_updates=20_000,
            aw_budget_seconds=3.0,
        )
        self.assertEqual(
            result["gates"]["decision"],
            "conditional-measure-omitted-floors-before-promotion",
        )

    def test_slow_or_over_memory_trace_retires_the_family(self) -> None:
        slow = audit.analyze_log(
            synthetic_log(2_000.0),
            expected_samples=10,
            expected_references=560,
            expected_cell_updates=20_000,
            aw_budget_seconds=3.0,
        )
        self.assertEqual(slow["gates"]["decision"], "retire-direct-subgrid-family")
        over_memory = synthetic_log(1_000.0).replace(
            "projected_full_geometry_peak_bytes=1000000",
            "projected_full_geometry_peak_bytes=2000000",
        )
        result = audit.analyze_log(
            over_memory,
            expected_samples=10,
            expected_references=560,
            expected_cell_updates=20_000,
            aw_budget_seconds=3.0,
            full_geometry_memory_limit_bytes=1_500_000,
        )
        self.assertEqual(result["gates"]["decision"], "retire-direct-subgrid-family")

    def test_rejects_changed_call_or_reference_contract(self) -> None:
        changed_calls = synthetic_log().replace("logical_dispatches=11", "logical_dispatches=9", 1)
        with self.assertRaisesRegex(audit.SubgridSpeedOfLightError, "expected 11"):
            audit.analyze_log(
                changed_calls,
                expected_samples=10,
                expected_references=560,
                expected_cell_updates=20_000,
            )
        changed_references = synthetic_log().replace(
            "logical_plan_references=224",
            "logical_plan_references=223",
            1,
        )
        with self.assertRaisesRegex(audit.SubgridSpeedOfLightError, "56-reference"):
            audit.analyze_log(
                changed_references,
                expected_samples=10,
                expected_references=560,
                expected_cell_updates=20_000,
            )


if __name__ == "__main__":
    unittest.main()
