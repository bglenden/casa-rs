#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS exact-CF-key FFT occupancy reducer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_cf_key_fft_occupancy.py")
SPEC = importlib.util.spec_from_file_location(
    "vlass_cf_key_fft_occupancy",
    MODULE_PATH,
)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def synthetic_event(
    tile_side: int,
    *,
    total_units: int,
    references_per_tile: float = 8.0,
    spectrum_bytes: int = 1_000,
) -> str:
    """Return one complete tile-curve event."""

    return (
        f"awproject_cf_key_fft_occupancy tile_side={tile_side} samples=10 "
        "windows=2 logical_calls=11 logical_plan_references=560 "
        "exact_kernel_keys=20 logical_rhs_buckets=30 logical_active_tiles=70 "
        f"references_per_active_tile={references_per_tile:.6f} "
        "padded_fft_cells=1000 transform_complex_units=2000 "
        f"kernel_fft_units=100 total_complex_units={total_units} "
        f"persistent_kernel_spectrum_bytes={spectrum_bytes} "
        "peak_tile_scratch_bytes=500"
    )


def synthetic_log(
    *,
    references_per_tile: float = 8.0,
    spectrum_bytes: int = 1_000,
) -> str:
    """Return a four-point tile curve with side 64 best."""

    return "\n".join(
        (
            synthetic_event(
                32,
                total_units=5_000,
                references_per_tile=references_per_tile,
                spectrum_bytes=spectrum_bytes,
            ),
            synthetic_event(
                64,
                total_units=3_000,
                references_per_tile=references_per_tile,
                spectrum_bytes=spectrum_bytes,
            ),
            synthetic_event(
                128,
                total_units=4_000,
                references_per_tile=references_per_tile,
                spectrum_bytes=spectrum_bytes,
            ),
            synthetic_event(
                256,
                total_units=6_000,
                references_per_tile=references_per_tile,
                spectrum_bytes=spectrum_bytes,
            ),
        )
    )


class VlassCfKeyFftOccupancyTest(unittest.TestCase):
    def test_selects_formula_minimum_but_requires_hardware_race(self) -> None:
        result = audit.analyze_log(
            synthetic_log(),
            expected_samples=10,
            expected_references=560,
            aw_budget_seconds=2.0,
        )
        self.assertEqual(result["selection"]["tile_side"], 64)
        self.assertEqual(
            result["selection"]["decision"],
            "promote-matched-metal-fft-race",
        )
        self.assertIn("cannot compare", result["claim_boundary"])

    def test_retires_no_reuse_or_over_memory_routes(self) -> None:
        no_reuse = audit.analyze_log(
            synthetic_log(references_per_tile=1.5),
            expected_samples=10,
            expected_references=560,
        )
        self.assertEqual(
            no_reuse["selection"]["decision"],
            "retire-exact-key-overlap-save-no-spatial-reuse",
        )
        over_memory = audit.analyze_log(
            synthetic_log(spectrum_bytes=2_000),
            expected_samples=10,
            expected_references=560,
            memory_limit_bytes=2_400,
        )
        self.assertEqual(
            over_memory["selection"]["decision"],
            "retire-persistent-exact-key-spectra",
        )

    def test_rejects_missing_tile_or_reference_contract(self) -> None:
        missing = "\n".join(synthetic_log().splitlines()[:-1])
        with self.assertRaisesRegex(audit.CfKeyFftOccupancyError, "tile sides"):
            audit.analyze_log(
                missing,
                expected_samples=10,
                expected_references=560,
            )
        changed = synthetic_log().replace(
            "logical_plan_references=560",
            "logical_plan_references=559",
            1,
        )
        with self.assertRaisesRegex(
            audit.CfKeyFftOccupancyError,
            "reference contract",
        ):
            audit.analyze_log(
                changed,
                expected_samples=10,
                expected_references=560,
            )


if __name__ == "__main__":
    unittest.main()
